package com.rtb.bid;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rtb.ad.Ad;
import com.rtb.ad.AdMapper;
import com.rtb.budget.BudgetService;
import com.rtb.counter.BidEvent;
import com.rtb.counter.BidEventPublisher;
import com.rtb.detail.BidDetailBuffer;
import com.rtb.resilience.CircuitBreaker;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * 竞价主链路 · M1 朴素实现 + M3 预算 Lua + M4 二价 + M5 超时/熔断/兜底 + M6 事件旁路
 *
 * <p>链路：候选检索 → 定向过滤（已下推 SQL）→ 二价计费（fall-through）→ 返回。
 *
 * <p><b>本轮（M5）新增的韧性机制</b>：
 * <ul>
 *   <li><b>超时预算</b>：入链定 {@code deadline = now + total-ms}，各跳受子预算约束，
 *       超期即放弃后续步骤、直接兜底，保证整条链路不越过 SLA。</li>
 *   <li><b>熔断</b>：DB 检索与 Redis 计费各持独立熔断器；
 *       连续失败超阈值 → OPEN（后续请求短路兜底），冷却后 HALF_OPEN 放一个探活，成功自愈。</li>
 *   <li><b>兜底广告</b>：任何失败（超时 / 熔断 OPEN / 候选为空 / 预算耗尽）一律返回 Redis 中的
 *       兜底广告（代码静态常量作最后保底），<b>绝不返回空广告、绝不请求失败</b>。</li>
 * </ul>
 *
 * <p><b>M6（事件旁路）</b>：每次 {@code /bid} 在 return 之前都会通过 {@link BidEventPublisher}
 * 异步投递一条 {@link BidEvent} 到 Kafka（分区键=adId），由三个独立消费组异步落库。
 * 发送是 fire-and-forget 且包在 try-catch 中，<b>绝不增加延迟、绝不因 Kafka 故障影响主链路</b>。
 *
 * <p>注意：M1<b>没有</b>本地缓存、<b>没有</b>走 MQ（同步链路刻意不进 MQ）。
 * 统计代码已注释（2026-10-06），/bid 只做纯服务化链路。
 */
@Slf4j
@Service
public class BidService {

    private final AdMapper adMapper;
    private final BudgetService budgetService;
    private final StringRedisTemplate redis;
    private final ObjectMapper objectMapper;
    private final BidEventPublisher bidEventPublisher;
    /** M7：竞价明细异步落库缓冲（本地有界队列 + 后台批量刷盘） */
    private final BidDetailBuffer bidDetailBuffer;

    /** 二价最小加价单位（元）。默认 1 分，可通过 rtb.min-increment 覆盖 */
    @Value("${rtb.min-increment:0.01}")
    private BigDecimal minIncrement;

    /** M5：整条链路总超时预算（ms），SLA 闸门 */
    @Value("${rtb.timeout.total-ms:1000}")
    private long totalTimeoutMs;

    /** M5：DB 候选检索子预算（ms） */
    @Value("${rtb.timeout.db-ms:800}")
    private long dbTimeoutMs;

    /** M5：Redis 计费子预算（ms） */
    @Value("${rtb.timeout.redis-ms:200}")
    private long redisTimeoutMs;

    /** M5：强制兜底开关，用于演示/复现「依赖全面故障」场景 */
    @Value("${rtb.fallback.force:false}")
    private boolean forceFallback;

    /**
     * 兜底总开关。<b>仅用于 A/B 对照实验</b>：置 false 时复现「没有兜底机制的旧实现」——
     * 所有失败出口直接返回空广告（hasAd=false），用来测量改造前的空广告率。
     * 生产语义下恒为 true：任何失败都必须收敛到兜底广告。
     */
    @Value("${rtb.fallback.enabled:true}")
    private boolean fallbackEnabled;

    /** M5：超时包裹用的有界线程池（仅阻塞式 DB/Redis 调用需要），避免 servlet 线程被长阻塞拖死 */
    private final ExecutorService timeoutPool =
            Executors.newFixedThreadPool(Math.max(16, Runtime.getRuntime().availableProcessors() * 4));

    /** M5：DB 与 Redis 各自独立熔断器，互不影响 */
    private final CircuitBreaker dbBreaker;
    private final CircuitBreaker redisBreaker;

    /** M5：兜底广告静态常量（Redis 不可用时的最后保底，保证永远有广告可返回） */
    private static final Ad STATIC_FALLBACK = buildStaticFallback();

    private static final String FALLBACK_AD_JSON =
            "{\"adId\":999999,\"advertiserId\":0,\"title\":\"兜底保底广告\",\"categoryCode\":\"mobile\",\"bid\":0.01}";

    private static final String FALLBACK_REDIS_KEY = "fallback:ad";

    public BidService(AdMapper adMapper,
                      BudgetService budgetService,
                      StringRedisTemplate redis,
                      ObjectMapper objectMapper,
                      BidEventPublisher bidEventPublisher,
                      BidDetailBuffer bidDetailBuffer,
                      @Value("${rtb.circuit-breaker.failure-threshold:5}") int failureThreshold,
                      @Value("${rtb.circuit-breaker.cooldown-ms:5000}") long cooldownMs) {
        this.adMapper = adMapper;
        this.budgetService = budgetService;
        this.redis = redis;
        this.objectMapper = objectMapper;
        this.bidEventPublisher = bidEventPublisher;
        this.bidDetailBuffer = bidDetailBuffer;
        this.dbBreaker = new CircuitBreaker(failureThreshold, cooldownMs);
        this.redisBreaker = new CircuitBreaker(failureThreshold, cooldownMs);
        seedFallbackAd();
    }

    /**
     * 执行一次竞价。M5 下任何「不该打 / 打失败 / 超时 / 预算耗尽」都统一收敛到兜底广告。
     * M6 下每个出口在 return 前都会异步发布一条竞价事件（旁路，不影响返回）。
     */
    public BidRequest.Result bid(BidRequest req) {
        String requestId = UUID.randomUUID().toString().replace("-", "");
        long startNanos = System.nanoTime();
        long deadline = startNanos + totalTimeoutMs * 1_000_000L;
        int candidateCostMs = 0;
        int billCostMs = 0;

        // 强制兜底（演示/复现开关）：直接返回兜底广告，模拟依赖全面故障
        if (forceFallback) {
            BidRequest.Result r = buildFallback(requestId);
            emit(requestId, req, r, ms(startNanos, System.nanoTime()), 0, 0);
            return r;
        }

        // ---------------------------------------------------------
        // ① 候选检索（套熔断 + 超时）。ORDER BY a.bid DESC 已把出价排序下推到 SQL。
        // ---------------------------------------------------------
        long cStart = System.nanoTime();
        List<Ad> candidates = retrieveCandidates(req, deadline);
        candidateCostMs = ms(cStart, System.nanoTime());
        if (candidates == null) {
            // 熔断 OPEN / 超时 / 异常 → 兜底，绝不返回空
            BidRequest.Result r = buildFallback(requestId);
            emit(requestId, req, r, ms(startNanos, System.nanoTime()), candidateCostMs, 0);
            return r;
        }

        // ---------------------------------------------------------
        // ② 定向过滤已在 SQL 的 WHERE/JOIN 完成，这里仅作拷贝占位
        // ---------------------------------------------------------
        List<Ad> matched = new ArrayList<>(candidates);

        // 候选为空（检索无命中）→ 兜底，不返回空广告
        if (matched.isEmpty()) {
            BidRequest.Result r = buildFallback(requestId);
            emit(requestId, req, r, ms(startNanos, System.nanoTime()), candidateCostMs, 0);
            return r;
        }

        // ---------------------------------------------------------
        // ③ 二价计费（Vickrey）+ fall-through
        //    候选已按出价降序（SQL ORDER BY a.bid DESC）。
        //    胜出者 = 沿降序第一个预算充足的候选；成交价 = min(胜出者出价, 次高出价 + 最小加价)。
        //    套 Redis 熔断 + 整体 deadline 约束：Redis 挂 / 预算耗尽 / 超时 → 兜底。
        // ---------------------------------------------------------
        Ad winner = null;
        BigDecimal payPrice = null;
        Long adId = null;
        Integer advertiserId = null;

        long bStart = System.nanoTime();
        if (redisBreaker.allowRequest()) {
            for (int i = 0; i < matched.size(); i++) {
                // 整体超时预算耗尽 → 放弃后续计费，兜底收尾
                if (System.nanoTime() > deadline) {
                    break;
                }
                Ad candidate = matched.get(i);
                // 二价成交价：次高出价 + 最小加价，且不超过自身出价
                BigDecimal secondBid = (i + 1 < matched.size())
                        ? matched.get(i + 1).getBid()      // 下一名（次高）的出价
                        : candidate.getBid();               // 仅一名候选，次高=自身
                BigDecimal price = secondBid.add(minIncrement).min(candidate.getBid());

                boolean deducted;
                try {
                    // Redis 计费包一层超时，避免慢 Redis 拖垮整条链路
                    deducted = withTimeout(() -> budgetService.deduct(
                            candidate.getId(), candidate.getDailyBudget(), price), deadline);
                    redisBreaker.onSuccess();
                } catch (Exception e) {
                    redisBreaker.onFailure();
                    deducted = false;
                }
                if (deducted) {
                    winner = candidate;
                    payPrice = price;
                    adId = candidate.getId();
                    advertiserId = candidate.getAdvertiserId();
                    break;
                }
            }
        }

        billCostMs = ms(bStart, System.nanoTime());

        // Redis 熔断 OPEN / 预算全部不足 / 整体超时 → 兜底广告保底
        if (winner == null) {
            BidRequest.Result r = buildFallback(requestId);
            emit(requestId, req, r, ms(startNanos, System.nanoTime()), candidateCostMs, billCostMs);
            return r;
        }

        // [已注释] 分段计时埋点 + bid_log 落库（M1 baseline 压测期间不采集统计，纯服务化）
        // persistLog(requestId, req, candidates.size(), matched.size(),
        //         adId, advertiserId, winner, payPrice,
        //         costMs, candidateCost, filterCost, sortCost, billCost);

        BidRequest.Result r = buildResult(requestId, candidates, matched, winner, payPrice);
        emit(requestId, req, r, ms(startNanos, System.nanoTime()), candidateCostMs, billCostMs);
        return r;
    }

    /**
     * M6：构造并发布竞价事件（旁路）。无论胜出还是兜底，都发送一条事件供下游三消费组使用。
     * 内部已 try-catch，任何异常都不影响主链路返回。
     */
    private void publishBidEvent(String requestId, String region, BidRequest.Result r) {
        try {
            BidEvent e = BidEvent.builder()
                    .eventId(UUID.randomUUID().toString().replace("-", ""))
                    .requestId(requestId)
                    .ts(System.currentTimeMillis())
                    .region(region)
                    .adId(r.getWinner() != null ? r.getWinner().getAdId() : null)
                    .advertiserId(r.getWinner() != null ? r.getWinner().getAdvertiserId() : null)
                    .winBid(r.getWinner() != null ? r.getWinner().getBid() : null)
                    .payPrice(r.getPayPrice())
                    .fallback(Boolean.TRUE.equals(r.getFallback()))
                    .hasAd(Boolean.TRUE.equals(r.getHasAd()))
                    .priceMode(r.getPriceMode() == null ? 0 : r.getPriceMode())
                    .build();
            bidEventPublisher.publish(e);
        } catch (Exception ex) {
            log.warn("发布竞价事件失败（忽略，不影响竞价返回）requestId={}: {}", requestId, ex.getMessage());
        }
    }

    /**
     * M7：把一次竞价结果「事件旁路(Kafka) + 明细落库(本地队列)」合并发出。
     * 主链路每个 return 前调用一次，保证计费事件与竞价明细一一对应、且都不阻塞返回。
     *
     * @param costMs          整条链路总耗时（ms），入链到 return 的墙钟时间
     * @param candidateCostMs 候选检索耗时（ms），DB 调用，含 SQL 内定向过滤/排序
     * @param billCostMs     计费耗时（ms），Redis 预算扣减循环
     */
    private void emit(String requestId, BidRequest req, BidRequest.Result r,
                      int costMs, int candidateCostMs, int billCostMs) {
        // 1) 计费事件旁路（Kafka，fire-and-forget，不阻塞）
        publishBidEvent(requestId, req.getRegion(), r);
        // 2) 竞价明细入本地有界队列，后台线程批量落库（可丢、可采样）
        try {
            BidLog log = new BidLog();
            log.setRequestId(requestId);
            log.setAdId(r.getWinner() != null ? r.getWinner().getAdId() : null);
            log.setAdvertiserId(r.getWinner() != null ? r.getWinner().getAdvertiserId() : null);
            log.setSlotCode(req.getSlotCode());
            log.setRegion(req.getRegion());
            log.setAge(req.getAge());
            log.setGender(req.getGender());
            log.setInterests(req.getInterests() != null ? req.getInterests() : "");
            log.setCandidateCnt(r.getCandidateCnt());
            log.setMatchCnt(r.getMatchCnt());
            log.setHasAd(Boolean.TRUE.equals(r.getHasAd()) ? 1 : 0);
            log.setWinBid(r.getWinner() != null ? r.getWinner().getBid() : null);
            log.setPayPrice(r.getPayPrice());
            log.setCostMs(costMs);
            log.setCandidateCost(candidateCostMs);
            // 定向过滤与出价排序在 M1 已下推进 SQL（WHERE/JOIN/ORDER BY a.bid DESC），
            // 不单独计时，瓶颈完全落在 candidate_cost（DB 检索）。
            log.setFilterCost(0);
            log.setSortCost(0);
            log.setBillCost(billCostMs);
            log.setBidAt(LocalDateTime.now());
            bidDetailBuffer.enqueue(log);
        } catch (Exception ex) {
            log.warn("构造/入队竞价明细失败（忽略，不影响竞价返回）requestId={}: {}", requestId, ex.getMessage());
        }
    }

    private static int ms(long fromNanos, long toNanos) {
        return (int) ((toNanos - fromNanos) / 1_000_000);
    }

    /**
     * ① 候选检索：熔断 OPEN 直接短路；否则用有界线程池包裹阻塞式 DB 调用并限时。
     *
     * @return 候选列表；任一失败（熔断/超时/异常）返回 null，由调用方走兜底
     */
    private List<Ad> retrieveCandidates(BidRequest req, long deadline) {
        if (!dbBreaker.allowRequest()) {
            return null; // 熔断 OPEN → 兜底
        }
        long remainingMs = (deadline - System.nanoTime()) / 1_000_000;
        if (remainingMs <= 0) {
            return null; // 总预算已耗尽 → 兜底
        }
        try {
            List<Ad> result = CompletableFuture
                    .supplyAsync(() -> adMapper.findCandidates(
                            req.getRegion(),
                            TargetingMapper.resolveAgeBucket(req.getAge(), req.getAgeBucket()),
                            TargetingMapper.toGender(req.getGender()),
                            req.getCategoryCode()), timeoutPool)
                    .get(Math.min(remainingMs, dbTimeoutMs), TimeUnit.MILLISECONDS);
            dbBreaker.onSuccess();
            return result;
        } catch (TimeoutException e) {
            dbBreaker.onFailure();
            log.warn("候选检索超时（>{}ms），走兜底", Math.min(remainingMs, dbTimeoutMs));
            return null;
        } catch (Exception e) {
            dbBreaker.onFailure();
            log.warn("候选检索异常，走兜底: {}", e.getMessage());
            return null;
        }
    }

    /** 用有界线程池包裹一个阻塞调用并限时（剩余预算与子预算取小） */
    private <T> T withTimeout(Supplier<T> supplier, long deadline) throws Exception {
        long remainingMs = (deadline - System.nanoTime()) / 1_000_000;
        if (remainingMs <= 0) {
            throw new TimeoutException("整体超时预算已耗尽");
        }
        return CompletableFuture
                .supplyAsync(supplier, timeoutPool)
                .get(Math.min(remainingMs, redisTimeoutMs), TimeUnit.MILLISECONDS);
    }

    /** 兜底广告出口：Redis 优先，失败回退静态常量。hasAd=1、fallback=true、不计二价 */
    private BidRequest.Result buildFallback(String requestId) {
        // A/B 对照组：兜底关闭时返回真正的空广告，用于测量改造前的空广告率
        if (!fallbackEnabled) {
            BidRequest.Result empty = new BidRequest.Result();
            empty.setRequestId(requestId);
            empty.setHasAd(false);
            empty.setFallback(false);
            empty.setPriceMode(2);
            empty.setCandidateCnt(0);
            empty.setMatchCnt(0);
            empty.setTopCandidates(List.of());
            return empty;
        }
        Ad ad = loadFallbackAd();
        BidRequest.Result r = new BidRequest.Result();
        r.setRequestId(requestId);
        r.setHasAd(true);
        r.setFallback(true);
        r.setPriceMode(2);          // 2 = 兜底，区别于 0 一价 / 1 二价
        r.setPayPrice(null);        // 兜底不计费
        r.setCandidateCnt(0);
        r.setMatchCnt(0);
        r.setTopCandidates(List.of());

        BidRequest.AdInfo info = new BidRequest.AdInfo();
        info.setAdId(ad.getId());
        info.setAdvertiserId(ad.getAdvertiserId());
        info.setTitle(ad.getTitle());
        info.setCategoryCode(ad.getCategoryCode());
        info.setBid(ad.getBid());
        r.setWinner(info);
        return r;
    }

    /** 从 Redis 读兜底广告；缺失或解析失败则回退静态常量 */
    private Ad loadFallbackAd() {
        try {
            String s = redis.opsForValue().get(FALLBACK_REDIS_KEY);
            if (s != null && !s.isBlank()) {
                Map<String, Object> m = objectMapper.readValue(s, Map.class);
                Ad a = new Ad();
                a.setId(Long.parseLong(String.valueOf(m.get("adId"))));
                a.setAdvertiserId(Integer.parseInt(String.valueOf(m.get("advertiserId"))));
                a.setTitle(String.valueOf(m.get("title")));
                a.setCategoryCode(String.valueOf(m.get("categoryCode")));
                a.setBid(new BigDecimal(String.valueOf(m.get("bid"))));
                return a;
            }
        } catch (Exception e) {
            log.warn("读取 Redis 兜底广告失败，回退静态兜底: {}", e.getMessage());
        }
        return STATIC_FALLBACK;
    }

    /** 启动时把兜底广告写入 Redis（best-effort，Redis 不在也可后续用静态常量） */
    private void seedFallbackAd() {
        try {
            redis.opsForValue().setIfAbsent(FALLBACK_REDIS_KEY, FALLBACK_AD_JSON);
        } catch (Exception e) {
            log.warn("兜底广告预热到 Redis 失败（将仅使用静态兜底）: {}", e.getMessage());
        }
    }

    private static Ad buildStaticFallback() {
        Ad a = new Ad();
        a.setId(999999L);
        a.setAdvertiserId(0);
        a.setTitle("兜底保底广告");
        a.setCategoryCode("mobile");
        a.setBid(BigDecimal.valueOf(0.01));
        return a;
    }

    private BidRequest.Result buildResult(String requestId, List<Ad> candidates,
                                          List<Ad> matched, Ad winner, BigDecimal payPrice) {
        BidRequest.Result r = new BidRequest.Result();
        r.setRequestId(requestId);
        r.setHasAd(winner != null);
        r.setFallback(false);
        r.setCandidateCnt(candidates.size());
        r.setMatchCnt(matched.size());
        // 1 = 二价（M4 引入：成交价=次高出价+最小加价，不超过自身出价）
        r.setPriceMode(1);
        r.setPayPrice(payPrice);

        if (winner != null) {
            BidRequest.AdInfo info = new BidRequest.AdInfo();
            info.setAdId(winner.getId());
            info.setAdvertiserId(winner.getAdvertiserId());
            info.setTitle(winner.getTitle());
            info.setCategoryCode(winner.getCategoryCode());
            info.setBid(winner.getBid());
            r.setWinner(info);
        }

        // 出价前 N 条，演示时展示「为什么是它赢」
        int topN = Math.min(10, matched.size());
        List<BidRequest.AdInfo> tops = new ArrayList<>(topN);
        for (int i = 0; i < topN; i++) {
            Ad a = matched.get(i);
            BidRequest.AdInfo info = new BidRequest.AdInfo();
            info.setAdId(a.getId());
            info.setAdvertiserId(a.getAdvertiserId());
            info.setTitle(a.getTitle());
            info.setCategoryCode(a.getCategoryCode());
            info.setBid(a.getBid());
            tops.add(info);
        }
        r.setTopCandidates(tops);

        // [已注释] Cost 分段耗时埋点（统计代码已禁用），result.cost 留空
        // BidRequest.Cost cost = new BidRequest.Cost();
        // cost.setTotal(costMs);
        // cost.setCandidate(candidateCost);
        // cost.setFilter(filterCost);
        // cost.setSort(sortCost);
        // cost.setBill(billCost);
        // r.setCost(cost);

        return r;
    }

    /*
     * ===== 以下为已禁用的统计代码（2026-10-06），恢复时取消注释 =====
     *
    private void persistLog(String requestId, BidRequest req,
                            int candidateCnt, int matchCnt,
                            Long adId, Integer advertiserId, Ad winner,
                            BigDecimal payPrice,
                            int costMs, int candidateCost,
                            int filterCost, int sortCost, int billCost) {
        try {
            BidLog entity = new BidLog();
            entity.setRequestId(requestId);
            entity.setAdId(adId);
            entity.setAdvertiserId(advertiserId);
            entity.setSlotCode(req.getSlotCode());
            entity.setRegion(req.getRegion());
            entity.setAge(req.getAge());
            entity.setGender(req.getGender());
            entity.setInterests(req.getInterests() != null ? req.getInterests() : "");
            entity.setCandidateCnt(candidateCnt);
            entity.setMatchCnt(matchCnt);
            entity.setHasAd(winner != null ? 1 : 0);
            entity.setWinBid(winner != null ? winner.getBid() : null);
            entity.setPayPrice(payPrice);
            entity.setCostMs(costMs);
            entity.setCandidateCost(candidateCost);
            entity.setFilterCost(filterCost);
            entity.setSortCost(sortCost);
            entity.setBillCost(billCost);
            entity.setBidAt(LocalDateTime.now());
            bidLogMapper.insert(entity);
        } catch (Exception e) {
            log.warn("写入竞价明细失败 requestId={}", requestId, e);
        }
    }

    private static int ms(long fromNanos, long toNanos) {
        return (int) ((toNanos - fromNanos) / 1_000_000);
    }
     * ===== 统计代码结束 =====
     */
}
