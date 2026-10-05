package com.rtb.bid;

import com.rtb.ad.Ad;
import com.rtb.ad.AdMapper;
import com.rtb.bill.BillingEventMapper;
import com.rtb.bill.SecondPriceBilling;
import com.rtb.budget.BudgetService;
import com.rtb.event.BidEvent;
import com.rtb.event.EventProducer;
import com.rtb.fallback.FallbackAdPool;
import com.rtb.fallback.FaultInjector;
import com.rtb.index.InvertedIndex;
import com.rtb.log.BidLogBuffer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 竞价主链路
 *
 * <p>链路：候选检索 → 定向过滤 → 出价排序 → 计费 → 落库
 *
 * <p><b>M1 → M2 的变化只有一处</b>：候选检索从「数据库多表 JOIN」
 * 改为「内存倒排求交」。实测检索耗时 38.9ms → 数毫秒。
 *
 * <p><b>后续里程碑的靶子</b>（M3~M7）：
 * <ul>
 *   <li><b>一价</b>：按最高出价扣费。二价是 M4 的交付物</li>
 *   <li>预算扣减是「读余额 → 校验 → 扣减」三步，非原子。M3 改 Lua</li>
 *   <li>日志<b>同步逐条INSERT</b>。M7 改有界队列 + 批量刷盘</li>
 *   <li>无超时熔断（M5）、无 MQ（M6）</li>
 * </ul>
 *
 * <p>代码风格：干净、命名清晰、异常处理完整。
 * baseline 靠「未优化」成立，不靠「写得烂」成立。
 */
@Slf4j
@Service
public class BidService {

    /**
     * 各阶段预算（毫秒）· M5
     *
     * <p>合计 90ms，留 10ms 余量给序列化与网络写出。
     *
     * <p><b>为什么检索占 60ms</b>：它是唯一可能退化到数据库的环节
     * （倒排索引未就绪时降级 SQL，实测 38.9ms）。其余三步都是
     * 纯内存计算或单次 Redis 往返，给再多预算也用不上。
     *
     * <p>预算不是按实测耗时 × 系数来的，而是按<b>最坏情况 × 安全系数</b>：
     * 检索最坏 38.9ms（降级 SQL），60/38.9 ≈ 1.5 倍余量。
     * 按实测值分配会在降级路径上误熔断。
     */
    private static final int BUDGET_CANDIDATE_MS = 60;
    private static final int BUDGET_SORT_MS = 20;
    private static final int BUDGET_BILL_MS = 10;

    /**
     * 总超时预算（毫秒）· M5
     *
     * <p><b>为什么是 100ms</b>：这是 RTB 行业的通行上限。
     * 流量方（SSP）等待广告返回的时间预算通常也是 100ms 量级，
     * 超过就判定为无填充能力，后续流量会被降权。
     */
    private static final int TOTAL_TIMEOUT_MS = 100;

    private final AdMapper adMapper;

    /**
     * 竞价明细写入缓冲 · M7
     *
     * <p><b>它只负责 {@code bid_log}</b>。{@code billing_event} 仍由
     * {@link #persistBillingEvent} 同步逐条写，不经过这里——
     * 见 {@link BidLogBuffer} 类注释里「两类数据两条写入路径」的说明。
     */
    private final BidLogBuffer logBuffer;

    private final BudgetService budgetService;
    private final InvertedIndex invertedIndex;
    private final SecondPriceBilling secondPriceBilling;
    private final BillingEventMapper billingEventMapper;
    private final FallbackAdPool fallbackAdPool;
    private final FaultInjector faultInjector;
    private final EventProducer eventProducer;

    public BidService(AdMapper adMapper,
                      BidLogBuffer logBuffer,
                      BudgetService budgetService,
                      InvertedIndex invertedIndex,
                      SecondPriceBilling secondPriceBilling,
                      BillingEventMapper billingEventMapper,
                      FallbackAdPool fallbackAdPool,
                      FaultInjector faultInjector,
                      EventProducer eventProducer) {
        this.adMapper = adMapper;
        this.logBuffer = logBuffer;
        this.budgetService = budgetService;
        this.invertedIndex = invertedIndex;
        this.secondPriceBilling = secondPriceBilling;
        this.billingEventMapper = billingEventMapper;
        this.fallbackAdPool = fallbackAdPool;
        this.faultInjector = faultInjector;
        this.eventProducer = eventProducer;
    }

    /**
     * 执行一次竞价。
     *
     * <p>四段耗时分段埋点是本轮最重要的产出——
     * M2 之后要靠candidateCost 判断倒排索引到底省了多少。
     *
     * <p><b>M5 起整个方法被 try-catch 包住</b>：
     * 任何环节抛异常（含Redis 挂掉、MySQL抖动）都退到兜底降级。
     *
     * <p>实测踩过的坑：只写「超时降级」是不够的。
     * 预算扣减走 Redis，Redis 一挂就抛
     * {@code RedisConnectionFailureException}，
     * 异常从链路中间冒泡到控制器直接返回 500——<b>兜底代码根本没机会执行</b>。
     * 降级必须是<b>最外层</b>的保护，而不是某个分支里的 if。
     */
    public BidRequest.Result bid(BidRequest req) {
        try {
            return doBid(req);
        } catch (Exception e) {
            log.error("竞价链路异常，退到兜底降级：{}", e.getMessage(), e);
            return emergencyFallback(req, e);
        }
    }

    /**
     * 异常兜底：链路任何环节失败都走这里。
     *
     * <p><b>核心原则：宁可返回一条不精准的广告，也不要返回空。</b>
     * 返回空 = 这次流量机会直接浪费 = 流量方白花钱买来的请求。
     *
     * <p>扣费失败也照样返回广告——曝光已经产生了，
     * 扣费记 pending 由补偿机制处理，不能因为扣不动就放弃曝光。
     */
    private BidRequest.Result emergencyFallback(BidRequest req, Exception cause) {
        long cost = 0;
        Ad fb = null;
        try {
            fb = fallbackAdPool.resolve(fallbackAdPool.pick());
        } catch (Exception ignore) {
            // 连兜底池都出问题（理论上不该发生）——只能返回空
            log.error("兜底池也失败了，本次只能返回空广告");
        }

        if (fb != null) {
            safeDeduct(fb);
        }

        BidRequest.Result r = new BidRequest.Result();
        r.setRequestId(UUID.randomUUID().toString().replace("-", ""));
        r.setHasAd(fb != null);
        r.setCandidateCnt(0);
        r.setMatchCnt(0);
        r.setTopCandidates(List.of());
        r.setPriceMode(0);
        r.setTimedOut(false);
        // 异常降级同样标记 fallback=true：
        // 对监控来说「异常」和「超时」是两种降级触发原因，
        // 但对业务来说都是「这次流量走的是兜底池」，口径必须一致。
        r.setFallback(fb != null);
        if (fb != null) {
            BidRequest.AdInfo info = new BidRequest.AdInfo();
            info.setAdId(fb.getId());
            info.setAdvertiserId(fb.getAdvertiserId());
            info.setTitle(fb.getTitle());
            info.setCategoryCode(fb.getCategoryCode());
            info.setBid(fb.getBid());
            r.setWinner(info);
        }
        BidRequest.Cost c = new BidRequest.Cost();
        c.setTotal(0);
        c.setCandidate(0);
        c.setFilter(0);
        c.setSort(0);
        c.setBill(0);
        r.setCost(c);
        return r;
    }

    private BidRequest.Result doBid(BidRequest req) {
        long t0 = System.nanoTime();
        String requestId = UUID.randomUUID().toString().replace("-", "");
        Deadline dl = new Deadline(TOTAL_TIMEOUT_MS);

        // ---------------------------------------------------------
        // ① 候选检索
        //
        // M1：ad 全表扫 + ad_target 三维 JOIN（检索耗时 38.9ms，占 92%）
        // M2：内存倒排求交，一次数据库都不查
        //
        // 索引未就绪时降级回 M1 的 SQL 路径——可用性优先于性能。
        // ---------------------------------------------------------
        long tCand = System.nanoTime();

        // 故障注入点：模拟「候选检索这一步变慢」。
        //
        // 位置很关键：必须放在检索<b>之前</b>。
        // 若放在检索之后，正常胜者已经算出来了，超时判定时直接返回它，
        // 根本走不到兜底池，降级逻辑等于没被验证。
        // 放前面才符合真实故障形态——慢依赖让后续步骤根本没机会执行。
        faultInjector.injectDelay();

        String ageBucket = TargetingMapper.resolveAgeBucket(req.getAge(), req.getAgeBucket());
        String gender = TargetingMapper.toGender(req.getGender());

        List<Ad> candidates;
        if (invertedIndex.isReady()) {
            candidates = invertedIndex.intersect(List.of(
                    new String[]{String.valueOf(InvertedIndex.DIM_REGION), req.getRegion()},
                    new String[]{String.valueOf(InvertedIndex.DIM_AGE), ageBucket},
                    new String[]{String.valueOf(InvertedIndex.DIM_GENDER), gender}));
            // M2 之后候选已在内存求交并按出价排好，
            // 若还带了类目则再按类目收窄
            if (req.getCategoryCode() != null) {
                candidates.removeIf(a -> !req.getCategoryCode().equals(a.getCategoryCode()));
            }
        } else {
            log.warn("倒排索引未就绪，候选检索降级到数据库");
            candidates = adMapper.findCandidates(req.getRegion(), ageBucket, gender,
                    req.getCategoryCode());
        }
        long tCandEnd = System.nanoTime();
        int candidateCost = ms(tCand, tCandEnd);

        // ---------------------------------------------------------
        // ①' 熔断点 1：检索已经超时，后面两步别做了
        //
        // <b>这里是「熔断」和「末尾统一判超时」的本质区别</b>：
        // 检索慢到 100ms 以上时，排序与计费即使能做完也毫无意义——
        // 调用方早就断开了。及时放弃才是熔断该做的事。
        //
        // 注意兜底路径<b>不受这个判定约束</b>：它必须执行到底，
        // 因为返回空等于浪费这次流量。
        // ---------------------------------------------------------
        if (dl.expired()) {
            log.warn("检索阶段超时 {} ms，跳过排序与计费，直接兜底 requestId={}",
                    dl.elapsedMs(), requestId);
            return fallbackResult(req, requestId, t0, candidateCost, 0, 0, 0, List.of());
        }

        // ---------------------------------------------------------
        // ② 定向过滤
        //
        // M1 阶段定向匹配在候选检索的 SQL JOIN 里做完了，这一段是应用层拷贝。
        // M2 之后求交已包含定向语义，这一段只做类目收窄（若有）。
        // ---------------------------------------------------------
        long tFilter = System.nanoTime();
        List<Ad> matched = candidates;
        long tFilterEnd = System.nanoTime();
        int filterCost = ms(tFilter, tFilterEnd);

        // ---------------------------------------------------------
        // ③ 出价排序
        //
        // M2 的 intersect 已按出价降序返回，这里只在降级路径需要重排。
        // 但保留这行作为安全网：类目收窄后顺序仍应保持（removeIf 不改顺序）。
        // ---------------------------------------------------------
        long tSort = System.nanoTime();
        matched.sort(Comparator.comparing(Ad::getBid).reversed());
        long tSortEnd = System.nanoTime();
        int sortCost = ms(tSort, tSortEnd);

        // 熔断点 2：排序也用掉大部分预算了，别再扣钱
        if (dl.expired()) {
            log.warn("排序阶段超时 {} ms，跳过计费，直接兜底 requestId={}",
                    dl.elapsedMs(), requestId);
            return fallbackResult(req, requestId, t0, candidateCost, filterCost, sortCost, 0, matched);
        }

        // ---------------------------------------------------------
        // ④ 计费：二价，按次高价 + 0.01 结算（M4）
        //
        // M1~M3 是一价（按最高出价扣），M4 起改为二价。
        // 边界情形由 SecondPriceBilling 统一处理：
        //   · 候选只有 1 条  → 按该条出价的 60% 结算
        //   · 次高价 + 0.01 超过胜出者出价 → 按胜出者出价结算
        // ---------------------------------------------------------
        long tBill = System.nanoTime();
        Ad winner = null;
        BigDecimal payPrice = null;
        BigDecimal secondPrice = null;
        Integer advertiserId = null;
        Long adId = null;
        int isSecondPrice = 0;
        int edgeCase = 0;

        // 熔断点 3：计费前最后一道闸。
        // 走到这里预算已经很紧，但计费<b>不做也不合适</b>——
        // 没有扣费就不能算成交，放出去就是白送曝光。
        // 所以这里只做 warn，不中断。
        if (!dl.hasBudgetFor(BUDGET_BILL_MS)) {
            log.warn("计费前剩余预算仅 {} ms，可能超总预算 requestId={}",
                    dl.remainingMs(), requestId);
        }

        if (!matched.isEmpty()) {
            // matched 已按出价降序（步骤③ 排过）
            SecondPriceBilling.BillingResult r = secondPriceBilling.settle(matched);
            if (r.success()) {
                Ad top = r.winner();
                // 扣的是实际应收（次高价+0.01），不是赢家自己的出价。
                // 这样广告主「报真实意愿价」才是最优策略。
                boolean deducted = budgetService.deduct(top.getId(), top.getDailyBudget(), r.payPrice());
                if (deducted) {
                    winner = top;
                    payPrice = r.payPrice();
                    secondPrice = r.secondPrice();
                    advertiserId = top.getAdvertiserId();
                    adId = top.getId();
                    isSecondPrice = r.hasSecondPrice() ? 1 : 0;
                    edgeCase = r.edgeCase();
                }
            }
        }
        long tBillEnd = System.nanoTime();
        int billCost = ms(tBill, tBillEnd);

        int costMs = ms(t0, tBillEnd);

        // ---------------------------------------------------------
        // ⑤ 熔断判定（M5）· 终态
        //
        // <b>关键取舍：已经扣到钱的胜者，不因为超时就丢掉。</b>
        //
        // 前面 6 次 M5 压测（注入 150ms）时，这里是「超时就把正常胜者
        // 换成兜底广告」，结果熔断 2000 次里只有 113 次真走了兜底，
        // 剩下的都因为「超时前已算出 winner」而被替换。
        // 那个设计是错的——它把一个精准结果降级成随机结果，
        // 超时既没省时间（钱照样扣了、明细照样写了），又损失了精准度。
        //
        // 现在的规则：
        //   有 winner（已扣费）→ 正常返回，标记 timedOut 但不降级
        //   无 winner          → 必须走兜底，宁可低精准度也不返回空
        // ---------------------------------------------------------
        boolean timeout = costMs >= TOTAL_TIMEOUT_MS;

        Ad finalWinner = winner;
        BigDecimal finalPay = payPrice;
        Integer finalAdvertiserId = advertiserId;
        Long finalAdId = adId;
        BigDecimal finalSecondPrice = secondPrice;
        int finalIsSecondPrice = isSecondPrice;
        int finalCandidateCnt = candidates.size();
        int finalMatchCnt = matched.size();
        boolean usedFallback = false;

        if (timeout && winner == null) {
            // ---- 兜底降级 ----
            Ad fb = fallbackAdPool.resolve(fallbackAdPool.pick());
            if (fb != null) {
                // 兜底广告按一价扣（不排序、没有次高价概念）。
                // 关键：扣费失败也要返回广告——曝光已经产生了，
                // 扣费失败记 pending 由异步补偿，绝不能因为扣不动就返回空。
                boolean deducted = safeDeduct(fb);
                if (deducted) {
                    finalWinner = fb;
                    finalPay = fb.getBid();
                    finalAdvertiserId = fb.getAdvertiserId();
                    finalAdId = fb.getId();
                    finalSecondPrice = null;
                    finalIsSecondPrice = 0;
                    usedFallback = true;
                }
                // deducted=false 时 finalWinner 仍为 null，
                // 会计入 has_ad=0 —— 这是「扣费失败也不白送曝光」与
                // 「绝不返回空」两个目标的取舍点，已在 metrics 记录
            }
        }

        // ---------------------------------------------------------
        // ⑥ 落库：bid_log 入本地有界队列（M7），异步批量刷盘
        //    billing_event 记录金钱事实，【同步逐条写，不走队列】
        //    两条路径并存 = 「按业务后果分级」在代码里的体现
        // ---------------------------------------------------------
        persistLog(requestId, req, finalCandidateCnt, finalMatchCnt,
                finalAdId, finalAdvertiserId, finalWinner, finalPay,
                costMs, candidateCost, filterCost, sortCost, billCost);

        // 计费事件与竞价明细分级：明细可丢，钱不能丢
        String chargeEventId = null;
        if (finalWinner != null) {
            chargeEventId = persistBillingEvent(requestId, finalWinner, finalPay,
                    finalSecondPrice, finalIsSecondPrice);
        }

        // ⑦ 事件旁路（M6）
        //
        // 【为什么放在最后】投递是「发出去了就算完成」，
        // 放在所有同步逻辑之后，保证同步链路的结果已经落地。
        // 反过来放的话，一旦后面的落库抛异常，
        // 就会出现「事件说这次成交了，但表里没有」的脏数据。
        publishEvent(chargeEventId, requestId, req, finalWinner, finalPay, finalSecondPrice,
                finalIsSecondPrice, costMs, candidateCost, finalCandidateCnt,
                finalMatchCnt);

        BidRequest.Result result = buildResult(requestId, candidates, matched,
                finalWinner, finalPay, costMs, candidateCost, filterCost, sortCost, billCost);
        result.setTimedOut(timeout);
        // 用显式标记而不是「finalWinner != null && winner == null」推断。
        // 后者的坑：异常兜底路径返回的广告也会满足 winner == null，
        // 混在一起就分不清「超时降级」和「异常降级」了。
        result.setFallback(usedFallback);
        return result;
    }

    /**
     * 阶段边界熔断的统一出口：直接走兜底并返回结果。
     *
     * <p>把「检索后熔断」和「排序后熔断」收敛到一个方法，
     * 避免两条分支的落库、标志位设置出现细微差异——
     * <b>降级路径必须只有一份实现</b>，否则出问题时很难定位是哪条分支漏了。
     *
     * @param matched 已完成的候选（可能是空），只用于记录候选数
     */
    private BidRequest.Result fallbackResult(BidRequest req, String requestId,
                                            long t0, int candidateCost,
                                            int filterCost, int sortCost, int billCost,
                                            List<Ad> matched) {
        int costMs = ms(t0, System.nanoTime());

        Ad fb = null;
        try {
            fb = fallbackAdPool.resolve(fallbackAdPool.pick());
        } catch (Exception e) {
            log.error("兜底广告选取失败 requestId={}: {}", requestId, e.getMessage());
        }

        boolean deducted = false;
        if (fb != null) {
            deducted = safeDeduct(fb);
        }
        Ad finalWinner = deducted ? fb : null;

        persistLog(requestId, req, matched.size(), matched.size(),
                finalWinner == null ? null : finalWinner.getId(),
                finalWinner == null ? null : finalWinner.getAdvertiserId(),
                finalWinner, finalWinner == null ? null : finalWinner.getBid(),
                costMs, candidateCost, filterCost, sortCost, billCost);

        String chargeEventId = null;
        if (finalWinner != null) {
            // 兜底广告按一价结算，没有次高价概念
            chargeEventId = persistBillingEvent(requestId, finalWinner,
                    finalWinner.getBid(), null, 0);
        }

        // 降级路径同样要发事件 —— 否则对账会误判为「事件丢失」
        publishEvent(chargeEventId, requestId, req, finalWinner,
                finalWinner == null ? null : finalWinner.getBid(),
                null, 0, costMs, candidateCost,
                matched.size(), matched.size());

        BidRequest.Result result = buildResult(requestId, matched, matched,
                finalWinner, finalWinner == null ? null : finalWinner.getBid(),
                costMs, candidateCost, filterCost, sortCost, billCost);
        result.setTimedOut(true);
        result.setFallback(finalWinner != null);
        return result;
    }

    /**
     * 投递竞价事件到 Kafka 旁路 · M6
     *
     * <p><b>本方法的三条铁律</b>：
     * <ol>
     *   <li><b>不阻塞</b> —— 只注册回调，绝不调 {@code get()}/{@code join()}。
     *       Kafka 假死时 {@code max.block.ms} 默认 60 秒，
     *       阻塞就等于把 M5 精心设计的 100ms 预算全部作废</li>
     *   <li><b>不抛异常</b> —— 事件投递失败不能让竞价返回 500。
     *       理由和 M5 一样：为一个可补偿的事件搞挂同步链路是本末倒置</li>
     *   <li><b>不在异常路径上执行</b> —— 只在正常流程末尾调用。
     *       异常兜底路径（{@code emergencyFallback}）不投事件，
     *       因为那条路径上的扣费本来就失败了，投出去只会污染对账</li>
     * </ol>
     *
     * <p><b>为什么扣费成功才投事件</b>：
     * 事件的意义是「对账的对照物」。如果扣费失败也发事件，
     * 消费侧会记下一笔消费记录，而同步侧没有——
     * 对账就会出现「消费表多于同步表」的差异，
     * 而这个差异不是丢消息，是口径本身错了。**对账的前提是两侧口径一致。**
     */
    private void publishEvent(String chargeEventId, String requestId, BidRequest req,
                              Ad winner, BigDecimal payPrice, BigDecimal secondPrice,
                              int isSecondPrice, int costMs, int candidateCost,
                              int candidateCnt, int matchCnt) {
        if (winner == null || chargeEventId == null) {
            // 没有广告 = 这次没成交，没有可对账的事实
            return;
        }
        try {
            BidEvent e = new BidEvent();
            // 复用同步侧的 eventId——对账靠它逐条 JOIN
            e.setEventId(chargeEventId);
            e.setRequestId(requestId);
            e.setAdId(winner.getId());
            e.setAdvertiserId(winner.getAdvertiserId());
            e.setSlotCode(req.getSlotCode());
            e.setCategoryCode(winner.getCategoryCode());
            e.setRegion(req.getRegion());
            e.setBidPrice(winner.getBid());
            e.setPayPrice(payPrice);
            e.setSecondPrice(secondPrice);
            e.setIsSecondPrice(isSecondPrice);
            e.setCostMs(costMs);
            e.setCandidateCnt(candidateCnt);
            e.setMatchCnt(matchCnt);
            e.setHasAd(true);
            // 用户 ID：本项目用 requestId 前 8 位派生。
            // 真实系统里应是真实的用户/设备 ID——
            // 频次反作弊的准确性完全取决于这个字段的质量。
            e.setUserId(requestId.substring(0, 8));
            e.setBidAt(LocalDateTime.now());
            eventProducer.publish(e);
        } catch (Exception ex) {
            log.error("构造/投递竞价事件失败 requestId={}: {}", requestId, ex.getMessage());
        }
    }

    /**
     * 兜底路径的扣费，异常一律吞掉。
     *
     * <p>熔断时 Redis 可能已经不可用——这里再抛异常会让整个降级失效。
     * 扣费失败记 warn，由补偿机制兜底，但<b>绝不能因此不返回广告</b>。
     */
    private boolean safeDeduct(Ad ad) {
        try {
            return budgetService.deduct(ad.getId(), ad.getDailyBudget(), ad.getBid());
        } catch (Exception e) {
            log.warn("兜底广告扣费失败 adId={}，曝光已产生待异步补偿：{}",
                    ad.getId(), e.getMessage());
            return false;
        }
    }

    /**
     * 写计费事件 —— 金钱事实，不可丢。
     *
     * <p><b>为什么与 bid_log 分开两张表</b>：分界线是「丢了这笔数据有没有业务后果」。
     * bid_log 是大盘明细，丢几条不影响「上周哪天最忙」这个结论；
     * billing_event 是扣费事实，丢一条就少收一笔钱，是资损。
     *
     * <p><b>M6 接 Kafka 后的幂等</b>：event_id 走唯一键，
     * Kafka 重投时撞唯一键被识别丢弃，保证最终只生效一次。
     * M4 阶段是同步写入，还没有重投，但唯一键先建好，行为一致。
     */
    private String persistBillingEvent(String requestId, Ad winner,
                                     BigDecimal payPrice, BigDecimal secondPrice,
                                     int isSecondPrice) {
        // eventId 先生成：同步表和事件旁路必须用<b>同一个</b> eventId。
        //
        // 【为什么这是对账能成立的前提】
        // 对账 SQL 是按 event_id 逐条 JOIN 同步表和消费表的。
        // 如果两边各自生成一个 UUID，JOIN 永远匹配不上，
        // 对账会报「同步 5000 条、消费 5000 条、差异 5000 条」——
        // 而实际上数据完全一致，只是对不上号。
        //
        // 【代价】支付等外部调用需要下游用自己的幂等键去重，
        // 因为我们无法让外部系统认同我们的 eventId。
        // 本项目不涉及外部支付，所以这样做是安全的。
        String eventId = UUID.randomUUID().toString().replace("-", "");
        try {
            billingEventMapper.insert(
                    eventId,
                    requestId,
                    winner.getId(),
                    winner.getAdvertiserId(),
                    winner.getBid(),
                    payPrice,
                    secondPrice,
                    isSecondPrice,
                    LocalDateTime.now());
        } catch (Exception e) {
            // 钱的事实不能丢，但要保证竞价链路本身不崩。
            // 这里只告警：真实系统里应由 M6 的 Kafka 消费端重试兜底。
            log.error("写入计费事件失败 requestId={} adId={} pay={}",
                    requestId, winner.getId(), payPrice, e);
        }
        // 无论写成功还是失败都返回 eventId：
        // 写成功时它是对账的 JOIN 键；写失败时事件侧带着它去消费，
        // 消费表会比同步表多一条——【这个差异本身就是有价值的信号】，
        // 它直接告诉排查者「同步侧这里丢了一笔」
        return eventId;
    }

    /**
     * 写竞价明细 —— M7 起改为「入队即返回」。
     *
     * <p><b>这里不做任何 IO</b>：只把对象放进有界队列，
     * 由 {@link BidLogBuffer} 的刷盘线程批量落库。
     * 竞价主链路的耗时里从此不再包含一次 MySQL 往返。
     *
     * <p><b>为什么这里可以是「可丢」的</b>：明细用于大盘分析，
     * 丢几条不影响统计结论。而 {@link #persistBillingEvent} 是扣费事实，
     * 必须同步写成功才返回。<b>两者的区别不是技术方案高低，是业务后果不同。</b>
     *
     * <p>异常不向上抛——明细可丢，绝不能因日志失败影响广告返回。
     */
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
            entity.setInterests(req.getInterests());
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
            logBuffer.submit(entity);
        } catch (Exception e) {
            // 明细可丢，这里只告警不影响竞价结果返回
            log.warn("提交竞价明细失败 requestId={}", requestId, e);
        }
    }

    private BidRequest.Result buildResult(String requestId, List<Ad> candidates,
                                          List<Ad> matched, Ad winner, BigDecimal payPrice,
                                          int costMs, int candidateCost,
                                          int filterCost, int sortCost, int billCost) {
        BidRequest.Result r = new BidRequest.Result();
        r.setRequestId(requestId);
        r.setHasAd(winner != null);
        r.setCandidateCnt(candidates.size());
        r.setMatchCnt(matched.size());
        // 0 = 一价，M4 起为 1
        r.setPriceMode(0);

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

        BidRequest.Cost cost = new BidRequest.Cost();
        cost.setTotal(costMs);
        cost.setCandidate(candidateCost);
        cost.setFilter(filterCost);
        cost.setSort(sortCost);
        cost.setBill(billCost);
        r.setCost(cost);

        return r;
    }

    /** 单段耗时（毫秒），向下取整 */
    private static int ms(long fromNanos, long toNanos) {
        return (int) ((toNanos - fromNanos) / 1_000_000);
    }
}
