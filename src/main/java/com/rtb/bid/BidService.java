package com.rtb.bid;

import com.rtb.ad.Ad;
import com.rtb.ad.AdMapper;
import com.rtb.bill.BillingEventMapper;
import com.rtb.bill.SecondPriceBilling;
import com.rtb.budget.BudgetService;
import com.rtb.index.InvertedIndex;
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

    private final AdMapper adMapper;
    private final BidLogMapper bidLogMapper;
    private final BudgetService budgetService;
    private final InvertedIndex invertedIndex;
    private final SecondPriceBilling secondPriceBilling;
    private final BillingEventMapper billingEventMapper;

    public BidService(AdMapper adMapper,
                      BidLogMapper bidLogMapper,
                      BudgetService budgetService,
                      InvertedIndex invertedIndex,
                      SecondPriceBilling secondPriceBilling,
                      BillingEventMapper billingEventMapper) {
        this.adMapper = adMapper;
        this.bidLogMapper = bidLogMapper;
        this.budgetService = budgetService;
        this.invertedIndex = invertedIndex;
        this.secondPriceBilling = secondPriceBilling;
        this.billingEventMapper = billingEventMapper;
    }

    /**
     * 执行一次竞价。
     *
     * <p>四段耗时分段埋点是本轮最重要的产出——
     * M2 之后要靠candidateCost 判断倒排索引到底省了多少。
     */
    public BidRequest.Result bid(BidRequest req) {
        long t0 = System.nanoTime();
        String requestId = UUID.randomUUID().toString().replace("-", "");

        // ---------------------------------------------------------
        // ① 候选检索
        //
        // M1：ad 全表扫 + ad_target 三维 JOIN（检索耗时 38.9ms，占 92%）
        // M2：内存倒排求交，一次数据库都不查
        //
        // 索引未就绪时降级回 M1 的 SQL 路径——可用性优先于性能。
        // ---------------------------------------------------------
        long tCand = System.nanoTime();

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
        // ⑤ 落库：bid_log 同步逐条INSERT（M7 改批量）
        //    billing_event 记录金钱事实，与 bid_log 分级
        // ---------------------------------------------------------
        persistLog(requestId, req, candidates.size(), matched.size(),
                adId, advertiserId, winner, payPrice,
                costMs, candidateCost, filterCost, sortCost, billCost);

        // 计费事件与竞价明细分级：明细可丢，钱不能丢
        if (winner != null) {
            persistBillingEvent(requestId, winner, payPrice, secondPrice, isSecondPrice);
        }

        return buildResult(requestId, candidates, matched, winner, payPrice,
                costMs, candidateCost, filterCost, sortCost, billCost);
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
    private void persistBillingEvent(String requestId, Ad winner,
                                     BigDecimal payPrice, BigDecimal secondPrice,
                                     int isSecondPrice) {
        try {
            billingEventMapper.insert(
                    UUID.randomUUID().toString().replace("-", ""),
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
    }

    /**
     * 写竞价明细。M1 同步逐条 INSERT，异常不向上抛——
     * 明细是可丢数据（M7 会显式定义丢弃策略），不能因日志失败影响竞价返回。
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
            bidLogMapper.insert(entity);
        } catch (Exception e) {
            // 明细可丢，这里只告警不影响竞价结果返回
            log.warn("写入竞价明细失败 requestId={}", requestId, e);
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
