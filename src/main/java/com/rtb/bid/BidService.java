package com.rtb.bid;

import com.rtb.ad.Ad;
import com.rtb.ad.AdMapper;
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

    public BidService(AdMapper adMapper,
                      BidLogMapper bidLogMapper,
                      BudgetService budgetService,
                      InvertedIndex invertedIndex) {
        this.adMapper = adMapper;
        this.bidLogMapper = bidLogMapper;
        this.budgetService = budgetService;
        this.invertedIndex = invertedIndex;
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
        // ④ 计费：一价，按最高出价扣
        // ---------------------------------------------------------
        long tBill = System.nanoTime();
        Ad winner = null;
        BigDecimal payPrice = null;
        Integer advertiserId = null;
        Long adId = null;

        if (!matched.isEmpty()) {
            Ad top = matched.get(0);
            // 一价：出多少扣多少
            boolean deducted = budgetService.deduct(top.getId(), top.getDailyBudget(), top.getBid());
            if (deducted) {
                winner = top;
                payPrice = top.getBid();
                advertiserId = top.getAdvertiserId();
                adId = top.getId();
            }
        }
        long tBillEnd = System.nanoTime();
        int billCost = ms(tBill, tBillEnd);

        int costMs = ms(t0, tBillEnd);

        // ---------------------------------------------------------
        // ⑤ 落库：同步逐条 INSERT（M7 改批量）
        // ---------------------------------------------------------
        persistLog(requestId, req, candidates.size(), matched.size(),
                adId, advertiserId, winner, payPrice,
                costMs, candidateCost, filterCost, sortCost, billCost);

        return buildResult(requestId, candidates, matched, winner, payPrice,
                costMs, candidateCost, filterCost, sortCost, billCost);
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
