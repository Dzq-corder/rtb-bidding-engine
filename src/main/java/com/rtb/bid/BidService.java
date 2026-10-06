package com.rtb.bid;

import com.rtb.ad.Ad;
import com.rtb.ad.AdMapper;
import com.rtb.budget.BudgetService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 竞价主链路 · M1 朴素实现
 *
 * <p>链路：候选检索 → 定向过滤 → 出价排序 → 计费 → 落库
 *
 * <p><b>本轮刻意保留的「未优化」特征</b>（它们是 M2~M7 的靶子）：
 * <ul>
 *   <li>候选检索走 {@code WHERE status=1 AND category_code=?}，
 *       <b>不建索引</b>，因此全表扫描。M2 用倒排索引替代</li>
 *   <li><b>无定向匹配能力</b>。地域/年龄/性别/兴趣的匹配本身就是 M2 的交付物
 *       （倒排多条件求交），M1 不做。这不属于「砍业务逻辑」——
 *       倒排是<b>替代方案</b>，不是业务规则本身</li>
 *   <li>出价排序已下推到 SQL（findCandidates 末尾 {@code ORDER BY a.bid DESC}），应用层不再排序</li>
 *   <li><b>一价</b>：按最高出价扣费。二价是 M4 的交付物</li>
 *   <li>预算扣减是「读余额 → 校验 → 扣减」三步，非原子。M3 改Lua</li>
 *   <li>日志<b>同步逐条 INSERT</b>。M7 改有界队列 + 批量刷盘</li>
 * </ul>
 *
 * <p>注意：M1<b>没有</b>超时熔断、<b>没有</b>本地缓存、<b>没有</b>走MQ。
 * 这三样分别是 M5/M3/M6 的交付物。
 *
 * <p>代码风格要求：干净、命名清晰、异常处理完整。
 * baseline 靠「未优化」成立，不靠「写得烂」成立。
 */
@Slf4j
@Service
public class BidService {

    private final AdMapper adMapper;
    private final BidLogMapper bidLogMapper;
    private final BudgetService budgetService;

    public BidService(AdMapper adMapper,
                      BidLogMapper bidLogMapper,
                      BudgetService budgetService) {
        this.adMapper = adMapper;
        this.bidLogMapper = bidLogMapper;
        this.budgetService = budgetService;
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
        // ① 候选检索：ad 全表扫 + ad_target 多维 JOIN
        //
        // ad 表刻意不建索引（承担 baseline 的「慢」），
        // ad_target 建 idx_dim_lookup（承担「筛得准」）。
        // M2 会把整个求交挪到内存 BitSet，对比就来自这一处。
        // ---------------------------------------------------------
        long tCand = System.nanoTime();
        List<Ad> candidates = adMapper.findCandidates(
                req.getRegion(),
                TargetingMapper.resolveAgeBucket(req.getAge(), req.getAgeBucket()),
                TargetingMapper.toGender(req.getGender()),
                req.getCategoryCode());
        long tCandEnd = System.nanoTime();
        int candidateCost = ms(tCand, tCandEnd);

        // ---------------------------------------------------------
        // ② 定向过滤
        // ---------------------------------------------------------
        // M1 的定向匹配已经在候选检索的 SQL JOIN 里做完了（体现为 candidateCnt）。
        // 这一段在 M1 里是应用层拷贝，M2 起倒排求交后两者的边界会重新划分。
        long tFilter = System.nanoTime();
        List<Ad> matched = new ArrayList<>(candidates);
        long tFilterEnd = System.nanoTime();
        int filterCost = ms(tFilter, tFilterEnd);

        // ---------------------------------------------------------
        // ③ 出价排序：已下推到 SQL（findCandidates 末尾 ORDER BY a.bid DESC）
        //    应用层不再二次排序，matched 顺序即 DB 返回顺序（出价降序），
        //    fall-through 扣预算时保证从最高出价往下找。
        // ---------------------------------------------------------
        int sortCost = 0;

        // ---------------------------------------------------------
        // ④ 计费：一价，按最高出价扣
        // ---------------------------------------------------------
        long tBill = System.nanoTime();
        Ad winner = null;
        BigDecimal payPrice = null;
        Integer advertiserId = null;
        Long adId = null;

        // 一价竞价 + fall-through：沿排序后的候选列表（按出价降序）依次尝试扣预算，
        // 命中第一个预算充足的广告即成交。第一名预算耗尽不再"直接空"，
        // 而是继续往后找——真实竞价应"尽力成交"。
        // 失败的 deduct 不会改写余额（校验不通过即返回 false，无 decrement），
        // 因此多试几个候选是安全的。
        if (!matched.isEmpty()) {
            for (Ad candidate : matched) {
                // 一价：出多少扣多少
                boolean deducted = budgetService.deduct(candidate.getId(), candidate.getDailyBudget(), candidate.getBid());
                if (deducted) {
                    winner = candidate;
                    payPrice = candidate.getBid();
                    advertiserId = candidate.getAdvertiserId();
                    adId = candidate.getId();
                    break;
                }
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
            // interests 在请求里可选；为 null 时落空串，命中 schema 的 DEFAULT '' 语义，避免 NOT NULL 报错
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
