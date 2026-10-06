package com.rtb.bid;

import com.rtb.ad.Ad;
import com.rtb.ad.AdMapper;
import com.rtb.budget.BudgetService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * 竞价主链路 · M1 朴素实现
 *
 * <p>链路：候选检索 → 定向过滤 → 计费 → 返回
 *
 * <p><b>本轮刻意保留的「未优化」特征</b>（它们是 M2~M7 的靶子）：
 * <ul>
 *   <li>候选检索走 {@code WHERE status=1 AND category_code=?}，
 *       <b>不建索引</b>，因此全表扫描。M2 用倒排索引替代</li>
 *   <li><b>无定向匹配能力</b>。地域/年龄/性别/兴趣的匹配本身就是 M2 的交付物
 *       （倒排多条件求交），M1 不做。</li>
 *   <li>一价：按最高出价扣费。二价是 M4 的交付物</li>
 *   <li>预算扣减是「读余额 → 校验 → 扣减」三步，非原子。M3 改Lua</li>
 * </ul>
 *
 * <p>注意：M1<b>没有</b>超时熔断、<b>没有</b>本地缓存、<b>没有</b>走MQ。
 *
 * <p><b>统计代码已注释（2026-10-06）</b>：分段耗时埋点（candidateCost 等）
 * 与 bid_log 落库（persistLog）在本阶段全部禁用，/bid 只做纯服务化链路，
 * 压测数字不被落库与计时拖慢。恢复时取消对应注释即可。
 */
@Slf4j
@Service
public class BidService {

    private final AdMapper adMapper;
    private final BudgetService budgetService;

    public BidService(AdMapper adMapper,
                      BudgetService budgetService) {
        this.adMapper = adMapper;
        this.budgetService = budgetService;
    }

    /**
     * 执行一次竞价。
     */
    public BidRequest.Result bid(BidRequest req) {
        String requestId = UUID.randomUUID().toString().replace("-", "");

        // ---------------------------------------------------------
        // ① 候选检索：ad 全表扫 + ad_target 多维 JOIN
        //    ad 表刻意不建索引（承担 baseline 的「慢」），
        //    ad_target 建 idx_dim_lookup（承担「筛得准」）。M2 会把整个求交挪到内存 BitSet。
        // ---------------------------------------------------------
        List<Ad> candidates = adMapper.findCandidates(
                req.getRegion(),
                TargetingMapper.resolveAgeBucket(req.getAge(), req.getAgeBucket()),
                TargetingMapper.toGender(req.getGender()),
                req.getCategoryCode());

        // ---------------------------------------------------------
        // ② 定向过滤：M1 已在候选检索的 SQL JOIN 里完成，这里仅作拷贝占位
        // ---------------------------------------------------------
        List<Ad> matched = new ArrayList<>(candidates);

        // ---------------------------------------------------------
        // ③ 出价排序已下推到 SQL（findCandidates 末尾 ORDER BY a.bid DESC），
        //    应用层不再排序，matched 顺序即 DB 返回顺序（出价降序）。
        // ---------------------------------------------------------

        // ---------------------------------------------------------
        // ④ 计费：一价，沿出价降序 fall-through 找第一个预算充足的广告成交
        // ---------------------------------------------------------
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

        // [已注释] 分段计时埋点 + bid_log 落库（M1 baseline 压测期间不采集统计，纯服务化）
        // persistLog(requestId, req, candidates.size(), matched.size(),
        //         adId, advertiserId, winner, payPrice,
        //         costMs, candidateCost, filterCost, sortCost, billCost);

        return buildResult(requestId, candidates, matched, winner, payPrice);
    }

    private BidRequest.Result buildResult(String requestId, List<Ad> candidates,
                                          List<Ad> matched, Ad winner, BigDecimal payPrice) {
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
