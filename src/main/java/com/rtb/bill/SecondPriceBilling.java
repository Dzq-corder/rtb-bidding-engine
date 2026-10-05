package com.rtb.bill;

import com.rtb.ad.Ad;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

/**
 * 二价拍卖计费 · M4
 *
 * <p><b>规则</b>：胜出者按「次高价 + 最小加价单位」结算，不是按自己的出价。
 *
 * <p><b>为什么二价是行业标准</b>
 *
 * <p>一价下广告主必须猜别人的出价，出高了怕浪费、出低了怕拿不到展示，
 * 于是普遍压价，导致「出价不反映真实意愿」。
 *
 * <p>二价下，广告主只要报出真实愿意付的最高价即可——
 * 因为反正只按第二名加一美分结算，报高一点不额外花钱，
 * 报低了反而可能拿不到展示。<b>「报真实价」成为最优策略</b>，
 * 出价数据才有分析价值，平台才能积累出真实的流量价格参考。
 *
 * <p><b>理论 vs 实践的分歧</b>
 *
 * <p>理论上应该只比第二名多 0.01 元（英式拍卖）。但实践中很多平台
 * 直接按次高价成交，原因是：
 * <ul>
 *   <li>最小加价单位需要按广告位定价策略配置，不宜写死</li>
 *   <li>直接按次高价成交能减少「因为差一分钱导致落败」的怨气</li>
 *   <li>结算金额与出价脱钩后，对账口径更简单——不必区分是否踩了加价线</li>
 * </ul>
 *
 * <p>本项目保留「次高价 + 0.01」的标准做法（更贴近教科书定义），
 * 同时把最小加价单位做成可配置项，而不是写死。
 */
@Component
public class SecondPriceBilling {

    /** 最小加价单位：0.01 元。这是行业惯例，用「分」的精度结算 */
    private static final BigDecimal MIN_INCREMENT = new BigDecimal("0.01");

    /**
     * 按二价规则计算应付金额。
     *
     * @param sortedCandidates 已按出价降序排好的候选，index 0 是最高价
     * @return 结算结果；候选为空时返回 {@link BillingResult#empty()}
     */
    public BillingResult settle(List<Ad> sortedCandidates) {
        if (sortedCandidates == null || sortedCandidates.isEmpty()) {
            return BillingResult.empty();
        }

        Ad winner = sortedCandidates.get(0);
        BigDecimal winBid = winner.getBid();

        // ---- 边界 1：只有一条候选 ------------------------------------
        // 没有第二名可参考，按「该条出价的一定比例」结算。
        //
        // 为什么不能按 100% 收：那会让最后一条候选的实际成本等于出价，
        // 广告主在这种局面下会主动压价（因为压价也能拿到展示）。
        // 收 60% 是一种折中：既保证平台有收入，也让「压价」不至于无风险。
        //
        // 为什么不能按 0 收：平台白跑一次竞价，流量机会被浪费。
        if (sortedCandidates.size() == 1) {
            BigDecimal pay = winBid.multiply(new BigDecimal("0.60"))
                    .setScale(2, RoundingMode.HALF_UP);
            // 至少收最小加价单位，避免 0.01 元出价收 0 元
            if (pay.compareTo(MIN_INCREMENT) < 0) {
                pay = MIN_INCREMENT;
            }
            return new BillingResult(winner, winBid, null, pay, true,
                    BillingResult.SECOND_PRICE_NONE);
        }

        // ---- 边界 2：次高价低于最小加价单位 ---------------------------
        // 例如出价 1.00、0.005。次高价 + 0.01 = 0.015，会超过胜出者出价。
        // 此时按胜出者出价结算，并把次高价抬到 0 以便追溯。
        BigDecimal secondBid = sortedCandidates.get(1).getBid();
        BigDecimal pay = secondBid.add(MIN_INCREMENT)
                .setScale(2, RoundingMode.HALF_UP);

        if (pay.compareTo(winBid) > 0) {
            // 次高价加完超过胜出者出价，说明两者价差过小，
            // 按胜出者出价收，不能收超过它出价的金额
            pay = winBid;
            secondBid = BigDecimal.ZERO.setScale(2);
        }

        return new BillingResult(winner, winBid, secondBid, pay, true, BillingResult.NORMAL);
    }

    /**
     * 结算结果。
     *
     * @param winner       胜出广告
     * @param winBid       胜出者出价
     * @param secondPrice  次高价；仅一条候选时为 null
     * @param payPrice     实际应收
     * @param isSecondPrice 是否按二价结算
     * @param edgeCase     命中的边界情形说明，正常为 0
     */
    public record BillingResult(Ad winner,
                                BigDecimal winBid,
                                BigDecimal secondPrice,
                                BigDecimal payPrice,
                                boolean success,
                                int edgeCase) {

        /** 正常二价结算 */
        public static final int NORMAL = 0;

        /** 候选只有一条，按出价的 60% 结算 */
        public static final int SECOND_PRICE_NONE = 1;

        public static BillingResult empty() {
            return new BillingResult(null, null, null, null, false, NORMAL);
        }

        public boolean hasSecondPrice() {
            return secondPrice != null;
        }
    }
}
