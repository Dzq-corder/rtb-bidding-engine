package com.rtb.stat;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 小时级预聚合 Mapper · M6
 */
@Mapper
public interface HourlyPreaggMapper {

    /**
     * 写入/累加一个小时级汇总行。
     *
     * <p>主键是 {@code (stat_hour, slot_code, category_code)}，
     * 三个维度合起来唯一，所以同一小时的同一组合用累加语义。
     *
     * <p><b>为什么不用 REPLACE</b>：REPLACE 会真的删掉旧行再插新的，
     * 丢失「这条汇总历史上被更新过几次」的信息。
     * 而且 M8 第 4 轮要做增量更新，累加语义才好扩展。
     *
     * <p><b>返回值只用于统计</b>：Connector/J 默认返回 found_rows，
     * 1/2 的语义不可靠（见 BillingEventMapper 注释）。
     * 本方法不需要判重——累加逻辑在 SQL 里，数据库自己保证。
     *
     * @param totCostMs 累计耗时<b>之和</b>，不是平均值。
     *                  M8 第 4 轮修正：均值不可加，累加语义下会失真。
     *                  平均耗时由查询侧 {@code SUM(tot_cost_ms)/SUM(cnt)} 现算。
     * @param p99CostMs 近似 P99，本轮取窗口内最大值
     * @param totalPayPrice 成交总价<b>之和</b>，不是平均成交价（理由同上）
     */
    int upsert(@Param("statHour") LocalDateTime statHour,
               @Param("slotCode") String slotCode,
               @Param("categoryCode") String categoryCode,
               @Param("cnt") long cnt,
               @Param("adCnt") long adCnt,
               @Param("totCostMs") long totCostMs,
               @Param("p99CostMs") int p99CostMs,
               @Param("totalPayPrice") BigDecimal totalPayPrice,
               @Param("totalCost") BigDecimal totalCost);
}
