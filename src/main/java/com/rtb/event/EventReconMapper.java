package com.rtb.event;

import lombok.Data;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * 旁路对账 Mapper · M6
 */
@Mapper
public interface EventReconMapper {

    /**
     * 同步表 vs 消费表对账。
     *
     * <p><b>为什么按 event_id 逐条比对而不是只比总额</b>：
     * 只比总额的话，「A 少扣 5 元、B 多扣 5 元」会互相抵消，
     * 总额相等但明细是错的。按event_id 比对才能发现这种情况。
     *
     * <p><b>差异有两个方向，必须分开查</b>：
     * <ul>
     *   <li><b>少</b>：同步表有、消费表没有 → 事件没投出去/没消费到。
     *       由主查询的 LEFT JOIN 检出（diffCount）。</li>
     *   <li><b>多</b>：消费表有、同步表没有 → 有事件没对应的扣费事实。
     *       <b>LEFT JOIN 检不出来</b>——它以同步表为基准，
     *       消费表多出来的行根本不参与运算。
     *       所以额外用一个 NOT EXISTS 子查询单独统计（extraCount）。</li>
     * </ul>
     *
     * <p>M6 第一版只写了 LEFT JOIN，注释却声称「两个方向都能发现」。
     * 实际演示时如果只看 diffCount=0 就下结论，
     * 会漏掉「凭空多出事件」这种更严重的问题。
     */
    @org.apache.ibatis.annotations.Select("""
            SELECT
              COUNT(b.event_id)                                        AS syncRows,
              COUNT(c.event_id)                                        AS consumeRows,
              COALESCE(ROUND(SUM(b.pay_price), 2), 0)                 AS syncAmount,
              COALESCE(ROUND(SUM(c.pay_price), 2), 0)                 AS consumeAmount,
              COALESCE(ROUND(SUM(CASE
                    WHEN c.event_id IS NULL THEN b.pay_price
                    WHEN ABS(b.pay_price - c.pay_price) > 0.001 THEN b.pay_price - c.pay_price
                    ELSE 0 END), 2), 0)                              AS diffAmount,
              COALESCE(SUM(CASE
                    WHEN c.event_id IS NULL OR ABS(b.pay_price - c.pay_price) > 0.001
                    THEN 1 ELSE 0 END), 0)                           AS diffCount,
              (SELECT COUNT(*) FROM billing_event_consumer x
                WHERE x.consumer_group = 'billing-recon'
                  AND x.consumed_at >= #{since}
                  AND NOT EXISTS (SELECT 1 FROM billing_event bb
                                  WHERE bb.event_id = x.event_id))     AS extraCount
            FROM billing_event b
            LEFT JOIN billing_event_consumer c
                   ON c.event_id = b.event_id
                  AND c.consumer_group = 'billing-recon'
            WHERE b.billed_at >= #{since}
            """)
    ReconResult recon(@Param("since") String since);

    /** 审计统计：看幂等到底拦下过多少 */
    @org.apache.ibatis.annotations.Select("""
            SELECT consumer_group AS groupName, action, COUNT(*) AS cnt
            FROM consumer_audit
            GROUP BY consumer_group, action
            """)
    List<Map<String, Object>> auditByGroupAction();

    /**
     * 找出重复的 event_id —— <b>必须返回空</b>。
     *
     * <p>这张表有唯一键，理论上不可能出现重复。
     * 这个查询是<b>反向验证</b>：如果它返回了非空，
     * 说明唯一键没建对——这是最可靠的幂等检查方式，
     * 因为它检查的是数据本身而不是代码逻辑。
     */
    @org.apache.ibatis.annotations.Select("""
            SELECT event_id, COUNT(*) AS cnt
            FROM billing_event_consumer
            GROUP BY event_id
            HAVING cnt > 1
            LIMIT 20
            """)
    List<Map<String, Object>> duplicateEventIds();

    @Data
    class ReconResult {
        private long syncRows;
        private long consumeRows;
        private BigDecimal syncAmount;
        private BigDecimal consumeAmount;
        private long diffCount;
        private BigDecimal diffAmount;
        /** 消费表有、同步表没有的条数，必须为 0 */
        private long extraCount;
    }
}
