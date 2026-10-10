package com.rtb.entity;

import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 竞价明细。
 *
 * <p><b>可丢数据</b>：用于大盘分析的竞价明细。走本地有界队列 + 批量刷盘（M7 实现），
 * 队列满时允许丢弃低价值明细。
 *
 * <p><b>不可与 {@code billing_event} 混淆</b>：涉及金钱的计费事件在
 * {@code billing_event}，那类数据不可丢。「什么能丢、什么不能丢」是按
 * 业务后果划的，不是按性能划的。
 */
@Data
public class BidLog {

    private Long id;

    /** 竞价请求唯一 ID，同时是幂等键 */
    private String requestId;

    private Long adId;

    private Integer advertiserId;

    private String slotCode;

    // ---- 请求上下文（用户画像）

    private String region;

    private Integer age;

    private Integer gender;

    private String interests;

    // ---- 竞价结果

    /** 定向过滤前候选数，M1 = 检索返回行数 */
    private Integer candidateCnt;

    /** 定向过滤后数量，M1 无定向过滤，等于 candidateCnt；M2 起为真实求交结果 */
    private Integer matchCnt;

    private Integer hasAd;

    private BigDecimal winBid;

    /** 实际扣费。M1 一价（= winBid），M4 起二价 */
    private BigDecimal payPrice;

    // ---- 耗时分段（毫秒），用于定位瓶颈

    private Integer costMs;

    /** 候选检索耗时，M2 优化的核心指标 */
    private Integer candidateCost;

    private Integer filterCost;

    private Integer sortCost;

    private Integer billCost;

    /** 竞价时间。M8-3 按此字段做 RANGE 分区 */
    private LocalDateTime bidAt;
}
