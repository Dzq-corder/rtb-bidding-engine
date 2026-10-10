package com.rtb.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * 竞价事件（M6 旁路投递到 Kafka 的消息体）。
 *
 * <p>每次 {@code /bid} 在 return 之前都会构造并发送一个 BidEvent，
 * 由三个独立消费组（计费对账 / 小时预聚合 / 反作弊频次）各自消费。
 *
 * <p>字段设计原则：只包含下游需要的事实，且 {@code eventId} 作为幂等键，
 * 保证重复投递下各消费组只生效一次。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BidEvent {

    /** 事件唯一 ID（UUID），幂等键 */
    private String eventId;

    /** 本次竞价请求 ID */
    private String requestId;

    /** 竞价发生时间戳（epoch ms） */
    private long ts;

    /** 地域编码 */
    private String region;

    /** 胜出广告 ID；兜底事件为兜底广告自身 id（如 999999），不会为 null */
    private Long adId;

    /** 广告主 ID；兜底广告为 0 */
    private Integer advertiserId;

    /** 最高出价（winner.bid）；兜底时为 null */
    private BigDecimal winBid;

    /** 二价实扣（payPrice）；兜底时为 null（不计费） */
    private BigDecimal payPrice;

    /** 是否为兜底广告（依赖故障/超时/预算耗尽时返回） */
    private boolean fallback;

    /** 是否返回了广告（始终 true，因兜底也返回广告） */
    private boolean hasAd;

    /** 计费方式：0 一价 / 1 二价 / 2 兜底 */
    private int priceMode;
}
