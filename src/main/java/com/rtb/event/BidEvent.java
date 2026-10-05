package com.rtb.event;

import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 竞价事件 · M6
 *
 * <p><b>为什么事件体里字段要「够用但不全」</b>
 *
 * <p>事件体设计有个常见错误：把整个请求对象序列化进去。
 * 那样任何字段变动都会导致消费端反序列化失败，
 * 而且每个消费组其实只关心自己那几个字段。
 *
 * <p>本事件体是<b>三个消费组的字段并集</b>——
 * 计费组要钱，频次组要 userId，预聚合组要广告位/类目/耗时。
 * 不放无用的字段（如 interests、age），
 * 因为它们既不参与计费也不参与聚合，写进去只是白占带宽。
 *
 * <p><b>为什么不定义成「点击/曝光」多类型事件</b>：
 * 简化成单一类型便于演示，而类型字段留着扩展位。
 * 真实系统里事件类型是必需的，但本轮的重点是
 * <b>旁路解耦与幂等</b>，不是事件建模的完备性。
 */
@Data
public class BidEvent {

    /**
     * 事件唯一 ID —— <b>幂等的唯一依据</b>
     *
     * <p>设计要求：
     * <ol>
     *   <li><b>全局唯一</b>——靠 UUID 而不是自增 ID。
     *       自增 ID 需要先查库才知道自己是第几条，重投时会产生新 ID，幂等失效</li>
     *   <li><b>生产者生成</b>——不是消费者生成的。
     *       消费者生成的话，重投时会生成新 ID，幂等直接失效</li>
     *   <li><b>与 request_id 不同</b>——request_id 是业务链路标识，
     *       可能因降级路径被复用；event_id 是一次投递的标识</li>
     * </ol>
     */
    private String eventId;

    /** 关联同步链路的 request_id，对账时用它回溯同步侧写了什么 */
    private String requestId;

    /** <b>Kafka 分区键</b>：同一广告的事件进同一分区，保证曝光/点击有序 */
    private Long adId;

    private Integer advertiserId;
    private String slotCode;
    private String categoryCode;
    private String region;

    private BigDecimal bidPrice;
    private BigDecimal payPrice;
    private BigDecimal secondPrice;

    /** 0 = 一价（兜底路径），1 = 二价 */
    private int isSecondPrice;

    private int costMs;
    private int candidateCnt;
    private int matchCnt;
    private boolean hasAd;

    /** 频次反作弊用；本项目用 requestId 派生，真实系统应是真实用户 ID */
    private String userId;

    private LocalDateTime bidAt;

    /**
     * 事件类型 · 扩展位
     *
     * <p>本轮只用 {@code BID}，预留 {@code CLICK} / {@code IMPRESSION}。
     * <b>提前留这个字段不是为了优雅，是为了演示「加类型不改表不改结构」</b>——
     * 消费端按类型分派，后续加点击事件不用动消息结构。
     */
    private String eventType = "BID";
}
