package com.rtb.dto;

import lombok.Data;

import java.util.List;

/**
 * 竞价请求 —— 流量方在用户打开 App 时发来的一次竞价。
 */
@Data
public class BidRequest {

    /** 广告位编码 */
    private String slotCode;

    /** 地域编码，如 110000 */
    private String region;

    /**
     * 年龄段，映射到 ad_target.dim_type=2 的取值。
     * 传原始年龄（如 28）由服务端换算成 25-30，避免调用方各写一套映射。
     */
    private Integer age;

    /** 1 男 2 女 0 未知 */
    private Integer gender;

    /** 年龄段，直接传 ad_target 的原值时优先于此字段 */
    private String ageBucket;

    /** 兴趣标签，逗号分隔 */
    private String interests;

    /** 广告偏好类目。M1 用它做粗筛，M2 起定向条件走倒排求交 */
    private String categoryCode;

    /**
     * 竞价结果详情，演示时展示给用户看。
     * M9 才会被前端真正用起来，M1 先在接口返回里带着，便于冒烟时肉眼核对。
     */
    @Data
    public static class Result {
        private String requestId;
        /** 是否返回了广告 */
        private Boolean hasAd;
        /** 是否为兜底广告（M5 引入）：true=依赖故障/超时/预算耗尽时返回的保底广告，不计费 */
        private Boolean fallback;
        /** 胜出广告 */
        private AdInfo winner;
        /** 候选集大小，检索返回行数 */
        private Integer candidateCnt;
        /** 定向过滤后数量 */
        private Integer matchCnt;
        /** 出价降序后的前 N 条，M1 为 10 */
        private List<AdInfo> topCandidates;
        /** 计费方式：0 一价 1 二价 */
        private Integer priceMode;
        /** 实际成交价（元）。二价下低于胜出者出价，演示时与 winner.bid 对照 */
        private java.math.BigDecimal payPrice;
        private Cost cost;
    }

    @Data
    public static class AdInfo {
        private Long adId;
        private Integer advertiserId;
        private String title;
        private String categoryCode;
        private java.math.BigDecimal bid;
    }

    /** 耗时分段，M2 优化的核心指标是 candidateCost */
    @Data
    public static class Cost {
        private Integer total;
        /** 候选检索耗时 */
        private Integer candidate;
        /** 定向过滤耗时 */
        private Integer filter;
        /** 排序耗时 */
        private Integer sort;
        /** 计费耗时 */
        private Integer bill;
    }
}
