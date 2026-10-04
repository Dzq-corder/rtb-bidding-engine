package com.rtb.bid;

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

    /** 用户年龄 */
    private Integer age;

    /** 1 男 2 女 0 未知 */
    private Integer gender;

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
