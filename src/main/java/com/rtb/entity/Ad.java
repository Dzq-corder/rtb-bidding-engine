package com.rtb.entity;

import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 广告实体。
 *
 * <p>M1 阶段的检索方式：{@code WHERE status = 1 AND category_code = ?}，
 * <b>不建任何索引</b>，因此走全表扫描（EXPLAIN 应为 type=ALL、rows≈49万）。
 * 这是 M1 baseline 的前提，M2 才引入倒排索引。
 */
@Data
public class Ad {

    private Long id;

    /** 广告主 ID */
    private Integer advertiserId;

    /** 创意标题 */
    private String title;

    private String creativeUrl;

    /** 行业类目，Zipf 分布 */
    private String categoryCode;

    /** 出价，单位元。对数正态长尾 */
    private BigDecimal bid;

    /** 日预算，离散档位 */
    private BigDecimal dailyBudget;

    /** 1 在投 / 2 暂停 / 3 已耗尽 */
    private Integer status;

    private LocalDateTime createdAt;

    private LocalDateTime updatedAt;
}
