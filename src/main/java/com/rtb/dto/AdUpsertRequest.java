package com.rtb.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.Data;

import java.math.BigDecimal;
import java.util.List;

/**
 * 广告新建 / 更新入参。
 *
 * <p><b>为什么定向条件要一并放在这里</b>：{@code ad_target} 是 ad 的从属数据。
 * {@code findCandidates} 的候选是靠 {@code JOIN ad_target} 求交出来的，
 * 一条没有任何定向条件的广告**永远 JOIN 不上**，等于一条「死广告」——
 * 所以新建与更新必须在同一个事务里把 ad 和它的定向一起落下去。
 *
 * <p><b>不可变的字段</b>：{@code id} 由路径传入；{@code advertiserId} 只在新建时生效，
 * 更新时忽略（广告归属不允许变更）；{@code createdAt / updatedAt} 由 DB 维护。
 */
@Data
public class AdUpsertRequest {

    /** 广告主 ID。新建必填（DTO 上不标 @NotNull，因为更新时不需要） */
    @Min(value = 1, message = "应大于 0")
    private Integer advertiserId;

    @NotBlank(message = "不能为空")
    @Size(max = 128, message = "长度不能超过 128")
    private String title;

    @NotBlank(message = "不能为空")
    @Size(max = 255, message = "长度不能超过 255")
    private String creativeUrl;

    /** 行业类目，如 mobile / ecommerce */
    @NotBlank(message = "不能为空")
    @Size(max = 32, message = "长度不能超过 32")
    private String categoryCode;

    /** 单次展示出价（元），与 ad.bid DECIMAL(10,4) 对齐 */
    @NotNull(message = "不能为空")
    @DecimalMin(value = "0.0001", message = "应大于 0")
    @Digits(integer = 6, fraction = 4, message = "整数位最多 6 位、小数位最多 4 位")
    private BigDecimal bid;

    /** 日预算（元），与 ad.daily_budget DECIMAL(12,2) 对齐 */
    @NotNull(message = "不能为空")
    @DecimalMin(value = "1.00", message = "应不小于 1 元")
    @Digits(integer = 10, fraction = 2, message = "整数位最多 10 位、小数位最多 2 位")
    private BigDecimal dailyBudget;

    /**
     * 1 在投 2 暂停。新建不传默认 1。
     * <b>3（已耗尽）是系统状态</b>，由 BudgetService 在预算扣减失败时写入，不接受外部指定。
     */
    @Min(value = 1, message = "只能为 1(在投)/2(暂停)")
    @Max(value = 2, message = "只能为 1(在投)/2(暂停)")
    private Integer status;

    /**
     * 定向条件。
     * <ul>
     *   <li>新建：必填且至少 1 条（service 层校验）；</li>
     *   <li>更新：传 {@code null} 表示不动定向，传空数组表示清空定向。</li>
     * </ul>
     */
    @Valid
    private List<TargetItem> targets;
}
