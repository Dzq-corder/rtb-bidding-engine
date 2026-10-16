package com.rtb.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import lombok.Data;

/**
 * 单条定向条件，对应 {@code ad_target} 的一行（dim_type + dim_value）。
 */
@Data
public class TargetItem {

    /** 1 地域 2 年龄段 3 性别 4 兴趣 5 系统 6 设备 */
    @Min(value = 1, message = "应在 1~6 之间")
    @Max(value = 6, message = "应在 1~6 之间")
    private Integer dimType;

    /** 定向值，如 110000 / 25-30 / male / 数码科技 */
    @NotBlank(message = "不能为空")
    @Size(max = 64, message = "长度不能超过 64")
    private String dimValue;
}
