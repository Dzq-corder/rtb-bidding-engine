package com.rtb.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import lombok.Data;

/**
 * 广告列表查询条件。
 *
 * <p><b>主入口是 advertiserId</b>：管理后台是「广告商登录后看自己的广告」，
 * 所以按广告主筛选是最高频的查询；status 用于在列表里切换「在投 / 暂停 / 已耗尽」页签。
 *
 * <p><b>当前性能提示</b>：{@code ad} 表除主键外没有任何索引（M1 baseline 的刻意设计），
 * 所以按 advertiserId 查询目前是 50 万行全表扫 + filesort。
 * 后续补 {@code KEY idx_adv (advertiser_id)} 后，可借 InnoDB 二级索引自带主键的特性
 * 直接反向扫描满足 {@code ORDER BY id DESC}，无需额外排序。
 */
@Data
public class AdQuery {

    /** 按广告主筛选，最常用的入口 */
    private Integer advertiserId;

    /** 1 在投 2 暂停 3 已耗尽 */
    @Min(value = 1, message = "应在 1~3 之间")
    @Max(value = 3, message = "应在 1~3 之间")
    private Integer status;

    /** 标题模糊匹配 */
    private String titleLike;

    /** 页码，从 1 开始 */
    @Min(value = 1, message = "应不小于 1")
    private Integer pageNum = 1;

    /** 每页条数，上限 200，避免一次拉爆内存 */
    @Min(value = 1, message = "应不小于 1")
    @Max(value = 200, message = "不应超过 200")
    private Integer pageSize = 20;

    /** 规范化后的页码（防御 JSON 显式传 null 覆盖默认值） */
    public int safePageNum() {
        return (pageNum == null || pageNum < 1) ? 1 : pageNum;
    }

    /** 规范化后的每页条数 */
    public int safePageSize() {
        int ps = (pageSize == null || pageSize < 1) ? 20 : pageSize;
        return Math.min(ps, 200);
    }

    /** 供 SQL 使用：LIMIT 偏移量 */
    public int getOffset() {
        return (safePageNum() - 1) * safePageSize();
    }

    /** 供 SQL 使用：LIMIT 条数 */
    public int getLimit() {
        return safePageSize();
    }
}
