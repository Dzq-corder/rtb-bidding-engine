package com.rtb.controller;

import com.rtb.common.ApiResponse;
import com.rtb.common.PageResult;
import com.rtb.dto.AdQuery;
import com.rtb.dto.AdUpsertRequest;
import com.rtb.entity.Ad;
import com.rtb.service.AdAdminService;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * 广告管理端 CRUD 接口（媒体侧自营广告平台的「投放管理」那一块）。
 *
 * <table border="1">
 *   <caption>接口一览</caption>
 *   <tr><th>方法</th><th>路径</th><th>说明</th></tr>
 *   <tr><td>POST</td><td>/admin/ad</td><td>新建（ad 与定向同事务）</td></tr>
 *   <tr><td>GET</td><td>/admin/ad/{id}</td><td>详情（含定向）</td></tr>
 *   <tr><td>GET</td><td>/admin/ad</td><td>列表（按广告主 / 状态筛选 + 分页）</td></tr>
 *   <tr><td>PUT</td><td>/admin/ad/{id}</td><td>更新（定向为 null 时不动）</td></tr>
 *   <tr><td>DELETE</td><td>/admin/ad/{id}</td><td>删除（级联清定向）</td></tr>
 *   <tr><td>PUT</td><td>/admin/ad/{id}/status</td><td>上下架（1 在投 ↔ 2 暂停）</td></tr>
 * </table>
 *
 * <p>响应统一走 {@link ApiResponse}；业务错误由 {@code BizException} 抛出，
 * 在全局异常处理里转成对应状态码（404 不存在 / 409 冲突 / 400 参数不合法）。
 */
@RestController
@RequestMapping("/admin/ad")
public class AdAdminController {

    private final AdAdminService adAdminService;

    public AdAdminController(AdAdminService adAdminService) {
        this.adAdminService = adAdminService;
    }

    /** 新建广告。请求体需带 advertiserId 与至少 1 条定向条件 */
    @PostMapping
    public ApiResponse<Map<String, Object>> create(@Valid @RequestBody AdUpsertRequest req) {
        Long id = adAdminService.create(req);
        return ApiResponse.ok(Map.of("id", id));
    }

    /** 广告详情，返回 { ad: {...}, targets: [{dimType, dimValue}] } */
    @GetMapping("/{id}")
    public ApiResponse<Map<String, Object>> detail(@PathVariable Long id) {
        return ApiResponse.ok(adAdminService.detail(id));
    }

    /**
     * 广告列表。查询参数绑定到 {@link AdQuery}：
     * {@code ?advertiserId=1001&status=1&pageNum=1&pageSize=20}
     *
     * <p>这里的 {@code @Valid} 触发的是数据绑定校验（失败抛 BindException），
     * 与请求体校验（MethodArgumentNotValidException）不同，两者在全局异常处理里合并处理。
     */
    @GetMapping
    public ApiResponse<PageResult<Ad>> page(@Valid AdQuery query) {
        return ApiResponse.ok(adAdminService.page(query));
    }

    /** 更新广告。targets 传 null 表示不动定向，传空数组表示清空 */
    @PutMapping("/{id}")
    public ApiResponse<Map<String, Object>> update(@PathVariable Long id,
                                                  @Valid @RequestBody AdUpsertRequest req) {
        adAdminService.update(id, req);
        return ApiResponse.ok(Map.of("id", id, "updated", true));
    }

    /** 删除广告，连带清理它的定向条件 */
    @DeleteMapping("/{id}")
    public ApiResponse<Map<String, Object>> delete(@PathVariable Long id) {
        adAdminService.delete(id);
        return ApiResponse.ok(Map.of("id", id, "deleted", true));
    }

    /** 上下架：status=1 投放在线 / status=2 暂停 */
    @PutMapping("/{id}/status")
    public ApiResponse<Map<String, Object>> changeStatus(@PathVariable Long id,
                                                        @RequestParam Integer status) {
        adAdminService.changeStatus(id, status);
        return ApiResponse.ok(Map.of("id", id, "status", status));
    }
}
