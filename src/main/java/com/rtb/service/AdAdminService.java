package com.rtb.service;

import com.rtb.common.BizException;
import com.rtb.common.PageResult;
import com.rtb.dto.AdQuery;
import com.rtb.dto.AdUpsertRequest;
import com.rtb.entity.Ad;
import com.rtb.mapper.AdMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 广告管理端服务：服务 {@code /admin/ad} 这批 CRUD 接口。
 *
 * <p><b>与检索路径的关系</b>：这里只写 {@code ad} 与 {@code ad_target} 两张表，
 * 不触碰竞价链路。写完之后，下一次竞价检索自然就能命中新数据——
 * 因为检索是<b>直读 MySQL</b> 而非走内存索引，所以这里天然没有「索引更新延迟」的问题。
 */
@Slf4j
@Service
public class AdAdminService {

    /** 1 在投 */
    private static final int STATUS_ACTIVE = 1;
    /** 2 暂停 */
    private static final int STATUS_PAUSED = 2;

    private final AdMapper adMapper;

    public AdAdminService(AdMapper adMapper) {
        this.adMapper = adMapper;
    }

    /**
     * 新建广告。
     *
     * <p><b>为什么必须校验定向非空</b>：{@code findCandidates} 的候选是靠
     * {@code JOIN ad_target} 求交出来的，一条没有定向条件的广告永远 JOIN 不上，
     * 等于建了一条「死广告」——建完就查不到、投不出去。所以在入口就拦掉。
     *
     * <p>ad 与 ad_target 必须在**同一事务**里落库，否则中途失败会留下孤儿广告。
     */
    @Transactional
    public Long create(AdUpsertRequest req) {
        if (req.getAdvertiserId() == null) {
            throw BizException.badRequest("advertiserId 不能为空");
        }
        if (req.getTargets() == null || req.getTargets().isEmpty()) {
            throw BizException.badRequest("新建广告必须至少带 1 条定向条件，否则该广告永远不会被检索命中");
        }

        Ad ad = new Ad();
        ad.setAdvertiserId(req.getAdvertiserId());
        ad.setTitle(req.getTitle());
        ad.setCreativeUrl(req.getCreativeUrl());
        ad.setCategoryCode(req.getCategoryCode());
        ad.setBid(req.getBid());
        ad.setDailyBudget(req.getDailyBudget());
        ad.setStatus(req.getStatus() == null ? STATUS_ACTIVE : req.getStatus());

        adMapper.insertAd(ad);                                  // 自增主键回填到 ad.id
        adMapper.batchInsertTargets(ad.getId(), req.getTargets());

        log.info("新建广告 id={} advertiserId={} 定向 {} 条",
                ad.getId(), ad.getAdvertiserId(), req.getTargets().size());
        return ad.getId();
    }

    /** 详情：广告本体 + 它的全部定向条件 */
    public Map<String, Object> detail(Long id) {
        Ad ad = mustGet(id);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("ad", ad);
        out.put("targets", adMapper.listTargetsByAdId(id));
        return out;
    }

    /** 列表：先查总数再查当页，保证 total 与 list 出自同一份筛选条件 */
    public PageResult<Ad> page(AdQuery q) {
        long total = adMapper.countBy(q);
        List<Ad> list = (total == 0) ? List.of() : adMapper.selectPage(q);
        return PageResult.of(total, q.safePageNum(), q.safePageSize(), list);
    }

    /**
     * 更新广告。
     *
     * <p><b>targets 的三态语义</b>（与请求体的 null 语义对齐）：
     * <ul>
     *   <li>{@code null} —— 不动定向，只改 ad 本体字段；</li>
     *   <li>空数组 —— 清空全部定向（会让广告检索不到，慎用）；</li>
     *   <li>非空 —— 全删重插，等价于「覆盖式替换」。</li>
     * </ul>
     * 全删重插而非增量 diff，是为了避免定向的增删改三种情况各写一套 SQL；
     * 代价是 {@code DELETE FROM ad_target WHERE ad_id = ?} 在没有 ad_id 索引时全表扫，
     * 这也是后续要补 {@code idx_ad_dir(ad_id, dim_type)} 的原因。
     */
    @Transactional
    public void update(Long id, AdUpsertRequest req) {
        mustGet(id);

        Ad patch = new Ad();
        patch.setId(id);
        patch.setTitle(req.getTitle());
        patch.setCreativeUrl(req.getCreativeUrl());
        patch.setCategoryCode(req.getCategoryCode());
        patch.setBid(req.getBid());
        patch.setDailyBudget(req.getDailyBudget());
        patch.setStatus(req.getStatus());
        adMapper.updateAd(patch);

        if (req.getTargets() != null) {
            adMapper.deleteTargetsByAdId(id);
            if (!req.getTargets().isEmpty()) {
                adMapper.batchInsertTargets(id, req.getTargets());
            }
        }

        log.info("更新广告 id={} 是否重写定向={}", id, req.getTargets() != null);
    }

    /** 删除：先删从属的定向条件、再删本体；同一事务，避免留下孤儿定向行 */
    @Transactional
    public void delete(Long id) {
        mustGet(id);
        int targets = adMapper.deleteTargetsByAdId(id);
        adMapper.deleteAd(id);
        log.info("删除广告 id={}，连带清理定向 {} 条", id, targets);
    }

    /**
     * 上下架：在 1(在投) 与 2(暂停) 之间切换。
     *
     * <p><b>3(已耗尽) 不接受外部指定</b>——它是 BudgetService 在预算扣减失败时写入的系统状态。
     *
     * <p><b>一个必须知道的坑</b>：把 3 改回 1 <b>并不能真正恢复投放</b>。
     * 因为预算耗尽的根因在 Redis 里——{@code spent:{adId}:{yyyyMMdd}} 的计数仍是满的，
     * 下一次竞价扣减还是失败，广告会被立刻重新标记为 3。
     * 真正的恢复要先重置预算（清掉当日 spent 计数），那是预算管理接口的职责，
     * 不在这个接口里悄悄做，避免调用方以为「改个状态就恢复了」。
     */
    @Transactional
    public void changeStatus(Long id, Integer status) {
        if (status == null || (status != STATUS_ACTIVE && status != STATUS_PAUSED)) {
            throw BizException.badRequest(
                    "status 只能为 1(在投) 或 2(暂停)；3(已耗尽) 是系统状态，不接受手工指定");
        }
        Ad ad = mustGet(id);
        adMapper.updateStatus(id, status);
        log.info("广告 id={} 状态 {} -> {}", id, ad.getStatus(), status);
    }

    /** 取广告，不存在直接抛 404，避免各处重复判空 */
    private Ad mustGet(Long id) {
        Ad ad = adMapper.selectById(id);
        if (ad == null) {
            throw BizException.notFound("广告不存在: id=" + id);
        }
        return ad;
    }
}
