package com.rtb.ad;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Map;
/**
 * 广告 Mapper。
 *
 * <p><b>M1 的检索方式</b>：ad 表全表扫描 + ad_target 多维 JOIN 求交。
 *
 * <p><b>为什么是「一半优化」的</b>：M1 有两个互相冲突的要求——
 * 候选集要落在 100~200 条（否则造数与演示都不成立），
 * 而检索要足够慢（否则 M2 没有对照价值）。
 * 解法是分工：<b>ad 表不建索引承担「慢」</b>，<b>ad_target 建索引承担「筛得准」</b>。
 * 详见 {@code sql/m1_indexes.sql}。
 */
@Mapper
public interface AdMapper {

    /**
     * 按定向条件做多维交集检索候选广告。
     *
     * <p>三个定向维度（地域 / 年龄段 / 性别）取交集，再叠加类目与在投状态。
     * 任一维度传 null 表示该维度不参与过滤，对应的 JOIN 会被拼掉。
     *
     * <p>这里的 JOIN 是朴素的多表连接。M2 会把整个求交过程挪到内存
     * BitSet 上做，从「DB 全表扫 + JOIN」变成「位运算求交」，
     * 届时 candidateCost 的 A→B 对比就来自这一处。
     */
    List<Ad> findCandidates(@Param("region") String region,
                            @Param("ageBucket") String ageBucket,
                            @Param("gender") String gender,
                            @Param("categoryCode") String categoryCode);

    /**
     * 在投广告总数，M1 用于填充率与候选集规模的基线统计。
     */
    long countActive();

    /**
     * 读取指定维度的全部定向取值（dim_type, dim_value）。
     *
     * <p>用途：压测接口生成随机请求时，需要从「库里真实存在的取值」里抽样。
     * 硬编码词表会与造数参数脱节——M1 调整地域码后，
     * 硬编码的旧值在库中不存在，导致候选恒为 0、空广告率 100%。
     *
     * <p>只读一次，结果缓存在控制器里，不在每次竞价时调用。
     *
     * @param dimTypes 只查这些维度。ad_target 有 596 万行，全表读会拖慢启动。
     */
    List<Map<String, Object>> listTargetingValues(@Param("dimTypes") List<Integer> dimTypes);

    /**
     * 统计指定维度各取值的出现次数。
     *
     * <p>用途：压测接口按权重抽样地域。
     * 地域有 4,748 个，但广告的地域定向按人口权重偏斜——
     * 头部城市约 7,900 条，长尾只有几十条。
     * 均匀随机会让 82.8% 的请求落到长尾地域，候选只剩 1~49 条，
     * 测出来的分布与真实流量不符。按命中数加权后才一致。
     */
    List<Map<String, Object>> countTargetingValues(@Param("dimTypes") List<Integer> dimTypes);

    /**
     * 全量加载广告详情，供内存倒排索引常驻缓存。
     *
     * <p>一次返回 50 万条——对象本身约 50MB，可接受。
     * 若数据量再涨一个数量级，应改为分批加载。
     */
    List<Ad> loadAllAds();

    /**
     * 按主键游标分页读取定向条件，供 M2 内存倒排索引构建。
     *
     * <p>用游标（WHERE id > lastId）而不是 OFFSET 分页：
     * OFFSET 在深翻页时要扫描并丢弃前面的行，596 万行下会非常慢。
     */
    List<com.rtb.index.InvertedIndex.TargetRow> selectTargetingBatch(@Param("lastId") long lastId,
                                                                    @Param("limit") int limit);

    /**
     * 查兜底广告候选池（M5 熔断降级用）。
     *
     * <p>筛选条件的设计意图见 {@code FallbackAdPool} 的类注释，
     * 简单说：<b>预算充足 + 定向宽泛 + 出价低</b>。
     *
     * <p>出价低是硬要求：熔断往往发生在系统高负载时，
     * 此时扣高价广告的钱等于在最不该扣的时候扣最多的钱。
     *
     * <p>按定向条件数升序：定向越宽泛，越容易在紧急时刻被选中兜底。
     */
    List<Ad> findFallbackAds(@Param("limit") int limit);

    /**
     * 按主键查单条广告，用于兜底时取详情。
     */
    Ad findById(@Param("id") Long id);

    // =================================================================
    // 写入侧 · M9
    //
    // 存在的唯一理由是「改预算 / 改定向，保存即生效」。
    // 这里只负责改数据库，内存索引的同步由 {@code AdAdminController}
    // 调 {@code InvertedIndex} 的增量方法完成——
    //
    // 【为什么不把两件事塞进一个 Service】
    // 因为它们的失败语义不同：数据库写成功而内存更新失败，
    // 系统会进入「库里是新的、内存是旧的」的不一致状态。
    // 拆开后调用方能明确决定「先改库还是先改内存」，
    // 也更容易在演示时观察出问题出在哪一侧。
    // =================================================================

    /**
     * 改日预算（可同时改出价与在投状态）。
     *
     * <p><b>刻意用一条 UPDATE 而不是一个通用 SET 拼接</b>：
     * 通用 SET 拼接要防 SQL 注入，而这里所有可改字段都是数值或固定枚举，
     * 用显式参数绑定既安全又能让 MySQL 用上 {@code updated_at} 的自动更新。
     *
     * <p>出价单独给了 setter：出价影响二价结算结果，
     * 演示「改出价 → 扣费金额变化」时需要能单独改。
     */
    int updateBudget(@Param("id") Long id,
                     @Param("dailyBudget") java.math.BigDecimal dailyBudget);

    int updateBid(@Param("id") Long id, @Param("bid") java.math.BigDecimal bid);

    /** 改状态：1 在投 / 2 暂停 / 3 已耗尽 */
    int updateStatus(@Param("id") Long id, @Param("status") Integer status);

    /**
     * 读一条广告的全部定向项，供「保存定向」算差集。
     *
     * <p>返回 {@link com.rtb.index.InvertedIndex.TargetRow} 而不是新造 DTO：
     * 同一份结构索引加载时就在用，少一个类型就少一处可能写错的地方。
     */
    java.util.List<com.rtb.index.InvertedIndex.TargetRow> listTargetingOf(@Param("adId") Long adId);

    /**
     * 删掉某条广告的<b>一条</b>定向条件。
     *
     * <p>刻意不做「按 ad_id 全删」：596 万行的表上那个写法走全表扫，
     * 实测直接撞 5 秒超时。带上 dim_type + dim_value 后
     * 走 {@code idx_dim_lookup} 的 range 扫描，{@code rows=1}。
     * 详见 {@code AdMapper.xml} 里 {@code deleteTargetingOne} 的注释。
     */
    int deleteTargetingOne(@Param("adId") Long adId,
                           @Param("dimType") Integer dimType,
                           @Param("dimValue") String dimValue);

    /**
     * 清空某条广告的全部定向（兜底路径，低频）。
     *
     * <p>会扫全表，因此 SQL 上单独放宽了超时。
     * 正常保存流程走「先读现状 → 逐条对比 → 只删要删的」，
     * 不会用到这条。
     */
    int deleteTargetingOf(@Param("adId") Long adId);

    int insertTargeting(@Param("adId") Long adId,
                        @Param("dimType") Integer dimType,
                        @Param("dimValue") String dimValue);

    /**
     * 恢复一条广告的日预算到指定值（演示重置用）。
     *
     * <p>与 {@link #updateBudget} 分开是因为语义不同：
     * 这个会顺带把 status 从「已耗尽」改回「在投」，
     * 而前者不会——广告主主动调预算和运维重置演示数据不是一回事。
     */
    int resetBudgetForDemo(@Param("id") Long id,
                           @Param("dailyBudget") java.math.BigDecimal dailyBudget);

    /**
     * 管理端列表：按出价倒序取前 N 条在投广告。
     *
     * <p>刻意<b>不用分页</b>：管理端只给演示看，出价最高的 50 条就够了，
     * 而且按出价排序天然把「值得演示的广告」排在前面。
     */
    java.util.List<Ad> listTopByBid(@Param("limit") int limit);
}
