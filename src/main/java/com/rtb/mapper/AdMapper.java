package com.rtb.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;

import java.util.List;
import java.util.Map;
import com.rtb.entity.Ad;
import com.rtb.service.BudgetService;
/**
 * 广告 Mapper。
 *
 * <p><b>M1 的检索方式</b>：ad 表全表扫描 + ad_target 多维 JOIN 求交。
 *
 * <p><b>为什么是「一半优化」的</b>：M1 有两个互相冲突的要求——
 * 候选集要落在 100~200 条（否则造数与演示都不成立），
 * 而检索要足够慢（否则 M2 没有对照价值）。
 * 解法是分工：<b>ad 表不建索引承担「慢」</b>，<b>ad_target 建索引承担「筛得准」</b>。
 * 详见 {@code sql/all-schema.sql}。
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
     * 标记广告当日预算已耗尽（status=1 -> 3）。
     *
     * <p>仅当广告当前在投（status=1）时才更新，已是 3 的不动，重复调用幂等。
     * 由 BudgetService 在 deduct 余额不足时调用；检索 SQL 带
     * {@code WHERE a.status = 1}，标记后该广告在候选阶段即被排除。
     */
    @Update("UPDATE ad SET status = 3 WHERE id = #{id} AND status = 1")
    int markExhausted(@Param("id") Long id);
}
