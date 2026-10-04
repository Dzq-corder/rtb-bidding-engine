package com.rtb.ad;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

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
}
