package com.rtb.bid;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

/**
 * 竞价明细 Mapper。
 *
 * <p>M1 阶段逐条 INSERT（{@code rewriteBatchedStatements} 尚未启用批量路径），
 * M7 才改为有界队列 + 批量刷盘。
 *
 * <p>本表无任何查询索引、无分区，M8 四轮调优才逐步加：
 * <ol>
 *   <li>覆盖索引——消除回表</li>
 *   <li>深分页——游标翻页 / 延迟关联</li>
 *   <li>时间分区——RANGE 按天，裁剪 partitions</li>
 *   <li>预聚合——大盘改查小时汇总表</li>
 * </ol>
 */
@Mapper
public interface BidLogMapper {

    /**
     * 写入一条竞价明细。
     *
     * <p>M1 朴素实现：单条 INSERT，逐条调用。
     * M7 起只用于批量刷盘关闭时的 baseline 对照路径（{@code rtb.log.batch-enabled=false}）。
     */
    int insert(BidLog log);

    /**
     * 批量写入竞价明细 —— M7 主路径。
     *
     * <p><b>为什么单条和批量是两个方法而不是一个</b>：
     * 这不是重复代码，而是<strong>两条写入策略的分界线</strong>。
     * {@link #insert(BidLog)} 是「不可丢」语义的写法（虽然本表可丢，
     * 但 baseline 路径必须能和 M1 对齐），
     * 本方法是「可丢、可批量」语义。合成一个方法会让
     * 「计费不可丢 / 明细可丢」这个分级在代码里消失，
     * 而它恰恰是本项目最想表达的东西。
     *
     * <p><b>为什么用多值 INSERT 而不是 MyBatis 的 ExecutorType.BATCH</b>：
     * 两者都依赖 {@code rewriteBatchedStatements=true}，
     * 但多值 INSERT 是<strong>一条语句、一次网络往返</strong>，
     * 而 BATCH 模式仍是 N 次 send（只是合并了网络包）。
     * 本项目的主要开销是往返延迟（Docker 端口转发约 4.8ms/次），
     * 所以选减少往返次数的那种。
     *
     * <p><b>返回值不可用于判重</b>：同 M6 坑 1，Connector/J 的
     * {@code useAffectedRows} 默认 false。本表本来也不需要判重。
     *
     * @param logs 一批明细，建议 ≤ 500 条（受 {@code max_allowed_packet} 限制）
     */
    int insertBatch(java.util.List<BidLog> logs);

    /**
     * 统计有广告的占比，M1 用于空广告率基线。
     */
    long countHasAd();

    long countAll();
}
