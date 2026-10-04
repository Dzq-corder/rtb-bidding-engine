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
     * M7 改为攒批后批量 INSERT。
     */
    int insert(BidLog log);

    /**
     * 统计有广告的占比，M1 用于空广告率基线。
     */
    long countHasAd();

    long countAll();
}
