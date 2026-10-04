package com.rtb.ad;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * 广告 Mapper。
 *
 * <p>M1 阶段刻意保持朴素：SQL 里只有最基础的条件，不加任何提示性优化
 * （没有 hint、没有覆盖索引、没有强制索引），让 MySQL 自然走全表扫描。
 */
@Mapper
public interface AdMapper {

    /**
     * 按类目检索在投广告。
     *
     * <p><b>M1 版本</b>：{@code WHERE status = 1 AND category_code = ?}，
     * 无索引，EXPLAIN 为 type=ALL。这是有意为之——M2 才引入倒排索引替代它。
     *
     * <p>注意：{@code status} 上不建索引，因为一旦建了索引，
     * baseline 就变成range 而非ALL，M8 第1 轮就没有对照价值了。
     */
    List<Ad> findActiveByCategory(@Param("categoryCode") String categoryCode);

    /**
     * 查询全部在投广告数量，M1 用于填充率与候选集规模的基线统计。
     */
    long countActive();
}
