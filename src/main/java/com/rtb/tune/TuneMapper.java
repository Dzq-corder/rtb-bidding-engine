package com.rtb.tune;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Map;

/**
 * SQL 调优专用查询 · M8
 *
 * <p><b>为什么单独建一个 Mapper 而不复用业务 Mapper</b>：
 * 这四轮调优要同时保留「优化前」和「优化后」两种写法（深分页 vs 游标、
 * 明细表 vs 汇总表），必须能随时切着跑才能现场对比。
 * 混进业务 Mapper 会让「哪种写法是线上在用的」变模糊。
 *
 * <p>时间参数用 {@code String} 而不是 {@code LocalDateTime}：
 * 调优要反复手工改窗口试执行计划，字符串最省事，
 * 且 MySQL 会把 'yyyy-MM-dd HH:mm:ss' 正确转成 datetime 比较。
 */
@Mapper
public interface TuneMapper {

    /**
     * M8 第 1 轮靶子：某广告位在给定时间窗内的各结果分布。
     *
     * <p>这是大盘里最典型的一类查询——按维度分组计数。
     * 优化前没有任何二级索引，只能全表扫。
     */
    List<Map<String, Object>> dashGroupByResult(@Param("slot") String slot,
                                                @Param("from") String from,
                                                @Param("to") String to);

    /** 同上，但前缀 EXPLAIN，用于取执行计划。 */
    List<Map<String, Object>> explainDashGroupByResult(@Param("slot") String slot,
                                                       @Param("from") String from,
                                                       @Param("to") String to);

    /**
     * M8 第 4 轮对照：同样口径的查询，但走小时级汇总表。
     *
     * <p>注意这里的分组维度是 {@code stat_hour} 落在窗口内，
     * 而不是 {@code bid_at}——汇总表的主键就是小时。
     */
    List<Map<String, Object>> dashGroupByResultPreagg(@Param("slot") String slot,
                                                      @Param("from") String from,
                                                      @Param("to") String to);

    /** 同上，前缀 EXPLAIN。 */
    List<Map<String, Object>> explainDashGroupByResultPreagg(@Param("slot") String slot,
                                                             @Param("from") String from,
                                                             @Param("to") String to);
}
