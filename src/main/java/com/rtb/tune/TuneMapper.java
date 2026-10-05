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
     * M8 第 4 轮对照：同样口径的成交指标，但走小时级汇总表。
     *
     * <p>⚠️ <b>口径为什么必须是「成交维度」而不是「结果分布」</b>：
     * {@code HourlyPreaggConsumer} 的数据源是 Kafka 事件流，而
     * {@code BidService.publishEvent} 在没有胜出广告时直接 return
     * ——空广告竞价根本不进 Kafka。所以预聚合表里
     * {@code ad_cnt} 恒等于 {@code cnt}，{@code cnt - ad_cnt} 永远是 0，
     * <b>这张表算不出空广告率</b>。
     *
     * <p>成交维度则两侧严格等价：
     * <pre>
     *   明细：  COUNT(*)                        WHERE has_ad = 1
     *   汇总：  SUM(ad_cnt)
     *   明细：  SUM(pay_price)                 WHERE has_ad = 1
     *   汇总：  SUM(total_pay_price)
     * </pre>
     * 均价两边都是「总价 / 笔数」，只是汇总表要先 SUM 再除
     * （这就是 M8 第 4 轮把 avg_pay_price 改成 total_pay_price 的原因）。
     */
    List<Map<String, Object>> dashTradePreagg(@Param("slot") String slot,
                                               @Param("from") String from,
                                               @Param("to") String to);

    /** 同上，前缀 EXPLAIN。 */
    List<Map<String, Object>> explainDashTradePreagg(@Param("slot") String slot,
                                                     @Param("from") String from,
                                                     @Param("to") String to);

    /**
     * M8 第 4 轮 · 明细侧的同口径查询（对照组）。
     *
     * <p>返回成交笔数与成交总价，用来和 {@link #dashTradePreagg} 逐项对账。
     * 两边数字必须完全相等——不相等就说明回填口径错了，预聚合就不可信。
     */
    List<Map<String, Object>> dashTradeDetail(@Param("slot") String slot,
                                              @Param("from") String from,
                                              @Param("to") String to);

    /** 同上，前缀 EXPLAIN。 */
    List<Map<String, Object>> explainDashTradeDetail(@Param("slot") String slot,
                                                    @Param("from") String from,
                                                    @Param("to") String to);

    // =================================================================
    // M8 第 2 轮 · 深分页
    // =================================================================

    /**
     * 取第 offset 条处的游标值（只在测量前跑一次，不计入耗时）。
     *
     * <p>游标翻页需要一个「上一页最后一行」的锚点，这个锚点必须先算出来。
     * 它自己就是一条深分页查询，所以<b>必须在计时之外跑</b>，
     * 否则等于把 A 的代价偷偷算进了 B。
     */
    List<Map<String, Object>> cursorAt(@Param("slot") String slot,
                                       @Param("offset") long offset);

    /**
     * A：直查深分页——最直觉也最常见的写法。
     *
     * <p>SELECT 里带 {@code request_id / cost_ms / pay_price} 是刻意的：
     * 这几列不在索引里，MySQL 必须为<b>被跳过的 100,020 行逐行回表</b>。
     * 只查索引列的"假深分页"会低估代价，那种写法在现实中也不存在
     * ——列表页不可能只显示 id。
     */
    List<Map<String, Object>> pageOffset(@Param("slot") String slot,
                                         @Param("offset") long offset,
                                         @Param("limit") int limit);

    /**
     * B：延迟关联——把"跳过"这一步锁在覆盖索引上，只对最终 20 行回表。
     *
     * <p><b>它的存在理由是「还能跳页」</b>：游标翻页更快，但只能一页页往下翻，
     * 跳不到第 N 页。延迟关联保留了 {@code LIMIT offset} 的语义，
     * 代价仍随 offset 线性增长，只是把每行的代价从"一次随机回表"降到"一次索引项扫描"。
     */
    List<Map<String, Object>> pageDeferred(@Param("slot") String slot,
                                           @Param("offset") long offset,
                                           @Param("limit") int limit);

    /**
     * C：游标翻页——把 offset 换成"上一页最后一行"的行值比较。
     *
     * <p><b>⚠️ 光写行值比较是不够的，必须再带一个 {@code bid_at <= ?}</b>：
     * 只有行值比较时，MySQL 8.0 把它当 Filter 而不是 range 起点，
     * 实测倒扫该广告位 494 万条索引项、耗时 198 ms，和直查深分页（191 ms）一样慢。
     * 补上 {@code bid_at <= ?} 后 {@code type} 由 ref 变 range、
     * {@code key_len} 由 130 变 137，倒扫直接定位到锚点。
     * 详情见 XML 里那段注释。
     *
     * <p><b>为什么排序键写成 {@code (bid_at, has_ad, id)} 三列</b>：
     * 现有索引是 {@code (slot_code, bid_at, has_ad)}，InnoDB 会自动把主键 id 追加在后面，
     * 所以它的实际顺序是 {@code (slot_code, bid_at, has_ad, id)}。
     * 只写 {@code ORDER BY bid_at DESC} 时顺序不确定（bid_at 是 datetime(3)，会重复），
     * 补上 {@code id DESC} 又会被中间的 has_ad 打断而退化成 filesort。
     * <b>把排序键写成与索引完全一致的 (bid_at, has_ad, id)，
     * 既拿到了全序（可以安全做游标），又不需要额外索引，也不会 filesort。</b>
     *
     * <p>这个结论是 EXPLAIN 试出来的：先写 {@code ORDER BY bid_at DESC, id DESC}，
     * 看到 Using filesort 才改成三列。
     */
    List<Map<String, Object>> pageCursor(@Param("slot") String slot,
                                         @Param("bidAt") String bidAt,
                                         @Param("hasAd") int hasAd,
                                         @Param("lastId") long lastId,
                                         @Param("limit") int limit);

    List<Map<String, Object>> explainPageOffset(@Param("slot") String slot,
                                                @Param("offset") long offset,
                                                @Param("limit") int limit);

    List<Map<String, Object>> explainPageDeferred(@Param("slot") String slot,
                                                  @Param("offset") long offset,
                                                  @Param("limit") int limit);

    List<Map<String, Object>> explainPageCursor(@Param("slot") String slot,
                                                @Param("bidAt") String bidAt,
                                                @Param("hasAd") int hasAd,
                                                @Param("lastId") long lastId,
                                                @Param("limit") int limit);
}
