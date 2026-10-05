package com.rtb.tune;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * SQL 调优观测端点 · M8
 *
 * <p>存在理由和 M6 的 {@code /kafka/*}、M7 的 {@code /logbuf/*} 一样：
 * <b>「加了索引快了」这句话本身没有信息量，必须把 EXPLAIN 和耗时都摆出来。</b>
 *
 * <p><b>为什么耗时默认跑 5 次取最小</b>：
 * 单次耗时受 Buffer Pool 冷热、机器抖动影响很大，第一次往往特别慢。
 * 取最小值是因为它最接近「缓存已热、无外部干扰」的真实查询代价，
 * 同时把 5 次原始值都返回——<b>不允许只报一个挑出来的数字</b>。
 */
@RestController
@RequestMapping("/tune")
public class TuningController {

    private final TuneMapper mapper;

    public TuningController(TuneMapper mapper) {
        this.mapper = mapper;
    }

    /**
     * 第 1 轮靶子查询：某广告位时间窗内各结果分布。
     *
     * <p>{@code source=detail}（默认）查 1000 万行的 {@code bid_log}；
     * {@code source=preagg} 查几十万行的 {@code bid_stat_hourly}（第 4 轮对照）。
     */
    @GetMapping("/dash")
    public Map<String, Object> dash(
            @RequestParam(defaultValue = "feed_top") String slot,
            @RequestParam(defaultValue = "2026-10-05 20:00:00") String from,
            @RequestParam(defaultValue = "2026-10-05 21:00:00") String to,
            @RequestParam(defaultValue = "detail") String source,
            @RequestParam(defaultValue = "5") int repeat) {

        List<Long> costs = new ArrayList<>();
        List<Map<String, Object>> last = null;
        for (int i = 0; i < repeat; i++) {
            long t0 = System.nanoTime();
            last = "preagg".equalsIgnoreCase(source)
                    ? mapper.dashGroupByResultPreagg(slot, from, to)
                    : mapper.dashGroupByResult(slot, from, to);
            costs.add((System.nanoTime() - t0) / 1_000_000);
        }
        long min = costs.stream().mapToLong(Long::longValue).min().orElse(0);
        double avg = costs.stream().mapToLong(Long::longValue).average().orElse(0);

        Map<String, Object> m = new LinkedHashMap<>();
        m.put("source", source);
        m.put("slot", slot);
        m.put("window", from + " ~ " + to);
        m.put("repeat", repeat);
        m.put("costMs", costs);
        m.put("minMs", min);
        m.put("avgMs", Math.round(avg * 100.0) / 100.0);
        m.put("result", last);
        return m;
    }

    /** 取上面查询的执行计划。 */
    @GetMapping("/explain/dash")
    public Map<String, Object> explainDash(
            @RequestParam(defaultValue = "feed_top") String slot,
            @RequestParam(defaultValue = "2026-10-05 20:00:00") String from,
            @RequestParam(defaultValue = "2026-10-05 21:00:00") String to,
            @RequestParam(defaultValue = "detail") String source) {
        List<Map<String, Object>> rows = "preagg".equalsIgnoreCase(source)
                ? mapper.explainDashGroupByResultPreagg(slot, from, to)
                : mapper.explainDashGroupByResult(slot, from, to);
        return pick(rows, source);
    }

    /**
     * 只挑出 EXPLAIN 里真正会讲的那几列。
     *
     * <p>完整 EXPLAIN 有 12 列，大部分在面试现场用不上；
     * 重点是 {@code type}（访问方式）、{@code key}（用了哪个索引）、
     * {@code rows}（预估扫描行数）、{@code partitions}（第 3 轮分区裁剪）、
     * {@code Extra}（有没有 Using index / Using temporary / Using filesort）。
     */
    private static Map<String, Object> pick(List<Map<String, Object>> rows, String source) {
        List<Map<String, Object>> brief = new ArrayList<>();
        for (Map<String, Object> r : rows) {
            Map<String, Object> b = new LinkedHashMap<>();
            for (String k : new String[]{"table", "partitions", "type", "key", "key_len",
                    "rows", "filtered", "Extra"}) {
                if (r.containsKey(k)) {
                    b.put(k, r.get(k));
                }
            }
            brief.add(b);
        }
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("source", source);
        m.put("plan", brief);
        return m;
    }
}
