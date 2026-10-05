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
     * M8 第 2 轮：三种深分页写法同口径对比。
     *
     * <p><b>为什么三个变体要放进一个接口里</b>：
     * 它们必须共享同一个锚点（第 offset 条），否则"翻到的页"不一样，
     * 耗时对比就没有意义。放一个接口里才能保证锚点一致。
     *
     * <p><b>返回的 firstId 用于自检</b>：三种写法翻到的第一行必须是同一行。
     * 如果哪次对不上，说明排序键有问题——这比只报耗时可靠得多。
     */
    @GetMapping("/page")
    public Map<String, Object> page(
            @RequestParam(defaultValue = "feed_top") String slot,
            @RequestParam(defaultValue = "100000") long offset,
            @RequestParam(defaultValue = "20") int limit,
            @RequestParam(defaultValue = "5") int repeat) {

        // 锚点：只在计时外跑一次。它自己就是一条深分页查询，
        // 计进 B 的耗时等于把 A 的代价偷渡给 B。
        Map<String, Object> anchor = mapper.cursorAt(slot, offset).get(0);
        String bidAt = toDateTimeString(anchor.get("bid_at"));
        int hasAd = ((Number) anchor.get("has_ad")).intValue();
        long lastId = ((Number) anchor.get("id")).longValue();

        Map<String, Object> m = new LinkedHashMap<>();
        m.put("slot", slot);
        m.put("offset", offset);
        m.put("limit", limit);
        m.put("anchor", bidAt + " / has_ad=" + hasAd + " / id=" + lastId);
        m.put("repeat", repeat);

        m.put("offsetScan", run("offset", repeat, i -> mapper.pageOffset(slot, offset, limit)));
        m.put("deferredJoin", run("deferred", repeat, i -> mapper.pageDeferred(slot, offset, limit)));
        m.put("cursor", run("cursor", repeat, i -> mapper.pageCursor(slot, bidAt, hasAd, lastId, limit)));

        // 自检：三种写法是不是翻到了同一页
        @SuppressWarnings("unchecked")
        Map<String, Object> a = (Map<String, Object>) ((Map<String, Object>) m.get("offsetScan")).get("page");
        @SuppressWarnings("unchecked")
        Map<String, Object> b = (Map<String, Object>) ((Map<String, Object>) m.get("deferredJoin")).get("page");
        @SuppressWarnings("unchecked")
        Map<String, Object> c = (Map<String, Object>) ((Map<String, Object>) m.get("cursor")).get("page");
        m.put("samePage", a != null && b != null && c != null
                && a.equals(b) && b.equals(c));
        return m;
    }

    /** 取三种深分页写法的执行计划。 */
    @GetMapping("/explain/page")
    public Map<String, Object> explainPage(
            @RequestParam(defaultValue = "feed_top") String slot,
            @RequestParam(defaultValue = "100000") long offset,
            @RequestParam(defaultValue = "20") int limit) {
        Map<String, Object> anchor = mapper.cursorAt(slot, offset).get(0);
        String bidAt = toDateTimeString(anchor.get("bid_at"));
        int hasAd = ((Number) anchor.get("has_ad")).intValue();
        long lastId = ((Number) anchor.get("id")).longValue();

        Map<String, Object> m = new LinkedHashMap<>();
        m.put("offsetScan", pick(mapper.explainPageOffset(slot, offset, limit), "offset"));
        m.put("deferredJoin", pick(mapper.explainPageDeferred(slot, offset, limit), "deferred"));
        m.put("cursor", pick(mapper.explainPageCursor(slot, bidAt, hasAd, lastId, limit), "cursor"));
        return m;
    }

    /** 跑 repeat 次，返回全部原始耗时 + min/avg + 第一行（用于自检同一页）。 */
    private Map<String, Object> run(String name, int repeat,
                                    java.util.function.IntFunction<List<Map<String, Object>>> q) {
        List<Long> costs = new ArrayList<>();
        List<Map<String, Object>> last = null;
        for (int i = 0; i < repeat; i++) {
            long t0 = System.nanoTime();
            last = q.apply(i);
            costs.add((System.nanoTime() - t0) / 1_000_000);
        }
        long min = costs.stream().mapToLong(Long::longValue).min().orElse(0);
        double avg = costs.stream().mapToLong(Long::longValue).average().orElse(0);

        Map<String, Object> m = new LinkedHashMap<>();
        m.put("name", name);
        m.put("costMs", costs);
        m.put("minMs", min);
        m.put("avgMs", Math.round(avg * 100.0) / 100.0);
        m.put("rows", last == null ? 0 : last.size());
        // 只留第一行，20 行全返回会把响应撑得没法看
        m.put("page", (last == null || last.isEmpty()) ? null : last.get(0));
        return m;
    }

    /**
     * 把 JDBC 返回的时间对象转成 MySQL 能直接比较的字符串。
     *
     * <p>MyBatis 用 {@code Map} 接结果时，{@code datetime(3)} 可能回来的是
     * {@code Timestamp} 也可能是 {@code LocalDateTime}，这里统一处理，
     * 否则游标比较会因为时区/格式问题静默查不到数据。
     */
    private static String toDateTimeString(Object v) {
        if (v instanceof java.sql.Timestamp ts) {
            return ts.toLocalDateTime().toString().replace('T', ' ');
        }
        if (v instanceof java.time.LocalDateTime ldt) {
            return ldt.toString().replace('T', ' ');
        }
        return String.valueOf(v);
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
