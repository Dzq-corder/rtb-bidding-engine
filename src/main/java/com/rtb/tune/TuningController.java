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
     * <p>{@code source=detail}（默认）查 1000 万行的 {@code bid_log}。
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
            last = "trade".equalsIgnoreCase(source)
                    ? mapper.dashTradeDetail(slot, from, to)
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

    /**
     * M8 第 4 轮：大盘改查预聚合表前后的 A→B。
     *
     * <p>两侧口径必须完全等价，所以这个接口除了计时，还<b>逐项比对数字</b>。
     * 预聚合快是没意义的——如果它算出来的数和明细表不一样，快的是错的。
     */
    @GetMapping("/dash/compare")
    public Map<String, Object> dashCompare(
            @RequestParam(defaultValue = "feed_top") String slot,
            @RequestParam(defaultValue = "2026-09-24 00:00:00") String from,
            @RequestParam(defaultValue = "2026-10-01 00:00:00") String to,
            @RequestParam(defaultValue = "5") int repeat) {

        Map<String, Object> a = timed(() -> mapper.dashTradeDetail(slot, from, to), repeat);
        Map<String, Object> b = timed(() -> mapper.dashTradePreagg(slot, from, to), repeat);

        Map<String, Object> m = new LinkedHashMap<>();
        m.put("slot", slot);
        m.put("window", from + " ~ " + to);
        m.put("repeat", repeat);
        m.put("detail", a);
        m.put("preagg", b);
        m.put("speedup", round(((Number) a.get("minMs")).longValue()
                / Math.max(1L, ((Number) b.get("minMs")).longValue())));
        m.put("sameResult", sameNumbers((List<Map<String, Object>>) a.get("result"),
                (List<Map<String, Object>>) b.get("result")));
        return m;
    }

    /**
     * 逐项按数值比较两侧结果。
     *
     * <p><b>为什么不能用 {@code Map.equals}</b>：
     * 明细侧 {@code COUNT(*)} 返回 {@code Long}，
     * 预聚合侧 {@code SUM(ad_cnt)} 返回 {@code BigDecimal}
     * （MySQL 里 SUM 整数返回 DECIMAL）。
     * 值明明都是 242158，对象 equals 却是 false——
     * <b>用对象相等去判断"两个数字一样"是错的</b>，必须按数值比。
     *
     * <p>这个 bug 第一版就踩了：接口返回 {@code sameResult:false}，
     * 但把两边的 result 打出来看，每个数字都完全一致。
     */
    private static boolean sameNumbers(List<Map<String, Object>> a,
                                       List<Map<String, Object>> b) {
        if (a == null || b == null || a.size() != b.size()) {
            return false;
        }
        for (int i = 0; i < a.size(); i++) {
            Map<String, Object> x = a.get(i);
            Map<String, Object> y = b.get(i);
            if (x.size() != y.size()) {
                return false;
            }
            for (String k : x.keySet()) {
                if (!y.containsKey(k)) {
                    return false;
                }
                // ⚠️ 不能写成 !a.compareTo(b) == 0：
                //    Java 里 ! 的优先级高于 ==，会被解析成 (!a.compareTo(b)) == 0
                //    ——对 int 取 ! 再和 0 比，编译都过不了，逻辑也是错的。
                if (toDecimal(x.get(k)).compareTo(toDecimal(y.get(k))) != 0) {
                    return false;
                }
            }
        }
        return true;
    }

    private static java.math.BigDecimal toDecimal(Object v) {
        if (v == null) {
            return java.math.BigDecimal.ZERO;
        }
        if (v instanceof java.math.BigDecimal bd) {
            return bd;
        }
        return new java.math.BigDecimal(String.valueOf(v));
    }

    /** 跑 repeat 次，返回全部原始耗时 + min/avg + 最后一次的结果。 */
    private Map<String, Object> timed(
            java.util.function.Supplier<List<Map<String, Object>>> q, int repeat) {
        List<Long> costs = new ArrayList<>();
        List<Map<String, Object>> last = null;
        for (int i = 0; i < repeat; i++) {
            long t0 = System.nanoTime();
            last = q.get();
            costs.add((System.nanoTime() - t0) / 1_000_000);
        }
        long min = costs.stream().mapToLong(Long::longValue).min().orElse(0);
        double avg = costs.stream().mapToLong(Long::longValue).average().orElse(0);
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("costMs", costs);
        m.put("minMs", min);
        m.put("avgMs", round(avg));
        m.put("result", last);
        return m;
    }

    private static double round(double v) {
        return Math.round(v * 100.0) / 100.0;
    }

    /** 取上面查询的执行计划。 */
    @GetMapping("/explain/dash")
    public Map<String, Object> explainDash(
            @RequestParam(defaultValue = "feed_top") String slot,
            @RequestParam(defaultValue = "2026-10-05 20:00:00") String from,
            @RequestParam(defaultValue = "2026-10-05 21:00:00") String to,
            @RequestParam(defaultValue = "detail") String source) {
        List<Map<String, Object>> rows = "trade".equalsIgnoreCase(source)
                ? mapper.explainDashTradeDetail(slot, from, to)
                : mapper.explainDashGroupByResult(slot, from, to);
        return pick(rows, source);
    }

    /** 第 4 轮两侧的执行计划对照。 */
    @GetMapping("/explain/dash/compare")
    public Map<String, Object> explainDashCompare(
            @RequestParam(defaultValue = "feed_top") String slot,
            @RequestParam(defaultValue = "2026-09-24 00:00:00") String from,
            @RequestParam(defaultValue = "2026-10-01 00:00:00") String to) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("detail", pick(mapper.explainDashTradeDetail(slot, from, to), "detail"));
        m.put("preagg", pick(mapper.explainDashTradePreagg(slot, from, to), "preagg"));
        return m;
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
