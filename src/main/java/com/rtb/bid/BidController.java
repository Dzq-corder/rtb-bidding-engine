package com.rtb.bid;

import com.rtb.ad.AdMapper;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 竞价接口。
 *
 * <p>路径刻意保持与简历/演示手册一致：{@code POST /bid}。
 */
@Slf4j
@RestController
@RequiredArgsConstructor
public class BidController {

    /** ad_target.dim_type 常量 */
    private static final int DIM_REGION = 1;
    private static final int DIM_INTEREST = 4;

    /** 库读失败时的兜底值，保证压测接口不会因缓存未初始化而 NPE */
    private static final String[] FALLBACK_REGIONS = {"1101", "3101", "440101", "330101"};
    private static final String[] FALLBACK_INTERESTS = {"数码科技", "美妆护肤", "运动健身"};

    private final BidService bidService;
    private final AdMapper adMapper;

    /** 启动时从 ad_target 读一次定向取值，避免硬编码与造数数据脱节 */
    private volatile Map<String, String[]> targetingPool = Map.of();

    /** 各地域被多少条广告定向，键为dim_value，值为 [count] */
    private volatile Map<String, String[]> regionHitCounts = Map.of();

    @PostConstruct
    void loadTargetingValues() {
        try {
            Map<String, List<String>> pool = new HashMap<>();
            for (Map<String, Object> row : adMapper.listTargetingValues(List.of(DIM_REGION, DIM_INTEREST))) {
                String dim = String.valueOf(row.get("dimType"));
                String value = String.valueOf(row.get("dimValue"));
                pool.computeIfAbsent(dim, k -> new ArrayList<>()).add(value);
            }
            Map<String, String[]> result = new HashMap<>();
            for (Map.Entry<String, List<String>> e : pool.entrySet()) {
                // 带上出现次数作为权重，按权重抽样而非均匀随机
                result.put(e.getKey(), e.getValue().toArray(new String[0]));
            }
            this.targetingPool = result;

            // 各定向取值的广告数——决定被抽中的概率
            Map<String, String[]> counts = new HashMap<>();
            for (Map<String, Object> row : adMapper.countTargetingValues(List.of(DIM_REGION))) {
                counts.put(String.valueOf(row.get("dimValue")),
                        new String[]{String.valueOf(row.get("cnt"))});
            }
            this.regionHitCounts = counts;

            log.info("已加载定向词表：{} 个地域、{} 个兴趣标签",
                    regionPool().length, interestPool().length);
        } catch (Exception e) {
            log.warn("加载定向取值失败，压测将使用兜底词表：{}", e.getMessage());
        }
    }

    /**
     * 按「该地域被多少条广告定向」加权抽样。
     *
     * <p><b>为什么不均匀随机</b>：地域有 4,748 个，但广告的地域定向是
     * 按人口权重偏斜的——头部城市约 7,900 条广告，长尾只有几十条。
     * 均匀随机时 82.8% 的请求落到长尾地域，候选只有 1~49 条，
     * 测出来的分布与真实流量不符。改为按命中数加权后，
     * 请求分布才与真实流量一致。
     *
     * <p>这个权重是「该地域有多少广告愿意投」，等价于真实系统里
     * 「流量大的地方广告主也愿意投」的均衡状态。
     */
    private String pickRegion(ThreadLocalRandom rnd) {
        String[] pool = regionPool();
        int total = 0;
        for (String r : pool) {
            total += regionWeight(r);
        }
        int hit = rnd.nextInt(Math.max(total, 1));
        int acc = 0;
        for (String r : pool) {
            acc += regionWeight(r);
            if (hit < acc) {
                return r;
            }
        }
        return pool[rnd.nextInt(pool.length)];
    }

    private int regionWeight(String region) {
        String[] c = regionHitCounts.get(region);
        if (c == null) {
            return 1;
        }
        try {
            return Math.max(1, Integer.parseInt(c[0]));
        } catch (NumberFormatException e) {
            return 1;
        }
    }

    private String[] regionPool() {
        String[] v = targetingPool.get(String.valueOf(DIM_REGION));
        return v == null || v.length == 0 ? FALLBACK_REGIONS : v;
    }

    private String[] interestPool() {
        String[] v = targetingPool.get(String.valueOf(DIM_INTEREST));
        return v == null || v.length == 0 ? FALLBACK_INTERESTS : v;
    }

    @PostMapping("/bid")
    public Map<String, Object> bid(@RequestBody BidRequest req) {
        long t0 = System.nanoTime();
        BidRequest.Result result = bidService.bid(req);
        long wall = (System.nanoTime() - t0) / 1_000_000;

        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("code", 0);
        resp.put("msg", "ok");
        resp.put("data", result);
        // 接口整体墙钟耗时，与埋点里的 cost.total 区分开：
        // 后者是链路内部累计，前者含序列化和网络写出
        resp.put("wallMs", wall);
        return resp;
    }

    /**
     * 压测入口：一次提交 N 次竞价，返回实际 QPS、P99 与空广告率。
     *
     * <p>演示手册第 7 步用它，避免现场切 JMeter 把笔记本打满。
     * 刻意加了 {@code MAX_BATCH} 上限——演示时宁可少打也不要卡死。
     *
     * <p><b>为什么 M1 用它而不是 JMeter</b>：M1 单次竞价在数百毫秒量级，
     * JMeter 本身要占内存、自身开销会干扰测量结果。
     * 这个接口零额外依赖，测的是真实 HTTP 链路（含网络开销）。
     * JMeter 留给 M9 出正式报告——第三方工具的报告在简历上更有说服力。
     *
     * <p><b>concurrency 参数的作用</b>：P99 只在并发下才有意义，
     * 串行跑的延迟曲线没有长尾。所以压测必须固定并发档位，
     * baseline 与优化后在<b>同一并发</b>下对比才有效。
     */
    @PostMapping("/bid/batch")
    public Map<String, Object> batch(@RequestParam(defaultValue = "1000") int count,
                                     @RequestParam(defaultValue = "1") int concurrency) {
        final int MAX_BATCH = 20000;
        final int MAX_CONCURRENCY = 64;
        int n = Math.min(count, MAX_BATCH);
        int c = Math.min(Math.max(concurrency, 1), MAX_CONCURRENCY);

        // 记录每次竞价的耗时，用于算 P50/P95/P99
        int[] latencies = new int[n];
        java.util.concurrent.atomic.AtomicInteger cursor =
                new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.atomic.AtomicInteger hasAdCounter =
                new java.util.concurrent.atomic.AtomicInteger();

        long start = System.nanoTime();
        if (c == 1) {
            // 串行路径：无额外线程开销，延迟分布最纯净
            for (int i = 0; i < n; i++) {
                long t = System.nanoTime();
                boolean hit = Boolean.TRUE.equals(bidService.bid(randomRequest()).getHasAd());
                latencies[i] = (int) ((System.nanoTime() - t) / 1_000_000);
                if (hit) {
                    hasAdCounter.incrementAndGet();
                }
            }
        } else {
            java.util.concurrent.ExecutorService pool =
                    java.util.concurrent.Executors.newFixedThreadPool(c);
            List<Future<?>> futures = new ArrayList<>(c);
            for (int w = 0; w < c; w++) {
                futures.add(pool.submit(() -> {
                    int i;
                    while ((i = cursor.getAndIncrement()) < n) {
                        long t = System.nanoTime();
                        boolean hit = Boolean.TRUE.equals(bidService.bid(randomRequest()).getHasAd());
                        latencies[i] = (int) ((System.nanoTime() - t) / 1_000_000);
                        if (hit) {
                            hasAdCounter.incrementAndGet();
                        }
                    }
                }));
            }
            for (Future<?> f : futures) {
                try {
                    f.get();
                } catch (Exception ignored) {
                    // 单个工作线程异常不影响整体统计
                }
            }
            pool.shutdown();
        }
        long elapsed = System.nanoTime() - start;

        int hasAd = hasAdCounter.get();
        double qps = n / (elapsed / 1_000_000_000.0);
        double emptyRate = n == 0 ? 0 : (n - hasAd) * 100.0 / n;

        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("code", 0);
        resp.put("msg", "ok");
        resp.put("requested", n);
        resp.put("concurrency", c);
        resp.put("hitCapped", count > MAX_BATCH);
        resp.put("qps", round(qps));
        resp.put("fillRate", round(100 - emptyRate));
        resp.put("emptyRate", round(emptyRate));
        resp.put("elapsedMs", elapsed / 1_000_000);
        resp.put("p50", percentile(latencies, 50));
        resp.put("p95", percentile(latencies, 95));
        resp.put("p99", percentile(latencies, 99));
        resp.put("max", percentile(latencies, 100));
        return resp;
    }

    /**
     * 计算百分位。
     *
     * <p><b>为什么不用 Micrometer 的 Timer</b>：Timer 在多线程下要额外处理
     * 时间戳队列与 Histogram 快照，M1 阶段只需一个精确的百分位数值。
     * M9 出正式报告时改用 JMeter，它会给出完整的分布直方图。
     */
    private static int percentile(int[] arr, int p) {
        if (arr.length == 0) {
            return 0;
        }
        int[] copy = arr.clone();
        java.util.Arrays.sort(copy);
        int idx = (int) Math.ceil(p / 100.0 * copy.length) - 1;
        return copy[Math.max(0, Math.min(idx, copy.length - 1))];
    }

    private static double round(double v) {
        return Math.round(v * 100) / 100.0;
    }

    /**
     * 随机生成一个竞价请求。
     *
     * <p>分布直接对齐造数脚本的词表与权重，保证演示时看到的
     * 候选数/过滤数比例是可复现的。
     *
     * <p>地域与兴趣从 ad_target 实表里取，不在本类硬编码。
     * 原因：M1 调整造数参数时地域码从六位市码改成六位区县码
     * （110000 -> 110101），硬编码的旧值在库里不存在，
     * 会导致候选恒为 0、空广告率 100%。从库读可避免这类脱节。
     *
     * <p>性别按 5:5 拆分，类目按 Zipf 权重——不是均匀随机。
     */
    private BidRequest randomRequest() {
        String[] interests = interestPool();
        int[] ages = {24, 28, 32, 35, 40, 45, 52, 26, 30, 38};
        String[] categories = {"mobile", "fitness", "home", "beauty", "education",
                "auto", "food", "finance", "game", "travel"};
        int[] categoryWeights = {180, 130, 95, 75, 60, 48, 40, 34, 29, 25};
        String[] slots = {"feed_top", "feed_mid", "feed_bottom"};

        var rnd = ThreadLocalRandom.current();
        BidRequest req = new BidRequest();
        req.setSlotCode(slots[rnd.nextInt(slots.length)]);
        req.setRegion(pickRegion(rnd));
        req.setAge(ages[rnd.nextInt(ages.length)]);
        req.setGender(rnd.nextInt(100) < 50 ? 1 : 2);
        req.setInterests(String.join(",", pickDistinct(rnd, interests, 3)));
        req.setCategoryCode(categories[pickWeighted(rnd, categoryWeights)]);
        return req;
    }

    /** 从数组里不重复地取 n 个 */
    private static String[] pickDistinct(ThreadLocalRandom rnd, String[] pool, int n) {
        int m = Math.min(n, pool.length);
        String[] picked = new String[m];
        for (int i = 0; i < m; i++) {
            picked[i] = pool[rnd.nextInt(pool.length)];
        }
        return picked;
    }

    /** 按权重抽下标，权重量级（如 180 表示 18%）不影响相对比例 */
    private static int pickWeighted(ThreadLocalRandom rnd, int[] weights) {
        int total = 0;
        for (int w : weights) {
            total += w;
        }
        int r = rnd.nextInt(total);
        int acc = 0;
        for (int i = 0; i < weights.length; i++) {
            acc += weights[i];
            if (r < acc) {
                return i;
            }
        }
        return weights.length - 1;
    }
}
