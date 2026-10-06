package com.rtb.index;

import com.rtb.ad.Ad;
import com.rtb.ad.AdMapper;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 内存倒排索引 · M2
 *
 * <p><b>解决什么问题</b>：M1 每次竞价都要在 50 万条广告里做多维 JOIN，
 * 实测检索耗时 38.9 ms、占总耗时 92.1%。M2 把定向条件全量加载进内存，
 * 求交变成纯内存运算，一次数据库都不查。
 *
 * <p><b>为什么用 int 数组而不是 BitSet</b>：这是本设计最关键的一个取舍。
 *
 * <p>直觉方案是「每个定向值一个 BitSet，位宽 = 广告总数（50 万）」。
 * 但实测地域维度有 4,748 个取值，4,748 × 500,000 bit = <b>283 MB</b>，
 * 光一个维度就吃掉近 300MB 堆。
 *
 * <p>而实际数据分布是：<b>每个地域平均只对应 315 条广告</b>。
 * 稀疏到这个程度，位图的优势（密集位运算）完全用不上，
 * 而固定 50 万位的开销却是实打实的。
 *
 * <p>所以改用<b>「维度 + 取值 → 广告 ID 有序数组」</b>：
 * 596 万条 int + 数组开销约 15 MB，是 BitSet 方案的 1/19。
 *
 * <p><b>求交复杂度</b>：O(小集合 × log 大集合)，用二分查找而不是双指针全扫。
 * 典型场景：地域 315 条 ∩ 年龄段 8 万条 ∩ 性别 26 万条，
 * 双指针要扫 8 万 + 26 万次，二分只需约 315 × 17 + 315 × 18 次。
 */
@Slf4j
@Component
public class InvertedIndex {

    /** 倒排表的一行，MyBatis 映射用 */
    public record TargetRow(Long id, Integer dimType, String dimValue, Long adId) {
    }

    /**
     * 每条广告的定向条件条数 · M5
     *
     * <p>加载索引时顺手统计，供兜底广告池排序用——
     * <b>定向越宽泛的广告，越适合做熔断兜底</b>。
     *
     * <p>为什么放在内存算而不是 SQL 查：JOIN 596 万行 ad_target 做 GROUP BY
     * 会超时（实测报 MySQLTimeoutException），而索引加载时本来就把每行读进来了，
     * 顺手计数几乎零成本。
     */
    private final Map<Long, Integer> adTargetCount = new ConcurrentHashMap<>();

    /**
     * 反向表：广告 ID -&gt; 它命中的全部索引 key · M9
     *
     * <p><b>为什么必须有这张表</b>：
     * 正向表是「取值 → 广告数组」，方向天然是「给定条件找广告」。
     * 但管理端要回答的是反过来的问题：<b>「我改了广告 X 的定向，
     * 它原来命中了哪些 key？该从哪些数组里把它摘掉？」</b>
     * 没有这张表就只能遍历全部 4,844 个 key 去反查，
     * 为了改 1 条定向做 4,844 次比较——能跑，但明显是错工具。
     *
     * <p>这也是「正向索引必然需要反向表」的一个具体例子：
     * 写入时按正向键组织，读入时却经常要按反向键提问。
     *
     * <p>key 格式 "{dimType} {dimValue}"，与 {@link #key} 一致。
     * 值为不可变集合，增删都靠整体替换而非原地改。
     */
    private final Map<Long, Set<String>> adTargetKeys = new ConcurrentHashMap<>();

    /** 某条广告的定向条件条数；不在索引里返回 0 */
    public int targetCountOf(Long adId) {
        return adTargetCount.getOrDefault(adId, 0);
    }

    /** dim_type 常量，与 ad_target 对齐 */
    public static final int DIM_REGION = 1;
    public static final int DIM_AGE = 2;
    public static final int DIM_GENDER = 3;
    public static final int DIM_INTEREST = 4;

    /** 索引是否就绪。未就绪时请求应降级到 DB 查询 */
    private volatile boolean ready = false;

    /**
     * 索引主体：key = "维度:取值"，value = 该取值命中的广告 ID（有序去重）
     *
     * <p>用嵌套 Map 而非拼接字符串：拼接会产生 4,844 个临时 String，
     * 嵌套 Map 零字符串分配。
     *
     * <p><b>为什么是 ConcurrentHashMap（M9 起）</b>：
     * 原本是 HashMap，启动后只读所以安全。M9 加了「改定向即时生效」后，
     * 投放端会在竞价线程读的同时往里 put——而 HashMap 在 put 触发扩容时
     * 可能让并发 get 死循环（著名的 HashMap 扩容 race）。
     * <b>这不是「理论上有风险」，是真实会挂的故障模式</b>，
     * 演示时改一次定向就可能让整个竞价接口卡住。
     */
    private final Map<Integer, Map<String, int[]>> index = new ConcurrentHashMap<>();

    /** 同上，adCache 也要并发安全：改预算会替换其中的 Ad 对象 */
    private final Map<Long, Ad> adCache = new ConcurrentHashMap<>();

    /**
     * 加载期的全局累积区：key -> 广告 ID 集合。
     *
     * <p>必须在全部数据读完后才 freeze 成数组，原因见 indexBatch 的注释。
     */
    private Map<String, Set<Integer>> pending = new HashMap<>();

    /** 加载耗时（毫秒），M2 指标之一 */
    private volatile long loadCostMs = 0;

    private final AdMapper adMapper;

    public InvertedIndex(AdMapper adMapper) {
        this.adMapper = adMapper;
    }

    /**
     * 启动时全量加载。
     *
     * <p>596 万行一次性 SELECT 会占用较大内存与连接时间，
     * 因此<b>分批加载</b>：每批 20 万行，避免单次结果集过大导致 OOM。
     */
    @PostConstruct
    public void load() {
        long t0 = System.currentTimeMillis();
        try {
            // 1. 广告详情入内存（50 万条）
            List<Ad> ads = adMapper.loadAllAds();
            for (Ad a : ads) {
                adCache.put(a.getId(), a);
            }
            log.info("已缓存广告 {} 条", ads.size());

            // 2. 定向条件分批加载（596 万行）
            //用游标分页而非一次性 SELECT 全部 596 万行，
            // 避免单次结果集过大导致 OOM。
            int total = 0;
            final int BATCH = 200_000;
            int lastId = 0;
            while (true) {
                List<TargetRow> batch = adMapper.selectTargetingBatch(lastId, BATCH);
                if (batch == null || batch.isEmpty()) {
                    break;
                }
                total += indexBatch(batch);
                lastId = batch.get(batch.size() - 1).id().intValue();
                log.info("  倒排索引加载进度：{} 条", total);                if (batch.size() < BATCH) {
                    break;
                }
            }

            // 3. 全部数据读完后，统一冻结成有序数组
            freeze();

            this.loadCostMs = System.currentTimeMillis() - t0;
            this.ready = true;
            log.info("倒排索引加载完成：{} 条定向记录，{} 个索引键，耗时 {} ms",
                    total, keyCount(), loadCostMs);
        } catch (Exception e) {
            log.error("倒排索引加载失败，候选检索将降级到数据库", e);
            this.ready = false;
        }
    }

    /**
     * 把一批记录并入索引。
     *
     * <p><b>踩过的坑</b>：最初每批各自建HashSet 然后<b>直接 put覆盖</b>，
     * 结果索引里的广告数只有实际的 1/40（地域 5104 只有 27 条，实际 1162 条）。
     *
     * <p>原因是 ad_target 一行只存一个 ad_id，
     * 同一条广告的多条定向条件在表里是<b>不连续</b>的——
     * 20 万行一批切下去，同一个 ad_id 的不同条件落在不同批里，
     * 各批的 HashSet 互相看不见，合并时被后写的覆盖。
     *
     * <p>正确做法：全局累积到一个 Map，待全部数据读完后<b>统一转成数组</b>。
     * 这样跨批的广告 ID 会在同一个 Set 里累积，不会丢。
     */
    private int indexBatch(List<TargetRow> batch) {
        for (TargetRow r : batch) {
            pending.computeIfAbsent(r.dimType() + ":" + r.dimValue(), k -> new HashSet<>())
                    .add(r.adId().intValue());
            // 顺手统计每广告的定向条数，供 M5 兜底池排序
            adTargetCount.merge(r.adId(), 1, Integer::sum);
            // M9：同时登记反向表。加载期单线程，add 后即可；
            // 增量更新期（add/removeTargeting）会整体替换成新集合
            adTargetKeys.computeIfAbsent(r.adId(), k -> ConcurrentHashMap.newKeySet())
                    .add(key(r.dimType(), r.dimValue()));
        }
        return batch.size();
    }

    /**
     * 全部数据读完后，把累积的 Set 转成有序数组。
     *
     * <p>此时才做去重与排序，是因为 Set 只有在见到全部数据后才是完整的。
     */
    private void freeze() {
        pending.forEach((key, set) -> {
            int[] arr = set.stream().mapToInt(Integer::intValue).sorted().toArray();
            int split = key.indexOf(':');
            int dimType = Integer.parseInt(key.substring(0, split));
            index.computeIfAbsent(dimType, k -> new ConcurrentHashMap<>())
                    .put(key.substring(split + 1), arr);
        });
        int totalAds = pending.values().stream().mapToInt(Set::size).sum();
        log.info("冻结索引：{} 个键，去重后共{} 条广告引用", pending.size(), totalAds);
        pending = null;
    }

    private int keyCount() {
        return index.values().stream().mapToInt(Map::size).sum();
    }

    public boolean isReady() {
        return ready;
    }

    public long getLoadCostMs() {
        return loadCostMs;
    }

    public Map<Integer, Integer> getIndexShape() {
        Map<Integer, Integer> shape = new HashMap<>();
        index.forEach((dim, m) -> shape.put(dim, m.size()));
        return shape;
    }

    /** 某个定向值命中的广告数，供诊断接口与数据库 COUNT 对照 */
    public int hitCount(int dimType, String dimValue) {
        Map<String, int[]> dimMap = index.get(dimType);
        if (dimMap == null) {
            return -1;
        }
        int[] arr = dimMap.get(dimValue);
        return arr == null ? 0 : arr.length;
    }

    // =================================================================
    // 增量更新 · M9
    //
    // 「改预算 / 改定向，保存即生效」这件事的全部难点都在这里。
    //
    // 【为什么不能只改数据库】
    // M2 把广告详情与 596 万条定向全量缓存进了内存。改 MySQL 之后：
    //   · 改预算 → adCache 里的 Ad.dailyBudget 还是旧值，
    //     而 BidService 扣费用的是缓存对象的字段，界面改了库却没改行为；
    //   · 改定向 → 倒排表里那条广告还在，下次竞价照样命中。
    // 这就是典型的「缓存一致性」问题。真实系统里的选择有三条：
    //   1. 全量重建索引（M2 实测要几十秒，演示时不可接受）
    //   2. 写时复制整个索引（内存翻倍，且此刻所有请求都得等）
    //   3. 增量修改本条广告涉及的少量 key（本项目选这条）
    //
    // 【第三条为什么便宜】
    // 一条广告平均 10 个定向项，只影响 10 个 key。
    // 每个 key 的数组平均 315 条（地域维度最坏 7,900），
    // 增量改一次就是重排一个几百元素的数组——微秒级。
    // 换来的性质是「改完立刻生效，不用重启，也不用等重建」。
    //
    // 【不做什么】
    // 不做数据库与内存的双写补偿、不做变更日志。
    // 因为本项目的写入方只有演示后台，量级是分钟级几次，
    // 上「binlog 订阅 + 补偿」属于过度设计。
    // 真实系统里这一层是必须的，面试时要主动说清这个前提。
    // =================================================================

    /**
     * 替换广告详情缓存，使改预算 / 改出价立刻生效。
     *
     * <p><b>用 replaceAd 而不是 put</b>：Ad 对象是 Lombok @Data 生成的，
     * 直接 put 一个改了字段的旧对象，JVM 里可能还有别的线程持有引用。
     * 整体替换比原地改字段更安全，也更容易推理。
     */
    public void replaceAd(Ad ad) {
        if (ad == null || ad.getId() == null) {
            return;
        }
        if (ad.getStatus() != null && ad.getStatus() != 1) {
            // 暂停/已耗尽的广告不参与竞价，缓存它就是浪费内存
            adCache.remove(ad.getId());
        } else {
            adCache.put(ad.getId(), ad);
        }
    }

    /** 从缓存取广告详情，供管理端回显 */
    public Ad cachedAd(Long adId) {
        return adCache.get(adId);
    }

    /**
     * 给某条广告<b>新增</b>一条定向条件。
     *
     * <p>幂等：已存在时直接返回 false，不重复插入。
     * 演示时后台可能重复点「保存」，插入侧必须能扛住。
     */
    public boolean addTargeting(Long adId, int dimType, String dimValue) {
        int id = adId.intValue();
        Map<String, int[]> dimMap = index.computeIfAbsent(dimType, k -> new ConcurrentHashMap<>());
        int[] cur = dimMap.get(dimValue);
        if (cur != null && Arrays.binarySearch(cur, id) >= 0) {
            return false;
        }
        int[] next;
        if (cur == null) {
            next = new int[]{id};
        } else {
            int pos = Arrays.binarySearch(cur, id);
            if (pos >= 0) {
                return false;
            }
            int insertAt = -(pos + 1);
            next = new int[cur.length + 1];
            System.arraycopy(cur, 0, next, 0, insertAt);
            next[insertAt] = id;
            System.arraycopy(cur, insertAt, next, insertAt + 1, cur.length - insertAt);
        }
        // 【为什么可以安全地整体替换数组】
        // 竞价线程读的是 dimMap.get() 拿到的引用，替换引用不影响它继续用旧数组。
        // int[] 内部不会被就地修改，所以「读旧数组」永远是读到一个自洽的快照。
        // 这比给索引加读写锁便宜得多——锁会让每次竞价都可能被后台写入阻塞。
        dimMap.put(dimValue, next);
        adTargetCount.merge(adId, 1, Integer::sum);
        adTargetKeys.computeIfAbsent(adId, k -> ConcurrentHashMap.newKeySet())
                .add(key(dimType, dimValue));
        return true;
    }

    /**
     * 移除某条广告的一条定向条件。
     *
     * <p>返回被移除前该 key 下的广告数，供管理端展示。
     */
    public int removeTargeting(Long adId, int dimType, String dimValue) {
        Map<String, int[]> dimMap = index.get(dimType);
        if (dimMap == null) {
            return 0;
        }
        int[] cur = dimMap.get(dimValue);
        if (cur == null) {
            return 0;
        }
        int id = adId.intValue();
        int pos = Arrays.binarySearch(cur, id);
        if (pos < 0) {
            return cur.length;
        }
        int[] next = new int[cur.length - 1];
        System.arraycopy(cur, 0, next, 0, pos);
        System.arraycopy(cur, pos + 1, next, pos, cur.length - pos - 1);
        if (next.length == 0) {
            // 整个 key 空了：删掉，否则会留下一个永远命中不到广告的死 key
            dimMap.remove(dimValue);
        } else {
            dimMap.put(dimValue, next);
        }
        adTargetCount.computeIfPresent(adId, (k, v) -> v <= 1 ? null : v - 1);
        adTargetKeys.computeIfPresent(adId, (k, set) -> {
            set.remove(key(dimType, dimValue));
            return set.isEmpty() ? null : set;
        });
        return cur.length;
    }

    /**
     * 用数据库里的真实定向全量替换该广告的定向条件。
     *
     * <p>后台「保存定向」走这条而不是 add/remove 组合：
     * 先读库里现状，算出与内存索引的差集，只改动真正变化的部分。
     *
     * <p><b>靠 {@code adTargetKeys} 这张反向表算差集</b>。
     * 没有它就只能「把这个广告可能命中的所有 key 都找一遍」，
     * 而 key 总数 4,844、且无法反查某条广告落在哪些 key 上——
     * 为了改 1 条定向去遍历全部 key 属于用错工具。
     *
     * <p><b>分隔符用空格而不是冒号</b>：定向值本身可能含冒号
     * （造数里的兴趣标签是中文，不含；但维度键一旦扩展就难说），
     * 空格的歧义风险更低，且 {@code indexOf(' ')} 只查第一个，
     * 不会出现「值里有分隔符」导致的错解析。
     *
     * @param current 库里现有的定向项
     * @return 变更摘要，供接口回显，也让演示能说出「改了几条」
     */
    public Map<String, Object> syncTargeting(Long adId, List<TargetRow> current) {
        Set<String> want = new HashSet<>();
        for (TargetRow r : current) {
            want.add(key(r.dimType(), r.dimValue()));
        }
        Set<String> have = adTargetKeys.getOrDefault(adId, Set.of());

        int added = 0;
        for (String k : want) {
            if (!have.contains(k)) {
                int sp = k.indexOf(' ');
                if (addTargeting(adId, Integer.parseInt(k.substring(0, sp)), k.substring(sp + 1))) {
                    added++;
                }
            }
        }
        int removed = 0;
        for (String k : have) {
            if (!want.contains(k)) {
                int sp = k.indexOf(' ');
                removeTargeting(adId, Integer.parseInt(k.substring(0, sp)), k.substring(sp + 1));
                removed++;
            }
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("want", want.size());
        out.put("had", have.size());
        out.put("added", added);
        out.put("removed", removed);
        out.put("effective", added + removed);
        return out;
    }

    /** 某条广告当前的定向项集合（形如 "1 2301"），供管理端回显 */
    public Set<String> targetingKeysOf(Long adId) {
        return adTargetKeys.getOrDefault(adId, Set.of());
    }

    private static String key(int dimType, String dimValue) {
        return dimType + " " + dimValue;
    }

    /**
     * 逐维求交并记录每一步的结果，供诊断接口使用。
     *
     * <p>刻意保留每一步的中间集合大小——M2 排查时发现
     * 「数据库 JOIN 得出 233 条、内存求交得出 0 条」，
     * 靠的就是这个中间过程才能定位到是哪一步丢的。
     */
    public Map<String, Object> debugIntersect(String region, String ageBucket, String gender) {
        Map<String, Object> out = new LinkedHashMap<>();
        Map<String, Integer> steps = new LinkedHashMap<>();

        int[] result = null;
        if (region != null) {
            result = arrayOf(DIM_REGION, region);
            steps.put("地域 " + region, result == null ? -1 : result.length);
        }
        if (ageBucket != null) {
            int[] next = arrayOf(DIM_AGE, ageBucket);
            steps.put("年龄 " + ageBucket, next == null ? -1 : next.length);
            result = merge(result, next);
            steps.put("  ∩ 结果", result == null ? -1 : result.length);
        }
        if (gender != null) {
            int[] next = arrayOf(DIM_GENDER, gender);
            steps.put("性别 " + gender, next == null ? -1 : next.length);
            result = merge(result, next);
            steps.put("  ∩ 结果", result == null ? -1 : result.length);
        }

        out.put("steps", steps);
        out.put("finalSize", result == null ? 0 : result.length);
        if (result != null && result.length > 0) {
            out.put("sampleAdIds", Arrays.copyOf(result, Math.min(5, result.length)));
            out.put("命中缓存的广告数", Arrays.stream(result)
                    .mapToObj(id -> adCache.get((long) id)).filter(java.util.Objects::nonNull).count());
        }
        return out;
    }

    private int[] arrayOf(int dim, String value) {
        Map<String, int[]> m = index.get(dim);
        return m == null ? null : m.get(value);
    }

    private static int[] merge(int[] a, int[] b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        return gallop(a, b);
    }

    /**
     * 按多维条件求交，返回候选广告。
     *
     * @param conditions 键为 "维度:取值"，值为广告 ID 数组
     * @return 按出价降序排的候选广告
     *
     * <p><b>返回的 List 一定可修改（mutable），包括空结果时。</b>
     * 调用方 {@code BidService} 会对结果做 {@code removeIf}（按类目收窄）
     * 和 {@code sort}，若这里返回 {@code List.of()} 这类不可变列表，
     * 会抛 {@code UnsupportedOperationException}。
     *
     * <p><b>这个坑是 M5 压测时踩出来的</b>：空候选恰好是「返回不可变空列表」
     * 这条分支，而它只在候选为 0 时触发，样本极少，
     * 正常流程测不出来。压测 5000 次里碰到 4 次，全被异常兜底吃掉——
     * <b>降级机制救了业务，但根因没修，填充率会平白掉一截</b>。
     */
    public List<Ad> intersect(List<String[]> conditions) {
        int[] result = null;
        // 关键：先取最小的集合做种子，再用 galloping 求交。
        // 因为小集合 ∩ 大集合 比 大集合 ∩ 小集合 便宜得多。
        for (String[] c : conditions) {
            int dim = Integer.parseInt(c[0]);
            Map<String, int[]> dimMap = index.get(dim);
            if (dimMap == null) {
                return new ArrayList<>(0);
            }
            int[] arr = dimMap.get(c[1]);
            if (arr == null || arr.length == 0) {
                return new ArrayList<>(0);
            }
            result = (result == null) ? arr : gallop(result, arr);
            if (result.length == 0) {
                return new ArrayList<>(0);
            }
        }

        List<Ad> ads = new ArrayList<>(result.length);
        for (int adId : result) {
            Ad a = adCache.get((long) adId);
            if (a != null) {
                ads.add(a);
            }
        }
        ads.sort(Comparator.comparing(Ad::getBid).reversed());
        return ads;
    }

    /**
     * 两个有序数组求交。
     *
     * <p>朴素双指针是 O(n+m)，但真实场景两个集合大小悬殊
     * （地域 315 条 vs 性别 26 万条），双指针会白扫 26 万次。
     *
     * <p>这里以较小的 a 为基准，在较大的 b 里二分定位：
     * 复杂度降到 O(|a| x log |b|)。以实际数据计算——
     * 地域 315 条 ∩ 性别 26 万条，双指针要比较 26 万次，
     * 二分只需 315 x 18 ≈ 5,700 次，**快 45 倍**。
     */
    private static int[] gallop(int[] a, int[] b) {
        if (a.length > b.length) {
            // 保证 a 是较小的那个
            int[] t = a;
            a = b;
            b = t;
        }
        int[] out = new int[Math.min(a.length, b.length)];
        int n = 0;
        for (int x : a) {
            int lo = lowerBound(b, x, 0, b.length);
            if (lo < b.length && b[lo] == x) {
                out[n++] = x;
            }
        }
        return n == out.length ? out : Arrays.copyOf(out, n);
    }

    private static int lowerBound(int[] arr, int target, int from, int to) {
        int lo = from;
        int hi = to;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (arr[mid] < target) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }
}
