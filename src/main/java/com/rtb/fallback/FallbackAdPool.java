package com.rtb.fallback;

import com.rtb.ad.Ad;
import com.rtb.ad.AdMapper;
import com.rtb.index.InvertedIndex;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 兜底广告池 · M5
 *
 * <p><b>为什么必须有兜底广告</b>
 *
 * <p>RTB 里返回空广告 = <b>这次流量机会直接浪费</b>。
 * 流量方花钱买来的请求，白白丢掉；
 * 而且这次竞价通常还有二次请求机会（SSP 会重试），
 * 第一次返回空就可能直接被判为「无填充能力」，后续流量被降权。
 *
 * <p>所以生产环境的原则是：<b>宁可返回一条不精准的广告，也不要返回空</b>。
 *
 * <p><b>兜底广告怎么选 —— 三个条件缺一不可</b>
 *
 * <ol>
 *   <li><b>预算充足</b>：熔断时如果扣费失败就白送；预算太紧的扣了会变负数</li>
 *   <li><b>命中宽泛定向</b>：定向条件越多越容易被过滤掉。
 *       熔断时我们没时间做完整匹配，只能靠预筛</li>
 *   <li><b>出价低</b>：<b>这是最关键的一条</b>。
 *       熔断往往发生在系统压力大的时候，此时若还扣高价广告的钱，
 *       等于在最不该扣的时候扣最多的钱。选低价广告把风险压到最低</li>
 * </ol>
 *
 * <p><b>为什么不能选高消耗的</b>：熔断时计费可能已经超预算或 Redis 不可用，
 * 高价广告的扣费要么失败（白送曝光，损失曝光收益）
 * 要么成功（在高负载时扣走大额预算，广告主体验差）。
 * 低价广告把这两类损失都压到最小。
 *
 * <p><b>缓存策略</b>：兜底广告 ID 列表缓存在 Redis + 本地内存双层。
 * 因为熔断时正是系统最忙的时候，<b>不能再去查数据库</b>——
 * 查库本身就是要超时的那个原因。
 */
@Slf4j
@Component
public class FallbackAdPool {

    /** 兜底广告数量。太多则不够「兜底」，太少则可能重复命中导致预算集中消耗 */
    private static final int POOL_SIZE = 50;

    /**
     * 候选集放大倍数。
     *
     * <p>SQL 只按「在投+ 低出价 + 高预算」过滤并 LIMIT 一个放大后的数量，
     * 然后<b>在内存里按定向条数排序</b>取前 POOL_SIZE 条。
     *
     * <p><b>为什么不在 SQL 里排序</b>：试过 {@code ORDER BY (SELECT COUNT(*) FROM ad_target
     * WHERE ad_id=a.id)} 和 {@code LEFT JOIN ad_target GROUP BY}，
     * 两种写法在 596 万行的 ad_target 上都直接超时。
     * 而 M2 的倒排索引加载时已经统计了每条广告的定向条数，
     * <b>直接读内存即可，零查询成本</b>。
     */
    private static final int CANDIDATE_MULTIPLIER = 20;

    /** 本地缓存有效期（秒）。兜底数据不需要实时，过期了就刷新 */
    private static final long LOCAL_TTL_SECONDS = 300;

    private static final String REDIS_KEY = "fallback:ad_ids";

    private final AdMapper adMapper;
    private final StringRedisTemplate redis;
    private final InvertedIndex index;

    /** 本地兜底列表 —— 熔断时读内存，绝不碰外部 */
    private volatile List<Long> localPool = List.of();

    /** 本地缓存过期时间戳 */
    private volatile long localExpireAt = 0L;

    public FallbackAdPool(AdMapper adMapper, StringRedisTemplate redis,
                          InvertedIndex index) {
        this.adMapper = adMapper;
        this.redis = redis;
        this.index = index;
    }

    /**
     * 启动时异步加载兜底池。
     *
     * <p><b>为什么异步而不是 @PostConstruct 同步</b>：
     * 这个查询要 JOIN 596 万行的 ad_target 做聚合，
     * 同步执行会拖慢启动几秒。而兜底池只在熔断时才用到，
     * 启动早期本来就还没有流量——异步加载完之前由 pick() 兜住。
     */
    @PostConstruct
    public void init() {
        Thread t = new Thread(() -> {
            try {
                refresh();
            } catch (Exception e) {
                log.error("兜底广告池初始化失败，熔断降级不可用：{}", e.getMessage());
            }
        }, "fallback-pool-init");
        t.setDaemon(true);
        t.start();
    }

    /**
     * 刷新兜底广告池。
     *
     * <p>筛选 SQL 见 {@code AdMapper.findFallbackAds}：
     * <ul>
     *   <li>status = 1 在投</li>
     *   <li>出价在低档（不超过 3 元）—— 压低熔断时的扣费风险</li>
     *   <li>日预算 10000 元档 —— 预算充足，不会扣成负数</li>
     *   <li>按定向条件数升序 —— 定向越宽泛越容易被选中兜底</li>
     * </ul>
     */
    public void refresh() {
        try {
            // 优先读 Redis（可能有多个实例共享）
            String cached = redis.opsForValue().get(REDIS_KEY);
            if (cached != null && !cached.isBlank()) {
                localPool = parseIds(cached);
                localExpireAt = System.currentTimeMillis() + LOCAL_TTL_SECONDS * 1000;
                log.info("兜底广告池从 Redis 加载，{} 条", localPool.size());
                return;
            }

            // Redis 没有则查库，并写回 Redis 供其他实例复用
            log.info("开始从 MySQL 加载兜底广告池...");
            long t0 = System.currentTimeMillis();
            List<Ad> ads = adMapper.findFallbackAds(POOL_SIZE * CANDIDATE_MULTIPLIER);
            log.info("兜底候选查询完成，耗时 {} ms，返回 {} 条",
                    System.currentTimeMillis() - t0, ads == null ? 0 : ads.size());

            // 在内存里按「定向条数升序」排序，取前 POOL_SIZE 条。
            // 定向越宽泛越容易在紧急时刻被选中兜底。
            List<Ad> sorted = new ArrayList<>(ads == null ? List.of() : ads);
            sorted.sort(Comparator.comparingInt(
                    a -> index.targetCountOf(a.getId())));

            List<Long> ids = new ArrayList<>(Math.min(POOL_SIZE, sorted.size()));
            for (int i = 0; i < sorted.size() && ids.size() < POOL_SIZE; i++) {
                ids.add(sorted.get(i).getId());
            }
            localPool = ids;
            localExpireAt = System.currentTimeMillis() + LOCAL_TTL_SECONDS * 1000;
            redis.opsForValue().set(REDIS_KEY, joinIds(ids), Duration.ofHours(24));
            log.info("兜底广告池刷新完成，{} 条", ids.size());
        } catch (Exception e) {
            this.lastError = e.getClass().getName() + ": " + e.getMessage();
            log.error("刷新兜底广告池失败，沿用本地缓存 cacheSize={}: {}",
                    localPool.size(), e.getMessage(), e);
        }
    }

    /**
     * 随机取一条兜底广告。
     *
     * <p><b>为什么是随机而不是取第一条</b>：固定取第一条会让同一条广告被反复消耗，
     * 几分钟内就把它的预算扣光，然后熔断时连兜底都没有了。
     * 随机能在池子里均匀分摊。
     *
     * <p><b>本方法绝不能抛异常</b>：熔断时它是最后一道防线，
     * 这里抛异常等于让整个降级链路失效。
     *
     * @return 兜底广告 ID；池子为空时返回 null
     */
    public Long pick() {
        List<Long> pool = localPool;
        if (pool.isEmpty()) {
            // 池子空：可能 Redis 挂了导致初始化失败
            if (System.currentTimeMillis() > localExpireAt) {
                try {
                    refresh();
                } catch (Exception ignore) {
                    // 刷新也失败，只能返回 null
                }
            }
            pool = localPool;
            if (pool.isEmpty()) {
                return null;
            }
        }
        return pool.get(ThreadLocalRandom.current().nextInt(pool.size()));
    }

    /** 取兜底广告详情，可能为 null（广告已被删或状态变更） */
    public Ad resolve(Long adId) {
        if (adId == null) {
            return null;
        }
        return adMapper.findById(adId);
    }

    /** 兜底广告是否预算充足——扣费前要再确认一次 */
    public boolean hasEnoughBudget(Ad ad, BigDecimal amount) {
        return ad != null && ad.getDailyBudget() != null
                && ad.getDailyBudget().compareTo(amount) >= 0;
    }

    public int size() {
        return localPool.size();
    }

    /** 最近一次刷新失败的原因，供诊断端点读取 */
    private volatile String lastError = null;

    public String getLastError() {
        return lastError;
    }

    private List<Long> parseIds(String s) {
        List<Long> ids = new ArrayList<>();
        for (String part : s.split(",")) {
            try {
                if (!part.isBlank()) {
                    ids.add(Long.parseLong(part.trim()));
                }
            } catch (NumberFormatException ignore) {
                // 单个 id 格式错误不影响整体
            }
        }
        return ids;
    }

    private String joinIds(List<Long> ids) {
        StringBuilder sb = new StringBuilder();
        for (Long id : ids) {
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append(id);
        }
        return sb.toString();
    }
}
