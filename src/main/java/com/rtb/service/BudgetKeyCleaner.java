package com.rtb.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * 预算 key 定时清理（保留最近 N 天，清理更早的日期分桶 key）。
 *
 * <p><b>为什么需要它</b>：{@code budget:{adId}:{yyyyMMdd}} / {@code spent:{adId}:{yyyyMMdd}}
 * 按天分桶，但写入时（Lua 的 {@code SET}）<b>没有设 TTL</b>，旧日期的 key 会永久累积。
 * Redis 配置为 noeviction，内存打满后写命令被拒 → 预算扣减失败（可用性事故）。
 *
 * <p><b>策略</b>：每天凌晨把「保留窗口」之外的日期分桶 key 删掉（默认保留最近 2 天，
 * 即清掉「前天」及更早）。跨天 key 名天然不同，因此删除旧日期 bucket 绝不会影响当天预算。
 *
 * <p><b>为什么不阻塞 Redis</b>：
 * <ul>
 *   <li>用 {@code SCAN} 游标增量遍历，而非 {@code KEYS}——后者 O(N) 且会卡住单线程；</li>
 *   <li>用 {@code UNLINK} 异步回收，而非 {@code DEL}——后者同步释放内存，大批量删除会阻塞；</li>
 *   <li>遍历中分批（{@value #DELETE_BATCH} 个/批）提交删除，避免一次性堆出超大数组。</li>
 * </ul>
 */
@Slf4j
@Component
public class BudgetKeyCleaner {

    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("yyyyMMdd");

    /** 需要清理的两类 key 前缀（日期分桶后缀由 SCAN 后在内存中判断） */
    private static final String BUDGET_PREFIX = "budget:";
    private static final String SPENT_PREFIX = "spent:";

    /** 单次 SCAN 建议返回条数（仅 hint，Redis 不保证） */
    private static final int SCAN_COUNT = 500;
    /** 每攒够该数量即批量 UNLINK，避免一次性堆大数组 */
    private static final int DELETE_BATCH = 500;

    private final StringRedisTemplate redis;

    /** 保留天数：保留「今天」往前共 N 天，清理更早的日期分桶。默认 2（保留今天 + 昨天）。 */
    @Value("${rtb.budget.cleanup.retention-days:2}")
    private int retentionDays;

    public BudgetKeyCleaner(StringRedisTemplate redis) {
        this.redis = redis;
    }

    /**
     * 每天凌晨清理超出保留窗口的日期分桶 key。
     * cron 格式：秒 分 时 日 月 周，默认每天 01:00。
     */
    @Scheduled(cron = "${rtb.budget.cleanup.cron:0 0 1 * * ?}")
    public void cleanExpiredKeys() {
        int days = Math.max(retentionDays, 1);
        // 保留窗口最早的一天（含），早于它的日期分桶都删掉
        LocalDate oldestKept = LocalDate.now().minusDays(days - 1L);

        long total = cleanByPrefix(BUDGET_PREFIX, oldestKept)
                + cleanByPrefix(SPENT_PREFIX, oldestKept);

        log.info("预算 key 清理完成：保留最近 {} 天(start={})，本次删除 {} 个 key",
                days, oldestKept.format(DATE_FMT), total);
    }

    /** SCAN 按前缀遍历，删除日期早于 oldestKept 的 key，返回删除数量。 */
    private long cleanByPrefix(String prefix, LocalDate oldestKept) {
        long deleted = 0;
        ScanOptions options = ScanOptions.scanOptions().match(prefix + "*").count(SCAN_COUNT).build();
        try (Cursor<String> cursor = redis.scan(options)) {
            List<String> batch = new ArrayList<>(DELETE_BATCH);
            while (cursor.hasNext()) {
                String key = cursor.next();
                if (isBefore(key, oldestKept)) {
                    batch.add(key);
                    if (batch.size() >= DELETE_BATCH) {
                        deleted += unlink(batch);
                        batch.clear();
                    }
                }
            }
            if (!batch.isEmpty()) {
                deleted += unlink(batch);
            }
        } catch (Exception e) {
            // 清理失败不能影响其他调度/业务，仅记日志；下次调度会重试
            log.warn("预算 key 清理失败 prefix={}: {}", prefix, e.getMessage());
        }
        return deleted;
    }

    /** 解析 key 末尾的 {@code yyyyMMdd} 并判断是否早于 cutoff；格式不合法的 key 一律跳过。 */
    private boolean isBefore(String key, LocalDate cutoff) {
        int idx = key.lastIndexOf(':');
        if (idx < 0 || idx == key.length() - 1) {
            return false;
        }
        String date = key.substring(idx + 1);
        try {
            return LocalDate.parse(date, DATE_FMT).isBefore(cutoff);
        } catch (Exception e) {
            return false;
        }
    }

    /** 异步批量删除，返回实际删除数量。 */
    private long unlink(List<String> keys) {
        Long n = redis.unlink(keys);
        return n == null ? 0L : n;
    }
}
