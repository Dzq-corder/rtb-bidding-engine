package com.rtb.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

import com.rtb.entity.Ad;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

/**
 * 预算服务
 *
 * <p><b>M3 实现：Lua 原子扣减</b>。把「读余额 → 校验 → 扣减 → 记消耗」收进
 * 一个 Lua 脚本，由 Redis 单线程原子执行，消除 M1 两步写法的并发超扣窗口。
 *
 * <p><b>为什么 M1 没这么做</b>：M1 用 {@code GET → 比较 → decrement} 三步、两次往返，
 * 是最直观的写法。并发下请求 A、B 同时读到余额 10 元，都通过校验、各自扣 8 元，
 * 余额变成 -6 元，超扣发生。这一段「先量 baseline、再针对性加固」正是简历上
 * 「由 A 降到 B」的证据链：M1 暴露问题 → M3 用 Lua 把超扣压到 0。
 *
 * <p><b>Key 命名</b>：{@code budget:{adId}:{yyyyMMdd}} 剩余预算(分)，
 * {@code spent:{adId}:{yyyyMMdd}} 累计消耗(分)。按天分桶让过期策略自然生效。
 * 金额统一以「分」为单位存 Redis，避免 double 浮点误差。
 */
@Slf4j
@Service
public class BudgetService {

    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("yyyyMMdd");

    /**
     * 原子预算扣减脚本。
     * KEYS[1] = budget:{adId}:{date} 剩余预算(分)
     * KEYS[2] = spent:{adId}:{date}  累计消耗(分)
     * ARGV[1] = 本次扣减额(分)
     * ARGV[2] = 日预算(分)，key 缺失时用于播种
     * 返回：1 = 扣减成功；0 = 余额不足（含播种后仍不足）
     */
    private static final String DEDUCT_LUA =
            "local bal = redis.call('GET', KEYS[1])\n" +
            "if not bal then\n" +
            "  bal = ARGV[2]\n" +
            "  redis.call('SET', KEYS[1], bal)\n" +
            "  redis.call('SET', KEYS[2], 0)\n" +
            "end\n" +
            "local amt = tonumber(ARGV[1])\n" +
            "local remain = tonumber(bal)\n" +
            "if remain < amt then\n" +
            "  return 0\n" +
            "end\n" +
            "redis.call('DECRBY', KEYS[1], amt)\n" +
            "redis.call('INCRBY', KEYS[2], amt)\n" +
            "return 1\n";

    private static final RedisScript<Long> DEDUCT_SCRIPT = new DefaultRedisScript<>(DEDUCT_LUA, Long.class);

    private final StringRedisTemplate redis;

    /** 预热播种时给 key 设置的存活时长（秒），默认 2 天；与 BudgetKeyCleaner 的保留窗口互为兜底 */
    @Value("${rtb.budget.key-ttl-seconds:172800}")
    private long keyTtlSeconds;

    public BudgetService(StringRedisTemplate redis) {
        this.redis = redis;
    }

    private String budgetKey(Long adId) {
        return budgetKey(adId, LocalDate.now().format(DATE_FMT));
    }

    private String budgetKey(Long adId, String date) {
        return "budget:" + adId + ":" + date;
    }

    private String spentKey(Long adId) {
        return "spent:" + adId + ":" + LocalDate.now().format(DATE_FMT);
    }

    /**
     * 查询当前余额，不存在则按广告日预算初始化（惰性初始化，播种来自 MySQL）。
     * M3 起检索阶段会批量预热，避免竞价链路上出现初始化写。
     */
    public BigDecimal getBalance(Long adId, BigDecimal dailyBudget) {
        String v = redis.opsForValue().get(budgetKey(adId));
        if (v == null) {
            initIfAbsent(adId, dailyBudget);
            v = redis.opsForValue().get(budgetKey(adId));
        }
        return parse(v == null ? String.valueOf(toCents(dailyBudget)) : v);
    }

    /**
     * 扣减预算。M3 版本：单个 Lua 脚本原子完成「校验 → 扣减 → 记消耗」。
     *
     * <p>Redis 以单线程执行 Lua，脚本运行期间不会被其他命令打断，因此并发请求
     * 之间不存在「都读到旧余额、都通过校验」的窗口，超扣被彻底消除。
     *
     * <p><b>M7 修正（职责解耦）</b>：本方法现在<b>只做 Redis 原子扣减</b>，不再内嵌
     * MySQL 状态写。余额不足（返回 {@code false}）时由调用方（{@code BidService}）在
     * <b>脱离 Redis 超时包裹与 Redis 熔断器</b>的上下文中单独落 {@code ad.status=0}。
     * 原因：内嵌 MySQL 写会让「DB 写慢 / 异常」被误计入 Redis 耗时字段
     * （{@code billCostMs}），更糟的是会被 {@code redisBreaker.onFailure()} 误判为
     * Redis 故障从而错误打开 Redis 熔断器（职责错乱）。两种性质截然不同的操作必须分开。
     *
     * @return true 扣减成功（脚本返回 1）；false 余额不足（脚本返回 0）
     */
    public boolean deduct(Long adId, BigDecimal dailyBudget, BigDecimal amount) {
        List<String> keys = Arrays.asList(budgetKey(adId), spentKey(adId));
        Long result = redis.execute(DEDUCT_SCRIPT, keys,
                String.valueOf(toCents(amount)),
                String.valueOf(toCents(dailyBudget)));
        return result != null && result == 1L;
    }

    /**
     * 检索阶段批量预热：为本次候选惰性补建 Redis 预算 key，把「初始化写」从计费热路径
     * （Lua {@code deduct} 里的播种）提前挪到检索侧。
     *
     * <p>用 {@code SET NX}（{@code setIfAbsent}）——<b>只在 key 不存在时播种</b>，绝不覆盖当天
     * 已消耗的预算；同时带上 {@code keyTtlSeconds} 过期，与 {@link BudgetKeyCleaner} 的定时清理
     * 互为兜底（一个被动过期、一个主动扫尾）。
     *
     * <p>{@code spent} 无需在此播种：判断只看 {@code budget} key，{@code spent} 由扣减时的
     * {@code INCRBY} 按缺省 0 自动创建。
     *
     * <p>失败可容忍：预热失败时计费阶段的 Lua 仍会惰性播种，不影响正确性，故由调用方包 try-catch。
     */
    public void warmBudgets(List<Ad> candidates) {
        if (candidates == null || candidates.isEmpty()) {
            return;
        }
        String date = LocalDate.now().format(DATE_FMT);
        Duration ttl = Duration.ofSeconds(keyTtlSeconds);
        redis.executePipelined(new SessionCallback<Object>() {
            @Override
            @SuppressWarnings({"rawtypes", "unchecked"})
            public Object execute(RedisOperations operations) {
                ValueOperations<String, String> ops = operations.opsForValue();
                for (Ad ad : candidates) {
                    if (ad == null || ad.getId() == null || ad.getDailyBudget() == null) {
                        continue;   // 防御：字段缺失的候选跳过，交给计费时惰性兜底
                    }
                    ops.setIfAbsent(budgetKey(ad.getId(), date),
                            String.valueOf(toCents(ad.getDailyBudget())), ttl);
                }
                return null;
            }
        });
    }

    /** 金额统一以「分」为单位存进 Redis，避免 double 的浮点误差 */
    private static long toCents(BigDecimal yuan) {
        return yuan.movePointRight(2).longValue();
    }

    /** 分 -> 元 */
    private static BigDecimal parse(String cents) {
        return new BigDecimal(cents).movePointLeft(2);
    }

    private void initIfAbsent(Long adId, BigDecimal dailyBudget) {
        redis.opsForValue().setIfAbsent(budgetKey(adId), String.valueOf(toCents(dailyBudget)));
    }

    /**
     * 预算是否耗尽，用于日志与调试。
     */
    public boolean isExhausted(Long adId, BigDecimal dailyBudget) {
        return getBalance(adId, dailyBudget).signum() <= 0;
    }
}
