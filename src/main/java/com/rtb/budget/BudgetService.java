package com.rtb.budget;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * 预算服务 · M3 原子扣减
 *
 * <p><b>M1 版本的缺陷（本类已修掉）</b>
 *
 * <p>M1 的扣减是「读余额 → 校验 → 扣减」三条独立命令、两次往返。
 * 并发下的错位场景：
 * <pre>
 *   请求 A、B 同时读到余额 10 元
 *   两个都通过校验（10 ≥ 8）
 *   各自扣 8 元
 *   结果余额变成 -6 元 —— 超扣
 * </pre>
 *
 * <p>这个问题的暴露过程值得讲：M1 的压测填充率正常，因为单线程串行跑时
 * 三步之间没有插队的窗口。**必须上并发才会复现**。
 *
 * <p><b>M3 的解法</b>：把三步收进一个 Lua 脚本。
 * Redis 执行 Lua 期间不会处理其他命令，因此不存在中间态。
 *
 * <p><b>顺带的性能收益</b>：网络往返从 2 次降到 1 次。
 * M2 之后计费占了 76% 的耗时（两次Redis 往返的固定成本），
 * 所以这一轮既是正确性修复也是延迟优化。
 *
 * <p>Key 命名：{@code budget:{adId}:{yyyyMMdd}}，按天分桶让过期策略自然生效。
 */
@Slf4j
@Service
public class BudgetService {

    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("yyyyMMdd");

    /** 余额不足的返回码，与 Lua 脚本约定一致 */
    private static final long NOT_ENOUGH = -1L;
    private static final long INIT_FAILED = -2L;

    private final StringRedisTemplate redis;
    private final RedisScript<Long> deductScript;

    public BudgetService(StringRedisTemplate redis) {
        this.redis = redis;
        this.deductScript = buildScript();
    }

    @PostConstruct
    private void warmScript() {
        // 提前加载脚本并算好 SHA1。Redis 的 EVALSHA 会用这个 SHA 去缓存里找脚本，
        // 首次执行时若本地没有才回源加载。预热可避免首个请求多一次往返。
        try {
            redis.execute(deductScript, List.of("warmup:key"), "0", "0");
            log.info("预算扣减 Lua 脚本已预热");
        } catch (Exception e) {
            log.warn("预算扣减 Lua 脚本预热失败，将在首次调用时加载：{}", e.getMessage());
        }
    }

    @SuppressWarnings("rawtypes")
    private RedisScript<Long> buildScript() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("lua/deduct_budget.lua"));
        script.setResultType(Long.class);
        return script;
    }

    private String key(Long adId) {
        return "budget:" + adId + ":" + LocalDate.now().format(DATE_FMT);
    }

    /**
     * 查询当前余额，不存在则按广告日预算初始化。
     *
     * <p>惰性初始化：Redis 里没有就查 MySQL 拿日预算写进去。
     * M2 之后这一步仍在竞价链路上，属于可优化项。
     */
    public BigDecimal getBalance(Long adId, BigDecimal dailyBudget) {
        String v = redis.opsForValue().get(key(adId));
        if (v == null) {
            initIfAbsent(adId, dailyBudget);
            v = redis.opsForValue().get(key(adId));
        }
        return parse(v == null ? String.valueOf(toCents(dailyBudget)) : v);
    }

    /**
     * 扣减预算 —— 原子操作。
     *
     * <p>「校验余额够不够」和「扣减」必须原子完成，
     * 否则并发下会超扣。见类注释里的错位场景。
     *
     * @return true 扣减成功；false 余额不足
     */
    public boolean deduct(Long adId, BigDecimal dailyBudget, BigDecimal amount) {
        String k = key(adId);
        Long remain = redis.execute(deductScript,
                List.of(k),
                String.valueOf(toCents(amount)),
                String.valueOf(toCents(dailyBudget)));

        if (remain == null) {
            // 脚本执行异常，按扣减失败处理。
            // 宁可少扣一次让广告主重投，也不能在不确定状态下返回成功。
            log.error("预算扣减脚本返回空，adId={} amount={}", adId, amount);
            return false;
        }
        if (remain == NOT_ENOUGH) {
            return false;
        }
        if (remain == INIT_FAILED) {
            log.error("预算初始化失败，adId={}", adId);
            return false;
        }
        return true;
    }

    /**
     * 扣减并返回扣减后的余额，供演示与对账使用。
     *
     * @return 扣减后的余额（分）；余额不足返回 {@link #NOT_ENOUGH}
     */
    public long deductAndGetRemain(Long adId, BigDecimal dailyBudget, BigDecimal amount) {
        String k = key(adId);
        Long remain = redis.execute(deductScript,
                List.of(k),
                String.valueOf(toCents(amount)),
                String.valueOf(toCents(dailyBudget)));
        return remain == null ? INIT_FAILED : remain;
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
        redis.opsForValue().setIfAbsent(key(adId), String.valueOf(toCents(dailyBudget)));
    }

    /** 预算是否耗尽，用于日志与调试。 */
    public boolean isExhausted(Long adId, BigDecimal dailyBudget) {
        return getBalance(adId, dailyBudget).signum() <= 0;
    }
}
