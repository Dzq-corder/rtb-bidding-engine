package com.rtb.budget;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * 预算服务 · M1 朴素实现
 *
 * <p><b>本轮刻意不做原子性封装</b>：余额校验和扣减是两条独立命令、
 * 两次往返。M3 才把它们合并进一个 Lua 脚本。
 *
 * <p><b>为什么这不是「故意写丑」</b>：校验 + 扣减分两步是最直观、最容易
 * 想到的写法，它是「正确的实现，尚未加固」。真正的并发超扣问题要等到
 * 有实际并发压测时才会暴露，M3 的 Lua 脚本是对那个问题的直接回应。
 * 面试时的表述是：「初版用两步做，压测发现会超扣，改成 Lua 原子操作」——
 * 这比一开始就写 Lua 更可信，因为前者说明你真的遇到过那个问题。
 *
 * <p>Key 命名：{@code budget:{adId}:{yyyyMMdd}}，按天分桶让过期策略自然生效。
 */
@Slf4j
@Service
public class BudgetService {

    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("yyyyMMdd");

    private final StringRedisTemplate redis;

    public BudgetService(StringRedisTemplate redis) {
        this.redis = redis;
    }

    private String key(Long adId) {
        return "budget:" + adId + ":" + LocalDate.now().format(DATE_FMT);
    }

    /**
     * 查询当前余额，不存在则按广告日预算初始化。
     *
     * <p>惰性初始化：Redis 里没有就查 MySQL 拿日预算写进去。
     * M3 起这里会配合批量预热，避免竞价链路上出现初始化写。
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
     * 扣减预算。M1 版本：三步操作，非原子。
     *
     * <p>并发下的错位场景：请求 A、B 同时读到余额 10 元，都通过校验，
     * 各自扣 8 元，结果余额变成 -6 元，超扣发生。
     * M3 用 Lua 把三步收进一个原子操作，消除这个窗口。
     *
     * @return true 扣减成功；false 余额不足
     */
    public boolean deduct(Long adId, BigDecimal dailyBudget, BigDecimal amount) {
        String k = key(adId);

        // 第一步：读余额
        String cur = redis.opsForValue().get(k);
        if (cur == null) {
            // 初始化后必须重新读，不能用 dailyBudget 的字符串代替：
            // 存进 Redis 的是「分」，直接用「元」的字符串会差100 倍
            initIfAbsent(adId, dailyBudget);
            cur = redis.opsForValue().get(k);
            if (cur == null) {
                cur = String.valueOf(toCents(dailyBudget));
            }
        }

        // 第二步：校验
        if (parse(cur).compareTo(amount) < 0) {
            return false;
        }

        // 第三步：扣减
        redis.opsForValue().decrement(k, toCents(amount));
        return true;
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

    /**
     * 预算是否耗尽，用于日志与调试。
     */
    public boolean isExhausted(Long adId, BigDecimal dailyBudget) {
        return getBalance(adId, dailyBudget).signum() <= 0;
    }
}
