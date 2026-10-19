package com.rtb.config;

import com.alibaba.csp.sentinel.slots.block.RuleConstant;
import com.alibaba.csp.sentinel.slots.block.degrade.DegradeRule;
import com.alibaba.csp.sentinel.slots.block.degrade.DegradeRuleManager;
import com.rtb.common.breaker.SentinelResources;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * Sentinel 熔断规则装载 —— <b>规则全部在代码内定义</b>。
 *
 * <p><b>为什么不接控制台（dashboard）。</b>
 * <ul>
 *   <li>控制台是独立进程，单机压测环境下要跟引擎抢 CPU / 内存，RT 数字会被污染；</li>
 *   <li>控制台上改的规则<b>默认不持久化</b>（只存客户端内存，重启即还原），
 *       要持久化得再引 Nacos / Apollo，对单实例 demo 是纯负担；</li>
 *   <li>规则随代码走版本控制，评审时和代码一起看，比在界面上点更可靠。</li>
 * </ul>
 * 所以只引 {@code sentinel-core}，不引 {@code sentinel-transport-*}。
 *
 * <p><b>为什么用「慢调用比例」而不是「异常比例 / 异常数」。</b>
 * 本项目的故障形态是「DB / Redis 变慢」，不是「快速报错」：
 * 底层子预算把慢调用表现成一次超时异常，但**慢本身才是信号**。
 * 异常比例模式下，一个「RT 打满 800ms 但最终成功」的调用完全不算失败，规则看不见它；
 * 慢调用比例模式直接按 RT 判定，语义天然对齐，也不需要业务侧额外上报耗时。
 *
 * <p><b>字段口径（容易踩的两个单位坑）。</b>
 * <ul>
 *   <li>{@code count}：RT 模式下是「慢调用判定阈值（毫秒）」，不是秒；</li>
 *   <li>{@code timeWindow}：熔断持续时长，单位是<b>秒</b>，所以要把配置里的
 *       {@code cooldown-ms} 除以 1000（Sentinel 不接受毫秒）。</li>
 * </ul>
 */
@Slf4j
@Configuration
@ConditionalOnProperty(name = "rtb.circuit-breaker.impl", havingValue = "sentinel", matchIfMissing = true)
public class SentinelRuleConfig {

    /** 慢调用比例阈值（0~1）：统计窗口内慢调用占比超过它就熔断 */
    @Value("${rtb.circuit-breaker.slow-ratio:0.5}")
    private double slowRatio;

    /** DB 慢调用判定阈值（ms）：单次候选检索慢于此值即记一次慢调用 */
    @Value("${rtb.circuit-breaker.db-slow-rt-ms:200}")
    private double dbSlowRtMs;

    /** Redis 慢调用判定阈值（ms）：单次预算扣减慢于此值即记一次慢调用 */
    @Value("${rtb.circuit-breaker.redis-slow-rt-ms:100}")
    private double redisSlowRtMs;

    /** 触发熔断所需的最小请求数（统计窗口内），防止样本太少就误开 */
    @Value("${rtb.circuit-breaker.min-request-amount:5}")
    private int minRequestAmount;

    /** 统计窗口时长（ms） */
    @Value("${rtb.circuit-breaker.stat-interval-ms:1000}")
    private int statIntervalMs;

    /** 熔断持续时长（ms）；Sentinel 只吃秒，装载时换算 */
    @Value("${rtb.circuit-breaker.cooldown-ms:5000}")
    private long cooldownMs;

    @PostConstruct
    public void loadRules() {
        int timeWindowSec = (int) Math.max(1, cooldownMs / 1000);

        List<DegradeRule> rules = new ArrayList<>(2);
        rules.add(slowCallRule(SentinelResources.DB_CANDIDATES, dbSlowRtMs, timeWindowSec));
        rules.add(slowCallRule(SentinelResources.REDIS_BUDGET, redisSlowRtMs, timeWindowSec));

        // 覆盖式装载：本类是全进程唯一装载 DegradeRule 的地方
        DegradeRuleManager.loadRules(rules);

        log.info("Sentinel 熔断规则已装载（慢调用比例模式）：" +
                        "DB 资源 {} 慢调用阈值 {}ms / Redis 资源 {} 阈值 {}ms；" +
                        "慢调用比例 >{}，最小请求数 {}，统计窗口 {}ms，熔断时长 {}s",
                SentinelResources.DB_CANDIDATES, dbSlowRtMs,
                SentinelResources.REDIS_BUDGET, redisSlowRtMs,
                slowRatio, minRequestAmount, statIntervalMs, timeWindowSec);
    }

    /** 构造一条「慢调用比例」熔断规则 */
    private DegradeRule slowCallRule(String resource, double slowRtMs, int timeWindowSec) {
        DegradeRule rule = new DegradeRule(resource);
        rule.setGrade(RuleConstant.DEGRADE_GRADE_RT);   // RT = 慢调用比例模式
        rule.setCount(slowRtMs);                        // 慢调用判定阈值（ms）
        rule.setSlowRatioThreshold(slowRatio);          // 慢调用比例阈值
        rule.setMinRequestAmount(minRequestAmount);     // 最小请求数
        rule.setStatIntervalMs(statIntervalMs);         // 统计窗口（ms）
        rule.setTimeWindow(timeWindowSec);              // 熔断持续时长（秒）
        return rule;
    }
}
