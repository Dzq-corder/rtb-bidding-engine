package com.rtb.config;

import com.rtb.common.CircuitBreaker;
import com.rtb.common.breaker.BreakerGate;
import com.rtb.common.breaker.LocalBreakerGate;
import com.rtb.common.breaker.SentinelBreakerGate;
import com.rtb.common.breaker.SentinelResources;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 熔断门装配：按 {@code rtb.circuit-breaker.impl} 决定注入哪套实现。
 *
 * <p>DB 与 Redis 拿到<b>两个互相独立</b>的 Gate（两个不同资源名 / 两个不同熔断器）。
 * 这是分层熔断的关键：Redis 挂了不该把 DB 检索一起短路，反之亦然。
 *
 * <p>切实现只需改一行配置、重启，规则与阈值口径不变：
 * <pre>
 *   rtb.circuit-breaker.impl: sentinel   # 默认
 *   rtb.circuit-breaker.impl: local      # 自写熔断器（对照基线）
 * </pre>
 */
@Slf4j
@Configuration
public class CircuitBreakerConfig {

    /**
     * DB 候选检索的熔断门。
     *
     * <p>阈值口径与 {@code rtb.timeout.db-ms}（默认 800ms）绑在一起看：
     * 子预算一旦作废，超时就表现为「调用耗时打满子预算」，所以慢调用阈值要显著小于它。
     */
    @Bean("dbBreakerGate")
    public BreakerGate dbBreakerGate(
            @Value("${rtb.circuit-breaker.impl:sentinel}") String impl,
            @Value("${rtb.circuit-breaker.failure-threshold:5}") int failureThreshold,
            @Value("${rtb.circuit-breaker.cooldown-ms:5000}") long cooldownMs) {
        return build(impl, SentinelResources.DB_CANDIDATES, failureThreshold, cooldownMs);
    }

    /**
     * Redis 预算扣减的熔断门。
     *
     * <p>阈值口径与 {@code rtb.timeout.redis-ms}（默认 200ms）绑在一起看。
     */
    @Bean("redisBreakerGate")
    public BreakerGate redisBreakerGate(
            @Value("${rtb.circuit-breaker.impl:sentinel}") String impl,
            @Value("${rtb.circuit-breaker.failure-threshold:5}") int failureThreshold,
            @Value("${rtb.circuit-breaker.cooldown-ms:5000}") long cooldownMs) {
        return build(impl, SentinelResources.REDIS_BUDGET, failureThreshold, cooldownMs);
    }

    private BreakerGate build(String impl, String resource, int failureThreshold, long cooldownMs) {
        if ("local".equalsIgnoreCase(impl)) {
            log.warn("熔断实现 = local（自写 CircuitBreaker，对照基线）：resource={}，连续失败 {} 次打开，冷却 {}ms",
                    resource, failureThreshold, cooldownMs);
            return new LocalBreakerGate(resource, new CircuitBreaker(failureThreshold, cooldownMs));
        }
        log.info("熔断实现 = sentinel：resource={}（阈值见 rtb.circuit-breaker.*）", resource);
        return new SentinelBreakerGate(resource);
    }
}
