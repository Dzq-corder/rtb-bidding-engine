package com.rtb.common.breaker;

import com.rtb.common.CircuitBreaker;

/**
 * 本地熔断门：包装项目早期自写的 {@link CircuitBreaker}，<b>保留作对照基线</b>。
 *
 * <p>存在意义是能产出 A/B 数据：把 {@code rtb.circuit-breaker.impl} 在
 * {@code local} / {@code sentinel} 之间切换，其余配置不变，对比同一故障演练下的
 * 「熔断打开时刻、短路比例、恢复耗时、P99」。没有这条基线，「换成 Sentinel 更好」就只是一句话。
 *
 * <p><b>注意它保留了原实现的一个已知缺陷</b>（刻意不改，否则对照基线就不叫基线了）：
 * {@code CircuitBreaker#onFailure()} 在 {@code OPEN} 状态下也会
 * {@code openedAt.set(now)} 重置冷却计时。DB 变慢时池里积压的僵尸任务会陆续回报失败，
 * 每次都把冷却往后推，<b>冷却期可能永远走不完</b>，表现为「重启才恢复」。
 * Sentinel 版没有这个问题（状态机由框架托管，熔断期内不再接收失败回报）。
 * 这正好是对照实验里值得观察的一条差异。
 */
public class LocalBreakerGate implements BreakerGate {

    private final String resource;
    private final CircuitBreaker delegate;

    public LocalBreakerGate(String resource, CircuitBreaker delegate) {
        this.resource = resource;
        this.delegate = delegate;
    }

    @Override
    public <T> T call(BreakerAction<T> action) throws Exception {
        if (!delegate.allowRequest()) {
            // OPEN 冷却中 / HALF_OPEN 探活名额已被占 → 短路，不触达下游
            throw new BreakerOpenException(resource + "[local]", null);
        }
        try {
            T result = action.run();
            delegate.onSuccess();
            return result;
        } catch (Exception e) {
            delegate.onFailure();
            throw e;
        }
    }

    @Override
    public String impl() {
        return "local(resource=" + resource + ", state=" + delegate.currentState() + ")";
    }
}
