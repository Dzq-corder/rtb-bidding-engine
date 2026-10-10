package com.rtb.common;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 极简熔断器 · M5
 *
 * <p>状态机：{@code CLOSED → OPEN → HALF_OPEN → CLOSED}。
 * 设计取舍（贴合本项目「单实例 demo、不引新依赖」前提，纯 JDK 原子类实现）：
 * <ul>
 *   <li><b>CLOSED</b>：正常放行；连续失败累计到阈值 → 转 OPEN</li>
 *   <li><b>OPEN</b>：冷却期内全部短路（调用方直接走兜底）；冷却到期转 HALF_OPEN 并独占一个探活名额</li>
 *   <li><b>HALF_OPEN</b>：仅放一个探活请求；成功 → CLOSED（自愈），失败 → 回 OPEN（再冷却）</li>
 * </ul>
 *
 * <p>注意：这是<b>实例级</b>熔断器，单实例 demo 足够；多实例部署需改为共享状态（如 Redis 计数）。
 */
public class CircuitBreaker {

    private enum State { CLOSED, OPEN, HALF_OPEN }

    private final int failureThreshold;
    private final long cooldownMs;

    private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
    private final AtomicLong openedAt = new AtomicLong(0);
    private final AtomicReference<State> state = new AtomicReference<>(State.CLOSED);
    /** HALF_OPEN 期间只允许一个探活在飞，避免多个探活并行把尚未恢复的依赖打爆 */
    private final AtomicReference<Boolean> probeInFlight = new AtomicReference<>(false);

    public CircuitBreaker(int failureThreshold, long cooldownMs) {
        this.failureThreshold = failureThreshold;
        this.cooldownMs = cooldownMs;
    }

    /** 是否放行本次请求。OPEN 冷却期内返回 false → 调用方走兜底。 */
    public boolean allowRequest() {
        State s = state.get();
        if (s == State.CLOSED) {
            return true;
        }
        if (s == State.OPEN) {
            if (System.currentTimeMillis() - openedAt.get() >= cooldownMs) {
                // 冷却到期：尝试进入 HALF_OPEN 并独占唯一探活名额
                if (state.compareAndSet(State.OPEN, State.HALF_OPEN)
                        && probeInFlight.compareAndSet(false, true)) {
                    return true;
                }
                return false;
            }
            return false;
        }
        // HALF_OPEN：仅当探活名额空闲时放行（防多个探活并发）
        return probeInFlight.compareAndSet(false, true);
    }

    /** 依赖调用成功：复位失败计数，回到 CLOSED。 */
    public void onSuccess() {
        consecutiveFailures.set(0);
        probeInFlight.set(false);
        state.set(State.CLOSED);
    }

    /** 依赖调用失败：计数 +1；达阈值或探活失败则 OPEN。 */
    public void onFailure() {
        if (state.get() == State.HALF_OPEN) {
            probeInFlight.set(false);
            openedAt.set(System.currentTimeMillis());
            state.set(State.OPEN);
            return;
        }
        if (consecutiveFailures.incrementAndGet() >= failureThreshold) {
            openedAt.set(System.currentTimeMillis());
            state.set(State.OPEN);
        }
    }

    /** 当前状态名，仅用于日志 / 观测。 */
    public String currentState() {
        return state.get().name();
    }
}
