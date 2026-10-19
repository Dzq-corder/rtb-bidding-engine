package com.rtb;

import com.alibaba.csp.sentinel.slots.block.RuleConstant;
import com.alibaba.csp.sentinel.slots.block.degrade.DegradeRule;
import com.alibaba.csp.sentinel.slots.block.degrade.DegradeRuleManager;
import com.rtb.common.breaker.BreakerOpenException;
import com.rtb.common.breaker.SentinelBreakerGate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Sentinel 熔断门单测 —— <b>不依赖 Spring / MySQL / Redis / Kafka</b>，纯本地毫秒级跑完。
 *
 * <p>存在的意义是锁住三条「熔断器最容易改坏、且坏掉之后不会报错、只会静默失效」的语义：
 * <ol>
 *   <li><b>慢调用比例真的能触发熔断</b>——规则字段口径写错（比如把 {@code count} 当成秒、
 *       把 {@code timeWindow} 当成毫秒）时，规则永远不触发，线上表现为「熔断形同虚设」，
 *       但代码不会报任何错。</li>
 *   <li><b>短路时下游动作真的没被执行</b>——这是熔断的全部价值所在。
 *       若把 {@code BlockException} 和真实失败混成一个分支，代码看着照跑，
 *       实际每次请求仍在打下游，熔断等于没装。</li>
 *   <li><b>真实失败原样上抛</b>——不能被包装成 {@link BreakerOpenException}，
 *       否则调用方会把「打了下游但失败」误判成「没打下游」，进而不回报失败，熔断计数永远不满。</li>
 * </ol>
 *
 * <p>每个用例用**各自独立的资源名**，避免共享 Sentinel 的滑动窗口统计互相污染
 * （Sentinel 的统计按资源名全局持有，清理规则不会重置已累积的统计）。
 * {@code @AfterEach} 清空全部规则：无规则 = 全部放行，对同 JVM 里的其他测试是安全状态。
 */
@DisplayName("Sentinel 熔断门：慢调用比例熔断 + 短路语义")
class SentinelBreakerGateTest {

    /** 慢调用判定阈值 50ms、最少 2 个请求、慢调用占比 > 50% 即熔断、熔断 10s（测试期内不会自动恢复） */
    private static final double SLOW_RT_MS = 50;
    private static final int MIN_REQUEST_AMOUNT = 2;
    private static final double SLOW_RATIO = 0.5;
    private static final int TIME_WINDOW_SEC = 10;

    private static final String RES_SLOW = "test:breaker:slow";
    private static final String RES_FAIL = "test:breaker:fail";
    private static final String RES_FAST = "test:breaker:fast";

    @AfterEach
    void tearDown() {
        DegradeRuleManager.loadRules(List.of());
    }

    @Test
    @DisplayName("慢调用比例超阈值 → 熔断打开，下一次调用短路且下游动作完全不执行")
    void opensOnSlowCallRatioThenShortCircuits() throws Exception {
        SentinelBreakerGate gate = new SentinelBreakerGate(RES_SLOW);
        DegradeRuleManager.loadRules(List.of(slowCallRule(RES_SLOW)));

        // 制造 2 次慢调用（60ms > 50ms 阈值）：满足最小请求数，且慢调用占比 100% > 50%
        for (int i = 0; i < 2; i++) {
            gate.call(() -> {
                Thread.sleep(60);
                return null;
            });
        }

        // 熔断已打开：必须短路，且 action 一次都不能被执行
        AtomicInteger executed = new AtomicInteger(0);
        assertThrows(BreakerOpenException.class, () -> gate.call(() -> {
            executed.incrementAndGet();
            return null;
        }), "慢调用比例达标后应打开熔断并抛出 BreakerOpenException");
        assertEquals(0, executed.get(),
                "熔断短路时下游动作不应被执行——否则熔断只是「记了个状态」，流量照旧打到下游");
    }

    @Test
    @DisplayName("下游真实失败 → 原样上抛，不被误包装成短路信号")
    void propagatesRealFailureAsIs() {
        SentinelBreakerGate gate = new SentinelBreakerGate(RES_FAIL);
        DegradeRuleManager.loadRules(List.of(slowCallRule(RES_FAIL)));

        IllegalStateException boom = new IllegalStateException("下游炸了");
        Exception thrown = assertThrows(Exception.class, () -> gate.call(() -> {
            throw boom;
        }));
        assertSame(boom, thrown,
                "真实失败必须原样上抛：调用方要据此回报失败给熔断器，包装掉就丢了这个信号");
    }

    @Test
    @DisplayName("快调用不触发熔断（规则不能被配成「碰一下就开」）")
    void fastCallsDoNotTrip() {
        SentinelBreakerGate gate = new SentinelBreakerGate(RES_FAST);
        DegradeRuleManager.loadRules(List.of(slowCallRule(RES_FAST)));

        for (int i = 0; i < 10; i++) {
            assertDoesNotThrow(() -> gate.call(() -> "ok"));
        }
    }

    /** 与 {@code SentinelRuleConfig#slowCallRule} 同口径构造一条慢调用比例规则 */
    private static DegradeRule slowCallRule(String resource) {
        DegradeRule rule = new DegradeRule(resource);
        rule.setGrade(RuleConstant.DEGRADE_GRADE_RT);        // 慢调用比例模式
        rule.setCount(SLOW_RT_MS);                           // 慢调用判定阈值（ms）
        rule.setSlowRatioThreshold(SLOW_RATIO);              // 慢调用比例阈值
        rule.setMinRequestAmount(MIN_REQUEST_AMOUNT);        // 最小请求数
        rule.setStatIntervalMs(1000);                        // 统计窗口
        rule.setTimeWindow(TIME_WINDOW_SEC);                 // 熔断持续时长（秒）
        return rule;
    }
}
