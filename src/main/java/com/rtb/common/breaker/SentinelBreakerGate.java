package com.rtb.common.breaker;

import com.alibaba.csp.sentinel.Entry;
import com.alibaba.csp.sentinel.SphU;
import com.alibaba.csp.sentinel.Tracer;
import com.alibaba.csp.sentinel.slots.block.BlockException;

/**
 * Sentinel 熔断门（默认实现）。
 *
 * <p><b>为什么用 {@code SphU} 原生 API 而不是 {@code @SentinelResource} 注解。</b>
 * 注解版要引 {@code sentinel-annotation-aspectj} 并注册切面，靠 Spring 代理拦截方法边界。
 * 而本项目要保护的恰恰不是「一个方法」，是 {@code BidService} 内部的<b>两段代码</b>
 * （候选检索、计费循环），它们嵌在一个大方法里，切不出方法边界。原生 API 直接把代码段圈起来，
 * 既不动代理结构也不改方法签名，改造面最小。
 *
 * <p><b>RT 是怎么被测到的（这是选「慢调用比例」的前提）。</b>
 * Sentinel 在 {@code SphU.entry()} 记下开始时刻，在 {@code entry.exit()} 时算出这段墙钟耗时。
 * 本实现圈住的正是 {@code action.run()}，也就是 {@code CompletableFuture#get(timeout)} 的等待段，
 * 所以测出来的就是<b>这次下游调用的真实耗时</b>——DB 慢到触发 800ms 超时，就记 800ms。
 * 不需要调用方手工上报耗时。
 *
 * <p><b>异常为什么要显式 {@code Tracer.traceEntry}。</b> Sentinel 只统计「从 entry 块里抛出去的异常」，
 * 而本实现是 try-finally 结构、并不吞异常，看起来异常自然会冒到框架里——但那是
 * <b>BlockException 那条外层路径</b>。内层 {@code action.run()} 抛出的异常若不显式 trace，
 * 框架侧的 error 计数就是 0，一旦把规则从「慢调用比例」换成「异常比例 / 异常数」，规则会永远不触发。
 * 所以这里显式记一笔，把两种故障形态的埋点都备齐，切换规则时不用再改代码。
 */
public class SentinelBreakerGate implements BreakerGate {

    private final String resource;

    public SentinelBreakerGate(String resource) {
        this.resource = resource;
    }

    @Override
    public <T> T call(BreakerAction<T> action) throws Exception {
        Entry entry = null;
        try {
            // 未放行（正在熔断 / 被限流）→ 抛 BlockException。
            // 注意它在【外层】try，所以不会被内层的 catch (Exception) 抢走。
            entry = SphU.entry(resource);
            try {
                T result = action.run();
                // 成功：不需要任何动作。entry.exit() 时框架按这段耗时统计 RT 与成功数。
                return result;
            } catch (Exception e) {
                // 真实失败：显式计入 error 统计（备将来切「异常比例」模式用），再原样上抛。
                Tracer.traceEntry(e, entry);
                throw e;
            }
        } catch (BlockException be) {
            // 熔断短路 → 转成统一的未放行信号，且【不】回报失败（否则冷却期永远走不完）
            throw new BreakerOpenException(resource, be);
        } finally {
            if (entry != null) {
                entry.exit();
            }
        }
    }

    @Override
    public String impl() {
        return "sentinel(resource=" + resource + ")";
    }
}
