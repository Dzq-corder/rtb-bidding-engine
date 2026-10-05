package com.rtb.fallback;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 熔断故障注入开关 · M5
 *
 * <p><b>为什么需要它</b>
 *
 * <p>M5 的验收指标是「熔断触发时能否仍返回兜底广告」，
 * 但<b>正常运行时熔断几乎不发生</b>：
 * M2~M4 优化后单次竞价总耗时 2~7ms，而预算是 100ms，
 * 也就是说熔断路径在压测里一次都走不到。
 *
 * <p>这带来两个问题：
 * <ol>
 *   <li><b>指标测不出来</b> —— 空广告率 A→B 里的那个 B 根本没样本</li>
 *   <li><b>演示靠运气</b> —— 现场无法稳定展示降级行为</li>
 * </ol>
 *
 * <p>停 Redis 能触发<b>异常兜底</b>，但触发不了<b>超时熔断</b>：
 * 异常是立刻抛出的，耗时不长，总耗时压根到不了 100ms。
 * 所以必须能主动把某一步拖慢。
 *
 * <p><b>生产环境必须关闭</b>：{@code rtb.fallback.inject-enabled=false}。
 * 留这个开关的原因是要在预发环境演练降级能力，
 * 真实生产里靠事故来验证降级是最糟的做法。
 */
@Slf4j
@Component
public class FaultInjector {

    /**
     * 注入总开关。默认 {@code false}，即生产安全。
     *
     * <p>用配置项而不是硬编码常量，是为了让它<b>能被关掉</b>——
     * 故障注入代码留在仓库里没问题，留在生产路径上才有问题。
     */
    @Value("${rtb.fallback.inject-enabled:false}")
    private boolean enabled;

    /** 当前生效的注入延迟（毫秒），0 = 未注入 */
    private final AtomicInteger delayMs = new AtomicInteger(0);

    /** 累计注入次数，用于确认演练真的发生过 */
    private final AtomicInteger injectedCount = new AtomicInteger(0);

    /**
     * 在竞价链路中等待指定毫秒数，模拟慢依赖。
     *
     * <p><b>实现选择：用 {@code Thread.sleep} 而不是真的去压数据库</b>。
     * 理由：我们要模拟的是「依赖变慢」这个<b>结果</b>，
     * 而不是某一种特定的慢法。用一个可控的 sleep 可以精确控制
     * 「刚好超过 100ms 预算」这个边界，而真实压测很难稳定命中。
     *
     * <p>注意 sleep 期间会占用 Tomcat 线程 —— 这正是真实超时场景的
     * 放大器：<b>慢请求会让并发能力整体下降</b>，而不是只慢这一个。
     * 这也是「为什么要有兜底」的另一个理由：
     * 超时不只是这一次请求失败，持续超时会拖垮整个服务。
     */
    public void injectDelay() {
        if (!enabled) {
            return;
        }
        int ms = delayMs.get();
        if (ms <= 0) {
            return;
        }
        try {
            Thread.sleep(ms);
            injectedCount.incrementAndGet();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 设置注入延迟。
     *
     * @param ms 毫秒；传 0 关闭注入
     */
    public void setDelay(int ms) {
        int v = Math.max(0, ms);
        delayMs.set(v);
        log.warn("熔断故障注入延迟已设置为 {} ms（注入开关 enabled={}）", v, enabled);
    }

    public int getDelay() {
        return delayMs.get();
    }

    public int getInjectedCount() {
        return injectedCount.get();
    }

    public boolean isEnabled() {
        return enabled;
    }
}
