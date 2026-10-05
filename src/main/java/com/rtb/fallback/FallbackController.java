package com.rtb.fallback;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 熔断降级的故障注入与观测端点 · M5
 *
 * <p><b>为什么需要这个</b>：熔断逻辑的代码分支在正常运行时永远走不到
 * —— 超时率不到 1%，抽样很难碰到；而 M5 的核心指标恰恰是
 * 「触发熔断时能否仍返回广告」。
 *
 * <p>所以必须提供<b>主动触发</b>的手段：
 * 演示时不用靠运气等系统过载，一键就能展示降级行为。
 */
@RestController
@RequiredArgsConstructor
public class FallbackController {

    private final FallbackAdPool pool;
    private final StringRedisTemplate redis;
    private final FaultInjector faultInjector;

    /** 兜底池状态，看填充是否正常 */
    @GetMapping("/fallback/status")
    public Map<String, Object> status() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("poolSize", pool.size());
        m.put("lastError", pool.getLastError());
        Long sample = pool.pick();
        m.put("sampleAdId", sample);
        m.put("redisKey", "fallback:ad_ids");
        m.put("injectEnabled", faultInjector.isEnabled());
        m.put("injectDelayMs", faultInjector.getDelay());
        m.put("injectedCount", faultInjector.getInjectedCount());
        return m;
    }

    /**
     * 主动注入慢依赖，触发<b>超时熔断</b>（不是异常兜底）。
     *
     * <p><b>与停 Redis 的区别</b>，这是本轮最容易讲混的一点：
     * <table border="1">
     *   <tr><th>手段</th><th>触发的分支</th><th>耗时</th></tr>
     *   <tr><td>停 Redis</td><td>异常兜底 emergencyFallback</td><td>立刻，几十毫秒</td></tr>
     *   <tr><td>注入延迟</td><td>超时熔断 timeout 分支</td><td>可超过 100ms 预算</td></tr>
     * </table>
     *
     * <p>两条降级路径都必须验证——只验一条就等于没验。
     *
     * <p>用法：{@code /fallback/inject?delayMs=150} 打开，
     * 压测后 {@code /fallback/inject?delayMs=0} 关闭。
     */
    @PostMapping("/fallback/inject")
    public Map<String, Object> inject(@RequestParam(defaultValue = "0") int delayMs) {
        faultInjector.setDelay(delayMs);
        return Map.of(
                "injectDelayMs", faultInjector.getDelay(),
                "injectEnabled", faultInjector.isEnabled(),
                "hint", faultInjector.isEnabled()
                        ? "已生效，可直接调 /bid/batch 观察 timeoutCnt"
                        : "注入开关未开启（rtb.fallback.inject-enabled=false），本调用无效果");
    }

    /**
     * 模拟 Redis 不可用后的降级行为。
     *
     * <p>验收场景：把 Redis kill 掉，兜底仍能返回广告。
     * 本接口不真的改 Redis 状态，只报告「如果 Redis 挂了会怎样」——
     * 真正的验证靠停掉 Redis 容器后调/bid。
     */
    @GetMapping("/fallback/probe")
    public Map<String, Object> probe() {
        Map<String, Object> m = new LinkedHashMap<>();
        try {
            redis.opsForValue().get("fallback:probe");
            m.put("redisAlive", true);
        } catch (Exception e) {
            m.put("redisAlive", false);
            m.put("redisError", e.getClass().getSimpleName());
        }
        m.put("poolSize", pool.size());
        // 关键：Redis 挂了，池子靠本地内存仍能用
        m.put("pickWorksWithoutRedis", pool.pick() != null);
        return m;
    }

    /** 强制刷新兜底池（改数据后用） */
    @PostMapping("/fallback/refresh")
    public Map<String, Object> refresh() {
        pool.refresh();
        return Map.of("poolSize", pool.size());
    }

    /**
     * 预热：把兜底池写进 Redis。
     *
     * <p>验收「kill Redis 后仍能兜底」之前必须先调用这个——
     * 否则 Redis 一挂，池子是空的（虽然本地还有一份，但多实例场景下不成立）。
     */
    @PostMapping("/fallback/warmup")
    public Map<String, Object> warmup() {
        pool.refresh();
        Long id = pool.pick();
        return Map.of("poolSize", pool.size(), "sampleAdId", id);
    }
}
