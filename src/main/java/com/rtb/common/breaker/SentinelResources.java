package com.rtb.common.breaker;

/**
 * Sentinel 资源名常量。
 *
 * <p>这三个字面量必须<b>三处一致</b>，否则熔断悄悄失效（规则挂在 A 名字上、入口统计 B 名字，
 * 规则永远不触发）：① 这里；② {@code SentinelRuleConfig} 装载规则时；③ Gate 入口 {@code SphU.entry()} 时。
 * 其中 ②③ 都引用本常量，所以只需要维护这一处。
 *
 * <p>命名用 {@code rtb:<依赖>:<动作>} 冒号分隔，方便在 Sentinel 的 metrics 日志 / 监控里按依赖归类。
 */
public final class SentinelResources {

    private SentinelResources() {
    }

    /**
     * DB 候选检索 —— {@code AdMapper#findCandidates}。
     * 慢调用阈值见 {@code rtb.circuit-breaker.db-slow-rt-ms}（默认 200ms）。
     */
    public static final String DB_CANDIDATES = "rtb:db:candidates";

    /**
     * Redis 预算扣减 —— {@code BudgetService#deduct}。
     * 慢调用阈值见 {@code rtb.circuit-breaker.redis-slow-rt-ms}（默认 100ms）。
     */
    public static final String REDIS_BUDGET = "rtb:redis:budget";
}
