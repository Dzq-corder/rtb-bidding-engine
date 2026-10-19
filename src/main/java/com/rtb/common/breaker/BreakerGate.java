package com.rtb.common.breaker;

/**
 * 熔断门：把「放行判断 → 执行 → 回报结果」这套三步式流程收敛成一次调用。
 *
 * <p><b>为什么要有这层抽象。</b> 改造前 {@code BidService} 里散落着三句手写调用：
 * {@code allowRequest()} 先问能不能打、成功后 {@code onSuccess()}、失败后 {@code onFailure()}。
 * 这三句必须成对出现，任何一处漏写都会导致熔断器统计失真（漏 {@code onFailure} 就永远熔不断，
 * 漏 {@code onSuccess} 就永远自愈不了）。收敛成 {@link #call} 之后，
 * 「进入 / 成功 / 失败 / 退出」的时序由实现保证，调用方不可能写漏。
 *
 * <p><b>两种实现，可切换对照</b>（见 {@code rtb.circuit-breaker.impl}）：
 * <ul>
 *   <li>{@link SentinelBreakerGate} —— 阿里 Sentinel，<b>默认</b>。用「慢调用比例」原生表达
 *       「下游变慢」这个故障形态，状态机与探活由框架托管。</li>
 *   <li>{@link LocalBreakerGate} —— 项目早期自写的纯 JDK 熔断器，<b>保留作对照基线</b>，
 *       用于产出「换成 Sentinel 前后」的 A/B 压测数据。</li>
 * </ul>
 * 与项目里已有的 {@code rtb.fallback.enabled} / {@code rtb.fallback.force} /
 * {@code rtb.detail.sync-insert} 三个测量开关同一套路。
 *
 * <p><b>调用方约定（务必遵守）。</b> {@link BreakerOpenException} 是 unchecked 的，
 * 所以 catch 顺序必须是：
 * <pre>
 *   try {
 *       gate.call(...);
 *   } catch (BreakerOpenException e) {   // ← 必须在前：短路，未触达下游
 *       // 走兜底，且不要回报失败
 *   } catch (Exception e) {              // → 真实失败：超时 / IO 异常
 *       // 走兜底
 *   }
 * </pre>
 * 写反了（{@code Exception} 在前）短路就会被吞进「真实失败」分支，熔断语义随即失效。
 */
public interface BreakerGate {

    /**
     * 包裹一次下游调用。
     *
     * @param action 真正打下游的动作；它抛出的任何异常都会被记为一次失败
     * @param <T>    下游返回类型
     * @return 下游返回值
     * @throws BreakerOpenException 熔断未放行 —— <b>短路，本次未触达下游</b>
     * @throws Exception            下游真实失败（超时 / 连接异常 / 业务异常）
     */
    <T> T call(BreakerAction<T> action) throws Exception;

    /** 当前实现的自述（含资源名 / 状态），仅用于启动日志与观测。 */
    String impl();
}
