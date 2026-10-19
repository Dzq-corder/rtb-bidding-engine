package com.rtb.common.breaker;

/**
 * 被熔断门包裹的下游调用。
 *
 * <p>刻意<b>允许抛受检异常</b>（而不是 {@code Supplier}）：本项目要保护的下游调用是
 * {@code CompletableFuture#get(timeout)} 这类阻塞调用，它会抛受检的
 * {@code TimeoutException / InterruptedException / ExecutionException}。
 * 用 {@code Supplier} 就得在 lambda 里手工包一层 RuntimeException，把「超时」这个语义抹平，
 * 而超时恰恰是本项目最主要的故障形态，语义不能丢。
 *
 * @param <T> 下游返回类型
 */
@FunctionalInterface
public interface BreakerAction<T> {

    /** 执行一次下游调用；抛出的异常会被熔断门记为一次失败。 */
    T run() throws Exception;
}
