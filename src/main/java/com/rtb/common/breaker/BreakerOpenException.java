package com.rtb.common.breaker;

/**
 * 熔断短路信号：熔断门<b>未放行</b>，本次调用<b>没有触达下游</b>。
 *
 * <p>它和「下游真实失败」是两件必须分开的事，这是熔断器语义的核心约束：
 * <ul>
 *   <li><b>短路</b>（本异常）：压根没打下游，<b>不能再喂给熔断器</b>。
 *       否则熔断 OPEN 期间每个被短路的请求都会回报一次「失败」，失败计数只增不减，
 *       冷却期永远走不完，熔断就再也开不回来了。</li>
 *   <li><b>真实失败</b>（其他异常）：确实打了下游且失败了，必须喂给熔断器计数。</li>
 * </ul>
 * 拆成两个类型就是让调用方在 catch 时无法把两者混为一谈。
 *
 * <p>继承 {@link RuntimeException} 是为了不污染调用方的 throws 签名（{@code BreakerGate#call}
 * 的签名已经要声明 {@code throws Exception} 了，再加一个受检异常会让调用点更难写）。
 * <b>代价</b>：它是 unchecked 的，所以调用方在 catch 时<b>必须把它放在
 * {@code catch (Exception)} 之前</b>——放在后面会被宽泛分支吞掉，短路和真实失败就又混在一起了。
 */
public class BreakerOpenException extends RuntimeException {

    /** 未放行的资源名（DB / Redis），用于日志定位是哪一个依赖被熔断 */
    private final String resource;

    public BreakerOpenException(String resource, Throwable cause) {
        super("熔断门未放行（OPEN 冷却中或探活名额被占），本次未触达下游：" + resource, cause);
        this.resource = resource;
    }

    public String getResource() {
        return resource;
    }
}
