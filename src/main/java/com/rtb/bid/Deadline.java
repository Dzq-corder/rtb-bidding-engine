package com.rtb.bid;

/**
 * 竞价链路的时间预算 · M5
 *
 * <p><b>为什么不能只在链路末尾判一次超时</b>
 *
 * <p>最初的实现是在 {@code doBid} 结尾算总耗时，超过 100ms 就把结果
 * 换成兜底广告。压测时暴露出两个问题：
 *
 * <ol>
 *   <li><b>熔断形同虚设</b>：链路已经跑完了，正常胜者都算出来了，
 *       这时候再换成兜底广告，等于把一个「精准」的结果降级成「随机」——
 *       <b>超时不但没省下时间，还损失了广告精准度</b>。
 *       真正的熔断必须「来不及就不做」，而不是「做完了再扔」。</li>
 *   <li><b>下游白跑</b>：计费和落库都已经执行了才判定超时，
 *       钱扣了、明细写了，然后告诉调用方「这次算降级」。
 *       这在业务上是脏数据。</li>
 * </ol>
 *
 * <p>所以改成<b>分段 Deadline</b>：每个阶段开始前检查剩余预算，
 * 不够就<b>就地放弃</b>，直接进入兜底，不再往下走。
 *
 * <p><b>预算怎么分配</b>（对应开发文档 M5 的设计）：
 * <pre>
 *   候选检索  60 ms   ← 占大头。M2 后实际只要 1.3ms，
 *                留足余量应对索引未就绪时降级查库（38.9ms）
 *   出价排序  20 ms   ← 应用层排序，候选少时远快于此
 *   计费      10 ms   ← 一次 Redis 往返
 *   兜底      10 ms   ← 预留给降级路径
 * </pre>
 *
 * <p><b>为什么检索占 60%</b>：它是唯一可能退化到数据库的环节
 * （索引未就绪时降级 SQL，实测 38.9ms）。其余三步都是纯内存或
 * 单次 IO，给再多预算也用不上。
 *
 * <p><b>本类不做强制打断</b>：没有用 {@code Future.cancel} 或
 * {@code Thread.interrupt} 去中断正在执行的步骤。原因是
 * 正在跑的步骤要么是纯内存计算（打断无意义），
 * 要么是单次数据库往返（打断反而可能留下未释放的连接），
 * 强行打断的开销大于收益。<b>务实做法是在阶段边界检查，
 * 不在阶段内部打断</b>。
 */
final class Deadline {

    private final long startNanos;
    private final int totalMs;

    Deadline(int totalMs) {
        this.totalMs = totalMs;
        this.startNanos = System.nanoTime();
    }

    /** 已消耗的毫秒数 */
    int elapsedMs() {
        return (int) ((System.nanoTime() - startNanos) / 1_000_000);
    }

    /**
     * 该阶段是否还有足够预算执行。
     *
     * <p>注意用的是 <b>&gt;=</b>：预算刚好用完时也不该再进下一步，
     * 因为下一步本身还要花时间，进去必然超。
     *
     * @param stageBudgetMs 该阶段计划占用的预算
     */
    boolean hasBudgetFor(int stageBudgetMs) {
        return elapsedMs() + stageBudgetMs < totalMs;
    }

    /** 是否已耗尽总预算（即应熔断） */
    boolean expired() {
        return elapsedMs() >= totalMs;
    }

    /**
     * 剩余预算，用于给兜底路径用。
     *
     * <p><b>为什么返回 0 而不是负数</b>：兜底路径本身还要花时间
     * （查广告详情 + 一次 Redis 扣减），但它<b>必须执行</b>——
     * 宁可超预算也要返回一条广告。返回 0 表示「已经没有余量了」，
     * 调用方据此判断兜底不能再做重活。
     */
    int remainingMs() {
        return Math.max(0, totalMs - elapsedMs());
    }

    int totalMs() {
        return totalMs;
    }
}
