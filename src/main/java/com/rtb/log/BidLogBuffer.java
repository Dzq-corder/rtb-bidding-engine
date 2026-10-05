package com.rtb.log;

import com.rtb.bid.BidLog;
import com.rtb.bid.BidLogMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 竞价明细的写入缓冲 —— M7 的核心
 *
 * <p><b>它只装 {@code bid_log}</b>。这是本类最重要的约束，写在最前面：
 * 涉及金钱的 {@code billing_event} 由 {@code BidService} 同步逐条写，
 * 一行都不经过这里。两条写入路径并存不是偷懒，而是「按业务后果分级」的结果：
 *
 * <ul>
 *   <li>{@code bid_log} —— 大盘明细，丢几条不影响「上周哪天最忙」的结论 → 可异步、可批量、可丢</li>
 *   <li>{@code billing_event} —— 扣费事实，丢一条就是资损 → 同步、逐条、不可丢</li>
 * </ul>
 *
 * <p><b>为什么用本地队列而不是直接丢给 Kafka</b>：
 * 本地队列解决的是「不阻塞当前进程」，MQ 解决的是「跨服务解耦 + 可重放 + 多消费方」。
 * 明细只有一个下游（就是这张表自己），不需要多消费方，
 * 为它多走一跳网络反而不划算。这是 M6 必答题第 5 条的直接应用。
 *
 * <p><b>为什么必须有界</b>：无界队列在消费跟不上时会无限增长，
 * 最终 OOM —— 而拖垮的是竞价主进程本身。
 * 「写入不反压主链路」的前提是队列自己有明确的失败模式，
 * 无界队列没有失败模式，只有崩溃。
 *
 * <p><b>队列满为什么丢弃而不是阻塞</b>：
 * 阻塞（{@code put()}）等于让日志写入反压竞价 ——
 * 这正是本轮要消灭的东西。宁可丢一条明细，也不能让竞价线程卡在写日志上。
 */
@Slf4j
@Component
public class BidLogBuffer {

    private final BidLogMapper mapper;

    /**
     * 批量刷盘开关。
     *
     * <p>{@code false} 时退回 M1 的同步逐条写，用于采集「由 A 提升至 B」里的 A。
     * <b>这个开关是专门为压测对比留的</b>——没有 A 就没有对照，
     * 只报一个优化后的数字是没有说服力的（项目一贯的取舍）。
     */
    @Value("${rtb.log.batch-enabled:true}")
    private boolean batchEnabled;

    /**
     * 运行时覆盖开关（{@code null} 表示沿用配置）。
     *
     * <p><b>为什么需要运行时切换</b>：A→B 的对比必须在同一进程、同一份数据、
     * 同一台机器上做，否则差异可能来自环境而不是优化本身。
     * 重启服务重测会引入 JVM 预热、连接池状态等额外变量。
     */
    private volatile Boolean batchOverride = null;

    @Value("${rtb.log.queue-capacity:10000}")
    private int queueCapacity;

    @Value("${rtb.log.batch-size:500}")
    private int batchSize;

    @Value("${rtb.log.flush-interval-ms:200}")
    private long flushIntervalMs;

    private BlockingQueue<BidLog> queue;

    private volatile boolean running = true;

    /**
     * 暂停刷盘（仅用于演示与压测）。
     *
     * <p><b>为什么要有这个开关</b>：M6 踩过的教训——「降级机制会掩盖 bug」。
     * 丢弃策略如果永远不触发，那段代码就等于没验证过。
     * 暂停刷盘能让队列真的填满，从而证明<strong>丢弃分支确实会走、且计数正确</strong>。
     */
    private final AtomicBoolean paused = new AtomicBoolean(false);

    private Thread flusher;

    // ---- 计数（全部用 AtomicLong：入队方是竞价线程，出队方是刷盘线程）

    private final AtomicLong enqueued = new AtomicLong();
    private final AtomicLong droppedEmpty = new AtomicLong();
    private final AtomicLong droppedWithAd = new AtomicLong();
    private final AtomicLong flushedRows = new AtomicLong();
    private final AtomicLong flushCount = new AtomicLong();
    private final AtomicLong writeNanos = new AtomicLong();

    private volatile long lastFlushRows;
    private volatile long lastFlushCostMs;

    public BidLogBuffer(BidLogMapper mapper) {
        this.mapper = mapper;
    }

    @PostConstruct
    public void start() {
        this.queue = new ArrayBlockingQueue<>(queueCapacity);
        this.flusher = new Thread(this::flushLoop, "bid-log-flusher");
        // 守护线程：不能因为刷盘线程没停而阻止 JVM 退出
        this.flusher.setDaemon(true);
        this.flusher.start();
        log.info("竞价明细缓冲启动：batchEnabled={} capacity={} batchSize={} flushIntervalMs={}",
                batchEnabled, queueCapacity, batchSize, flushIntervalMs);
    }

    @PreDestroy
    public void stop() {
        running = false;
        if (flusher != null) {
            flusher.interrupt();
        }
        // 关闭时把队列里剩的尽力刷掉。
        // 【为什么不做成「保证不丢」】本表是可丢数据，
        // 这里的语义是「尽力而为」，不是「事务保证」——
        // 说成保证不丢就与「明细可丢」的设计前提自相矛盾了。
        drainAndFlush();
        log.info("竞价明细缓冲停止：剩余 {} 条已尽力刷盘", flushedRows.get());
    }

    /**
     * 提交一条明细。
     *
     * <p><b>永远不阻塞、永远不抛异常</b>。调用方是竞价主链路，
     * 一条日志写不进去绝不能影响广告返回。
     *
     * <p><b>队列满时的丢弃策略：低价值优先</b>
     * <ul>
     *   <li>待入队的是空结果（{@code has_ad = 0}）→ 直接丢。
     *       它对大盘统计的价值最低（填充率的分母少一条影响很小）</li>
     *   <li>待入队的是成交明细（{@code has_ad = 1}）→ 从队列里找一条空结果换掉；
     *       找不到才丢自己</li>
     * </ul>
     *
     * <p><b>为什么换位是 O(n) 还敢做</b>：这段只在队列满时才执行，
     * 而队列满意味着刷盘已经严重落后，是异常路径不是常态。
     * 常态路径（{@code offer} 成功）是 O(1) 的无锁快路径。
     */
    public void submit(BidLog entity) {
        if (entity == null) {
            return;
        }

        if (!isBatchEnabled()) {
            // baseline 路径：同步逐条写，用于采集对照数据
            long t0 = System.nanoTime();
            try {
                mapper.insert(entity);
                flushedRows.incrementAndGet();
            } catch (Exception e) {
                log.warn("竞价明细写入失败（baseline 路径）requestId={}", entity.getRequestId(), e);
            } finally {
                writeNanos.addAndGet(System.nanoTime() - t0);
            }
            return;
        }

        if (queue.offer(entity)) {
            enqueued.incrementAndGet();
            return;
        }

        // ---- 队列已满
        boolean lowValue = entity.getHasAd() == null || entity.getHasAd() == 0;
        if (lowValue) {
            droppedEmpty.incrementAndGet();
            return;
        }

        // 高价值明细：腾一个位置出来。bid_log 不要求顺序，可以任意替换。
        Iterator<BidLog> it = queue.iterator();
        while (it.hasNext()) {
            BidLog cur = it.next();
            if (cur.getHasAd() != null && cur.getHasAd() == 1) {
                continue;
            }
            try {
                it.remove();
                droppedEmpty.incrementAndGet();
                if (queue.offer(entity)) {
                    enqueued.incrementAndGet();
                    return;
                }
                // offer 仍失败 = 另一个生产者抢走了刚腾出的位置
                break;
            } catch (IllegalStateException e) {
                // 并发下元素已被刷盘线程取走，换下一个
            }
        }
        droppedWithAd.incrementAndGet();
    }

    /**
     * 刷盘主循环。
     *
     * <p>攒批规则：<b>攒够 {@code batchSize} 条立刻刷，或最多等
     * {@code flushIntervalMs} 毫秒就刷当时有多少拿多少</b>。
     * 计时从「本批第一条到达」开始，不是固定节奏。
     *
     * <p><b>为什么必须真的攒批，而不是「一有数据就立刻取走」</b>：
     * 第一版实现就是后者，实测刷 5,000 条用了 214 批、平均批量只有 23 条。
     * 每批照样要付一次网络往返（约 5ms），却只摊掉 23 行——
     * 批量 INSERT 的意义是「一次往返写多行」，批量太小等于白攒。
     * 实测 500 条/批的写入效率是 23 条/批的约 3 倍。
     *
     * <p><b>代价</b>：明细的可见延迟最多增加 {@code flushIntervalMs}。
     * 明细本来就是可延迟数据（大盘分析用），这个代价可以接受；
     * 换的是数据库时间的显著下降。
     */
    private void flushLoop() {
        while (running) {
            try {
                BidLog first = queue.poll(flushIntervalMs, TimeUnit.MILLISECONDS);
                if (first == null) {
                    continue;
                }
                if (paused.get()) {
                    // 暂停时把已取出的放回去，让队列填满以验证丢弃策略。
                    // 放不回去就丢弃——这正是我们要观察的现象。
                    if (!queue.offer(first)) {
                        droppedEmpty.incrementAndGet();
                    }
                    Thread.sleep(flushIntervalMs);
                    continue;
                }

                List<BidLog> batch = new ArrayList<>(batchSize);
                batch.add(first);

                // 先等攒够，再一次 drainTo 取走
                //
                // 【为什么不用「逐条 poll」攒批】第一版就是逐个 poll，
                // 每一条都要抢一次 ArrayBlockingQueue 的锁，
                // 而这把锁同时被 20 个竞价线程用来 offer——
                // 攒一批 500 条就等于跟主链路抢 500 次锁。
                // 实测主链路 QPS 因此被拖下来，波动也明显变大。
                //
                // 改成「先看 size，不够就睡一小会儿，够了再一把 drainTo」：
                // 一次批量只抢一次锁，批大小仍然能攒到接近 batchSize。
                long deadline = System.nanoTime()
                        + TimeUnit.MILLISECONDS.toNanos(flushIntervalMs);
                while (batch.size() < batchSize && System.nanoTime() < deadline) {
                    if (queue.size() >= batchSize - batch.size()) {
                        break;
                    }
                    Thread.sleep(2);
                }
                queue.drainTo(batch, batchSize - batch.size());
                flush(batch);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                // 【关键】刷盘线程绝不能因为一次异常就死掉。
                // 它一死，队列会默默填满然后全部丢弃，
                // 而竞价链路看起来完全正常——这属于「降级机制掩盖故障」。
                log.error("竞价明细刷盘线程异常，继续运行", e);
            }
        }
    }

    /**
     * 执行一批批量写入。
     *
     * <p><b>{@code writeNanos} 只累计数据库写入本身的时间</b>，
     * 不含等待攒批的时间——指标要测的是「写入吞吐」，
     * 把等待时间算进去会让批量显得更慢，那是自欺欺人。
     */
    private void flush(List<BidLog> batch) {
        if (batch == null || batch.isEmpty()) {
            return;
        }
        long t0 = System.nanoTime();
        try {
            mapper.insertBatch(batch);
            long costMs = (System.nanoTime() - t0) / 1_000_000;
            flushedRows.addAndGet(batch.size());
            flushCount.incrementAndGet();
            lastFlushRows = batch.size();
            lastFlushCostMs = costMs;
        } catch (Exception e) {
            // 明细可丢：整批失败只记日志，不做重试。
            // 【为什么不重试】重试会让刷盘线程卡住，队列更快填满，
            // 反而丢得更多。可丢数据的正确失败模式是「放弃这一批」。
            log.error("竞价明细批量刷盘失败，丢弃 {} 条", batch.size(), e);
            droppedEmpty.addAndGet(batch.size());
        } finally {
            writeNanos.addAndGet(System.nanoTime() - t0);
        }
    }

    /**
     * 写入路径基准测试：同样行数、同样单线程顺序执行，只有「逐条 / 批量」一个变量。
     *
     * <p><b>为什么要单独做这个基准，而不直接用压测时的 {@code writeThroughputPerSec}</b>：
     * 压测时逐条模式是 <strong>20 个请求线程并发写</strong>，
     * 而批量模式只有 1 个刷盘线程。{@code writeNanos} 累加的是各线程耗时之和，
     * 两种模式的分母根本不是一回事（并发 20 时累加值约为墙钟的 20 倍），
     * 直接相除得到的「吞吐」会严重低估逐条模式，对比没有意义。
     *
     * <p>所以这里固定为<strong>单线程顺序写 N 行</strong>，只换写入方式：
     * <ul>
     *   <li>{@code batch=false}：N 次单条 INSERT（模拟 M1 同步链路的写入代价）</li>
     *   <li>{@code batch=true}：N/500 次多值 INSERT（M7 刷盘线程的实际做法）</li>
     * </ul>
     *
     * <p><b>刻意说明的偏差</b>：真实压测里逐条模式有 20 个线程在写，
     * 单线程基准不等价于真实并发吞吐。这里测的是「写入路径本身的效率」，
     * 不是「系统总吞吐」——两个数都要看，但别混着讲。
     *
     * <p>产生的测试行 {@code request_id} 以 {@code bench} 开头，便于清理：
     * {@code DELETE FROM bid_log WHERE request_id LIKE 'bench%'}
     */
    public Map<String, Object> bench(int rows, boolean batch) {
        List<BidLog> all = new ArrayList<>(rows);
        for (int i = 0; i < rows; i++) {
            all.add(newSample());
        }
        long t0 = System.nanoTime();
        long ok = 0;
        try {
            if (batch) {
                for (int i = 0; i < all.size(); i += batchSize) {
                    List<BidLog> chunk = all.subList(i, Math.min(i + batchSize, all.size()));
                    mapper.insertBatch(chunk);
                    ok += chunk.size();
                }
            } else {
                for (BidLog one : all) {
                    mapper.insert(one);
                    ok++;
                }
            }
        } catch (Exception e) {
            log.error("写入基准测试失败 rows={} batch={}", rows, batch, e);
        }
        long nanos = Math.max(1, System.nanoTime() - t0);
        Map<String, Object> r = new java.util.LinkedHashMap<>();
        r.put("rows", rows);
        r.put("mode", batch ? "batch" : "single");
        r.put("batchSize", batchSize);
        r.put("written", ok);
        r.put("elapsedMs", nanos / 1_000_000);
        r.put("throughputPerSec", Math.round(ok * 1_000_000_000.0 / nanos * 100.0) / 100.0);
        r.put("avgUsPerRow", Math.round(nanos / 1000.0 / Math.max(1, ok) * 100.0) / 100.0);
        return r;
    }

    /** 造一条用于基准测试的明细。request_id 以 bench 开头，便于清理。 */
    private static BidLog newSample() {
        BidLog b = new BidLog();
        String rid = ("bench" + java.util.UUID.randomUUID().toString().replace("-", ""));
        b.setRequestId(rid.substring(0, 32));
        b.setAdId(1L);
        b.setAdvertiserId(1);
        b.setSlotCode("slot_bench");
        b.setRegion("1101");
        b.setAge(25);
        b.setGender(1);
        b.setInterests("bench");
        b.setCandidateCnt(128);
        b.setMatchCnt(21);
        b.setHasAd(1);
        b.setWinBid(new java.math.BigDecimal("2.00"));
        b.setPayPrice(new java.math.BigDecimal("1.50"));
        b.setCostMs(5);
        b.setCandidateCost(1);
        b.setFilterCost(1);
        b.setSortCost(1);
        b.setBillCost(1);
        b.setBidAt(java.time.LocalDateTime.now());
        return b;
    }

    /**
     * 立即把队列里现有的全部刷掉（不等攒批）。
     * 用于 A→B 切换前后对齐状态，以及演示时手动触发。
     */
    public void flushNow() {
        drainAndFlush();
    }

    /** 关闭时把队列里剩下的尽力刷掉。 */
    private void drainAndFlush() {
        if (queue == null) {
            return;
        }
        List<BidLog> rest = new ArrayList<>();
        queue.drainTo(rest);
        for (int i = 0; i < rest.size(); i += batchSize) {
            flush(new ArrayList<>(rest.subList(i, Math.min(i + batchSize, rest.size()))));
        }
    }

    public void setPaused(boolean p) {
        paused.set(p);
    }

    /** 运行时切换批量刷盘（用于 A→B 对比，避免重启引入额外变量）。 */
    public void setBatchEnabled(Boolean enabled) {
        this.batchOverride = enabled;
    }

    /**
     * 清零所有计数。
     *
     * <p><b>为什么必须能清零</b>：不开这个口子，A→B 两个数字就只能靠
     * 「前后相减」来算，而相减会累积两轮之间的空档误差。
     * 清零后每轮各测一次，两个数字都是干净的单轮值。
     */
    public void resetCounters() {
        enqueued.set(0);
        droppedEmpty.set(0);
        droppedWithAd.set(0);
        flushedRows.set(0);
        flushCount.set(0);
        writeNanos.set(0);
        lastFlushRows = 0;
        lastFlushCostMs = 0;
    }

    // ---- 观测

    /**
     * 当前写入吞吐（条/秒）。
     *
     * <p>分子是成功落库的行数，分母是数据库写入的累计耗时。
     * 开与关两种模式用同一个口径，才能直接比。
     */
    public double writeThroughputPerSec() {
        long nanos = writeNanos.get();
        if (nanos <= 0) {
            return 0;
        }
        return flushedRows.get() * 1_000_000_000.0 / nanos;
    }

    public long getEnqueued() {
        return enqueued.get();
    }

    public long getDroppedEmpty() {
        return droppedEmpty.get();
    }

    public long getDroppedWithAd() {
        return droppedWithAd.get();
    }

    public long getFlushedRows() {
        return flushedRows.get();
    }

    public long getFlushCount() {
        return flushCount.get();
    }

    public long getLastFlushRows() {
        return lastFlushRows;
    }

    public long getLastFlushCostMs() {
        return lastFlushCostMs;
    }

    public int getQueueSize() {
        return queue == null ? 0 : queue.size();
    }

    public boolean isPaused() {
        return paused.get();
    }

    public boolean isBatchEnabled() {
        return batchOverride != null ? batchOverride : batchEnabled;
    }
}
