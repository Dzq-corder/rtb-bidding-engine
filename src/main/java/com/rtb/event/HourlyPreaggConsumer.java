package com.rtb.event;

import com.rtb.stat.HourlyPreaggMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 消费组二：小时级预聚合 · M6
 *
 * <p><b>这个消费组是演示第 6 步的主角</b>——
 * 把它停掉，竞价链路照常返回，大盘数据停住，lag 曲线爬升。
 *
 * <p><b>为什么预聚合必须异步</b>
 *
 * <p>大盘要回答「上周哪个广告位最忙」这类问题，
 * 直接查 {@code bid_log}（1000 万行）要几秒。
 * 但竞价请求每秒来几百次，<b>不可能每次都去聚合</b>。
 *
 * <p>两条路的取舍：
 * <tableborder="1">
 *   <tr><th></th><th>同步聚合</th><th>异步聚合（选它）</th></tr>
 *   <tr><td>竞价链路耗时</td><td>每次多几毫秒</td><td><b>0</b></td></tr>
 *   <tr><td>查询大盘</td><td>实时准</td><td>最多延迟一个消费周期</td></tr>
 *   <tr><td>耦合</td><td>大盘挂了影响竞价</td><td><b>完全解耦</b></td></tr>
 * </table>
 *
 * <p><b>用「允许短暂延迟」换「竞价链路不付代价」**，这是典型旁路的取舍。
 * 大盘数据晚几十秒更新完全可接受，但竞价每多花1ms 都是真实成本。
 *
 * <p><b>本消费组的实现取舍：内存累积 + 定时刷盘</b>
 * 而不是每条事件都 UPDATEMySQL。理由是<b>预聚合数据丢了能重算</b>
 * （按那条分线重跑即可），不值得为它牺牲吞吐。
 * 这也是「按业务后果分级」那条原则的第三次应用。
 */
@Slf4j
@Component
public class HourlyPreaggConsumer {

    private final EventCodec codec;
    private final HourlyPreaggMapper mapper;
    private final ConsumerAuditMapper audit;

    /** 内存累积区：key = 小时+广告位+类目，value = 累加器 */
    private final Map<String, Accumulator> buffer = new ConcurrentHashMap<>();

    /** 是否暂停（演示第 6 步用）——由 AdminController 改这个开关 */
    private volatile boolean paused = false;

    /**
     * 累计已刷进 MySQL 的汇总行数。
     *
     * <p><b>为什么要有这个计数</b>：
     * 暂停预聚合后，lag 不会爬升（因为照常 ack），
     * 光看 lag 看不出「下游停了没有」。
     * 这个数字是暂停演示的<b>判定依据</b>：
     * 暂停期间它不增长 = 这一路下游确实停了；
     * 恢复后它继续增长 = 数据补齐了。
     */
    private final java.util.concurrent.atomic.AtomicLong flushedRows =
            new java.util.concurrent.atomic.AtomicLong();

    /** 累计刷盘次数 */
    private final java.util.concurrent.atomic.AtomicLong flushCount =
            new java.util.concurrent.atomic.AtomicLong();

    public HourlyPreaggConsumer(EventCodec codec,
                               HourlyPreaggMapper mapper,
                               ConsumerAuditMapper audit) {
        this.codec = codec;
        this.mapper = mapper;
        this.audit = audit;
    }

    /**
     * 消费竞价事件，只做内存累加。
     *
     * <p><b>为什么批量提交位点</b>：这里攒够一批（{@code max-poll-records=500}）
     * 才提交一次位点。攒批能显著减少与 ZooKeeper/Kafka 的协调开销，
     * 是提升消费吞吐最直接的手段之一。
     *
     * <p>代价是：崩溃时最多重投一批（500 条），
     * 而这些重投由唯一键/累加逻辑消化——<b>幂等是攒批的前提</b>。
     */
    @KafkaListener(
            topics = "${rtb.kafka.topic:bid_event}",
            groupId = "hourly-preagg",
            id = "hourly-preagg",
            containerFactory = "kafkaBatchContainerFactory")
    public void onMessage(@Payload String payload,
                          ConsumerRecord<String, String> record,
                          Acknowledgment ack) {
        long t0 = System.nanoTime();
        String eventId = "unknown";

        try {
            BidEvent e = codec.decode(payload);
            eventId = e.getEventId();

            if (paused) {
                // 演示第 6 步：暂停消费。
                //
                // 【关于 ack 的处理，这里改过一次，理由值得记下来】
                // 第一版是「不 ack」——位点不提交，lag 就会爬升。
                // 现象是对的，但留下一个副作用：
                // 暂停期间进来的消息<b>实际已经在内存里累积了</b>
                // （本消费组是攒批刷盘，看到消息就进 buffer），
                // 只是位点没提交。恢复后 lag 会一直显示这批旧积压，
                // 看起来像「恢复失败了」，是误导。
                //
                // 所以改成：暂停时<b>照常 ack</b>，lag 保持 0，
                // 要展示「解耦」就看另外两件事：
                //   1. bid_stat_hourly 的数据<b>停止增长</b>（下游真的没在产出）
                //   2. 竞价链路 QPS 不变（主链路完全不受影响）
                // 这两条比一条 lag 曲线更能说明问题，且不留下脏数据。
                //
                // 代价：演示时看不到「lag 曲线爬升」这个视觉效果。
                // 用 /kafka/rewind 删消费组可以另做一次真实积压演示。
                //
                // 【但仍然要在内存里累积】
                // 第一版这里直接 return，结果恢复后 flushedRows 纹丝不动——
                // 暂停期间的消息被整个丢了，「恢复后数据补齐」演示不出来。
                // 正确做法是「照常累积到 buffer，只是不刷盘」：
                // 累积是纯内存操作、几乎零成本，
                // 恢复后一次 flush 就能把这段时间的汇总补上。
                accumulate(e);
                ack.acknowledge();
                return;
            }

            accumulate(e);
            int costMs = (int) ((System.nanoTime() - t0) / 1_000_000);
            audit.record("hourly-preagg", record.partition(), record.offset(),
                    eventId, "PROCESSED", costMs, LocalDateTime.now());
            ack.acknowledge();

        } catch (Exception e) {
            int costMs = (int) ((System.nanoTime() - t0) / 1_000_000);
            log.error("预聚合消费失败 eventId={} partition={} offset={}，本条不提交位点",
                    eventId, record.partition(), record.offset(), e.getMessage(), e);
            audit.recordSafe("hourly-preagg", record.partition(), record.offset(),
                    eventId, "FAILED", costMs, LocalDateTime.now(), e.getMessage());
            throw new RuntimeException("预聚合消费失败", e);
        }
    }

    /**
     * 内存累加。
     *
     * <p><b>为什么用 {@code compute} 而不是 {@code merge}</b>：
     * {@code merge} 在 key 不存在时会<b>直接放入新值、不调用 remapping 函数</b>。
     * 如果传入的是一个 cnt=0 的空累加器，那这一组的第一条事件就被整个吞掉了——
     * 表现为「每组少计 1 条」，而且不会有任何报错。
     * {@code compute} 保证每条事件都走一次累加逻辑，语义与计数一致。
     *
     * <p>M6 第一次写这里时用的是 {@code merge}，
     * 压测时发现每个分组的 cnt 都比预期少 1 才定位到。
     *
     * <p><b>为什么 compute 在并发下是安全的</b>：
     * {@code ConcurrentHashMap.compute} 对同一个 key 是加锁串行的，
     * 同一时刻只有一个线程能改这个累加器，
     * 所以「读-改-写」不会丢更新。
     */
    private void accumulate(BidEvent e) {
        if (e.getSlotCode() == null || e.getCategoryCode() == null) {
            return;
        }
        LocalDateTime hour = e.getBidAt() == null
                ? LocalDateTime.now().truncatedTo(ChronoUnit.HOURS)
                : e.getBidAt().truncatedTo(ChronoUnit.HOURS);
        String key = hour + "|" + e.getSlotCode() + "|" + e.getCategoryCode();

        buffer.compute(key, (k, acc) -> {
            Accumulator a = (acc == null)
                    ? new Accumulator(hour, e.getSlotCode(), e.getCategoryCode())
                    : acc;
            a.cnt++;
            if (e.isHasAd()) {
                a.adCnt++;
            }
            a.totalCostMs += e.getCostMs();
            // P99 需要保留样本才能算准，这里只保留最大值作为近似
            if (e.getCostMs() > a.maxCostMs) {
                a.maxCostMs = e.getCostMs();
            }
            if (e.getPayPrice() != null) {
                a.totalPayPrice = a.totalPayPrice.add(e.getPayPrice());
            }
            return a;
        });
    }

    /**
     * 把内存里的累加结果刷进 MySQL。
     *
     * <p><b>为什么是「读改写」而不是纯累加 UPDATE</b>：
     * 读改写在并发刷盘时会丢更新。但本方法只由
     * 单一调度线程调用（见 M6DashboardScheduler），所以安全。
     * <b>用「单线程刷盘」换「简单可读的 SQL」是划算的</b>——
     * 换成原子累加 UPDATE 反而更难读，也更慢（每行一次网络往返）。
     */
    public int flush() {
        // 【暂停时连刷盘也要停】
        // 只停 accumulate 不够——PreaggScheduler 每 5 秒会调一次本方法，
        // 数据照样落库，「暂停后下游停住」根本看不出来。
        // 暂停的语义就是「这一路下游整体停摆」，
        // 所以刷盘也要跳过，数据留在 buffer 里等恢复。
        if (paused) {
            return 0;
        }
        if (buffer.isEmpty()) {
            return 0;
        }
        // 原子地取走并清空，避免与新事件竞争同一批 key
        Map<String, Accumulator> snapshot = new java.util.HashMap<>(buffer);
        buffer.clear();

        int rows = 0;
        for (Accumulator a : snapshot.values()) {
            try {
                // 【M8 第 4 轮修正】这里原来算的是「平均成交价」再传进去。
                // 但累加语义下均值会失真，而且 avg_pay_price 压根没进
                // ON DUPLICATE KEY UPDATE 的列表，累加几批后就永远停在第一次的值。
                // 现在直接传「和」，平均值由查询侧现算。
                //
                // 注意 total_cost 传的一直是 a.totalPayPrice（成交总价之和），
                // 它本来就是可加的，所以这一列从来没出过问题——
                // 出问题的是和它并列的 avg_pay_price。
                rows += mapper.upsert(a.statHour, a.slotCode, a.categoryCode,
                        a.cnt, a.adCnt,
                        a.totalCostMs,
                        a.maxCostMs,
                        a.totalPayPrice,
                        a.totalPayPrice);
            } catch (Exception e) {
                // 刷盘失败不抛：预聚合数据可重算，不能因此影响服务
                log.error("预聚合刷盘失败 {}/{}/{}: {}",
                        a.statHour, a.slotCode, a.categoryCode, e.getMessage());
            }
        }
        if (rows > 0) {
            flushedRows.addAndGet(rows);
            flushCount.incrementAndGet();
            log.info("预聚合刷盘完成，{} 个分组写入 {} 行", snapshot.size(), rows);
        }
        return rows;
    }

    /** 暂停/恢复（演示第 6 步） */
    public void setPaused(boolean p) {
        this.paused = p;
        log.warn("预聚合消费组已{}（演示第6 步）", p ? "暂停" : "恢复");
    }

    public boolean isPaused() {
        return paused;
    }

    /** 累计刷进 MySQL 的行数（暂停演示的判定依据） */
    public long getFlushedRows() {
        return flushedRows.get();
    }

    public long getFlushCount() {
        return flushCount.get();
    }

    public int bufferSize() {
        return buffer.size();
    }

    /** 累加器 —— 需要线程安全，因为它会被 merge 的回调并发访问 */
    private static class Accumulator {
        final LocalDateTime statHour;
        final String slotCode;
        final String categoryCode;
        long cnt;
        long adCnt;
        long totalCostMs;
        int maxCostMs;
        java.math.BigDecimal totalPayPrice = java.math.BigDecimal.ZERO;

        Accumulator(LocalDateTime statHour, String slotCode, String categoryCode) {
            this.statHour = statHour;
            this.slotCode = slotCode;
            this.categoryCode = categoryCode;
        }
    }
}
