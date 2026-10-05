package com.rtb.event;

import com.rtb.bill.BillingEventMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * 消费组一：计费对账 · M6
 *
 * <p><b>职责</b>：把 Kafka 里的竞价事件落成独立的消费侧计费记录，
 * 作为同步表 {@code billing_event} 的<b>对照物</b>。
 *
 * <p><b>为什么这是「对账」而不是「再记一次账」</b>
 *
 * <p>关键在于两张表是<b>独立写入</b>的：
 * <ul>
 *   <li>{@code billing_event} —— M4 同步链路直接写，<b>代表「钱实际扣了多少」</b></li>
 *   <li>{@code billing_event_consumer} —— 本消费组异步写，
 *       <b>代表「事件流里流过了多少」</b></li>
 * </ul>
 *
 * <p>两表按 ad_id 汇总后差额为 0，说明：
 * <b>每一次扣费都成功发出去了事件，且每一次事件都被消费到了</b>。
 * 这正是「对账差异 0 条」这个验收指标的含义。
 *
 * <p>如果不建独立的消费表，就无法区分「事件没发出去」和「事件发出去了但没消费到」——
 * 而这两种故障的排查方向完全不同：前者要查生产者，后者要查消费组。
 */
@Slf4j
@Component
public class BillingReconConsumer {

    private final EventCodec codec;
    private final BillingEventMapper mapper;
    private final ConsumerAuditMapper audit;

    public BillingReconConsumer(EventCodec codec,
                                BillingEventMapper mapper,
                                ConsumerAuditMapper audit) {
        this.codec = codec;
        this.mapper = mapper;
        this.audit = audit;
    }

    /**
     * 消费竞价事件。
     *
     * <p><b>为什么必须手动提交位点</b>：{@code acks=manual_immediate} 下，
     * <b>不调 {@code ack.acknowledge()} 就不会提交位点</b>，
     * 消息会在重启后被重新投递。这是 at-least-once 的保证。
     *
     * <p><b>ack 与幂等是一对</b>：手动提交 + 至少一次投递 = 一定会重复。
     * 所以幂等不是可选项而是<b>必需项</b>——手动提交换来不丢消息，
     * 代价是可能重复，幂等负责消化重复。**两者必须配套，只做一个就是错的。**
     */
    @KafkaListener(
            topics = "${rtb.kafka.topic:bid_event}",
            groupId = "billing-recon",
            // 【为什么要显式起 id】不写 id 的话 Spring 自动生成
            // 「类名 + 方法名」，想停掉某一个容器就只能靠字符串拼，
            // 拼错了不会报错，只是「没找到容器」然后静默什么也不做。
            // 显式命名后EventAdminController 才能按名字精确停启。
            id = "billing-recon",
            containerFactory = "kafkaBatchContainerFactory")
    public void onMessage(@Payload String payload,
                          ConsumerRecord<String, String> record,
                          Acknowledgment ack) {
        long t0 = System.nanoTime();
        String eventId = "unknown";
        int partition = record.partition();
        long offset = record.offset();

        try {
            BidEvent e = codec.decode(payload);
            eventId = e.getEventId();

            // 统一在这里取一次本地时间：本条记录的所有时间字段都用它，
            // 保证同一行内 billed_at / consumed_at 口径一致。
            //
            // 【必须截断到毫秒】这是 M6 排查最久的一个坑。
            // LocalDateTime.now() 在 Windows 上带微秒精度（如 .990405），
            // 但 consumed_at 列是 datetime(3)（毫秒），
            // MySQL 写入时会把尾数四舍五入掉（变成 .990000）。
            // 于是 Java 侧拿原值去比库里的值，几乎永远不相等，
            // 表现就是「4738 条全新事件里 4733 条被判成重复」。
            // 截断到毫秒后，两侧字面量完全一致。
            java.time.LocalDateTime now =
                    java.time.LocalDateTime.now()
                            .truncatedTo(java.time.temporal.ChronoUnit.MILLIS);

            if (e.getAdId() == null || e.getPayPrice() == null) {
                // 兜底降级时扣费可能失败，事件里没有 adId/payPrice。
                // 这类事件对计费对账没有意义，但不代表「丢失」——
                // 记一笔审计跳过，让口径可解释。
                audit.record("billing-recon", partition, offset, eventId,
                        "SKIPPED_NO_CHARGE", 0, now);
                ack.acknowledge();
                return;
            }

            mapper.insertConsumerEvent(e, "billing-recon", now);
            int costMs = (int) ((System.nanoTime() - t0) / 1_000_000);

            // 【关键】判重必须单独查，且时间要格式化成字符串再比。
            // 1) 不能用 insert 的返回值：驱动默认返回 found_rows
            //    而非 affected_rows，命中唯一键时也是 1。
            // 2) 不能直接传 LocalDateTime 让驱动比：实测那样写
            //    会把几乎所有事件都误判成重复（4712 条里错 4708 条）。
            //    改成与 DATE_FORMAT 同形的字符串后逐字符可比。
            int fresh = mapper.wasInserted(e.getEventId(), formatForCompare(now));

            if (fresh == 1) {
                audit.record("billing-recon", partition, offset, eventId,
                        "PROCESSED", costMs, now);
            } else {
                // 命中唯一键 —— 幂等拦截。
                // 【为什么要记这一笔】幂等是静默的，
                // 不记账就无法证明「重投确实被拦下来了」。
                audit.record("billing-recon", partition, offset, eventId,
                        "DUPLICATE_SKIPPED", costMs, now);
            }

            // 只有处理成功才提交位点
            ack.acknowledge();

        } catch (Exception e) {
            int costMs = (int) ((System.nanoTime() - t0) / 1_000_000);
            // 【关键】失败时绝不能 ack
            // 不 ack → 位点不提交 → 重启后重新消费 → 至少一次投递
            log.error("计费对账消费失败 eventId={} partition={} offset={}，本条不提交位点",
                    eventId, partition, offset, e.getMessage(), e);
            audit.recordSafe("billing-recon", partition, offset, eventId,
                    "FAILED", costMs, java.time.LocalDateTime.now(), e.getMessage());
            throw new RuntimeException("计费对账消费失败", e);
        }
    }

    /**
     * 把时间格式化成与 MySQL {@code DATE_FORMAT('%Y-%m-%d %H:%i:%s.%f')}
     * 完全同形的字符串，用于判重时逐字符比较。
     *
     * <p><b>为什么必须自己格式化</b>：
     * 直接把 {@link java.time.LocalDateTime} 交给驱动去和 datetime 列比较，
     * 实测会出现「刚写进去的值紧接着比却不相等」的情况，
     * 导致 4712 条全新事件里有 4708 条被误判为重复。
     * 根因是驱动对写入参数与比较绑定的处理路径不同。
     * 两边都变成纯字符串后，这个不确定性就消失了。
     *
     * <p><b>传入的时间必须先截断到毫秒</b>：{@code %f} 输出 6 位（微秒）
     * 而列是 {@code datetime(3)}（毫秒）。如果传进来的是带微秒的原值，
     * 写出来是 {@code .990405000}，库里存的却是 {@code .990000000}，永远不相等。
     * 调用方已用 {@code truncatedTo(MILLIS)} 处理，这里只是补零对齐位数。
     */
    private static String formatForCompare(java.time.LocalDateTime t) {
        return t.format(QUERY_COMPARE_FMT);
    }

    private static final java.time.format.DateTimeFormatter QUERY_COMPARE_FMT =
            java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
}
