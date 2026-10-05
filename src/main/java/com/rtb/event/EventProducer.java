package com.rtb.event;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

/**
 * 事件生产者 · M6
 *
 * <p><b>最关键的设计：投递失败绝不影响竞价链路</b>
 *
 * <p>这是整个 M6 的核心取舍。实现方式是
 * {@link #publish} <b>不做任何阻塞等待</b>——
 * 不调 {@code get()}、不调 {@code join()}，
 * 只注册一个回调然后立刻返回。
 *
 * <p><b>为什么这样选</b>
 * 同步竞价链路的预算是 100ms（M5）。而一次 Kafka 投递：
 * <ul>
 *   <li>正常情况：0.2~2ms（同机房网络）</li>
 *   <li>Broker 负载高或GC：可能 10~50ms</li>
 *   <li>Broker 假死/网络抖动：会一直阻塞到 {@code max.block.ms}（默认 60s）</li>
 * </ul>
 *
 * <p>最后那一种是致命的：<b>Kafka 抖动会直接吃掉整个超时预算，
 * 让竞价从毫秒级变成秒级</b>。这正是本项目从头到尾坚持的原则：
 * 同步链路不经 MQ。
 *
 * <p><b>但「不阻塞」不等于「不管」</b>：
 * 失败会进回调，日志 ERROR 级别，并累加计数器。
 * 真实系统里还应该把失败事件落本地表再补偿——
 * 本项目没做，已列入遗留（见 metrics M6）。
 */
@Slf4j
@Component
public class EventProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final EventCodec codec;

    @Value("${rtb.kafka.topic:bid_event}")
    private String topic;

    /**
     * 是否启用旁路。
     *
     * <p><b>为什么要有这个开关</b>：M6 之前的事件是同步写库的。
     * 删掉 M6 之后要能一键关掉 Kafka，
     * 否则「回退到 M5 状态」需要改代码。
     */
    @Value("${rtb.kafka.enabled:true}")
    private boolean enabled;

    /** 投递成功计数 */
    private final java.util.concurrent.atomic.AtomicLong sentCount =
            new java.util.concurrent.atomic.AtomicLong();

    /** 投递失败计数 */
    private final java.util.concurrent.atomic.AtomicLong failedCount =
            new java.util.concurrent.atomic.AtomicLong();

    public EventProducer(KafkaTemplate<String, String> kafkaTemplate, EventCodec codec) {
        this.kafkaTemplate = kafkaTemplate;
        this.codec = codec;
    }

    /**
     * 投递竞价事件 —— <b>永不阻塞、永不抛异常</b>。
     *
     * @return true 表示已交给Kafka 发送缓冲（不代表已到达 Broker）
     */
    public boolean publish(BidEvent event) {
        if (!enabled) {
            return false;
        }

        long t0 = System.nanoTime();
        try {
            String payload = codec.encode(event);

            // 分区键用 adId 的字符串形式。
            // 为什么用 adId 而不�� requestId：
            //   「同一广告的曝光/点击必须有序」是业务要求——
            //   频次反作弊要按「这个用户对这条广告的曝光序列」判断，
            //   顺序乱了会把连续 5 次曝光算成 5 个不同序列。
            //   requestId 作分区键只能保证「同一次竞价的事件」有序，
            //   但一次竞价本来就只有一个事件，起不到作用。
            String partitionKey = String.valueOf(event.getAdId());

            CompletableFuture<SendResult<String, String>> future =
                    kafkaTemplate.send(topic, partitionKey, payload);

            future.whenComplete((result, ex) -> {
                if (ex != null) {
                    failedCount.incrementAndGet();
                    // 失败必须留痕：这是「钱的事实」的投递失败，
                    // 线上出现这个日志要立刻告警
                    log.error("事件投递失败 eventId={} adId={}: {}",
                            event.getEventId(), event.getAdId(), ex.getMessage());
                } else {
                    sentCount.incrementAndGet();
                    long costUs = (System.nanoTime() - t0) / 1_000;
                    // 投递耗时单独打点：这是判断「旁路是否反压同步链路」的关键指标
                    log.debug("事件投递成功 eventId={} partition={} offset={} {} us",
                            event.getEventId(),
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset(),
                            costUs);
                }
            });

            return true;

        } catch (Exception e) {
            // 无论是序列化失败（代码 bug）还是发送异常（基础设施问题），都不许抛出去。
            // 抛出就会让竞价返回 500——为一个「事后可补偿」的事件搞挂同步链路，
            // 是本末倒置。
            failedCount.incrementAndGet();
            log.error("事件投递失败 eventId={} adId={} type={}: {}",
                    event.getEventId(), event.getAdId(), e.getClass().getSimpleName(),
                    e.getMessage());
            return false;
        }
    }

    public long getSentCount() {
        return sentCount.get();
    }

    public long getFailedCount() {
        return failedCount.get();
    }

    public boolean isEnabled() {
        return enabled;
    }
}
