package com.rtb.counter;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * 竞价事件发布者（M6 旁路）。
 *
 * <p><b>设计铁律：旁路不可影响主链路。</b> 发送是 fire-and-forget：
 * <ul>
 *   <li>仅把消息交给 Kafka 生产者缓冲即返回，不等 broker 确认，不增加 {@code /bid} 延迟；</li>
 *   <li>整个发送包在 try-catch 中，Kafka 挂了 / 序列化失败 / 超时都只记日志，<b>绝不抛异常、绝不阻塞返回</b>；</li>
 *   <li>分区键 = adId（兜底用 "fallback"），保证同一广告的事件落同一分区、严格有序。</li>
 * </ul>
 */
@Slf4j
@Component
public class BidEventPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String topic;

    public BidEventPublisher(KafkaTemplate<String, String> kafkaTemplate,
                            ObjectMapper objectMapper,
                            @Value("${rtb.kafka.topic:bid_event}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.topic = topic;
    }

    public void publish(BidEvent event) {
        try {
            String key = event.getAdId() != null ? String.valueOf(event.getAdId()) : "fallback";
            String value = objectMapper.writeValueAsString(event);
            // CompletableFuture：发送失败仅记日志，绝不抛异常、不影响主链路
            kafkaTemplate.send(topic, key, value).whenComplete((result, ex) -> {
                if (ex != null) {
                    log.warn("发送竞价事件失败（旁路忽略，不影响竞价返回）eventId={}: {}",
                            event.getEventId(), ex.getMessage());
                }
            });
        } catch (Exception e) {
            // 序列化等异常同样被忽略，保证主链路不受旁路故障牵连
            log.warn("构造竞价事件异常（旁路忽略）eventId={}: {}", event.getEventId(), e.getMessage());
        }
    }
}
