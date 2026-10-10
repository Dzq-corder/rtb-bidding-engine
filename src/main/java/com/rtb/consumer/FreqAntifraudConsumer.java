package com.rtb.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import com.rtb.dto.BidEvent;
import com.rtb.dto.BidRequest;

/**
 * 消费组：freq-antifraud（频次与反作弊）。
 *
 * <p>职责：按 {@code ad_id} 累计曝光频次，落 {@code ad_freq}，用于识别异常曝光行为。
 *
 * <p><b>维度简化说明（诚实标注）</b>：m1-explore 线当前 {@code BidRequest} 无 userId，
 * 无法按"用户"维度统计频次，此处退而按 {@code ad_id} 维度计数（功能模式——消费组/幂等/lag——
 * 完全真实，仅聚合维度简化）。真实生产应按 userId 维度。
 *
 * <p>幂等同另两组：先去重表，再 upsert 累加。
 */
@Slf4j
@Component
public class FreqAntifraudConsumer {

    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;

    public FreqAntifraudConsumer(JdbcTemplate jdbc, ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(groupId = "freq-antifraud", topics = "${rtb.kafka.topic:bid_event}")
    @Transactional
    public void onEvent(ConsumerRecord<String, String> rec, Acknowledgment ack) {
        try {
            BidEvent e = objectMapper.readValue(rec.value(), BidEvent.class);
            int inserted = jdbc.update(
                    "INSERT IGNORE INTO processed_event(event_id, grp) VALUES(?, ?)",
                    e.getEventId(), "freq-antifraud");
            if (inserted == 0) {
                ack.acknowledge();
                return;
            }
            jdbc.update(
                    "INSERT INTO ad_freq(ad_id, cnt) VALUES(?,1) ON DUPLICATE KEY UPDATE cnt=cnt+1",
                    e.getAdId());
            ack.acknowledge();
        } catch (Exception ex) {
            log.error("freq-antifraud 处理失败（未 ack，将重投）event={}: {}", rec.value(), ex.getMessage());
            // 重新抛出：触发 @Transactional 回滚，去重标记与频次累加同进同退；不 ack → 重投重试
            throw new RuntimeException(ex);
        }
    }
}
