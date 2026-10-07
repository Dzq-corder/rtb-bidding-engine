package com.rtb.counter;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

/**
 * 消费组：billing-recon（计费对账）。
 *
 * <p>职责：把每条竞价事件的实扣金额（payPrice）落 {@code billing_event}，
 * 事后与 Redis 预算消耗对账，验证"扣的钱 = 该收的钱"。
 *
 * <p><b>幂等（验收核心：重复投递下对账差异为 0）</b>：
 * 先 {@code INSERT IGNORE processed_event(event_id, 'billing-recon')}，
 * 影响行数=0 说明该事件本组已处理过，直接跳过；再写 {@code billing_event}
 * （唯一键 event_id，重复投递 {@code ON DUPLICATE KEY UPDATE} 不产生重复账单）。
 */
@Slf4j
@Component
public class BillingReconConsumer {

    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;

    public BillingReconConsumer(JdbcTemplate jdbc, ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(groupId = "billing-recon", topics = "${rtb.kafka.topic:bid_event}")
    public void onEvent(ConsumerRecord<String, String> rec, Acknowledgment ack) {
        try {
            BidEvent e = objectMapper.readValue(rec.value(), BidEvent.class);
            // 1) 幂等去重：本组已处理过则跳过
            int inserted = jdbc.update(
                    "INSERT IGNORE INTO processed_event(event_id, grp) VALUES(?, ?)",
                    e.getEventId(), "billing-recon");
            if (inserted == 0) {
                ack.acknowledge();
                return;
            }
            // 2) 落计费事件（唯一键 event_id 防重复记账）
            jdbc.update(
                    "INSERT INTO billing_event(event_id, ad_id, advertiser_id, pay_price, ts) "
                            + "VALUES(?,?,?,?,?) "
                            + "ON DUPLICATE KEY UPDATE pay_price=VALUES(pay_price)",
                    e.getEventId(),
                    e.getAdId(),
                    e.getAdvertiserId(),
                    e.getPayPrice(),
                    e.getTs());
            ack.acknowledge();
        } catch (Exception ex) {
            log.error("billing-recon 处理失败（未 ack，将重投）event={}: {}", rec.value(), ex.getMessage());
            // 不 ack → 消息重投，保证至少一次投递
        }
    }
}
