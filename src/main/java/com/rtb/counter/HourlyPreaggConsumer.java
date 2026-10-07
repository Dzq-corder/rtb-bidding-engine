package com.rtb.counter;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * 消费组：hourly-preagg（小时级预聚合）。
 *
 * <p>职责：按 {@code (stat_hour, ad_id)} 累加曝光数与消耗，落 {@code bid_stat_hourly}，
 * 作为大盘统计的数据源（避免每次查询都扫全量明细）。
 *
 * <p>幂等与 billing-recon 一致：先去重表，再 upsert 累加（重复投递只生效一次）。
 */
@Slf4j
@Component
public class HourlyPreaggConsumer {

    private static final DateTimeFormatter HOUR_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:00");

    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;

    public HourlyPreaggConsumer(JdbcTemplate jdbc, ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(groupId = "hourly-preagg", topics = "${rtb.kafka.topic:bid_event}")
    public void onEvent(ConsumerRecord<String, String> rec, Acknowledgment ack) {
        try {
            BidEvent e = objectMapper.readValue(rec.value(), BidEvent.class);
            int inserted = jdbc.update(
                    "INSERT IGNORE INTO processed_event(event_id, grp) VALUES(?, ?)",
                    e.getEventId(), "hourly-preagg");
            if (inserted == 0) {
                ack.acknowledge();
                return;
            }
            String hour = LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(e.getTs()), ZoneId.systemDefault()).format(HOUR_FMT);
            BigDecimal spend = e.getPayPrice() != null ? e.getPayPrice() : BigDecimal.ZERO;
            jdbc.update(
                    "INSERT INTO bid_stat_hourly(stat_hour, ad_id, impressions, spend) VALUES(?,?,1,?) "
                            + "ON DUPLICATE KEY UPDATE impressions=impressions+1, spend=spend+VALUES(spend)",
                    hour, e.getAdId(), spend);
            ack.acknowledge();
        } catch (Exception ex) {
            log.error("hourly-preagg 处理失败（未 ack，将重投）event={}: {}", rec.value(), ex.getMessage());
        }
    }
}
