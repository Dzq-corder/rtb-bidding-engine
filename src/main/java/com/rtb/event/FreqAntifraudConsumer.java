package com.rtb.event;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

/**
 * 消费组三：频次与反作弊 · M6
 *
 * <p><b>这个消费组展示的是「同一份事件流，不同消费组互不干扰」</b>
 *
 * <p>三个消费组读的是<b>同一个 topic 的同一批消息</b>，
 * 但各��独立的消费组、独立的位点、独立的处理逻辑。
 * 停掉任何一个，另外两个照常工作——<b>这就是解耦的具体形态</b>。
 *
 * <p><b>频次判断为什么放 Redis 而不是 MySQL</b>
 *
 * <p>频次要回答「这个用户多久看到过这条广告」，是毫秒级读写的实时判断。
 * 走 MySQL 的话每次曝光一次读写，QPS 直接被数据库拖住，
 * 而且这个判断的<b>时延会传导到竞价链路</b>。
 *
 * <p>所以：<b>实时频次在 Redis，小时打点快照在 MySQL</b>。
 * Redis 挂了不影响主流程——频次只是风控辅助信号，
 * 拿不到就降级为「不拦截」，不能因为风控数据缺失就丢流量。
 */
@Slf4j
@Component
public class FreqAntifraudConsumer {

    private final EventCodec codec;
    private final ConsumerAuditMapper audit;
    private final FreqAntifraudMapper mapper;
    private final StringRedisTemplate redis;

    /** 频次上限：同一用户对同一广告，1 小时内超过这次数就计数为异常 */
    @Value("${rtb.freq.threshold:5}")
    private int threshold;

    private static final Duration TTL = Duration.ofHours(2);

    /** 累计被判定为超频的事件数 */
    private final java.util.concurrent.atomic.AtomicLong overFreqCount =
            new java.util.concurrent.atomic.AtomicLong();

    /** 累计处理的事件数 */
    private final java.util.concurrent.atomic.AtomicLong processedCount =
            new java.util.concurrent.atomic.AtomicLong();

    /** 待刷盘的小时打点批次 */
    private final List<FreqRecord> pending = new ArrayList<>();

    public FreqAntifraudConsumer(EventCodec codec,
                                 FreqAntifraudMapper mapper,
                                 ConsumerAuditMapper audit,
                                 StringRedisTemplate redis) {
        this.codec = codec;
        this.mapper = mapper;
        this.audit = audit;
        this.redis = redis;
    }

    @KafkaListener(
            topics = "${rtb.kafka.topic:bid_event}",
            groupId = "freq-antifraud",
            id = "freq-antifraud",
            containerFactory = "kafkaBatchContainerFactory")
    public void onMessage(@Payload String payload,
                          ConsumerRecord<String, String> record,
                          Acknowledgment ack) {
        long t0 = System.nanoTime();
        String eventId = "unknown";
        try {
            BidEvent e = codec.decode(payload);
            eventId = e.getEventId();

            if (!e.isHasAd() || e.getAdId() == null || e.getUserId() == null) {
                ack.acknowledge();
                return;
            }

            // ---- 实时频次：Redis 原子自增 ----
            //
            // 【为什么用 INCR 而不是 INCRBY + 业务判断】
            // INCR 是原子的，返回值就是自增后的值。
            // 如果先 GET 再 INCR，中间会被别的请求插入，
            // 判断就基于了过期的值。INCR 一次搞定。
            String key = "freq:" + e.getUserId() + ":" + e.getAdId();
            Long cnt;
            try {
                cnt = redis.opsForValue().increment(key);
                if (cnt != null && cnt == 1L) {
                    // 第一次出现时设过期，2 小时后自动清理
                    redis.expire(key, TTL);
                }
            } catch (Exception ex) {
                // Redis 挂了不能影响风控主流程：
                // 频次只是辅助信号，拿不到就降级为「不拦截」
                cnt = null;
            }

            boolean over = cnt != null && cnt > threshold;
            if (over) {
                overFreqCount.incrementAndGet();
                log.warn("频次超限 userId={} adId={} 1小时内{} 次（阈值{}）",
                        e.getUserId(), e.getAdId(), cnt, threshold);
            }

            // ---- 小时打点入内存批次 ----
            pending.add(new FreqRecord(e.getAdId(), e.getUserId(),
                    e.getBidAt() == null
                            ? LocalDateTime.now().truncatedTo(ChronoUnit.HOURS)
                            : e.getBidAt().truncatedTo(ChronoUnit.HOURS)));

            processedCount.incrementAndGet();
            int costMs = (int) ((System.nanoTime() - t0) / 1_000_000);
            audit.record("freq-antifraud", record.partition(), record.offset(),
                    eventId, over ? "OVER_FREQ" : "PROCESSED", costMs,
                    LocalDateTime.now());
            ack.acknowledge();

        } catch (Exception e) {
            int costMs = (int) ((System.nanoTime() - t0) / 1_000_000);
            log.error("频次消费失败 eventId={} partition={} offset={}，本条不提交位点",
                    eventId, record.partition(), record.offset(), e.getMessage(), e);
            audit.recordSafe("freq-antifraud", record.partition(), record.offset(),
                    eventId, "FAILED", costMs, LocalDateTime.now(), e.getMessage());
            throw new RuntimeException("频次消费失败", e);
        }
    }

    /**
     * 把待处理的小时打点批量写入 MySQL。
     *
     * <p><b>为什么必须批量</b>：频次事件的量级和竞价事件一样大，
     * 逐条写 MySQL 会让这个消费组成为整个系统最慢的一环。
     * 而打点数据「丢了能重算」，没有实时性要求——
     * <b>所以攒到一定量再批量刷，是这个场景下最合理的选择</b>。
     */
    public int flush() {
        if (pending.isEmpty()) {
            return 0;
        }
        List<FreqRecord> batch = new ArrayList<>(pending);
        pending.clear();
        int rows = 0;
        // 刷盘时刻由 Java 取一次即可，全批共用：
        // 这批数据的语义是「在这一刻打点」，逐条取时间反而没有意义。
        java.time.LocalDateTime now = LocalDateTime.now();
        for (FreqRecord r : batch) {
            try {
                mapper.upsert(r.adId, r.userId, r.statHour, now);
                rows++;
            } catch (Exception e) {
                log.error("频次打点写入失败 adId={} userId={}: {}",
                        r.adId, r.userId, e.getMessage());
            }
        }
        return rows;
    }

    public long getOverFreqCount() {
        return overFreqCount.get();
    }

    public long getProcessedCount() {
        return processedCount.get();
    }

    public int getPendingSize() {
        return pending.size();
    }

    private static class FreqRecord {
        final Long adId;
        final String userId;
        final LocalDateTime statHour;

        FreqRecord(Long adId, String userId, LocalDateTime statHour) {
            this.adId = adId;
            this.userId = userId;
            this.statHour = statHour;
        }
    }
}
