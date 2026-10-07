package com.rtb.detail;

import com.rtb.bid.BidLog;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * M7 · 竞价明细异步落库缓冲。
 *
 * <p><b>职责</b>：承接主链路 {@code /bid} 在 return 前 enqueue 的 {@link BidLog}，
 * 由一条常驻后台线程攒批（{@code BATCH_SIZE} 条或 {@code FLUSH_MS} 毫秒触发其一）后
 * 批量 INSERT 进 {@code bid_log}。
 *
 * <p><b>为什么不用同步单条 INSERT</b>：M1 基线每条竞价都同步写一行，写入延迟直接计入链路；
 * 明细属于「可丢、可采样」数据（见《数据库设计说明》§四），没必要为它阻塞主链路。
 * 改成内存队列 + 批量刷盘后，主链路只做一次 {@code offer()}（µs 级），写入吞吐由后台线程决定。
 *
 * <p><b>队列满的处理</b>：用 {@link ArrayBlockingQueue}（有界），容量 {@code QUEUE_CAP}。
 * 队列满时 {@code offer()} 返回 false，直接丢弃这条明细（可采样），<b>绝不阻塞、绝不抛异常</b>。
 * 兜底 / 空广告这类低价值明细本就是最先可丢的；正常负载（数十 QPS）下队列几乎不会满。
 *
 * <p><b>与 billing_event 的分级</b>：计费事件走 Kafka（不可丢），竞价明细走本地队列（可丢）。
 * 同一份数据按「丢了有没有业务后果」分两条不同的管道，这是本项目最核心的设计取舍。
 */
@Slf4j
@Component
public class BidDetailBuffer implements InitializingBean, DisposableBean {

    /** 队列容量：有界，防止流量尖峰把内存打爆 */
    private static final int QUEUE_CAP = 10_000;
    /** 攒够这么多条就触发一次批量刷盘 */
    private static final int BATCH_SIZE = 500;
    /** 或最多等这么久（毫秒）也触发一次刷盘，避免低流量时尾部明细迟迟不落库 */
    private static final long FLUSH_MS = 200;

    private final JdbcTemplate jdbc;
    private final ArrayBlockingQueue<BidLog> queue = new ArrayBlockingQueue<>(QUEUE_CAP);
    private final Thread flusher;

    private volatile boolean running = true;
    private final AtomicLong dropped = new AtomicLong(0);
    private final AtomicLong flushed = new AtomicLong(0);

    private static final String SQL =
            "INSERT INTO bid_log (request_id, ad_id, advertiser_id, slot_code, region, age, gender, interests, "
                    + "candidate_cnt, match_cnt, has_ad, win_bid, pay_price, "
                    + "cost_ms, candidate_cost, filter_cost, sort_cost, bill_cost, bid_at) "
                    + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

    public BidDetailBuffer(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
        this.flusher = new Thread(this::loop, "bid-detail-flusher");
        this.flusher.setDaemon(true);
    }

    @Override
    public void afterPropertiesSet() {
        this.flusher.start();
    }

    @Override
    public void destroy() {
        running = false;
        flusher.interrupt();
        // 退出前尽力把残余明细刷完（最多分批刷）
        while (!queue.isEmpty()) {
            drainUpTo(BATCH_SIZE);
        }
    }

    /**
     * 主链路调用：入队一条竞价明细。队列满则丢弃（可采样），恒不阻塞主链路。
     */
    public void enqueue(BidLog detail) {
        if (!queue.offer(detail)) {
            long d = dropped.incrementAndGet();
            if (d % 1000 == 1) {
                log.warn("bid_log 队列已满(容量{})，丢弃可采样明细，累计 dropped={}", QUEUE_CAP, d);
            }
        }
    }

    /** 暴露给 Actuator / 脚本观测的计数 */
    public long droppedTotal() {
        return dropped.get();
    }

    public long flushedTotal() {
        return flushed.get();
    }

    private void loop() {
        while (running) {
            try {
                // 阻塞等待下一条，最多 FLUSH_MS；超时说明没攒满也要刷一次
                BidLog head = queue.poll(FLUSH_MS, TimeUnit.MILLISECONDS);
                if (head == null) {
                    drainUpTo(BATCH_SIZE);
                    continue;
                }
                List<BidLog> batch = new ArrayList<>(BATCH_SIZE);
                batch.add(head);
                queue.drainTo(batch, BATCH_SIZE - 1);   // 再凑一批，最多补到 BATCH_SIZE-1
                flush(batch);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("bid-detail-flusher 异常: {}", e.getMessage());
            }
        }
        // 线程退出（running=false 或被中断）：把残余明细全部刷完
        while (!queue.isEmpty()) {
            drainUpTo(BATCH_SIZE);
        }
    }

    /** 从队列取出至多 max 条并批量落库 */
    private void drainUpTo(int max) {
        List<BidLog> batch = new ArrayList<>(max);
        queue.drainTo(batch, max);
        if (!batch.isEmpty()) {
            flush(batch);
        }
    }

    private void flush(List<BidLog> batch) {
        int n = batch.size();
        List<Object[]> params = new ArrayList<>(n);
        for (BidLog l : batch) {
            params.add(new Object[]{
                    l.getRequestId(), l.getAdId(), l.getAdvertiserId(), l.getSlotCode(),
                    l.getRegion(), l.getAge(), l.getGender(), l.getInterests(),
                    l.getCandidateCnt(), l.getMatchCnt(), l.getHasAd(),
                    l.getWinBid(), l.getPayPrice(),
                    l.getCostMs(), l.getCandidateCost(), l.getFilterCost(), l.getSortCost(), l.getBillCost(),
                    l.getBidAt()
            });
        }
        try {
            jdbc.batchUpdate(SQL, params);
            flushed.addAndGet(n);
        } catch (Exception e) {
            // 落库失败：丢弃本批（明细可丢），记录但不影响主链路
            log.error("批量落 bid_log 失败（丢弃本批 {} 条）: {}", n, e.getMessage());
        }
    }
}
