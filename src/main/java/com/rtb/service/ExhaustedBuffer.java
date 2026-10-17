package com.rtb.service;

import com.rtb.mapper.AdMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * M9 · 「广告预算耗尽」状态回写缓冲。
 *
 * <p><b>职责</b>：承接竞价主链路在计费相位结束后投递的「余额不足」广告 id，
 * 由一条常驻后台线程攒批后一次性 {@code UPDATE ad SET status = 3 WHERE id IN (...)}，
 * 把 N 次数据库往返压成 1 次。
 *
 * <p><b>为什么不用同步逐条写</b>：{@code status = 3} 只是<b>检索优化</b>，不是正确性依赖——
 * 真正的「能不能扣、扣多少」由 Redis 的 Lua 脚本每次请求实时判定，本状态的作用仅仅是让
 * 后续检索的 {@code WHERE a.status = 1} 少返回一条。既然丢了也没有业务后果，就不该为它
 * 阻塞请求线程（RTB 的出价响应窗口是百毫秒级）、也不该为它反复消耗 Hikari 连接与 fsync。
 *
 * <p><b>为什么攒批而不是「每请求开线程立即写」</b>：延迟 {@code FLUSH_MS}（50ms）的代价是
 * 「这 50ms 内同一广告会被多查一次、多跑一次 Lua 并再次返回余额不足」，即多一次 Redis
 * 往返（亚毫秒级），业务后果为零；而逐条 UPDATE 的成本是每次一个网络往返 + 一次 redo fsync
 * + 一次连接借还，是真实的数据库压力。用一个几乎无代价的延迟换掉 N 倍写入压力，是划算的。
 *
 * <p><b>队列满的处理</b>：{@link ArrayBlockingQueue} 有界（{@code QUEUE_CAP}），
 * 满了 {@code offer()} 返回 false，直接丢弃，<b>绝不阻塞、绝不抛异常</b>。
 * 丢弃的后果仅是「该广告下次请求时还会被查到、再判一次余额不足」，可自愈。
 *
 * <p><b>与 {@link BidDetailBuffer} 的分工</b>：两者都是「可丢数据 + 本地队列 + 后台批量落库」，
 * 区别只在本类攒批窗口更短（50ms vs 200ms）、批次更小（100 vs 500）——因为状态回写
 * 虽不要求实时，但希望尽快生效以早日减少无效检索。
 */
@Slf4j
@Component
public class ExhaustedBuffer implements InitializingBean, DisposableBean {

    /** 队列容量：有界，防止流量尖峰把内存打爆 */
    private static final int QUEUE_CAP = 10_000;
    /** 攒够这么多条就触发一次批量 UPDATE */
    private static final int BATCH_SIZE = 100;
    /** 或最多等这么久（毫秒）也触发一次，保证低流量时单条也能在 50ms 内落库 */
    private static final long FLUSH_MS = 50;

    private final AdMapper adMapper;

    private final ArrayBlockingQueue<Long> queue = new ArrayBlockingQueue<>(QUEUE_CAP);
    private final Thread flusher;

    /** 停止信号：destroy() 置 false，让后台线程自行退出 */
    private volatile boolean running = true;

    /** 因队列满被丢弃的 id 计数 */
    private final AtomicLong dropped = new AtomicLong(0);
    /** 真正由 status=1 翻成 3 的行数（重复投递的 id 第二次不计入） */
    private final AtomicLong flushed = new AtomicLong(0);

    public ExhaustedBuffer(AdMapper adMapper) {
        this.adMapper = adMapper;
        this.flusher = new Thread(this::loop, "ad-exhausted-flusher");
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
        // 退出前尽力把残余 id 刷完（分批，避免一次 IN 过长）
        while (!queue.isEmpty()) {
            drainUpTo(BATCH_SIZE);
        }
    }

    /**
     * 主链路调用：入队一个「余额不足」的广告 id。队列满则丢弃，恒不阻塞主链路。
     *
     * <p><b>调用约束</b>：只允许投递「Redis 正常返回余额不足」的 id。
     * Redis 超时/连接异常（基础设施故障）时<b>绝不能</b>投递——否则 Redis 抖动一次
     * 就会把大批正常广告永久标成耗尽。
     */
    public void enqueue(Long adId) {
        if (!queue.offer(adId)) {
            long d = dropped.incrementAndGet();
            if (d % 1000 == 1) {
                log.warn("ad.status 回写队列已满(容量{})，丢弃 id={}，累计 dropped={}", QUEUE_CAP, adId, d);
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

    /**
     * 后台刷盘线程主循环：攒批上限 + 停留预算。
     * 取到第一条后进入「攒批窗口」：尽量凑满 {@code BATCH_SIZE}，但自第一条起总停留
     * 不超过 {@code FLUSH_MS}，二者先到者触发 flush。
     */
    private void loop() {
        // 用 LinkedHashSet 作批次容器：同一批内重复的 adId 自动去重（多请求可能命中同一广告），
        // 避免 IN 列表里出现重复 id 浪费 SQL 长度，同时保持插入顺序便于排查
        Set<Long> batch = new LinkedHashSet<>(BATCH_SIZE);
        while (running) {
            try {
                // 1) 空闲等待第一条：最多 FLUSH_MS 没数据就继续等（空闲心跳）
                Long first = queue.poll(FLUSH_MS, TimeUnit.MILLISECONDS);
                if (first == null) {
                    if (!batch.isEmpty()) {          // 防御性：刷掉残留（正常不会进这里）
                        flush(batch);
                        batch.clear();
                    }
                    continue;
                }
                batch.add(first);

                // 2) 把队列里立即可用的尽量捞过来（突发流量下直接凑满一批）
                queue.drainTo(batch, BATCH_SIZE - batch.size());
                if (batch.size() >= BATCH_SIZE) {
                    flush(batch);
                    batch.clear();
                    continue;
                }

                // 3) 没满：在剩余停留预算内继续等数据顶满。
                //    以 FLUSH_MS 作为「自第一条起算」的总预算，而非每条都续期，
                //    否则低流量下每来一条就把等待再拖 50ms，永远刷不出去。
                long deadline = System.nanoTime() + FLUSH_MS * 1_000_000L;
                while (batch.size() < BATCH_SIZE) {
                    long remain = deadline - System.nanoTime();
                    if (remain <= 0) break;
                    Long next = queue.poll(remain, TimeUnit.NANOSECONDS);
                    if (next == null) break;
                    batch.add(next);
                }
                flush(batch);
                batch.clear();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("ad-exhausted-flusher 异常: {}", e.getMessage());
                batch.clear();                        // 清空本批，避免坏数据反复刷导致卡死
            }
        }
        // 线程退出：把残余全部刷完
        if (!batch.isEmpty()) {
            flush(batch);
            batch.clear();
        }
        while (!queue.isEmpty()) {
            drainUpTo(BATCH_SIZE);
        }
    }

    /** 从队列取出至多 max 条并批量落库 */
    private void drainUpTo(int max) {
        List<Long> tmp = new ArrayList<>(max);
        queue.drainTo(tmp, max);
        if (!tmp.isEmpty()) {
            Set<Long> batch = new LinkedHashSet<>(tmp);
            flush(batch);
        }
    }

    private void flush(Set<Long> batch) {
        int n = batch.size();
        if (n == 0) {
            return;
        }
        try {
            int rows = adMapper.markExhaustedBatch(new ArrayList<>(batch));
            flushed.addAndGet(rows);
        } catch (Exception e) {
            // 回写失败：丢弃本批（可丢数据），记录但不影响主链路，也不重试
            // （下次对该广告的请求会再判一次余额不足并重新投递，天然自愈）
            log.error("批量回写 ad.status 失败（丢弃本批 {} 条）: {}", n, e.getMessage());
        }
    }
}
