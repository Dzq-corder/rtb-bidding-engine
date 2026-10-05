package com.rtb.event;

import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 预聚合刷盘调度 · M6
 *
 * <p><b>为什么用定时刷而不是每条都写 MySQL</b>
 *
 * <p>预聚合事件的量级和竞价事件一样大（每秒几百到几千条）。
 * 每条都 UPDATEMySQL 的话，这个消费组会成为整个系统最慢的一环。
 *
 * <p>而预聚合数据的性质是<b>「丢了能重算」</b>——
 * 按分区重放就能恢复。这条判断直接来自项目里那条原则：
 * <b>按业务后果分级</b>。计费事件不能丢所以同步写，
 * 预聚合能重算所以攒批异步写。
 *
 * <p>⚠️ <b>但要诚实说明这个取舍的代价</b>：
 * 刷盘间隔内进程崩溃，这段时间的预聚合数据会丢。
 * 真实系统里的做法是双保险：
 * <ul>
 *   <li>刷盘前把内存快照写到本地文件（M7 的有界队列正好干这个）</li>
 *   <li>或者干脆不落库，查询时按分区回放 Kafka 重算</li>
 * </ul>
 * 本项目没做，已列入遗留。
 */
@Slf4j
@Component
public class PreaggScheduler {

    private final HourlyPreaggConsumer preagg;
    private final FreqAntifraudConsumer freq;

    public PreaggScheduler(HourlyPreaggConsumer preagg, FreqAntifraudConsumer freq) {
        this.preagg = preagg;
        this.freq = freq;
    }

    /**
     * 每 5 秒刷一次预聚合。
     *
     * <p><b>为什么是 5 秒</b>：这是「大盘数据延迟」与「数据库写入压力」的折中。
     * 1 秒刷一次对演示来说更实时，但压测时每秒几万次 UPDATEMySQL
     * 会让数据库成为瓶颈，反而影响主链路的观测数据。
     *
     * <p>演示第6 步需要看到「恢复后数据补上来」的过程，
     * 5 秒的间隔足够看清追赶过程又不至于让人等太久。
     */
    @Scheduled(fixedDelay = 5000)
    public void flushPreagg() {
        try {
            preagg.flush();
        } catch (Exception e) {
            log.error("预聚合定时刷盘异常: {}", e.getMessage());
        }
    }

    /**
     * 每 10 秒刷一次频次打点。
     *
     * <p>比预聚合慢，因为频次打点<b>完全没有实时性要求</b>——
     * 它只用于离线分析「哪些广告被反复曝光」，
     * 延迟一分钟也没关系。
     *
     * <p>刷得越慢越省数据库资源，这正是「按业务后果分级」的直接体现。
     */
    @Scheduled(fixedDelay = 10000)
    public void flushFreq() {
        try {
            freq.flush();
        } catch (Exception e) {
            log.error("频次定时刷盘异常: {}", e.getMessage());
        }
    }
}
