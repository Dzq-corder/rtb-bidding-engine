package com.rtb.log;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 竞价明细落库缓冲的观测与控制端点 · M7
 *
 * <p>存在理由与 M6 的 {@code /kafka/*} 一样：<b>「写入不反压主链路」这句话
 * 本身没有信息量，必须能现场演示</b>。
 *
 * <p>两个关键演示：
 * <ol>
 *   <li><b>A→B 对比</b>：{@code /logbuf/batch?enabled=false} 退回同步逐条写，
 *       跑一轮压测记下吞吐；再 {@code enabled=true} 跑一轮。同一进程内对比，
 *       排除 JVM 预热、连接池状态等干扰</li>
 *   <li><b>丢弃策略真的会触发</b>：{@code /logbuf/pause} 停掉刷盘 → 队列填满 →
 *       观察 {@code droppedEmpty} 增长，同时竞价 QPS 不受影响。
 *       <b>降级分支如果不验证，那段代码等于没写过</b>（M5、M6 都踩过这个坑）</li>
 * </ol>
 */
@RestController
@RequestMapping("/logbuf")
public class LogBufferController {

    private final BidLogBuffer buffer;

    public LogBufferController(BidLogBuffer buffer) {
        this.buffer = buffer;
    }

    @GetMapping("/stats")
    public Map<String, Object> stats() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("batchEnabled", buffer.isBatchEnabled());
        m.put("paused", buffer.isPaused());
        m.put("queueSize", buffer.getQueueSize());
        m.put("enqueued", buffer.getEnqueued());
        m.put("flushedRows", buffer.getFlushedRows());
        m.put("flushCount", buffer.getFlushCount());
        m.put("lastFlushRows", buffer.getLastFlushRows());
        m.put("lastFlushCostMs", buffer.getLastFlushCostMs());
        m.put("droppedEmpty", buffer.getDroppedEmpty());
        m.put("droppedWithAd", buffer.getDroppedWithAd());
        m.put("writeThroughputPerSec", round(buffer.writeThroughputPerSec()));
        return m;
    }

    /**
     * 切换批量刷盘。
     *
     * <p>切换前先 {@code flushNow()}：<b>不让上一轮的数据混进下一轮的计数</b>，
     * 否则「由 A 提升至 B」里的 A 会被 B 的残留污染。
     */
    @PostMapping("/batch")
    public Map<String, Object> setBatch(@RequestParam boolean enabled) {
        buffer.flushNow();
        buffer.setBatchEnabled(enabled);
        return stats();
    }

    /** 暂停刷盘：用于让队列真的填满，验证丢弃策略。 */
    @PostMapping("/pause")
    public Map<String, Object> pause() {
        buffer.setPaused(true);
        return stats();
    }

    /** 恢复刷盘。 */
    @PostMapping("/resume")
    public Map<String, Object> resume() {
        buffer.setPaused(false);
        return stats();
    }

    /** 立即刷掉队列里现有的数据。 */
    @PostMapping("/flush")
    public Map<String, Object> flush() {
        buffer.flushNow();
        return stats();
    }

    /**
     * 写入路径基准测试：{@code rows} 行，单线程顺序写，只换「逐条 / 批量」。
     *
     * <p>与压测统计分开的理由写在 {@link BidLogBuffer#bench} 的注释里——
     * 压测时两种模式的并发度不同，累加耗时不能直接相除比较。
     */
    @PostMapping("/bench")
    public Map<String, Object> bench(@RequestParam(defaultValue = "5000") int rows,
                                     @RequestParam(defaultValue = "true") boolean batch) {
        return buffer.bench(rows, batch);
    }

    /** 清零计数：A→B 两轮各测一次，避免用「前后相减」引入空档误差。 */
    @PostMapping("/reset")
    public Map<String, Object> reset() {
        buffer.resetCounters();
        return stats();
    }

    private static double round(double v) {
        return Math.round(v * 100.0) / 100.0;
    }
}
