package com.rtb.event;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 事件旁路管理端点 · M6
 *
 * <p><b>为什么必须有这些端点</b>
 *
 * <p>开发文档写的是「提供暂停/恢复消费者的 Admin 接口（演示第6 步要用）」。
 * 表面看是给演示用的，但实际它承担了一个更重要的作用：
 * <b>让「解耦」这件事变得可验证</b>。
 *
 * <p>「旁路不影响主链路」这句话本身没有信息量，
 * 必须能现场展示：停掉一个消费组 → 竞价照常 → lag 爬升 → 恢复后数据补齐。
 * 没有这套端点，面试官只能选择「相信你」或者「不信任」。
 */
@Slf4j
@RestController
public class EventAdminController {

    private static final String[] GROUPS = {"billing-recon", "hourly-preagg", "freq-antifraud"};

    private final HourlyPreaggConsumer preagg;
    private final FreqAntifraudConsumer freq;
    private final EventProducer producer;
    private final EventReconMapper recon;
    private final KafkaListenerEndpointRegistry registry;

    @Value("${rtb.kafka.topic:bid_event}")
    private String topic;

    @Value("${spring.kafka.bootstrap-servers:127.0.0.1:9092}")
    private String bootstrapServers;

    public EventAdminController(HourlyPreaggConsumer preagg,
                                FreqAntifraudConsumer freq,
                                EventProducer producer,
                                EventReconMapper recon,
                                KafkaListenerEndpointRegistry registry) {
        this.preagg = preagg;
        this.freq = freq;
        this.producer = producer;
        this.recon = recon;
        this.registry = registry;
    }

    // -----------------------------------------------------------------
    // 演示第 6 步：暂停 / 恢复预聚合消费组
    // -----------------------------------------------------------------

    /**
     * 暂停预聚合消费组。
     *
     * <p><b>要观察的现象</b>：竞价照常返回（QPS 不掉），
     * 但 {@code bid_stat_hourly} 这一路的下游产出<b>停住</b>了。
     *
     * <p><b>为什么这里看数据而不是看 lag</b>：
     * 暂停时本消费组仍然 ack（详见 {@code HourlyPreaggConsumer} 的注释），
     * 所以 lag 恒为 0，看不到曲线爬升。这是刻意改过的——
     * 不 ack 会留下「恢复后 lag 一直降不下去」的脏数据，反而误导。
     *
     * <p>要演示积压曲线，用 {@code /kafka/rewind} 删掉消费组，
     * 那是一次真实的重放积压。
     */
    @PostMapping("/kafka/pause-preagg")
    public Map<String, Object> pausePreagg() {
        preagg.setPaused(true);
        return lagReport();
    }

    @PostMapping("/kafka/resume-preagg")
    public Map<String, Object> resumePreagg() {
        preagg.setPaused(false);
        preagg.flush();
        return lagReport();
    }

    // -----------------------------------------------------------------
    // 幂等验证：把消费组位点往回拨，强制重投
    // -----------------------------------------------------------------

    /**
     * 把指定消费组的位点回退，<b>强制重投已消费过的消息</b>。
     *
     * <p><b>为什么必须真的制造一次重投</b>
     *
     * <p>M6 验收里最容易糊弄过去的一点是：
     * 「对账差异 0 条」在<b>没有发生过重投</b>时也能成立。
     * 正常运行时 Kafka 不会重投，唯一键那套逻辑一行都没执行过，
     * 这时候说「幂等已验证」是不成立的。
     *
     * <p>所以这里主动把位点拨回起点，让同一批消息<b>再消费一遍</b>，
     * 观察的是<b>数据结果</b>而不是代码路径：
     * <ul>
     *   <li>{@code billing_event_consumer} 行数<b>不变</b>（唯一键拦住了）</li>
     *   <li>{@code consumer_audit} 里 {@code DUPLICATE_SKIPPED} <b>增长</b>
     *       （证明重投真的发生了，而且被识别成重复）</li>
     * </ul>
     *
     * <p><b>为什么必须先停容器才能改位点</b>
     *
     * <p>这是实测踩到的坑，记录下来：
     * <ul>
     *   <li>用 {@code kafka-consumer-groups.sh --reset-offsets} 会报
     *       {@code Assignments can only be reset if the group is inactive}</li>
     *   <li>改用 AdminClient 的 {@code alterConsumerGroupOffsets} 直接报
     *       {@code UnknownMemberIdException}</li>
     *   <li>停容器后 {@code alterConsumerGroupOffsets} <b>报告成功</b>，
     *       但位点<b>根本没变</b>——消费者重新加入组时
     *       带着内存里的旧位点，把刚写进去的 0 覆盖回去了。</li>
     * </ul>
     *
     * <p>结论：<b>外部进程改不了在线/刚重启消费者的位点</b>。
     * 唯一干净的做法是<b>把消费组删掉</b>——组不存在时，
     * 新加入的消费者按 {@code auto-offset-reset=earliest} 从头消费，
     * 于是全量重放。
     *
     * <p><b>这个流程对应的真实故障</b>：
     * 消费组 offset 提交失败后被人工重置、消费者长时间离线导致位点过期、
     * 或运维误删消费组需要重建。都属于 at-least-once 的固有行为。
     */
    @PostMapping("/kafka/rewind")
    public Map<String, Object> rewind(@RequestParam(defaultValue = "billing-recon") String group) {
        Map<String, Object> out = new LinkedHashMap<>();
        if (!java.util.Arrays.asList(GROUPS).contains(group)) {
            out.put("error", "未知消费组: " + group);
            out.put("allowed", GROUPS);
            return out;
        }

        // 1. 停容器，让消费组离开组、变为 inactive
        java.util.concurrent.CountDownLatch stopped = new java.util.concurrent.CountDownLatch(1);
        registry.stop(() -> stopped.countDown());
        try {
            if (!stopped.await(15, java.util.concurrent.TimeUnit.SECONDS)) {
                out.put("error", "等待容器停止超时");
                return out;
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            out.put("error", "等待容器停止被中断");
            return out;
        }

        try {
            // 2. 删掉消费组。组不存在后，新消费者会按
            //    auto-offset-reset=earliest 从 0 开始消费 —— 这才是真正有效的重投
            try (AdminClient admin = AdminClient.create(
                    java.util.Map.of("bootstrap.servers", bootstrapServers))) {
                admin.deleteConsumerGroups(java.util.List.of(group)).all().get();
            }
        } catch (Exception e) {
            // 组本来就不存在也算成功（幂等）
            log.info("删除消费组 {} 返回: {}", group, e.getMessage());
        } finally {
            // 3. 无论成败都要把容器拉起来，否则消费组永久停摆
            registry.start();
        }

        log.warn("消费组 {} 已被删除并重建（演示：强制全量重投验证幂等）", group);
        out.put("group", group);
        out.put("method", "delete-consumer-group + earliest");
        out.put("note", "容器已重启，会从 offset 0 全量重放。"
                + "等十几秒后看 /kafka/idempotency："
                + "billing-recon 的 DUPLICATE_SKIPPED 应等于本轮事件数，"
                + "而 duplicateEventIds 应仍为空数组");
        out.put("sideEffect", "KafkaListenerEndpointRegistry 的 stop()/start() 是全量的，"
                + "三个消费组会一起停下再一起起来。"
                + "但只有被删组的位点会重置，另两组从原位点继续");
        return out;
    }

    // -----------------------------------------------------------------
    // lag 查询：喂给大盘的 lag 曲线
    // -----------------------------------------------------------------

    /**
     * 各消费组的消费进度与积压。
     *
     * <p><b>lag 怎么算的</b>：
     * {@code lag = 分区最新位点 - 消费组已提交位点}
     *
     * <p><b>为什么这个差值就是「积压的消息数」</b>：
     * 左边是 Broker 里已经写入的最新位置，右边是这个消费组处理到的位置。
     * 中间夹着的消息就是「写进去了但还没处理」的。
     *
     * <p><b>为什么必须手动查而不能等 Kafka 推送</b>：
     * Consumer lag 监控指标（consumer-lag）需要额外装 Prometheus/Exporter。
     * 本项目刻意不引入监控组件（见技术栈约定），
     * 所以用 Admin API 直接查——够用且零依赖。
     */
    @GetMapping("/kafka/lag")
    public Map<String, Object> lag() {
        return lagReport();
    }

    private Map<String, Object> lagReport() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("topic", topic);
        out.put("preaggPaused", preagg.isPaused());
        out.put("sent", producer.getSentCount());
        out.put("failed", producer.getFailedCount());
        // 预聚合实际产出量：暂停演示时看这个数字是否停止增长
        out.put("preaggFlushedRows", preagg.getFlushedRows());
        out.put("preaggFlushCount", preagg.getFlushCount());

        Map<String, Object> groups = new LinkedHashMap<>();
        long totalLag = 0;
        try (AdminClient admin = AdminClient.create(
                java.util.Map.of("bootstrap.servers", bootstrapServers))) {

            int parts = partitionCount(admin);

            // 1. 各分区的最新位点（Broker 端的 log end offset）
            //    注意：Kafka 2.5 的 OffsetSpec.latest() 是无参工厂方法，
            //    「查哪几个分区」由 listOffsets 的入参 Map 决定，不是 latest() 的参数。
            Map<TopicPartition, org.apache.kafka.clients.admin.OffsetSpec> specReq =
                    new HashMap<>();
            for (int p = 0; p < parts; p++) {
                specReq.put(new TopicPartition(topic, p),
                        org.apache.kafka.clients.admin.OffsetSpec.latest());
            }
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets =
                    admin.listOffsets(specReq).all().get();

            // 2. 各消费组已提交位点
            for (String g : GROUPS) {
                Map<TopicPartition, OffsetAndMetadata> committed =
                        admin.listConsumerGroupOffsets(g)
                                .partitionsToOffsetAndMetadata()
                                .get();
                if (committed == null) {
                    committed = Map.of();
                }

                long groupLag = 0;
                List<Map<String, Object>> partList = new ArrayList<>();
                for (int p = 0; p < parts; p++) {
                    TopicPartition tp = new TopicPartition(topic, p);
                    ListOffsetsResult.ListOffsetsResultInfo info = endOffsets.get(tp);
                    long end = info == null ? 0 : info.offset();
                    OffsetAndMetadata om = committed.get(tp);
                    long pos = om == null ? 0 : om.offset();
                    long lag = Math.max(0, end - pos);
                    groupLag += lag;

                    Map<String, Object> pm = new LinkedHashMap<>();
                    pm.put("partition", p);
                    pm.put("latest", end);
                    pm.put("committed", pos);
                    pm.put("lag", lag);
                    partList.add(pm);
                }
                totalLag += groupLag;

                Map<String, Object> gi = new LinkedHashMap<>();
                gi.put("totalLag", groupLag);
                gi.put("partitions", partList);
                gi.put("running", isRunning(g));
                // 【为什么 running 要同时看开关和容器】
                // preagg 的「暂停」是业务开关（容器还在跑，只是不处理）；
                // rewind 的「停止」是容器真的停了。两种「没在消费」原因不同，
                // 排查时要能区分开，所以 running 由两者共同决定。
                gi.put("paused", g.equals("hourly-preagg") && preagg.isPaused());
                groups.put(g, gi);
            }
        } catch (Exception e) {
            log.error("查询 lag 失败: {}", e.getMessage());
            out.put("error", e.getClass().getSimpleName() + ": " + e.getMessage());
        }

        out.put("groups", groups);
        out.put("totalLag", totalLag);
        return out;
    }

    /** 容器是否真的在消费（不是业务开关，是 Kafka 容器状态） */
    private boolean isRunning(String group) {
        try {
            MessageListenerContainer c = registry.getListenerContainer(group);
            return c != null && c.isRunning();
        } catch (Exception e) {
            return false;
        }
    }

    private int partitionCount(AdminClient admin) {        try {
            var infos = admin.describeTopics(java.util.List.of(topic))
                    .allTopicNames().get();
            var desc = infos.get(topic);
            return desc == null || desc.partitions() == null ? 3 : desc.partitions().size();
        } catch (Exception e) {
            log.warn("查询分区数失败，按默认 3 处理: {}", e.getMessage());
            return 3;
        }
    }

    // -----------------------------------------------------------------
    // 验收：对账差异
    // -----------------------------------------------------------------

    /**
     * 同步表与消费表对账 —— <b>M6 的核心验收指标</b>。
     *
     * <p><b>怎么读这个结果</b>：
     * <ul>
     *   <li>{@code passed = true}（diffCount=0 且 extraCount=0）
     *       → 每一次扣费都成功发出了事件，每一次事件都被消费到了，
     *       且没有凭空多出来的事件</li>
     *   <li>{@code diffCount > 0} → 消费表<b>少于</b>同步表 = 事件丢了（查生产者）</li>
     *   <li>{@code extraCount > 0} → 消费表<b>多于</b>同步表 = 有事件没有扣费事实（查兜底路径）</li>
     * </ul>
     *
     * <p><b>为什么必须带时间窗口</b>：
     * {@code billing_event} 累积全天数据，而消费表只包含
     * Kafka 实际投递到的那部分（历史数据没有回放过）。
     * 不限定窗口会算出巨大差额，看起来像严重丢消息，实际是口径错误。
     *
     * <p><b>注意两个方向的时间基准不同</b>：
     * 主查询按 {@code billed_at}（扣费时刻）过滤，
     * 反向查询按 {@code consumed_at}（消费时刻）过滤——
     * 各自的含义是「这一侧在窗口内产生的记录」。
     */
    @GetMapping("/kafka/recon")
    public Map<String, Object> recon(@RequestParam(required = false) String since) {
        Map<String, Object> out = new LinkedHashMap<>();
        if (since == null || since.isBlank()) {
            out.put("error", "必须传 since 参数（'YYYY-MM-DD HH:MM:SS'，本地时间）");
            out.put("reason", "billing_event 累积全天数据，不限定窗口口径会不一致");
            return out;
        }
        EventReconMapper.ReconResult r = recon.recon(since);
        out.put("since", since);
        out.put("syncRows", r.getSyncRows());
        out.put("consumeRows", r.getConsumeRows());
        out.put("syncAmount", r.getSyncAmount());
        out.put("consumeAmount", r.getConsumeAmount());
        out.put("diffCount", r.getDiffCount());
        out.put("diffAmount", r.getDiffAmount());
        out.put("extraCount", r.getExtraCount());
        // 两个方向都要为 0：少了（漏消费）和多了（凭空多出事件）都算不通过
        out.put("passed", r.getDiffCount() == 0 && r.getExtraCount() == 0);
        return out;
    }

    /**
     * 幂等生效验证。
     *
     * <p><b>这是「重复投递下对账差异 0 条」的另一半证据</b>。
     *
     * <p>对账差异 0 条只说明「没丢没多」，但没证明「重投确实发生过」。
     * 要证明幂等真的在工作，需要看审计表里
     * {@code DUPLICATE_SKIPPED} 的计数 &gt; 0——
     * 说明有重复消息被消费到了，并且被唯一键拦了下来。
     */
    @GetMapping("/kafka/idempotency")
    public Map<String, Object> idempotency() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("auditByGroupAction", recon.auditByGroupAction());
        out.put("duplicateEventIds", recon.duplicateEventIds());
        out.put("freqProcessed", freq.getProcessedCount());
        out.put("freqOverThreshold", freq.getOverFreqCount());
        out.put("preaggBufferSize", preagg.bufferSize());
        out.put("freqPending", freq.getPendingSize());
        out.put("note", "duplicateEventIds 必须为空数组（表里不能有重复 event_id）；"
                + "auditByGroupAction 里 DUPLICATE_SKIPPED > 0 说明幂等实际拦下过重投");
        return out;
    }
}
