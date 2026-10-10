package com.rtb.controller;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * M6 消费组管理接口（演示第 6 步用）。
 *
 * <ul>
 *   <li>{@code POST /admin/consumer/pause} —— 暂停全部监听容器（模拟消费端故障）；</li>
 *   <li>{@code POST /admin/consumer/resume} —— 恢复全部监听容器；</li>
 *   <li>{@code GET /admin/kafka/lag?group=xxx} —— 查某消费组的积压（lag）分布，供大盘曲线。</li>
 * </ul>
 *
 * <p>暂停期间 {@code /bid} 主链路完全不受影响，事件在 broker 中堆积、lag 爬升，
 * 恢复后消费组从已提交位点继续重放——这正是"旁路解耦"的直观证据。
 */
@RestController
@RequestMapping("/admin")
public class ConsumerAdminController {

    private final KafkaListenerEndpointRegistry registry;
    private final String bootstrap;

    public ConsumerAdminController(KafkaListenerEndpointRegistry registry,
                                   @Value("${spring.kafka.bootstrap-servers}") String bootstrap) {
        this.registry = registry;
        this.bootstrap = bootstrap;
    }

    @PostMapping("/consumer/pause")
    public Map<String, String> pause() {
        registry.getAllListenerContainers().forEach(c -> c.pause());
        return Map.of("status", "paused");
    }

    @PostMapping("/consumer/resume")
    public Map<String, String> resume() {
        registry.getAllListenerContainers().forEach(c -> c.resume());
        return Map.of("status", "resumed");
    }

    @GetMapping("/kafka/lag")
    public List<Map<String, Object>> lag(@RequestParam String group) throws InterruptedException, ExecutionException {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        try (AdminClient admin = AdminClient.create(props)) {
            Map<TopicPartition, OffsetAndMetadata> committed =
                    admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get();
            Set<TopicPartition> tps = committed.keySet();
            Map<TopicPartition, OffsetSpec> specs = tps.stream()
                    .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> ends =
                    admin.listOffsets(specs).all().get();

            List<Map<String, Object>> out = new ArrayList<>();
            for (TopicPartition tp : tps) {
                long committedOff = committed.get(tp).offset();
                long endOff = ends.get(tp).offset();
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("topic", tp.topic());
                row.put("partition", tp.partition());
                row.put("committed", committedOff);
                row.put("end", endOff);
                row.put("lag", endOff - committedOff);
                out.add(row);
            }
            return out;
        }
    }
}
