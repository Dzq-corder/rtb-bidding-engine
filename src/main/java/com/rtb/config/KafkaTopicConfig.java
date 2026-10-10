package com.rtb.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * M6 主题声明。应用启动时由 Spring Kafka 的 {@code KafkaAdmin} 自动建主题，
 * 避免依赖 broker 的默认自动建主题（其分区数会退化为 1，无法体现按 adId 分区）。
 */
@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic bidEventTopic(@Value("${rtb.kafka.topic:bid_event}") String topic,
                                 @Value("${rtb.kafka.partitions:3}") int partitions) {
        // 单节点 demo：replicas=1。生产环境应改为 RF=3 + min.insync.replicas=2（详见 metrics 文档说明）
        return TopicBuilder.name(topic)
                .partitions(partitions)
                .replicas(1)
                .build();
    }
}
