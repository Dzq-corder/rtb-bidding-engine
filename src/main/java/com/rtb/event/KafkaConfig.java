package com.rtb.event;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka 配置 · M6
 *
 * <p><b>本类是 M6 全部可调参数的落点</b>，每一个都有明确理由，
 * 不是抄默认值。
 */
@Slf4j
@Configuration
public class KafkaConfig {

    @Value("${rtb.kafka.topic:bid_event}")
    private String topic;

    @Value("${rtb.kafka.partitions:3}")
    private int partitions;

    /**
     * 消费端批量容器。
     *
     * <p><b>为什么必须手动提交位点（不是自动提交）</b>
     *
     * <p>自动提交（{@code enable.auto.commit=true}）是<b>按时间</b>提交的——
     * 每隔 N 秒提交一次当前位点。问题是：如果这个时间窗口内
     * 刚poll 到一批消息、正要处理时进程崩溃，
     * <b>这批消息的位点已经提交了，但数据还没落库</b>——消息丢了。
     *
     * <p>手动提交是<b>按结果</b>提交：处理成功才提交，
     * 崩溃时位点没提交，重启后重新消费。
     * <b>多消费几次总比丢消息好</b>——重复由幂等消化，丢失无法弥补。
     *
     * <p>这个取舍的代价就是<b>必须配套幂等</b>，两者是一套机制。
     */
    @Bean
    public org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory<
            String, String> kafkaBatchContainerFactory(
            org.springframework.kafka.core.ConsumerFactory<String, String> consumerFactory) {

        var factory = new org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(consumerFactory);

        // 手动立即提交：ack.acknowledge() 后立刻提交，不等批量窗口
        factory.getContainerProperties().setAckMode(AckMode.MANUAL_IMMEDIATE);

        // 空轮询超时。默认 5 秒太短，容易在下一次拉取前被判定为死掉
        factory.getContainerProperties().setPollTimeout(10000);

        /**
         * 【重试与死信的处理】
         *
         * 默认行为是「失败就无限重试当前消息」——
         * 这会导致一条毒消息（poison message）把整个分区卡死，
         * 后面的消息永远处理不了。
         *
         * 这里设成：失败 3 次后跳过。
         * 为什么是 3 次：数据库抖动这类瞬时故障重试几次就好了，
         * 而真正的代码 bug 重试一万次也不会好。
         *
         * 注意：跳过并不等于丢弃——位点照样提交，
         * 消费审计表里记了 FAILED，所以「跳过了什么」是可查的。
         * <b>可观测的丢弃才是可接受的丢弃。</b>
         */
        var errorHandler = new DefaultErrorHandler(
                new FixedBackOff(1000L, 2L));   // 间隔 1s，最多重试 2 次（共 3 次）
        errorHandler.setAckAfterHandle(true);
        factory.setCommonErrorHandler(errorHandler);

        // 并发消费者数量。
        // 【为什么设成 1】
        // 演示环境只有 3 个分区，1 个消费者已经能跑满；
        // 而且并发度上去了，HourlyPreaggConsumer 的内存 flush 就需要加锁，
        // 复杂度不划算。真实系统会设成 = 分区数
        //（不能超过分区数，多出来的线程拿不到消息，白占资源）。
        factory.setConcurrency(1);

        log.info("Kafka 消费容器：topic={} partitions={} 手动提交位点 攒批 500",
                topic, partitions);
        return factory;
    }

    /**
     * 启动时自动建 topic。
     *
     * <p><b>为什么用代码建 topic 而不用 kafka-topics.sh</b>：
     * 演示时少一步手工操作，少一个出错点。
     *
     * <p><b>为什么分区数是 3</b>：
     * 3 个分区能演示「同一 adId 路由到同一分区」的顺序性，
     * 而且分区数决定了消费组的并行度上限。
     * 分区数太少（1 个）无法演示并行消费，
     * 太多（几十个）单节点Broker 也扛不住且没有实际意义。
     */
    @Bean
    public NewTopic bidEventTopic() {
        return TopicBuilder.name(topic)
                .partitions(partitions)
                // 【单节点必须为 1】只有 1 个 Broker 时副本因子 > 1
                // 的主题无法分配 leader，创建会失败或写入报 NOT_ENOUGH_REPLICAS
                .replicas(1)
                .build();
    }
}
