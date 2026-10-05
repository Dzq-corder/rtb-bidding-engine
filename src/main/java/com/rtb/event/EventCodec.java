package com.rtb.event;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.stereotype.Component;

/**
 * 事件编解码 · M6
 *
 * <p><b>为什么序列化用 String 而不是直接发对象</b>
 *
 * <p>{@code KafkaTemplate<String, BidEvent>} 需要自定义序列化器；
 * 而用 String 有两个实际好处：
 * <ol>
 *   <li><b>跨语言可读</b>：消息是 JSON 文本，
 *       用 {@code kafka-console-consumer} 就能直接看懂内容。
 *       这在排查线上问题时的价值很高</li>
 *   <li><b>消费端不依赖生产端的类</b>：改字段顺序不会导致老消费者反序列化失败</li>
 * </ol>
 *
 * <p>代价是每条消息多一次序列化/反序列化。对本项目的事件量级可忽略，
 * 真实系统里高吞吐场景会用二进制序列化（Protobuf/Avro），
 * 但那会牺牲可调试性——<b>这是个明确的取舍，不是没考虑</b>。
 */
@Component
public class EventCodec {

    private final ObjectMapper objectMapper;

    public EventCodec() {
        this.objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                // 消费端遇到未知字段不能失败。
                // 【为什么重要】生产端加字段时，如果消费端先上线，
                // 严格模式的 ObjectMapper 会抛 UnrecognizedPropertyException，
                // 导致整批消息卡住。宽松模式保证新旧版本共存。
                .configure(com.fasterxml.jackson.databind.DeserializationFeature
                        .FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public String encode(BidEvent event) throws JsonProcessingException {
        return objectMapper.writeValueAsString(event);
    }

    public BidEvent decode(String payload) throws JsonProcessingException {
        return objectMapper.readValue(payload, BidEvent.class);
    }
}
