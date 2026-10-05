package com.rtb.event;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.time.LocalDateTime;

/**
 * 消费审计 Mapper · M6
 *
 * <p><b>为什么幂等拦截也要记账</b>
 *
 * <p>幂等的实现是「命中唯一键就静默跳过」——
 * 对业务完全透明，<b>不产生任何异常或日志</b>。
 * 但这样就有一个问题：<b>无法证明幂等到底生效了没有</b>。
 *
 * <p>本项目的 M6 验收是「重复投递下对账差异 0 条」。
 * 这个结论要成立，前提是能证明「重投的消息确实被消费到了，
 * 而且确实被唯一键拦下来了」——
 * 审计表的 {@code DUPLICATE_SKIPPED} 计数就是那个证据。
 */
@Mapper
public interface ConsumerAuditMapper {

    /**
     * 记一笔消费审计。
     *
     * <p><b>processedAt 由 Java 传入</b>：MySQL 容器的时区是 UTC，
     * 用 {@code NOW()} 会与billing_event.billed_at（Java 本地时间）差 8 小时。
     * 审计表是排查问题的依据，时间口径不一致会让「查某个时间窗内的失败」
     * 直接查不到。
     *
     * @return 影响行数
     */
    int record(@Param("group") String group,
               @Param("partition") int partition,
               @Param("offset") long offset,
               @Param("eventId") String eventId,
               @Param("action") String action,
               @Param("costMs") int costMs,
               @Param("processedAt") LocalDateTime processedAt);

    /**
     * 记一笔失败审计 —— <b>绝不抛异常</b>。
     *
     * <p><b>为什么需要单独一个方法</b>：失败路径上如果审计本身也失败
     * （比如数据库连接断了），再抛一次异常会<b>掩盖原始故障</b>——
     * 排查时看到的是「审计失败」而不是「计费消费失败」，
     * 根因被埋掉了。
     *
     * <p>所以这里的异常必须被吞掉，只打 WARN。
     */
    void recordSafe(@Param("group") String group,
                    @Param("partition") int partition,
                    @Param("offset") long offset,
                    @Param("eventId") String eventId,
                    @Param("action") String action,
                    @Param("costMs") int costMs,
                    @Param("processedAt") LocalDateTime processedAt,
                    @Param("errMsg") String errMsg);
}
