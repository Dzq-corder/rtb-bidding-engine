package com.rtb.bill;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.time.LocalDateTime;

/**
 * 计费事件 Mapper。
 *
 * <p><b>这张表的数据绝对不能丢</b>，与 bid_log 分级明确：
 * bid_log 是大盘明细，丢几条不影响统计结论；
 * billing_event 是金钱事实，丢一条就少收一笔钱。
 */
@Mapper
public interface BillingEventMapper {

    /**
     * 写入一条计费事件。
     *
     * <p>{@code event_id} 走唯一键，是幂等的物理保障。
     * 重复写入会撞唯一键被识别出来丢弃，保证最终只生效一次。
     */
    int insert(@Param("eventId") String eventId,
               @Param("requestId") String requestId,
               @Param("adId") Long adId,
               @Param("advertiserId") Integer advertiserId,
               @Param("bidPrice") java.math.BigDecimal bidPrice,
               @Param("payPrice") java.math.BigDecimal payPrice,
               @Param("secondPrice") java.math.BigDecimal secondPrice,
               @Param("isSecondPrice") int isSecondPrice,
               @Param("billedAt") LocalDateTime billedAt);

    /**
     * 写入消费侧计费事件 · M6
     *
     * <p><b>为什么不用「先查再插」做幂等</b>：
     * <pre>
     *   if (selectCount(eventId) == 0) { insert(...); }
     * </pre>
     * 这种写法在并发消费下仍然会重复——
     * 两个线程同时 select 都得到 0，然后都插。
     * <b>唯一键是数据库层的原子保障，业务代码的 if 不是。</b>
     * 所以真正拦住重复的是唯一键，Java 侧只负责<b>如实记账</b>。
     *
     * <p><b>为什么 consumedAt 由 Java 传入而不是 SQL 里写 NOW()</b>：
     * MySQL 容器的时区是 UTC，而 JVM 是 Asia/Shanghai，两者差 8 小时。
     * {@code billing_event.billed_at} 写的是 Java 本地时间，
     * 如果消费表用 {@code NOW()} 就会写成 UTC——
     * 两张表的时间字段差 8 小时，
     * 对账 SQL 按时间窗口过滤时会算出「消费表一行都没有」的假象。
     * M6 第一次就踩了这个坑，现象是
     * {@code consumeRows = 0} 但 {@code diffCount = 4714}。
     * <b>凡是跨表比对的时间字段，必须统一由 Java 侧生成。</b>
     *
     * <p><b>插入之后必须调 {@link #wasInserted} 判重</b>，
     * 不要直接用本方法的返回值。
     *
     * <p><b>为什么返回值不能用来判重</b>：这是 M6 实测踩到的坑。
     * MySQL 手工执行时 {@code ROW_COUNT()} 确实返回 0，
     * 但走 JDBC 时 MyBatis 拿到的是 1——
     * Connector/J 的 {@code useAffectedRows} 默认 false，
     * 返回的是 found_rows（匹配行数）而不是 affected_rows（变更行数）。
     * 命中唯一键时虽然一行都没改，但「匹配到了 1 行」，
     * 于是 found_rows = 1，重投被误判成新事件。
     *
     * <p><b>为什么不改成全局 {@code useAffectedRows=true}</b>：
     * 改连接串会影响所有 Mapper 的返回值语义，
     * M4 同步链路的 insert 也返回 affected_rows，
     * 为了一个判重去动全局配置，风险不划算。
     *
     * @return MyBatis 的 insert 影响行数，<b>不可用于判重</b>
     */
    int insertConsumerEvent(@Param("event") com.rtb.event.BidEvent event,
                            @Param("group") String group,
                            @Param("consumedAt") LocalDateTime consumedAt);

    /**
     * 判断刚才那次插入是<b>新增</b>还是<b>命中唯一键</b>。
     *
     * <p>依据：{@code ON DUPLICATE KEY UPDATE} 分支不会更新
     * {@code consumed_at}，所以重复命中的行保留的是上一次消费时刻。
     *
     * <p><b>consumedAtStr 必须由 Java 侧格式化成
     * {@code yyyy-MM-dd HH:mm:ss.SSSSSS}</b>，
     * 与 SQL 里 {@code DATE_FORMAT(..., '%Y-%m-%d %H:%i:%s.%f')} 的输出同形。
     * 不能直接传 {@link LocalDateTime} 让驱动去比——
     * 实测那样写会导致几乎所有事件都被误判成重复。
     *
     * @return 1 = 本次新增；0 = 命中唯一键被幂等跳过
     */
    int wasInserted(@Param("eventId") String eventId,
                    @Param("consumedAtStr") String consumedAtStr);

}
