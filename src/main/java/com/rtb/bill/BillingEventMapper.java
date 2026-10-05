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
}
