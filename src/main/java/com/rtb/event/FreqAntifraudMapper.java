package com.rtb.event;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.time.LocalDateTime;

/**
 * 频次打点 Mapper · M6
 */
@Mapper
public interface FreqAntifraudMapper {

    /**
     * 小时打点写入（幂等累加）。
     *
     * <p>唯一键 {@code (ad_id, user_id, stat_hour)}，
     * 命中时 {@code cnt = cnt + 1}。
     *
     * <p><b>这才是贴合业务语义的幂等</b>：同一用户同一小时对同一广告
     * 的第 1 次和第 5 次曝光，对「频次」这个指标来说等价——
     * 都只该+1。所以用唯一键累加，<b>重投 100 次也只 +1</b>。
     *
     * <p>对比 {@code billing_event_consumer} 用 event_id 做幂等键：
     * 那是「事件去重」，语义是这条记录只该存在一份；
     * 这里是「计数累加」，语义是重复的曝光也应该被计进去。
     * <b>两者都是幂等，但幂等键的选择依据是业务语义，不是技术方便。</b>
     *
     * <p><b>返回值的坑，与 billing_event_consumer 同源</b>：
     * 理论上 1 = 新增、2 = 命中唯一键并累加，
     * 但 Connector/J 默认返回 found_rows 而非 affected_rows，
     * 所以这个返回值<b>不能用来判重</b>。
     * 本方法正好不需要判重——
     * 累加逻辑写在 SQL 里（{@code cnt = cnt + 1}），
     * 新增还是命中都由数据库保证，Java 侧不需要区分。
     * {@code flush()} 也只把它当「处理了多少条」计数用。
     *
     * @return MySQL 语义下 1 = 新增、2 = 命中唯一键；<b>受驱动影响不可靠</b>
     */
    int upsert(@Param("adId") Long adId,
               @Param("userId") String userId,
               @Param("statHour") LocalDateTime statHour,
               @Param("updatedAt") LocalDateTime updatedAt);
}
