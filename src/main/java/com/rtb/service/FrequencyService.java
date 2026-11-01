package com.rtb.service;

import com.rtb.entity.Ad;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 频次控制（Frequency Capping）—— 解决「同一用户反复刷到同一广告」。
 *
 * <p><b>规则</b>：同标识 + 同广告 + 同自然日，曝光上限 {@code rtb.freq.daily-cap} 次；
 * 达到上限的候选在竞价时被剔除，不参与二价、不被计费。
 *
 * <p><b>标识</b>：{@code u:{userId}} 优先；未传 userId 时退化为 {@code ip:{clientIp}}；两者都无 → 不频控。
 *
 * <p><b>软频控</b>：计数发生在「中标」而非「真实曝光」（项目暂无曝光回传接口），
 * 因此允许轻微超发，换来实现成本最低、且与 bid_event 链路天然衔接。
 *
 * <p><b>降级</b>：本类的任何异常都不外抛，一律「放行」。频控是体验优化，
 * 绝不能因为它导致不出广告——这与预算扣减（错了就是扣错钱）是两种完全不同的失败语义。
 * 也因此<b>刻意不套 {@code redisBreaker}</b>：那个门承载的是「Redis 是否可信到可以扣钱」，
 * 频控查不动不该累加它的失败计数。
 */
@Slf4j
@Service
public class FrequencyService {

    private static final String KEY_PREFIX = "freq:";
    private static final DateTimeFormatter DAY_FMT = DateTimeFormatter.ofPattern("yyyyMMdd");

    private final StringRedisTemplate redis;

    /** 总开关：false 时整条链路跳过，行为与改造前完全一致（压测 A/B 对照用） */
    @Value("${rtb.freq.enabled:true}")
    private boolean enabled;

    /** 同标识 + 同广告 + 同自然日的曝光上限 */
    @Value("${rtb.freq.daily-cap:3}")
    private int dailyCap;

    /**
     * 单次竞价最多检查前多少个候选（按出价降序）。
     * 计费是 fall-through，正常第 1 个就成交，698 个候选全量 MGET 是白付 RT。
     * {@code <= 0} 表示不限制（全量检查）。
     */
    @Value("${rtb.freq.max-check:50}")
    private int maxCheck;

    /** 计数 key 的 TTL（秒），默认 2 天，与预算 key 口径一致 */
    @Value("${rtb.freq.ttl-seconds:172800}")
    private long ttlSeconds;

    public FrequencyService(StringRedisTemplate redis) {
        this.redis = redis;
    }

    /**
     * 过滤结果。
     *
     * @param remaining 可参与计费的候选（保持原出价降序）
     * @param filtered  被频控剔除的数量
     */
    public record FilterResult(List<Ad> remaining, int filtered) {
        public static FilterResult passThrough(List<Ad> matched) {
            return new FilterResult(matched, 0);
        }
    }

    /**
     * 批量判断并剔除已超频的候选。
     *
     * <p>实现要点：<b>一次 {@code multiGet}（MGET）</b>取回全部计数再在内存判断，
     * 绝不逐个 GET——N 个候选逐个 GET 就是 N 次往返，直接把这跳从 1ms 拉到几十毫秒。
     *
     * @param ident   频控标识（见 {@link #identOf}），null 表示无标识、直接放行
     * @param matched 候选列表（出价降序，频控前）
     */
    public FilterResult filter(String ident, List<Ad> matched) {
        if (!enabled || dailyCap <= 0 || ident == null || matched == null || matched.isEmpty()) {
            return FilterResult.passThrough(matched);
        }
        try {
            int checkN = (maxCheck <= 0) ? matched.size() : Math.min(matched.size(), maxCheck);
            List<String> keys = new ArrayList<>(checkN);
            for (int i = 0; i < checkN; i++) {
                keys.add(key(ident, matched.get(i).getId()));
            }
            List<String> vals = redis.opsForValue().multiGet(keys);

            List<Ad> remain = new ArrayList<>(matched.size());
            int filtered = 0;
            for (int i = 0; i < matched.size(); i++) {
                // checkN 之后的候选未做判断，一律保留（只查前 N 的取舍，见方案 §4.4）
                if (i < checkN && cntOf(vals, i) >= dailyCap) {
                    filtered++;
                } else {
                    remain.add(matched.get(i));
                }
            }
            if (filtered > 0) {
                log.debug("频控剔除 {} 个候选 ident={}", filtered, ident);
            }
            return new FilterResult(remain, filtered);
        } catch (Exception e) {
            // 频控降级：放行。绝不影响出广告。
            // 打 getSimpleName：多数 Redis 异常 message 为 null，只打 getMessage 会输出空串。
            log.warn("频控查询失败，本次放行 ident={}: {} - {}",
                    ident, e.getClass().getSimpleName(), e.getMessage());
            return FilterResult.passThrough(matched);
        }
    }

    /**
     * 中标后计数 +1。
     *
     * <p>必须在 {@code billCostMs} 统计<b>之后</b>调用：否则它的耗时会被算进「计费耗时」，
     * 污染这个已用于压测归因的指标。
     *
     * <p>失败只记日志：漏计一次只是让该用户多看一次，不影响正确性。
     */
    public void recordWin(String ident, Long adId) {
        if (!enabled || dailyCap <= 0 || ident == null || adId == null) {
            return;
        }
        try {
            String k = key(ident, adId);
            Long v = redis.opsForValue().increment(k);
            // 只在首次创建时补 TTL，避免每条请求都多发一次 EXPIRE
            if (v != null && v == 1L) {
                redis.expire(k, ttlSeconds, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            log.warn("频控计数失败（忽略） ident={} adId={}: {}",
                    ident, adId, e.getClass().getSimpleName());
        }
    }

    /**
     * 由 userId / clientIp 组装频控标识。
     *
     * <p>带 {@code u:} / {@code ip:} 前缀是为了在同一 key 命名空间下区分标识类型，
     * 避免 user_id 与 IP 字面值撞车。
     *
     * @return null 表示无标识 → 不频控
     */
    public static String identOf(String userId, String clientIp) {
        if (userId != null && !userId.isBlank()) {
            return "u:" + userId.trim();
        }
        if (clientIp != null && !clientIp.isBlank()) {
            return "ip:" + clientIp.trim();
        }
        return null;
    }

    /** 供演示/排障读取当前计数，不参与竞价决策 */
    public int currentCount(String ident, Long adId) {
        if (ident == null || adId == null) {
            return 0;
        }
        try {
            String v = redis.opsForValue().get(key(ident, adId));
            return v == null ? 0 : Integer.parseInt(v);
        } catch (Exception e) {
            log.warn("读取频控计数失败 ident={} adId={}: {}", ident, adId, e.getClass().getSimpleName());
            return 0;
        }
    }

    /**
     * key 结构：{@code freq:{ident}:ad:{adId}:{yyyyMMdd}}
     *
     * <p>{@code ad:} 是预留的 scope 位，将来加广告主级频控时换成 {@code adv:{advId}}，
     * 结构不变、无需迁移数据。日期用服务器本地时区，与预算 key（budget:{adId}:{yyyyMMdd}）同口径，
     * 避免「预算按 A 时区跨天、频控按 B 时区跨天」的错位。
     */
    private String key(String ident, Long adId) {
        return KEY_PREFIX + ident + ":ad:" + adId + ":" + LocalDate.now().format(DAY_FMT);
    }

    /** 越界 + null 安全的计数解析；脏数据（非数字）按 0 处理，等价于放行 */
    private static int cntOf(List<String> vals, int i) {
        if (vals == null || i >= vals.size()) {
            return 0;
        }
        String v = vals.get(i);
        if (v == null || v.isEmpty()) {
            return 0;
        }
        try {
            return Integer.parseInt(v);
        } catch (NumberFormatException e) {
            return 0;
        }
    }
}
