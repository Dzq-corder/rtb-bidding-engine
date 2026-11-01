package com.rtb;

import com.rtb.entity.Ad;
import com.rtb.service.FrequencyService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * FrequencyService 单测（纯 Mockito，<b>不启动 Spring 上下文、不依赖 Redis</b>）。
 *
 * <p>覆盖三类容易被写错的行为：
 * <ol>
 *   <li><b>降级</b>：Redis 抛异常时必须「放行」，绝不能因为频控把广告拦掉；</li>
 *   <li><b>边界</b>：只检查前 maxCheck 个候选，其后的即便超频也保留；</li>
 *   <li><b>TTL</b>：只在 INCR 首次创建（返回 1）时补过期，避免每条请求多发一次 EXPIRE。</li>
 * </ol>
 */
class FrequencyServiceTest {

    private StringRedisTemplate redis;
    private ValueOperations<String, String> ops;
    private FrequencyService service;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        redis = mock(StringRedisTemplate.class);
        ops = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(ops);

        service = new FrequencyService(redis);
        ReflectionTestUtils.setField(service, "enabled", true);
        ReflectionTestUtils.setField(service, "dailyCap", 3);
        ReflectionTestUtils.setField(service, "maxCheck", 50);
        ReflectionTestUtils.setField(service, "ttlSeconds", 172800L);
    }

    private static Ad ad(long id, String bid) {
        Ad a = new Ad();
        a.setId(id);
        a.setAdvertiserId(1);
        a.setBid(new BigDecimal(bid));
        return a;
    }

    // ---------------- identOf ----------------

    @Test
    @DisplayName("标识拼接：userId 优先，其次 IP，都没有则返回 null（不频控）")
    void identOf_priority() {
        assertEquals("u:10086", FrequencyService.identOf("10086", "203.0.113.7"));
        assertEquals("u:10086", FrequencyService.identOf(" 10086 ", null));
        assertEquals("ip:203.0.113.7", FrequencyService.identOf(null, "203.0.113.7"));
        assertEquals("ip:203.0.113.7", FrequencyService.identOf("  ", "203.0.113.7"));
        assertNull(FrequencyService.identOf(null, null));
    }

    // ---------------- filter ----------------

    @Test
    @DisplayName("达到上限的候选被剔除，未达上限的保留，顺序不变")
    void filter_removesCappedCandidates() {
        List<Ad> matched = List.of(ad(101, "1.5"), ad(102, "1.2"), ad(103, "0.9"));
        // 101 已达 3 次（上限 3）、102 只有 1 次、103 无记录
        when(ops.multiGet(anyList())).thenReturn(Arrays.asList("3", "1", null));

        FrequencyService.FilterResult r = service.filter("u:1", matched);

        assertEquals(1, r.filtered());
        assertEquals(List.of(102L, 103L), r.remaining().stream().map(Ad::getId).toList());
    }

    @Test
    @DisplayName("Redis 异常时放行全部候选（频控降级，绝不能因为频控不出广告）")
    void filter_failOpenOnRedisError() {
        List<Ad> matched = List.of(ad(101, "1.5"), ad(102, "1.2"));
        when(ops.multiGet(anyList())).thenThrow(new RedisConnectionFailureException("mock down"));

        FrequencyService.FilterResult r = service.filter("u:1", matched);

        assertEquals(0, r.filtered());
        assertEquals(2, r.remaining().size());
    }

    @Test
    @DisplayName("只检查前 maxCheck 个：其后的候选即便已超频也一律保留")
    void filter_onlyChecksFirstN() {
        ReflectionTestUtils.setField(service, "maxCheck", 2);
        List<Ad> matched = List.of(ad(101, "1.5"), ad(102, "1.2"), ad(103, "0.9"));
        when(ops.multiGet(anyList())).thenReturn(Arrays.asList("5", "0"));  // 只返回 2 个 key 的值

        FrequencyService.FilterResult r = service.filter("u:1", matched);

        assertEquals(1, r.filtered());
        // 103 未参与判断 → 保留，即使它的真实计数可能已超限
        assertEquals(List.of(102L, 103L), r.remaining().stream().map(Ad::getId).toList());
    }

    @Test
    @DisplayName("无标识（userId 与 IP 都为空）时不查 Redis，直接放行")
    void filter_noIdent_noRedisCall() {
        List<Ad> matched = List.of(ad(101, "1.5"));

        FrequencyService.FilterResult r = service.filter(null, matched);

        assertEquals(0, r.filtered());
        assertEquals(1, r.remaining().size());
        verifyNoInteractions(ops);
    }

    @Test
    @DisplayName("enabled=false 时整条链路跳过，不产生任何 Redis 交互（压测 A/B 对照的前提）")
    void filter_disabled_noRedisCall() {
        ReflectionTestUtils.setField(service, "enabled", false);
        List<Ad> matched = List.of(ad(101, "1.5"));

        service.filter("u:1", matched);
        service.recordWin("u:1", 101L);

        verifyNoInteractions(ops);
    }

    @Test
    @DisplayName("脏数据（非数字）按 0 处理，等价于放行")
    void filter_garbageValueTreatedAsZero() {
        List<Ad> matched = List.of(ad(101, "1.5"));
        when(ops.multiGet(anyList())).thenReturn(List.of("not-a-number"));

        FrequencyService.FilterResult r = service.filter("u:1", matched);

        assertEquals(0, r.filtered());
        assertEquals(1, r.remaining().size());
    }

    // ---------------- recordWin ----------------

    @Test
    @DisplayName("首次计数（INCR 返回 1）才补 TTL，避免每条请求都发 EXPIRE")
    void recordWin_setsTtlOnlyOnFirstIncrement() {
        when(ops.increment(anyString())).thenReturn(1L);

        service.recordWin("u:1", 101L);

        verify(redis).expire(startsWith("freq:u:1:ad:101:"), eq(172800L), eq(TimeUnit.SECONDS));
    }

    @Test
    @DisplayName("非首次计数（INCR 返回 2）不再重复设置 TTL")
    void recordWin_noTtlOnLaterIncrement() {
        when(ops.increment(anyString())).thenReturn(2L);

        service.recordWin("u:1", 101L);

        verify(redis, never()).expire(anyString(), eq(172800L), eq(TimeUnit.SECONDS));
    }

    @Test
    @DisplayName("计数失败只记日志，不外抛（不能影响竞价返回）")
    void recordWin_failSilently() {
        when(ops.increment(anyString())).thenThrow(new RedisConnectionFailureException("mock down"));

        // 不抛异常即通过
        service.recordWin("u:1", 101L);
    }
}
