package com.rtb;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.web.servlet.MockMvc;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 竞价主链路端到端集成测试（A1：真连 Kafka）。
 *
 * <p><b>不 mock 任何 Kafka bean、不关闭监听器</b>：MySQL / Redis / Kafka 三者都需真实可用
 * ——本地由 docker-compose（rtb2-mysql / rtb2-redis / rtb2-kafka）提供，CI 由 services 提供。
 *
 * <p>断言覆盖两层：
 * <ul>
 *   <li>同步链路：{@code /bid} 返回胜出者 900002、二价 {@code priceMode=1}；
 *       余额不足的 900001 在扣减失败后回写 {@code status=3}；</li>
 *   <li>异步旁路：一条 {@code bid_event} 被三个消费组真实消费，分别落
 *       {@code billing_event} / {@code bid_stat_hourly} / {@code ad_freq}。</li>
 * </ul>
 */
@SpringBootTest
@AutoConfigureMockMvc
@Sql(scripts = "classpath:test-fixtures.sql", executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD)
@Sql(scripts = "classpath:test-cleanup.sql", executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
class BidFlowTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private StringRedisTemplate redis;

    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("yyyyMMdd");

    /** 消费组异步落库的等待上限（ms）：Kafka 消费 + 消费组 rebalance 需留足时间 */
    private static final long AWAIT_TIMEOUT_MS = 15_000;

    @AfterEach
    void cleanRedisBudgetKeys() {
        String d = LocalDate.now().format(FMT);
        redis.delete(List.of(
                "budget:900001:" + d, "spent:900001:" + d,
                "budget:900002:" + d, "spent:900002:" + d));
    }

    /**
     * 命中一条高出价但预算极低的广告（900001）：它排第一被尝试扣减，余额不足被跳过，
     * 并触发回写 MySQL status=3；最终胜出者是有充足预算的 900002。
     * 随后校验该竞价事件已被三个消费组各自消费落库（真 Kafka 旁路）。
     */
    @Test
    void bid_endToEnd_winnerBilledAndConsumedByAllGroups() throws Exception {
        mockMvc.perform(post("/bid")
                        .contentType("application/json")
                        .content("{\"region\":\"440318\",\"ageBucket\":\"25-30\",\"age\":28,"
                                + "\"gender\":0,\"slotCode\":\"test-slot\",\"categoryCode\":\"industrial\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(0))
                .andExpect(jsonPath("$.data.hasAd").value(true))
                .andExpect(jsonPath("$.data.fallback").value(false))
                .andExpect(jsonPath("$.data.winner.adId").value(900002))
                .andExpect(jsonPath("$.data.priceMode").value(1))
                .andExpect(jsonPath("$.data.candidateCnt").value(2));

        // 同步链路：余额不足的高出价广告（900001）应在扣减失败后回写 status=3
        Integer status = jdbcTemplate.queryForObject(
                "SELECT status FROM ad WHERE id = 900001", Integer.class);
        assertEquals(3, status, "预算耗尽应回写 status=3");

        // 异步旁路：三个消费组各自消费同一条 bid_event（真投真消费）
        awaitRowsByAdId("billing_event", "billing-recon");
        awaitRowsByAdId("bid_stat_hourly", "hourly-preagg");
        awaitRowsByAdId("ad_freq", "freq-antifraud");
    }

    /** 轮询等待指定消费组把本轮事件（ad_id=900002）落库；超时则失败并指出是哪个组。 */
    private void awaitRowsByAdId(String table, String group) throws InterruptedException {
        String sql = "SELECT COUNT(*) FROM " + table + " WHERE ad_id = 900002";
        long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT_MS;
        int last = 0;
        while (System.currentTimeMillis() < deadline) {
            Integer c = jdbcTemplate.queryForObject(sql, Integer.class);
            last = c == null ? 0 : c;
            if (last >= 1) {
                return;
            }
            Thread.sleep(200);
        }
        fail("消费组 [" + group + "] 未在 " + AWAIT_TIMEOUT_MS + "ms 内落库: " + table + " 实际行数=" + last);
    }
}
