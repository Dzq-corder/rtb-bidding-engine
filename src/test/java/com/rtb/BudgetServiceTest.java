package com.rtb;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.jdbc.Sql;

import com.rtb.service.BudgetService;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * BudgetService 预算扣减单元测试。
 *
 * <p>直接验证 M3 Lua 原子扣减的两种结果：
 * ① 预算充足 -> 返回 true 且余额按分精确扣减；
 * ② 余额不足 -> 返回 false 且回写 MySQL status=3（检索阶段即被排除）。
 *
 * <p>真连 Kafka：上下文含 Kafka 生产者/消费者与 {@code KafkaAdmin}（建 topic），
 * 故需 Kafka broker 可用（本地 docker-compose / CI services）。
 */
@SpringBootTest
@Sql(scripts = "classpath:test-fixtures.sql", executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD)
@Sql(scripts = "classpath:test-cleanup.sql", executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
class BudgetServiceTest {

    @Autowired
    private BudgetService budgetService;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private StringRedisTemplate redis;

    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("yyyyMMdd");

    /** 每次运行前复位状态 + 清 Redis 预算 key，保证幂等 */
    @BeforeEach
    void reset() {
        jdbcTemplate.update("UPDATE ad SET status = 1 WHERE id IN (900010, 900011)");
        String d = LocalDate.now().format(FMT);
        redis.delete(List.of(
                "budget:900010:" + d, "spent:900010:" + d,
                "budget:900011:" + d, "spent:900011:" + d));
    }

    @AfterEach
    void cleanRedis() {
        String d = LocalDate.now().format(FMT);
        redis.delete(List.of(
                "budget:900010:" + d, "spent:900010:" + d,
                "budget:900011:" + d, "spent:900011:" + d));
    }

    @Test
    void deduct_succeedsWhenBudgetSufficient() {
        boolean ok = budgetService.deduct(900010L, new BigDecimal("100.00"), new BigDecimal("0.10"));
        assertTrue(ok, "预算充足应扣减成功");

        BigDecimal balance = budgetService.getBalance(900010L, new BigDecimal("100.00"));
        assertEquals(0, balance.compareTo(new BigDecimal("99.90")), "扣减 0.10 后应剩 99.90 元");
    }

    @Test
    void deduct_failsAndMarksExhaustedWhenBudgetInsufficient() {
        boolean ok = budgetService.deduct(900011L, new BigDecimal("0.01"), new BigDecimal("5.00"));
        assertFalse(ok, "余额不足应扣减失败");

        Integer status = jdbcTemplate.queryForObject(
                "SELECT status FROM ad WHERE id = 900011", Integer.class);
        assertEquals(3, status, "余额不足应回写 status=3");
    }
}
