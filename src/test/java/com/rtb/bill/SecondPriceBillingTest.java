package com.rtb.bill;

import com.rtb.ad.Ad;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 二价计费规则单元测试。
 *
 * <p>二价的边界情况比一价多，规则容易在特定组合下被改坏。
 * 用测试固定住每种情形，比跑起来看数字可靠。
 */
class SecondPriceBillingTest {

    private final SecondPriceBilling billing = new SecondPriceBilling();

    private Ad ad(long id, String bid) {
        Ad a = new Ad();
        a.setId(id);
        a.setBid(new BigDecimal(bid));
        return a;
    }

    @Test
    @DisplayName("标准场景：赢家按次高价 + 0.01 结算")
    void 正常二价() {
        List<Ad> cands = List.of(
                ad(1, "10.00"),   // 胜者
                ad(2, "6.00"),    // 次高价
                ad(3, "3.00"));

        var r = billing.settle(cands);

        assertTrue(r.success());
        assertEquals(new BigDecimal("10.00"), r.winBid());
        assertEquals(new BigDecimal("6.00"), r.secondPrice());
        assertEquals(new BigDecimal("6.01"), r.payPrice());
        assertEquals(SecondPriceBilling.BillingResult.NORMAL, r.edgeCase());
    }

    @Test
    @DisplayName("边界1：只有一条候选，按出价 60% 结算")
    void 单候选按比例结算() {
        List<Ad> cands = List.of(ad(1, "10.00"));

        var r = billing.settle(cands);

        assertTrue(r.success());
        assertEquals(new BigDecimal("10.00"), r.winBid());
        assertEquals(new BigDecimal("6.00"), r.payPrice());
        // 没有第二名，secondPrice 为 null
        assertNull(r.secondPrice());
        assertEquals(SecondPriceBilling.BillingResult.SECOND_PRICE_NONE, r.edgeCase());
    }

    @Test
    @DisplayName("边界1补充：单候选且出价极低，至少收 0.01 元")
    void 单候选极低出价() {
        List<Ad> cands = List.of(ad(1, "0.01"));

        var r = billing.settle(cands);

        // 0.01 x 60% = 0.006，四舍五入到 0.01
        assertEquals(new BigDecimal("0.01"), r.payPrice());
    }

    @Test
    @DisplayName("边界2：次高价 + 0.01 超过胜者出价时，按胜者出价结算")
    void 次高价加价超过胜者出价() {
        // 胜者 5.00、次高价 4.999 —— 加0.01 后是 5.009，超过胜者出价。
        // 现实里出价保留两位不会这样，但规则必须能兜住。
        List<Ad> cands = List.of(
                ad(1, "5.00"),
                ad(2, "4.999"));

        var r = billing.settle(cands);

        // 收 5.00（胜者出价），不能收 5.009
        assertEquals(new BigDecimal("5.00"), r.payPrice());
        // 次高价被抬到 0 以便追溯（保留两位小数）
        assertEquals(new BigDecimal("0.00"), r.secondPrice());
    }

    @Test
    @DisplayName("边界2补充：次高价 + 0.01 恰好等于胜者出价，不算越界")
    void 次高价加价刚好等于胜者出价() {
        // 5.00 与 4.99：4.99 + 0.01 = 5.00，恰好等于，不触发边界2
        List<Ad> cands = List.of(
                ad(1, "5.00"),
                ad(2, "4.99"));

        var r = billing.settle(cands);

        assertEquals(new BigDecimal("5.00"), r.payPrice());
        // 正常路径，secondPrice 保留真实值
        assertEquals(new BigDecimal("4.99"), r.secondPrice());
    }

    @Test
    @DisplayName("空候选：返回空结果，不抛异常")
    void 空候选() {
        var r = billing.settle(List.of());

        assertTrue(!r.success());
        assertNull(r.winner());
        assertNull(r.payPrice());
    }

    @Test
    @DisplayName("大量候选：只取前两名，后面的不影响结算")
    void 大量候选只取前两名() {
        List<Ad> cands = List.of(
                ad(1, "20.00"),
                ad(2, "12.50"),
                ad(3, "11.00"),
                ad(4, "10.00"),
                ad(5, "9.00"));

        var r = billing.settle(cands);

        assertEquals(new BigDecimal("12.51"), r.payPrice());
    }

    @Test
    @DisplayName("一价与二价的差额：验证让利金额")
    void 验证让利金额() {
        List<Ad> cands = List.of(
                ad(1, "10.00"),
                ad(2, "6.00"));

        var r = billing.settle(cands);

        BigDecimal firstPrice = new BigDecimal("10.00");   // 一价
        BigDecimal secondPrice = r.payPrice();               // 二价

        // 让利 3.99 元
        assertEquals(new BigDecimal("3.99"), firstPrice.subtract(secondPrice));
    }

    @Test
    @DisplayName("出价相同的两条：次高价等于胜者出价，按胜者出价结算")
    void 出价相同() {
        List<Ad> cands = List.of(
                ad(1, "8.00"),
                ad(2, "8.00"));

        var r = billing.settle(cands);

        // 次高价 + 0.01 = 8.01 > 8.00，按胜者出价收
        assertEquals(new BigDecimal("8.00"), r.payPrice());
    }
}
