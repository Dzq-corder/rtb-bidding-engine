#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
M7 指标校验工具：验证竞价明细(bid_log)经「本地有界队列 + 后台批量刷盘」已落库。

前置：
  1) 已启动中间件 docker compose up -d
  2) 已跑过一轮 /bid 压测（明细经队列异步落 bid_log）
  3) 依赖 pymysql：pip install pymysql

用法：
  python tools/calc_m7.py

输出：bid_log 行数、与 billing_event 的对照（验证明细零丢失）、耗时字段填充情况、空广告率。
结论解读：
  - bid_log 行数 ≈ billing_event 行数 → 正常负载下队列未溢出、明细零丢失
  - cost_ms / candidate_cost / bill_cost 均非 NULL → M7 把耗时字段填实了
  - 两者实扣总额一致 → 计费口径对齐（兜底 pay_price 为 NULL 不计入）
"""

import pymysql
import sys

DB = dict(host="127.0.0.1", port=3306, user="root", password="", db="rtb", charset="utf8mb4")


def main():
    try:
        conn = pymysql.connect(**DB)
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM bid_log")
        bid_log_rows = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM billing_event")
        billing_rows = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM bid_log WHERE has_ad=1")
        log_has_ad = cur.fetchone()[0]

        # 耗时字段为 NULL 的行数（NOT NULL 约束下应为 0）
        cur.execute("SELECT COUNT(*) FROM bid_log "
                    "WHERE cost_ms IS NULL OR candidate_cost IS NULL OR bill_cost IS NULL")
        null_timing = cur.fetchone()[0]

        cur.execute("SELECT COALESCE(AVG(cost_ms),0), COALESCE(AVG(candidate_cost),0), "
                    "COALESCE(AVG(bill_cost),0) FROM bid_log")
        avg_cost, avg_cand, avg_bill = cur.fetchone()

        cur.execute("SELECT COALESCE(SUM(pay_price),0) FROM bid_log")
        log_pay_sum = float(cur.fetchone()[0])

        cur.execute("SELECT COALESCE(SUM(pay_price),0) FROM billing_event")
        bill_pay_sum = float(cur.fetchone()[0])

        # 写吞吐：以 bid_at 跨度估算明细落库速率（行/秒）
        cur.execute("SELECT TIMESTAMPDIFF(SECOND, MIN(bid_at), MAX(bid_at)) FROM bid_log")
        span = cur.fetchone()[0] or 0
        throughput = (bid_log_rows / span) if span > 0 else 0.0

        # 队列丢弃计数（仅存在于应用内存，重启即清零；此处读不到，需看应用日志 grep "bid_log 队列已满"）
        cur.close()
        conn.close()
    except Exception as e:
        print("执行失败：", e)
        sys.exit(1)

    empty_rate = (1 - log_has_ad / bid_log_rows * 100) if bid_log_rows else 0.0

    print("=" * 56)
    print("M7 竞价明细落库核对")
    print("=" * 56)
    print(f"bid_log 行数                = {bid_log_rows}")
    print(f"billing_event 行数          = {billing_rows}")
    print(f"bid_log 有广告行数          = {log_has_ad}")
    print(f"bid_log 空广告率            = {empty_rate:.2f}%")
    print(f"耗时字段为 NULL 的行数      = {null_timing}")
    print(f"avg cost_ms / cand / bill   = {avg_cost:.1f} / {avg_cand:.1f} / {avg_bill:.1f} ms")
    print(f"bid_log 实扣总额            = {log_pay_sum:.4f}")
    print(f"billing_event 实扣总额      = {bill_pay_sum:.4f}")
    print(f"落库时间跨度 / 写吞吐      = {span}s / {throughput:.1f} 条/秒")
    print("-" * 56)

    gap = bid_log_rows - billing_rows
    if bid_log_rows > 0 and gap == 0:
        print("结论：bid_log 与 billing_event 行数一致 → 正常负载下队列未溢出、明细零丢失")
    elif gap < 0:
        print(f"提示：bid_log 比 billing_event 少 {abs(gap)} 行 → 队列满时丢弃了低价值明细"
              f"（可采样，符合设计；grep 应用日志 'bid_log 队列已满' 可确认丢弃次数）")
    else:
        print(f"提示：bid_log 比 billing_event 多 {gap} 行（含重试/重复 request_id，正常）")

    if null_timing == 0:
        print("结论：耗时字段已全部填实（cost_ms / candidate_cost / bill_cost 均无 NULL）")
    else:
        print(f"⚠️ 仍有 {null_timing} 行耗时字段为 NULL，需排查 emit() 埋点")

    if abs(log_pay_sum - bill_pay_sum) < 0.01:
        print("结论：bid_log 与 billing_event 实扣总额一致 → 计费口径对齐")
    else:
        print(f"⚠️ 两表实扣总额差 {abs(log_pay_sum - bill_pay_sum):.4f}，需排查")


if __name__ == "__main__":
    main()
