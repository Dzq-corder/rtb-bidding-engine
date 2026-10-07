#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
M3 预算超扣对账：统计「单广告累计扣费 > 日预算」的广告数，应为 0。

依据：BudgetService 用 Lua 脚本原子完成「校验余额 → 扣减 → 记消耗」，
Redis 单线程执行期间不会被打断，因此并发请求之间不存在「都读到旧余额、都通过校验」
的窗口，单广告累计扣费不会超过日预算。本脚本用计费事件表做事后对账验证。

注意：计费事件表（billing_event）在 m6-counter-schema.sql 中的列是
  event_id, ad_id, advertiser_id, pay_price, ts
（与 schema.sql 的版本不同，但 ad_id / pay_price 两列都存在，本脚本只依赖这两列）。

前置：
  1) 已启动中间件 docker compose up -d
  2) 已跑过一轮 /bid 压测且确有成交（billing_event 有数据）
  3) 依赖 pymysql：pip install pymysql

用法：
  python tools/calc_m3.py
"""

import pymysql
import sys

DB = dict(host="127.0.0.1", port=3306, user="root", password="", db="rtb", charset="utf8mb4")

# 单广告累计实扣 > 日预算 的广告数
SQL_OVER = """
SELECT COUNT(*) FROM (
    SELECT be.ad_id
    FROM billing_event be
    JOIN ad a ON a.id = be.ad_id
    GROUP BY be.ad_id, a.daily_budget
    HAVING COALESCE(SUM(be.pay_price), 0) > a.daily_budget
) t
"""


def main():
    try:
        conn = pymysql.connect(**DB)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM billing_event")
        ev = cur.fetchone()[0]
        cur.execute(SQL_OVER)
        over = cur.fetchone()[0]
        cur.close()
        conn.close()
    except Exception as e:
        print("执行失败：", e)
        sys.exit(1)

    print("=" * 56)
    print("M3 预算超扣对账")
    print("=" * 56)
    print(f"billing_event 事件数 : {ev}")
    print(f"超扣广告数          : {over}  （设计上应为 0）")
    print("-" * 56)
    if ev == 0:
        print("⚠️ billing_event 为空：先确认 M6 旁路已把事件落库（见手册前置核对），再跑本脚本。")
    elif over == 0:
        print("结论：Lua 原子扣减下，单广告累计扣费不超过日预算 → 超扣 0 笔")
    else:
        print(f"⚠️ 发现 {over} 个广告超扣，需排查 BudgetService / 对账口径")


if __name__ == "__main__":
    main()
