#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
M6 指标校验工具：读取 MySQL 中三消费组落库数据，核对对账一致性与幂等去重。

前置：
  1) 已执行 sql/m6-counter-schema.sql 建表
  2) 已跑过一轮 /bid 压测（事件已发往 Kafka 并被三消费组消费落库）
  3) 依赖 pymysql：pip install pymysql

用法：
  python tools/calc_m6.py

输出：大盘曝光总数、计费事件行数、计费实扣总额、各消费组去重行数、对账差异。
结论解读：
  - 对账差异 = 0  → 重复投递下无重复账单（幂等生效）
  - billing_event 行数 ≈ 大盘曝光总数 → 每条竞价事件都进了计费
"""

import pymysql
import sys

DB = dict(host="127.0.0.1", port=3306, user="root", password="", db="rtb", charset="utf8mb4")


def main():
    try:
        conn = pymysql.connect(**DB)
        cur = conn.cursor()

        # 发出的事件总数 ≈ 大盘曝光总数（每条竞价事件都会进 hourly-preagg）
        cur.execute("SELECT COALESCE(SUM(impressions), 0) FROM bid_stat_hourly")
        total_imp = cur.fetchone()[0]

        # 计费事件行数（billing-recon 去重后）
        cur.execute("SELECT COUNT(*) FROM billing_event")
        billing_rows = cur.fetchone()[0]

        # 计费实扣总额
        cur.execute("SELECT COALESCE(SUM(pay_price), 0) FROM billing_event")
        billing_sum = float(cur.fetchone()[0])

        # 去重表各组的行数（幂等去重记录）
        cur.execute("SELECT grp, COUNT(*) FROM processed_event GROUP BY grp")
        dedup = dict(cur.fetchall())

        # 频次表总曝光计数
        cur.execute("SELECT COALESCE(SUM(cnt), 0) FROM ad_freq")
        freq_total = cur.fetchone()[0]

        # 对账：billing-recon 去重行数 应等于 billing_event 行数（无重复账单）
        recon_dedup = dedup.get("billing-recon", 0)
        diff = billing_rows - recon_dedup

        cur.close()
        conn.close()
    except Exception as e:
        print("执行失败：", e)
        sys.exit(1)

    print("=" * 56)
    print("M6 旁路消费落库核对")
    print("=" * 56)
    print(f"大盘曝光总数 (bid_stat_hourly.impressions) = {total_imp}")
    print(f"计费事件行数 (billing_event)               = {billing_rows}")
    print(f"计费实扣总额 (SUM pay_price)               = {billing_sum:.4f}")
    print(f"频次表总曝光 (SUM ad_freq.cnt)             = {freq_total}")
    print(f"幂等去重表分组行数 (processed_event)        = {dedup}")
    print("-" * 56)
    print(f"对账差异 (billing_event 行数 - billing-recon 去重行) = {diff}")
    if diff == 0:
        print("结论：差异=0 → 重复投递下无重复账单（幂等生效）")
    else:
        print(f"⚠️ 差异≠0 → 存在重复账单或落库不一致，需排查")
    print(f"说明：billing_event 行数({billing_rows}) 应与大盘曝光数({total_imp})一致"
          f"（兜底事件也计费? 否，兜底 pay_price 为 NULL 但仍记一行，仅用于计数）")


if __name__ == "__main__":
    main()
