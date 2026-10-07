#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
M5 指标计算：读取 JMeter 写出的 results/bid-detail.csv，统计
  - 样本数 / 填充率 / 兜底率
  - 延迟分布（avg / P50 / P95 / P99 / max）

用法：
  python tools/calc_m5.py [csv_path]
  默认读取 jmeter/results/bid-detail.csv

字段要求（由 rtb-bid-m1.jmx 的 JSR223 写出）：
  ts,label,hasAd,winBid,payPrice,rtMs,fallback
"""

import csv
import os
import sys


def percentile(sorted_vals, p):
    if not sorted_vals:
        return 0
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    idx = (len(sorted_vals) - 1) * p / 100.0
    lo = int(idx)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = idx - lo
    return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac


def main():
    default = os.path.join(os.path.dirname(__file__), "..", "jmeter", "results", "bid-detail.csv")
    path = sys.argv[1] if len(sys.argv) > 1 else default
    path = os.path.abspath(path)

    if not os.path.exists(path):
        print(f"找不到文件: {path}")
        print("请先跑压测生成 jmeter/results/bid-detail.csv")
        sys.exit(1)

    total = 0
    has_ad = 0
    fallback = 0
    rt = []

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # 跳过非数据行（表头已由 DictReader 处理）
            if row.get("hasAd") in (None, ""):
                continue
            total += 1
            if row.get("hasAd") == "1":
                has_ad += 1
            if row.get("fallback") == "1":
                fallback += 1
            try:
                rt.append(int(row.get("rtMs", "0")))
            except ValueError:
                pass

    if total == 0:
        print("CSV 无有效数据行")
        sys.exit(1)

    rt_sorted = sorted(rt)
    fill_rate = has_ad * 100.0 / total
    fallback_rate = fallback * 100.0 / total

    print("=" * 60)
    print("M5 · 超时预算 + 熔断 + 兜底 指标")
    print("=" * 60)
    print(f"样本数        : {total}")
    print(f"填充率        : {fill_rate:.2f}%   (hasAd=1)")
    print(f"兜底率        : {fallback_rate:.2f}%   (fallback=1)")
    print(f"正常成交率    : {100.0 - fallback_rate:.2f}%   (非兜底)")
    print("-" * 60)
    print(f"rt avg   : {sum(rt) / len(rt):.1f} ms")
    print(f"rt P50   : {percentile(rt_sorted, 50):.0f} ms")
    print(f"rt P95   : {percentile(rt_sorted, 95):.0f} ms")
    print(f"rt P99   : {percentile(rt_sorted, 99):.0f} ms")
    print(f"rt max   : {rt_sorted[-1]} ms")
    print("=" * 60)

    # 自检提示
    if fallback == 0:
        print("⚠️ 本轮未触发任何兜底：依赖健康，全部正常成交（fill_rate=100% 来自真实广告）。")
        print("   若要演示 M5 兜底能力，把 rtb.fallback.force 改为 true，或临时调小 rtb.timeout.db-ms。")
    elif fallback == total:
        print("✅ 本轮 100% 兜底：依赖故障被完全吸收，填充率仍为 100%（兜底广告填补），空广告率=0%。")
    else:
        print(f"ℹ️ 本轮部分兜底（{fallback_rate:.1f}%）：可能是熔断 OPEN 期间 + 恢复探活期间的混合结果。")


if __name__ == "__main__":
    main()
