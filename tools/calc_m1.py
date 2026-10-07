#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
M1 baseline 压测指标：解析 JMeter 产物，输出
  - 吞吐 QPS（总样本 / 总时长，秒）
  - 延迟 avg / P50 / P95 / P99 / max（基于 JTL 的 elapsed 列）
  - 空广告率 / 兜底率（来自 results/bid-detail.csv 的 hasAd / fallback 列）

这是简历首段的「优化前 A」：QPS(A) 与 P99(A)。
与 M7（当前 m1-explore 分支 HEAD）再跑一轮得到「优化后 B」，两者即成对。

前置：先跑 JMeter 生成 jmeter/results/rtb-m1.jtl 与 jmeter/results/bid-detail.csv
用法：
  python tools/calc_m1.py [jtl_path] [csv_path]
  默认读取 jmeter/results/ 下上述两文件
"""

import csv
import os
import sys


def pct(sorted_vals, p):
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
    base = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "jmeter", "results")
    jtl = os.path.abspath(sys.argv[1] if len(sys.argv) > 1 else os.path.join(base, "rtb-m1.jtl"))
    csvp = os.path.abspath(sys.argv[2] if len(sys.argv) > 2 else os.path.join(base, "bid-detail.csv"))

    if not os.path.exists(jtl):
        print("找不到 JTL:", jtl)
        print("请先跑：jmeter -n -t jmeter/rtb-bid-m1.jmx -l jmeter/results/rtb-m1.jtl")
        sys.exit(1)

    ts, el, ok, tot = [], [], 0, 0
    with open(jtl, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            tot += 1
            try:
                el.append(int(row["elapsed"]))
                ts.append(int(row["timeStamp"]))
                if (row.get("success") or "").strip().lower() == "true":
                    ok += 1
            except (ValueError, KeyError):
                pass

    if tot == 0:
        print("JTL 无有效样本行")
        sys.exit(1)

    el.sort()
    dur = (max(ts) - min(ts)) / 1000.0 if ts else 0.0
    qps = tot / dur if dur > 0 else 0.0

    print("=" * 60)
    print("M1 Baseline · 压测汇总")
    print("=" * 60)
    print(f"样本数        : {tot}")
    print(f"成功率        : {ok * 100.0 / tot:.2f}%")
    print(f"总时长        : {dur:.1f} s")
    print(f"吞吐 QPS      : {qps:.1f}")
    print("-" * 60)
    print(f"延迟 avg  : {sum(el) / len(el):.1f} ms")
    print(f"延迟 P50  : {pct(el, 50):.0f} ms")
    print(f"延迟 P95  : {pct(el, 95):.0f} ms")
    print(f"延迟 P99  : {pct(el, 99):.0f} ms")
    print(f"延迟 max  : {el[-1]} ms")

    if os.path.exists(csvp):
        h = fb = n = 0
        with open(csvp, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("hasAd") in (None, ""):
                    continue
                n += 1
                if row.get("hasAd") == "1":
                    h += 1
                if row.get("fallback") == "1":
                    fb += 1
        if n > 0:
            print("-" * 60)
            print(f"空广告率      : {(1 - h / n) * 100:.2f}%  (hasAd=0)")
            print(f"兜底率        : {fb * 100.0 / n:.2f}%  (fallback=1)")
            if fb == 0:
                print("  （本论依赖健康，全部正常成交 / 真实广告）")
    print("=" * 60)


if __name__ == "__main__":
    main()
