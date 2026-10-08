#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
索引前后对比：读取两次 JMeter 压测产物（before / after），输出对比表。

前置：用 jmeter/rtb-bid-index-cmp.jmx 在建索引前、后各跑一次：
    jmeter -n -t jmeter/rtb-bid-index-cmp.jmx -Joutfile=before ...
    （建索引）
    jmeter -n -t jmeter/rtb-bid-index-cmp.jmx -Joutfile=after  ...
分别产出 jmeter/results/before.jtl 与 after.jtl（及对应 -detail.csv）。

用法：
    python tools/compare_index.py
    python tools/compare_index.py --before jmeter/results/before.jtl --after jmeter/results/after.jtl

变化列语义：
    - 延迟类（avg/P50/P95/P99/max）：负值 = 下降 = 改善（如 -92.7%）
    - 吞吐 QPS：正值 = 上升 = 改善（如 +242.0%）
    - 成功率/空广告率/兜底率：直接看绝对数值变化
"""

import argparse
import csv
import os
import sys


def pct(vals, p):
    if not vals:
        return 0
    if len(vals) == 1:
        return vals[0]
    idx = (len(vals) - 1) * p / 100.0
    lo = int(idx)
    hi = min(lo + 1, len(vals) - 1)
    frac = idx - lo
    return vals[lo] * (1 - frac) + vals[hi] * frac


def load_jtl(path):
    ts, el, ok, tot = [], [], 0, 0
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            tot += 1
            try:
                el.append(int(row["elapsed"]))
                ts.append(int(row["timeStamp"]))
                if (row.get("success") or "").strip().lower() == "true":
                    ok += 1
            except (ValueError, KeyError):
                pass
    return ts, el, ok, tot


def load_detail(path):
    """返回 (has_ad_count, fallback_count, total) 或 None（文件不存在）。"""
    if not path or not os.path.exists(path):
        return None
    h = fb = n = 0
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("hasAd") in (None, ""):
                continue
            n += 1
            if row.get("hasAd") == "1":
                h += 1
            if row.get("fallback") == "1":
                fb += 1
    return (h, fb, n)


def stats(ts, el, ok, tot):
    el_sorted = sorted(el)
    dur = (max(ts) - min(ts)) / 1000.0 if ts else 0.0
    qps = tot / dur if dur > 0 else 0.0
    return {
        "tot": tot,
        "ok": ok,
        "dur": dur,
        "qps": qps,
        "avg": sum(el) / len(el) if el else 0,
        "p50": pct(el_sorted, 50),
        "p95": pct(el_sorted, 95),
        "p99": pct(el_sorted, 99),
        "max": el_sorted[-1] if el_sorted else 0,
        "succ": ok * 100.0 / tot if tot else 0,
    }


def main():
    base = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "jmeter", "results")
    ap = argparse.ArgumentParser(description="RTB 索引前后 JMeter 压测对比")
    ap.add_argument("--before", default=os.path.join(base, "before.jtl"))
    ap.add_argument("--after", default=os.path.join(base, "after.jtl"))
    ap.add_argument("--before-csv", default=os.path.join(base, "before-detail.csv"))
    ap.add_argument("--after-csv", default=os.path.join(base, "after-detail.csv"))
    args = ap.parse_args()

    for p in (args.before, args.after):
        if not os.path.exists(p):
            print("找不到:", p)
            print("请先用 jmeter/rtb-bid-index-cmp.jmx 在建索引前/后各跑一次。")
            sys.exit(1)

    b = stats(*load_jtl(args.before))
    a = stats(*load_jtl(args.after))
    cb = load_detail(args.before_csv)
    ca = load_detail(args.after_csv)

    def change(before, after):
        if before == 0:
            return "NA"
        return f"{(after - before) / before * 100:+.1f}%"

    print("=" * 74)
    print("索引前后对比 · POST /bid 端到端（结果文件: before vs after）")
    print("=" * 74)
    print(f"{'指标':<14}{'建索引前':>16}{'建索引后':>16}{'变化':>16}")
    print("-" * 74)
    print(f"{'样本数':<14}{b['tot']:>16.0f}{a['tot']:>16.0f}{'':>16}")
    print(f"{'成功率':<14}{b['succ']:>15.2f}%{a['succ']:>15.2f}%{change(b['succ'], a['succ']):>16}")
    print(f"{'吞吐 QPS':<14}{b['qps']:>16.1f}{a['qps']:>16.1f}{change(b['qps'], a['qps']):>16}")
    print(f"{'延迟 avg':<14}{b['avg']:>15.1f}ms{a['avg']:>15.1f}ms{change(b['avg'], a['avg']):>16}")
    print(f"{'延迟 P50':<14}{b['p50']:>15.0f}ms{a['p50']:>15.0f}ms{change(b['p50'], a['p50']):>16}")
    print(f"{'延迟 P95':<14}{b['p95']:>15.0f}ms{a['p95']:>15.0f}ms{change(b['p95'], a['p95']):>16}")
    print(f"{'延迟 P99':<14}{b['p99']:>15.0f}ms{a['p99']:>15.0f}ms{change(b['p99'], a['p99']):>16}")
    print(f"{'延迟 max':<14}{b['max']:>15.0f}ms{a['max']:>15.0f}ms{change(b['max'], a['max']):>16}")

    if cb and ca:
        b_empty = (1 - cb[0] / cb[2]) * 100 if cb[2] else 0
        a_empty = (1 - ca[0] / ca[2]) * 100 if ca[2] else 0
        b_fb = cb[1] * 100.0 / cb[2] if cb[2] else 0
        a_fb = ca[1] * 100.0 / ca[2] if ca[2] else 0
        print(f"{'空广告率':<14}{b_empty:>15.2f}%{a_empty:>15.2f}%{change(b_empty, a_empty):>16}")
        print(f"{'兜底率':<14}{b_fb:>15.2f}%{a_fb:>15.2f}%{change(b_fb, a_fb):>16}")
    print("-" * 74)
    print("说明：延迟类负值=下降(改善)；QPS 正值=上升(改善)。")
    print("建索引目标：ad(status, bid) 消除全表扫描 + filesort。")
    print("=" * 74)


if __name__ == "__main__":
    main()
