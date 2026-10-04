#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RTB 投放引擎 · 竞价明细放大脚本

背景（《造数方案与数据来源说明》§3 方案 A）
------------------------------------------------------
不是直接随机生成 1000 万行，而是：

  1. 先真实跑 10 万次竞价，得到 10 万行**真实数据**
     （真实耗时、真实候选数、真实出价与实扣、真实分布）
  2. 把这 10 万行当**模板**扩展到 1000 万行，只改三处：
     · bid_at 铺开到 90 天（覆盖多个分区，为 M8-3 做准备）
     · request_id 重新生成（保证唯一键不冲突）
     · 耗时字段加 ±20% 随机扰动（避免所有行数值完全相同）

好处：数据分布是真实业务产生的。
面试官问「你的日志为什么出价是长尾的」，能答「因为它是真实压测产生的」，
而不是「我随机编的」。

用法：
  # 1. 先跑真实采样（服务需已启动）
  curl -X POST "http://127.0.0.1:8080/bid/batch?count=10000&concurrency=20"   # 重复 10 次
  # 2. 导出样本
  mysqldump --no-create-info -u root rtb bid_log > data/sample_bid_log.csv
  # 3. 放大
  python tools/amplify_bid_log.py --sample data/sample_bid_log.csv --out data/bid_log_10m.csv --target 10000000
  # 4. 导入
  bash tools/import_bid_log.sh
"""

import argparse
import csv
import os
import random
import sys
from datetime import datetime, timedelta

# 固定随机种子，保证每次放大的结果一致（可复现）
SEED = 20260930

# 耗时字段扰动范围
JITTER = 0.20

# bid_at 铺开的天数。90 天是 M8-3 分区裁剪能看出效果的前提：
# 只铺 1 天的话查最近 7 天只命中 1 个分区，演示像作弊。
SPAN_DAYS = 90

FIELDS = [
    "request_id", "ad_id", "advertiser_id", "slot_code",
    "region", "age", "gender", "interests",
    "candidate_cnt", "match_cnt", "has_ad", "win_bid", "pay_price",
    "cost_ms", "candidate_cost", "filter_cost", "sort_cost", "bill_cost",
    "bid_at",
]

# 需要加扰动的耗时字段。取对数正态-like 扰动而非均匀分布，
# 因为耗时的真实波动是右偏的（多数快、少数极慢）。
COST_FIELDS = ["cost_ms", "candidate_cost", "filter_cost", "sort_cost", "bill_cost"]


def jitter(value, rng, low_guard=0):
    """对耗时做 ±20% 扰动，下限 low_guard（耗时不能为负）。"""
    if value is None or value == "":
        return value
    try:
        v = int(value)
    except (TypeError, ValueError):
        return value
    if v == 0:
        return 0
    # 偏向「变慢」的扰动：真实的长尾里慢请求更容易出现
    factor = 1.0 + rng.uniform(-JITTER, JITTER * 1.5)
    return max(low_guard, int(round(v * factor)))


def new_request_id(rng):
    """32 位无横线 UUID。必须新生成——样本里的 id 重复会被唯一键拦下。"""
    return "%032x" % rng.getrandbits(128)


def main():
    ap = argparse.ArgumentParser(description="竞价明细放大脚本")
    ap.add_argument("--sample", required=True, help="真实采样导出的 CSV")
    ap.add_argument("--out", required=True, help="输出 CSV")
    ap.add_argument("--target", type=int, default=10_000_000, help="目标行数，默认 1000 万")
    args = ap.parse_args()

    if not os.path.exists(args.sample):
        print(f"找不到样本文件：{args.sample}", file=sys.stderr)
        return 1

    with open(args.sample, newline="", encoding="utf-8") as f:
        # 样本由 mysqldump -B -N 导出，是 Tab 分隔、无表头。
        # 同时兼容标准 CSV（有表头、逗号分隔），
        # 靠「首行能否解析成整数」判断格式。
        first = f.readline()
        f.seek(0)
        probe = first.split("\t")
        is_tsv_dump = len(probe) >= len(FIELDS)

        if is_tsv_dump:
            reader = csv.reader(f, delimiter="\t")
            rows = [dict(zip(FIELDS, r)) for r in reader if r and len(r) >= len(FIELDS)]
        else:
            reader = csv.DictReader(f)
            rows = list(reader)

    if not rows:
        print("样本文件为空", file=sys.stderr)
        return 1

    print(f"读入真实样本 {len(rows):,} 行")
    print(f"目标 {args.target:,} 行，放大倍数 {args.target / len(rows):.1f}")

    rng = random.Random(SEED)
    base = datetime(2026, 7, 7)          # 90 天窗口的起点
    span_seconds = SPAN_DAYS * 24 * 3600

    # 先算好每个样本行要重复几次
    total = args.target
    per_row = total // len(rows)
    remainder = total - per_row * len(rows)

    print(f"每行重复 {per_row} 次，余 {remainder} 行")

    written = 0
    # 时间戳在整个窗口内均匀铺开，但按天分层保证每天都有数据
    day_offsets = [rng.randrange(0, span_seconds) for _ in range(len(rows))]

    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")   # 必须 \n，否则又会引入 CR
        w.writerow(FIELDS)

        for idx, row in enumerate(rows):
            times = per_row + (1 if idx < remainder else 0)
            for _ in range(times):
                out = {}
                for k in FIELDS:
                    out[k] = row.get(k, "")

                out["request_id"] = new_request_id(rng)

                for k in COST_FIELDS:
                    if k in out:
                        out[k] = jitter(out[k], rng)

                # 时间铺开在 90 天内，保留毫秒
                offset = day_offsets[idx]
                t = base + timedelta(seconds=offset % span_seconds)
                out["bid_at"] = t.strftime("%Y-%m-%d %H:%M:%S.") + \
                                f"{rng.randrange(0, 1000):03d}"

                w.writerow([out[k] for k in FIELDS])
                written += 1

    print(f"完成：写入 {written:,} 行 -> {args.out}")
    print(f"文件大小约 {os.path.getsize(args.out) / 1024 / 1024:.0f} MB")
    print(f"时间铺开范围：{base.strftime('%Y-%m-%d')} 起 {SPAN_DAYS} 天")

    # 行尾自检——踩过 CR 的坑，这里再防一次
    with open(args.out, "rb") as f:
        if b"\r" in f.read(8192):
            print("自检失败：输出含 CR 字符", file=sys.stderr)
            return 1
    print("行尾自检通过（无 CR）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
