#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RTB 引擎 · 空广告率 / 兜底率 对照压测工具（M5 超时熔断 + 兜底降级）

=====================================================================
测什么
=====================================================================
按 A/B 两组口径，测「空广告率」：

    空广告率 = N(响应 data.hasAd == false 或 HTTP 非 200) / N(总请求)

A 组（对照组，无兜底）：服务以 --rtb.fallback.enabled=false 启动，
    此时 BidService.buildFallback 直接返回 hasAd=false 的空结果，
    等价于「这套超时/熔断机制还没有兜底出口之前」的实现。
B 组（实验组，有兜底）：服务以默认配置启动，任何失败都收敛到兜底广告。

=====================================================================
四类故障场景怎么跑
=====================================================================
请求层能造的只有「候选为空」，其余三类靠改服务的启动参数 / 改基础设施，
所以一个场景 = 一次服务启动，四个场景分别启动四次：

  S1 正常      服务默认参数            --mode normal
  S2 候选为空  服务默认参数            --mode nocand   （region 填 999999）
  S3 Redis 不可达  启动时加 --spring.data.redis.port=6399   --mode normal
  S4 DB 慢查询    ALTER TABLE ad_target DROP INDEX idx_dim_lookup  --mode normal

每次启动都要固定 thread count 与 warmup，避免 JIT / 连接池未预热影响首屏延迟。

=====================================================================
用法（Windows CMD）
=====================================================================
    python tools/bench_fallback.py --port 8081 --n 2000 --mode normal --tag "A-S1正常"
    python tools/bench_fallback.py --port 8081 --n 2000 --mode nocand --tag "A-S2候选为空"

结果会同时打到 stdout 和追加进 data/bench_fallback_history.csv，方便 A/B 两组对齐看。
"""

import argparse
import csv
import json
import os
import random
import statistics
import sys
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DIM_DICT = os.path.join(ROOT, "data", "dim_dict.csv")
HISTORY = os.path.join(ROOT, "data", "bench_fallback_history.csv")

AGES = [19, 23, 27, 29, 33, 38, 43, 48, 55, 62]
GENDERS = [1, 2]


def load_regions():
    """从 dim_dict.csv 取 dim_type=1 的全部区县码。"""
    if not os.path.exists(DIM_DICT):
        sys.exit(f"[x] 找不到 {DIM_DICT}，请先跑 tools/gen_ad_data.py 生成维度字典")
    codes = []
    with open(DIM_DICT, "r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            if row["dim_type"].strip() == "1":
                codes.append(row["dim_value"].strip())
    if not codes:
        sys.exit("[x] dim_dict.csv 里没有 dim_type=1 的地域取值")
    return codes


def pct(sorted_vals, q):
    """百分位数。q 取 0~100，返回排序后第 ceil(q% * n) 个元素。"""
    if not sorted_vals:
        return 0.0
    n = len(sorted_vals)
    idx = max(1, min(n, int(n * q / 100.0 + 0.999999)))
    return sorted_vals[idx - 1]


def one_call(port, payload, timeout_s):
    """发一次 /bid。返回 (http_code, hasAd, fallback, latency_ms, err)"""
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}/bid",
        data=payload.encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            body = json.loads(resp.read().decode("utf-8"))
        ms = (time.perf_counter() - t0) * 1000.0
        data = body.get("data") or {}
        return resp.status, bool(data.get("hasAd")), bool(data.get("fallback")), ms, ""
    except urllib.error.HTTPError as e:
        ms = (time.perf_counter() - t0) * 1000.0
        return e.code, False, False, ms, f"HTTP{e.code}"
    except Exception as e:  # noqa: BLE001
        ms = (time.perf_counter() - t0) * 1000.0
        return 0, False, False, ms, type(e).__name__


def build_payload(regions, mode, rnd, slot_seed):
    region = "999999" if mode == "nocand" else rnd.choice(regions)
    body = {
        "slotCode": f"slot-{slot_seed:03d}",
        "region": region,
        "age": rnd.choice(AGES),
        "gender": rnd.choice(GENDERS),
    }
    return json.dumps(body, ensure_ascii=False)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8081)
    ap.add_argument("--n", type=int, default=2000, help="总请求数")
    ap.add_argument("--concurrency", type=int, default=32)
    ap.add_argument("--warmup", type=int, default=50, help="预热请求数，不计入统计")
    ap.add_argument("--timeout", type=float, default=30.0, help="单次请求客户端超时（秒）")
    ap.add_argument("--mode", choices=["normal", "nocand"], default="normal")
    ap.add_argument("--tag", default="", help="场景标签，写进历史文件方便对照")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    regions = load_regions()
    rnd = random.Random(args.seed)

    # ---------- 预热：触发 JIT / Hikari / Lettuce 初始化，不计入统计 ----------
    for i in range(args.warmup):
        one_call(args.port, build_payload(regions, args.mode, rnd, i % 20), args.timeout)

    payloads = [build_payload(regions, args.mode, rnd, i % 20) for i in range(args.n)]

    lat, results = [], []
    t_start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        futs = [pool.submit(one_call, args.port, p, args.timeout) for p in payloads]
        for fu in futs:
            results.append(fu.result())
    wall = time.perf_counter() - t_start

    for code, has_ad, fb, ms, err in results:
        lat.append(ms)
    lat.sort()

    total = len(results)
    http_bad = sum(1 for r in results if r[0] != 200)
    empty = sum(1 for r in results if not r[1])          # hasAd=false 即空广告
    fb_cnt = sum(1 for r in results if r[2])             # fallback=true
    err_cnt = sum(1 for r in results if r[4])

    line = "-" * 64
    print(line)
    print(f"场景标签        : {args.tag or '(未命名)'}")
    print(f"模式/并发/样本  : {args.mode} / {args.concurrency} 线程 / {total} 次  (seed={args.seed})")
    print(f"总墙钟          : {wall:.2f}s   吞吐 ≈ {total / wall:.1f} req/s")
    print(line)
    print(f"HTTP 非 200     : {http_bad}  ({http_bad / total * 100:.2f}%)")
    print(f"客户端异常      : {err_cnt}")
    print(f"** 空广告数     : {empty}  ({empty / total * 100:.2f}%) **")
    print(f"** 兜底数       : {fb_cnt}  ({fb_cnt / total * 100:.2f}%) **")
    print(f"正常竞价数      : {total - empty - fb_cnt}  ({(total - empty - fb_cnt) / total * 100:.2f}%)")
    print(line)
    print(f"延迟 ms         : P50={pct(lat, 50):.2f}  P90={pct(lat, 90):.2f}  "
          f"P95={pct(lat, 95):.2f}  P99={pct(lat, 99):.2f}  max={lat[-1]:.2f}  "
          f"mean={statistics.fmean(lat):.2f}")
    print(line)

    os.makedirs(os.path.dirname(HISTORY), exist_ok=True)
    need_header = not os.path.exists(HISTORY) or os.path.getsize(HISTORY) == 0
    with open(HISTORY, "a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        if need_header:
            w.writerow(["tag", "mode", "total", "empty_cnt", "empty_pct",
                        "fallback_cnt", "fallback_pct", "http_bad", "p50_ms",
                        "p90_ms", "p99_ms", "max_ms", "qps"])
        w.writerow([args.tag, args.mode, total, empty, f"{empty / total * 100:.2f}",
                    fb_cnt, f"{fb_cnt / total * 100:.2f}", http_bad,
                    f"{pct(lat, 50):.2f}", f"{pct(lat, 90):.2f}", f"{pct(lat, 99):.2f}",
                    f"{lat[-1]:.2f}", f"{total / wall:.1f}"])
    print(f"已追加到 {os.path.relpath(HISTORY, ROOT)}")


if __name__ == "__main__":
    main()
