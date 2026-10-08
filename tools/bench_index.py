#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
bench_index.py — 测量 ad 表建立 (status, bid) 联合索引前后
                 findCandidates 候选检索 SQL 的耗时对比（SQL 级，纯数据库，不含应用链路）

为什么是 SQL 级而不是接口级：
    接口 /bid 链路包含 Redis 扣减、Kafka 旁路、明细落库等，会把「SQL 索引」这一单一变量的
    收益稀释掉。本项目要求「每个数字都能现场复现、能归因」，所以这里直接对那条 SQL 计时，
    干净地隔离出索引前后的差异。若需接口级数字，见文末「接口级对照」说明，复用 jmeter + calc_m1.py。

连接：perf MySQL（localhost:3306, user=root, 空密码, db=rtb）
依赖：pip install pymysql

用法：
    python tools/bench_index.py                  # 默认：baseline(无索引) -> 建索引 -> 再测，最后恢复无索引
    python tools/bench_index.py --rounds 50 --warmup 10
    python tools/bench_index.py --keep-index     # 测完保留索引（便于接着跑接口级压测）
    python tools/bench_index.py --with-category  # 查询额外带 a.category_code = ? 条件

输出：每个阶段的 EXPLAIN（type/key/rows/Extra）+ 耗时 avg/p50/p99 + 提升百分比
结果可直接填入简历「联合索引优化」段（见 RTB投放引擎-简历项目经历.md）。
"""

import pymysql
import time
import statistics
import argparse
import sys

# ---- 连接参数（perf MySQL）----
HOST = "127.0.0.1"
PORT = 3306
USER = "root"
PASSWORD = ""   # perf 库 root 空密码
DB = "rtb"

INDEX_NAME = "idx_status_bid"
CREATE_INDEX = f"CREATE INDEX {INDEX_NAME} ON ad (status, bid)"
DROP_INDEX = f"DROP INDEX {INDEX_NAME} ON ad"

# 复现 AdMapper.xml#findCandidates 的 SQL（无 LIMIT，与线上一致）
def build_sql(region, age, gender, category, with_category):
    joins = []
    if region is not None:
        joins.append(f"JOIN ad_target t1 ON t1.ad_id = a.id AND t1.dim_type = 1 AND t1.dim_value = '{region}'")
    if age is not None:
        joins.append(f"JOIN ad_target t2 ON t2.ad_id = a.id AND t2.dim_type = 2 AND t2.dim_value = '{age}'")
    if gender is not None:
        joins.append(f"JOIN ad_target t3 ON t3.ad_id = a.id AND t3.dim_type = 3 AND t3.dim_value = '{gender}'")
    cat = f"AND a.category_code = '{category}'" if (with_category and category) else ""
    return (
        "SELECT a.id, a.advertiser_id, a.title, a.creative_url, a.category_code, "
        "a.bid, a.daily_budget, a.status, a.created_at, a.updated_at "
        "FROM ad a " + " ".join(joins) +
        f" WHERE a.status = 1 {cat} ORDER BY a.bid DESC"
    )

def fetch_dim_values(conn):
    """从 ad_target 取一组真实存在的定向值，确保查询有命中（模拟真实请求分布）。"""
    with conn.cursor() as cur:
        def one(dim_type):
            cur.execute("SELECT dim_value FROM ad_target WHERE dim_type=%s GROUP BY dim_value LIMIT 1", (dim_type,))
            r = cur.fetchone()
            return r[0] if r else None
        region = one(1)
        age = one(2)
        gender = one(3)
        cur.execute("SELECT category_code FROM ad WHERE status=1 GROUP BY category_code LIMIT 1")
        cat = cur.fetchone()
        category = cat[0] if cat else None
    return region, age, gender, category

def explain(conn, sql):
    with conn.cursor() as cur:
        cur.execute("EXPLAIN " + sql)
        desc = [d[0] for d in cur.description]
        rows = cur.fetchall()
    print("  EXPLAIN:")
    for r in rows:
        d = dict(zip(desc, r))
        print(f"    table={str(d.get('table')):<8} type={str(d.get('type')):<6} "
              f"key={str(d.get('key')):<16} rows={str(d.get('rows')):<8} Extra={d.get('Extra')}")

def benchmark(conn, sql, rounds, warmup):
    # warmup：让 buffer pool 预热，避免冷启动抖动污染数字
    for _ in range(warmup):
        with conn.cursor() as cur:
            cur.execute(sql)
            cur.fetchall()
    times = []
    for _ in range(rounds):
        t0 = time.perf_counter()
        with conn.cursor() as cur:
            cur.execute(sql)
            cur.fetchall()
        times.append((time.perf_counter() - t0) * 1000.0)
    times.sort()
    avg = statistics.mean(times)
    p50 = times[len(times) // 2]
    p99 = times[min(len(times) - 1, int(len(times) * 0.99))]
    return avg, p50, p99

def index_exists(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM information_schema.STATISTICS "
            "WHERE TABLE_SCHEMA=%s AND TABLE_NAME='ad' AND INDEX_NAME=%s LIMIT 1",
            (DB, INDEX_NAME))
        return cur.fetchone() is not None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rounds", type=int, default=30, help="正式计时轮数")
    ap.add_argument("--warmup", type=int, default=5, help="预热轮数（不计入统计）")
    ap.add_argument("--keep-index", action="store_true", help="测完后保留索引（便于接着跑接口级压测）")
    ap.add_argument("--with-category", action="store_true", help="查询额外带 a.category_code 条件")
    args = ap.parse_args()

    try:
        conn = pymysql.connect(host=HOST, port=PORT, user=USER, password=PASSWORD, db=DB,
                               charset="utf8mb4", autocommit=True)
    except Exception as e:
        print(f"[ERROR] 无法连接 perf MySQL ({HOST}:{PORT}, db={DB}): {e}", file=sys.stderr)
        print("请确认 rtb-mysql-perf 容器已启动、端口 3306 可访问、库 rtb 已导入数据。", file=sys.stderr)
        sys.exit(1)

    region, age, gender, category = fetch_dim_values(conn)
    print(f"[参数] region={region} age={age} gender={gender} category={category} with_category={args.with_category}")
    sql = build_sql(region, age, gender, category, args.with_category)
    print(f"[SQL] {sql}\n")

    # ---------- 阶段1：baseline（确保无索引）----------
    print("=" * 64)
    print("阶段1 / baseline：无联合索引（M1 全表扫描 + filesort）")
    print("=" * 64)
    if index_exists(conn):
        with conn.cursor() as cur:
            cur.execute(DROP_INDEX)
        print(f"  已 DROP 旧索引 {INDEX_NAME}")
    explain(conn, sql)
    avg1, p50_1, p99_1 = benchmark(conn, sql, args.rounds, args.warmup)
    print(f"  耗时(ms): avg={avg1:.2f}  p50={p50_1:.2f}  p99={p99_1:.2f}\n")

    # ---------- 阶段2：建索引后 ----------
    print("=" * 64)
    print(f"阶段2 / 建联合索引：{CREATE_INDEX}")
    print("=" * 64)
    with conn.cursor() as cur:
        cur.execute(CREATE_INDEX)
    explain(conn, sql)
    avg2, p50_2, p99_2 = benchmark(conn, sql, args.rounds, args.warmup)
    print(f"  耗时(ms): avg={avg2:.2f}  p50={p50_2:.2f}  p99={p99_2:.2f}\n")

    # ---------- 对比 ----------
    print("=" * 64)
    print("对比结果")
    print("=" * 64)
    def pct(before, after):
        return (1 - after / before) * 100 if before else 0.0
    print(f"  平均耗时: {avg1:.2f} ms -> {avg2:.2f} ms  (下降 {pct(avg1, avg2):.1f}%)")
    print(f"  P50 耗时: {p50_1:.2f} ms -> {p50_2:.2f} ms  (下降 {pct(p50_1, p50_2):.1f}%)")
    print(f"  P99 耗时: {p99_1:.2f} ms -> {p99_2:.2f} ms  (下降 {pct(p99_1, p99_2):.1f}%)")
    print()
    print("  >>> 把上面的 avg / p99 前后数字填入简历「联合索引优化」段（SQL 反应速度）")

    # ---------- 收尾 ----------
    if args.keep_index:
        print(f"[收尾] 已保留索引 {INDEX_NAME}（可接着跑接口级压测；结束后用 `DROP INDEX {INDEX_NAME} ON ad;` 恢复）")
    else:
        with conn.cursor() as cur:
            cur.execute(DROP_INDEX)
        print(f"[收尾] 已 DROP 索引 {INDEX_NAME}，库恢复为 baseline 无索引状态（不影响其他对照）")

    conn.close()

if __name__ == "__main__":
    main()
