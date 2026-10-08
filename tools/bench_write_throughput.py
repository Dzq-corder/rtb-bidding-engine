#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
M7 · 竞价明细写吞吐基准（单条 INSERT vs 批量 INSERT）

目的
----
简历 bullet「竞价明细本地队列批量落库」要填两个 xxx：
    "写吞吐由 xxx 条/秒 提升至 xxx 条/秒（数据源开 rewriteBatchedStatements 走 JDBC 批量协议）"

口径（与用户确认）：对比「每来一条就单条写」与「攒批后批量写」的 条/秒 写吞吐。
- 单条模式 (baseline / M1)：每条记录一次 INSERT，autocommit（与 Spring jdbc.update 默认行为一致）。
- 批量模式 (optimized / M7)：每 500 条拼成一条多值 INSERT 一次发出。
  这正是 MySQL Connector/J 在 rewriteBatchedStatements=true 时做的事——
  把 N 次 addBatch 的 `INSERT ... VALUES (?)` 重写成一条 `INSERT ... VALUES (...),(...),...`。
  因此本脚本用 Python 手工拼多值 INSERT 来忠实复现该 JDBC 批量协议的网络/解析收益。

表：在 perf 库新建 bid_log_bench（与 bid_log 同构，含 PK + uk_request 唯一键，
     索引维护计入写入成本），不污染真实 bid_log，也不与运行中的应用竞争写入。

用法
----
  python tools/bench_write_throughput.py --rows 100000 --batch 500
"""
import argparse
import random
import sys
import time
from datetime import datetime

import pymysql

COLUMNS = [
    "request_id", "ad_id", "advertiser_id", "slot_code", "region", "age",
    "gender", "interests", "candidate_cnt", "match_cnt", "has_ad",
    "win_bid", "pay_price", "cost_ms", "candidate_cost", "filter_cost",
    "sort_cost", "bill_cost", "bid_at",
]
NCOL = len(COLUMNS)
PLACEHOLDER = "(" + ",".join(["%s"] * NCOL) + ")"
CREATE_SQL = """
CREATE TABLE IF NOT EXISTS bid_log_bench (
  id             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  request_id     CHAR(32)        NOT NULL,
  ad_id          BIGINT UNSIGNED          DEFAULT NULL,
  advertiser_id  INT UNSIGNED             DEFAULT NULL,
  slot_code      VARCHAR(32)     NOT NULL,
  region         VARCHAR(16)     NOT NULL,
  age            TINYINT         NOT NULL,
  gender         TINYINT         NOT NULL,
  interests      VARCHAR(255)    NOT NULL DEFAULT '',
  candidate_cnt  INT             NOT NULL,
  match_cnt      INT             NOT NULL,
  has_ad         TINYINT         NOT NULL,
  win_bid        DECIMAL(10,4)            DEFAULT NULL,
  pay_price      DECIMAL(10,4)            DEFAULT NULL,
  cost_ms        INT             NOT NULL,
  candidate_cost INT             NOT NULL,
  filter_cost    INT             NOT NULL,
  sort_cost      INT             NOT NULL,
  bill_cost      INT             NOT NULL,
  bid_at         DATETIME(3)     NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_request (request_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='bid_log 写吞吐基准表'
"""
INSERT_ONE = "INSERT INTO bid_log_bench (" + ",".join(COLUMNS) + ") VALUES " + PLACEHOLDER


def make_row(i: int):
    """生成一行与真实 bid_log 量级相近的随机数据。"""
    has_ad = 1 if (i % 10 != 0) else 0          # ~90% 有广告
    win = round(random.uniform(0.5, 9.9), 4) if has_ad else None
    pay = round(win * random.uniform(0.3, 0.9), 4) if has_ad else None
    return (
        f"{i:032d}",                              # request_id：定长 32，唯一
        random.randint(100000, 999999) if has_ad else None,
        random.randint(1, 5000) if has_ad else None,
        "feed_top",
        random.choice(["1101", "3101", "440101", "5101"]),
        random.randint(18, 60),
        random.randint(0, 2),
        "sports,tech",
        random.randint(100, 200),
        random.randint(10, 30),
        has_ad,
        win, pay,
        random.randint(200, 900),
        random.randint(50, 300),
        0, 0,
        random.randint(10, 80),
        datetime.now().strftime("%Y-%m-%d %H:%M:%S.") + "%03d" % (datetime.now().microsecond // 1000),
    )


def gen_rows(n: int):
    for i in range(n):
        yield make_row(i)


def run_single(conn, n: int):
    """baseline：每条一次 INSERT（autocommit，等同 M1 单条写）。"""
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE bid_log_bench")
    conn.commit()
    t0 = time.perf_counter()
    for row in gen_rows(n):
        cur.execute(INSERT_ONE, row)
    conn.commit()
    dt = time.perf_counter() - t0
    cur.close()
    return dt


def run_batch(conn, n: int, batch: int):
    """optimized：每 batch 条拼成一条多值 INSERT（等同 rewriteBatchedStatements 重写结果）。"""
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE bid_log_bench")
    conn.commit()
    rows = list(gen_rows(n))          # 一次性生成，排除生成耗时不计入写吞吐
    t0 = time.perf_counter()
    for i in range(0, n, batch):
        chunk = rows[i:i + batch]
        vals = ",".join(PLACEHOLDER for _ in chunk)
        sql = "INSERT INTO bid_log_bench (" + ",".join(COLUMNS) + ") VALUES " + vals
        flat = [v for r in chunk for v in r]
        cur.execute(sql, flat)
    conn.commit()
    dt = time.perf_counter() - t0
    cur.close()
    return dt


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=100000)
    ap.add_argument("--batch", type=int, default=500)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=3306)
    ap.add_argument("--user", default="root")
    ap.add_argument("--password", default="")
    ap.add_argument("--db", default="rtb")
    a = ap.parse_args()

    conn = pymysql.connect(host=a.host, port=a.port, user=a.user,
                           password=a.password, database=a.db, autocommit=True)
    conn.cursor().execute(CREATE_SQL)
    conn.commit()

    print(f"== 写吞吐基准：rows={a.rows} batch={a.batch} ==")
    print("  单条模式（baseline，每条一次 INSERT）...")
    t_single = run_single(conn, a.rows)
    r_single = a.rows / t_single
    print(f"    耗时 {t_single:.2f}s  吞吐 {r_single:,.0f} 条/秒")

    print("  批量模式（optimized，每 500 条一条多值 INSERT）...")
    t_batch = run_batch(conn, a.rows, a.batch)
    r_batch = a.rows / t_batch
    print(f"    耗时 {t_batch:.2f}s  吞吐 {r_batch:,.0f} 条/秒")

    factor = r_batch / r_single
    print("-" * 60)
    print(f"  结论：写吞吐 {r_single:,.0f} 条/秒 -> {r_batch:,.0f} 条/秒  "
          f"（约 {factor:.1f}x，单条 RT 受网络往返限制，批量合并后消除）")
    conn.close()


if __name__ == "__main__":
    main()
