"""M5 超扣对账：逐广告核对 Redis 实际余额与 MySQL 计费事件。

⚠️ 必须传时间窗口（--since），否则对不上。

原因：billing_event 会累积一天里所有轮次压测的数据，而 Redis
一旦被 FLUSH 就只保留最后一轮。两者口径不一致会算出巨大差额，
看起来像超扣，实际是统计窗口错了。

口径
----
Redis 里每个 budget:{adId}:{yyyyMMdd} 的值是「剩余余额（分）」。
余额 = 初始日预算（分） - 实扣（分），所以

    实扣 = 初始预算 - 剩余余额

MySQL 侧 billing_event 记的是每一笔应收（pay_price，单位元）。
两边按 ad_id 汇总后比对，差额应为 0。

同时检查：
  · 余额为负的广告（超扣）
  · 逐广告不一致

用法
----
    # 先清 Redis、跑一轮压测，记下起始时刻，再执行
    python tools/reconcile_budget.py --since "2026-10-05 15:43:00"
"""
import argparse
import subprocess
import sys
from decimal import Decimal
from collections import defaultdict

MYSQL = ["docker", "exec", "rtb-mysql", "mysql", "-uroot", "rtb", "-N", "-B", "-e"]
REDIS = ["docker", "exec", "rtb-redis", "redis-cli"]


def sh(cmd):
    return subprocess.run(cmd, capture_output=True, text=True, check=True).stdout


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", required=True,
                    help="本轮压测的起始时刻，格式 'YYYY-MM-DD HH:MM:SS'")
    args = ap.parse_args()
    since = args.since

    # ---- 1. 从 Redis 读所有余额 ----
    keys = [k for k in sh(REDIS + ["--scan", "--pattern", "budget:*"]).split() if k]
    if not keys:
        print("FAIL: Redis 里没有预算键，无法对账")
        return 1

    balances = {}
    for i in range(0, len(keys), 500):
        chunk = keys[i:i + 500]
        args = REDIS + ["MGET"] + chunk
        vals = sh(args).splitlines()
        for k, v in zip(chunk, vals):
            ad_id = int(k.split(":")[1])
            balances[ad_id] = int(v) if v else None

    print(f"对账窗口：billed_at >= {since}")
    print(f"Redis 预算键：{len(balances)} 个")

    # ---- 2. 查这些广告的初始日预算 ----
    ids = sorted(balances.keys())
    rows = []
    for i in range(0, len(ids), 500):
        chunk = ",".join(str(x) for x in ids[i:i + 500])
        out = sh(MYSQL + [
            f"SELECT id, daily_budget FROM ad WHERE id IN ({chunk});"
        ])
        for line in out.strip().splitlines():
            if not line.strip():
                continue
            a, b = line.split("\t")
            rows.append((int(a), Decimal(b)))
    init = dict(rows)
    print(f"查到初始预算：{len(init)} 条")

    # ---- 3. 从 MySQL 汇总应收 ----
    should = defaultdict(Decimal)
    for i in range(0, len(ids), 500):
        chunk = ",".join(str(x) for x in ids[i:i + 500])
        out = sh(MYSQL + [
            f"SELECT ad_id, ROUND(SUM(pay_price),2) FROM billing_event "
            f"WHERE ad_id IN ({chunk}) AND billed_at >= '{since}' GROUP BY ad_id;"
        ])
        for line in out.strip().splitlines():
            if not line.strip():
                continue
            a, s = line.split("\t")
            should[int(a)] = Decimal(s)
    print(f"计费事件涉及：{len(should)} 条广告")

    # ---- 4. 逐广告比对 ----
    negative = 0
    mismatch = []
    total_should = Decimal("0")
    total_actual = Decimal("0")

    for ad_id, remain in balances.items():
        if remain is None:
            mismatch.append((ad_id, "余额为空", 0))
            continue
        if remain < 0:
            negative += 1
        base = init.get(ad_id)
        if base is None:
            mismatch.append((ad_id, "ad 表无此 id", remain))
            continue
        # 单位换算：预算表是元，Redis 是分
        actual = (base * 100 - remain) / 100
        expect = should.get(ad_id, Decimal("0"))
        total_should += expect
        total_actual += actual
        if abs(actual - expect) >= Decimal("0.01"):
            mismatch.append((ad_id, f"应扣 {expect} / 实扣 {actual}", remain))

    print()
    print("=" * 56)
    print(f"{'对账样本':<24}{len(balances):>10} 条广告")
    print(f"{'应扣合计（MySQL）':<24}{total_should:>22} 元")
    print(f"{'实扣合计（余额倒推）':<22}{total_actual:>22} 元")
    print(f"{'差额':<24}{total_actual - total_should:>22} 元")
    print(f"{'余额为负':<24}{negative:>22} 条")
    print(f"{'逐广告不一致':<22}{len(mismatch):>22} 条")
    print("=" * 56)

    if mismatch:
        print("\n不一致明细（前 20 条）：")
        for ad_id, why, remain in mismatch[:20]:
            print(f"  adId={ad_id} remain={remain} {why}")
        return 1

    if negative:
        return 1
    if abs(total_actual - total_should) >= Decimal("0.01"):
        return 1
    print("\n对账通过：差额 0.00 元，无超扣。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
