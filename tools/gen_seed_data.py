#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RTB 投放引擎 · M0 业务种子数据生成器

生成两类数据（对应《造数方案与数据来源说明》§2）：
  1. ad.csv        50 万广告
  2. ad_target.csv 约 500 万定向倒排行（由广告的定向条件反推展开，不是直接造）

核心原则：分布必须有依据，不能均匀随机。
  bid          对数正态长尾—— 真实出价是「大量低价 + 少量高价」
  category     Zipf 分布      —— 真实广告投放是集中的，前 5 个类目占约 60%
  daily_budget 离散档位       —— 真实广告主按档位设置
  status       80/15/5        —— 让检索阶段真的需要做状态过滤
  定向条数     8~13 条/广告   —— 平均约 10 条，50万 x 10 = 500万

为什么长尾重要：均匀分布会让最高价频繁换人，
二价拍卖的演示效果会失真，面试时讲不出「为什么二价让出价更真实」。

用法：
  python tools/gen_seed_data.py --out data/
  python tools/gen_seed_data.py --out data/ --ads 100000   # 小规模自测

注意：随机种子固定，保证每次生成的分布完全一致。
      面试时刷新页面数字才有可比性。
"""

import argparse
import csv
import os
import random
import time

# ---------------------------------------------------------------- 随机种子
# 固定不动。换种子 = 换数据集 = 之前测的指标全部作废。
SEED = 20260930

# ---------------------------------------------------------------- 词表
# 标题 = 行业前缀 x 品牌词 x 产品词。不用随机字符串。
BRANDS = [
    "星辰", "锐驰", "云麦", "沃野", "锦程", "拓远", "恒信", "灵犀",
    "极目", "青柠", "沐野", "弘毅", "简行", "乐言", "清和", "弘景",
    "问津", "白泽", "长风", "知行", "悦动", "谷雨", "霜降", "晚舟",
]

CATEGORY_PREFIX = {
    "mobile":     ["数码", "通信", "智能"],
    "fitness":    ["运动", "健身", "户外"],
    "home":       ["家居", "家纺", "收纳"],
    "beauty":     ["美妆", "护肤", "个护"],
    "education":  ["教育", "培训", "阅读"],
    "auto":       ["汽车", "车品", "出行"],
    "food":       ["食品", "生鲜", "零食"],
    "finance":    ["金融", "保险", "理财"],
    "game":       ["游戏", "电竞", "手游"],
    "travel":     ["旅游", "酒店", "机票"],
    "medical":    ["医疗", "健康", "保健"],
    "baby":       ["母婴", "童装", "玩具"],
    "pet":        ["宠物", "猫粮", "犬用"],
    "office":     ["办公", "文具", "耗材"],
    "hardware":   ["家居建材", "五金", "工具"],
    "sportswear": ["运动装", "球鞋", "潮服"],
    "digital":    ["数码配件", "耳机", "充电"],
    "kitchen":    ["厨具", "餐具", "锅具"],
    "garden":     ["园艺", "花艺", "绿植"],
    "camping":    ["露营", "户外装备", "帐篷"],
}

# 类目权重：Zipf 分布，前 5 个占约 60%
CATEGORY_WEIGHTS = [
    0.180, 0.130, 0.095, 0.075, 0.060,   # 前 5 = 54%
    0.048, 0.040, 0.034, 0.029, 0.025,   # 累计约 78%
    0.021, 0.018, 0.016, 0.014, 0.012,
    0.011, 0.010, 0.009, 0.008, 0.005,
]

PRODUCTS = {
    "mobile":     ["5G手机", "折叠屏", "智能手表", "耳机", "充电宝", "平板"],
    "fitness":    ["跑步机", "瑜伽垫", "哑铃", "筋膜枪", "运动服", "登山杖"],
    "home":       ["四件套", "记忆枕", "收纳箱", "台灯", "沙发垫", "晾衣架"],
    "beauty":     ["精华液", "面膜", "口红", "洁面", "防晒", "护发素"],
    "education":  ["少儿编程", "考研课", "英语课", "网课", "题库", "学习机"],
    "auto":       ["行车记录仪", "脚垫", "车充", "座套", "轮胎", "车载香薰"],
    "food":       ["坚果礼盒", "咖啡豆", "牛排", "坚果", "零食大包", "燕麦"],
    "finance":    ["重疾险", "意外险", "基金", "开户礼", "理财课", "记账本"],
    "game":       ["手游", "游戏本", "手柄", "点卡", "外设", "鼠标"],
    "travel":     ["机票", "酒店券", "签证", "旅行攻略", "行李箱", "门票"],
    "medical":    ["血压计", "血糖仪", "保健品", "口罩", "维生素", "体温计"],
    "baby":       ["奶粉", "纸尿裤", "婴儿车", "童装", "辅食", "玩具"],
    "pet":        ["猫粮", "狗粮", "猫砂", "宠物零食", "牵引绳", "猫窝"],
    "office":     ["打印机", "办公椅", "笔记本", "文件夹", "订书机", "白板"],
    "hardware":   ["电钻", "螺丝刀", "油漆", "水管", "锁具", "梯子"],
    "sportswear": ["跑鞋", "运动裤", "冲锋衣", "卫衣", "篮球", "背包"],
    "digital":    ["蓝牙耳机", "键盘", "显示器", "充电器", "数据线", "音箱"],
    "kitchen":    ["炒锅", "刀具", "砧板", "保鲜盒", "电饭煲", "空气炸锅"],
    "garden":     ["花盆", "营养土", "绿植", "花剪", "喷壶", "肥料"],
    "camping":    ["帐篷", "睡袋", "折叠椅", "头灯", "烤炉", "天幕"],
}

# ---------------------------------------------------------------- 定向值词表
# dim_type: 1地域 2年龄段 3性别 4兴趣 5操作系统 6设备类型
# 地域编码按真实人口权重：一线及新一线权重高
REGIONS = [
    ("110000", 0.050), ("310000", 0.048), ("440100", 0.045), ("440300", 0.040),
    ("330100", 0.036), ("320100", 0.035), ("510100", 0.033), ("420100", 0.032),
    ("610100", 0.028), ("370200", 0.026), ("230100", 0.022), ("500000", 0.021),
    ("420500", 0.020), ("370100", 0.019), ("220100", 0.018), ("350200", 0.018),
    ("120000", 0.017), ("320500", 0.017), ("210200", 0.015), ("340100", 0.014),
    ("650000", 0.012), ("430100", 0.011), ("360100", 0.010), ("140500", 0.008),
    ("530100", 0.007), ("460300", 0.007), ("410100", 0.006), ("620100", 0.005),
]

AGE_BUCKETS = [
    ("18-24", 0.16), ("25-30", 0.24), ("31-35", 0.22), ("36-40", 0.15),
    ("41-45", 0.10), ("46-50", 0.07), ("51+", 0.06),
]

GENDERS = [("male", 0.50), ("female", 0.48), ("unknown", 0.02)]

INTERESTS = [
    ("运动健身", 0.11), ("美妆护肤", 0.10), ("数码科技", 0.10), ("母婴亲子", 0.08),
    ("美食探店", 0.08), ("旅行出游", 0.07), ("游戏电竞", 0.07), ("家居生活", 0.07),
    ("服饰鞋包", 0.06), ("金融理财", 0.05), ("汽车出行", 0.05), ("学习教育", 0.05),
    ("宠物", 0.03), ("健康医疗", 0.03), ("家居办公", 0.03), ("其他", 0.02),
]

OS_TYPES = [("android", 0.62), ("ios", 0.35), ("harmony", 0.03)]

DEVICE_TYPES = [("phone", 0.86), ("tablet", 0.09), ("other", 0.05)]

# 日预算离散档位
BUDGET_TIERS = [100, 500, 2000, 10000]

SLOTS = [
    ("feed_top", 1.0),
    ("feed_mid", 1.0),
    ("feed_bottom", 0.8),
    ("detail_page", 0.6),
    ("search_page", 0.7),
]


def weighted_pick(rng, pairs):
    """按权重抽样。pairs = [(value, weight), ...]"""
    total = sum(w for _, w in pairs)
    r = rng.random() * total
    acc = 0.0
    for value, w in pairs:
        acc += w
        if r <= acc:
            return value
    return pairs[-1][0]


def gen_ads(count, out_dir):
    """生成广告与定向条件。

    出价用对数正态：mu=0.8 sigma=0.62 -> 中位数约 2.2 元，P99 约 12 元。
    刻意不用均匀分布。
    """
    rng = random.Random(SEED)
    ad_rows = []
    target_rows = []
    ad_id = 0

    # 定时更新时间戳，5 万条打印一次进度
    t0 = time.time()

    while ad_id < count:
        ad_id += 1

        cat = weighted_pick(rng, list(zip(CATEGORY_PREFIX.keys(), CATEGORY_WEIGHTS)))
        prefix = rng.choice(CATEGORY_PREFIX[cat])
        brand = rng.choice(BRANDS)
        product = rng.choice(PRODUCTS[cat])
        title = f"{prefix}{brand}{product}"

        advertiser_id = rng.randint(1, 20000)

        # 对数正态长尾出价
        bid = round(math_lognormal(rng, mu=0.80, sigma=0.62), 2)
        bid = min(max(bid, 0.50), 50.00)

        daily_budget = float(rng.choice(BUDGET_TIERS))

        # 80% 在投 / 15% 暂停 / 5% 已耗尽
        r = rng.random()
        status = 1 if r < 0.80 else (2 if r < 0.95 else 3)

        created_at = "2026-%02d-%02d %02d:%02d:%02d" % (
            rng.randint(1, 9), rng.randint(1, 28),
            rng.randint(0, 23), rng.randint(0, 59), rng.randint(0, 59))

        ad_rows.append([
            ad_id, advertiser_id, title, f"https://cdn.rtb.local/creative/{ad_id}.jpg",
            cat, f"{bid:.2f}", f"{daily_budget:.2f}", status, created_at, created_at,
        ])

        # ---- 定向条件：平均约 10 条，展开成倒排行
        # 数量选择依据：50万广告 x 平均10条 = 500万倒排行
        # （500万是倒排多条件求交与覆盖索引有实际体现的量级下限）
        n_region = rng.randint(1, 3)
        n_age = rng.randint(1, 3)
        n_gender = rng.randint(1, 3)
        n_interest = rng.randint(2, 5)
        n_os = rng.randint(0, 2)
        n_device = rng.randint(0, 2)

        picked_regions = set()
        for _ in range(n_region):
            picked_regions.add(weighted_pick(rng, REGIONS))
        for v in picked_regions:
            target_rows.append([ad_id, 1, v])

        for v in set(weighted_pick(rng, AGE_BUCKETS) for _ in range(n_age)):
            target_rows.append([ad_id, 2, v])

        for v in set(weighted_pick(rng, GENDERS) for _ in range(n_gender)):
            target_rows.append([ad_id, 3, v])

        for v in set(weighted_pick(rng, INTERESTS) for _ in range(n_interest)):
            target_rows.append([ad_id, 4, v])

        if n_os:
            target_rows.append([ad_id, 5, weighted_pick(rng, OS_TYPES)])
        if n_device:
            target_rows.append([ad_id, 6, weighted_pick(rng, DEVICE_TYPES)])

        if ad_id % 50000 == 0:
            print(f"  已生成 {ad_id:,} 广告 / {len(target_rows):,} 定向行  "
                  f"({time.time() - t0:.0f}s)", flush=True)

    ad_path = os.path.join(out_dir, "ad.csv")
    target_path = os.path.join(out_dir, "ad_target.csv")

    with open(ad_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id", "advertiser_id", "title", "creative_url",
                    "category_code", "bid", "daily_budget", "status",
                    "created_at", "updated_at"])
        w.writerows(ad_rows)

    with open(target_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id", "ad_id", "dim_type", "dim_value"])
        # id 需要全局唯一，这里用 offset 保证
        w.writerows([[i + 1, r[0], r[1], r[2]] for i, r in enumerate(target_rows)])

    print(f"\n完成：")
    print(f"  广告     {len(ad_rows):>10,} 行 -> {ad_path}")
    print(f"  定向倒排 {len(target_rows):>10,} 行 -> {target_path}")
    print(f"  平均定向项/广告 = {len(target_rows) / len(ad_rows):.2f}")
    print(f"  耗时 {time.time() - t0:.0f}s")

    return ad_path, target_path


def math_lognormal(rng, mu, sigma):
    """标准正态 -> 对数正态。用 Box-Muller 变换，不依赖外部库。"""
    import math
    u1 = rng.random()
    u2 = rng.random()
    # 避免 log(0)
    while u1 <= 1e-12:
        u1 = rng.random()
    z = math.sqrt(-2.0 * math.log(u1)) * math.cos(2.0 * math.pi * u2)
    return math.exp(mu + sigma * z)


def gen_bid_requests(count, out_dir, seed=SEED + 1):
    """生成竞价请求样本（地域/年龄/兴趣按真实分布）。

    目标：让未过滤候选落在 100~200 条、过滤后落在 10~30 条。
    这个比例要等M1 跑通后按实际候选集大小调整分布参数，
    但请求本身的分布先按真实人口结构定好。
    """
    rng = random.Random(seed)
    path = os.path.join(out_dir, "bid_request_sample.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["slot_code", "region", "age", "gender", "interests"])
        for _ in range(count):
            slot = weighted_pick(rng, SLOTS)
            region = weighted_pick(rng, REGIONS)[0]
            # 年龄正态分布，峰值 25~35
            age = min(70, max(18, int(rng.gauss(33, 9))))
            gender = weighted_pick(rng, GENDERS)
            n_int = rng.randint(2, 4)
            interests = ",".join(
                sorted({weighted_pick(rng, INTERESTS) for _ in range(n_int)}))
            w.writerow([slot, region, age, gender, interests])
    print(f"  竞价请求样本 {count:,} 行 -> {path}")
    return path


def main():
    ap = argparse.ArgumentParser(description="RTB 投放引擎 · M0 种子数据生成器")
    ap.add_argument("--out", default="data", help="输出目录，默认 data/")
    ap.add_argument("--ads", type=int, default=500000, help="广告数量，默认 50 万")
    ap.add_argument("--requests", type=int, default=2000, help="竞价请求样本数")
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)

    print("=" * 60)
    print(f"RTB 种子数据生成器  seed={SEED}")
    print("=" * 60)
    print(f"\n[1/2] 生成广告与定向倒排（目标 {args.ads:,} 广告）")
    gen_ads(args.ads, args.out)

    print(f"\n[2/2] 生成竞价请求样本")
    gen_bid_requests(args.requests, args.out)

    print(f"\n下一步：")
    print(f"  bash tools/import_seed_data.sh    # 导入 MySQL")


if __name__ == "__main__":
    main()
