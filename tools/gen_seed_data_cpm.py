#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RTB 投放引擎 · 真实计价版种子数据生成器（CPM 版）

与 gen_seed_data.py 的唯一区别在「出价」：
  原版：bid 直接是「单次展示出价（元）」，演示量级（中位数 2.2 元/次）。
  本版：bid 仍是「单次展示出价（元）」，但数值由真实 CPM 推导 ——
        CPM（每千次展示出价）先按对数正态长尾生成（中位数约 10 元/千次，
        落在真实展示广告 CPM 区间 2~80 元/千次），再除以 1000 得到单次价写入 bid。

为什么这样设计（面试要点）：
  * 真实展示广告按 CPM 计价，单次展示只有几分钱；原版「单次几元」是为看清二价
    效果刻意放大的教学尺度，不是真实行情。
  * 把 CPM 缩成单次价写入 bid，有以下好处：
      - ad 表列结构不变、导入脚本不变、应用代码不变（仍把 bid 当单次出价算二价）；
      - pay_price 自动变成几分钱/次，语义完全自洽；
      - 日预算档位 [100,500,2000,10000] 元/天 ÷ 单次 0.01 元 = 1万~100万次/天，
        比原版「日预算 100 元只够 45 次展示」更符合「日预算」的真实语义。

前置（重要，否则数据错误）：
  sql/schema.sql 里 ad.bid 的精度须为 DECIMAL(10,4)，
  否则 CPM=3 时单次 0.003 会被截断成 0.00，二价全算 0。
  （已在上游把 schema.sql 的 bid 由 DECIMAL(10,2) 改为 DECIMAL(10,4)）

其余分布（类目 Zipf / 预算离散档位 / 状态 80-15-5 / 定向 8~13 条）完全沿用原版，
保证「分布有依据」的原则不变，只是出价量级换成真实尺度。

用法：
  python tools/gen_seed_data_cpm.py --out data
  python tools/gen_seed_data_cpm.py --out data --ads 100000   # 小规模自测

注意：随机种子固定，保证每次生成的分布完全一致，面试时刷新页面数字才有可比性。
"""

import argparse
import csv
import math
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
#
# 【M1 调整记录】地域与兴趣的取值基数经历三轮调整
#
# 起点问题：候选集压不下来。地域单维平均命中 34,577 条、兴趣 98,613 条，
#   三维求交后还剩 1,094 条（目标 100~200）。根因是取值基数太小，不是 SQL 写法。
#
# 调整过程（每轮都实测验证，不靠估算）：
#   轮1  地域 28 / 兴趣 16  ->  加权平均候选 1,094
#   轮2  地域 403 / 兴趣 80 ->  加权平均候选 524
#   轮3  地域 4,748 / 兴趣 80 ->  预期加权平均约 50，落进目标区间
#
# 地域最终取 4,748 个：全国有 2,800 余个县级区划，加上地级市本级约 340 个，
# 再加上直辖市辖区约 1,600 个，合计约 4,700，与真实行政区划量级一致。
# 基数不够时再怎么调 SQL 都压不下来——根因在数据，不在查询。
#
# 地域编码规则：2 位省码 + 2 位市码 + 2 位区码，与真实行政区划编码规则一致。

def _build_regions():
    """构造约 4700 个地域（省-市-区三级），权重按城市能级递减。

    权重设计：直辖市 > 一线 > 新一线 > 二线 > 三线及区县。
    头部城市命中率高、尾部稀疏，用 Zipf 衰减模拟真实流量分布。
    """
    # (省码, 省份权重, 下辖市数量, 是否有下辖区县)
    # 直辖市只有市本级，没有下辖区县，用 has_county=False
    provinces = [
        ("11", 1.00, 16, False),  # 北京
        ("31", 0.98, 20, False),  # 上海
        ("44", 0.92, 21, True),   # 广东
        ("33", 0.86, 18, True),   # 浙江
        ("32", 0.85, 24, True),   # 江苏
        ("51", 0.78, 25, True),   # 四川
        ("42", 0.76, 21, True),   # 湖北
        ("61", 0.70, 19, True),   # 陕西
        ("37", 0.70, 22, True),   # 山东
        ("50", 0.62, 37, True),   # 重庆
        ("35", 0.62, 28, True),   # 福建
        ("43", 0.65, 28, True),   # 湖南
        ("41", 0.66, 29, True),   # 河南
        ("34", 0.60, 27, True),   # 安徽
        ("46", 0.60, 12, True),   # 海南
        ("36", 0.52, 25, True),   # 江西
        ("13", 0.55, 31, True),   # 河北
        ("14", 0.48, 25, True),   # 山西
        ("21", 0.52, 18, True),   # 辽宁
        ("22", 0.42, 20, True),   # 吉林
        ("23", 0.42, 21, True),   # 黑龙江
        ("62", 0.36, 30, True),   # 甘肃
        ("64", 0.32, 10, True),   # 宁夏
        ("65", 0.33, 20, True),   # 新疆
        ("53", 0.42, 35, True),   # 云南
        ("52", 0.40, 26, True),   # 贵州
        ("45", 0.45, 28, True),   # 广西
    ]

    regions = []
    for prov, prov_w, n_city, has_county in provinces:
        for c in range(1, n_city + 1):
            city_code = f"{prov}{c:02d}"
            # 省会/首府是第一个市，权重为省内的 2.4 倍，其余按衰减
            city_w = prov_w * (2.4 if c == 1 else 1.0 / (1.0 + c * 0.30))
            regions.append((city_code, city_w))
            if has_county:
                # 每个市下辖的区县数量：直辖市辖区多，人口大市区县多
                # 沿海经济大市下辖多，欠发达省份少
                n_county = 4 + (n_city - c) % 7
                for d in range(1, n_county + 1):
                    # 远郊区县权重低于市辖区
                    district_w = city_w * (1.0 if d == 1 else 0.55 / (1.0 + d * 0.22))
                    regions.append((f"{city_code}{d:02d}", district_w))
    return regions


REGIONS = _build_regions()

AGE_BUCKETS = [
    ("18-24", 0.16), ("25-30", 0.24), ("31-35", 0.22), ("36-40", 0.15),
    ("41-45", 0.10), ("46-50", 0.07), ("51+", 0.06),
]

GENDERS = [("male", 0.50), ("female", 0.48), ("unknown", 0.02)]

# 兴趣标签扩到 60 个，权重仍呈长尾：头部几个占大头，尾部很稀疏。
# 分组便于阅读：数码/生活/内容/运动/金融/出行/教育/其他
_INTEREST_GROUPS = {
    "数码": ["数码科技", "手机通讯", "电脑办公", "智能穿戴", "智能家居",
             "耳机音响", "相机摄影", "游戏设备", "汽车出行", "新能源车"],
    "生活": ["美妆护肤", "家居生活", "服饰鞋包", "母婴亲子", "宠物",
             "美食探店", "生鲜果蔬", "家居办公", "收纳整理", "床上用品"],
    "内容": ["阅读书单", "知识付费", "短视频", "网络小说", "音乐欣赏",
             "影视娱乐", "动漫二次元", "直播打赏", "兴趣社群", "本地生活"],
    "运动": ["运动健身", "户外露营", "骑行运动", "球类竞技", "健身私教",
             "瑜伽普拉提", "游泳潜水", "登山徒步", "极限运动", "康复理疗"],
    "金融": ["金融理财", "保险保障", "房产置业", "股票基金", "信贷分期",
             "税务筹划", "消费金融", "支付工具", "征信服务", "财富管理"],
    "出行": ["旅游出游", "酒店住宿", "机票航司", "火车票务", "自驾租车",
             "打车出行", "火车票优惠", "景区门票", "境外旅行", "邮轮假期"],
    "教育": ["学习教育", "少儿培训", "考研考公", "语言学习", "职业技能",
             "在线课程", "教育考试", "留学服务", "职业教育", "家庭教育"],
    "健康": ["健康医疗", "运动康复", "营养保健", "心理疏导", "睡眠管理",
             "体检服务", "口腔护理", "视力健康", "慢病管理", "中医养生"],
}
INTERESTS = []
for _grp, _items in _INTEREST_GROUPS.items():
    for _i, _it in enumerate(_items):
        # 每组内权重递减，组间按 1.0/0.85/0.7 ... 递减
        _grp_idx = list(_INTEREST_GROUPS.keys()).index(_grp)
        _w = (1.0 - _grp_idx * 0.11) * (1.0 if _i == 0 else 0.82)
        INTERESTS.append((_it, _w))

# 类目：词表 key 与 Zipf 权重对齐后合成一个表，供 weighted_pick 复用
CATEGORIES = list(zip(CATEGORY_PREFIX.keys(), CATEGORY_WEIGHTS))

OS_TYPES = [("android", 0.62), ("ios", 0.35), ("harmony", 0.03)]

DEVICE_TYPES = [("phone", 0.86), ("tablet", 0.09), ("other", 0.05)]

# 日预算离散档位（元/天）。真实计价下，单次 0.01 元 ÷ 预算 = 1万~100万次/天，语义合理。
BUDGET_TIERS = [100, 500, 2000, 10000]

SLOTS = [
    ("feed_top", 1.0),
    ("feed_mid", 1.0),
    ("feed_bottom", 0.8),
    ("detail_page", 0.6),
    ("search_page", 0.7),
]



# 权重抽样缓存。key 为词表名，value 为 (values, prefix_sums, total)
_WEIGHT_CACHE = {}


def weighted_pick(rng, pairs, cache_key):
    """按权重抽样。pairs = [(value, weight), ...]

    用「前缀和 + 二分查找」而不是线性扫描。
    地域扩到 4,748 个之后，线性版每次抽样要扫 4,748 项，
    50 万条广告 x 每条 3 个地域 = 70 亿次比较，耗时从 2 分钟涨到 10 分钟以上。
    前缀和只需构造一次，之后每次抽样 O(log n)。

    cache_key 传词表名字符串。用名字而非 id(pairs) 做 key，
    避免 Python 对象被回收后 id 复用导致缓存串味。
    """
    cached = _WEIGHT_CACHE.get(cache_key)
    if cached is None:
        values = [v for v, _ in pairs]
        prefix = []
        acc = 0.0
        for _, w in pairs:
            acc += w
            prefix.append(acc)
        cached = (values, prefix, acc)
        _WEIGHT_CACHE[cache_key] = cached

    values, prefix, total = cached
    import bisect
    return values[bisect.bisect_right(prefix, rng.random() * total)]



def math_lognormal(rng, mu, sigma):
    """标准正态 -> 对数正态。用 Box-Muller 变换，不依赖外部库。"""
    import math as _m
    u1 = rng.random()
    u2 = rng.random()
    # 避免 log(0)
    while u1 <= 1e-12:
        u1 = rng.random()
    z = _m.sqrt(-2.0 * _m.log(u1)) * _m.cos(2.0 * _m.pi * u2)
    return _m.exp(mu + sigma * z)


def gen_ads(count, out_dir):
    """生成广告与定向条件。

    出价（CPM 版）：先按对数正态生成「每千次展示出价 CPM（元）」，
    中位数约 10 元/千次（真实展示广告 CPM 的常见区间），长尾分布；
    再除以 1000 得到单次展示出价写入 bid 列（保留 4 位小数）。
    """
    rng = random.Random(SEED)
    ad_rows = []
    target_rows = []
    ad_id = 0

    # 定时更新时间戳，5 万条打印一次进度
    t0 = time.time()

    while ad_id < count:
        ad_id += 1

        cat = weighted_pick(rng, CATEGORIES, 'CATEGORIES')
        prefix = rng.choice(CATEGORY_PREFIX[cat])
        brand = rng.choice(BRANDS)
        product = rng.choice(PRODUCTS[cat])
        title = f"{prefix}{brand}{product}"

        advertiser_id = rng.randint(1, 20000)

        # ---- CPM 版出价：每千次展示出价（元），再换算成单次展示出价 ----
        # CPM 对数正态：mu=log(10) -> 中位数约 10 元/千次；sigma=0.60 长尾。
        # 真实展示广告 CPM 区间大致 2~80 元/千次，钳制在此范围内。
        cpm = math_lognormal(rng, mu=math.log(10.0), sigma=0.60)
        cpm = min(max(cpm, 2.0), 80.0)
        # 单次展示出价 = CPM / 1000，保留 4 位小数（schema 已改为 DECIMAL(10,4)）
        bid = round(cpm / 1000.0, 4)

        daily_budget = float(rng.choice(BUDGET_TIERS))

        # 80% 在投 / 15% 暂停 / 5% 已耗尽
        r = rng.random()
        status = 1 if r < 0.80 else (2 if r < 0.95 else 3)

        created_at = "2026-%02d-%02d %02d:%02d:%02d" % (
            rng.randint(1, 9), rng.randint(1, 28),
            rng.randint(0, 23), rng.randint(0, 59), rng.randint(0, 59))

        ad_rows.append([
            ad_id, advertiser_id, title, f"https://cdn.rtb.local/creative/{ad_id}.jpg",
            cat, f"{bid:.4f}", f"{daily_budget:.2f}", status, created_at, created_at,
        ])

        # ---- 定向条件：平均约 10 条，展开成倒排行 ----
        n_region = rng.randint(2, 4)
        n_age = rng.randint(1, 3)
        n_gender = rng.randint(1, 3)
        n_interest = rng.randint(3, 6)
        n_os = rng.randint(0, 2)
        n_device = rng.randint(0, 2)

        picked_regions = set()
        for _ in range(n_region):
            picked_regions.add(weighted_pick(rng, REGIONS, 'REGIONS'))
        for v in picked_regions:
            target_rows.append([ad_id, 1, v])

        for v in set(weighted_pick(rng, AGE_BUCKETS, 'AGE_BUCKETS') for _ in range(n_age)):
            target_rows.append([ad_id, 2, v])

        for v in set(weighted_pick(rng, GENDERS, 'GENDERS') for _ in range(n_gender)):
            target_rows.append([ad_id, 3, v])

        for v in set(weighted_pick(rng, INTERESTS, 'INTERESTS') for _ in range(n_interest)):
            target_rows.append([ad_id, 4, v])

        if n_os:
            target_rows.append([ad_id, 5, weighted_pick(rng, OS_TYPES, 'OS_TYPES')])
        if n_device:
            target_rows.append([ad_id, 6, weighted_pick(rng, DEVICE_TYPES, 'DEVICE_TYPES')])

        if ad_id % 50000 == 0:
            print(f"  已生成 {ad_id:,} 广告 / {len(target_rows):,} 定向行  "
                  f"({time.time() - t0:.0f}s)", flush=True)

    ad_path = os.path.join(out_dir, "ad.csv")
    target_path = os.path.join(out_dir, "ad_target.csv")

    with open(ad_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["id", "advertiser_id", "title", "creative_url",
                    "category_code", "bid", "daily_budget", "status",
                    "created_at", "updated_at"])
        w.writerows(ad_rows)

    with open(target_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["id", "ad_id", "dim_type", "dim_value"])
        # id 需要全局唯一，这里用 offset 保证
        w.writerows([[i + 1, r[0], r[1], r[2]] for i, r in enumerate(target_rows)])

    print(f"\n完成：")
    print(f"  广告     {len(ad_rows):>10,} 行 -> {ad_path}")
    print(f"  定向倒排 {len(target_rows):>10,} 行 -> {target_path}")
    print(f"  平均定向项/广告 = {len(target_rows) / len(ad_rows):.2f}")
    print(f"  耗时 {time.time() - t0:.0f}s")
    # 自检：打印出价区间，确认已是「几分钱/次」的真实尺度
    bids = [float(r[5]) for r in ad_rows]
    print(f"  单次展示出价 bid：min={min(bids):.4f}  max={max(bids):.4f}  "
          f"中位数≈{sorted(bids)[len(bids)//2]:.4f} 元/次 "
          f"（对应 CPM≈{sorted(bids)[len(bids)//2]*1000:.1f} 元/千次）")

    # ---- 自检：行尾必须是纯 \n，不能带 \r ----
    # 踩过的坑：csv.writer 默认写 \r\n，而导入端 LINES TERMINATED BY '\n'
    # 会把 \r 留在最后一个字段里，于是查 'male' 查不到、实际值是 'male\r'。
    # 表现为 GROUP BY 能看到该值、等值匹配却恒为 0，极难定位。
    for path, last_col in ((ad_path, "updated_at"), (target_path, "dim_value")):
        with open(path, "rb") as f:
            head = f.read(4096)
        if b"\r" in head:
            raise SystemExit(
                f"自检失败：{os.path.basename(path)} 含 CR 字符。"
                f"检查 csv.writer 是否指定了 lineterminator='\\n'"
            )
    print(f"  行尾自检通过（无 CR）")

    return ad_path, target_path


def gen_bid_requests(count, out_dir, seed=SEED + 1):
    """生成竞价请求样本（地域/年龄/兴趣按真实分布）。

    目标：让未过滤候选落在 100~200 条、过滤后落在 10~30 条。
    这个比例要等M1 跑通后按实际候选集大小调整分布参数，
    但请求本身的分布先按真实人口结构定好。
    """
    rng = random.Random(seed)
    path = os.path.join(out_dir, "bid_request_sample.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["slot_code", "region", "age", "gender", "interests"])
        for _ in range(count):
            slot = weighted_pick(rng, SLOTS, 'SLOTS')
            region = weighted_pick(rng, REGIONS, 'REGIONS')
            # 年龄正态分布，峰值 25~35
            age = min(70, max(18, int(rng.gauss(33, 9))))
            gender = weighted_pick(rng, GENDERS, 'GENDERS')
            n_int = rng.randint(3, 6)
            interests = ",".join(
                sorted({weighted_pick(rng, INTERESTS, 'INTERESTS') for _ in range(n_int)}))
            w.writerow([slot, region, age, gender, interests])
    print(f"  竞价请求样本 {count:,} 行 -> {path}")
    return path


def main():
    ap = argparse.ArgumentParser(description="RTB 投放引擎 · 真实计价版(CPM)种子数据生成器")
    ap.add_argument("--out", default="data", help="输出目录，默认 data/")
    ap.add_argument("--ads", type=int, default=500000, help="广告数量，默认 50 万")
    ap.add_argument("--requests", type=int, default=2000, help="竞价请求样本数")
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)

    print("=" * 60)
    print(f"RTB 真实计价版(CPM)种子数据生成器  seed={SEED}")
    print("=" * 60)
    print(f"\n[1/2] 生成广告与定向倒排（目标 {args.ads:,} 广告）")
    gen_ads(args.ads, args.out)

    print(f"\n[2/2] 生成竞价请求样本")
    gen_bid_requests(args.requests, args.out)

    print(f"\n下一步：")
    print(f"  bash tools/import_seed_data.sh    # 导入 MySQL（无需改动）")


if __name__ == "__main__":
    main()
