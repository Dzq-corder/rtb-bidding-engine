#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RTB 实时竞价广告投放引擎 · 造数脚本（M1 基线数据）
=====================================================================
生成两张主链路表的数据文件：

    data/ad.csv          50 万行    广告主投放的广告
    data/ad_target.csv   ~480 万行  定向条件倒排
    data/dim_dict.csv    维度字典（码 -> 含义，仅供人查，不导入 MySQL）

---------------------------------------------------------------------
一、这个脚本要解决什么问题
---------------------------------------------------------------------
《数据库设计说明》§七 记录了一个待修项：旧造数脚本地域只有 28 个取值、
兴趣只有 16 个，维度基数太小，导致「地域 ∩ 年龄段 ∩ 性别」求交后
候选仍有约 1,094 条，达不到 100~200 条的目标。

候选集大小可以近似算出来：

    |候选| ≈ N_active × P(地域) × P(年龄段) × P(性别)

其中 N_active = 50 万 × 80%(status=1) = 40 万。
地域取值数为 R、每广告平均定向 k 个地域时，P(地域) ≈ k / R（近似）。
所以要把 N_active × 0.21 × 0.58 压到 150 条，需要：

    k / R ≈ 150 / (400000 × 0.21 × 0.58) ≈ 3.1e-3
    R ≈ k / 3.1e-3 ≈ 320 × k

地域若只有 337 个（地级市），k 必须压到 1 个才够——但真实广告主不会
只投 1 个市。所以唯一自洽的解法是**把地域下沉到区县级**（约 2,500 个
取值，这也是巨量引擎 / 腾讯广告真实支持的定向颗粒），此时 k = 6~8 个
区县（即「投一个市的核心城区」）就能自然落在目标区间。

因此本脚本默认 REGION_LEVEL = "district"（区县级）。
若想复现设计说明里「约 300」的地市级方案，用 --region-level city，
但自检会提示候选集偏大——这是数学必然，不是脚本 bug。

---------------------------------------------------------------------
二、分布是怎么定的（面试要能讲）
---------------------------------------------------------------------
ad 表
  · advertiser_id  1~20000，广告主拥有的广告条数按 Zipf 幂律（头部约 300 条，
                   长尾 1 条）——真实投放里大广告主贡献绝大部分广告
  · category_code  20 个类目，Zipf(α=1.0)，前 5 个约占 63%
  · bid            对数正态 lognormal(μ=ln2.7, σ=0.50)，叠加 3% 高价抢量广告，
                   截断到 [0.10, 50.00]。实测 P50≈2.7 / P95≈6.1 / max≈45
  · daily_budget   离散四档 100 / 500 / 2000 / 10000，权重 30/28/24/18
                   （想严格对齐设计说明§3.1 的四档各 25% → 改 BUDGET_TIERS）
  · status         1 在投 80% / 2 暂停 15% / 3 已耗尽 5%
  · title          「类目前缀 × 品牌词 × 产品词 × 版本后缀」，不用随机字符串
  · 硬约束         bid 必须 < daily_budget（设计说明实测 50 万条中违规 0 条）

ad_target 表（每广告平均约 9.6 个条件 → 约 480 万行）
  · dim=1 地域  区县码，按「中心市 + 同省若干市 + 全省」三种投放范围生成，
                地理上聚集（真实广告主不会随机撒几百个不相邻的区县）
  · dim=2 年龄段 7 桶，与 TargetingMapper.AGE_BUCKETS 严格一致
  · dim=3 性别   70% 单性别 / 20% 男女双选 / 10% 含 unknown 全选
  · dim=4 兴趣   60 个取值，Zipf(α=0.8)，每广告 1~10 个
  · dim=5 系统   45% 的广告定向，android 62% / ios 35% / harmony 3%
  · dim=6 设备   45% 的广告定向，phone 85% / tablet 12% / other 3%

---------------------------------------------------------------------
三、用法
---------------------------------------------------------------------
  python tools/gen_ad_data.py                      全量生成 + 自检
  python tools/gen_ad_data.py --quick              10 万条快速试参数（自检线性外推）
  python tools/gen_ad_data.py --region-level city  地域退到地市级（337 个）
  python tools/gen_ad_data.py --seed 2026          换随机种子（默认 42，保证可复现）
  python tools/gen_ad_data.py --out data           指定输出目录

导入 MySQL（先 docker cp 进容器，容器名按 docker-compose.yml）：
  docker cp data/ad.csv        rtb-mysql:/tmp/ad.csv
  docker cp data/ad_target.csv rtb-mysql:/tmp/ad_target.csv
  docker exec -i rtb-mysql mysql -uroot rtb --local-infile=1 -e \
    "LOAD DATA LOCAL INFILE '/tmp/ad.csv' INTO TABLE ad
     FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'
     (id,advertiser_id,title,creative_url,category_code,bid,daily_budget,status,created_at,updated_at)"
  docker exec -i rtb-mysql mysql -uroot rtb --local-infile=1 -e \
    "LOAD DATA LOCAL INFILE '/tmp/ad_target.csv' INTO TABLE ad_target
     FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' (ad_id,dim_type,dim_value)"
"""

import argparse
import bisect
import csv
import math
import os
import random
import sys
from collections import defaultdict
from datetime import datetime, timedelta

# =====================================================================
# 一、可调参数（全部集中在这里，改这里就能改数据形态）
# =====================================================================

AD_COUNT = 500_000          # 广告行数
ADVERTISER_COUNT = 20_000   # 广告主数

# --- 广告主规模分布 ---
ADVERTISER_ZIPF_ALPHA = 0.5     # 越大越集中；0.5 时头部约 300 条、长尾约 12 条
MAX_ADS_PER_ADVERTISER = 300    # 单个广告主条数上限（截断后余量重分配）

# --- 类目 ---
CATEGORY_ZIPF_ALPHA = 1.0       # 1.0 时前 5 个类目约占 63%

# --- 出价：对数正态 + 高价长尾 ---
# 对齐《数据库设计说明》§3.1 实测：中位数 2.7 / P95 6.1 / max 45.14
# 纯 lognormal 抬不出 45 这个量级（50 万样本的 max 只到 ~18），
# 所以叠 1% 的「高价抢量」广告（金融、医美、教育这类高客单价行业）。
# 注意：lognormal 右偏，mean 必然高于 median，本配置 mean≈3.0。
# 文档里的「平均 2.69」与「中位数 2.7 / P95 6.1」在数学上无法同时成立，
# 这里以两个分位数为准（分位数才是能被 SQL 复现的数字）。
BID_LOGN_MU = math.log(2.7)     # 中位数 = e^mu = 2.7
BID_LOGN_SIGMA = 0.465          # 使整体 P95 ≈ 6.1
BID_HIGH_RATIO = 0.01           # 1% 高价抢量广告，抬出 45 元量级的最大值
BID_HIGH_MU = math.log(7.85)
BID_HIGH_SIGMA = 0.50
BID_MIN, BID_MAX = 0.10, 50.00

# --- 日预算四档 ---
BUDGET_TIERS = [(100.0, 0.30), (500.0, 0.28), (2000.0, 0.24), (10000.0, 0.18)]

# --- 状态 ---
STATUS_WEIGHTS = [(1, 0.80), (2, 0.15), (3, 0.05)]

# --- 地域 ---
REGION_LEVEL = "district"       # district(约 2500 个区县，默认) | city(337 个地市)
# 投放范围：60% 只投中心市 / 30% 中心市 + 同省 1~3 个市 / 10% 全省
REGION_SCOPE_WEIGHTS = [(0, 0.60), (1, 0.30), (2, 0.10)]
# 地域条件数分布：(最少, 最多, 占比)
# 这个数直接决定候选集大小：|候选| ≈ N_active × (地域条数/地域取值数) × P(年龄) × P(性别)
# 取值 2684 个、本分布实测约 5 条/广告 → 候选 P50 落在 100~200。
# 想压总行数就往下调，代价是候选集变小；想让候选更集中就往上调。
REGION_CNT_DIST = [(1, 3, 0.55), (4, 8, 0.30), (9, 20, 0.12), (21, 50, 0.03)]

# --- 年龄段 ---
AGE_CNT_DIST = [(1, 1, 0.45), (2, 2, 0.35), (3, 4, 0.20)]

# --- 性别：选几个取值 ---
GENDER_CNT_DIST = [(1, 0.70), (2, 0.20), (3, 0.10)]

# --- 兴趣 ---
INTEREST_ZIPF_ALPHA = 0.8
INTEREST_CNT_DIST = [(1, 2, 0.45), (3, 5, 0.40), (6, 10, 0.15)]

# --- 系统 / 设备（这两个维度「不限」就不落行，故覆盖率 < 100%）---
OS_DIRECTED_RATIO = 0.52
OS_CNT_DIST = [(1, 1, 0.70), (2, 2, 0.30)]
DEVICE_DIRECTED_RATIO = 0.52
DEVICE_CNT_DIST = [(1, 1, 0.80), (2, 2, 0.20)]

# --- 自检 ---
SELF_TEST_SAMPLES = 3000        # 候选集模拟的请求数
QUICK_AD_COUNT = 100_000        # --quick 时的广告行数

# =====================================================================
# 二、词库与码表
# =====================================================================

# 20 个类目：(code, 中文前缀, 品牌词池, 产品词池)
CATEGORIES = [
    ("mobile",      "数码",   ["星辰", "极光", "云途", "磐石", "雷泽"],
                             ["5G手机", "蓝牙耳机", "智能手表", "平板电脑", "快充套装"]),
    ("computer",    "办公",   ["智联", "天枢", "弘毅", "飞鱼", "青云"],
                             ["轻薄本", "机械键盘", "显示器", "固态硬盘", "扩展坞"]),
    ("beauty",      "美妆",   ["花漾", "沁颜", "素心", "澜庭", "初见"],
                             ["精华液", "保湿面膜", "口红", "防晒霜", "洁面乳"]),
    ("apparel",     "服饰",   ["锦时", "南风", "织羽", "沐野", "一格"],
                             ["风衣", "针织衫", "牛仔裤", "连衣裙", "卫衣"]),
    ("food",        "食品",   ["禾谷", "鲜物语", "田原", "满仓", "拾味"],
                             ["坚果礼盒", "低温酸奶", "手工挂面", "冻干水果", "黑咖啡"]),
    ("home",        "家居",   ["宜然", "木栖", "安居", "格物", "朴屋"],
                             ["实木餐桌", "记忆棉床垫", "收纳柜", "落地灯", "四件套"]),
    ("maternal",    "母婴",   ["安贝", "亲亲", "柔恩", "童行", "萌芽"],
                             ["纸尿裤", "婴儿推车", "辅食机", "奶瓶", "爬行垫"]),
    ("education",   "教育",   ["博学", "知远", "启航", "明德", "优学"],
                             ["英语课程", "编程训练营", "考研辅导", "职业证书", "少儿美术"]),
    ("finance",     "财经",   ["汇通", "金策", "盈丰", "恒信", "智投"],
                             ["理财课程", "记账会员", "信用卡", "基金定投", "财报工具"]),
    ("insurance",   "保险",   ["安泰", "守护", "康宁", "众安行", "馨安"],
                             ["医疗险", "重疾险", "车险", "意外险", "年金险"]),
    ("travel",      "旅游",   ["途野", "远行", "云游", "漫迹", "行知"],
                             ["机票套餐", "跟团游", "民宿券", "景区门票", "签证服务"]),
    ("auto",        "汽车",   ["驭风", "驰远", "路擎", "星轨", "越程"],
                             ["新能源车", "保养套餐", "车险报价", "轮胎", "车载支架"]),
    ("game",        "游戏",   ["幻域", "燃光", "天启", "苍穹", "星火"],
                             ["手游礼包", "游戏手柄", "电竞椅", "点卡充值", "联名周边"]),
    ("medical",     "医疗",   ["康和", "济世", "仁术", "安欣", "本草"],
                             ["体检套餐", "口腔护理", "在线问诊", "护腰仪", "血压计"]),
    ("fitness",     "运动",   ["劲能", "跃动", "型格", "疾风", "铁馆"],
                             ["跑步机", "瑜伽垫", "蛋白粉", "哑铃套装", "骑行装备"]),
    ("pet",         "宠物",   ["萌宠", "毛球", "乐它", "陪伴", "爪印"],
                             ["猫粮", "狗零食", "猫砂盆", "宠物牵引绳", "智能喂食器"]),
    ("realestate",  "房产",   ["安家", "筑巢", "优居", "恒产", "栖地"],
                             ["新房带看", "租房平台", "装修报价", "二手房", "楼盘团购"]),
    ("recruit",     "招聘",   ["伯乐", "职达", "猎英", "才汇", "直聘"],
                             ["简历优化", "猎头服务", "校招专区", "技能培训", "岗位订阅"]),
    ("industrial",  "工业",   ["重工", "精工", "鼎力", "锐进", "铸远"],
                             ["数控刀具", "工业传感器", "变频器", "轴承", "气动元件"]),
    ("agriculture", "农业",   ["丰年", "沃土", "耕读", "绿源", "穗丰"],
                             ["复合肥", "种子套装", "植保无人机", "灌溉设备", "饲料添加剂"]),
]

TITLE_SUFFIX = ["", "Pro", "Max", "青春版", "旗舰版", "2026款", "限量版", "Plus"]

# 60 个兴趣标签
INTERESTS = [
    "数码科技", "手机通讯", "电脑办公", "摄影摄像", "游戏电竞", "动漫二次元",
    "影视娱乐", "音乐音频", "阅读学习", "教育培训", "考试升学", "职场技能",
    "财经投资", "股票基金", "保险理财", "房产家居", "装修设计", "家用电器",
    "美妆护肤", "香水彩妆", "个人护理", "服饰穿搭", "鞋包配饰", "奢侈品",
    "运动健身", "户外徒步", "减肥塑形", "瑜伽舞蹈", "母婴育儿", "孕产护理",
    "儿童教育", "宠物饲养", "美食烹饪", "餐厅探店", "烘焙甜品", "咖啡茶饮",
    "旅游出行", "酒店民宿", "自驾游", "汽车资讯", "购车用车", "新能源汽车",
    "摩托车", "医疗健康", "中医养生", "心理健康", "法律服务", "婚恋交友",
    "社交娱乐", "直播购物", "电商优惠", "二手交易", "家居收纳", "园艺种植",
    "手工DIY", "公益环保", "时事新闻", "军事历史",
    "工业制造", "物流运输",
]

# 7 个年龄段：必须与 TargetingMapper.AGE_BUCKETS 完全一致
AGE_BUCKETS = ["18-24", "25-30", "31-35", "36-40", "41-45", "46-50", "51+"]
# 广告主对各年龄段的偏好权重（25~35 最抢手）
AGE_AD_WEIGHTS = [0.13, 0.20, 0.19, 0.15, 0.13, 0.10, 0.10]
# 请求侧人口分布（自检抽样用）
AGE_REQ_WEIGHTS = [0.15, 0.18, 0.17, 0.15, 0.13, 0.11, 0.11]

GENDERS = ["male", "female", "unknown"]
GENDER_AD_WEIGHTS = [0.48, 0.48, 0.04]     # 广告主定向偏好
GENDER_REQ_WEIGHTS = [0.49, 0.49, 0.02]    # 请求侧分布

OS_VALUES = ["android", "ios", "harmony"]
OS_WEIGHTS = [0.62, 0.35, 0.03]
DEVICE_VALUES = ["phone", "tablet", "other"]
DEVICE_WEIGHTS = [0.85, 0.12, 0.03]

# 31 个省级行政区：(省码前两位, 省名, 地市数量)
PROVINCES = [
    ("11", "北京", 1), ("12", "天津", 1), ("13", "河北", 11), ("14", "山西", 11),
    ("15", "内蒙古", 12), ("21", "辽宁", 14), ("22", "吉林", 9), ("23", "黑龙江", 13),
    ("31", "上海", 1), ("32", "江苏", 13), ("33", "浙江", 11), ("34", "安徽", 16),
    ("35", "福建", 9), ("36", "江西", 11), ("37", "山东", 16), ("41", "河南", 17),
    ("42", "湖北", 13), ("43", "湖南", 14), ("44", "广东", 21), ("45", "广西", 14),
    ("46", "海南", 4), ("50", "重庆", 1), ("51", "四川", 21), ("52", "贵州", 9),
    ("53", "云南", 16), ("54", "西藏", 7), ("61", "陕西", 10), ("62", "甘肃", 14),
    ("63", "青海", 8), ("64", "宁夏", 5), ("65", "新疆", 14),
]

# 城市层级权重：一线 > 新一线 > 二线 > 三线 > 其他
TIER_WEIGHTS = {"t1": 40.0, "t1b": 14.0, "t2": 7.0, "t3": 3.5, "t4": 1.0}
# 各层级的区县数量
TIER_DISTRICT_CNT = {"t1": 20, "t1b": 14, "t2": 11, "t3": 8, "t4": 6}

# 热门城市真实名（市码 -> 名称），其余按「省名+序号」程序化命名
CITY_NAME_OVERRIDE = {
    "1101": "北京市", "1201": "天津市", "3101": "上海市", "5001": "重庆市",
    "4401": "广州市", "4403": "深圳市", "5101": "成都市", "3301": "杭州市",
    "6101": "西安市", "3205": "苏州市", "4201": "武汉市", "3201": "南京市",
    "4101": "郑州市", "4301": "长沙市", "4419": "东莞市", "3702": "青岛市",
    "2101": "沈阳市", "3302": "宁波市", "5301": "昆明市", "3502": "厦门市",
    "4406": "佛山市", "3401": "合肥市", "2102": "大连市", "3701": "济南市",
    "1301": "石家庄市", "3601": "南昌市", "5201": "贵阳市", "4501": "南宁市",
    "6201": "兰州市", "1501": "呼和浩特市", "6501": "乌鲁木齐市", "2201": "长春市",
    "2301": "哈尔滨市", "1401": "太原市", "3202": "无锡市", "3303": "温州市",
    "3501": "福州市", "3706": "烟台市", "4103": "洛阳市", "4601": "海口市",
}
# 强制指定层级的市码（不按省内地市序号推断）
CITY_TIER_OVERRIDE = {
    "1101": "t1", "3101": "t1", "4401": "t1", "4403": "t1",   # 北上广深
    "1201": "t1b", "5001": "t1b",                              # 天津、重庆
}
# 省会 / 首府所在市（按省码前两位 -> 市序号），用于层级推断
PROVINCIAL_CAPITAL_IDX = 1   # 各省 01 号市即省会

# =====================================================================
# 三、工具函数
# =====================================================================


def make_picker(values, weights):
    """构造 O(log n) 加权抽样器：预计算累积权重 + 二分查找。"""
    cum = []
    acc = 0.0
    for w in weights:
        acc += w
        cum.append(acc)
    total = acc
    n = len(values)
    values = list(values)

    def pick(rnd):
        idx = bisect.bisect_left(cum, rnd.random() * total)
        return values[idx if idx < n else n - 1]

    return pick


def sample_unique(picker, k, rnd, pool_size=None):
    """
    按权重不重复抽 k 个。
    抽不满（k 太接近池容量导致拒绝采样退化）时返回 None，由调用方回退到均匀抽样。
    """
    if k <= 0:
        return []
    if pool_size is not None and k >= pool_size:
        return None
    out, seen = [], set()
    guard = 0
    limit = k * 30 + 30
    while len(out) < k and guard < limit:
        v = picker(rnd)
        if v not in seen:
            seen.add(v)
            out.append(v)
        guard += 1
    return out if len(out) == k else None


def pct(sorted_vals, p):
    """从已排序序列取分位数。"""
    if not sorted_vals:
        return 0.0
    idx = int(len(sorted_vals) * p)
    idx = min(idx, len(sorted_vals) - 1)
    return sorted_vals[idx]


# =====================================================================
# 四、地域码表构建
# =====================================================================


def build_geo(level):
    """
    构造省 -> 市 -> 区县的三级码表。

    返回：
      cities          市码(4位) 列表，如 '1101'
      city_name       市码 -> 名称
      city_tier       市码 -> 层级
      city_province   市码 -> 省码(2位)
      city_districts  市码 -> 区县码(6位) 列表
      province_cities 省码(2位) -> 市码列表
      districts       全部区县码（level='district' 时的地域取值）
    """
    cities, city_name, city_tier = [], {}, {}
    city_province, city_districts = {}, {}
    province_cities = defaultdict(list)

    for pcode, pname, pcity_cnt in PROVINCES:
        for i in range(1, pcity_cnt + 1):
            ccode = "%s%02d" % (pcode, i)
            cities.append(ccode)
            city_province[ccode] = pcode
            province_cities[pcode].append(ccode)

            if ccode in CITY_NAME_OVERRIDE:
                city_name[ccode] = CITY_NAME_OVERRIDE[ccode]
            else:
                city_name[ccode] = "%s%02d市" % (pname, i)

            if ccode in CITY_TIER_OVERRIDE:
                tier = CITY_TIER_OVERRIDE[ccode]
            elif i == PROVINCIAL_CAPITAL_IDX:
                # 省会：经济强省的省会进新一线，其余进二线
                tier = "t1b" if pcode in ("32", "33", "44", "37", "51", "42",
                                          "43", "41", "61", "35", "34", "21",
                                          "36", "13", "50") else "t2"
            elif i <= 3:
                tier = "t2"
            elif i <= 6:
                tier = "t3"
            else:
                tier = "t4"
            city_tier[ccode] = tier

    # 生成区县码：市码 + 2 位序号
    districts = []
    for ccode in cities:
        cnt = TIER_DISTRICT_CNT[city_tier[ccode]]
        lst = []
        for j in range(1, cnt + 1):
            dcode = "%s%02d" % (ccode, j)   # 市码4位 + 2位 = 6位
            lst.append(dcode)
            districts.append(dcode)
        city_districts[ccode] = lst

    if level == "city":
        # 地市级方案：取值就是 337 个市码（后补 00 成 6 位，与区县码同构）
        values = [c + "00" for c in cities]
        value_of_city = {c: [c + "00"] for c in cities}
    else:
        values = districts
        value_of_city = city_districts

    # 每个地域取值的权重（用于抽样）：市层级权重 ÷ 该市取值数，使市内均匀、市间按层级
    weights = []
    for c in cities:
        w = TIER_WEIGHTS[city_tier[c]] / len(value_of_city[c])
        for _ in value_of_city[c]:
            weights.append(w)

    # 预计算「省 -> 全部区县」，供 10% 的全省投放直接用，避免每次重新拼接
    province_districts = {}
    for p, clist in province_cities.items():
        pool = []
        for c in clist:
            pool.extend(city_districts[c])
        province_districts[p] = pool

    return {
        "cities": cities,
        "city_name": city_name,
        "city_tier": city_tier,
        "city_province": city_province,
        "city_districts": city_districts,
        "province_cities": dict(province_cities),
        "province_districts": province_districts,
        "values": values,
        "weights": weights,
        "value_of_city": value_of_city,
    }


# =====================================================================
# 五、ad 表生成
# =====================================================================


def gen_ads(rnd, n, now):
    """生成 n 条广告，返回 list[dict]。"""
    cat_codes = [c[0] for c in CATEGORIES]
    cat_w = [1.0 / ((i + 1) ** CATEGORY_ZIPF_ALPHA) for i in range(len(cat_codes))]
    pick_cat = make_picker(cat_codes, cat_w)

    # 广告主规模：Zipf 幂律 + 上限截断 + 余量重分配
    adv_w = [1.0 / ((i + 1) ** ADVERTISER_ZIPF_ALPHA) for i in range(ADVERTISER_COUNT)]
    total_w = sum(adv_w)
    quota = [int(n * w / total_w) for w in adv_w]
    quota = [min(q, MAX_ADS_PER_ADVERTISER) for q in quota]
    deficit = n - sum(quota)
    # 余量按权重从小到大补（补在长尾，不破坏头部形态）
    order = sorted(range(ADVERTISER_COUNT), key=lambda i: adv_w[i])
    i = 0
    while deficit > 0:
        idx = order[i % ADVERTISER_COUNT]
        room = MAX_ADS_PER_ADVERTISER - quota[idx]
        if room > 0:
            add = min(room, deficit)
            quota[idx] += add
            deficit -= add
        i += 1
        if i > ADVERTISER_COUNT * 20:
            break
    # 展开成广告主 ID 序列后打乱，避免同广告主的行在文件里连续
    advertiser_ids = []
    for aid, q in enumerate(quota, start=1):
        advertiser_ids.extend([aid] * q)
    if len(advertiser_ids) < n:
        # 兜底：幂律配额理论上一定够，这里只在改坏参数时防止越界
        advertiser_ids.extend(rnd.choices(range(1, ADVERTISER_COUNT + 1),
                                          k=n - len(advertiser_ids)))
    advertiser_ids = advertiser_ids[:n]
    rnd.shuffle(advertiser_ids)

    pick_budget = make_picker([t[0] for t in BUDGET_TIERS], [t[1] for t in BUDGET_TIERS])
    pick_status = make_picker([s[0] for s in STATUS_WEIGHTS], [s[1] for s in STATUS_WEIGHTS])
    pick_suffix = make_picker(TITLE_SUFFIX, [1.0] * len(TITLE_SUFFIX))

    cat_map = {c[0]: c for c in CATEGORIES}
    span_days = 180

    ads = []
    for i in range(n):
        code = pick_cat(rnd)
        _, prefix, brands, products = cat_map[code]
        title = prefix + rnd.choice(brands) + rnd.choice(products) + pick_suffix(rnd)

        budget = pick_budget(rnd)
        # 出价：97% 主分布 + 3% 高价抢量
        if rnd.random() < BID_HIGH_RATIO:
            bid = rnd.lognormvariate(BID_HIGH_MU, BID_HIGH_SIGMA)
        else:
            bid = rnd.lognormvariate(BID_LOGN_MU, BID_LOGN_SIGMA)
        bid = round(min(max(bid, BID_MIN), BID_MAX), 2)
        # 硬约束：单次出价必须小于日预算，否则这条广告一竞就破产
        if bid >= budget:
            bid = round(budget * rnd.uniform(0.05, 0.5), 2)
            bid = max(bid, BID_MIN)

        created = now - timedelta(days=rnd.randint(0, span_days),
                                  hours=rnd.randint(0, 23),
                                  minutes=rnd.randint(0, 59))
        updated = created + timedelta(days=rnd.randint(0, 30))
        if updated > now:
            updated = now

        ads.append({
            "id": i + 1,
            "advertiser_id": advertiser_ids[i],
            "title": title,
            "creative_url": "https://cdn.rtb-demo.com/creative/%s/%d/%d.jpg"
                            % (code, (i + 1) % 1000, i + 1),
            "category_code": code,
            "bid": bid,
            "daily_budget": budget,
            "status": pick_status(rnd),
            "created_at": created,
            "updated_at": updated,
        })
    return ads


def write_ads(path, ads):
    with open(path, "w", encoding="utf-8", newline="") as f:
        buf = []
        for a in ads:
            buf.append("%d,%d,%s,%s,%s,%.2f,%.2f,%d,%s,%s\n" % (
                a["id"], a["advertiser_id"], a["title"], a["creative_url"],
                a["category_code"], a["bid"], a["daily_budget"], a["status"],
                a["created_at"].strftime("%Y-%m-%d %H:%M:%S"),
                a["updated_at"].strftime("%Y-%m-%d %H:%M:%S")))
            if len(buf) >= 50_000:
                f.write("".join(buf))
                buf.clear()
        if buf:
            f.write("".join(buf))


# =====================================================================
# 六、ad_target 倒排生成（同时建内存倒排供自检）
# =====================================================================


def gen_targets(path, ads, geo, rnd):
    """
    生成定向倒排。返回 (总行数, 各维度行数, 倒排索引 dict)。

    倒排索引只收录 status=1 的广告，且只收 dim 1/2/3 —— 与
    AdMapper.findCandidates 实际 JOIN 的三个维度一致，自检时用得上。
    """
    pick_age = make_picker(AGE_BUCKETS, AGE_AD_WEIGHTS)
    pick_gender = make_picker(GENDERS, GENDER_AD_WEIGHTS)
    int_w = [1.0 / ((i + 1) ** INTEREST_ZIPF_ALPHA) for i in range(len(INTERESTS))]
    pick_interest = make_picker(INTERESTS, int_w)
    pick_os = make_picker(OS_VALUES, OS_WEIGHTS)
    pick_device = make_picker(DEVICE_VALUES, DEVICE_WEIGHTS)

    # 所有「抽几个条件」的抽样器都要预建，绝不能放进 50 万次循环里重建累积数组
    pick_scope = make_picker([s for s, _ in REGION_SCOPE_WEIGHTS],
                             [w for _, w in REGION_SCOPE_WEIGHTS])
    pick_reg_cnt = make_picker([(a, b) for a, b, _ in REGION_CNT_DIST],
                               [c for _, _, c in REGION_CNT_DIST])
    pick_age_cnt = make_picker([(a, b) for a, b, _ in AGE_CNT_DIST],
                               [c for _, _, c in AGE_CNT_DIST])
    pick_gender_cnt = make_picker([c for c, _ in GENDER_CNT_DIST],
                                  [w for _, w in GENDER_CNT_DIST])
    pick_int_cnt = make_picker([(a, b) for a, b, _ in INTEREST_CNT_DIST],
                               [c for _, _, c in INTEREST_CNT_DIST])

    # 地域：中心市按城市层级加权抽（一线市被广告主选中的概率远高于四线市），
    # 区县在「已选中的投放范围」内均匀抽 —— 范围本身已带层级权重，市内再加权意义不大。
    city_w = [TIER_WEIGHTS[geo["city_tier"][c]] for c in geo["cities"]]
    pick_city = make_picker(geo["cities"], city_w)
    province_codes = sorted(geo["province_cities"].keys())
    prov_w = []
    for p in province_codes:
        prov_w.append(sum(TIER_WEIGHTS[geo["city_tier"][c]]
                          for c in geo["province_cities"][p]))
    pick_province = make_picker(province_codes, prov_w)

    dim_cnt = defaultdict(int)
    inv = {1: defaultdict(list), 2: defaultdict(list), 3: defaultdict(list)}
    total = 0

    city_districts = geo["city_districts"]
    province_cities = geo["province_cities"]
    province_districts = geo["province_districts"]

    with open(path, "w", encoding="utf-8", newline="") as f:
        buf = []
        for a in ads:
            ad_id = a["id"]
            active = (a["status"] == 1)
            rows = []

            # ---- dim=1 地域 ----
            # 先定「投放范围」（中心市 / 中心市+同省 / 全省），再在范围内抽 k 个区县，
            # 这样同一条广告的地域在地理上是聚集的，不会随机撒几百个不相邻的区县。
            scope = pick_scope(rnd)
            if scope == 0:                      # 60%：只投中心市
                pool = city_districts[pick_city(rnd)]
            elif scope == 1:                    # 30%：中心市 + 同省 1~3 个市
                c = pick_city(rnd)
                p = geo["city_province"][c]
                pool = list(city_districts[c])
                others = [x for x in province_cities[p] if x != c]
                for o in rnd.sample(others, min(rnd.randint(1, 3), len(others))):
                    pool.extend(city_districts[o])
            else:                               # 10%：投全省
                pool = province_districts[pick_province(rnd)]
            lo, hi = pick_reg_cnt(rnd)
            k = min(rnd.randint(lo, hi), len(pool))
            for v in rnd.sample(pool, k):
                rows.append((1, v))
                if active:
                    inv[1][v].append(ad_id)

            # ---- dim=2 年龄段 ----
            lo, hi = pick_age_cnt(rnd)
            k = min(rnd.randint(lo, hi), len(AGE_BUCKETS))
            vals = sample_unique(pick_age, k, rnd, len(AGE_BUCKETS))
            if vals is None:
                vals = rnd.sample(AGE_BUCKETS, k)
            for v in vals:
                rows.append((2, v))
                if active:
                    inv[2][v].append(ad_id)

            # ---- dim=3 性别 ----
            k = min(pick_gender_cnt(rnd), len(GENDERS))
            if k >= len(GENDERS):
                vals = list(GENDERS)
            else:
                vals = sample_unique(pick_gender, k, rnd, len(GENDERS))
                if vals is None:
                    vals = rnd.sample(GENDERS, k)
            for v in vals:
                rows.append((3, v))
                if active:
                    inv[3][v].append(ad_id)

            # ---- dim=4 兴趣 ----
            lo, hi = pick_int_cnt(rnd)
            k = min(rnd.randint(lo, hi), len(INTERESTS))
            vals = sample_unique(pick_interest, k, rnd, len(INTERESTS))
            if vals is None:
                vals = rnd.sample(INTERESTS, k)
            for v in vals:
                rows.append((4, v))

            # ---- dim=5 系统（45% 的广告定向，其余「不限」不落行）----
            if rnd.random() < OS_DIRECTED_RATIO:
                k = 1 if rnd.random() < 0.70 else 2
                vals = sample_unique(pick_os, k, rnd, len(OS_VALUES))
                if vals is None:
                    vals = rnd.sample(OS_VALUES, k)
                for v in vals:
                    rows.append((5, v))

            # ---- dim=6 设备（45% 的广告定向）----
            if rnd.random() < DEVICE_DIRECTED_RATIO:
                k = 1 if rnd.random() < 0.80 else 2
                vals = sample_unique(pick_device, k, rnd, len(DEVICE_VALUES))
                if vals is None:
                    vals = rnd.sample(DEVICE_VALUES, k)
                for v in vals:
                    rows.append((6, v))

            for dt, dv in rows:
                buf.append("%d,%d,%s\n" % (ad_id, dt, dv))
                dim_cnt[dt] += 1
            total += len(rows)

            if len(buf) >= 100_000:
                f.write("".join(buf))
                buf.clear()
        if buf:
            f.write("".join(buf))

    return total, dim_cnt, inv


# =====================================================================
# 七、自检
# =====================================================================


def self_test(ads, geo, total_rows, dim_cnt, inv, rnd, scale, quick):
    line = "=" * 72
    print("\n" + line)
    print("  ad 表分布自检")
    print(line)
    n = len(ads)
    print("  总行数            %d" % n)

    st = defaultdict(int)
    for a in ads:
        st[a["status"]] += 1
    print("  status            1在投=%d(%.2f%%) 2暂停=%d(%.2f%%) 3已耗尽=%d(%.2f%%)" % (
        st[1], st[1] / n * 100, st[2], st[2] / n * 100, st[3], st[3] / n * 100))

    cc = defaultdict(int)
    for a in ads:
        cc[a["category_code"]] += 1
    top = sorted(cc.values(), reverse=True)
    print("  category_code     %d 个取值，Top1=%.1f%% 前5=%.1f%% 前10=%.1f%%" % (
        len(cc), top[0] / n * 100, sum(top[:5]) / n * 100, sum(top[:10]) / n * 100))

    bids = sorted(a["bid"] for a in ads)
    print("  bid               P50=%.2f P90=%.2f P95=%.2f P99=%.2f max=%.2f mean=%.2f" % (
        pct(bids, 0.50), pct(bids, 0.90), pct(bids, 0.95), pct(bids, 0.99),
        bids[-1], sum(bids) / n))
    print("                    (目标：P50≈2.7 / P95≈6.1 / max≈45)")

    bb = defaultdict(int)
    for a in ads:
        bb[a["daily_budget"]] += 1
    print("  daily_budget      " + "  ".join(
        "%d元=%.1f%%" % (k, bb[k] / n * 100) for k in sorted(bb)))

    viol = sum(1 for a in ads if a["bid"] >= a["daily_budget"])
    print("  bid>=预算 违规行  %d  (必须为 0)" % viol)

    adv = defaultdict(int)
    for a in ads:
        adv[a["advertiser_id"]] += 1
    counts = sorted(adv.values(), reverse=True)
    sc0 = scale if quick else 1.0
    print("  广告主            实际 %d 个，条数 max=%d P50=%d min=%d%s" % (
        len(adv), counts[0] * sc0, counts[len(counts) // 2] * sc0,
        counts[-1] * sc0, "（×%.0f 外推）" % scale if quick else ""))

    # 预算档位 × 平均可竞次数（对齐设计说明 §3.1 那张表）
    print("\n  日预算档位 → 平均可竞次数（daily_budget ÷ 平均 bid）")
    for k in sorted(bb):
        sub = [a["bid"] for a in ads if a["daily_budget"] == k]
        print("    %7.0f 元   广告数 %7d   平均bid %.2f   可竞约 %6d 次" % (
            k, len(sub), sum(sub) / len(sub), int(k / (sum(sub) / len(sub)))))

    print("\n" + line)
    print("  ad_target 倒排自检")
    print(line)
    names = {1: "地域", 2: "年龄段", 3: "性别", 4: "兴趣", 5: "系统", 6: "设备"}
    nvals = {1: len(geo["values"]), 2: len(AGE_BUCKETS), 3: len(GENDERS),
             4: len(INTERESTS), 5: len(OS_VALUES), 6: len(DEVICE_VALUES)}
    for d in sorted(dim_cnt):
        rows = dim_cnt[d]
        print("    dim=%d %-5s 行数 %9d  取值 %5d  平均命中 %8.1f  每广告 %.2f 条" % (
            d, names[d], rows, nvals[d], rows / nvals[d], rows / n))
    print("    合计            行数 %9d  每广告平均 %.2f 条" % (total_rows, total_rows / n))
    print("                    注：设计说明§3.2 的 482 万是「地域仅 28 个取值」时代的产物。")
    print("                    按 §七 把地域下沉到区县级后，单广告的地域条件数必然变多，")
    print("                    行数随之上升——这是修复候选集规模的代价，不是脚本跑偏。")
    print("                    想压回 482 万就调小 REGION_CNT_DIST，代价是候选集变小。")

    # ---- 候选集模拟：这是本脚本最关键的一个数字 ----
    print("\n" + line)
    print("  候选集模拟（地域 ∩ 年龄段 ∩ 性别，仅 status=1）")
    print(line)
    r_sets = {v: set(ids) for v, ids in inv[1].items()}
    a_sets = {v: set(ids) for v, ids in inv[2].items()}
    g_sets = {v: set(ids) for v, ids in inv[3].items()}

    # 请求按「地域命中数」加权抽样 —— 与 AdMapper.countTargetingValues 的加权口径一致
    rvals = [v for v in r_sets if r_sets[v]]
    rw = [len(r_sets[v]) for v in rvals]
    pick_r = make_picker(rvals, rw)
    pick_a_req = make_picker(AGE_BUCKETS, AGE_REQ_WEIGHTS)
    pick_g_req = make_picker(GENDERS, GENDER_REQ_WEIGHTS)

    sizes = []
    for _ in range(SELF_TEST_SAMPLES):
        r = pick_r(rnd)
        a = pick_a_req(rnd)
        g = pick_g_req(rnd)
        sizes.append(len(r_sets[r] & a_sets.get(a, set()) & g_sets.get(g, set())))
    sizes.sort()
    sc = scale if quick else 1.0
    tag = "（×%.0f 线性外推到 50 万）" % scale if quick else ""
    print("  按命中数加权抽样 %d 次 %s" % (SELF_TEST_SAMPLES, tag))
    print("    min=%d  P10=%.0f  P50=%.0f  P90=%.0f  max=%d  mean=%.0f" % (
        sizes[0] * sc, pct(sizes, 0.10) * sc, pct(sizes, 0.50) * sc,
        pct(sizes, 0.90) * sc, sizes[-1] * sc, sum(sizes) / len(sizes) * sc))
    p50 = pct(sizes, 0.50) * sc
    if 100 <= p50 <= 200:
        print("    ✅ P50 落在目标区间 100~200 —— 候选集规模达标")
    else:
        print("    ⚠️  P50=%.0f 不在目标区间 100~200" % p50)
        if p50 > 200:
            print("       调大 REGION_CNT_DIST 或改用 --region-level district 抬升地域基数；")
            print("       也可调小 AGE_CNT_DIST / GENDER_CNT_DIST 让年龄、性别筛得更狠。")
        else:
            print("       调小 REGION_CNT_DIST，或降低 AGE_CNT_DIST 的条件数。")

    # 顺带给出「地域单维命中」分布，说明为什么是这个量级
    hits = sorted(len(v) for v in r_sets.values())
    if hits:
        print("\n  地域单维命中（status=1）P50=%.0f P90=%.0f max=%d，取值数 %d" % (
            pct(hits, 0.50) * sc, pct(hits, 0.90) * sc, hits[-1] * sc, len(hits)))
    print(line)


def write_dim_dict(path, geo):
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["dim_type", "dim_value", "label"])
        city_of = {}
        for c, lst in geo["value_of_city"].items():
            for v in lst:
                city_of[v] = c
        for v in geo["values"]:
            c = city_of.get(v, "")
            w.writerow([1, v, "%s %s" % (geo["city_name"].get(c, ""), v)])
        for v in AGE_BUCKETS:
            w.writerow([2, v, "年龄段 " + v])
        for v in GENDERS:
            w.writerow([3, v, {"male": "男", "female": "女", "unknown": "未知"}[v]])
        for v in INTERESTS:
            w.writerow([4, v, v])
        for v in OS_VALUES:
            w.writerow([5, v, v])
        for v in DEVICE_VALUES:
            w.writerow([6, v, v])


# =====================================================================
# 八、主流程
# =====================================================================


def main():
    ap = argparse.ArgumentParser(description="RTB 引擎造数脚本（ad + ad_target）")
    ap.add_argument("--out", default="data", help="输出目录，默认 data")
    ap.add_argument("--seed", type=int, default=42, help="随机种子，默认 42")
    ap.add_argument("--region-level", default=REGION_LEVEL,
                    choices=["district", "city"],
                    help="地域颗粒：district 区县约 2500 个（默认）/ city 地市 337 个")
    ap.add_argument("--quick", action="store_true",
                    help="只生成 10 万条广告用于快速试参数，自检结果线性外推")
    args = ap.parse_args()

    ad_count = QUICK_AD_COUNT if args.quick else AD_COUNT
    scale = AD_COUNT / ad_count if args.quick else 1.0

    rnd = random.Random(args.seed)
    now = datetime(2026, 10, 1, 0, 0, 0)
    os.makedirs(args.out, exist_ok=True)

    print("RTB 造数脚本")
    print("  广告行数   %d%s" % (ad_count, "  (--quick)" if args.quick else ""))
    print("  地域颗粒   %s" % args.region_level)
    print("  随机种子   %d" % args.seed)
    print("  输出目录   %s" % args.out)

    geo = build_geo(args.region_level)
    print("  地域取值   %d 个（%d 市）" % (len(geo["values"]), len(geo["cities"])))

    print("\n[1/3] 生成 ad ...")
    ads = gen_ads(rnd, ad_count, now)
    ad_path = os.path.join(args.out, "ad.csv")
    write_ads(ad_path, ads)
    print("      -> %s (%d 行)" % (ad_path, len(ads)))

    print("[2/3] 生成 ad_target ...")
    t_path = os.path.join(args.out, "ad_target.csv")
    total, dim_cnt, inv = gen_targets(t_path, ads, geo, rnd)
    print("      -> %s (%d 行)" % (t_path, total))

    print("[3/3] 生成维度字典 ...")
    d_path = os.path.join(args.out, "dim_dict.csv")
    write_dim_dict(d_path, geo)
    print("      -> %s" % d_path)

    self_test(ads, geo, total, dim_cnt, inv, rnd, scale, args.quick)

    if not args.quick:
        print("\n导入 MySQL（先把 csv 拷进容器）：")
        print("  docker cp %s rtb-mysql:/tmp/ad.csv" % ad_path.replace("\\", "/"))
        print("  docker cp %s rtb-mysql:/tmp/ad_target.csv" % t_path.replace("\\", "/"))
        print("  docker exec -i rtb-mysql mysql -uroot rtb --local-infile=1 -e \"LOAD DATA LOCAL INFILE '/tmp/ad.csv' INTO TABLE ad FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' (id,advertiser_id,title,creative_url,category_code,bid,daily_budget,status,created_at,updated_at)\"")
        print("  docker exec -i rtb-mysql mysql -uroot rtb --local-infile=1 -e \"LOAD DATA LOCAL INFILE '/tmp/ad_target.csv' INTO TABLE ad_target FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' (ad_id,dim_type,dim_value)\"")


if __name__ == "__main__":
    main()
