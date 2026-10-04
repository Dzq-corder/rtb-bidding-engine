-- =====================================================================
-- RTB 实时竞价广告投放引擎 · M0 建表脚本
-- =====================================================================
-- 执行方式（数据卷已在容器内，CSV 需 docker cp 后用服务端 LOAD DATA INFILE）：
--   docker compose exec -T mysql < sql/schema.sql
--
-- 设计说明（面试要点）：
--
-- 1. 本版本【不建任何分区、不加任何查询索引】
--    理由：M1 需要一个「未优化」的对照组，EXPLAIN 必须是 type=ALL（全表扫描）。
--    索引与分区分别在 M8 的第 1 轮、第 3 轮加入，届时才有真实的优化前后对照。
--
-- 2. 主键保持干净的单一列 id
--    MySQL 分区表要求「分区键必须出现在每个唯一索引中」。
--    本项目按 bid_at 分区，到 M8-3 时主键必须改成 (id, bid_at)、
--    唯一键必须改成 (request_id, bid_at)，否则建表直接报 ERROR 1503。
--    这个约束留到真正加分区时亲手踩一遍。
--
-- 3. 预算挂在广告（ad）上而非广告主上
--    对账口径是「SUM(消耗) > 日预算的广告数 = 0」，即按广告维度校验。
-- =====================================================================

CREATE DATABASE IF NOT EXISTS rtb
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_0900_ai_ci;

USE rtb;

-- ---------------------------------------------------------------------
-- 1. ad · 广告主投放的广告（50 万行）
--
-- 字段分布刻意不均匀，理由见《造数方案与数据来源说明》§2.1：
--   bid         对数正态长尾，真实出价是「大量低价 + 少量高价」
--   category    Zipf 分布，前 5 个类目占约 60%
--   daily_budget 离散档位 100/500/2000/10000，真实广告主按档位设置
--   status      80% 在投 / 15% 暂停 / 5% 已耗尽，让检索阶段真的需要状态过滤
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS ad;
CREATE TABLE ad (
  id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  advertiser_id INT UNSIGNED    NOT NULL COMMENT '广告主 ID',
  title         VARCHAR(128)    NOT NULL COMMENT '创意标题，由行业前缀 x 品牌词 x 产品词组合',
  creative_url  VARCHAR(255)    NOT NULL COMMENT '素材地址',
  category_code VARCHAR(32)     NOT NULL COMMENT '行业类目，Zipf 分布',
  bid           DECIMAL(10,2)   NOT NULL COMMENT '出价，单位元，对数正态长尾',
  daily_budget  DECIMAL(12,2)   NOT NULL COMMENT '日预算，离散档位',
  status        TINYINT         NOT NULL DEFAULT 1 COMMENT '1在投 2暂停 3已耗尽',
  created_at    DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at    DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='在投广告';

-- ---------------------------------------------------------------------
-- 2. ad_target · 定向条件倒排（500 万行）
--
-- 数量不是拍脑袋来的，而是由广告的定向条件反推展开：
--   50 万广告 x 平均 10 个定向项 ≈ 500 万行
-- 这个推导过程本身就是面试要讲的内容。
--
-- dim_type: 1地域 2年龄段 3性别 4兴趣标签 5操作系统 6设备类型
-- 一个广告有 8~13 行，每行一个定向项。
--
-- M1 阶段本表不使用索引（会全表扫描 500 万行），M2 才倒排化进内存。
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS ad_target;
CREATE TABLE ad_target (
  id        BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  ad_id     BIGINT UNSIGNED NOT NULL COMMENT '广告 ID',
  dim_type  TINYINT         NOT NULL COMMENT '1地域 2年龄段 3性别 4兴趣 5系统 6设备',
  dim_value VARCHAR(64)     NOT NULL COMMENT '定向值，如 110000 / 25-34 / male / 运动',
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='定向条件倒排';

-- ---------------------------------------------------------------------
-- 3. bid_log · 竞价明细（1000 万行，M8 四轮调优的主战场）
--
-- 【可丢数据】用于大盘分析的竞价明细。走本地有界队列 + 批量刷盘，
--   队列满时允许丢弃低价值明细。
--   【不可与 billing_event 混淆】涉及金钱的计费事件在下一张表。
--
-- M1 阶段写入是逐条 INSERT；M7 才改批量。
-- 无任何查询索引与分区，M8 逐轮加。
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS bid_log;
CREATE TABLE bid_log (
  id             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  request_id     CHAR(32)        NOT NULL COMMENT '竞价请求唯一 ID，同时是幂等键',
  ad_id          BIGINT UNSIGNED          DEFAULT NULL COMMENT '胜出广告 ID，has_ad=0 时为 NULL',
  advertiser_id  INT UNSIGNED             DEFAULT NULL COMMENT '胜出广告主 ID',
  slot_code      VARCHAR(32)     NOT NULL COMMENT '广告位',
  -- 请求上下文（用户画像）
  region         VARCHAR(16)     NOT NULL COMMENT '地域编码',
  age            TINYINT         NOT NULL COMMENT '年龄',
  gender         TINYINT         NOT NULL COMMENT '1男 2女 0未知',
  interests      VARCHAR(255)    NOT NULL DEFAULT '' COMMENT '兴趣标签，逗号分隔',
  -- 竞价结果
  candidate_cnt  INT             NOT NULL COMMENT '定向过滤前候选数，目标 100~200',
  match_cnt      INT             NOT NULL COMMENT '定向过滤后数量，目标 10~30',
  has_ad         TINYINT         NOT NULL COMMENT '1有广告 0空广告',
  win_bid        DECIMAL(10,2)            DEFAULT NULL COMMENT '胜出出价',
  pay_price      DECIMAL(10,2)            DEFAULT NULL COMMENT '实际扣费，M1 一价、M4 起二价',
  -- 耗时分段（毫秒），用于定位瓶颈
  cost_ms        INT             NOT NULL COMMENT '竞价总耗时',
  candidate_cost INT             NOT NULL COMMENT '候选检索耗时，M2 优化的核心指标',
  filter_cost    INT             NOT NULL COMMENT '定向过滤耗时',
  sort_cost      INT             NOT NULL COMMENT '排序耗时',
  bill_cost      INT             NOT NULL COMMENT '计费耗时',
  bid_at         DATETIME(3)     NOT NULL COMMENT '竞价时间，M8-3 按此字段做 RANGE 分区',
  PRIMARY KEY (id),
  -- 幂等键。此处是干净的单列唯一键；M8-3 加分区后必须改为 (request_id, bid_at)
  UNIQUE KEY uk_request (request_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='竞价明细';

-- ---------------------------------------------------------------------
-- 4. billing_event · 计费事件（M6 起经 Kafka 旁路投递）
--
-- 【不可丢数据】涉及金钱。走 Kafka + 消费端幂等落库，
--   不允许采样或丢弃。resume 上「对账差异 0 条」指的是这张表。
--
-- event_id 是消费端幂等键：落库用 INSERT ... ON DUPLICATE KEY UPDATE
-- 或状态机校验，重复投递只会被识别并丢弃，保证最终只生效一次。
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS billing_event;
CREATE TABLE billing_event (
  id             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  event_id       CHAR(32)        NOT NULL COMMENT '事件唯一 ID，消费端幂等键',
  request_id     CHAR(32)        NOT NULL COMMENT '来源竞价请求 ID',
  ad_id          BIGINT UNSIGNED NOT NULL,
  advertiser_id  INT UNSIGNED    NOT NULL,
  bid_price      DECIMAL(10,2)   NOT NULL COMMENT '胜出出价',
  pay_price      DECIMAL(10,2)   NOT NULL COMMENT '实际扣费',
  second_price   DECIMAL(10,2)            DEFAULT NULL COMMENT '次高价，二价结算依据',
  is_second_price TINYINT        NOT NULL DEFAULT 0 COMMENT '1二价 0一价（M1 阶段）',
  billed_at      DATETIME(3)     NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_event (event_id),
  KEY idx_advertiser_time (advertiser_id, billed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='计费事件，不可丢';

-- ---------------------------------------------------------------------
-- 5. bid_stat_hourly · 小时级预聚合（几十 万行）
--
-- 数据源是 M6 的 hourly-preagg 消费组，不是定时任务扫明细表。
--   这条链路让 SQL 调优不是孤立练习——汇总表的每一行都由真实事件流产生。
--
-- 关于 p99_cost_ms：SQL 的 AVG/COUNT 无法算出分位数。
--   该列由消费端在内存滑动窗口内算出后随小时汇总一起落库，
--   不是对明细表做 SQL 聚合得到的。
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS bid_stat_hourly;
CREATE TABLE bid_stat_hourly (
  stat_hour     DATETIME NOT NULL COMMENT '整点小时',
  slot_code     VARCHAR(32) NOT NULL COMMENT '广告位',
  category_code VARCHAR(32) NOT NULL COMMENT '行业类目',
  cnt           BIGINT UNSIGNED NOT NULL COMMENT '竞价请求数',
  ad_cnt        BIGINT UNSIGNED NOT NULL COMMENT '返回广告数，cnt - ad_cnt 即空广告数',
  avg_cost_ms   INT NOT NULL COMMENT '平均耗时',
  p99_cost_ms   INT NOT NULL COMMENT 'P99 耗时，消费端内存滑动窗口计算',
  avg_pay_price DECIMAL(10,2) NOT NULL COMMENT '平均成交价',
  total_cost    DECIMAL(14,2) NOT NULL COMMENT '总消耗',
  PRIMARY KEY (stat_hour, slot_code, category_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='小时级预聚合大盘';
