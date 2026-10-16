-- =====================================================================
-- RTB 实时竞价广告投放引擎 · 统一建表脚本（唯一权威 DDL）
-- =====================================================================
-- 本文件整合 M0 核心业务表 + M6 旁路消费表，作为初始化唯一入口。
--
-- 执行方式（MySQL 客户端，库名 rtb，对应 docker-compose.yml 的 MYSQL_DATABASE）：
--   docker exec -i rtb-mysql mysql -uroot rtb < sql/all-schema.sql
--   或（宿主已装客户端，项目根目录）：
--   mysql -h127.0.0.1 -uroot rtb < sql/all-schema.sql
--
-- 说明：
--   原 schema.sql / m6-counter-schema.sql 对 billing_event、bid_stat_hourly
--   存在双重定义且结构冲突（schema.sql 版本与消费代码不匹配，会导致
--   BillingReconConsumer / HourlyPreaggConsumer 报 bad SQL grammar）。
--   本文件以「与消费代码匹配」的版本为准，去重后共 7 张唯一表。
--   旧的三个分散文件（schema.sql / m1_indexes.sql / m6-counter-schema.sql）
--   初始化时不要再单独执行，只用本文件。
--
-- 存量库升级（本文件含 DROP TABLE，不能直接跑在已有数据的库上）：
--   后续新增的两个「管理端索引」用在线 DDL 添加，不阻塞读写：
--     ALTER TABLE ad        ADD KEY idx_adv (advertiser_id),
--                           ALGORITHM=INPLACE, LOCK=NONE;
--     ALTER TABLE ad_target ADD KEY idx_ad_dir (ad_id, dim_type, dim_value),
--                           ALGORITHM=INPLACE, LOCK=NONE;
-- =====================================================================

CREATE DATABASE IF NOT EXISTS rtb
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_0900_ai_ci;

USE rtb;

-- ---------------------------------------------------------------------
-- 1. ad · 广告主投放的广告（50 万行）
--    bid           DECIMAL(10,4) 单次展示出价（元）
--    daily_budget  DECIMAL(12,2) 日预算，离散档位
--    status        1在投 2暂停 3已耗尽（检索阶段需状态过滤）
--
--    idx_adv(advertiser_id) ——【管理端索引】服务 /admin/ad 的列表查询
--      （WHERE advertiser_id = ? ORDER BY id DESC）。
--      它不参与竞价检索，因此不改动 M1 baseline 下「ad 走主键 eq_ref」
--      的执行计划结论——「ad 不建索引」这条基线说的是检索路径。
--      借 InnoDB 二级索引隐含主键的特性，该索引可直接反向扫描满足倒序分页。
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS ad;
CREATE TABLE ad (
  id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  advertiser_id INT UNSIGNED    NOT NULL COMMENT '广告主 ID',
  title         VARCHAR(128)    NOT NULL COMMENT '创意标题',
  creative_url  VARCHAR(255)    NOT NULL COMMENT '素材地址',
  category_code VARCHAR(32)     NOT NULL COMMENT '行业类目，Zipf 分布',
  bid           DECIMAL(10,4)   NOT NULL COMMENT '单次展示出价（元）',
  daily_budget  DECIMAL(12,2)   NOT NULL COMMENT '日预算，离散档位',
  status        TINYINT         NOT NULL DEFAULT 1 COMMENT '1在投 2暂停 3已耗尽',
  created_at    DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  updated_at    DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  PRIMARY KEY (id),
  KEY idx_adv (advertiser_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='在投广告';

-- ---------------------------------------------------------------------
-- 2. ad_target · 定向条件倒排（500 万行）
--    dim_type: 1地域 2年龄段 3性别 4兴趣 5系统 6设备
--    M1 即在 ad_target 上建 idx_dim_lookup，承担「筛得准」；而 ad 表不建
--    「检索路径相关」的索引（承担「慢」，全表扫描基线）——
--    ad 上的 idx_adv 只服务管理后台查询，与检索路径无关，不违背该结论。
--
--    ① 倒排索引 (dim_type, dim_value, ad_id)：条件 → 广告（竞价检索用）
--      dim_type + dim_value 是定向倒排的两个条件列（等值前缀定位候选）
--      ad_id 是倒排指向的广告 ID，供 JOIN / 回表使用（覆盖索引）
--
--    ② 正排索引 (ad_id, dim_type, dim_value)：广告 → 条件（管理后台用）
--      ad_id 必须在最左，才能支持 WHERE ad_id = ?（最左前缀原则；
--      倒排索引里 ad_id 是第三列，单独用它走不了索引 → 只能全表扫 644 万行）
--      三列同时覆盖 SELECT dim_type, dim_value，查询无需回表
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS ad_target;
CREATE TABLE ad_target (
  id        BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  ad_id     BIGINT UNSIGNED NOT NULL COMMENT '广告 ID，关联 ad.id',
  dim_type  TINYINT         NOT NULL COMMENT '1地域 2年龄段 3性别 4兴趣 5系统 6设备',
  dim_value VARCHAR(64)     NOT NULL COMMENT '定向值，如 110000 / 25-30 / male / 数码科技',
  PRIMARY KEY (id),
  KEY idx_dim_lookup (dim_type, dim_value, ad_id),
  KEY idx_ad_dir (ad_id, dim_type, dim_value)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='定向条件倒排';

-- ---------------------------------------------------------------------
-- 3. bid_log · 竞价明细（1000 万行，M8 四轮调优主战场）
--    【可丢数据】走本地有界队列 + 批量刷盘，队列满允许丢弃低价值明细
--    无任何查询索引与分区，M8 逐轮加
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS bid_log;
CREATE TABLE bid_log (
  id             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  request_id     CHAR(32)        NOT NULL COMMENT '竞价请求唯一 ID，幂等键',
  ad_id          BIGINT UNSIGNED          DEFAULT NULL COMMENT '胜出广告 ID，has_ad=0 时 NULL',
  advertiser_id  INT UNSIGNED             DEFAULT NULL COMMENT '胜出广告主 ID，has_ad=0 时 NULL',
  slot_code      VARCHAR(32)     NOT NULL COMMENT '广告位',
  region         VARCHAR(16)     NOT NULL COMMENT '地域编码',
  age            TINYINT         NOT NULL COMMENT '年龄',
  gender         TINYINT         NOT NULL COMMENT '1男 2女 0未知',
  interests      VARCHAR(255)    NOT NULL DEFAULT '' COMMENT '兴趣标签，逗号分隔',
  candidate_cnt  INT             NOT NULL COMMENT '定向过滤前候选数，目标 100~200',
  match_cnt      INT             NOT NULL COMMENT '定向过滤后数量，目标 10~30',
  has_ad         TINYINT         NOT NULL COMMENT '1有广告 0空广告',
  win_bid        DECIMAL(10,4)            DEFAULT NULL COMMENT '胜出出价',
  pay_price      DECIMAL(10,4)            DEFAULT NULL COMMENT '实际扣费，M1 一价、M4 起二价',
  cost_ms        INT             NOT NULL COMMENT '竞价总耗时',
  candidate_cost INT             NOT NULL COMMENT '候选检索耗时，M2 优化核心指标',
  filter_cost    INT             NOT NULL COMMENT '定向过滤耗时',
  sort_cost      INT             NOT NULL COMMENT '排序耗时',
  bill_cost      INT             NOT NULL COMMENT '计费耗时',
  bid_at         DATETIME(3)     NOT NULL COMMENT '竞价时间，M8-3 按此字段做 RANGE 分区',
  PRIMARY KEY (id),
  UNIQUE KEY uk_request (request_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='竞价明细';

-- ---------------------------------------------------------------------
-- 4. billing_event · 计费事件（M6 旁路由 BillingReconConsumer 写入）
--    【不可丢数据】涉及金钱，走 Kafka + 消费端幂等落库
--    列结构必须与 BillingReconConsumer 的
--      INSERT INTO billing_event(event_id, ad_id, advertiser_id, pay_price, ts)
--      ON DUPLICATE KEY UPDATE pay_price=VALUES(pay_price)
--    完全一致；event_id 为主键（唯一键）支撑幂等
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS billing_event;
CREATE TABLE billing_event (
  event_id       VARCHAR(64) NOT NULL COMMENT '事件唯一 ID，消费端幂等键',
  ad_id          BIGINT      NULL COMMENT '广告 ID，关联 ad.id',
  advertiser_id  INT         NULL COMMENT '广告主 ID，关联 ad.advertiser_id',
  pay_price      DECIMAL(12,4) NULL COMMENT '实际扣费',
  ts             BIGINT      NULL COMMENT '事件时间戳（毫秒）',
  PRIMARY KEY (event_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='M6 计费对账明细（消费端幂等落库）';

-- ---------------------------------------------------------------------
-- 5. bid_stat_hourly · 小时级预聚合（M6 旁路由 HourlyPreaggConsumer 写入）
--    数据源是 M6 的 hourly-preagg 消费组，非定时任务扫明细
--    列结构必须与 HourlyPreaggConsumer 的
--      INSERT INTO bid_stat_hourly(stat_hour, ad_id, impressions, spend)
--      ON DUPLICATE KEY UPDATE impressions=impressions+1, spend=spend+VALUES(spend)
--    完全一致；主键 (stat_hour, ad_id) 支撑 upsert 累加
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS bid_stat_hourly;
CREATE TABLE bid_stat_hourly (
  stat_hour    VARCHAR(16) NOT NULL COMMENT '整点小时（yyyy-MM-dd HH:00）',
  ad_id        BIGINT      NOT NULL COMMENT '广告 ID，与 stat_hour 组成主键支撑 upsert 累加',
  impressions  INT     NOT NULL DEFAULT 0 COMMENT '曝光数',
  spend        DECIMAL(14,4) NOT NULL DEFAULT 0 COMMENT '消耗',
  PRIMARY KEY (stat_hour, ad_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='M6 小时级曝光/消耗聚合';

-- ---------------------------------------------------------------------
-- 6. processed_event · 消费幂等去重（M6 三个消费组共用）
--    INSERT IGNORE 在 (event_id, grp) 已存在时返回 0 影响行数 → 消费者据此跳过
--    grp 区分消费组：billing-recon / hourly-preagg / freq-antifraud
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS processed_event;
CREATE TABLE processed_event (
  event_id VARCHAR(64) NOT NULL COMMENT '事件唯一 ID',
  grp      VARCHAR(32) NOT NULL COMMENT '消费组标识',
  PRIMARY KEY (event_id, grp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='M6 消费幂等去重';

-- ---------------------------------------------------------------------
-- 7. ad_freq · 频次反作弊（M6 旁路由 FreqAntifraudConsumer 写入）
--    维度简化：m1-explore 线无 userId，按 ad_id 计数（功能模式，仅聚合维度简化）
--    列结构必须与 FreqAntifraudConsumer 的
--      INSERT INTO ad_freq(ad_id, cnt) VALUES(?,1) ON DUPLICATE KEY UPDATE cnt=cnt+1
--    完全一致
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS ad_freq;
CREATE TABLE ad_freq (
  ad_id  BIGINT NOT NULL COMMENT '广告 ID',
  cnt    INT    NOT NULL DEFAULT 0 COMMENT '曝光频次',
  PRIMARY KEY (ad_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='M6 广告曝光频次（反作弊用）';
