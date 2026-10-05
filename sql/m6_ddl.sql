-- =====================================================================
-- RTB 投放引擎 · M6 Kafka 事件旁路的消费落地表
--
-- 执行方式：
--   docker exec -i rtb-mysql mysql -uroot rtb < sql/m6_ddl.sql
--
-- 设计说明（面试要点）：
--
-- 1. 【为什么消费端要独立落表，而不是直接读 bid_log】
--    bid_log 是同步链路写的，事件走 Kafka 异步消费后如果复用同一张表，
--    就分不清「这行是同步写的还是消费写的」，出问题无法定位来源。
--    独立表让「同步产出」与「异步消费」可对账——
--    这正是 M6 的验收方式：两张表逐条比对，差异 0 条。
--
-- 2. 【三张表对应三个消费组，各自独立】
--    billing_event_consumer  ← billing-recon    （计费对账，金钱事实）
--    bid_stat_hourly         ← hourly-preagg   （小时级预聚合，大盘）
--    ad_freq_counter         ← freq-antifraud  （频次与反作弊，中间态）
--
--    分界线还是那条：【丢了这笔数据有没有业务后果】。
--    计费不能丢、预聚合丢了能重算、频次丢了能容忍。
--
-- 3. 【幂等靠唯一键，不靠代码里的 if 判断】
--    三张表都建了事件唯一键（uk_event_*）。
--    Kafka 的 at-least-once 语义必然重投，代码层的「查一下再插」
--    在并发下仍会重复；唯一键是数据库层的物理保障。
-- =====================================================================

USE rtb;

-- ---------------------------------------------------------------------
-- 1. billing_event_consumer · 计费对账落地表
--
-- 【为什么和 billing_event 结构一样却不复用同一张表】
--   billing_event 是 M4 同步写入的「此刻发生的成交」；
--   本表是「Kafka 消费到的成交」。
--   两者在正常情况下应完全一致，出现差异只有两种可能：
--     · 同步写了但事件丢了     → 消费表少于同步表（漏）
--     · 事件重投被幂等拦截     → 两表相等（正常）
--   所以 M6 的验收 SQL 就是：两表按 ad_id 汇总对比，差额 0。
--
--   如果不建独立表，这个对照根本无法表达。
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS billing_event_consumer (
  id            bigint unsigned  NOT NULL AUTO_INCREMENT,
  event_id      char(32)        NOT NULL COMMENT '事件唯一 ID，幂等键',
  request_id    char(32)        NOT NULL COMMENT '关联同步链路的 request_id',
  ad_id         bigint unsigned NOT NULL COMMENT '广告 ID，Kafka 分区键',
  advertiser_id int unsigned    NOT NULL,
  bid_price     decimal(10,2)   NOT NULL COMMENT '胜者出价',
  pay_price     decimal(10,2)   NOT NULL COMMENT '应收（二价）',
  second_price  decimal(10,2)   NULL     COMMENT '次高价，一价为 NULL',
  is_second_price tinyint       NOT NULL DEFAULT 0,
  consumed_at   datetime(3)     NOT NULL COMMENT '消费落库时刻',
  consumer_group varchar(64)    NOT NULL COMMENT '消费组名，排查用',
  PRIMARY KEY (id),
  -- 幂等的物理保障：同一 event_id 重复投递只会命中这个唯一键
  UNIQUE KEY uk_event (event_id),
  KEY idx_ad_time (ad_id, consumed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_0900_ai_ci COMMENT='M6 计费对账消费落地表';


-- ---------------------------------------------------------------------
-- 2. ad_freq_counter · 频次与反作弊中间态
--
-- 【为什么频次计数存 Redis 不存 MySQL】
--   频次判断是「同一个人多久看到同一条广告」，要求毫秒级读写。
--   走 MySQL 的话每次曝光要一次读写，QPS 直接被数据库拖住。
--   本表只存「按小时打点后的频次快照」，供离线分析使用：
--   实时判断在 Redis，事后分析在 MySQL，两者职责分开。
--
-- 【为什么唯一键是 (ad_id, user_id, stat_hour) 而不是 event_id】
--   频次计数的业务语义是「计数」，不是「记录事件」。
--   同一用户同一小时对同一广告的第 1 次和第 5 次曝光，
--   对频次这个指标来说等价——都只该 +1。
--   用 (ad_id,user_id,hour) 做唯一键 + ON DUPLICATE KEY UPDATE cnt=cnt+1
--   天然实现幂等：重投 100 次也只 +1。
--   这比用 event_id 判重更贴合业务语义。
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ad_freq_counter (
  id          bigint unsigned NOT NULL AUTO_INCREMENT,
  ad_id       bigint unsigned NOT NULL,
  user_id     bigint unsigned NOT NULL COMMENT '用户 ID，本项目用 request_id 派生',
  stat_hour   datetime        NOT NULL COMMENT '整点小时',
  cnt         int unsigned    NOT NULL DEFAULT 0 COMMENT '该小时内的曝光次数',
  updated_at  datetime(3)     NOT NULL,
  PRIMARY KEY (id),
  -- 唯一键即幂等键：重投时走 ON DUPLICATE KEY UPDATE 而非新增行
  UNIQUE KEY uk_freq (ad_id, user_id, stat_hour),
  KEY idx_hour (stat_hour)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_0900_ai_ci COMMENT='M6 频次与反作弊消费表（小时打点）';


-- ---------------------------------------------------------------------
-- 3. 【预聚合表已在 M0 建好，这里不重复建】
--
--   bid_stat_hourly(stat_hour, slot_code, category_code)
--     ← hourly-preagg 消费组写入
--
--   唯一键就是主键，所以「按小时 + 广告位 + 类目」三个维度
--   天然幂等：同一小时内同一组合的多次事件，
--   消费端用 REPLACE 或 ON DUPLICATE KEY UPDATE 累加即可。
--
--   M8 的第 4 轮优化会改成「预聚合表读写分离 + 增量更新」，
--   本轮不提前做，留出对照空间。
-- ---------------------------------------------------------------------


-- ---------------------------------------------------------------------
-- 4. 消费审计表：记录每个消费组处理了多少、跳过了多少
--
-- 【为什么要这张表】
--   演示第 6 步要展示「停掉 hourly-preagg 后竞价不受影响而 lag 爬升」。
--   光看 Kafka 的 lag 只能说明「没消费」，说不出「消费组在运行但处理失败」。
--   这张表让「幂等跳过了多少条」可查——
--   幂等是静默的，重复投递被唯一键拦下时没有任何异常，
--   不记账就看不出幂等到底生效没有。
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS consumer_audit (
  id              bigint unsigned NOT NULL AUTO_INCREMENT,
  consumer_group  varchar(64)    NOT NULL,
  partition_no    int            NOT NULL,
  offset_no       bigint         NOT NULL,
  event_id        char(32)       NOT NULL,
  action          varchar(16)    NOT NULL COMMENT 'PROCESSED / DUPLICATE_SKIPPED / FAILED',
  cost_ms         int            NOT NULL DEFAULT 0,
  processed_at    datetime(3)    NOT NULL,
  PRIMARY KEY (id),
  KEY idx_group_time (consumer_group, processed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_0900_ai_ci COMMENT='M6 消费审计表，幂等跳过的记录也记账';


-- ---------------------------------------------------------------------
-- 5. M6 验收 SQL（写在这里方便现场直接跑）
-- ---------------------------------------------------------------------

-- 验收 1：同步表与消费表逐条对账，差异应为 0
--   注意时间窗口：billing_event 累积全天数据，
--   而消费表只包含 Kafka 实际投递到的那部分。
--   必须限定「本轮压测开始之后」的时间窗口，否则口径不一致会算出巨大差额。
--
-- SELECT b.ad_id,
--        ROUND(SUM(b.pay_price), 2)   AS sync_amt,
--        ROUND(SUM(c.pay_price), 2)   AS consume_amt,
--        ROUND(SUM(b.pay_price) - SUM(c.pay_price), 2) AS diff
-- FROM billing_event b
-- LEFT JOIN billing_event_consumer c
--        ON c.event_id = b.event_id AND c.consumer_group = 'billing-recon'
-- WHERE b.billed_at >= '2026-01-01 00:00:00'
-- GROUP BY b.ad_id
-- HAVING ABS(diff) > 0.01;

-- 验收 2：幂等生效验证（先看audit 有没有 DUPLICATE_SKIPPED，
--          再确认表里没有重复的 event_id）
-- SELECT action, COUNT(*) FROM consumer_audit GROUP BY consumer_group, action;
-- SELECT event_id, COUNT(*) c FROM billing_event_consumer
--  GROUP BY event_id HAVING c > 1;   -- 应返回空

-- 验收 3：分区数
--   kafka-topics.sh --describe --topic bid_event | grep PartitionCount
