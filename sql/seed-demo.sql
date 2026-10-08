-- =====================================================================
-- RTB 实时竞价广告投放引擎 · 演示造数脚本（轻量版）
-- =====================================================================
-- 用途：本地演示 / 自测时快速灌入少量数据。只造「竞价主链路」需要的
--       两张表：
--         ad          —— 广告（约 200 条）
--         ad_target   —— 定向条件倒排（每条广告 4 条，约 800 条）
--       其余表（bid_log / billing_event / bid_stat_hourly /
--       processed_event / ad_freq）由应用在竞价 / 消费过程中自行写入，
--       无需预灌。
--
-- 覆盖性保证：每条广告的 4 个定向维度（地域/年龄/性别/兴趣）由 n 的
--       取模枚举而成，200 条广告恰好铺满 5×4×2×5 = 200 种组合，
--       因此任意 (region,age,gender,interest) 请求都能命中候选，演示不会
--       出现「空广告」。
--
-- 调量：改顶部 @seed_cnt 一个数即可（5 处 CTE 上限会一起跟着变）。
--       注意 MySQL 默认 cte_max_recursion_depth=1000，超过需先
--       SET SESSION cte_max_recursion_depth = N;
--
-- 执行（库名 rtb，须先跑 all-schema.sql 建好表）：
--   docker exec -i rtb2-mysql mysql -uroot rtb < sql/seed-demo.sql
--   或（宿主已装客户端，项目根目录）：
--   mysql -h127.0.0.1 -uroot rtb < sql/seed-demo.sql
--
-- 重复执行前先清空（本脚本只 INSERT，不清表）：
--   TRUNCATE ad_target; TRUNCATE ad;
--
-- 模拟「暂停 / 已耗尽」测试状态过滤：
--   UPDATE ad SET status = 2 WHERE id = 1;     -- 暂停
--   UPDATE ad SET status = 3 WHERE id = 2;     -- 已耗尽
-- =====================================================================

USE rtb;

-- 造数总量（改这一个地方即可调量）
SET @seed_cnt = 200;

-- ---------------------------------------------------------------------
-- 1) 造 ad：@seed_cnt 条广告
--    id 显式指定为 1..@seed_cnt，与 ad_target.ad_id 一一对应
-- ---------------------------------------------------------------------
INSERT INTO ad (id, advertiser_id, title, creative_url, category_code, bid, daily_budget, status)
WITH RECURSIVE seq(n) AS (
  SELECT 1
  UNION ALL
  SELECT n + 1 FROM seq WHERE n < @seed_cnt
)
SELECT
  n,                                              -- id
  (n MOD 50) + 1,                                 -- advertiser_id：50 个广告主循环
  CONCAT('创意标题-', n),                          -- title
  CONCAT('https://cdn.example.com/creative/', n, '.jpg'),  -- creative_url
  ELT(1 + (n MOD 8), 'news','game','edu','finance','ecom','travel','auto','health'),  -- category_code（Zipf 近似）
  ROUND(0.50 + (n MOD 20) * 0.10, 4),             -- bid：0.50 ~ 2.40
  ELT(1 + (n MOD 5), 1000.00, 5000.00, 10000.00, 50000.00, 100000.00),  -- daily_budget（离散档位）
  1                                               -- status：演示默认全在投；暂停/耗尽用上面的 UPDATE 模拟
FROM seq;

-- ---------------------------------------------------------------------
-- 2) 造 ad_target：每条广告 4 条定向（地域 / 年龄 / 性别 / 兴趣）
--    dim_type: 1地域 2年龄段 3性别 4兴趣
--    组合枚举公式（保证 200 条铺满全部组合）：
--      r = n % 5            -> 5 个地域
--      a = (n DIV 5) % 4    -> 4 个年龄段
--      g = (n DIV 20) % 2   -> 2 个性别
--      i = (n DIV 40) % 5   -> 5 个兴趣
-- ---------------------------------------------------------------------

-- 2.1 地域（dim_type=1）
INSERT INTO ad_target (ad_id, dim_type, dim_value)
WITH RECURSIVE seq(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < @seed_cnt
)
SELECT n, 1, ELT(1 + (n MOD 5), '110000','310000','440100','510100','330100')
FROM seq;

-- 2.2 年龄段（dim_type=2）
INSERT INTO ad_target (ad_id, dim_type, dim_value)
WITH RECURSIVE seq(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < @seed_cnt
)
SELECT n, 2, ELT(1 + ((n DIV 5) MOD 4), '18-24','25-30','31-40','41-50')
FROM seq;

-- 2.3 性别（dim_type=3）
INSERT INTO ad_target (ad_id, dim_type, dim_value)
WITH RECURSIVE seq(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < @seed_cnt
)
SELECT n, 3, ELT(1 + ((n DIV 20) MOD 2), 'male','female')
FROM seq;

-- 2.4 兴趣（dim_type=4）
INSERT INTO ad_target (ad_id, dim_type, dim_value)
WITH RECURSIVE seq(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < @seed_cnt
)
SELECT n, 4, ELT(1 + ((n DIV 40) MOD 5), '数码科技','游戏','教育','电商','旅游')
FROM seq;

-- ---------------------------------------------------------------------
-- 验证（取消注释执行）：
--   SELECT COUNT(*) FROM ad;            -- 应 = @seed_cnt (默认 200)
--   SELECT COUNT(*) FROM ad_target;     -- 应 = @seed_cnt * 4 (默认 800)
--   SELECT * FROM ad LIMIT 5;
--   SELECT dim_type, dim_value, COUNT(*) FROM ad_target GROUP BY dim_type, dim_value ORDER BY dim_type, dim_value;
-- ---------------------------------------------------------------------
