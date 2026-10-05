-- =====================================================================
-- RTB 投放引擎 · M8 第 4 轮：小时级预聚合上线 + 90 天历史回填
-- =====================================================================
-- 这一轮要先解决两个前置问题（都是 M8 第 1 轮实测时发现的）：
--
-- 【前置 1】预聚合表几乎是空的
--   实测只有 60 行，覆盖 2026-10-05 17:00~20:00 这 4 小时，
--   而 bid_log 有 90 天。拿 60 行去和 1000 万行比是"作弊级"的好看。
--   必须先回填到真实量级（约 6~7 万行）再做 A/B。
--
-- 【前置 2】空广告率在预聚合表上算不出来
--   BidService.publishEvent 在 winner == null 时直接 return，
--   空广告竞价根本进不了 Kafka，所以本表 cnt 实际是「成交笔数」，
--   ad_cnt 恒等于 cnt。
--   → 第 4 轮的对照指标改用**两侧都算得出来的成交维度**：
--     成交数 / 成交总价 / 平均成交价
--   → 空广告率仍然只能查明细表，这一点在 metrics 里写明。
--
-- =====================================================================
-- 【第 1 步】改表：均值列换成「和」列
--
-- 为什么要改：累加语义下均值不可加。
--   avg_cost_ms   原来用 GREATEST 硬凑 = 取各批平均值里的最大值
--   avg_pay_price 原来根本不在 ON DUPLICATE KEY UPDATE 列表里，
--                 累加几批之后永远停在第一次的值
--   不改的话，第 4 轮的预聚合在平均成交价上根本对不上明细表。
--   （M0 的注释里其实已经预见了这一点，只是表结构没跟上。）
--
-- ⚠️ 只动这两列，PK 和其它列都不变，所以回填和消费端都很好对齐。
-- =====================================================================
ALTER TABLE bid_stat_hourly
  DROP COLUMN avg_cost_ms,
  DROP COLUMN avg_pay_price,
  ADD COLUMN tot_cost_ms     BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER ad_cnt,
  ADD COLUMN total_pay_price DECIMAL(14,2)   NOT NULL DEFAULT 0 AFTER p99_cost_ms;

-- =====================================================================
-- 【第 2 步】清空后回填 90 天
--
-- 为什么先 TRUNCATE：原来那 60 行是累加多批之后的产物，
-- 其中 avg 相关列本来就是错的（上面刚删掉），
-- 留着会和新回填的口径混在一起，对不上账。
--
-- 回填口径必须和 HourlyPreaggConsumer 完全一致，否则 A/B 不可信：
--   1. 只取 has_ad = 1  —— 消费端只能看到有胜出广告的事件
--   2. category_code 取自「胜出广告」而非请求 —— 消费端用 winner.getCategoryCode()
--      这也是为什么必须 JOIN ad 表：bid_log 自己没有 category 列
--   3. p99_cost_ms 取 MAX —— 消费端就是拿窗口内最大值近似 P99 的，
--      这里保持同样的近似，不假装能算出真 P99
-- =====================================================================
TRUNCATE TABLE bid_stat_hourly;

INSERT INTO bid_stat_hourly
  (stat_hour, slot_code, category_code,
   cnt, ad_cnt, tot_cost_ms, p99_cost_ms, total_pay_price, total_cost)
SELECT
  DATE_FORMAT(l.bid_at, '%Y-%m-%d %H:00:00') AS stat_hour,
  l.slot_code,
  a.category_code,
  COUNT(*)                                   AS cnt,
  COUNT(*)                                   AS ad_cnt,
  SUM(l.cost_ms)                             AS tot_cost_ms,
  MAX(l.cost_ms)                             AS p99_cost_ms,
  SUM(l.pay_price)                           AS total_pay_price,
  SUM(l.pay_price)                           AS total_cost
FROM bid_log l
JOIN ad a ON a.id = l.ad_id
WHERE l.has_ad = 1
GROUP BY stat_hour, l.slot_code, a.category_code;

-- =====================================================================
-- 【第 3 步】校验：回填出来的量必须和明细表对得上
--   对不上就说明回填口径错了，第 4 轮的 A→B 也就没有意义。
-- =====================================================================
SELECT
  (SELECT COUNT(*) FROM bid_log WHERE has_ad = 1)              AS detail_deals,
  (SELECT SUM(ad_cnt) FROM bid_stat_hourly)                   AS preagg_deals,
  (SELECT ROUND(SUM(pay_price),2) FROM bid_log WHERE has_ad=1) AS detail_amount,
  (SELECT ROUND(SUM(total_pay_price),2) FROM bid_stat_hourly)  AS preagg_amount;

-- 行数量级参考：约 90 天 × 24 小时 × 3 广告位 × 行业类目数
SELECT COUNT(*) AS preagg_rows FROM bid_stat_hourly;
