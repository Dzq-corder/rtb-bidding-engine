-- =====================================================================
-- RTB 投放引擎 · M8 第 3 轮 附加实验：DELETE vs DROP PARTITION
-- =====================================================================
-- 这一段回答一个比"EXPLAIN 出现 partitions"更重要的问题：
-- **分区到底带来了什么？**
--
-- 诚实的答案：对已经带时间窗 + 合适索引的查询，分区裁剪的提升很有限
-- （第 1 轮的索引已经把扫描范围压到 3.5 万行了）。
-- 分区真正值钱的地方是**数据生命周期管理**：
--   DELETE  逐行删除 → 写 undo/redo、留下碎片、data_length 不变
--   DROP    改元数据  → 瞬时完成、空间直接还给操作系统
--
-- ⚠️ 本脚本会**真的删掉 2026-07-07 这一天约 3.5 万行数据**。
--   这是演示数据，删的是"已经过了保留期的冷数据"，符合真实场景。
--   不想删就别跑。
-- =====================================================================

-- ---------------------------------------------------------------------
-- 第 1 步：记录删除前的表体积（DELETE 之后它不会变，这是碎片的证据）
-- ---------------------------------------------------------------------
SET SESSION information_schema_stats_expiry = 0;
SELECT ROUND(data_length/1024/1024) AS data_mb,
       ROUND(index_length/1024/1024) AS idx_mb
FROM information_schema.tables
WHERE table_schema='rtb' AND table_name='bid_log';

SELECT COUNT(*) AS rows_in_that_day
FROM bid_log
WHERE bid_at >= '2026-07-07 00:00:00' AND bid_at < '2026-07-08 00:00:00';

-- ---------------------------------------------------------------------
-- 第 2 步：用 DELETE 删掉这一天，记耗时
--   （在分区表上做这件事恰恰是最坏组合：明明可以 DROP 却在 DELETE）
-- ---------------------------------------------------------------------
DELETE FROM bid_log
WHERE bid_at >= '2026-07-07 00:00:00' AND bid_at < '2026-07-08 00:00:00';

-- 再看一次体积：几乎没变 → 空间还在表里，只是变成碎片
SET SESSION information_schema_stats_expiry = 0;
SELECT ROUND(data_length/1024/1024) AS data_mb,
       ROUND(index_length/1024/1024) AS idx_mb
FROM information_schema.tables
WHERE table_schema='rtb' AND table_name='bid_log';

-- ---------------------------------------------------------------------
-- 第 3 步：同样的事，用 DROP PARTITION
--   p20260707 这个分区现在已经空了，正好用来单独观察 DROP 本身的耗时
--   （第 2 步已经证明"删掉这一天"本身不慢，慢的是逐行删 + 留碎片）
-- ---------------------------------------------------------------------
ALTER TABLE bid_log DROP PARTITION p20260707;

-- 体积会真的掉下来（因为 DROP 的是一整个分区，含该分区的索引一起释放）
SET SESSION information_schema_stats_expiry = 0;
SELECT ROUND(data_length/1024/1024) AS data_mb,
       ROUND(index_length/1024/1024) AS idx_mb
FROM information_schema.tables
WHERE table_schema='rtb' AND table_name='bid_log';

-- ---------------------------------------------------------------------
-- 第 4 步：看分区裁剪
--   只命中 1 个分区 → p20261005
-- ---------------------------------------------------------------------
EXPLAIN
SELECT has_ad, COUNT(*) AS cnt
FROM bid_log
WHERE slot_code = 'feed_top'
  AND bid_at >= '2026-10-05 00:00:00' AND bid_at < '2026-10-05 01:00:00'
GROUP BY has_ad;

-- 反例：分区键套函数 → partitions 变成全部 92 个，裁剪完全失效
EXPLAIN
SELECT has_ad, COUNT(*) AS cnt
FROM bid_log
WHERE slot_code = 'feed_top'
  AND TO_DAYS(bid_at) = TO_DAYS('2026-10-05 12:00:00')
GROUP BY has_ad;
