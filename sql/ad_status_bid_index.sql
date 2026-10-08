-- =====================================================================
-- RTB 投放引擎 · ad 表联合索引（优化 findCandidates 候选检索）
-- =====================================================================
-- 对应 SQL（AdMapper.xml#findCandidates）：
--   SELECT ... FROM ad a
--   [JOIN ad_target t1/2/3 ON ... dim_type=? AND dim_value=?]
--   WHERE a.status = 1 [AND a.category_code = ?]
--   ORDER BY a.bid DESC
--
-- 痛点（M1 baseline）：ad 表只有主键，候选检索是 type=ALL 全表扫描 + Using filesort，
--   对 40 万 status=1 的行排序代价极高，是整条链路的瓶颈。
--
-- 优化：在 (status, bid) 上建联合索引
--   - status 等值过滤（最左前缀），直接缩小扫描范围
--   - bid 作为排序键置于索引末尾，ORDER BY bid DESC 可直接沿索引反向读取，消除 filesort
--   - ad_target 侧仍用既有 idx_dim_lookup(dim_type, dim_value, ad_id) 做 JOIN
--
-- 若线上请求「必带」category_code，可升级为 (status, category_code, bid) 进一步利用最左前缀。
-- =====================================================================

CREATE INDEX idx_status_bid ON ad (status, bid);
-- 升级版（按需启用，二者择一）：
-- CREATE INDEX idx_status_cat_bid ON ad (status, category_code, bid);

-- 恢复 baseline（删除索引）：
-- DROP INDEX idx_status_bid ON ad;
