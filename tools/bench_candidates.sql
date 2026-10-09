-- =====================================================================
-- RTB 引擎 · 候选检索 SQL 单线程串行基准（M1 baseline 的 A 值）
-- =====================================================================
-- 测什么：AdMapper.xml#findCandidates 的等价 SQL（地域 ∩ 年龄段 ∩ 性别，
--         status=1，ORDER BY bid DESC），在当前 50 万 ad / 643 万 ad_target
--         下的服务端执行耗时。
--
-- 怎么测：
--   1. 先把三个维度的取值与「命中广告数」物化成临时权重表 w1/w2/w3；
--   2. 每次请求按指数竞争法（-LOG(RAND())/c）加权抽样取值，
--      与 gen_ad_data.py 自检、AdMapper.countTargetingValues 的口径一致；
--   3. 每条请求现场 PREPARE + EXECUTE + DEALLOCATE（不复用 prepared stmt），
--      贴近 MyBatis 客户端预处理、每次都带解析开销的真实路径；
--   4. 全程在 MySQL 内完成计时，不含 docker exec / 网络 / JVM 开销。
--
-- 计时拆解：
--   total_ms = PREPARE 前 → EXECUTE 后（含服务端解析，≈ JDBC 一次执行）
--   exec_ms  = PREPARE 后 → EXECUTE 后（纯执行，不含解析）
--
-- 用法：
--   docker exec -i rtb2-mysql mysql -uroot rtb < tools/bench_candidates.sql
--   改样本数：把文件末尾的 CALL bench_candidates(50) 里的 50 改掉
--
-- 样本数怎么选（实测）：
--   带 idx_dim_lookup  → 单条 2~27ms，500 条约 10s，可以用 500
--   删掉 idx_dim_lookup → 单条 6~8s，  50 条就要 5m35s，别再往上加
-- =====================================================================

USE rtb;

DROP PROCEDURE IF EXISTS bench_candidates;

DELIMITER //
CREATE PROCEDURE bench_candidates(IN n INT)
BEGIN
  DECLARE i        INT DEFAULT 0;
  DECLARE v_r      VARCHAR(64);
  DECLARE v_a      VARCHAR(64);
  DECLARE v_g      VARCHAR(64);
  DECLARE t0       DATETIME(6);
  DECLARE t1       DATETIME(6);
  DECLARE t2       DATETIME(6);
  DECLARE v_cnt    INT;
  DECLARE sql_txt  TEXT;

  -- 权重表：每个维度取值命中多少条倒排行（status 无关的倒排全量）
  DROP TEMPORARY TABLE IF EXISTS w1;
  CREATE TEMPORARY TABLE w1 AS
    SELECT dim_value, COUNT(*) c FROM ad_target WHERE dim_type = 1 GROUP BY dim_value;
  DROP TEMPORARY TABLE IF EXISTS w2;
  CREATE TEMPORARY TABLE w2 AS
    SELECT dim_value, COUNT(*) c FROM ad_target WHERE dim_type = 2 GROUP BY dim_value;
  DROP TEMPORARY TABLE IF EXISTS w3;
  CREATE TEMPORARY TABLE w3 AS
    SELECT dim_value, COUNT(*) c FROM ad_target WHERE dim_type = 3 GROUP BY dim_value;

  -- 用普通表而非临时表：MySQL 不允许同一条语句里多次引用同一个临时表
  -- （结果统计里的 CTE 会引用两次，临时表会报 ERROR 1137 Can't reopen table）
  DROP TABLE IF EXISTS bench_log;
  CREATE TABLE bench_log (
    id        INT AUTO_INCREMENT PRIMARY KEY,
    region    VARCHAR(64),
    age       VARCHAR(64),
    gender    VARCHAR(64),
    cnt       INT,
    total_ms  DECIMAL(10,3),
    exec_ms   DECIMAL(10,3)
  );

  WHILE i < n DO
    -- 按命中数加权抽样（指数竞争法）
    SET v_r = (SELECT dim_value FROM w1 ORDER BY -LOG(RAND()) / c LIMIT 1);
    SET v_a = (SELECT dim_value FROM w2 ORDER BY -LOG(RAND()) / c LIMIT 1);
    SET v_g = (SELECT dim_value FROM w3 ORDER BY -LOG(RAND()) / c LIMIT 1);

    -- 与 AdMapper.xml#findCandidates 完全同构（categoryCode 为 null，不拼该条件）
    SET sql_txt = CONCAT(
      'SELECT COUNT(*) INTO @bench_cnt FROM (',
      'SELECT a.id, a.advertiser_id, a.title, a.creative_url, a.category_code, ',
      '       a.bid, a.daily_budget, a.status, a.created_at, a.updated_at ',
      'FROM ad a ',
      'JOIN ad_target t1 ON t1.ad_id = a.id AND t1.dim_type = 1 AND t1.dim_value = ''', v_r, ''' ',
      'JOIN ad_target t2 ON t2.ad_id = a.id AND t2.dim_type = 2 AND t2.dim_value = ''', v_a, ''' ',
      'JOIN ad_target t3 ON t3.ad_id = a.id AND t3.dim_type = 3 AND t3.dim_value = ''', v_g, ''' ',
      'WHERE a.status = 1 ',
      'ORDER BY a.bid DESC) x');

    SET t0 = NOW(6);
    SET @s = sql_txt;
    PREPARE stmt FROM @s;
    SET t1 = NOW(6);
    EXECUTE stmt;
    SET t2 = NOW(6);
    DEALLOCATE PREPARE stmt;
    SET v_cnt = @bench_cnt;

    INSERT INTO bench_log (region, age, gender, cnt, total_ms, exec_ms)
    VALUES (v_r, v_a, v_g, v_cnt,
            TIMESTAMPDIFF(MICROSECOND, t0, t2) / 1000.0,
            TIMESTAMPDIFF(MICROSECOND, t1, t2) / 1000.0);

    SET i = i + 1;
  END WHILE;
END//
DELIMITER ;

CALL bench_candidates(50);

-- ---- 结果 1：整体分布（含解析）----
WITH s AS (
  SELECT total_ms, ROW_NUMBER() OVER (ORDER BY total_ms) rn FROM bench_log
), m AS (
  SELECT COUNT(*) n FROM bench_log
)
SELECT
  (SELECT n FROM m)                                                     AS samples,
  (SELECT total_ms FROM s WHERE rn = 1)                                 AS min_ms,
  (SELECT total_ms FROM s WHERE rn = CEIL(0.50 * (SELECT n FROM m)))    AS p50_ms,
  (SELECT total_ms FROM s WHERE rn = CEIL(0.90 * (SELECT n FROM m)))    AS p90_ms,
  (SELECT total_ms FROM s WHERE rn = CEIL(0.95 * (SELECT n FROM m)))    AS p95_ms,
  (SELECT total_ms FROM s WHERE rn = CEIL(0.99 * (SELECT n FROM m)))    AS p99_ms,
  (SELECT total_ms FROM s WHERE rn = (SELECT n FROM m))                 AS max_ms,
  (SELECT ROUND(AVG(total_ms), 3) FROM bench_log)                       AS avg_ms;

-- ---- 结果 2：纯执行耗时（不含解析）----
SELECT ROUND(AVG(exec_ms), 3) AS avg_exec_ms,
       ROUND(MIN(exec_ms), 3) AS min_exec_ms,
       ROUND(MAX(exec_ms), 3) AS max_exec_ms
FROM bench_log;

-- ---- 结果 3：冷 vs 热（前 5 条 vs 其余）----
SELECT '前5条(冷)' AS phase, ROUND(AVG(total_ms), 3) AS avg_ms, ROUND(MAX(total_ms), 3) AS max_ms
  FROM bench_log WHERE id <= 5
UNION ALL
SELECT '第6条起(热)', ROUND(AVG(total_ms), 3), ROUND(MAX(total_ms), 3)
  FROM bench_log WHERE id > 5;

-- ---- 结果 4：候选集规模（顺带验证造数是否达标）----
WITH c AS (
  SELECT cnt, ROW_NUMBER() OVER (ORDER BY cnt) rn FROM bench_log
), m AS (
  SELECT COUNT(*) n FROM bench_log
)
SELECT
  (SELECT cnt FROM c WHERE rn = 1)                               AS min_cand,
  (SELECT cnt FROM c WHERE rn = CEIL(0.50 * (SELECT n FROM m)))  AS p50_cand,
  (SELECT cnt FROM c WHERE rn = CEIL(0.90 * (SELECT n FROM m)))  AS p90_cand,
  (SELECT cnt FROM c WHERE rn = (SELECT n FROM m))               AS max_cand,
  (SELECT ROUND(AVG(cnt), 1) FROM bench_log)                     AS avg_cand;

-- ---- 结果 5：最慢的 10 次，看慢在哪类请求 ----
SELECT id, region, age, gender, cnt, total_ms, exec_ms
FROM bench_log ORDER BY total_ms DESC LIMIT 10;

-- ---- 清理：bench_log 只是测量中间产物，不留在业务库里 ----
DROP TABLE IF EXISTS bench_log;
