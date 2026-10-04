#!/usr/bin/env bash
# =====================================================================
# RTB 投放引擎 · 竞价明细导入（1000 万行）
# =====================================================================
# 用法：
#   bash tools/import_bid_log.sh
#
# 写入方案性能（1000 万行）：
#   单条 INSERT 循环         半小时以上❌
#   批量 INSERT 5000 行/批   10-20 分钟   可用
#   LOAD DATA LOCAL INFILE   5-15 分钟    ✅ 本脚本采用
#
# 走客户端 LOCAL 变体：服务端 INFILE 受 secure-file-priv 限制，
# 而 CSV 挂在 /data，会报 ERROR 1290。
#
# 前置：tools/amplify_bid_log.py 已生成 data/bid_log_10m.csv

set -euo pipefail

# Git Bash 会把 /data/xxx 转换成 Windows 路径，必须关闭转换
export MSYS_NO_PATHCONV=1

DATA_DIR="${DATA_DIR:-data}"
CSV="${CSV:-bid_log_10m.csv}"
MYSQL_SERVICE="${MYSQL_SERVICE:-mysql}"

echo "=============================================="
echo " 竞价明细导入"
echo "=============================================="
echo " CSV: $DATA_DIR/$CSV"
echo

if [ ! -f "$DATA_DIR/$CSV" ]; then
  echo "找不到 $DATA_DIR/$CSV"
  echo "请先运行："
  echo "  python tools/amplify_bid_log.py --sample $DATA_DIR/sample_bid_log.csv \\"
  echo "         --out $DATA_DIR/$CSV --target 10000000"
  exit 1
fi

# ---------------------------------------------------------------
# 1. 确认容器内可见 + 统计行数
# ---------------------------------------------------------------
echo "[1/3] 确认 CSV..."
LINES=$(docker compose exec -T "$MYSQL_SERVICE" wc -l "/data/$CSV" 2>/dev/null | tr -d '\r' | awk '{print $1}')
if [ -z "$LINES" ]; then
  echo "  容器内看不到 /data/$CSV"
  exit 1
fi
echo "  $LINES 行（含表头）"
echo

# ---------------------------------------------------------------
# 2. 清空表
# ---------------------------------------------------------------
echo "[2/3] 清空 bid_log..."
docker compose exec -T "$MYSQL_SERVICE" mysql -u root rtb -e "TRUNCATE TABLE bid_log;"
echo "      完成"
echo

# ---------------------------------------------------------------
# 3. 导入
# ---------------------------------------------------------------
echo "[3/3] 导入（LOAD DATA LOCAL INFILE）..."
START=$(date +%s)
docker compose exec -T "$MYSQL_SERVICE" mysql -u root --local-infile=1 rtb <<SQL
SET SESSION unique_checks = 0;
SET SESSION foreign_key_checks = 0;
SET SESSION autocommit = 0;

LOAD DATA LOCAL INFILE '/data/$CSV'
INTO TABLE bid_log
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(request_id, ad_id, advertiser_id, slot_code,
 region, age, gender, interests,
 candidate_cnt, match_cnt, has_ad, win_bid, pay_price,
 cost_ms, candidate_cost, filter_cost, sort_cost, bill_cost,
 bid_at);

COMMIT;
SQL
END=$(date +%s)
echo "      完成，耗时 $((END - START)) 秒"
echo

# ---------------------------------------------------------------
# 4. 验证
# ---------------------------------------------------------------
echo "=============================================="
echo " 导入结果"
echo "=============================================="
docker compose exec -T "$MYSQL_SERVICE" mysql -u root rtb <<'SQL'
SELECT COUNT(*) AS 总行数,
       COUNT(DISTINCT request_id) AS 唯一请求数,
       ROUND(AVG(candidate_cnt),1) AS 平均候选,
       ROUND(SUM(has_ad)*100.0/COUNT(*),1) AS 填充率,
       ROUND(AVG(cost_ms),1) AS 平均耗时
FROM bid_log;

SELECT '时间跨度' AS 检查,
       MIN(bid_at) AS 最早,
       MAX(bid_at) AS 最晚,
       COUNT(DISTINCT DATE(bid_at)) AS 天数
FROM bid_log;

SELECT '分区裁剪前提：时间必须铺满多个日期' AS 说明;
SELECT DATE(bid_at) AS 日期, COUNT(*) AS 行数
FROM bid_log GROUP BY DATE(bid_at) ORDER BY 日期 LIMIT 5;
SQL

echo
echo "下一步：git tag 已打的 m1-baseline 是未加索引/未分区的版本，"
echo "可以用它做 M8 四轮调优的『优化前』对照。"
