# =====================================================================
# RTB 投放引擎 · M0 种子数据导入
# =====================================================================
# 用法：
#   python tools/gen_seed_data.py --out data
#   bash tools/import_seed_data.sh
#
# 写入方案性能对照（1000 万行级别）：
#   单条 INSERT 循环                 半小时以上   ❌ 别这么干
#   批量 INSERT 5000 行/批            10-20 分钟   可用
#   LOAD DATA INFILE + CSV             5-15 分钟   ✅ 本脚本采用
#
# 为什么用挂载卷而不是 docker cp：
#   MySQL 8 官方镜像的 ENTRYPOINT 会重建 /tmp，docker cp 进去的 CSV
#   会被清掉，表现为命令报 "Copied" 但 ls 看不到文件。挂载卷没有这个问题。
#
# 走服务端 LOAD DATA INFILE（不是 LOCAL）：路径固定为容器内 /data/xxx.csv，
#   不依赖 local_infile 客户端开关，也避开 Windows 宿主路径与容器路径不一致。
# =====================================================================

set -euo pipefail

# ---------------------------------------------------------------
# Windows + Git Bash 注意事项
# ---------------------------------------------------------------
# MSYS 会把形如 /data/xxx 的参数自动转换成宿主 Windows 路径，
# 导致 docker compose exec 收到 "C:/Program Files/.../data" 这种参数。
# 解决：设置 MSYS_NO_PATHCONV=1 关闭转换。详见 import 脚本内的注释。
export MSYS_NO_PATHCONV=1

DATA_DIR="${DATA_DIR:-data}"
MYSQL_SERVICE="${MYSQL_SERVICE:-mysql}"

echo "=============================================="
echo " RTB 种子数据导入"
echo "=============================================="
echo " 数据目录: $DATA_DIR"
echo " 服务名  : $MYSQL_SERVICE"
echo

for f in ad.csv ad_target.csv; do
  if [ ! -f "$DATA_DIR/$f" ]; then
    echo "找不到 $DATA_DIR/$f"
    echo "请先运行：python tools/gen_seed_data.py --out $DATA_DIR"
    exit 1
  fi
done

# ---------------------------------------------------------------
# 1. 确认 CSV 在容器内可见（走挂载卷）
# ---------------------------------------------------------------
echo "[1/4] 确认容器内可见 CSV..."
for f in ad.csv ad_target.csv; do
  lines=$(docker compose exec -T "$MYSQL_SERVICE" wc -l "/data/$f" 2>/dev/null | tr -d '\r' | awk '{print $1}')
  if [ -z "$lines" ]; then
    echo "  容器内看不到 /data/$f"
    echo "  请确认 docker-compose.yml 里 mysql 服务挂载了 ./data:/data:ro"
    echo "  修改后需要：docker compose up -d --force-recreate $MYSQL_SERVICE"
    exit 1
  fi
  echo "  /data/$f  OK ($lines 行)"
done
echo

# ---------------------------------------------------------------
# 2. 清空目标表
# ---------------------------------------------------------------
echo "[2/4] 清空目标表..."
docker compose exec -T "$MYSQL_SERVICE" mysql -u root rtb <<'SQL'
TRUNCATE TABLE ad;
TRUNCATE TABLE ad_target;
SQL
echo "      完成"
echo

# ---------------------------------------------------------------
# 3. 导入（LOAD DATA LOCAL INFILE）
#
# 为什么用 LOCAL 而不是服务端 INFILE：
#   服务端 INFILE 受 secure-file-priv 限制，MySQL 8 默认只允许读
#   /var/lib/mysql-files，而CSV 挂在 /data，会报
#   ERROR 1290: running with the --secure-file-priv option。
#   LOCAL 变体由客户端读文件再经协议传给服务端，不受该限制。
#   compose 的 mysql 服务已开 --local-infile=1，客户端也需加 --local-infile=1。
# ---------------------------------------------------------------
echo "[3/4] 导入数据（LOAD DATA LOCAL INFILE）..."
docker compose exec -T "$MYSQL_SERVICE" mysql -u root --local-infile=1 rtb <<'SQL'
SET SESSION unique_checks = 0;
SET SESSION foreign_key_checks = 0;
SET SESSION autocommit = 0;

LOAD DATA LOCAL INFILE '/data/ad.csv'
INTO TABLE ad
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(id, advertiser_id, title, creative_url, category_code, bid, daily_budget, status, created_at, updated_at);

LOAD DATA LOCAL INFILE '/data/ad_target.csv'
INTO TABLE ad_target
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(id, ad_id, dim_type, dim_value);

COMMIT;
SQL
echo "      完成"
echo

# ---------------------------------------------------------------
# 4. 验证
# ---------------------------------------------------------------
echo "=============================================="
echo " 导入结果"
echo "=============================================="
docker compose exec -T "$MYSQL_SERVICE" mysql -u root rtb <<'SQL'
SELECT '广告 ad'             AS 表, COUNT(*) AS 行数 FROM ad
UNION ALL
SELECT '定向倒排 ad_target',  COUNT(*) FROM ad_target;

SELECT
  ROUND(AVG(bid), 2) AS 平均出价,
  MIN(bid)           AS 最低出价,
  MAX(bid)           AS 最高出价,
  ROUND(AVG(daily_budget), 2) AS 平均日预算
FROM ad;

SELECT status AS 广告状态, COUNT(*) AS 数量,
       ROUND(COUNT(*)*100.0/(SELECT COUNT(*) FROM ad),1) AS 占比
FROM ad GROUP BY status ORDER BY status;

SELECT dim_type AS 定向维度, COUNT(*) AS 行数
FROM ad_target GROUP BY dim_type ORDER BY dim_type;
SQL

echo
echo "完成。下一步：起 Spring Boot 骨架，跑通第一次真实竞价。"
