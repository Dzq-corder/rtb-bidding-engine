#!/usr/bin/env bash
# =====================================================================
# M8 压测辅助：排空 Kafka 积压后，在同一基线状态下跑 N 轮竞价压测
# =====================================================================
# 存在理由（这是 M7 踩过的坑，必须固化成脚本而不是靠人记）：
#   billing-recon 消费组每条事件要查三次 MySQL，
#   只要有积压，整个测量过程都在和被测查询抢数据库，
#   测出来的 QPS 会莫名其妙掉一大截。
#   M7 就出现过「B 组 QPS 从 977 掉到 547」的假故障。
#
# 用法：bash scripts/m8_drain_and_bench.sh [轮数] [每轮请求数] [并发]
# =====================================================================
set -u
BASE=http://127.0.0.1:8080
ROUNDS=${1:-3}
COUNT=${2:-5000}
CONC=${3:-20}

drain() {
  for i in $(seq 1 120); do
    LAG=$(curl -s --noproxy '*' "$BASE/kafka/lag" \
          | sed -n 's/.*"billing-recon":{"totalLag":\([0-9]*\).*/\1/p')
    if [ "${LAG:-999}" = "0" ]; then
      echo "  [drain] 积压已排空（第 $i 次轮询）"
      return 0
    fi
    sleep 2
  done
  echo "  [drain] 警告：120 次轮询后仍有积压 ${LAG:-unknown}，继续测量但结果不可信"
}

for r in $(seq 1 "$ROUNDS"); do
  echo "=== 第 $r/$ROUNDS 轮 ==="
  drain
  curl -s --noproxy '*' -X POST "$BASE/logbuf/reset" > /dev/null
  curl -s --noproxy '*' -X POST "$BASE/bid/batch?count=$COUNT&concurrency=$CONC"
  echo
  curl -s --noproxy '*' "$BASE/logbuf/stats"
  echo
done
