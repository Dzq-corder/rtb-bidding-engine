# RTB 实时竞价广告投放引擎

> 一个面向面试作品集的**纯后端**实时竞价（RTB）广告投放引擎演示项目：在严格的毫秒级内完成「候选检索 → 预算扣减 → 二价计费 → 明细落库」，并把计费事件通过 Kafka 旁路投递给统计 / 对账 / 反作弊消费组。
>


---

## 一、技术栈

| 分类 | 选型 |
|---|---|
| 语言 / 框架 | Java 17 · Spring Boot 3.2.5 |
| 数据库 | MySQL 8.0 |
| 缓存 | Redis 7（预算余额 + Lua 原子扣减） |
| 消息 | Kafka 3 单节点 KRaft（`acks=all` + 幂等生产者） |
| 构建 | Maven |


---

## 二、架构总览

```
                       ┌──────────── 主链路（同步，SLA 总预算 1000ms）────────────┐
   流量方 ──POST /bid──► BidController
                              │
                              ▼
                         BidService
        ┌─────────────────────┼───────────────────────────────────┐
        ▼                     ▼                                    ▼
 ① 候选检索           ② 预算扣减(Redis)                   ③ 二价计费(Vickrey)
   ad JOIN ad_target     Lua 原子扣减 daily_budget           胜出者 = 首个预算充足候选
   idx_dim_lookup        超时/失败 → 兜底                    成交价 = min(胜出价, 次高价+最小加价)
        │                     │                                    │
        └─────────┬───────────┴────────────────────────────────────┘
                  ▼
            返回竞价结果（hasAd / winner / payPrice / cost）
                  │
        ┌─────────┴──────────────────────────────────────┐
        ▼                                               ▼
  enqueue → BidDetailBuffer                       publish → Kafka topic: bid_event
  （本地有界队列，异步批量落 bid_log）            （acks=all，不可丢）
                                                        │
                                                        ▼
                  ┌──────── Kafka 旁路消费组（M6：至少一次 + 幂等去重）────────┐
                  ▼                          ▼                            ▼
           hourly-preagg            billing-recon               freq-antifraud
           bid_stat_hourly          billing_event               ad_freq
           （小时级聚合）            （计费流水，不可丢）         （频次反作弊）
           共用 processed_event 做幂等去重（INSERT IGNORE (event_id, grp)）
```

**数据分级（本项目最核心的取舍）**
- **计费事件**走 Kafka，至少一次投递 + `processed_event` 幂等去重，**不可丢**。
- **竞价明细**走本地有界队列（`BidDetailBuffer`），可丢、可采样，**绝不阻塞主链路**。

---

## 三、快速开始

### 前置
- Docker Desktop（建议 ≥ 8GB 内存：MySQL 装量 + Kafka + 宿主 JVM 同时跑）
- JDK 17、Maven 3.9+

### 1. 启动中间件
```bash
cd <项目根目录>
docker compose up -d
docker compose ps      # 三个容器 healthy 即可
```
> 默认 `docker-compose.yml` 使用 `rtb2-` 前缀（容器 `rtb2-mysql` / `rtb2-redis` / `rtb2-kafka`，卷 `mysql-data-rtb2` / `kafka-data-rtb2`），避免与你机器上已有的 `rtb` 实例端口冲突。

### 2. 建表 + 造数
```bash
docker exec -i rtb2-mysql mysql -uroot rtb < sql/all-schema.sql
docker exec -i rtb2-mysql mysql -uroot rtb < sql/seed-demo.sql
```
- `all-schema.sql`：一次性建好全部 7 张表（含 `idx_dim_lookup` 关键索引），幂等可重跑。
- `seed-demo.sql`：灌入 200 条广告 + 800 条定向条件，定向组合全覆盖，保证演示不会空广告。

### 3. 启动应用
```bash
mvn spring-boot:run
# 或：java -jar target/rtb-engine-0.1.0-M0.jar
```

### 4. 验证
```bash
curl http://127.0.0.1:8080/actuator/health
curl -X POST http://127.0.0.1:8080/bid -H "Content-Type: application/json" \
  -d '{"slotCode":"slot-01","region":"110000","age":28,"gender":1,"categoryCode":"travel"}'
```

---

## 四、核心设计亮点（里程碑）

-  · 候选检索 **：
-  · 二价竞价（Vickrey）**：成交价 = min(胜出者出价, 次高出价 + 最小加价)，对广告主更友好。
-  · 超时 + 独立熔断 + 兜底**：总预算 1000ms（DB 800ms / Redis 200ms）；DB、Redis 各自独立熔断器（连续失败 5 次 OPEN，冷却 5s 后自愈）；任何依赖故障统一收敛到「兜底广告」，绝不返回空。
-  · Kafka 旁路三消费组**：`hourly-preagg` / `billing-recon` / `freq-antifraud` 分别做聚合、计费流水、反作弊；手动提交位点 + `processed_event` 幂等去重，保证「至少一次 + 最终只生效一次」。
- · 竞价明细异步批量落库**：`BidDetailBuffer` 使用本地有界队列（容量 1 万，攒 500 条 **或** 200ms 触发一次 `batchUpdate`）。



---

## 五、License

本项目采用 **MIT License**。详见仓库根目录的 [LICENSE](LICENSE) 文件。

Copyright (c) 2026 Dzq-corder。
