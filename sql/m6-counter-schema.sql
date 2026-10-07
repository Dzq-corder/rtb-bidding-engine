-- M6 旁路消费落库表结构
-- 执行方式（手动，MySQL 客户端或 IDE）：
--   mysql -h127.0.0.1 -uroot rtb < sql/m6-counter-schema.sql
-- 注意：rtb 数据库已存在（造数脚本创建），此处只是补建 M6 所需表。

-- 1) 幂等去重表：每个消费组对每条事件只处理一次
--    INSERT IGNORE 在 (event_id, grp) 已存在时返回 0 影响行数 → 消费者据此跳过
CREATE TABLE IF NOT EXISTS processed_event (
    event_id VARCHAR(64) NOT NULL,
    grp      VARCHAR(32) NOT NULL,
    PRIMARY KEY (event_id, grp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='M6 消费幂等去重';

-- 2) 计费对账表：每条竞价事件的实扣金额（唯一键 event_id 防重复记账）
CREATE TABLE IF NOT EXISTS billing_event (
    event_id       VARCHAR(64) NOT NULL,
    ad_id          BIGINT      NULL,
    advertiser_id  INT         NULL,
    pay_price      DECIMAL(12,4) NULL,
    ts             BIGINT      NULL,
    PRIMARY KEY (event_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='M6 计费对账明细';

-- 3) 小时级预聚合：大盘统计数据源（避免每次扫全量明细）
CREATE TABLE IF NOT EXISTS bid_stat_hourly (
    stat_hour    VARCHAR(16) NOT NULL,
    ad_id        BIGINT      NULL,
    impressions  INT     NOT NULL DEFAULT 0,
    spend        DECIMAL(14,4) NOT NULL DEFAULT 0,
    PRIMARY KEY (stat_hour, ad_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='M6 小时级曝光/消耗聚合';

-- 4) 频次反作弊（维度简化：m1-explore 线无 userId，按 ad_id 计数）
CREATE TABLE IF NOT EXISTS ad_freq (
    ad_id  BIGINT NOT NULL,
    cnt    INT    NOT NULL DEFAULT 0,
    PRIMARY KEY (ad_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='M6 广告曝光频次（反作弊用）';
