-- 测试清理：删除夹具数据，避免污染后续运行
DELETE FROM ad_target WHERE ad_id IN (900001, 900002, 900010, 900011);
DELETE FROM ad         WHERE id   IN (900001, 900002, 900010, 900011);

-- 清理三个消费组的落库表
DELETE FROM billing_event   WHERE ad_id IN (900001, 900002);
DELETE FROM bid_stat_hourly WHERE ad_id IN (900001, 900002);
DELETE FROM ad_freq         WHERE ad_id IN (900001, 900002);
