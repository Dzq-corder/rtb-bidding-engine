-- =====================================================================
-- 测试夹具：插入少量固定广告 + 定向行，DELETE 保证幂等（可重复执行）
-- 仅 900001 / 900002 需要命中 /bid（地域 + 年龄 + 性别三维度匹配）；
-- 900010 / 900011 供 BudgetServiceTest 直接扣减使用，不要求定向行。
-- =====================================================================

DELETE FROM ad_target WHERE ad_id IN (900001, 900002, 900010, 900011);
DELETE FROM ad         WHERE id   IN (900001, 900002, 900010, 900011);

-- 清理三个消费组的落库表（保证端到端断言在重复运行时干净）
DELETE FROM billing_event   WHERE ad_id IN (900001, 900002);
DELETE FROM bid_stat_hourly WHERE ad_id IN (900001, 900002);
DELETE FROM ad_freq         WHERE ad_id IN (900001, 900002);

INSERT INTO ad (id, advertiser_id, title, creative_url, category_code, bid, daily_budget, status)
VALUES
  (900001, 900001, 'TEST_Exhaust',         'https://test/c1.jpg',  'industrial', 5.00, 0.01,   1),
  (900002, 900002, 'TEST_Winner',          'https://test/c2.jpg',  'industrial', 0.50, 100.00, 1),
  (900010, 900010, 'TEST_BudgetOK',        'https://test/c10.jpg', 'industrial', 0.10, 100.00, 1),
  (900011, 900011, 'TEST_BudgetExhaust',   'https://test/c11.jpg', 'industrial', 5.00, 0.01,   1);

-- 900001 / 900002 命中 /bid 所需的三维定向（gender=0 -> 'unknown'）
INSERT INTO ad_target (ad_id, dim_type, dim_value) VALUES
  (900001, 1, '440318'), (900001, 2, '25-30'), (900001, 3, 'unknown'),
  (900002, 1, '440318'), (900002, 2, '25-30'), (900002, 3, 'unknown');
