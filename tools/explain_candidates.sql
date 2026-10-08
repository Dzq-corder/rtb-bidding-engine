-- 复现 BidService.findCandidates 的执行计划（与 JMeter 默认参数一致）
-- region=1101(原样透传) / ageBucket=25-30 / gender=1->male(服务端翻译) / category=mobile
EXPLAIN
SELECT a.id, a.advertiser_id, a.title, a.creative_url, a.category_code,
       a.bid, a.daily_budget, a.status, a.created_at, a.updated_at
FROM ad a
JOIN ad_target t1 ON t1.ad_id = a.id AND t1.dim_type = 1 AND t1.dim_value = '1101'
JOIN ad_target t2 ON t2.ad_id = a.id AND t2.dim_type = 2 AND t2.dim_value = '25-30'
JOIN ad_target t3 ON t3.ad_id = a.id AND t3.dim_type = 3 AND t3.dim_value = 'male'
WHERE a.status = 1 AND a.category_code = 'mobile'
ORDER BY a.bid DESC;
