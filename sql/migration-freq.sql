-- ---------------------------------------------------------------------
-- 频控改造（frequency capping）· 存量库迁移脚本
--
-- 用途：给已有数据的 bid_log 补两列。新库直接跑 all-schema.sql 即可，无需执行本脚本。
-- 幂等：重复执行会报 Duplicate column，属预期（MySQL 8 无 ADD COLUMN IF NOT EXISTS）；
--       跑之前用 SHOW COLUMNS 确认，见文件末尾。
--
-- 对应文档：docs/频控改造方案.md §7.1
-- ---------------------------------------------------------------------

ALTER TABLE bid_log
  ADD COLUMN user_id VARCHAR(64) DEFAULT NULL
    COMMENT '用户标识（登录态 userId / 未登录设备 ID），频控维度；为空说明按 IP 频控'
    AFTER slot_code,
  ADD COLUMN freq_filtered_cnt INT NOT NULL DEFAULT 0
    COMMENT '本轮被频控剔除的候选数，>0 说明频控生效'
    AFTER match_cnt;

-- 校验（期望看到 2 行）
-- SHOW COLUMNS FROM bid_log LIKE 'user_id';
-- SHOW COLUMNS FROM bid_log LIKE 'freq_filtered_cnt';
