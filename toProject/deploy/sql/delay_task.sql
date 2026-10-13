-- ============================================================================
-- 延时任务台账表
--
-- 背景：延时任务的「何时触发」由 Redis ZSet + Lua 原子领取驱动（见 others/delay），
-- 本表只是台账，用于：
--   1. 审计：谁在什么时候排了什么任务、最终执行结果如何；
--   2. 排障：按 member_id / project_code / type / status 检索，Redis 的 ZSet/Hash
--      做不到这种维度的查询；
--   3. 自愈：Redis 未开持久化或被清库后，靠本表 + 任务表把提醒重建回来
--      （POST /iteration/delay/rebuild）。
--
-- 幂等：可重复执行。
-- ============================================================================

CREATE TABLE IF NOT EXISTS `delay_task` (
    `id`           BIGINT       NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    `task_id`      VARCHAR(64)  NOT NULL                COMMENT '延时任务 ID（业务推导，如 task_remind:123）',
    `type`         VARCHAR(64)  NOT NULL                COMMENT '任务类型 task_remind / invite_expire',
    `payload`      TEXT         NULL                    COMMENT '任务负载 JSON',
    `run_at`       BIGINT       NOT NULL                COMMENT '计划触发时间（毫秒时间戳）',
    `status`       VARCHAR(16)  NOT NULL DEFAULT 'pending' COMMENT 'pending/inflight/done/canceled/failed',
    `attempts`     INT          NOT NULL DEFAULT 0      COMMENT '已执行次数',
    `member_id`    BIGINT       NOT NULL DEFAULT 0      COMMENT '关联成员（提醒接收人/邀请人）',
    `project_code` BIGINT       NOT NULL DEFAULT 0      COMMENT '关联项目',
    `last_error`   VARCHAR(512) NULL                    COMMENT '最后一次失败原因',
    `created_at`   DATETIME     NOT NULL                COMMENT '创建时间',
    `updated_at`   DATETIME     NOT NULL                COMMENT '更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_task_id` (`task_id`),
    KEY `idx_type_status` (`type`, `status`),
    KEY `idx_run_at` (`run_at`),
    KEY `idx_member` (`member_id`),
    KEY `idx_project` (`project_code`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_general_ci COMMENT = '延时任务台账';
