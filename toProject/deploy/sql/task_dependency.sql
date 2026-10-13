CREATE TABLE IF NOT EXISTS `task_dependency` (
    `id`            BIGINT       NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    `project_code`  BIGINT       NOT NULL DEFAULT 0      COMMENT '所属项目',
    `task_code`     BIGINT       NOT NULL DEFAULT 0      COMMENT '后继任务（依赖方）',
    `pre_task_code` BIGINT       NOT NULL DEFAULT 0      COMMENT '前驱任务（被依赖方）',
    `type`          VARCHAR(8)   NOT NULL DEFAULT 'FS'   COMMENT '依赖类型 FS/SS，FF/SF 预留',
    `lag`           BIGINT       NOT NULL DEFAULT 0      COMMENT '滞后量（毫秒，可为负）',
    `tenant_id`     BIGINT       NOT NULL DEFAULT 0      COMMENT '租户标识',
    `created_at`    DATETIME     NOT NULL                COMMENT '创建时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_project_task_pre_type` (`project_code`, `task_code`, `pre_task_code`, `type`),
    KEY `idx_project` (`project_code`),
    KEY `idx_task` (`task_code`),
    KEY `idx_pre_task` (`pre_task_code`),
    KEY `idx_tenant_project` (`tenant_id`, `project_code`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_general_ci COMMENT = '任务依赖关系（CPM 关键路径与依赖自动排期）';
