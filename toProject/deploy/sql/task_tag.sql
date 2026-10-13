CREATE TABLE IF NOT EXISTS `iteration_task_tag` (
    `id`           BIGINT      NOT NULL AUTO_INCREMENT,
    `project_code` BIGINT      NOT NULL DEFAULT 0 COMMENT '所属项目',
    `name`         VARCHAR(50) NOT NULL COMMENT '标签名',
    `color`        VARCHAR(20) NULL     COMMENT '标签颜色',
    `sort`         INT         NOT NULL DEFAULT 0,
    `create_by`    BIGINT      NOT NULL DEFAULT 0,
    `create_time`  BIGINT      NOT NULL DEFAULT 0 COMMENT '毫秒时间戳',
    `tenant_id`    BIGINT      NOT NULL DEFAULT 1 COMMENT '租户ID',
    PRIMARY KEY (`id`),
    KEY `idx_project` (`project_code`),
    KEY `idx_tenant_id` (`tenant_id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_general_ci COMMENT = '任务标签';
