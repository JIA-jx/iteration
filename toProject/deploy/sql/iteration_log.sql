CREATE TABLE IF NOT EXISTS `iteration_log` (
    `id`             BIGINT       NOT NULL AUTO_INCREMENT,
    `member_code`    BIGINT       NOT NULL DEFAULT 0 COMMENT '操作人 member id',
    `content`        TEXT         NULL                COMMENT '日志正文',
    `remark`         TEXT         NULL                COMMENT '备注',
    `type`           VARCHAR(255) NOT NULL DEFAULT 'create' COMMENT '日志类型',
    `create_time`    BIGINT       NULL                COMMENT '创建时间（毫秒时间戳）',
    `source_code`    BIGINT       NOT NULL DEFAULT 0  COMMENT '来源对象 id（任务 id 等）',
    `action_type`    VARCHAR(30)  NULL                COMMENT '动作类型',
    `to_member_code` BIGINT       NOT NULL DEFAULT 0  COMMENT '目标成员',
    `is_comment`     TINYINT(1)   NOT NULL DEFAULT 0  COMMENT '是否评论：0 否',
    `project_code`   BIGINT       NULL                COMMENT '所属项目',
    `icon`           VARCHAR(20)  NULL                COMMENT '图标',
    `is_robot`       TINYINT(1)   NOT NULL DEFAULT 0  COMMENT '是否机器人产生',
    `tenant_id`      BIGINT       NOT NULL DEFAULT 1  COMMENT '租户ID',
    PRIMARY KEY (`id`),
    KEY `idx_member_code` (`member_code`),
    KEY `idx_source_code` (`source_code`),
    KEY `idx_create_time` (`create_time`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_general_ci COMMENT = '项目日志表';
