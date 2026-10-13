CREATE TABLE IF NOT EXISTS `outbox_event` (
    `id`            BIGINT       NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    `topic`         VARCHAR(64)  NOT NULL                COMMENT '目标 Kafka 主题',
    `msg_key`       VARCHAR(128) NULL                    COMMENT '消息 key（用于分区有序）',
    `payload`       MEDIUMBLOB   NOT NULL                COMMENT '消息体 JSON',
    `status`        TINYINT      NOT NULL DEFAULT 0      COMMENT '0待投递/1已投递/2重试耗尽/3投递中',
    `retry_cnt`     INT          NOT NULL DEFAULT 0      COMMENT '已投递失败次数',
    `locked_by`     VARCHAR(64)  NULL                    COMMENT '领取者标识（实例ID-序号）',
    `locked_at`     DATETIME     NULL                    COMMENT '领取时刻，用于锁超时回收',
    `next_retry_at` DATETIME     NULL                    COMMENT '下次可投递时间（退避）',
    `last_error`    VARCHAR(512) NULL                    COMMENT '最后一次失败原因',
    `created_at`    DATETIME     NOT NULL                COMMENT '创建时间',
    `sent_at`       DATETIME     NULL                    COMMENT '投递成功时间',
    PRIMARY KEY (`id`),
    KEY `idx_status_next_retry` (`status`, `next_retry_at`, `id`),
    KEY `idx_locked_at` (`locked_at`),
    KEY `idx_topic_status` (`topic`, `status`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_general_ci COMMENT = '事务性发件箱';
