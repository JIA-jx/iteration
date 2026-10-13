CREATE TABLE IF NOT EXISTS `idempotent_record` (
    `id`         BIGINT       NOT NULL AUTO_INCREMENT,
    `idem_key`   VARCHAR(191) NOT NULL,
    `status`     VARCHAR(16)  NOT NULL,
    `result`     TEXT         NULL,
    `reason`     VARCHAR(512) NULL,
    `expire_at`  DATETIME     NOT NULL,
    `created_at` DATETIME     NOT NULL,
    `updated_at` DATETIME     NOT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_idem_key` (`idem_key`),
    KEY `idx_expire_at` (`expire_at`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_general_ci;

CREATE TABLE IF NOT EXISTS `audit_event` (
    `id`          BIGINT       NOT NULL AUTO_INCREMENT,
    `tenant_id`   BIGINT       NOT NULL,
    `actor_id`    BIGINT       NOT NULL,
    `actor_name`  VARCHAR(64)  NULL,
    `action`      VARCHAR(64)  NOT NULL,
    `resource`    VARCHAR(64)  NOT NULL,
    `resource_id` VARCHAR(128) NOT NULL,
    `before_data` MEDIUMTEXT   NULL,
    `after_data`  MEDIUMTEXT   NULL,
    `trace_id`    VARCHAR(64)  NULL,
    `ip`          VARCHAR(64)  NULL,
    `remark`      VARCHAR(512) NULL,
    `created_at`  DATETIME     NOT NULL,
    PRIMARY KEY (`id`),
    KEY `idx_tenant_created` (`tenant_id`, `created_at`),
    KEY `idx_action` (`action`),
    KEY `idx_resource` (`resource`, `resource_id`),
    KEY `idx_trace` (`trace_id`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_general_ci;

CREATE TABLE IF NOT EXISTS `tenant` (
    `id`               BIGINT       NOT NULL AUTO_INCREMENT,
    `code`             VARCHAR(64)  NOT NULL,
    `name`             VARCHAR(128) NOT NULL,
    `status`           TINYINT      NOT NULL DEFAULT 1,
    `member_quota`     INT          NOT NULL DEFAULT 0,
    `project_quota`    INT          NOT NULL DEFAULT 0,
    `storage_quota_mb` BIGINT       NOT NULL DEFAULT 0,
    `ai_quota_tokens`  BIGINT       NOT NULL DEFAULT 0,
    `ai_used_tokens`   BIGINT       NOT NULL DEFAULT 0,
    `expire_at`        DATETIME     NULL,
    `created_at`       DATETIME     NOT NULL,
    `updated_at`       DATETIME     NOT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_code` (`code`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_general_ci;

CREATE TABLE IF NOT EXISTS `notification` (
    `id`         BIGINT       NOT NULL AUTO_INCREMENT,
    `type`       VARCHAR(32)  NOT NULL,
    `topic`      VARCHAR(64)  NOT NULL,
    `msg_key`    VARCHAR(128) NULL,
    `payload`    TEXT         NULL,
    `status`     TINYINT      NOT NULL DEFAULT 0,
    `created_at` DATETIME     NOT NULL,
    PRIMARY KEY (`id`),
    KEY `idx_type` (`type`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_general_ci;

CREATE TABLE IF NOT EXISTS `dead_letter` (
    `id`         BIGINT       NOT NULL AUTO_INCREMENT,
    `topic`      VARCHAR(64)  NOT NULL,
    `reason`     VARCHAR(512) NULL,
    `payload`    TEXT         NULL,
    `created_at` DATETIME     NOT NULL,
    PRIMARY KEY (`id`),
    KEY `idx_topic` (`topic`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_general_ci;

