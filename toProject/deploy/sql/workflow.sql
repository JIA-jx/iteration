CREATE TABLE IF NOT EXISTS `workflow_definition` (
    `id`              BIGINT       NOT NULL AUTO_INCREMENT,
    `tenant_id`       BIGINT       NOT NULL DEFAULT 1,
    `def_key`         VARCHAR(64)  NOT NULL,
    `name`            VARCHAR(128) NOT NULL,
    `version`         INT          NOT NULL,
    `definition_json` MEDIUMTEXT   NOT NULL,
    `status`          TINYINT      NOT NULL DEFAULT 1,
    `remark`          VARCHAR(512) NULL,
    `created_at`      DATETIME     NOT NULL,
    `updated_at`      DATETIME     NOT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_tenant_key_version` (`tenant_id`, `def_key`, `version`),
    KEY `idx_tenant_key` (`tenant_id`, `def_key`, `status`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_general_ci;

CREATE TABLE IF NOT EXISTS `workflow_instance` (
    `id`                 VARCHAR(64)  NOT NULL,
    `tenant_id`          BIGINT       NOT NULL DEFAULT 1,
    `definition_key`     VARCHAR(64)  NOT NULL,
    `definition_version` INT          NOT NULL,
    `biz_id`             VARCHAR(128) NOT NULL,
    `submitter_id`       BIGINT       NOT NULL,
    `status`             VARCHAR(16)  NOT NULL,
    `current_key`        VARCHAR(64)  NULL,
    `instance_json`      MEDIUMTEXT   NOT NULL,
    `version`            BIGINT       NOT NULL DEFAULT 1,
    `created_at`         DATETIME     NOT NULL,
    `updated_at`         DATETIME     NOT NULL,
    `finished_at`        DATETIME     NULL,
    PRIMARY KEY (`id`),
    KEY `idx_tenant_biz` (`tenant_id`, `biz_id`),
    KEY `idx_status` (`status`),
    KEY `idx_submitter` (`submitter_id`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_general_ci;

CREATE TABLE IF NOT EXISTS `workflow_task` (
    `id`          VARCHAR(64)  NOT NULL,
    `instance_id` VARCHAR(64)  NOT NULL,
    `tenant_id`   BIGINT       NOT NULL DEFAULT 1,
    `node_key`    VARCHAR(64)  NOT NULL,
    `node_name`   VARCHAR(128) NULL,
    `approver_id` BIGINT       NOT NULL,
    `status`      VARCHAR(16)  NOT NULL,
    `comment`     VARCHAR(512) NULL,
    `created_at`  DATETIME     NOT NULL,
    `handled_at`  DATETIME     NULL,
    `timeout_at`  DATETIME     NULL,
    PRIMARY KEY (`id`),
    KEY `idx_instance` (`instance_id`),
    KEY `idx_approver` (`tenant_id`, `approver_id`, `status`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_general_ci;

CREATE TABLE IF NOT EXISTS `workflow_definition_archive` LIKE `workflow_definition`;
CREATE TABLE IF NOT EXISTS `workflow_instance_archive` LIKE `workflow_instance`;
CREATE TABLE IF NOT EXISTS `workflow_task_archive` LIKE `workflow_task`;
