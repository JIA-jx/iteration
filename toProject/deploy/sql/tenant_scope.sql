DROP PROCEDURE IF EXISTS iteration_add_tenant_id;

DELIMITER $$
CREATE PROCEDURE iteration_add_tenant_id()
BEGIN
    DECLARE tbl VARCHAR(64);
    DECLARE done INT DEFAULT 0;
    DECLARE cur CURSOR FOR
        SELECT t.table_name
        FROM information_schema.tables t
        WHERE t.table_schema = DATABASE()
          AND t.table_type = 'BASE TABLE'
          AND t.table_name IN (
              'iteration', 'iteration_auth', 'iteration_auth_node', 'iteration_collection',
              'iteration_department', 'iteration_file', 'iteration_log', 'iteration_member_account',
              'iteration_member_task_stat', 'iteration_menu', 'iteration_minio_file', 'iteration_node',
              'iteration_project_member', 'iteration_source_link', 'iteration_task', 
              'iteration_task_member', 'iteration_task_stages', 'iteration_task_tag', 'iteration_task_stages_template',
              'iteration_task_work_time', 'iteration_template', 'task_dependency',
              'iteration_member', 'iteration_organization',
              'iteration_log_archive'
          )
          AND NOT EXISTS (
              SELECT 1 FROM information_schema.columns c
              WHERE c.table_schema = t.table_schema
                AND c.table_name = t.table_name
                AND c.column_name = 'tenant_id'
          );
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

    OPEN cur;
    read_loop: LOOP
        FETCH cur INTO tbl;
        IF done = 1 THEN
            LEAVE read_loop;
        END IF;

        SET @ddl = CONCAT(
            'ALTER TABLE `', tbl, '`',
            ' ADD COLUMN `tenant_id` BIGINT NOT NULL DEFAULT 1 COMMENT ''租户ID'',',
            ' ADD KEY `idx_tenant_id` (`tenant_id`)'
        );
        PREPARE stmt FROM @ddl;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END LOOP;
    CLOSE cur;
END$$
DELIMITER ;

CALL iteration_add_tenant_id();
DROP PROCEDURE iteration_add_tenant_id;

UPDATE `task_dependency` SET `tenant_id` = 1 WHERE `tenant_id` = 0;
