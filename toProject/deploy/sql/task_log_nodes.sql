INSERT INTO `iteration_node` (`node`, `title`, `is_menu`, `is_auth`, `is_login`, `create_at`, `tenant_id`)
SELECT 'project/task_log', '任务日志管理', 0, 1, 1, UNIX_TIMESTAMP() * 1000, 1
WHERE NOT EXISTS (SELECT 1 FROM `iteration_node` WHERE `node` = 'project/task_log');

INSERT INTO `iteration_node` (`node`, `title`, `is_menu`, `is_auth`, `is_login`, `create_at`, `tenant_id`)
SELECT 'project/task_log/index', '任务日志列表', 0, 0, 1, UNIX_TIMESTAMP() * 1000, 1
WHERE NOT EXISTS (SELECT 1 FROM `iteration_node` WHERE `node` = 'project/task_log/index');

INSERT INTO `iteration_node` (`node`, `title`, `is_menu`, `is_auth`, `is_login`, `create_at`, `tenant_id`)
SELECT 'project/task_log/getlistbyselfproject', '个人项目日志列表', 0, 0, 1, UNIX_TIMESTAMP() * 1000, 1
WHERE NOT EXISTS (SELECT 1 FROM `iteration_node` WHERE `node` = 'project/task_log/getlistbyselfproject');

INSERT INTO `iteration_auth_node` (`auth`, `node`, `tenant_id`)
SELECT a.id, n.node, a.tenant_id
FROM `iteration_auth` a
         JOIN (SELECT 'project/task_log' AS node
               UNION ALL SELECT 'project/task_log/index'
               UNION ALL SELECT 'project/task_log/getlistbyselfproject') n
WHERE a.type = 'admin'
  AND NOT EXISTS (SELECT 1 FROM `iteration_auth_node` x WHERE x.auth = a.id AND x.node = n.node);
