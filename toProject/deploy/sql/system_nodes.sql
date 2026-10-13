INSERT INTO `iteration_node` (`node`, `title`, `is_menu`, `is_auth`, `is_login`, `create_at`, `tenant_id`)
SELECT 'project/system/log', '系统日志查询', 0, 1, 0, UNIX_TIMESTAMP() * 1000, 1
WHERE NOT EXISTS (SELECT 1 FROM `iteration_node` WHERE `node` = 'project/system/log');

INSERT INTO `iteration_node` (`node`, `title`, `is_menu`, `is_auth`, `is_login`, `create_at`, `tenant_id`)
SELECT 'project/system/config', '配置热重载', 0, 1, 0, UNIX_TIMESTAMP() * 1000, 1
WHERE NOT EXISTS (SELECT 1 FROM `iteration_node` WHERE `node` = 'project/system/config');

INSERT INTO `iteration_node` (`node`, `title`, `is_menu`, `is_auth`, `is_login`, `create_at`, `tenant_id`)
SELECT 'project/system/cache', '缓存清理', 0, 1, 0, UNIX_TIMESTAMP() * 1000, 1
WHERE NOT EXISTS (SELECT 1 FROM `iteration_node` WHERE `node` = 'project/system/cache');

INSERT INTO `iteration_auth_node` (`auth`, `node`, `tenant_id`)
SELECT a.id, 'project/system/log', a.tenant_id
FROM `iteration_auth` a
WHERE a.type = 'admin'
  AND NOT EXISTS (SELECT 1 FROM `iteration_auth_node` n WHERE n.auth = a.id AND n.node = 'project/system/log');

INSERT INTO `iteration_auth_node` (`auth`, `node`, `tenant_id`)
SELECT a.id, 'project/system/config', a.tenant_id
FROM `iteration_auth` a
WHERE a.type = 'admin'
  AND NOT EXISTS (SELECT 1 FROM `iteration_auth_node` n WHERE n.auth = a.id AND n.node = 'project/system/config');

INSERT INTO `iteration_auth_node` (`auth`, `node`, `tenant_id`)
SELECT a.id, 'project/system/cache', a.tenant_id
FROM `iteration_auth` a
WHERE a.type = 'admin'
  AND NOT EXISTS (SELECT 1 FROM `iteration_auth_node` n WHERE n.auth = a.id AND n.node = 'project/system/cache');
