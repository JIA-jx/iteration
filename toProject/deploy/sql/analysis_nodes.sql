INSERT INTO `iteration_node` (`node`, `title`, `is_menu`, `is_auth`, `is_login`, `create_at`, `tenant_id`)
SELECT 'project/analysis', '效能分析', 0, 1, 1, UNIX_TIMESTAMP() * 1000, 1
WHERE NOT EXISTS (SELECT 1 FROM `iteration_node` WHERE `node` = 'project/analysis');

INSERT INTO `iteration_node` (`node`, `title`, `is_menu`, `is_auth`, `is_login`, `create_at`, `tenant_id`)
SELECT 'project/analysis/delivery', '交付率统计', 0, 1, 1, UNIX_TIMESTAMP() * 1000, 1
WHERE NOT EXISTS (SELECT 1 FROM `iteration_node` WHERE `node` = 'project/analysis/delivery');

INSERT INTO `iteration_node` (`node`, `title`, `is_menu`, `is_auth`, `is_login`, `create_at`, `tenant_id`)
SELECT 'project/analysis/burndown', '燃尽图', 0, 1, 1, UNIX_TIMESTAMP() * 1000, 1
WHERE NOT EXISTS (SELECT 1 FROM `iteration_node` WHERE `node` = 'project/analysis/burndown');

INSERT INTO `iteration_node` (`node`, `title`, `is_menu`, `is_auth`, `is_login`, `create_at`, `tenant_id`)
SELECT 'project/analysis/teamload', '团队负载', 0, 1, 1, UNIX_TIMESTAMP() * 1000, 1
WHERE NOT EXISTS (SELECT 1 FROM `iteration_node` WHERE `node` = 'project/analysis/teamload');

INSERT INTO `iteration_node` (`node`, `title`, `is_menu`, `is_auth`, `is_login`, `create_at`, `tenant_id`)
SELECT 'project/analysis/airecommend', 'AI 任务建议', 0, 1, 1, UNIX_TIMESTAMP() * 1000, 1
WHERE NOT EXISTS (SELECT 1 FROM `iteration_node` WHERE `node` = 'project/analysis/airecommend');

INSERT INTO `iteration_node` (`node`, `title`, `is_menu`, `is_auth`, `is_login`, `create_at`, `tenant_id`)
SELECT 'project/task/worktimelist', '任务工时列表', 0, 1, 1, UNIX_TIMESTAMP() * 1000, 1
WHERE NOT EXISTS (SELECT 1 FROM `iteration_node` WHERE `node` = 'project/task/worktimelist');

INSERT INTO `iteration_node` (`node`, `title`, `is_menu`, `is_auth`, `is_login`, `create_at`, `tenant_id`)
SELECT 'project/project/recyclelist', '项目回收站列表', 0, 1, 1, UNIX_TIMESTAMP() * 1000, 1
WHERE NOT EXISTS (SELECT 1 FROM `iteration_node` WHERE `node` = 'project/project/recyclelist');

INSERT INTO `iteration_auth_node` (`auth`, `node`, `tenant_id`)
SELECT a.id, n.node, a.tenant_id
FROM `iteration_auth` a
         JOIN (SELECT 'project/analysis' AS node
               UNION ALL SELECT 'project/analysis/delivery'
               UNION ALL SELECT 'project/analysis/burndown'
               UNION ALL SELECT 'project/analysis/teamload'
               UNION ALL SELECT 'project/analysis/airecommend'
               UNION ALL SELECT 'project/task/worktimelist'
               UNION ALL SELECT 'project/project/recyclelist') n
WHERE a.type = 'admin'
  AND NOT EXISTS (SELECT 1 FROM `iteration_auth_node` x WHERE x.auth = a.id AND x.node = n.node);
