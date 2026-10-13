SET @c1 = (SELECT COUNT(*) FROM `iteration_task_log`);
SET @sql1 = IF(@c1 = 0, 'DROP TABLE IF EXISTS `iteration_task_log`', 'SELECT ''iteration_task_log 非空，跳过删除''');
PREPARE s1 FROM @sql1;
EXECUTE s1;
DEALLOCATE PREPARE s1;

SET @c2 = (SELECT COUNT(*) FROM `iteration_work_time_stat`);
SET @sql2 = IF(@c2 = 0, 'DROP TABLE IF EXISTS `iteration_work_time_stat`', 'SELECT ''iteration_work_time_stat 非空，跳过删除''');
PREPARE s2 FROM @sql2;
EXECUTE s2;
DEALLOCATE PREPARE s2;
