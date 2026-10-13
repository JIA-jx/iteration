UPDATE `iteration`
SET `deleted_time` = DATE_FORMAT(FROM_UNIXTIME(IF(`create_time` > 100000000000, `create_time` / 1000, `create_time`)), '%Y-%m-%d %H:%i:%s')
WHERE `deleted` = 1 AND (`deleted_time` IS NULL OR `deleted_time` = '');