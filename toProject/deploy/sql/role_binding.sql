INSERT INTO `iteration_member_account`
    (`member_code`, `organization_code`, `department_code`, `authorize`, `is_owner`,
     `name`, `mobile`, `email`, `create_time`, `last_login_time`, `status`, `tenant_id`)
SELECT m.id, o.id, 0,
       (SELECT a.id FROM `iteration_auth` a WHERE a.type = 'admin' ORDER BY a.id LIMIT 1),
       1, m.name, m.mobile, m.email, UNIX_TIMESTAMP() * 1000, UNIX_TIMESTAMP() * 1000, 1, o.tenant_id
FROM `iteration_member` m
         JOIN `iteration_organization` o ON o.member_id = m.id
WHERE NOT EXISTS (SELECT 1 FROM `iteration_member_account` a
                  WHERE a.member_code = m.id AND a.organization_code = o.id);

UPDATE `iteration_member_account` a
    JOIN `iteration_auth` admin ON admin.type = 'admin'
    JOIN `iteration_auth` member ON member.type = 'member'
SET a.authorize = CASE WHEN a.is_owner = 1 THEN admin.id ELSE member.id END
WHERE a.authorize IS NULL OR a.authorize = '' OR a.authorize = '0';