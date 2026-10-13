package dao

import (
	"context"
	"test.com/toProject/internal/data"
	"test.com/toProject/internal/database/gorms"
)

type NotificationDao struct {
	conn *gorms.GormConn
}

func NewNotificationDao() *NotificationDao {
	return &NotificationDao{conn: gorms.New()}
}

func (d *NotificationDao) SaveNotification(ctx context.Context, n *data.Notification) error {
	return d.conn.Master().WithContext(ctx).Create(n).Error
}

func (d *NotificationDao) SaveDeadLetter(ctx context.Context, dl *data.DeadLetter) error {
	return d.conn.Master().WithContext(ctx).Create(dl).Error
}
