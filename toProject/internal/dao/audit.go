package dao

import (
	"context"

	"github.com/gin-gonic/gin"
	"test.com/others/audit"
	"test.com/toProject/internal/database/gorms"
)

var (
	Audit       *audit.Recorder
	AuditReader audit.Reader
)

func InitAudit(opts audit.RecorderOptions) *audit.Recorder {
	writer := audit.NewGormWriter(gorms.GetDB())
	AuditReader = writer
	Audit = audit.NewRecorder(writer, opts)
	return Audit
}

func RecordAudit(ctx context.Context, action, resource, resourceId string, actorId int64, before, after interface{}, remark string) {
	if Audit == nil {
		return
	}

	beforeJSON, afterJSON := audit.Diff(before, after)

	Audit.Record(ctx, &audit.Event{
		Action:     action,
		Resource:   resource,
		ResourceId: resourceId,
		ActorId:    actorId,
		Before:     beforeJSON,
		After:      afterJSON,
		Remark:     remark,
	})
}

func AuditFromGin(c *gin.Context, action, resource, resourceId string, before, after interface{}, remark string) {
	if c == nil {
		return
	}
	ctx := context.Background()
	if c.Request != nil {
		ctx = c.Request.Context()
	}

	memberId := c.GetInt64("memberId")
	ip := c.ClientIP()

	if Audit == nil {
		return
	}

	beforeJSON, afterJSON := audit.Diff(before, after)

	Audit.Record(ctx, &audit.Event{
		Action:     action,
		Resource:   resource,
		ResourceId: resourceId,
		ActorId:    memberId,
		Before:     beforeJSON,
		After:      afterJSON,
		Ip:         ip,
		Remark:     remark,
	})
}
