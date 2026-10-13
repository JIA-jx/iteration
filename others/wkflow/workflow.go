package workflow

import (
	"context"
	"encoding/json"
	"test.com/others/errs"
	"test.com/others/event"
	"test.com/others/kk"
	"test.com/toProject/pkg/model"
	"time"
)

type AuthApplyFlow struct {
	kkWriter *kk.KafkaWriter
}

func NewAuthApplyFlow(broker string) *AuthApplyFlow {
	return &AuthApplyFlow{
		kkWriter: kk.GetWriter(broker),
	}
}

func (f *AuthApplyFlow) Submit(ctx context.Context, authId string, nodes []string) error {
	evt := event.WorkflowAuthApplyEvent{
		Biz:     "auth_apply",
		AuthId:  authId,
		Nodes:   nodes,
		Status:  "pending",
		EventAt: time.Now().Format(time.RFC3339),
	}
	data, _ := json.Marshal(evt)

	return f.kkWriter.SendSync(ctx, kk.KKData{
		Topic: evt.Topic(),
		Data:  data,
	})
}

func (f *AuthApplyFlow) Approve(ctx context.Context, authId string, authnodes []string, saveFunc func(ctx context.Context) error) error {
	if err := saveFunc(ctx); err != nil {
		return errs.GrpcError(model.BusinessError)
	}

	evt := event.WorkflowAuthApplyEvent{
		Biz:     "auth_apply",
		AuthId:  authId,
		Nodes:   authnodes,
		Status:  "approved",
		EventAt: time.Now().Format(time.RFC3339),
	}
	data, _ := json.Marshal(evt)

	return f.kkWriter.SendSync(ctx, kk.KKData{
		Topic: evt.Topic(),
		Data:  data,
	})
}
