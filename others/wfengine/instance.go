package wfengine

import (
	"fmt"
	"time"
)

type InstanceStatus string

const (
	InstanceRunning  InstanceStatus = "running"
	InstanceApproved InstanceStatus = "approved"
	InstanceRejected InstanceStatus = "rejected"
	InstanceCanceled InstanceStatus = "canceled"
)

type TaskStatus string

const (
	TaskPending  TaskStatus = "pending"
	TaskApproved TaskStatus = "approved"
	TaskRejected TaskStatus = "rejected"
	TaskSkipped  TaskStatus = "skipped"
	TaskCanceled TaskStatus = "canceled"
)

type Task struct {
	Id         string
	InstanceId string
	NodeKey    string
	NodeName   string
	ApproverId int64
	Status     TaskStatus
	Seq        int
	Comment    string
	CreatedAt  time.Time
	HandledAt  *time.Time
	TimeoutAt  *time.Time
	round      int
}

func (t *Task) Done() bool {
	if t == nil {
		return true
	}
	switch t.Status {
	case TaskPending:
		return false
	}
	return true
}

type LogEntry struct {
	At      time.Time
	Actor   int64
	Action  string
	NodeKey string
	TaskId  string
	Comment string
	Detail  string
}

type Instance struct {
	Id                string
	DefinitionKey     string
	DefinitionVersion int
	Definition        *Definition
	BizId             string
	TenantId          int64
	SubmitterId       int64
	Status            InstanceStatus
	CurrentKey        string
	Variables         map[string]string
	Tasks             []*Task
	Logs              []*LogEntry
	CreatedAt         time.Time
	UpdatedAt         time.Time
	FinishedAt        *time.Time
	Version           int64
	seqCounter        int
	rounds            map[string]int
	visited           map[string]bool
}

type StartRequest struct {
	BizId       string
	TenantId    int64
	SubmitterId int64
	Variables   map[string]string
}

func (in *Instance) Running() bool {
	if in == nil {
		return false
	}
	return in.Status == InstanceRunning
}

func (in *Instance) FindTask(taskId string) (*Task, bool) {
	if in == nil {
		return nil, false
	}
	for _, t := range in.Tasks {
		if t.Id == taskId {
			return t, true
		}
	}
	return nil, false
}

func (in *Instance) PendingTasks() []*Task {
	if in == nil {
		return nil
	}
	out := make([]*Task, 0, len(in.Tasks))
	for _, t := range in.Tasks {
		if t.Status == TaskPending {
			out = append(out, t)
		}
	}
	return out
}

func (in *Instance) TasksOfNode(nodeKey string) []*Task {
	if in == nil {
		return nil
	}
	out := make([]*Task, 0, len(in.Tasks))
	for _, t := range in.Tasks {
		if t.NodeKey == nodeKey {
			out = append(out, t)
		}
	}
	return out
}

func (in *Instance) roundOf(nodeKey string) int {
	if in == nil {
		return 0
	}
	if in.rounds == nil {
		return 0
	}
	return in.rounds[nodeKey]
}

func (in *Instance) setRound(nodeKey string, round int) {
	if in.rounds == nil {
		in.rounds = make(map[string]int)
	}
	in.rounds[nodeKey] = round
}

func (in *Instance) beginRound(nodeKey string) int {
	round := in.roundOf(nodeKey) + 1
	in.setRound(nodeKey, round)
	return round
}

func (in *Instance) roundTasks(nodeKey string) []*Task {
	round := in.roundOf(nodeKey)
	if round == 0 {
		return nil
	}
	out := make([]*Task, 0, len(in.Tasks))
	for _, t := range in.Tasks {
		if t.NodeKey == nodeKey && t.round == round {
			out = append(out, t)
		}
	}
	return out
}

func (in *Instance) nextSeq() int {
	in.seqCounter++
	return in.seqCounter
}

func (in *Instance) addLog(at time.Time, actor int64, action, nodeKey, taskId, comment, detail string) {
	in.Logs = append(in.Logs, &LogEntry{
		At:      at,
		Actor:   actor,
		Action:  action,
		NodeKey: nodeKey,
		TaskId:  taskId,
		Comment: comment,
		Detail:  detail,
	})
}

func (in *Instance) cancelPending(at time.Time, nodeKeys map[string]bool, comment string) int {
	n := 0
	for _, t := range in.Tasks {
		if t.Status != TaskPending {
			continue
		}
		if nodeKeys != nil && !nodeKeys[t.NodeKey] {
			continue
		}
		t.Status = TaskCanceled
		handled := at
		t.HandledAt = &handled
		t.Comment = comment
		n++
		in.addLog(at, t.ApproverId, "cancel", t.NodeKey, t.Id, comment, "pending task canceled")
	}
	return n
}

func (in *Instance) finishInstance(at time.Time, actor int64, action, comment string) {
	in.Status = InstanceApproved
	in.CurrentKey = ""
	in.FinishedAt = &at
	in.cancelPending(at, nil, "instance finished")
	in.addLog(at, actor, action, "", "", comment, "instance approved")
}

func (in *Instance) rejectInstance(at time.Time, actor int64, action, comment string) {
	in.Status = InstanceRejected
	in.CurrentKey = ""
	in.FinishedAt = &at
	in.cancelPending(at, nil, "instance rejected")
	in.addLog(at, actor, action, "", "", comment, "instance rejected")
}

func (in *Instance) appendLog(entry *LogEntry) {
	in.Logs = append(in.Logs, entry)
}

func (in *Instance) Clone() *Instance {
	return cloneInstance(in)
}

func cloneInstance(in *Instance) *Instance {
	if in == nil {
		return nil
	}
	out := &Instance{
		Id:                in.Id,
		DefinitionKey:     in.DefinitionKey,
		DefinitionVersion: in.DefinitionVersion,
		Definition:        in.Definition.Clone(),
		BizId:             in.BizId,
		TenantId:          in.TenantId,
		SubmitterId:       in.SubmitterId,
		Status:            in.Status,
		CurrentKey:        in.CurrentKey,
		CreatedAt:         in.CreatedAt,
		UpdatedAt:         in.UpdatedAt,
		Version:           in.Version,
		seqCounter:        in.seqCounter,
	}
	if len(in.Variables) > 0 {
		out.Variables = make(map[string]string, len(in.Variables))
		for k, v := range in.Variables {
			out.Variables[k] = v
		}
	}
	if len(in.Tasks) > 0 {
		out.Tasks = make([]*Task, 0, len(in.Tasks))
		for _, t := range in.Tasks {
			out.Tasks = append(out.Tasks, cloneTask(t))
		}
	}
	if len(in.Logs) > 0 {
		out.Logs = make([]*LogEntry, 0, len(in.Logs))
		for _, l := range in.Logs {
			out.Logs = append(out.Logs, cloneLog(l))
		}
	}
	if in.FinishedAt != nil {
		at := *in.FinishedAt
		out.FinishedAt = &at
	}
	if len(in.rounds) > 0 {
		out.rounds = make(map[string]int, len(in.rounds))
		for k, v := range in.rounds {
			out.rounds[k] = v
		}
	}
	return out
}

func cloneTask(t *Task) *Task {
	if t == nil {
		return nil
	}
	out := *t
	if t.HandledAt != nil {
		at := *t.HandledAt
		out.HandledAt = &at
	}
	if t.TimeoutAt != nil {
		at := *t.TimeoutAt
		out.TimeoutAt = &at
	}
	return &out
}

func cloneLog(l *LogEntry) *LogEntry {
	if l == nil {
		return nil
	}
	out := *l
	return &out
}

func instanceNotFound(id string) error {
	return fmt.Errorf("%w: %s", ErrInstanceNotFound, id)
}

func taskNotFound(id string) error {
	return fmt.Errorf("%w: %s", ErrTaskNotFound, id)
}

func versionConflict(id string, expected, actual int64) error {
	return fmt.Errorf("%w: instance %s expected version %d but stored version is %d", ErrVersionConflict, id, expected, actual)
}

func invalidRequest(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidRequest, fmt.Sprintf(format, args...))
}
