package wfengine

import (
	"encoding/json"
	"fmt"
	"time"
)

type timeJSON time.Time

func (t timeJSON) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Time(t))
}

func (t *timeJSON) UnmarshalJSON(data []byte) error {
	var value time.Time
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	*t = timeJSON(value)
	return nil
}

func (t timeJSON) Time() time.Time {
	return time.Time(t)
}

func (t *timeJSON) TimePtr() *time.Time {
	if t == nil {
		return nil
	}
	value := time.Time(*t)
	return &value
}

func toTimePtr(value *time.Time) *timeJSON {
	if value == nil {
		return nil
	}
	converted := timeJSON(*value)
	return &converted
}

type definitionJSON struct {
	Key      string `json:"Key"`
	Name     string `json:"Name"`
	Version  int    `json:"Version"`
	Nodes    []Node `json:"Nodes"`
	StartKey string `json:"StartKey"`
}

type taskDocument struct {
	taskJSON
	Round int `json:"Round"`
}

type taskJSON struct {
	Id         string     `json:"Id"`
	InstanceId string     `json:"InstanceId"`
	NodeKey    string     `json:"NodeKey"`
	NodeName   string     `json:"NodeName"`
	ApproverId int64      `json:"ApproverId"`
	Status     TaskStatus `json:"Status"`
	Seq        int        `json:"Seq"`
	Comment    string     `json:"Comment"`
	CreatedAt  timeJSON   `json:"CreatedAt"`
	HandledAt  *timeJSON  `json:"HandledAt,omitempty"`
	TimeoutAt  *timeJSON  `json:"TimeoutAt,omitempty"`
}

type instanceState struct {
	SeqCounter int            `json:"seqCounter"`
	Rounds     map[string]int `json:"rounds"`
}

type instanceDocument struct {
	Id                string            `json:"Id"`
	DefinitionKey     string            `json:"DefinitionKey"`
	DefinitionVersion int               `json:"DefinitionVersion"`
	Definition        *Definition       `json:"Definition"`
	BizId             string            `json:"BizId"`
	TenantId          int64             `json:"TenantId"`
	SubmitterId       int64             `json:"SubmitterId"`
	Status            InstanceStatus    `json:"Status"`
	CurrentKey        string            `json:"CurrentKey"`
	Variables         map[string]string `json:"Variables"`
	Tasks             []taskDocument    `json:"Tasks"`
	Logs              []*LogEntry       `json:"Logs"`
	CreatedAt         timeJSON          `json:"CreatedAt"`
	UpdatedAt         timeJSON          `json:"UpdatedAt"`
	FinishedAt        *timeJSON         `json:"FinishedAt,omitempty"`
	Version           int64             `json:"Version"`
	State             *instanceState    `json:"state"`
}

func toDefinitionJSON(d *Definition) definitionJSON {
	nodes := make([]Node, 0, len(d.Nodes))
	for _, n := range d.Nodes {
		nodes = append(nodes, cloneNode(n))
	}
	return definitionJSON{
		Key:      d.Key,
		Name:     d.Name,
		Version:  d.Version,
		Nodes:    nodes,
		StartKey: d.StartKey,
	}
}

func fromDefinitionJSON(doc definitionJSON) Definition {
	nodes := make([]Node, 0, len(doc.Nodes))
	for _, n := range doc.Nodes {
		nodes = append(nodes, cloneNode(n))
	}
	return Definition{
		Key:      doc.Key,
		Name:     doc.Name,
		Version:  doc.Version,
		Nodes:    nodes,
		StartKey: doc.StartKey,
	}
}

func toDefinitionPtr(d *Definition) *Definition {
	if d == nil {
		return nil
	}
	converted := fromDefinitionJSON(toDefinitionJSON(d))
	return &converted
}

func (d *Definition) MarshalJSON() ([]byte, error) {
	if d == nil {
		return []byte("null"), nil
	}
	return json.Marshal(toDefinitionJSON(d))
}

func (d *Definition) UnmarshalJSON(data []byte) error {
	if d == nil {
		return fmt.Errorf("%w: definition receiver is nil", ErrInvalidDefinition)
	}
	var doc definitionJSON
	if err := json.Unmarshal(data, &doc); err != nil {
		return err
	}
	*d = fromDefinitionJSON(doc)
	return nil
}

func (t *Task) MarshalJSON() ([]byte, error) {
	if t == nil {
		return []byte("null"), nil
	}
	return json.Marshal(toTaskDocument(t))
}

func (t *Task) UnmarshalJSON(data []byte) error {
	if t == nil {
		return fmt.Errorf("%w: task receiver is nil", ErrInvalidRequest)
	}
	var doc taskDocument
	if err := json.Unmarshal(data, &doc); err != nil {
		return err
	}
	*t = fromTaskDocument(doc)
	return nil
}

func (in *Instance) MarshalJSON() ([]byte, error) {
	if in == nil {
		return []byte("null"), nil
	}
	return json.Marshal(toInstanceDocument(in))
}

func (in *Instance) UnmarshalJSON(data []byte) error {
	if in == nil {
		return fmt.Errorf("%w: instance receiver is nil", ErrInvalidRequest)
	}
	doc := instanceDocument{
		State: &instanceState{},
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		return err
	}
	loaded, err := fromInstanceDocument(doc)
	if err != nil {
		return err
	}
	*in = *loaded
	return nil
}

func toTaskDocument(t *Task) taskDocument {
	return taskDocument{
		taskJSON: taskJSON{
			Id:         t.Id,
			InstanceId: t.InstanceId,
			NodeKey:    t.NodeKey,
			NodeName:   t.NodeName,
			ApproverId: t.ApproverId,
			Status:     t.Status,
			Seq:        t.Seq,
			Comment:    t.Comment,
			CreatedAt:  timeJSON(t.CreatedAt),
			HandledAt:  toTimePtr(t.HandledAt),
			TimeoutAt:  toTimePtr(t.TimeoutAt),
		},
		Round: t.round,
	}
}

func fromTaskDocument(doc taskDocument) Task {
	return Task{
		Id:         doc.Id,
		InstanceId: doc.InstanceId,
		NodeKey:    doc.NodeKey,
		NodeName:   doc.NodeName,
		ApproverId: doc.ApproverId,
		Status:     doc.Status,
		Seq:        doc.Seq,
		Comment:    doc.Comment,
		CreatedAt:  doc.CreatedAt.Time(),
		HandledAt:  doc.HandledAt.TimePtr(),
		TimeoutAt:  doc.TimeoutAt.TimePtr(),
		round:      doc.Round,
	}
}

func toInstanceDocument(in *Instance) instanceDocument {
	tasks := make([]taskDocument, 0, len(in.Tasks))
	for _, task := range in.Tasks {
		if task == nil {
			continue
		}
		tasks = append(tasks, toTaskDocument(task))
	}
	logs := make([]*LogEntry, 0, len(in.Logs))
	for _, entry := range in.Logs {
		logs = append(logs, cloneLog(entry))
	}
	rounds := make(map[string]int, len(in.rounds))
	for key, round := range in.rounds {
		rounds[key] = round
	}
	return instanceDocument{
		Id:                in.Id,
		DefinitionKey:     in.DefinitionKey,
		DefinitionVersion: in.DefinitionVersion,
		Definition:        toDefinitionPtr(in.Definition),
		BizId:             in.BizId,
		TenantId:          in.TenantId,
		SubmitterId:       in.SubmitterId,
		Status:            in.Status,
		CurrentKey:        in.CurrentKey,
		Variables:         copyVariables(in.Variables),
		Tasks:             tasks,
		Logs:              logs,
		CreatedAt:         timeJSON(in.CreatedAt),
		UpdatedAt:         timeJSON(in.UpdatedAt),
		FinishedAt:        toTimePtr(in.FinishedAt),
		Version:           in.Version,
		State: &instanceState{
			SeqCounter: in.seqCounter,
			Rounds:     rounds,
		},
	}
}

func fromInstanceDocument(doc instanceDocument) (*Instance, error) {
	if doc.Definition == nil {
		return nil, fmt.Errorf("%w: instance %s snapshot is missing", ErrInvalidDefinition, doc.Id)
	}
	loaded := &Instance{
		Id:                doc.Id,
		DefinitionKey:     doc.DefinitionKey,
		DefinitionVersion: doc.DefinitionVersion,
		Definition:        doc.Definition,
		BizId:             doc.BizId,
		TenantId:          doc.TenantId,
		SubmitterId:       doc.SubmitterId,
		Status:            doc.Status,
		CurrentKey:        doc.CurrentKey,
		Variables:         copyVariables(doc.Variables),
		Tasks:             tasksFromDocuments(doc.Tasks),
		Logs:              cloneLogs(doc.Logs),
		CreatedAt:         doc.CreatedAt.Time(),
		UpdatedAt:         doc.UpdatedAt.Time(),
		FinishedAt:        doc.FinishedAt.TimePtr(),
		Version:           doc.Version,
		rounds:            make(map[string]int),
		visited:           nil,
	}
	storedCounter := 0
	if doc.State != nil {
		for key, round := range doc.State.Rounds {
			if round > 0 {
				loaded.rounds[key] = round
			}
		}
		storedCounter = doc.State.SeqCounter
	}
	maxSeq := 0
	for _, task := range loaded.Tasks {
		if task == nil {
			continue
		}
		if task.Seq > maxSeq {
			maxSeq = task.Seq
		}
		if task.round <= 0 {
			task.round = loaded.rounds[task.NodeKey] + 1
		}
		if task.round > loaded.rounds[task.NodeKey] {
			loaded.rounds[task.NodeKey] = task.round
		}
	}
	loaded.seqCounter = storedCounter
	if maxSeq > loaded.seqCounter {
		loaded.seqCounter = maxSeq
	}
	return loaded, nil
}

func tasksFromDocuments(docs []taskDocument) []*Task {
	if docs == nil {
		return nil
	}
	out := make([]*Task, 0, len(docs))
	for _, doc := range docs {
		task := fromTaskDocument(doc)
		out = append(out, &task)
	}
	return out
}

func cloneTasks(tasks []*Task) []*Task {
	if tasks == nil {
		return nil
	}
	out := make([]*Task, 0, len(tasks))
	for _, task := range tasks {
		out = append(out, cloneTask(task))
	}
	return out
}

func cloneLogs(logs []*LogEntry) []*LogEntry {
	if logs == nil {
		return nil
	}
	out := make([]*LogEntry, 0, len(logs))
	for _, entry := range logs {
		out = append(out, cloneLog(entry))
	}
	return out
}
