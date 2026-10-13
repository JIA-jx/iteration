package wfengine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func compactJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	return data
}

func roundTripInstance(t *testing.T, source *Instance) *Instance {
	t.Helper()
	data := compactJSON(t, source)
	var loaded Instance
	if err := json.Unmarshal(data, &loaded); err != nil {
		t.Fatalf("unmarshal instance failed: %v", err)
	}
	return &loaded
}

func TestInstanceJSONRoundTripIsByteStable(t *testing.T) {
	h := newHarness(t)
	inst := startInstance(h, signAllDef(11, 12, 13), "biz-json", 1, vars("amount", "500"))
	first := taskByApprover(t, inst, "a", 11)
	partial := h.mustApprove(inst.Id, first.Id, 11)

	firstPass := compactJSON(t, partial)
	secondPass := compactJSON(t, roundTripInstance(t, partial))
	if !bytes.Equal(firstPass, secondPass) {
		t.Fatalf("json is not byte stable\nfirst  = %s\nsecond = %s", firstPass, secondPass)
	}
	thirdPass := compactJSON(t, roundTripInstance(t, roundTripInstance(t, partial)))
	if !bytes.Equal(firstPass, thirdPass) {
		t.Fatalf("json drifted after two round trips\nfirst = %s\nthird = %s", firstPass, thirdPass)
	}
}

func TestInstanceJSONKeepsUnexportedState(t *testing.T) {
	h := newHarness(t)
	inst := startInstance(h, signAllDef(11, 12), "biz-state", 1, nil)
	first := taskByApprover(t, inst, "a", 11)
	partial := h.mustApprove(inst.Id, first.Id, 11)

	data := compactJSON(t, partial)
	text := string(data)
	for _, key := range []string{"\"state\"", "\"seqCounter\"", "\"rounds\"", "\"Round\"", "\"Definition\"", "\"Logs\"", "\"Tasks\""} {
		if !contains(text, key) {
			t.Fatalf("json %s is missing key %s", text, key)
		}
	}
	if contains(text, "visited") {
		t.Fatalf("json must not carry transient advance state: %s", text)
	}

	loaded := roundTripInstance(t, partial)
	if loaded.seqCounter != partial.seqCounter {
		t.Fatalf("seqCounter = %d, want %d", loaded.seqCounter, partial.seqCounter)
	}
	if loaded.roundOf("a") != partial.roundOf("a") {
		t.Fatalf("round = %d, want %d", loaded.roundOf("a"), partial.roundOf("a"))
	}
	if loaded.visited != nil {
		t.Fatal("visited must be nil after unmarshal")
	}
	if loaded.Definition == nil {
		t.Fatal("definition snapshot missing")
	}
	if _, ok := loaded.Definition.Node("a"); !ok {
		t.Fatal("definition snapshot did not rebuild its index")
	}
	for i, task := range loaded.Tasks {
		if task.round != partial.Tasks[i].round {
			t.Fatalf("task %s round = %d, want %d", task.Id, task.round, partial.Tasks[i].round)
		}
		if !task.CreatedAt.Equal(partial.Tasks[i].CreatedAt) {
			t.Fatalf("task %s createdAt = %s", task.Id, task.CreatedAt)
		}
	}
	if len(loaded.Logs) != len(partial.Logs) {
		t.Fatalf("logs = %d, want %d", len(loaded.Logs), len(partial.Logs))
	}
	lastSource := partial.Logs[len(partial.Logs)-1]
	lastLoaded := loaded.Logs[len(loaded.Logs)-1]
	if lastLoaded.Action != lastSource.Action || lastLoaded.Actor != lastSource.Actor || !lastLoaded.At.Equal(lastSource.At) {
		t.Fatalf("log = %+v, want %+v", lastLoaded, lastSource)
	}
}

func TestInstanceJSONKeepsPointersAndDefinition(t *testing.T) {
	h := newHarness(t)
	inst := startInstance(h, timeoutDef(), "biz-pointer", 1, nil)
	done := h.mustApprove(inst.Id, pendingOf(t, inst, "a").Id, 91)
	if done.FinishedAt == nil {
		t.Fatal("finished at must be set")
	}
	loaded := roundTripInstance(t, done)
	if loaded.FinishedAt == nil {
		t.Fatal("finished at lost in round trip")
	}
	if !loaded.FinishedAt.Equal(*done.FinishedAt) {
		t.Fatalf("finished at = %s, want %s", loaded.FinishedAt, done.FinishedAt)
	}
	if loaded.DefinitionKey != done.DefinitionKey || loaded.DefinitionVersion != done.DefinitionVersion {
		t.Fatalf("definition metadata = %s@%d", loaded.DefinitionKey, loaded.DefinitionVersion)
	}
	if len(loaded.Definition.Nodes) != len(done.Definition.Nodes) {
		t.Fatalf("definition nodes = %d, want %d", len(loaded.Definition.Nodes), len(done.Definition.Nodes))
	}
	conditional := startInstance(h, conditionDef(), "biz-pointer-2", 1, vars("amount", "5000"))
	loadedConditional := roundTripInstance(t, conditional)
	node, ok := loadedConditional.Definition.Node("c")
	if !ok {
		t.Fatal("condition node missing")
	}
	if len(node.Branches) != 2 || node.Branches[0].Expr != "amount>1000" || node.Branches[0].Next != "big" {
		t.Fatalf("branches = %+v", node.Branches)
	}
	if !loadedConditional.CreatedAt.Equal(conditional.CreatedAt) {
		t.Fatalf("created at = %s", loadedConditional.CreatedAt)
	}
}

func TestInstanceJSONRejectsMissingDefinition(t *testing.T) {
	raw := []byte(`{"Id":"inst-1","Status":"running","Tasks":[],"Logs":[]}`)
	var loaded Instance
	err := json.Unmarshal(raw, &loaded)
	requireError(t, err, ErrInvalidDefinition, "missing definition snapshot")

	nilDefinition := []byte(`{"Id":"inst-1","Definition":null}`)
	err = json.Unmarshal(nilDefinition, &loaded)
	requireError(t, err, ErrInvalidDefinition, "null definition snapshot")

	if err := json.Unmarshal([]byte(`{`), &loaded); err == nil {
		t.Fatal("broken json must fail")
	}
	var nilInstance *Instance
	if err := nilInstance.UnmarshalJSON([]byte(`{}`)); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("nil receiver err = %v", err)
	}
}

func TestInstanceJSONInfersSeqCounterFromTasks(t *testing.T) {
	h := newHarness(t)
	legacy := `{
		"Id":"inst-legacy",
		"DefinitionKey":"sequence",
		"DefinitionVersion":1,
		"Definition":{"Key":"sequence","Name":"sequence","Version":1,"StartKey":"s","Nodes":[
			{"Key":"s","Name":"s","Type":"start","Next":"a"},
			{"Key":"a","Name":"chain","Type":"approval","Sign":"sequence","Approvers":[51,52,53],"Next":"e"},
			{"Key":"e","Name":"e","Type":"end"}
		]},
		"BizId":"biz-infer",
		"TenantId":7,
		"SubmitterId":1,
		"Status":"running",
		"CurrentKey":"a",
		"Variables":{},
		"Tasks":[
			{"Id":"task-7","InstanceId":"inst-legacy","NodeKey":"a","NodeName":"chain","ApproverId":51,"Status":"approved","Seq":7,"Round":1,"CreatedAt":"2024-03-01T09:00:00Z"},
			{"Id":"task-9","InstanceId":"inst-legacy","NodeKey":"a","NodeName":"chain","ApproverId":52,"Status":"pending","Seq":9,"Round":1,"CreatedAt":"2024-03-01T09:01:00Z"}
		],
		"Logs":[],
		"CreatedAt":"2024-03-01T09:00:00Z",
		"UpdatedAt":"2024-03-01T09:01:00Z",
		"Version":4,
		"state":{"seqCounter":0,"rounds":{"a":1}}
	}`
	var loaded Instance
	if err := json.Unmarshal([]byte(legacy), &loaded); err != nil {
		t.Fatalf("unmarshal legacy failed: %v", err)
	}
	if loaded.seqCounter != 9 {
		t.Fatalf("seqCounter = %d, want 9 inferred from task seq", loaded.seqCounter)
	}
	if loaded.roundOf("a") != 1 {
		t.Fatalf("round = %d, want 1", loaded.roundOf("a"))
	}
	store := NewMemoryStore()
	if err := store.Create(context.Background(), &loaded); err != nil {
		t.Fatalf("create failed: %v", err)
	}
	engine := NewEngine(store, Options{Now: h.clock.Now, IDGen: h.ids.Next})
	activated := taskByApprover(t, &loaded, "a", 52)
	moved, err := engine.Approve(context.Background(), loaded.Id, activated.Id, 52, "ok")
	if err != nil {
		t.Fatalf("approve failed: %v", err)
	}
	next := pendingOf(t, moved, "a")
	if next.ApproverId != 53 {
		t.Fatalf("next approver = %d, want 53", next.ApproverId)
	}
	if next.Seq <= 9 {
		t.Fatalf("new task seq = %d must stay above legacy seq 9", next.Seq)
	}
}

func TestInstanceJSONInferRoundWhenStateIsMissing(t *testing.T) {
	raw := []byte(`{
		"Id":"inst-old",
		"Definition":{"Key":"linear","Version":1,"StartKey":"s","Nodes":[
			{"Key":"s","Name":"s","Type":"start","Next":"a"},
			{"Key":"a","Name":"a","Type":"approval","Sign":"any","Approvers":[11],"Next":"e"},
			{"Key":"e","Name":"e","Type":"end"}
		]},
		"Status":"running",
		"Tasks":[{"Id":"task-1","NodeKey":"a","ApproverId":11,"Status":"pending","Seq":3,"CreatedAt":"2024-03-01T09:00:00Z"}],
		"Logs":[],
		"CreatedAt":"2024-03-01T09:00:00Z",
		"UpdatedAt":"2024-03-01T09:00:00Z",
		"Version":2
	}`)
	var loaded Instance
	if err := json.Unmarshal(raw, &loaded); err != nil {
		t.Fatalf("unmarshal legacy failed: %v", err)
	}
	if loaded.Tasks[0].round != 1 {
		t.Fatalf("round = %d, want 1", loaded.Tasks[0].round)
	}
	if loaded.rounds == nil {
		t.Fatal("rounds must be initialised")
	}
	if loaded.seqCounter != 3 {
		t.Fatalf("seqCounter = %d, want 3", loaded.seqCounter)
	}
	if loaded.visited != nil {
		t.Fatal("visited must stay nil")
	}
}

func TestRoundTripKeepsApprovalModesWorking(t *testing.T) {
	t.Run("all sign", func(t *testing.T) {
		h := newHarness(t)
		inst := startInstance(h, signAllDef(11, 12, 13), "biz-rt-all", 1, nil)
		partial := h.mustApprove(inst.Id, taskByApprover(t, inst, "a", 11).Id, 11)
		partial = h.mustApprove(partial.Id, taskByApprover(t, partial, "a", 12).Id, 12)

		loaded := roundTripInstance(t, partial)
		if loaded.Status != InstanceRunning || loaded.CurrentKey != "a" {
			t.Fatalf("status = %s current = %s", loaded.Status, loaded.CurrentKey)
		}
		if len(loaded.PendingTasks()) != 1 {
			t.Fatalf("pending tasks = %s", describeTasks(loaded))
		}
		engine := NewEngine(h.store, Options{Now: h.clock.Now, IDGen: h.ids.Next})
		last := taskByApprover(t, loaded, "a", 13)
		moved, err := engine.Approve(context.Background(), loaded.Id, last.Id, 13, "ok")
		if err != nil {
			t.Fatalf("approve failed: %v", err)
		}
		if moved.CurrentKey != "b" {
			t.Fatalf("current = %s, want b tasks = %s", moved.CurrentKey, describeTasks(moved))
		}
	})

	t.Run("sequence sign", func(t *testing.T) {
		h := newHarness(t)
		inst := startInstance(h, sequenceDef(), "biz-rt-seq", 1, nil)
		partial := h.mustApprove(inst.Id, pendingOf(t, inst, "a").Id, 51)

		loaded := roundTripInstance(t, partial)
		engine := NewEngine(h.store, Options{Now: h.clock.Now, IDGen: h.ids.Next})
		second := taskByApprover(t, loaded, "a", 52)
		if second.Seq > loaded.seqCounter {
			t.Fatalf("task seq %d must not exceed counter %d", second.Seq, loaded.seqCounter)
		}
		afterSecond, err := engine.Approve(context.Background(), loaded.Id, second.Id, 52, "ok")
		if err != nil {
			t.Fatalf("approve failed: %v", err)
		}
		third := pendingOf(t, afterSecond, "a")
		if third.ApproverId != 53 {
			t.Fatalf("next approver = %d, want 53 tasks = %s", third.ApproverId, describeTasks(afterSecond))
		}
		if !hasRoundApprover(afterSecond, "a", 52) {
			t.Fatal("approver 52 must stay recorded")
		}
	})

	t.Run("rollback", func(t *testing.T) {
		h := newHarness(t)
		inst := startInstance(h, rollbackDef(), "biz-rt-rollback", 1, nil)
		atManager := h.mustApprove(inst.Id, pendingOf(t, inst, "a").Id, 11)
		loaded := roundTripInstance(t, atManager)

		engine := NewEngine(h.store, Options{Now: h.clock.Now, IDGen: h.ids.Next})
		managerTask := pendingOf(t, loaded, "b")
		rolledBack, err := engine.Reject(context.Background(), loaded.Id, managerTask.Id, 21, "no", "a")
		if err != nil {
			t.Fatalf("reject failed: %v", err)
		}
		if rolledBack.CurrentKey != "a" || rolledBack.Status != InstanceRunning {
			t.Fatalf("status = %s current = %s", rolledBack.Status, rolledBack.CurrentKey)
		}
		if countTasks(rolledBack, "b", TaskSkipped) != 1 {
			t.Fatalf("manager task must stay terminated: %s", describeTasks(rolledBack))
		}
		newLeader := pendingOf(t, rolledBack, "a")
		if newLeader.round <= 1 {
			t.Fatalf("rollback must open a new round, tasks = %s", describeTasks(rolledBack))
		}
		if countTasks(rolledBack, "a", TaskApproved) != 1 {
			t.Fatalf("historical round must stay approved: %s", describeTasks(rolledBack))
		}

		replayed := roundTripInstance(t, rolledBack)
		engine = NewEngine(h.store, Options{Now: h.clock.Now, IDGen: h.ids.Next})
		again := pendingOf(t, replayed, "a")
		afterAgain, err := engine.Approve(context.Background(), replayed.Id, again.Id, 11, "ok")
		if err != nil {
			t.Fatalf("approve after rollback failed: %v", err)
		}
		newManager := pendingOf(t, afterAgain, "b")
		if newManager.Id == managerTask.Id {
			t.Fatalf("rewalk must create a fresh task, got %s", newManager.Id)
		}
		if newManager.ApproverId != 21 {
			t.Fatalf("rewalk approver = %d, want 21", newManager.ApproverId)
		}
		if newManager.round != 2 {
			t.Fatalf("rewalk round = %d, want 2", newManager.round)
		}
		done, err := engine.Approve(context.Background(), afterAgain.Id, newManager.Id, 21, "ok")
		if err != nil {
			t.Fatalf("final approve failed: %v", err)
		}
		if done.Status != InstanceApproved {
			t.Fatalf("status = %s, want approved tasks = %s", done.Status, describeTasks(done))
		}
	})
}

func TestTaskAndDefinitionJSONRoundTrip(t *testing.T) {
	handledAt := time.Date(2024, 3, 2, 10, 30, 0, 0, time.UTC)
	timeoutAt := time.Date(2024, 3, 2, 11, 30, 0, 0, time.UTC)
	task := &Task{
		Id:         "task-1",
		InstanceId: "inst-1",
		NodeKey:    "a",
		NodeName:   "leader",
		ApproverId: 11,
		Status:     TaskSkipped,
		Seq:        4,
		Comment:    "timeout",
		CreatedAt:  handledAt.Add(-time.Hour),
		HandledAt:  &handledAt,
		TimeoutAt:  &timeoutAt,
		round:      3,
	}
	data := compactJSON(t, task)
	var loaded Task
	if err := json.Unmarshal(data, &loaded); err != nil {
		t.Fatalf("unmarshal task failed: %v", err)
	}
	if loaded.round != 3 {
		t.Fatalf("round = %d, want 3", loaded.round)
	}
	if loaded.Id != task.Id || loaded.Seq != task.Seq || loaded.Status != task.Status || loaded.ApproverId != task.ApproverId {
		t.Fatalf("task = %+v", loaded)
	}
	if loaded.HandledAt == nil || !loaded.HandledAt.Equal(handledAt) {
		t.Fatalf("handled at = %v", loaded.HandledAt)
	}
	if loaded.TimeoutAt == nil || !loaded.TimeoutAt.Equal(timeoutAt) {
		t.Fatalf("timeout at = %v", loaded.TimeoutAt)
	}
	if !bytes.Equal(data, compactJSON(t, &loaded)) {
		t.Fatalf("task json is not stable\nfirst  = %s\nsecond = %s", data, compactJSON(t, &loaded))
	}

	definition := conditionDef()
	defData := compactJSON(t, definition)
	var loadedDefinition Definition
	if err := json.Unmarshal(defData, &loadedDefinition); err != nil {
		t.Fatalf("unmarshal definition failed: %v", err)
	}
	if loadedDefinition.Key != definition.Key || loadedDefinition.Version != definition.Version || loadedDefinition.StartKey != definition.StartKey {
		t.Fatalf("definition key = %s version = %d start = %s", loadedDefinition.Key, loadedDefinition.Version, loadedDefinition.StartKey)
	}
	if len(loadedDefinition.Nodes) != len(definition.Nodes) {
		t.Fatalf("nodes = %d, want %d", len(loadedDefinition.Nodes), len(definition.Nodes))
	}
	node, ok := loadedDefinition.Node("c")
	if !ok {
		t.Fatal("conditional node missing")
	}
	if len(node.Branches) != len(definition.Nodes[1].Branches) {
		t.Fatalf("branches = %+v", node.Branches)
	}
	if !bytes.Equal(defData, compactJSON(t, &loadedDefinition)) {
		t.Fatalf("definition json is not stable\nfirst  = %s\nsecond = %s", defData, compactJSON(t, &loadedDefinition))
	}
	if err := loadedDefinition.Validate(); err != nil {
		t.Fatalf("round tripped definition no longer validates: %v", err)
	}

	var nilTask *Task
	nilTaskData, err := json.Marshal(nilTask)
	if err != nil {
		t.Fatalf("marshal nil task failed: %v", err)
	}
	if string(nilTaskData) != "null" {
		t.Fatalf("nil task json = %s", nilTaskData)
	}
	var nilDefinition *Definition
	nilDefinitionData, err := json.Marshal(nilDefinition)
	if err != nil {
		t.Fatalf("marshal nil definition failed: %v", err)
	}
	if string(nilDefinitionData) != "null" {
		t.Fatalf("nil definition json = %s", nilDefinitionData)
	}
	var nilInstance *Instance
	nilInstanceData, err := json.Marshal(nilInstance)
	if err != nil {
		t.Fatalf("marshal nil instance failed: %v", err)
	}
	if string(nilInstanceData) != "null" {
		t.Fatalf("nil instance json = %s", nilInstanceData)
	}
	if err := nilTask.UnmarshalJSON([]byte(`{}`)); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("nil task receiver err = %v", err)
	}
	if err := nilDefinition.UnmarshalJSON([]byte(`{}`)); !errors.Is(err, ErrInvalidDefinition) {
		t.Fatalf("nil definition receiver err = %v", err)
	}
}

func TestRoundTripInstanceDrivesFreshStoreToCompletion(t *testing.T) {
	h := newHarness(t)
	inst := startInstance(h, signAllDef(11, 12), "biz-rt-fresh", 1, nil)
	partial := h.mustApprove(inst.Id, taskByApprover(t, inst, "a", 11).Id, 11)

	loaded := roundTripInstance(t, partial)
	fresh := NewMemoryStore()
	if err := fresh.Create(context.Background(), loaded); err != nil {
		t.Fatalf("create in fresh store failed: %v", err)
	}
	engine := NewEngine(fresh, Options{Now: h.clock.Now, IDGen: h.ids.Next})

	second := taskByApprover(t, loaded, "a", 12)
	atManager, err := engine.Approve(context.Background(), loaded.Id, second.Id, 12, "ok")
	if err != nil {
		t.Fatalf("approve after reload failed: %v", err)
	}
	if atManager.CurrentKey != "b" {
		t.Fatalf("current = %s, want b tasks = %s", atManager.CurrentKey, describeTasks(atManager))
	}
	manager := pendingOf(t, atManager, "b")
	done, err := engine.Approve(context.Background(), atManager.Id, manager.Id, 99, "ok")
	if err != nil {
		t.Fatalf("final approve failed: %v", err)
	}
	if done.Status != InstanceApproved {
		t.Fatalf("status = %s, want approved tasks = %s", done.Status, describeTasks(done))
	}
	if len(done.PendingTasks()) != 0 {
		t.Fatalf("pending tasks = %s", describeTasks(done))
	}
}

func TestRoundTripDoesNotShareMutableState(t *testing.T) {
	h := newHarness(t)
	inst := startInstance(h, linearApprovalDef(), "biz-rt-share", 1, vars("amount", "100"))
	loaded := roundTripInstance(t, inst)

	loaded.Variables["amount"] = "999"
	loaded.Tasks[0].Status = TaskApproved
	loaded.Logs[0].Action = "tampered"
	loaded.Definition.Nodes[1].Approvers = []int64{777}
	loaded.Definition.Nodes[1].Name = "tampered"

	text := string(compactJSON(t, inst))
	for _, unexpected := range []string{"999", "tampered", "777"} {
		if contains(text, unexpected) {
			t.Fatalf("source instance leaked mutation %q: %s", unexpected, text)
		}
	}
	if inst.Variables["amount"] != "100" {
		t.Fatalf("variables = %v", inst.Variables)
	}
	if inst.Tasks[0].Status != TaskPending {
		t.Fatalf("task status = %s", inst.Tasks[0].Status)
	}
}
