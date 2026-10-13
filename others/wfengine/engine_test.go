package wfengine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestStartSkipsAutomaticNodesAndCreatesApprovalTask(t *testing.T) {
	h := newHarness(t)
	inst := startInstance(h, notifyDef(), "biz-1", 5, nil)

	if inst.Status != InstanceRunning {
		t.Fatalf("status = %s, want running", inst.Status)
	}
	if inst.CurrentKey != "a" {
		t.Fatalf("current key = %s, want a", inst.CurrentKey)
	}
	if inst.DefinitionKey != "notify" || inst.DefinitionVersion != 1 {
		t.Fatalf("definition snapshot = %s@%d", inst.DefinitionKey, inst.DefinitionVersion)
	}
	if inst.TenantId != 7 || inst.SubmitterId != 5 || inst.BizId != "biz-1" {
		t.Fatalf("instance metadata = %+v", inst)
	}
	pending := inst.PendingTasks()
	if len(pending) != 1 {
		t.Fatalf("pending tasks = %d, want 1: %s", len(pending), describeTasks(inst))
	}
	task := pending[0]
	if task.NodeKey != "a" || task.ApproverId != 41 || task.NodeName != "owner" {
		t.Fatalf("task = %+v", task)
	}
	if !task.CreatedAt.Equal(h.clock.Now()) {
		t.Fatalf("task created at %s, want %s", task.CreatedAt, h.clock.Now())
	}
	if !hasLog(inst, "cc") {
		t.Fatalf("expected a cc log for the notify node, logs=%+v", inst.Logs)
	}
	if !hasLog(inst, "submit") {
		t.Fatalf("expected a submit log, logs=%+v", inst.Logs)
	}
	if inst.Version != 2 {
		t.Fatalf("version = %d, want 2", inst.Version)
	}

	finished := h.mustApprove(inst.Id, task.Id, 41)
	if finished.Status != InstanceApproved {
		t.Fatalf("status = %s, want approved", finished.Status)
	}
	if finished.CurrentKey != "" {
		t.Fatalf("current key = %s, want empty", finished.CurrentKey)
	}
	if finished.FinishedAt == nil {
		t.Fatal("finished at is nil")
	}
	if len(finished.PendingTasks()) != 0 {
		t.Fatalf("pending tasks = %d, want 0", len(finished.PendingTasks()))
	}
	if !hasLog(finished, "finish") {
		t.Fatalf("expected finish log, logs=%+v", finished.Logs)
	}
}

func TestStartRejectsInvalidRequest(t *testing.T) {
	h := newHarness(t)
	_, err := h.eng.Start(context.Background(), linearApprovalDef(), StartRequest{BizId: "b", SubmitterId: 0})
	requireError(t, err, ErrInvalidRequest, "submitter 0")
	_, err = h.eng.Start(context.Background(), linearApprovalDef(), StartRequest{BizId: "", SubmitterId: 1})
	requireError(t, err, ErrInvalidRequest, "empty biz id")
	_, err = h.eng.Start(context.Background(), nil, StartRequest{BizId: "b", SubmitterId: 1})
	requireError(t, err, ErrInvalidDefinition, "nil definition")
}

func TestStartPropagatesStoreError(t *testing.T) {
	h := newHarness(t)
	failing := &failingCreateStore{Store: h.store}
	h.eng = NewEngine(failing, Options{Now: h.clock.Now, IDGen: h.ids.Next})
	_, err := h.eng.Start(context.Background(), linearApprovalDef(), StartRequest{BizId: "b", SubmitterId: 1})
	if err == nil || err.Error() != "create refused" {
		t.Fatalf("err = %v, want create refused", err)
	}
}

type failingCreateStore struct {
	Store
}

func (s *failingCreateStore) Create(ctx context.Context, inst *Instance) error {
	return errors.New("create refused")
}

type failingListStore struct {
	Store
	err error
}

func (s *failingListStore) ListByBiz(ctx context.Context, tenantId int64, bizId string) ([]*Instance, error) {
	return nil, s.err
}

func TestHandleTimeoutReturnsStoreError(t *testing.T) {
	h := newHarness(t)
	startInstance(h, timeoutDef(), "biz-scan-err", 1, nil)
	sentinel := errors.New("scan refused")
	h.eng = NewEngine(&failingListStore{Store: h.store, err: sentinel}, Options{Now: h.clock.Now, IDGen: h.ids.Next})
	handled, err := h.eng.HandleTimeout(context.Background(), h.clock.Now().Add(2*time.Hour))
	requireError(t, err, sentinel, "timeout scan error")
	if handled != 0 {
		t.Fatalf("handled = %d, want 0", handled)
	}
}

func TestHandleTimeoutKeepsStaleCandidatesUntouched(t *testing.T) {
	h := newHarness(t)
	inst := startInstance(h, timeoutDef(), "biz-stale", 1, nil)
	task := pendingOf(t, inst, "a")
	h.mustApprove(inst.Id, task.Id, 91)
	handled := h.timeouts(h.clock.Now().Add(5 * time.Hour))
	if handled != 0 {
		t.Fatalf("handled = %d, want 0", handled)
	}
	after := h.mustGet(inst.Id)
	if countLogs(after, "timeout") != 0 {
		t.Fatalf("timeout logs = %d", countLogs(after, "timeout"))
	}
}

func TestApproveAnySignPassesWithSingleApprover(t *testing.T) {
	h := newHarness(t)
	inst := startInstance(h, linearApprovalDef(), "biz-any", 1, nil)
	task := pendingOf(t, inst, "a")

	done := h.mustApprove(inst.Id, task.Id, 11)
	if done.Status != InstanceApproved {
		t.Fatalf("status = %s, want approved", done.Status)
	}
	if countTasks(done, "a", TaskApproved) != 1 {
		t.Fatalf("approved tasks = %s", describeTasks(done))
	}
	if countTasks(done, "a", TaskCanceled) != 1 {
		t.Fatalf("expected the peer task canceled, tasks = %s", describeTasks(done))
	}
	peer := done.Tasks[0]
	if peer.Id == task.Id {
		peer = done.Tasks[1]
	}
	if peer.ApproverId != 12 || peer.Status != TaskCanceled {
		t.Fatalf("peer task = %+v", peer)
	}
}

func TestApproveRejectsWrongActorAndHandledTask(t *testing.T) {
	h := newHarness(t)
	inst := startInstance(h, linearApprovalDef(), "biz-guard", 1, nil)
	task := pendingOf(t, inst, "a")

	_, err := h.eng.Approve(context.Background(), inst.Id, task.Id, 99, "")
	requireError(t, err, ErrNotApprover, "wrong actor")
	_, err = h.eng.Approve(context.Background(), inst.Id, "task-missing", 11, "")
	requireError(t, err, ErrTaskNotFound, "missing task")

	done := h.mustApprove(inst.Id, task.Id, 11)
	_, err = h.eng.Approve(context.Background(), inst.Id, task.Id, 11, "")
	requireError(t, err, ErrInvalidState, "approve on finished instance")
	_, err = h.eng.Approve(context.Background(), done.Id, done.Tasks[1].Id, 12, "")
	requireError(t, err, ErrInvalidState, "approve canceled peer task")
	_, err = h.eng.Approve(context.Background(), "inst-missing", task.Id, 11, "")
	requireError(t, err, ErrInstanceNotFound, "unknown instance")
}

func TestApproveAllSignWaitsForEveryApprover(t *testing.T) {
	h := newHarness(t)
	inst := startInstance(h, signAllDef(11, 12, 13), "biz-all", 1, nil)
	if len(inst.PendingTasks()) != 3 {
		t.Fatalf("pending tasks = %s", describeTasks(inst))
	}

	first := taskByApprover(t, inst, "a", 11)
	afterFirst := h.mustApprove(inst.Id, first.Id, 11)
	if afterFirst.Status != InstanceRunning || afterFirst.CurrentKey != "a" {
		t.Fatalf("after first approve status=%s current=%s", afterFirst.Status, afterFirst.CurrentKey)
	}
	if len(afterFirst.PendingTasks()) != 2 {
		t.Fatalf("pending tasks = %s", describeTasks(afterFirst))
	}

	second := taskByApprover(t, afterFirst, "a", 12)
	afterSecond := h.mustApprove(afterFirst.Id, second.Id, 12)
	if afterSecond.Status != InstanceRunning || afterSecond.CurrentKey != "a" {
		t.Fatalf("after second approve status=%s current=%s", afterSecond.Status, afterSecond.CurrentKey)
	}

	third := taskByApprover(t, afterSecond, "a", 13)
	moved := h.mustApprove(afterSecond.Id, third.Id, 13)
	if moved.CurrentKey != "b" || moved.Status != InstanceRunning {
		t.Fatalf("after third approve status=%s current=%s", moved.Status, moved.CurrentKey)
	}
	if len(moved.PendingTasks()) != 1 {
		t.Fatalf("pending tasks = %s", describeTasks(moved))
	}

	last := pendingOf(t, moved, "b")
	if last.ApproverId != 99 {
		t.Fatalf("manager task = %+v", last)
	}
	finished := h.mustApprove(moved.Id, last.Id, 99)
	if finished.Status != InstanceApproved {
		t.Fatalf("status = %s, want approved", finished.Status)
	}
}

func TestApproveSequenceSignActivatesOneApproverAtATime(t *testing.T) {
	h := newHarness(t)
	inst := startInstance(h, sequenceDef(), "biz-seq", 1, nil)

	pending := inst.PendingTasks()
	if len(pending) != 1 {
		t.Fatalf("pending tasks = %s", describeTasks(inst))
	}
	if pending[0].ApproverId != 51 {
		t.Fatalf("first approver = %d, want 51", pending[0].ApproverId)
	}

	first := h.mustApprove(inst.Id, pending[0].Id, 51)
	if first.CurrentKey != "a" || first.Status != InstanceRunning {
		t.Fatalf("after first status=%s current=%s", first.Status, first.CurrentKey)
	}
	second := taskByApprover(t, first, "a", 52)
	if len(first.PendingTasks()) != 1 {
		t.Fatalf("pending tasks = %s", describeTasks(first))
	}

	afterSecond := h.mustApprove(first.Id, second.Id, 52)
	third := taskByApprover(t, afterSecond, "a", 53)

	finished := h.mustApprove(afterSecond.Id, third.Id, 53)
	if finished.Status != InstanceApproved {
		t.Fatalf("status = %s, want approved", finished.Status)
	}
	if countTasks(finished, "a", TaskApproved) != 3 {
		t.Fatalf("tasks = %s", describeTasks(finished))
	}

	_, err := h.eng.Approve(context.Background(), finished.Id, "task-2", 52, "")
	requireError(t, err, ErrInvalidState, "approve after finish")
}

func TestApproveSequenceSignRejectsOutOfOrderApprover(t *testing.T) {
	h := newHarness(t)
	inst := startInstance(h, sequenceDef(), "biz-seq-order", 1, nil)
	other, ok := inst.FindTask("task-2")
	if ok {
		_, err := h.eng.Approve(context.Background(), inst.Id, other.Id, 52, "")
		requireError(t, err, ErrNotApprover, "out of order approver")
	}
	_, err := h.eng.Approve(context.Background(), inst.Id, inst.PendingTasks()[0].Id, 53, "")
	requireError(t, err, ErrNotApprover, "later approver on active task")
}

func TestConditionBranchesAndFallback(t *testing.T) {
	cases := []struct {
		name      string
		variables map[string]string
		nodeKey   string
		approver  int64
	}{
		{name: "amount wins", variables: vars("amount", "5000"), nodeKey: "big", approver: 31},
		{name: "department match", variables: vars("amount", "10", "dept", "sales"), nodeKey: "middle", approver: 32},
		{name: "default end", variables: vars("amount", "10"), nodeKey: "", approver: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			inst := startInstance(h, conditionDef(), "biz-cond", 1, tc.variables)
			if tc.nodeKey == "" {
				if inst.Status != InstanceApproved {
					t.Fatalf("status = %s, want approved", inst.Status)
				}
				if len(inst.Tasks) != 0 {
					t.Fatalf("tasks = %s", describeTasks(inst))
				}
				return
			}
			if inst.CurrentKey != tc.nodeKey {
				t.Fatalf("current = %s, want %s", inst.CurrentKey, tc.nodeKey)
			}
			task := pendingOf(t, inst, tc.nodeKey)
			if task.ApproverId != tc.approver {
				t.Fatalf("approver = %d, want %d", task.ApproverId, tc.approver)
			}
			finished := h.mustApprove(inst.Id, task.Id, tc.approver)
			if finished.Status != InstanceApproved {
				t.Fatalf("status = %s, want approved", finished.Status)
			}
		})
	}
}

func TestConditionBranchOrderAndFallbackNext(t *testing.T) {
	h := newHarness(t)
	urgent := startInstance(h, fallbackBranchDef(), "biz-branch-urgent", 1, vars("urgent", "true"))
	if urgent.CurrentKey != "fast" {
		t.Fatalf("current = %s, want fast", urgent.CurrentKey)
	}
	slow := startInstance(h, fallbackBranchDef(), "biz-branch-slow", 1, vars("urgent", "false"))
	if slow.CurrentKey != "slow" {
		t.Fatalf("current = %s, want slow", slow.CurrentKey)
	}
	missing := startInstance(h, fallbackBranchDef(), "biz-branch-missing", 1, nil)
	if missing.CurrentKey != "slow" {
		t.Fatalf("current = %s, want slow", missing.CurrentKey)
	}
}

func TestConditionBranchMatrix(t *testing.T) {
	cases := []struct {
		variables map[string]string
		want      string
	}{
		{variables: vars("level", "1"), want: "l1"},
		{variables: vars("level", "2"), want: "l2"},
		{variables: vars("level", "3"), want: "l3"},
		{variables: vars("level", "9"), want: ""},
	}
	for _, tc := range cases {
		h := newHarness(t)
		inst := startInstance(h, branchDef(), "biz-matrix", 1, tc.variables)
		if inst.CurrentKey != tc.want {
			t.Fatalf("variables %v: current = %s, want %s", tc.variables, inst.CurrentKey, tc.want)
		}
	}
}

func TestRejectWithoutTargetTerminatesInstance(t *testing.T) {
	h := newHarness(t)
	inst := startInstance(h, multiLevelDef(), "biz-reject", 1, nil)
	task := pendingOf(t, inst, "a")

	rejected := h.mustReject(inst.Id, task.Id, 11, "")
	if rejected.Status != InstanceRejected {
		t.Fatalf("status = %s, want rejected", rejected.Status)
	}
	if rejected.FinishedAt == nil || rejected.CurrentKey != "" {
		t.Fatalf("finished at=%v current=%s", rejected.FinishedAt, rejected.CurrentKey)
	}
	if len(rejected.PendingTasks()) != 0 {
		t.Fatalf("pending tasks = %s", describeTasks(rejected))
	}
	if countTasks(rejected, "a", TaskSkipped) != 1 {
		t.Fatalf("tasks = %s", describeTasks(rejected))
	}
	if !hasLog(rejected, "reject") {
		t.Fatalf("logs = %+v", rejected.Logs)
	}
	_, err := h.eng.Approve(context.Background(), rejected.Id, task.Id, 11, "")
	requireError(t, err, ErrInvalidState, "approve rejected instance")
}

func TestRejectWithTargetRollsBackAndReplays(t *testing.T) {
	h := newHarness(t)
	inst := startInstance(h, rollbackDef(), "biz-rollback", 1, nil)
	leaderTask := pendingOf(t, inst, "a")
	atManager := h.mustApprove(inst.Id, leaderTask.Id, 11)
	if atManager.CurrentKey != "b" {
		t.Fatalf("current = %s, want b", atManager.CurrentKey)
	}
	managerTask := pendingOf(t, atManager, "b")

	rolledBack := h.mustReject(atManager.Id, managerTask.Id, 21, "a")
	if rolledBack.Status != InstanceRunning {
		t.Fatalf("status = %s, want running", rolledBack.Status)
	}
	if rolledBack.CurrentKey != "a" {
		t.Fatalf("current = %s, want a", rolledBack.CurrentKey)
	}
	if countTasks(rolledBack, "a", TaskPending) != 1 {
		t.Fatalf("old round task not reset: %s", describeTasks(rolledBack))
	}
	if countTasks(rolledBack, "b", TaskSkipped) != 1 {
		t.Fatalf("manager task not terminated by rollback: %s", describeTasks(rolledBack))
	}
	newLeader := pendingOf(t, rolledBack, "a")
	if newLeader.Id == leaderTask.Id {
		t.Fatalf("rollback reused task id %s", newLeader.Id)
	}
	if newLeader.round == taskRound(rolledBack, leaderTask.Id) {
		t.Fatalf("rollback did not open a new round: %s", describeTasks(rolledBack))
	}
	if logFor(rolledBack, "reject", managerTask.Id) == nil {
		t.Fatalf("missing reject log: %+v", rolledBack.Logs)
	}
	if !hasLog(rolledBack, "rollback") {
		t.Fatalf("missing rollback log: %+v", rolledBack.Logs)
	}

	again := h.mustApprove(rolledBack.Id, newLeader.Id, 11)
	newManager := pendingOf(t, again, "b")
	final := h.mustApprove(again.Id, newManager.Id, 21)
	if final.Status != InstanceApproved {
		t.Fatalf("status = %s, want approved", final.Status)
	}
	if countLogs(final, "approve") != 3 {
		t.Fatalf("approve logs = %d, logs=%+v", countLogs(final, "approve"), final.Logs)
	}
}

func TestRejectRollbackAlsoCancelsDeeperPendingTasks(t *testing.T) {
	h := newHarness(t)
	definition := def("deep", "s",
		node("s", NodeStart, next("a")),
		node("a", NodeApproval, named("leader"), approvers(11), sign(SignAny), next("b")),
		node("b", NodeApproval, named("manager"), approvers(21), sign(SignAll), next("c")),
		node("c", NodeApproval, named("finance"), approvers(31), sign(SignAny), next("e")),
		node("e", NodeEnd),
	)
	inst := startInstance(h, definition, "biz-deep", 1, nil)
	atB := h.mustApprove(inst.Id, pendingOf(t, inst, "a").Id, 11)
	atC := h.mustApprove(atB.Id, pendingOf(t, atB, "b").Id, 21)
	rolledBack := h.mustReject(atC.Id, pendingOf(t, atC, "c").Id, 31, "b")
	if rolledBack.CurrentKey != "b" {
		t.Fatalf("current = %s, want b", rolledBack.CurrentKey)
	}
	if countTasks(rolledBack, "c", TaskSkipped) != 1 {
		t.Fatalf("deeper pending task not canceled: %s", describeTasks(rolledBack))
	}
	newB := pendingOf(t, rolledBack, "b")
	final := h.mustApprove(rolledBack.Id, newB.Id, 21)
	if final.CurrentKey != "c" {
		t.Fatalf("current = %s, want c", final.CurrentKey)
	}
	if len(final.PendingTasks()) != 1 {
		t.Fatalf("pending tasks = %s", describeTasks(final))
	}
}

func TestRejectRollbackValidation(t *testing.T) {
	h := newHarness(t)
	inst := startInstance(h, rollbackDef(), "biz-rollback-guard", 1, nil)
	task := pendingOf(t, inst, "a")

	_, err := h.eng.Reject(context.Background(), inst.Id, task.Id, 11, "", "a")
	requireError(t, err, ErrInvalidRequest, "rollback to current node")
	_, err = h.eng.Reject(context.Background(), inst.Id, task.Id, 11, "", "missing")
	requireError(t, err, ErrDefinitionNotFound, "rollback to missing node")
	_, err = h.eng.Reject(context.Background(), inst.Id, task.Id, 11, "", "e")
	requireError(t, err, ErrInvalidState, "rollback to end node")
	_, err = h.eng.Reject(context.Background(), inst.Id, task.Id, 12, "", "b")
	requireError(t, err, ErrNotApprover, "rollback by stranger")

	afterFailedAttempts := h.mustGet(inst.Id)
	if afterFailedAttempts.Status != InstanceRunning || afterFailedAttempts.CurrentKey != "a" {
		t.Fatalf("failed reject mutated instance: %+v", afterFailedAttempts)
	}
}

func TestTransferChangesApproverAndRespectsNodeFlag(t *testing.T) {
	h := newHarness(t)
	inst := startInstance(h, transferDef(), "biz-transfer", 1, nil)
	task := pendingOf(t, inst, "a")

	transferred := h.mustTransfer(inst.Id, task.Id, 61, 62)
	updated, ok := transferred.FindTask(task.Id)
	if !ok {
		t.Fatalf("task %s missing", task.Id)
	}
	if updated.ApproverId != 62 {
		t.Fatalf("approver = %d, want 62", updated.ApproverId)
	}
	if logFor(transferred, "transfer", task.Id) == nil {
		t.Fatalf("missing transfer log: %+v", transferred.Logs)
	}

	_, err := h.eng.Approve(context.Background(), inst.Id, task.Id, 61, "")
	requireError(t, err, ErrNotApprover, "original approver lost rights")
	finished := h.mustApprove(inst.Id, task.Id, 62)
	if finished.Status != InstanceApproved {
		t.Fatalf("status = %s, want approved", finished.Status)
	}

	locked := newHarness(t)
	lockedInst := startInstance(locked, lockedApprovalDef(), "biz-locked", 1, nil)
	lockedTask := pendingOf(t, lockedInst, "a")
	_, err = locked.eng.Transfer(context.Background(), lockedInst.Id, lockedTask.Id, 61, 62, "")
	requireError(t, err, ErrNodeNotAllowed, "transfer not allowed")
	_, err = locked.eng.Transfer(context.Background(), lockedInst.Id, lockedTask.Id, 61, 0, "")
	requireError(t, err, ErrInvalidRequest, "invalid target member")
	_, err = locked.eng.Transfer(context.Background(), lockedInst.Id, lockedTask.Id, 12, 61, "")
	requireError(t, err, ErrNotApprover, "transfer by non approver")
	_, err = locked.eng.Transfer(context.Background(), lockedInst.Id, lockedTask.Id, 61, 61, "")
	requireError(t, err, ErrInvalidRequest, "transfer to the same member")
	_, err = h.eng.Transfer(context.Background(), inst.Id, task.Id, 62, 63, "")
	requireError(t, err, ErrInvalidState, "transfer on finished instance")
}

func TestAddSignerBeforeCurrentApprover(t *testing.T) {
	h := newHarness(t)
	inst := startInstance(h, addSignDef(), "biz-before", 1, nil)
	task := pendingOf(t, inst, "a")

	added := h.mustAddSigner(inst.Id, task.Id, 61, true, []int64{71, 72})
	if countTasks(added, "a", TaskPending) != 3 {
		t.Fatalf("pending tasks = %s", describeTasks(added))
	}
	current, _ := added.FindTask(task.Id)
	first := taskByApprover(t, added, "a", 71)
	second := taskByApprover(t, added, "a", 72)
	if !(first.Seq < second.Seq && second.Seq < current.Seq) {
		t.Fatalf("seq order wrong: %s", describeTasks(added))
	}
	if logFor(added, "addsign", task.Id) == nil {
		t.Fatalf("missing addsign log: %+v", added.Logs)
	}

	afterFirst := h.mustApprove(added.Id, first.Id, 71)
	if afterFirst.Status != InstanceRunning || len(afterFirst.PendingTasks()) != 2 {
		t.Fatalf("state = %s tasks = %s", afterFirst.Status, describeTasks(afterFirst))
	}
	afterSecond := h.mustApprove(afterFirst.Id, second.Id, 72)
	if afterSecond.Status != InstanceRunning || len(afterSecond.PendingTasks()) != 1 {
		t.Fatalf("state = %s tasks = %s", afterSecond.Status, describeTasks(afterSecond))
	}
	final := h.mustApprove(afterSecond.Id, task.Id, 61)
	if final.Status != InstanceApproved {
		t.Fatalf("status = %s, want approved tasks=%s", final.Status, describeTasks(final))
	}
}

func TestAddSignerAfterCurrentApproverWaitsItsTurn(t *testing.T) {
	h := newHarness(t)
	inst := startInstance(h, addSignDef(), "biz-after", 1, nil)
	task := pendingOf(t, inst, "a")

	added := h.mustAddSigner(inst.Id, task.Id, 61, false, []int64{73})
	if countTasks(added, "a", TaskPending) != 2 {
		t.Fatalf("pending tasks = %s", describeTasks(added))
	}
	current, _ := added.FindTask(task.Id)
	extra := taskByApprover(t, added, "a", 73)
	if extra.Seq <= current.Seq {
		t.Fatalf("after-sign task must sort later: %s", describeTasks(added))
	}

	stillRunning := h.mustApprove(added.Id, task.Id, 61)
	if stillRunning.Status != InstanceRunning {
		t.Fatalf("after-sign should hold the node open, status = %s tasks = %s", stillRunning.Status, describeTasks(stillRunning))
	}
	if len(stillRunning.PendingTasks()) != 1 {
		t.Fatalf("pending tasks = %s", describeTasks(stillRunning))
	}
	finished := h.mustApprove(stillRunning.Id, extra.Id, 73)
	if finished.Status != InstanceApproved {
		t.Fatalf("status = %s, want approved", finished.Status)
	}
}

func TestAddSignerGuards(t *testing.T) {
	h := newHarness(t)
	inst := startInstance(h, addSignDef(), "biz-addsign-guard", 1, nil)
	task := pendingOf(t, inst, "a")

	_, err := h.eng.AddSigner(context.Background(), inst.Id, task.Id, 61, true, nil, "")
	requireError(t, err, ErrInvalidRequest, "no members")
	_, err = h.eng.AddSigner(context.Background(), inst.Id, task.Id, 61, true, []int64{0}, "")
	requireError(t, err, ErrInvalidRequest, "zero member")
	_, err = h.eng.AddSigner(context.Background(), inst.Id, task.Id, 62, true, []int64{71}, "")
	requireError(t, err, ErrNotApprover, "actor not approver")
	_, err = h.eng.AddSigner(context.Background(), inst.Id, "task-999", 61, true, []int64{71}, "")
	requireError(t, err, ErrTaskNotFound, "unknown task")

	locked := newHarness(t)
	lockedInst := startInstance(locked, lockedApprovalDef(), "biz-addsign-locked", 1, nil)
	lockedTask := pendingOf(t, lockedInst, "a")
	_, err = locked.eng.AddSigner(context.Background(), lockedInst.Id, lockedTask.Id, 61, true, []int64{71}, "")
	requireError(t, err, ErrNodeNotAllowed, "add sign not allowed")
}

func TestWithdrawOnlyBySubmitterWhileRunning(t *testing.T) {
	h := newHarness(t)
	inst := startInstance(h, multiLevelDef(), "biz-withdraw", 42, nil)

	_, err := h.eng.Withdraw(context.Background(), inst.Id, 43, "")
	requireError(t, err, ErrNotSubmitter, "withdraw by other member")

	canceled := h.mustWithdraw(inst.Id, 42)
	if canceled.Status != InstanceCanceled {
		t.Fatalf("status = %s, want canceled", canceled.Status)
	}
	if canceled.FinishedAt == nil || canceled.CurrentKey != "" {
		t.Fatalf("finished at=%v current=%s", canceled.FinishedAt, canceled.CurrentKey)
	}
	if countTasks(canceled, "a", TaskCanceled) != 2 {
		t.Fatalf("tasks = %s", describeTasks(canceled))
	}
	if !hasLog(canceled, "withdraw") {
		t.Fatalf("logs = %+v", canceled.Logs)
	}
	_, err = h.eng.Withdraw(context.Background(), inst.Id, 42, "")
	requireError(t, err, ErrInvalidState, "withdraw twice")

	finished := startInstance(h, linearApprovalDef(), "biz-withdraw-done", 1, nil)
	doneTask := pendingOf(t, finished, "a")
	h.mustApprove(finished.Id, doneTask.Id, 11)
	_, err = h.eng.Withdraw(context.Background(), finished.Id, 1, "")
	requireError(t, err, ErrInvalidState, "withdraw approved instance")
}

func TestHandleTimeoutAutoPassesAnySignNode(t *testing.T) {
	h := newHarness(t)
	inst := startInstance(h, timeoutDef(), "biz-timeout", 1, nil)
	task := pendingOf(t, inst, "a")
	if task.TimeoutAt == nil {
		t.Fatal("timeout at is nil")
	}

	tooEarly := h.timeouts(h.clock.Now().Add(30 * time.Minute))
	if tooEarly != 0 {
		t.Fatalf("handled = %d, want 0", tooEarly)
	}

	handled := h.timeouts(h.clock.Now().Add(2 * time.Hour))
	if handled != 1 {
		t.Fatalf("handled = %d, want 1", handled)
	}
	after := h.mustGet(inst.Id)
	if after.Status != InstanceApproved {
		t.Fatalf("status = %s, want approved tasks=%s", after.Status, describeTasks(after))
	}
	timedOut, _ := after.FindTask(task.Id)
	if timedOut.Status != TaskSkipped {
		t.Fatalf("task status = %s, want skipped", timedOut.Status)
	}
	if timedOut.HandledAt == nil {
		t.Fatal("handled at is nil")
	}
	if logFor(after, "timeout", task.Id) == nil {
		t.Fatalf("missing timeout log: %+v", after.Logs)
	}
}

func TestHandleTimeoutAutoPassesAllSignNode(t *testing.T) {
	h := newHarness(t)
	inst := startInstance(h, allTimeoutDef(), "biz-timeout-all", 1, nil)
	if len(inst.PendingTasks()) != 2 {
		t.Fatalf("pending tasks = %s", describeTasks(inst))
	}

	handled := h.timeouts(h.clock.Now().Add(2 * time.Hour))
	if handled != 2 {
		t.Fatalf("handled = %d, want 2", handled)
	}
	after := h.mustGet(inst.Id)
	if after.Status != InstanceApproved {
		t.Fatalf("status = %s, want approved tasks=%s", after.Status, describeTasks(after))
	}
	if countTasks(after, "a", TaskSkipped) != 2 {
		t.Fatalf("tasks = %s", describeTasks(after))
	}
	if countLogs(after, "timeout") != 2 {
		t.Fatalf("timeout logs = %d", countLogs(after, "timeout"))
	}
}

func TestHandleTimeoutAdvancesSequenceChain(t *testing.T) {
	h := newHarness(t)
	inst := startInstance(h, sequenceTimeoutDef(), "biz-timeout-seq", 1, nil)
	if pendingOf(t, inst, "a").ApproverId != 91 {
		t.Fatalf("tasks = %s", describeTasks(inst))
	}

	first := h.timeouts(h.clock.Now().Add(2 * time.Hour))
	if first != 1 {
		t.Fatalf("handled = %d, want 1", first)
	}
	afterFirst := h.mustGet(inst.Id)
	if afterFirst.Status != InstanceRunning || afterFirst.CurrentKey != "a" {
		t.Fatalf("status = %s current = %s", afterFirst.Status, afterFirst.CurrentKey)
	}
	advanced := pendingOf(t, afterFirst, "a")
	if advanced.ApproverId != 92 {
		t.Fatalf("next approver = %d, want 92 tasks=%s", advanced.ApproverId, describeTasks(afterFirst))
	}

	second := h.timeouts(h.clock.Now().Add(3 * time.Hour))
	if second != 1 {
		t.Fatalf("handled = %d, want 1", second)
	}
	final := h.mustGet(inst.Id)
	if final.Status != InstanceApproved {
		t.Fatalf("status = %s, want approved tasks=%s", final.Status, describeTasks(final))
	}
	if countTasks(final, "a", TaskSkipped) != 2 {
		t.Fatalf("tasks = %s", describeTasks(final))
	}
	if countLogs(final, "timeout") != 2 {
		t.Fatalf("timeout logs = %d", countLogs(final, "timeout"))
	}
}

func TestHandleTimeoutScansEveryRunningInstance(t *testing.T) {
	h := newHarness(t)
	first := startInstance(h, timeoutDef(), "biz-scan-1", 1, nil)
	second := startInstance(h, timeoutDef(), "biz-scan-2", 1, nil)

	handled := h.timeouts(h.clock.Now().Add(2 * time.Hour))
	if handled != 2 {
		t.Fatalf("handled = %d, want 2", handled)
	}
	for _, id := range []string{first.Id, second.Id} {
		inst := h.mustGet(id)
		if inst.Status != InstanceApproved {
			t.Fatalf("instance %s status = %s", id, inst.Status)
		}
	}
	again := h.timeouts(h.clock.Now().Add(3 * time.Hour))
	if again != 0 {
		t.Fatalf("handled = %d, want 0", again)
	}
}

func TestHandleTimeoutSkipsCanceledAndFinishedInstances(t *testing.T) {
	h := newHarness(t)
	inst := startInstance(h, timeoutDef(), "biz-timeout-canceled", 1, nil)
	h.mustWithdraw(inst.Id, 1)
	handled := h.timeouts(h.clock.Now().Add(5 * time.Hour))
	if handled != 0 {
		t.Fatalf("handled = %d, want 0", handled)
	}
	after := h.mustGet(inst.Id)
	if after.Status != InstanceCanceled {
		t.Fatalf("status = %s, want canceled", after.Status)
	}
}

func TestHandleTimeoutWithoutDeadlineDoesNothing(t *testing.T) {
	h := newHarness(t)
	inst := startInstance(h, linearApprovalDef(), "biz-no-deadline", 1, nil)
	task := pendingOf(t, inst, "a")
	if task.TimeoutAt != nil {
		t.Fatalf("timeout at = %v, want nil", task.TimeoutAt)
	}
	handled := h.timeouts(h.clock.Now().Add(1000 * time.Hour))
	if handled != 0 {
		t.Fatalf("handled = %d, want 0", handled)
	}
	if h.mustGet(inst.Id).Status != InstanceRunning {
		t.Fatalf("status = %s, want running", h.mustGet(inst.Id).Status)
	}
}

func TestConcurrentApproveLosesNoUpdate(t *testing.T) {
	h := newHarness(t)
	inst := startInstance(h, signAllDef(11, 12), "biz-concurrent", 1, nil)
	first := taskByApprover(t, inst, "a", 11)
	second := taskByApprover(t, inst, "a", 12)
	staleVersion := inst.Version

	final := h.mustApprove(inst.Id, first.Id, 11)
	if final.Status != InstanceRunning {
		t.Fatalf("status = %s, want running", final.Status)
	}
	err := h.store.Update(context.Background(), inst.Id, staleVersion, func(*Instance) error {
		return nil
	})
	requireError(t, err, ErrVersionConflict, "stale version")

	current := h.mustGet(inst.Id)
	if countTasks(current, "a", TaskApproved) != 1 {
		t.Fatalf("tasks = %s", describeTasks(current))
	}
	pendingSecond := taskByApprover(t, current, "a", 12)
	if pendingSecond.Id != second.Id {
		t.Fatalf("second task changed: %+v", pendingSecond)
	}
	done := h.mustApprove(current.Id, pendingSecond.Id, 12)
	if done.Status != InstanceRunning || done.CurrentKey != "b" {
		t.Fatalf("status = %s current = %s tasks = %s", done.Status, done.CurrentKey, describeTasks(done))
	}
	last := pendingOf(t, done, "b")
	approved := h.mustApprove(done.Id, last.Id, 99)
	if approved.Status != InstanceApproved {
		t.Fatalf("status = %s, want approved", approved.Status)
	}
}

func TestConcurrentApproveSameTaskKeepsSingleWinner(t *testing.T) {
	h := newHarness(t)
	inst := startInstance(h, linearApprovalDef(), "biz-race", 1, nil)
	task := pendingOf(t, inst, "a")
	versionBefore := inst.Version

	var wg sync.WaitGroup
	results := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, err := h.eng.Approve(context.Background(), inst.Id, task.Id, 11, "")
			results[idx] = err
		}(i)
	}
	wg.Wait()

	success := 0
	conflict := 0
	for _, err := range results {
		if err == nil {
			success++
			continue
		}
		if errors.Is(err, ErrVersionConflict) || errors.Is(err, ErrTaskHandled) || errors.Is(err, ErrInvalidState) {
			conflict++
			continue
		}
		t.Fatalf("unexpected error: %v", err)
	}
	if success+conflict != 2 {
		t.Fatalf("success=%d conflict=%d", success, conflict)
	}
	final := h.mustGet(inst.Id)
	if final.Version != versionBefore+1 || countTasks(final, "a", TaskApproved) != 1 {
		t.Fatalf("tasks = %s version = %d", describeTasks(final), final.Version)
	}
}

func TestGetAndListByBiz(t *testing.T) {
	h := newHarness(t)
	first := startInstance(h, linearApprovalDef(), "biz-list", 1, nil)
	h.clock.Advance(time.Minute)
	second := startInstance(h, linearApprovalDef(), "biz-list", 1, nil)
	h.clock.Advance(time.Minute)
	other := startInstance(h, multiLevelDef(), "biz-other", 1, nil)

	listed, err := h.eng.ListByBiz(context.Background(), 7, "biz-list")
	if err != nil {
		t.Fatalf("list failed: %v", err)
	}
	if len(listed) != 2 {
		t.Fatalf("listed = %d, want 2", len(listed))
	}
	if listed[0].Id != first.Id || listed[1].Id != second.Id {
		t.Fatalf("ordering wrong: %s %s", listed[0].Id, listed[1].Id)
	}

	all, err := h.eng.ListByBiz(context.Background(), 7, "")
	if err != nil {
		t.Fatalf("list all failed: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("all = %d, want 3", len(all))
	}
	otherTenant, err := h.eng.ListByBiz(context.Background(), 8, "")
	if err != nil {
		t.Fatalf("list tenant failed: %v", err)
	}
	if len(otherTenant) != 0 {
		t.Fatalf("other tenant = %d, want 0", len(otherTenant))
	}

	_, err = h.eng.Get(context.Background(), "missing")
	requireError(t, err, ErrInstanceNotFound, "get missing instance")

	got := h.mustGet(other.Id)
	got.BizId = "tampered"
	got.Tasks = nil
	reloaded := h.mustGet(other.Id)
	if reloaded.BizId != "biz-other" || len(reloaded.Tasks) == 0 {
		t.Fatalf("store leaked internal state: %+v", reloaded)
	}
}

func TestListPendingTasksByApprover(t *testing.T) {
	h := newHarness(t)
	inst := startInstance(h, signAllDef(11, 12, 13), "biz-pending", 1, nil)

	tasks, err := h.eng.ListPendingTasks(context.Background(), 11, 7)
	if err != nil {
		t.Fatalf("list pending failed: %v", err)
	}
	if len(tasks) != 1 || tasks[0].ApproverId != 11 {
		t.Fatalf("tasks = %+v", tasks)
	}
	all, err := h.eng.ListPendingTasks(context.Background(), 0, 7)
	if err != nil {
		t.Fatalf("list all pending failed: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("all pending = %d, want 3", len(all))
	}
	nothing, err := h.eng.ListPendingTasks(context.Background(), 11, 99)
	if err != nil {
		t.Fatalf("list other tenant failed: %v", err)
	}
	if len(nothing) != 0 {
		t.Fatalf("other tenant pending = %d, want 0", len(nothing))
	}

	h.mustApprove(inst.Id, taskByApprover(t, inst, "a", 11).Id, 11)
	after, err := h.eng.ListPendingTasks(context.Background(), 11, 7)
	if err != nil {
		t.Fatalf("list after approve failed: %v", err)
	}
	if len(after) != 0 {
		t.Fatalf("after approve pending = %d, want 0", len(after))
	}
}

func TestStoreUpdateIsTransactional(t *testing.T) {
	h := newHarness(t)
	inst := startInstance(h, linearApprovalDef(), "biz-tx", 1, nil)
	before := h.mustGet(inst.Id)

	sentinel := errors.New("mutate failed")
	err := h.store.Update(context.Background(), inst.Id, before.Version, func(live *Instance) error {
		live.BizId = "changed"
		live.Status = InstanceApproved
		return sentinel
	})
	requireError(t, err, sentinel, "mutate error propagates")

	after := h.mustGet(inst.Id)
	if after.BizId != "biz-tx" || after.Status != InstanceRunning {
		t.Fatalf("failed mutate leaked: %+v", after)
	}
	if after.Version != before.Version {
		t.Fatalf("version moved from %d to %d", before.Version, after.Version)
	}

	okErr := h.store.Update(context.Background(), inst.Id, before.Version, func(live *Instance) error {
		live.BizId = "biz-tx-2"
		return nil
	})
	if okErr != nil {
		t.Fatalf("update failed: %v", okErr)
	}
	final := h.mustGet(inst.Id)
	if final.BizId != "biz-tx-2" || final.Version != before.Version+1 {
		t.Fatalf("final = %+v", final)
	}

	_, err = h.store.Get(context.Background(), "missing")
	requireError(t, err, ErrInstanceNotFound, "store get missing")
	err = h.store.Update(context.Background(), "missing", 1, func(*Instance) error { return nil })
	requireError(t, err, ErrInstanceNotFound, "store update missing")
	err = h.store.Update(context.Background(), inst.Id, final.Version, nil)
	requireError(t, err, ErrInvalidRequest, "nil mutate")
	err = h.store.Create(context.Background(), final)
	requireError(t, err, ErrInvalidRequest, "duplicated create")
	err = h.store.Create(context.Background(), nil)
	requireError(t, err, ErrInvalidRequest, "nil instance")
	err = h.store.Create(context.Background(), &Instance{})
	requireError(t, err, ErrInvalidRequest, "empty id")
}

func TestStoreHonoursContextCancellation(t *testing.T) {
	h := newHarness(t)
	inst := startInstance(h, linearApprovalDef(), "biz-ctx", 1, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := h.store.Get(ctx, inst.Id); !errors.Is(err, context.Canceled) {
		t.Fatalf("get err = %v", err)
	}
	if err := h.store.Create(ctx, &Instance{Id: "x", Version: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("create err = %v", err)
	}
	if err := h.store.Update(ctx, inst.Id, inst.Version, func(*Instance) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("update err = %v", err)
	}
	if _, err := h.store.ListByBiz(ctx, 7, ""); !errors.Is(err, context.Canceled) {
		t.Fatalf("list err = %v", err)
	}
	if _, err := h.store.ListPendingTasks(ctx, 0, 7); !errors.Is(err, context.Canceled) {
		t.Fatalf("list pending err = %v", err)
	}
	if _, err := h.eng.Get(ctx, "missing"); !errors.Is(err, context.Canceled) {
		t.Fatalf("engine get err = %v", err)
	}
}

func TestDefinitionSnapshotIsNotSharedWithCaller(t *testing.T) {
	h := newHarness(t)
	definition := linearApprovalDef()
	inst := startInstance(h, definition, "biz-snapshot", 1, nil)

	definition.Key = "mutated"
	definition.Nodes[1].Approvers = []int64{777}
	definition.Nodes[1].Name = "mutated"

	live := h.mustGet(inst.Id)
	if live.DefinitionKey != "linear" {
		t.Fatalf("definition key = %s", live.DefinitionKey)
	}
	node, ok := live.Definition.Node("a")
	if !ok {
		t.Fatal("node a missing from snapshot")
	}
	if node.Approvers[0] != 11 || node.Name != "leader approve" {
		t.Fatalf("snapshot mutated: %+v", node)
	}

	task := pendingOf(t, live, "a")
	if task.ApproverId != 11 {
		t.Fatalf("approver = %d", task.ApproverId)
	}
}

func TestEngineSubmitApprovesActiveTask(t *testing.T) {
	h := newHarness(t)
	inst := startInstance(h, linearApprovalDef(), "biz-submit", 1, nil)

	done, err := h.eng.Submit(context.Background(), inst.Id, 11, "auto")
	if err != nil {
		t.Fatalf("submit failed: %v", err)
	}
	if done.Status != InstanceApproved {
		t.Fatalf("status = %s, want approved", done.Status)
	}
	_, err = h.eng.Submit(context.Background(), done.Id, 11, "auto")
	requireError(t, err, ErrInvalidState, "submit without pending task")
}

func TestEngineUsesCustomEvaluator(t *testing.T) {
	h := newHarness(t, func(o *Options) {
		o.Evaluator = func(expr string, variables map[string]string) (bool, error) {
			return expr == "always", nil
		}
	})
	custom := def("custom", "s",
		node("s", NodeStart, next("c")),
		node("c", NodeCondition, branch("always", "a"), branch("amount>10", "other"), next("e")),
		node("a", NodeApproval, named("expert"), approvers(81), sign(SignAny), next("e")),
		node("other", NodeApproval, named("fallback expert"), approvers(82), sign(SignAny), next("e")),
		node("e", NodeEnd),
	)
	inst := startInstance(h, custom, "biz-eval", 1, nil)
	if inst.CurrentKey != "a" {
		t.Fatalf("current = %s, want a", inst.CurrentKey)
	}
	finished := h.mustApprove(inst.Id, pendingOf(t, inst, "a").Id, 81)
	if finished.Status != InstanceApproved {
		t.Fatalf("status = %s, want approved", finished.Status)
	}

	defaultEvaluator := newHarness(t)
	fallback := startInstance(defaultEvaluator, custom, "biz-eval-2", 1, vars("amount", "50"))
	if fallback.CurrentKey != "other" {
		t.Fatalf("current = %s, want other", fallback.CurrentKey)
	}
}

func TestEvaluatorFailsBranchOnError(t *testing.T) {
	h := newHarness(t, func(o *Options) {
		o.Evaluator = func(expr string, variables map[string]string) (bool, error) {
			return false, errors.New("boom")
		}
	})
	_, err := h.eng.Start(context.Background(), conditionDef(), StartRequest{BizId: "biz-eval-fail", TenantId: 7, SubmitterId: 1})
	requireError(t, err, ErrEvaluatorFailed, "evaluator failure")
	if h.store.Len() != 1 {
		t.Fatalf("instance count = %d, want 1", h.store.Len())
	}
	stuck := h.store.IDsWithPrefix("inst-")
	if len(stuck) != 1 {
		t.Fatalf("stuck instances = %v", stuck)
	}
	raw, found := h.store.Snapshot(stuck[0])
	if !found {
		t.Fatalf("instance %s missing from store", stuck[0])
	}
	loaded := h.mustGet(stuck[0])
	if loaded.Status != InstanceRunning {
		t.Fatalf("status = %s, want running", loaded.Status)
	}
	if len(loaded.Tasks) != 0 {
		t.Fatalf("tasks = %s", describeTasks(loaded))
	}
	if !hasLog(loaded, "submit") {
		t.Fatalf("logs = %+v", loaded.Logs)
	}
	if raw.Version != loaded.Version {
		t.Fatalf("snapshot version %d differs from loaded version %d", raw.Version, loaded.Version)
	}
}

func TestEngineDefaultsUseCryptoRandomIDs(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store, Options{})
	inst, err := eng.Start(context.Background(), linearApprovalDef(), StartRequest{BizId: "biz-default", SubmitterId: 1})
	if err != nil {
		t.Fatalf("start failed: %v", err)
	}
	if len(inst.Id) <= len("inst-") {
		t.Fatalf("id = %q", inst.Id)
	}
	if inst.Tasks[0].Id == inst.Id {
		t.Fatalf("task id equals instance id: %s", inst.Id)
	}
	if !contains(inst.Id, "inst-") {
		t.Fatalf("id = %q, want inst- prefix", inst.Id)
	}
	first := inst.Id
	other, err := eng.Start(context.Background(), linearApprovalDef(), StartRequest{BizId: "biz-default", SubmitterId: 1})
	if err != nil {
		t.Fatalf("second start failed: %v", err)
	}
	if other.Id == first {
		t.Fatalf("ids collided: %s", first)
	}
}

func TestMemoryStoreSnapshotAndHelpers(t *testing.T) {
	h := newHarness(t)
	inst := startInstance(h, multiLevelDef(), "biz-helper", 1, nil)

	snapshot, ok := h.store.Snapshot(inst.Id)
	if !ok {
		t.Fatal("snapshot missing")
	}
	snapshot.BizId = "tampered"
	snapshot.Tasks[0].Status = TaskApproved
	fresh, ok := h.store.Snapshot(inst.Id)
	if !ok || fresh.BizId != "biz-helper" || fresh.Tasks[0].Status != TaskPending {
		t.Fatalf("snapshot leaked: %+v", fresh)
	}
	if h.store.Len() != 1 {
		t.Fatalf("len = %d, want 1", h.store.Len())
	}
	if _, id, found := h.store.FindTask(inst.Tasks[0].Id); !found || id != inst.Id {
		t.Fatalf("find task = %s %v", id, found)
	}
	if _, _, found := h.store.FindTask("task-missing"); found {
		t.Fatal("unexpected task found")
	}
	all, err := h.store.AllPending(7)
	if err != nil {
		t.Fatalf("all pending failed: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("all pending = %d, want 2", len(all))
	}
	if ids := h.store.IDsWithPrefix("inst-"); len(ids) != 1 {
		t.Fatalf("ids = %v", ids)
	}
	if ids := h.store.IDsWithPrefix("zzz-"); len(ids) != 0 {
		t.Fatalf("ids = %v", ids)
	}
}

func taskByApproverStatus(inst *Instance, nodeKey string, approverId int64, status TaskStatus) *Task {
	for _, task := range inst.Tasks {
		if task.NodeKey == nodeKey && task.ApproverId == approverId && task.Status == status {
			return task
		}
	}
	return nil
}

func taskRound(inst *Instance, taskId string) int {
	task, ok := inst.FindTask(taskId)
	if !ok {
		return -1
	}
	return task.round
}
