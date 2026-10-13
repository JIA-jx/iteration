package wfengine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

type clock struct {
	mu sync.Mutex
	at time.Time
}

func newClock() *clock {
	return &clock{at: time.Date(2024, 3, 1, 9, 0, 0, 0, time.UTC)}
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.at = c.at.Add(d)
}

func (c *clock) Set(at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.at = at
}

type idSeq struct {
	mu sync.Mutex
	n  int
}

func (s *idSeq) Next(prefix string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.n++
	return fmt.Sprintf("%s%d", prefix, s.n)
}

type harness struct {
	t     *testing.T
	store *MemoryStore
	eng   *Engine
	clock *clock
	ids   *idSeq
}

func newHarness(t *testing.T, opts ...func(*Options)) *harness {
	t.Helper()
	h := &harness{
		t:     t,
		store: NewMemoryStore(),
		clock: newClock(),
		ids:   &idSeq{},
	}
	options := Options{
		Now:   h.clock.Now,
		IDGen: h.ids.Next,
	}
	for _, opt := range opts {
		opt(&options)
	}
	h.eng = NewEngine(h.store, options)
	return h
}

func (h *harness) mustStart(def *Definition, req StartRequest) *Instance {
	h.t.Helper()
	inst, err := h.eng.Start(context.Background(), def, req)
	if err != nil {
		h.t.Fatalf("start failed: %v", err)
	}
	return inst
}

func (h *harness) mustApprove(instanceId, taskId string, actor int64) *Instance {
	h.t.Helper()
	inst, err := h.eng.Approve(context.Background(), instanceId, taskId, actor, "ok")
	if err != nil {
		h.t.Fatalf("approve task %s by %d failed: %v", taskId, actor, err)
	}
	return inst
}

func (h *harness) mustReject(instanceId, taskId string, actor int64, target string) *Instance {
	h.t.Helper()
	inst, err := h.eng.Reject(context.Background(), instanceId, taskId, actor, "no", target)
	if err != nil {
		h.t.Fatalf("reject task %s by %d failed: %v", taskId, actor, err)
	}
	return inst
}

func (h *harness) mustTransfer(instanceId, taskId string, actor, to int64) *Instance {
	h.t.Helper()
	inst, err := h.eng.Transfer(context.Background(), instanceId, taskId, actor, to, "hand over")
	if err != nil {
		h.t.Fatalf("transfer task %s failed: %v", taskId, err)
	}
	return inst
}

func (h *harness) mustAddSigner(instanceId, taskId string, actor int64, before bool, members []int64) *Instance {
	h.t.Helper()
	inst, err := h.eng.AddSigner(context.Background(), instanceId, taskId, actor, before, members, "add sign")
	if err != nil {
		h.t.Fatalf("add signer on task %s failed: %v", taskId, err)
	}
	return inst
}

func (h *harness) mustWithdraw(instanceId string, actor int64) *Instance {
	h.t.Helper()
	inst, err := h.eng.Withdraw(context.Background(), instanceId, actor, "stop")
	if err != nil {
		h.t.Fatalf("withdraw %s failed: %v", instanceId, err)
	}
	return inst
}

func (h *harness) mustGet(instanceId string) *Instance {
	h.t.Helper()
	inst, err := h.eng.Get(context.Background(), instanceId)
	if err != nil {
		h.t.Fatalf("get %s failed: %v", instanceId, err)
	}
	return inst
}

func (h *harness) timeouts(when time.Time) int {
	h.t.Helper()
	n, err := h.eng.HandleTimeout(context.Background(), when)
	if err != nil {
		h.t.Fatalf("handle timeout failed: %v", err)
	}
	return n
}

func pendingOf(t *testing.T, inst *Instance, nodeKey string) *Task {
	t.Helper()
	var found *Task
	for _, task := range inst.Tasks {
		if task.NodeKey != nodeKey || task.Status != TaskPending {
			continue
		}
		if found == nil || task.Seq < found.Seq {
			found = task
		}
	}
	if found == nil {
		t.Fatalf("no pending task on node %s in instance %s tasks %s", nodeKey, inst.Id, describeTasks(inst))
	}
	return found
}

func taskByApprover(t *testing.T, inst *Instance, nodeKey string, approverId int64) *Task {
	t.Helper()
	for _, task := range inst.Tasks {
		if task.NodeKey == nodeKey && task.ApproverId == approverId && task.Status == TaskPending {
			return task
		}
	}
	t.Fatalf("no pending task on node %s for member %d in instance %s tasks %s", nodeKey, approverId, inst.Id, describeTasks(inst))
	return nil
}

func countTasks(inst *Instance, nodeKey string, status TaskStatus) int {
	n := 0
	for _, task := range inst.Tasks {
		if task.NodeKey == nodeKey && task.Status == status {
			n++
		}
	}
	return n
}

func describeTasks(inst *Instance) string {
	out := ""
	for _, task := range inst.Tasks {
		out += fmt.Sprintf("[%s node=%s member=%d status=%s seq=%d round=%d]", task.Id, task.NodeKey, task.ApproverId, task.Status, task.Seq, task.round)
	}
	return out
}

func hasLog(inst *Instance, action string) bool {
	for _, entry := range inst.Logs {
		if entry.Action == action {
			return true
		}
	}
	return false
}

func countLogs(inst *Instance, action string) int {
	n := 0
	for _, entry := range inst.Logs {
		if entry.Action == action {
			n++
		}
	}
	return n
}

func logFor(inst *Instance, action, taskId string) *LogEntry {
	for _, entry := range inst.Logs {
		if entry.Action == action && entry.TaskId == taskId {
			return entry
		}
	}
	return nil
}

func lastLog(t *testing.T, inst *Instance) *LogEntry {
	t.Helper()
	if len(inst.Logs) == 0 {
		t.Fatalf("instance %s has no logs", inst.Id)
	}
	return inst.Logs[len(inst.Logs)-1]
}

func vars(kv ...string) map[string]string {
	out := make(map[string]string, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		out[kv[i]] = kv[i+1]
	}
	return out
}

func requireError(t *testing.T, err error, want error, label string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: expected error %v but got nil", label, want)
	}
	if !errors.Is(err, want) {
		t.Fatalf("%s: expected error %v but got %v", label, want, err)
	}
}

func expectValidationError(t *testing.T, label string, want string, def *Definition) {
	t.Helper()
	err := def.Validate()
	requireError(t, err, ErrInvalidDefinition, label)
	if want != "" && !contains(err.Error(), want) {
		t.Fatalf("%s: error %q does not mention %q", label, err.Error(), want)
	}
}

func contains(haystack, needle string) bool {
	if needle == "" {
		return true
	}
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

func node(key string, nodeType NodeType, opts ...func(*Node)) Node {
	n := Node{Key: key, Name: key, Type: nodeType}
	for _, opt := range opts {
		opt(&n)
	}
	return n
}

func next(key string) func(*Node) {
	return func(n *Node) { n.Next = key }
}

func approvers(ids ...int64) func(*Node) {
	return func(n *Node) { n.Approvers = ids }
}

func sign(signType SignType) func(*Node) {
	return func(n *Node) { n.Sign = signType }
}

func named(name string) func(*Node) {
	return func(n *Node) { n.Name = name }
}

func timeout(d time.Duration) func(*Node) {
	return func(n *Node) { n.Timeout = d }
}

func allowTransfer() func(*Node) {
	return func(n *Node) { n.AllowTransfer = true }
}

func allowAddSign() func(*Node) {
	return func(n *Node) { n.AllowAddSign = true }
}

func branch(expr, to string) func(*Node) {
	return func(n *Node) { n.Branches = append(n.Branches, Branch{Expr: expr, Next: to}) }
}

func def(key string, startKey string, nodes ...Node) *Definition {
	return &Definition{
		Key:      key,
		Name:     key,
		Version:  1,
		Nodes:    nodes,
		StartKey: startKey,
	}
}

func linearApprovalDef() *Definition {
	return def("linear", "s",
		node("s", NodeStart, next("a")),
		node("a", NodeApproval, named("leader approve"), approvers(11, 12), sign(SignAny), next("e")),
		node("e", NodeEnd),
	)
}

func multiLevelDef() *Definition {
	return def("multi", "s",
		node("s", NodeStart, next("a")),
		node("a", NodeApproval, named("leader"), approvers(11, 12), sign(SignAny), next("b")),
		node("b", NodeApproval, named("manager"), approvers(21), sign(SignAny), next("e")),
		node("e", NodeEnd),
	)
}

func conditionDef() *Definition {
	return def("conditional", "s",
		node("s", NodeStart, next("c")),
		node("c", NodeCondition, branch("amount>1000", "big"), branch("dept==sales", "middle"), next("e")),
		node("big", NodeApproval, named("vp"), approvers(31), sign(SignAny), next("e")),
		node("middle", NodeApproval, named("director"), approvers(32), sign(SignAny), next("e")),
		node("e", NodeEnd),
	)
}

func notifyDef() *Definition {
	return def("notify", "s",
		node("s", NodeStart, next("n")),
		node("n", NodeNotify, named("cc group"), next("a")),
		node("a", NodeApproval, named("owner"), approvers(41), sign(SignAny), next("e")),
		node("e", NodeEnd),
	)
}

func signAllDef(approverIds ...int64) *Definition {
	return def("all", "s",
		node("s", NodeStart, next("a")),
		node("a", NodeApproval, named("committee"), approvers(approverIds...), sign(SignAll), next("b")),
		node("b", NodeApproval, named("manager"), approvers(99), sign(SignAny), next("e")),
		node("e", NodeEnd),
	)
}

func sequenceDef() *Definition {
	return def("sequence", "s",
		node("s", NodeStart, next("a")),
		node("a", NodeApproval, named("chain"), approvers(51, 52, 53), sign(SignSequence), next("e")),
		node("e", NodeEnd),
	)
}

func branchDef() *Definition {
	return def("branches", "s",
		node("s", NodeStart, next("c")),
		node("c", NodeCondition, branch("level==1", "l1"), branch("level==2", "l2"), branch("level==3", "l3"), next("e")),
		node("l1", NodeApproval, named("boss one"), approvers(71), sign(SignAny), next("e")),
		node("l2", NodeApproval, named("boss two"), approvers(72), sign(SignAny), next("e")),
		node("l3", NodeApproval, named("boss three"), approvers(73), sign(SignAny), next("e")),
		node("e", NodeEnd),
	)
}

func fallbackBranchDef() *Definition {
	return def("fallback", "s",
		node("s", NodeStart, next("c")),
		node("c", NodeCondition, branch("urgent==true", "fast"), next("slow")),
		node("fast", NodeApproval, named("fast lane"), approvers(81), sign(SignAny), next("e")),
		node("slow", NodeApproval, named("slow lane"), approvers(82), sign(SignAny), next("e")),
		node("e", NodeEnd),
	)
}

func rollbackDef() *Definition {
	return def("rollback", "s",
		node("s", NodeStart, next("a")),
		node("a", NodeApproval, named("leader"), approvers(11), sign(SignAny), next("b")),
		node("b", NodeApproval, named("manager"), approvers(21), sign(SignAny), next("e")),
		node("e", NodeEnd),
	)
}

func transferDef() *Definition {
	return def("transfer", "s",
		node("s", NodeStart, next("a")),
		node("a", NodeApproval, named("owner"), approvers(61), sign(SignAny), allowTransfer(), allowAddSign(), next("e")),
		node("e", NodeEnd),
	)
}

func lockedApprovalDef() *Definition {
	return def("locked", "s",
		node("s", NodeStart, next("a")),
		node("a", NodeApproval, named("owner"), approvers(61), sign(SignAny), next("e")),
		node("e", NodeEnd),
	)
}

func addSignDef() *Definition {
	return def("addsign", "s",
		node("s", NodeStart, next("a")),
		node("a", NodeApproval, named("committee"), approvers(61), sign(SignAll), allowAddSign(), next("e")),
		node("e", NodeEnd),
	)
}

func timeoutDef() *Definition {
	return def("timeout", "s",
		node("s", NodeStart, next("a")),
		node("a", NodeApproval, named("sla"), approvers(91), sign(SignAny), timeout(time.Hour), next("e")),
		node("e", NodeEnd),
	)
}

func allTimeoutDef() *Definition {
	return def("alltimeout", "s",
		node("s", NodeStart, next("a")),
		node("a", NodeApproval, named("committee"), approvers(91, 92), sign(SignAll), timeout(time.Hour), next("e")),
		node("e", NodeEnd),
	)
}

func sequenceTimeoutDef() *Definition {
	return def("seqtimeout", "s",
		node("s", NodeStart, next("a")),
		node("a", NodeApproval, named("chain"), approvers(91, 92), sign(SignSequence), timeout(time.Hour), next("e")),
		node("e", NodeEnd),
	)
}

func startInstance(h *harness, definition *Definition, businessId string, submitter int64, variables map[string]string) *Instance {
	return h.mustStart(definition, StartRequest{
		BizId:       businessId,
		TenantId:    7,
		SubmitterId: submitter,
		Variables:   variables,
	})
}
