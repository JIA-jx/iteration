package wfengine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"time"
)

type ConditionEvaluator func(expr string, vars map[string]string) (bool, error)

type IDGenerator func(prefix string) string

type Options struct {
	Evaluator ConditionEvaluator
	IDGen     func(prefix string) string
	Now       func() time.Time
}

type Engine struct {
	store     Store
	evaluator ConditionEvaluator
	idGen     IDGenerator
	now       func() time.Time
}

type ProgressRequest struct {
	TaskId      string
	Actor       int64
	Comment     string
	TargetKey   string
	ToMemberId  int64
	Before      bool
	MemberIds   []int64
	InstanceId  string
	AutoApprove bool
}

func NewEngine(store Store, opts Options) *Engine {
	e := &Engine{
		store:     store,
		evaluator: opts.Evaluator,
		idGen:     opts.IDGen,
		now:       opts.Now,
	}
	if e.evaluator == nil {
		e.evaluator = DefaultEvaluator
	}
	if e.idGen == nil {
		e.idGen = randomID
	}
	if e.now == nil {
		e.now = time.Now
	}
	return e
}

func randomID(prefix string) string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%s%d", prefix, time.Now().UnixNano())
	}
	return prefix + hex.EncodeToString(buf)
}

func (e *Engine) Start(ctx context.Context, def *Definition, req StartRequest) (*Instance, error) {
	if def == nil {
		return nil, fmt.Errorf("%w: definition is nil", ErrInvalidDefinition)
	}
	if err := def.Validate(); err != nil {
		return nil, err
	}
	if req.SubmitterId <= 0 {
		return nil, invalidRequest("submitter id %d is not valid", req.SubmitterId)
	}
	if req.BizId == "" {
		return nil, invalidRequest("biz id is empty")
	}
	now := e.now()
	inst := &Instance{
		Id:                e.idGen("inst-"),
		DefinitionKey:     def.Key,
		DefinitionVersion: def.Version,
		Definition:        def.Clone(),
		BizId:             req.BizId,
		TenantId:          req.TenantId,
		SubmitterId:       req.SubmitterId,
		Status:            InstanceRunning,
		Variables:         copyVariables(req.Variables),
		CreatedAt:         now,
		UpdatedAt:         now,
		rounds:            make(map[string]int),
		Version:           1,
	}
	inst.addLog(now, req.SubmitterId, "submit", def.StartKey, "", "instance submitted", fmt.Sprintf("definition %s version %d", def.Key, def.Version))
	if err := e.store.Create(ctx, inst); err != nil {
		return nil, err
	}
	current, ok := def.StartNode()
	if !ok {
		return nil, fmt.Errorf("%w: definition %s has no start node", ErrInvalidDefinition, def.Key)
	}
	created, err := e.store.Get(ctx, inst.Id)
	if err != nil {
		return nil, err
	}
	if err := e.store.Update(ctx, inst.Id, created.Version, func(live *Instance) error {
		return e.advance(live, current.Key, req.SubmitterId, "", "")
	}); err != nil {
		return nil, err
	}
	return e.Get(ctx, inst.Id)
}

func (e *Engine) Submit(ctx context.Context, instanceId string, actor int64, comment string) (*Instance, error) {
	inst, err := e.Get(ctx, instanceId)
	if err != nil {
		return nil, err
	}
	for _, t := range inst.Tasks {
		if t.Status == TaskPending {
			return e.Approve(ctx, instanceId, t.Id, actor, comment)
		}
	}
	return nil, fmt.Errorf("%w: instance %s has no pending task", ErrInvalidState, instanceId)
}

func (e *Engine) Approve(ctx context.Context, instanceId, taskId string, actor int64, comment string) (*Instance, error) {
	var result *Instance
	err := e.withInstance(ctx, instanceId, func(inst *Instance) error {
		updated, innerErr := e.approve(inst, taskId, actor, comment)
		if innerErr != nil {
			return innerErr
		}
		result = updated
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (e *Engine) Reject(ctx context.Context, instanceId, taskId string, actor int64, comment string, targetNodeKey string) (*Instance, error) {
	var result *Instance
	err := e.withInstance(ctx, instanceId, func(inst *Instance) error {
		updated, innerErr := e.reject(inst, taskId, actor, comment, targetNodeKey)
		if innerErr != nil {
			return innerErr
		}
		result = updated
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (e *Engine) Transfer(ctx context.Context, instanceId, taskId string, actor, toMemberId int64, comment string) (*Instance, error) {
	if toMemberId <= 0 {
		return nil, invalidRequest("target member id %d is not valid", toMemberId)
	}
	var result *Instance
	err := e.withInstance(ctx, instanceId, func(inst *Instance) error {
		if err := e.checkRunning(inst); err != nil {
			return err
		}
		task, ok := inst.FindTask(taskId)
		if !ok {
			return taskNotFound(taskId)
		}
		if task.Status != TaskPending {
			return fmt.Errorf("%w: task %s is %s", ErrTaskHandled, taskId, task.Status)
		}
		if task.ApproverId != actor {
			return fmt.Errorf("%w: task %s belongs to member %d", ErrNotApprover, taskId, task.ApproverId)
		}
		node, err := findNode(inst.Definition, task.NodeKey)
		if err != nil {
			return err
		}
		if task.ApproverId == toMemberId {
			return invalidRequest("task %s already belongs to member %d", taskId, toMemberId)
		}
		if !node.AllowTransfer {
			return fmt.Errorf("%w: node %s does not allow transfer", ErrNodeNotAllowed, node.Key)
		}
		from := task.ApproverId
		task.ApproverId = toMemberId
		task.Comment = comment
		now := e.now()
		inst.UpdatedAt = now
		inst.addLog(now, actor, "transfer", task.NodeKey, task.Id, comment, fmt.Sprintf("approver %d transferred to %d", from, toMemberId))
		result = cloneInstance(inst)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (e *Engine) AddSigner(ctx context.Context, instanceId, taskId string, actor int64, before bool, memberIds []int64, comment string) (*Instance, error) {
	if len(memberIds) == 0 {
		return nil, invalidRequest("no member id supplied for add sign")
	}
	for _, id := range memberIds {
		if id <= 0 {
			return nil, invalidRequest("member id %d is not valid", id)
		}
	}
	var result *Instance
	err := e.withInstance(ctx, instanceId, func(inst *Instance) error {
		if err := e.checkRunning(inst); err != nil {
			return err
		}
		task, ok := inst.FindTask(taskId)
		if !ok {
			return taskNotFound(taskId)
		}
		if task.Status != TaskPending {
			return fmt.Errorf("%w: task %s is %s", ErrTaskHandled, taskId, task.Status)
		}
		if task.ApproverId != actor {
			return fmt.Errorf("%w: task %s belongs to member %d", ErrNotApprover, taskId, task.ApproverId)
		}
		node, err := findNode(inst.Definition, task.NodeKey)
		if err != nil {
			return err
		}
		if !node.AllowAddSign {
			return fmt.Errorf("%w: node %s does not allow add sign", ErrNodeNotAllowed, node.Key)
		}
		now := e.now()
		mode := "after"
		if before {
			mode = "before"
		}
		count := len(memberIds)
		round := inst.roundOf(node.Key)
		if before {
			base := task.Seq
			for i, id := range memberIds {
				e.newTaskAt(inst, node, id, base+i, round, now)
			}
			task.Seq = base + count
		} else {
			base := task.Seq + 1
			bumpTasksAfter(inst, task.NodeKey, task.Seq, count)
			for i, id := range memberIds {
				e.newTaskAt(inst, node, id, base+i, round, now)
			}
		}
		inst.UpdatedAt = now
		inst.addLog(now, actor, "addsign", task.NodeKey, task.Id, comment, fmt.Sprintf("%s add %d signer(s) %v", mode, count, memberIds))
		result = cloneInstance(inst)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (e *Engine) Withdraw(ctx context.Context, instanceId string, actor int64, comment string) (*Instance, error) {
	var result *Instance
	err := e.withInstance(ctx, instanceId, func(inst *Instance) error {
		if inst.SubmitterId != actor {
			return fmt.Errorf("%w: instance %s belongs to submitter %d", ErrNotSubmitter, instanceId, inst.SubmitterId)
		}
		if !inst.Running() {
			return fmt.Errorf("%w: instance %s is %s", ErrInvalidState, instanceId, inst.Status)
		}
		now := e.now()
		inst.Status = InstanceCanceled
		inst.CurrentKey = ""
		inst.FinishedAt = &now
		inst.UpdatedAt = now
		inst.cancelPending(now, nil, "withdraw")
		inst.addLog(now, actor, "withdraw", "", "", comment, "instance withdrawn by submitter")
		result = cloneInstance(inst)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (e *Engine) HandleTimeout(ctx context.Context, now time.Time) (int, error) {
	if now.IsZero() {
		now = e.now()
	}
	instances, err := e.scanInstances(ctx)
	if err != nil {
		return 0, err
	}
	handled := 0
	errs := make([]error, 0)
	for _, snapshot := range instances {
		if snapshot.Status != InstanceRunning {
			continue
		}
		expired := expiredTaskIds(snapshot, now)
		if len(expired) == 0 {
			continue
		}
		instanceId := snapshot.Id
		version := snapshot.Version
		count := 0
		updateErr := e.store.Update(ctx, instanceId, version, func(inst *Instance) error {
			if err := e.checkRunning(inst); err != nil {
				return err
			}
			count = 0
			for _, taskId := range expired {
				task, ok := inst.FindTask(taskId)
				if !ok || task.Status != TaskPending {
					continue
				}
				if task.TimeoutAt == nil || task.TimeoutAt.After(now) {
					continue
				}
				task.Status = TaskSkipped
				handledAt := now
				task.HandledAt = &handledAt
				task.Comment = "timeout auto pass"
				count++
				inst.addLog(now, task.ApproverId, "timeout", task.NodeKey, task.Id, "timeout auto pass", fmt.Sprintf("expired at %s", task.TimeoutAt.Format(time.RFC3339)))
				node, nodeErr := findNode(inst.Definition, task.NodeKey)
				if nodeErr != nil {
					return nodeErr
				}
				if innerErr := e.signalNodeDone(inst, node, task.ApproverId, "timeout", "timeout auto pass", task.Seq); innerErr != nil {
					return innerErr
				}
			}
			if count > 0 {
				inst.UpdatedAt = now
			}
			return nil
		})
		if updateErr != nil {
			errs = append(errs, fmt.Errorf("instance %s: %w", instanceId, updateErr))
			continue
		}
		handled += count
	}
	if len(errs) > 0 {
		return handled, joinErrors("timeout scan finished with errors", errs)
	}
	return handled, nil
}

func (e *Engine) Get(ctx context.Context, instanceId string) (*Instance, error) {
	return e.store.Get(ctx, instanceId)
}

func (e *Engine) scanInstances(ctx context.Context) ([]*Instance, error) {
	type allLister interface {
		ListAll(ctx context.Context) ([]*Instance, error)
	}
	if lister, ok := e.store.(allLister); ok {
		return lister.ListAll(ctx)
	}
	return e.store.ListByBiz(ctx, 0, "")
}

func (e *Engine) ListByBiz(ctx context.Context, tenantId int64, bizId string) ([]*Instance, error) {
	return e.store.ListByBiz(ctx, tenantId, bizId)
}

func (e *Engine) ListPendingTasks(ctx context.Context, approverId int64, tenantId int64) ([]*Task, error) {
	return e.store.ListPendingTasks(ctx, approverId, tenantId)
}

func (e *Engine) withInstance(ctx context.Context, instanceId string, mutate func(*Instance) error) error {
	if instanceId == "" {
		return invalidRequest("instance id is empty")
	}
	inst, err := e.store.Get(ctx, instanceId)
	if err != nil {
		return err
	}
	return e.store.Update(ctx, instanceId, inst.Version, mutate)
}

func (e *Engine) checkRunning(inst *Instance) error {
	if !inst.Running() {
		return fmt.Errorf("%w: instance %s is %s", ErrInvalidState, inst.Id, inst.Status)
	}
	return nil
}

func (e *Engine) approve(inst *Instance, taskId string, actor int64, comment string) (*Instance, error) {
	if err := e.checkRunning(inst); err != nil {
		return nil, err
	}
	task, ok := inst.FindTask(taskId)
	if !ok {
		return nil, taskNotFound(taskId)
	}
	if task.Status != TaskPending {
		return nil, fmt.Errorf("%w: task %s is %s", ErrTaskHandled, taskId, task.Status)
	}
	if task.ApproverId != actor {
		return nil, fmt.Errorf("%w: task %s belongs to member %d", ErrNotApprover, taskId, task.ApproverId)
	}
	node, err := findNode(inst.Definition, task.NodeKey)
	if err != nil {
		return nil, err
	}
	now := e.now()
	task.Status = TaskApproved
	task.Comment = comment
	handledAt := now
	task.HandledAt = &handledAt
	moved := false
	switch node.Sign {
	case SignAny:
		moved = true
	case SignSequence:
		nextTask, seqErr := e.executeNextApprover(inst, node, inst.roundOf(node.Key), now)
		if seqErr != nil {
			return nil, seqErr
		}
		moved = nextTask == nil
	default:
		moved = !hasUnsettledApprover(inst, node, task.Seq)
	}
	inst.UpdatedAt = now
	if node.Sign == SignAny {
		inst.addLog(now, actor, "approve", node.Key, task.Id, comment, "or-sign node passed by one approver")
	} else {
		inst.addLog(now, actor, "approve", node.Key, task.Id, comment, "")
	}
	if !moved {
		return cloneInstance(inst), nil
	}
	if err := e.finishNode(inst, node, actor, "approve", comment, task.Seq); err != nil {
		return nil, err
	}
	return cloneInstance(inst), nil
}

func (e *Engine) reject(inst *Instance, taskId string, actor int64, comment string, targetNodeKey string) (*Instance, error) {
	if err := e.checkRunning(inst); err != nil {
		return nil, err
	}
	task, ok := inst.FindTask(taskId)
	if !ok {
		return nil, taskNotFound(taskId)
	}
	if task.Status != TaskPending {
		return nil, fmt.Errorf("%w: task %s is %s", ErrTaskHandled, taskId, task.Status)
	}
	if task.ApproverId != actor {
		return nil, fmt.Errorf("%w: task %s belongs to member %d", ErrNotApprover, taskId, task.ApproverId)
	}
	node, err := findNode(inst.Definition, task.NodeKey)
	if err != nil {
		return nil, err
	}
	var target *Node
	if targetNodeKey != "" {
		if targetNodeKey == node.Key {
			return nil, invalidRequest("reject target %s is the current node", targetNodeKey)
		}
		target, err = findNode(inst.Definition, targetNodeKey)
		if err != nil {
			return nil, err
		}
		if target.Type != NodeApproval {
			return nil, fmt.Errorf("%w: reject target %s is a %s node", ErrInvalidState, target.Key, target.Type)
		}
	}
	now := e.now()
	task.Status = TaskSkipped
	task.Comment = comment
	handledAt := now
	task.HandledAt = &handledAt
	inst.UpdatedAt = now
	if target == nil {
		inst.addLog(now, actor, "reject", node.Key, task.Id, comment, "reject terminates instance")
		inst.rejectInstance(now, actor, "reject", comment)
		return cloneInstance(inst), nil
	}
	inst.addLog(now, actor, "reject", node.Key, task.Id, comment, fmt.Sprintf("reject rollback to %s", target.Key))
	canceled := cancelPendingAfter(inst, task.Seq, "canceled by rollback to "+target.Key, now)
	inst.addLog(now, actor, "rollback", target.Key, task.Id, comment, fmt.Sprintf("canceled %d pending task(s) after seq %d", canceled, task.Seq))
	next, activateErr := e.activateNode(inst, target, now, comment)
	if activateErr != nil {
		return nil, activateErr
	}
	if next != "" {
		if advanceErr := e.advance(inst, next, actor, "", comment); advanceErr != nil {
			return nil, advanceErr
		}
	}
	return cloneInstance(inst), nil
}

func (e *Engine) advance(inst *Instance, fromKey string, actor int64, action, comment string) error {
	def := inst.Definition
	inst.visited = make(map[string]bool, len(def.Nodes))
	defer func() {
		inst.visited = nil
	}()
	current := fromKey
	guard := 0
	for {
		guard++
		if guard > len(def.Nodes)*4+16 {
			return fmt.Errorf("%w: instance %s exceeded advance step limit", ErrInvalidState, inst.Id)
		}
		next, err := e.executeNode(inst, actor, action, comment, current)
		if err != nil {
			return err
		}
		if next == "" {
			return nil
		}
		current = next
	}
}

func (e *Engine) executeNode(inst *Instance, actor int64, action, comment, key string) (string, error) {
	node, err := findNode(inst.Definition, key)
	if err != nil {
		return "", err
	}
	if inst.visited != nil && inst.visited[key] && node.Type != NodeApproval {
		return "", fmt.Errorf("%w: node %s revisited while advancing instance %s", ErrInvalidState, key, inst.Id)
	}
	now := e.now()
	switch node.Type {
	case NodeStart:
		return node.Next, nil
	case NodeEnd:
		inst.UpdatedAt = now
		inst.finishInstance(now, actor, "finish", comment)
		return "", nil
	case NodeNotify:
		inst.addLog(now, 0, "cc", node.Key, "", comment, "notify "+node.Name)
		return node.Next, nil
	case NodeCondition:
		target, matched, evalErr := e.pickBranch(node, inst.Variables)
		if evalErr != nil {
			return "", evalErr
		}
		detail := "no branch matched"
		if matched {
			detail = fmt.Sprintf("branch %q matched", target)
		}
		inst.addLog(now, 0, "condition", node.Key, "", comment, detail)
		if target == "" {
			return "", fmt.Errorf("%w: condition node %s matched no branch", ErrInvalidState, node.Key)
		}
		return target, nil
	case NodeApproval:
		return e.activateNode(inst, node, now, comment)
	}
	return "", fmt.Errorf("%w: node %s has unsupported type %s", ErrInvalidDefinition, node.Key, node.Type)
}

func (e *Engine) activateNode(inst *Instance, node *Node, now time.Time, comment string) (string, error) {
	if !inst.visited[node.Key] {
		if node.Sign == SignSequence {
			return e.activateSequenceNode(inst, node, now)
		}
		return e.activateGroupNode(inst, node, now)
	}
	if node.Sign == SignAny || hasPendingRoundTask(inst, node.Key, 0) {
		inst.CurrentKey = node.Key
		return "", nil
	}
	if node.Sign == SignSequence {
		next, err := e.executeNextApprover(inst, node, inst.roundOf(node.Key), now)
		if err != nil {
			return "", err
		}
		if next != nil {
			inst.CurrentKey = node.Key
			return "", nil
		}
	}
	return "", e.finishNode(inst, node, 0, "approve", comment, 0)
}

func (e *Engine) activateGroupNode(inst *Instance, node *Node, now time.Time) (string, error) {
	round := inst.beginRound(node.Key)
	for i, id := range node.Approvers {
		e.newTaskAt(inst, node, id, i+1, round, now)
	}
	if node.Sign == SignAny && len(node.Approvers) > 0 {
		inst.CurrentKey = node.Key
		return "", nil
	}
	if hasUnsettledApprover(inst, node, 0) {
		inst.CurrentKey = node.Key
		return "", nil
	}
	return "", e.finishNode(inst, node, 0, "approve", "", 0)
}

func (e *Engine) activateSequenceNode(inst *Instance, node *Node, now time.Time) (string, error) {
	round := inst.beginRound(node.Key)
	next, err := e.executeNextApprover(inst, node, round, now)
	if err != nil {
		return "", err
	}
	if next != nil {
		inst.CurrentKey = node.Key
		return "", nil
	}
	return "", e.finishNode(inst, node, 0, "approve", "", 0)
}

func (e *Engine) executeNextApprover(inst *Instance, node *Node, round int, now time.Time) (*Task, error) {
	tasks := inst.roundTasks(node.Key)
	currentApprover := int64(0)
	maxSeq := 0
	for _, t := range tasks {
		if t.Seq > maxSeq {
			maxSeq = t.Seq
			currentApprover = t.ApproverId
		}
	}
	lastIndex := -1
	for i, id := range node.Approvers {
		if id == currentApprover {
			lastIndex = i
		}
	}
	for i := lastIndex + 1; i < len(node.Approvers); i++ {
		id := node.Approvers[i]
		if hasRoundApprover(inst, node.Key, id) {
			continue
		}
		task := e.newTaskAt(inst, node, id, inst.nextSeq(), round, now)
		return task, nil
	}
	return nil, nil
}

func (e *Engine) finishNode(inst *Instance, node *Node, actor int64, action, comment string, sealSeq int) error {
	now := e.now()
	inst.addLog(now, actor, "node_complete", node.Key, "", comment, fmt.Sprintf("node %s completed", node.Key))
	if node.Next == "" {
		inst.UpdatedAt = now
		inst.finishInstance(now, actor, "finish", comment)
		return nil
	}
	return e.advance(inst, node.Next, actor, action, comment)
}

func (e *Engine) signalNodeDone(inst *Instance, node *Node, actor int64, action, comment string, sealSeq int) error {
	if node.Type != NodeApproval {
		return nil
	}
	switch node.Sign {
	case SignAny:
		return e.finishNode(inst, node, actor, action, comment, sealSeq)
	case SignSequence:
		next, err := e.executeNextApprover(inst, node, inst.roundOf(node.Key), e.now())
		if err != nil {
			return err
		}
		if next != nil {
			inst.CurrentKey = node.Key
			return nil
		}
		return e.finishNode(inst, node, actor, action, comment, sealSeq)
	default:
		if hasUnsettledApprover(inst, node, sealSeq) {
			inst.CurrentKey = node.Key
			return nil
		}
		return e.finishNode(inst, node, actor, action, comment, sealSeq)
	}
}

func hasUnsettledApprover(inst *Instance, node *Node, sealSeq int) bool {
	round := inst.roundOf(node.Key)
	for _, id := range node.Approvers {
		if hasPendingApproverTask(inst, node.Key, round, id) {
			return true
		}
	}
	for _, id := range node.Approvers {
		if !approverSettledForRound(inst, node.Key, round, id) {
			return true
		}
	}
	for _, t := range inst.Tasks {
		if t.NodeKey == node.Key && t.round == round && t.Status == TaskPending {
			return true
		}
	}
	return false
}

func (e *Engine) newTask(inst *Instance, node *Node, approverId int64, now time.Time) *Task {
	task := &Task{
		Id:         e.idGen("task-"),
		InstanceId: inst.Id,
		NodeKey:    node.Key,
		NodeName:   node.Name,
		ApproverId: approverId,
		Status:     TaskPending,
		Comment:    "",
		CreatedAt:  now,
	}
	if node.Timeout > 0 {
		deadline := now.Add(node.Timeout)
		task.TimeoutAt = &deadline
	}
	inst.Tasks = append(inst.Tasks, task)
	return task
}

func (e *Engine) newTaskAt(inst *Instance, node *Node, approverId int64, seq int, round int, now time.Time) *Task {
	task := e.newTask(inst, node, approverId, now)
	if seq > inst.seqCounter {
		inst.seqCounter = seq
	}
	task.round = round
	task.Seq = seq
	return task
}

func (e *Engine) pickBranch(node *Node, vars map[string]string) (string, bool, error) {
	var firstErr error
	for _, b := range node.Branches {
		ok, err := e.evaluator(b.Expr, vars)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("%w: branch %q on node %s: %s", ErrEvaluatorFailed, b.Expr, node.Key, err.Error())
			}
			continue
		}
		if ok {
			return b.Next, true, nil
		}
	}
	if firstErr != nil {
		return "", false, firstErr
	}
	return node.Next, false, nil
}

func cancelPendingAfter(inst *Instance, seq int, comment string, now time.Time) int {
	n := 0
	for _, t := range inst.Tasks {
		if t.Status != TaskPending || t.Seq <= seq {
			continue
		}
		t.Status = TaskCanceled
		handled := now
		t.HandledAt = &handled
		t.Comment = comment
		n++
		inst.addLog(now, t.ApproverId, "cancel", t.NodeKey, t.Id, comment, "pending task canceled by rollback")
	}
	return n
}

func bumpTasksAfter(inst *Instance, nodeKey string, seq int, delta int) {
	for _, t := range inst.Tasks {
		if t.NodeKey != nodeKey || t.Seq <= seq {
			continue
		}
		t.Seq += delta
	}
}

func hasPendingRoundTask(inst *Instance, nodeKey string, sealSeq int) bool {
	for _, t := range inst.Tasks {
		if t.NodeKey != nodeKey || t.round != inst.roundOf(nodeKey) {
			continue
		}
		if t.Status != TaskPending {
			continue
		}
		if sealSeq > 0 && t.Seq > sealSeq {
			continue
		}
		return true
	}
	return false
}

func hasPendingApproverTask(inst *Instance, nodeKey string, round int, approverId int64) bool {
	for _, t := range inst.Tasks {
		if t.NodeKey != nodeKey || t.round != round {
			continue
		}
		if t.ApproverId != approverId || t.Status != TaskPending {
			continue
		}
		return true
	}
	return false
}

func approverSettledForRound(inst *Instance, nodeKey string, round int, approverId int64) bool {
	for _, t := range inst.Tasks {
		if t.NodeKey != nodeKey || t.round != round {
			continue
		}
		if t.ApproverId != approverId {
			continue
		}
		switch t.Status {
		case TaskApproved, TaskSkipped:
			return true
		}
	}
	return false
}

func hasRoundApprover(inst *Instance, nodeKey string, approverId int64) bool {
	for _, t := range inst.Tasks {
		if t.NodeKey != nodeKey || t.round != inst.roundOf(nodeKey) {
			continue
		}
		if t.ApproverId == approverId {
			return true
		}
	}
	return false
}

func expiredTaskIds(inst *Instance, now time.Time) []string {
	out := make([]string, 0)
	for _, t := range inst.Tasks {
		if t.Status != TaskPending || t.TimeoutAt == nil {
			continue
		}
		if !t.TimeoutAt.After(now) {
			out = append(out, t.Id)
		}
	}
	sort.Strings(out)
	return out
}

func copyVariables(vars map[string]string) map[string]string {
	if len(vars) == 0 {
		return make(map[string]string)
	}
	out := make(map[string]string, len(vars))
	for k, v := range vars {
		out[k] = v
	}
	return out
}
