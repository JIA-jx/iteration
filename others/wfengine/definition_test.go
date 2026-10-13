package wfengine

import (
	"errors"
	"testing"
)

func TestValidDefinitionsPassValidation(t *testing.T) {
	definitions := map[string]*Definition{
		"linear":    linearApprovalDef(),
		"multi":     multiLevelDef(),
		"condition": conditionDef(),
		"notify":    notifyDef(),
		"all":       signAllDef(11, 12, 13),
		"sequence":  sequenceDef(),
		"branches":  branchDef(),
		"fallback":  fallbackBranchDef(),
		"rollback":  rollbackDef(),
		"transfer":  transferDef(),
		"addsign":   addSignDef(),
		"timeout":   timeoutDef(),
	}
	for name, definition := range definitions {
		if err := definition.Validate(); err != nil {
			t.Fatalf("%s: unexpected error %v", name, err)
		}
	}
}

func TestValidateStartKeyFallsBackToFirstStartNode(t *testing.T) {
	definition := def("nostart", "",
		node("s", NodeStart, next("a")),
		node("a", NodeApproval, named("leader"), approvers(11), sign(SignAny), next("e")),
		node("e", NodeEnd),
	)
	if err := definition.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	start, ok := definition.StartNode()
	if !ok || start.Key != "s" {
		t.Fatalf("start node = %+v %v", start, ok)
	}
}

func TestValidateFailures(t *testing.T) {
	cases := []struct {
		name       string
		definition *Definition
		mentions   []string
	}{
		{
			name:       "nil definition",
			definition: nil,
			mentions:   []string{"definition is nil"},
		},
		{
			name:       "empty key",
			definition: def("", "s", node("s", NodeStart, next("e")), node("e", NodeEnd)),
			mentions:   []string{"definition key is empty"},
		},
		{
			name:       "no nodes",
			definition: def("empty", "s"),
			mentions:   []string{"has no nodes"},
		},
		{
			name: "duplicated node key",
			definition: def("dup", "s",
				node("s", NodeStart, next("a")),
				node("a", NodeApproval, named("leader"), approvers(11), sign(SignAny), next("e")),
				node("a", NodeApproval, named("manager"), approvers(12), sign(SignAny), next("e")),
				node("e", NodeEnd),
			),
			mentions: []string{"duplicated node key a"},
		},
		{
			name: "empty node key",
			definition: def("blanknode", "s",
				node("s", NodeStart, next("")),
				node("", NodeEnd),
			),
			mentions: []string{"empty key"},
		},
		{
			name: "unknown node type",
			definition: def("badtype", "s",
				node("s", NodeStart, next("x")),
				node("x", NodeType("script"), next("e")),
				node("e", NodeEnd),
			),
			mentions: []string{"unknown type script"},
		},
		{
			name:       "no start node",
			definition: def("nostart", "s"),
			mentions:   []string{"has no nodes"},
		},
		{
			name: "start key points to approval node",
			definition: def("badstart", "a",
				node("a", NodeApproval, named("leader"), approvers(11), sign(SignAny), next("e")),
				node("e", NodeEnd),
			),
			mentions: []string{"is a approval node"},
		},
		{
			name: "start node has no successor",
			definition: def("danglingstart", "s",
				node("s", NodeStart),
				node("e", NodeEnd),
			),
			mentions: []string{"has no successor"},
		},
		{
			name: "successor does not exist",
			definition: def("dangling", "s",
				node("s", NodeStart, next("nope")),
				node("e", NodeEnd),
			),
			mentions: []string{"points to missing node nope"},
		},
		{
			name: "approval node without approvers",
			definition: def("noapprover", "s",
				node("s", NodeStart, next("a")),
				node("a", NodeApproval, named("leader"), sign(SignAny), next("e")),
				node("e", NodeEnd),
			),
			mentions: []string{"has no approvers"},
		},
		{
			name: "approval node with invalid sign type",
			definition: def("badsign", "s",
				node("s", NodeStart, next("a")),
				node("a", NodeApproval, named("leader"), approvers(11), sign(SignType("majority")), next("e")),
				node("e", NodeEnd),
			),
			mentions: []string{"unknown sign type majority"},
		},
		{
			name: "approval node with invalid member id",
			definition: def("badmember", "s",
				node("s", NodeStart, next("a")),
				node("a", NodeApproval, named("leader"), approvers(0), sign(SignAny), next("e")),
				node("e", NodeEnd),
			),
			mentions: []string{"not a valid member id"},
		},
		{
			name: "approval node with duplicated approver",
			definition: def("dupapprover", "s",
				node("s", NodeStart, next("a")),
				node("a", NodeApproval, named("leader"), approvers(11, 11), sign(SignAll), next("e")),
				node("e", NodeEnd),
			),
			mentions: []string{"duplicated approver 11"},
		},
		{
			name: "approval node with negative timeout",
			definition: def("negativetimeout", "s",
				node("s", NodeStart, next("a")),
				node("a", NodeApproval, named("leader"), approvers(11), sign(SignAny), timeout(-1), next("e")),
				node("e", NodeEnd),
			),
			mentions: []string{"negative timeout"},
		},
		{
			name: "condition node without branches",
			definition: def("nobranch", "s",
				node("s", NodeStart, next("c")),
				node("c", NodeCondition, next("e")),
				node("e", NodeEnd),
			),
			mentions: []string{"has no branches"},
		},
		{
			name: "branch points to missing node",
			definition: def("danglingbranch", "s",
				node("s", NodeStart, next("c")),
				node("c", NodeCondition, branch("amount>10", "ghost"), next("e")),
				node("e", NodeEnd),
			),
			mentions: []string{"branch 0 points to missing node ghost"},
		},
		{
			name: "branch has empty target",
			definition: def("emptybranch", "s",
				node("s", NodeStart, next("c")),
				node("c", NodeCondition, branch("amount>10", ""), next("e")),
				node("e", NodeEnd),
			),
			mentions: []string{"branch 0 points to missing node"},
		},
		{
			name: "notify node without successor",
			definition: def("danglingnotify", "s",
				node("s", NodeStart, next("n")),
				node("n", NodeNotify),
				node("e", NodeEnd),
			),
			mentions: []string{"has no successor"},
		},
		{
			name: "notify node with missing successor",
			definition: def("notifymissing", "s",
				node("s", NodeStart, next("n")),
				node("n", NodeNotify, next("ghost")),
				node("e", NodeEnd),
			),
			mentions: []string{"points to missing node ghost"},
		},
		{
			name: "no end node",
			definition: def("noend", "s",
				node("s", NodeStart, next("a")),
				node("a", NodeApproval, named("leader"), approvers(11), sign(SignAny), next("e")),
				node("e", NodeNotify, next("a")),
			),
			mentions: []string{"not reachable from start", "can not reach any end node"},
		},
		{
			name: "isolated node is not reachable",
			definition: def("isolated", "s",
				node("s", NodeStart, next("e")),
				node("e", NodeEnd),
				node("orphan", NodeApproval, named("leader"), approvers(11), sign(SignAny), next("e")),
			),
			mentions: []string{"not reachable from start", "can not reach any end node"},
		},
		{
			name: "end node is not reachable",
			definition: def("unreachableend", "s",
				node("s", NodeStart, next("a")),
				node("a", NodeApproval, named("leader"), approvers(11), sign(SignAny), next("e")),
				node("e", NodeEnd),
				node("far", NodeEnd),
			),
			mentions: []string{"not reachable from start", "can not reach any end node"},
		},
		{
			name: "cycle without end",
			definition: def("cycle", "s",
				node("s", NodeStart, next("a")),
				node("a", NodeApproval, named("one"), approvers(11), sign(SignAny), next("b")),
				node("b", NodeApproval, named("two"), approvers(12), sign(SignAny), next("a")),
				node("e", NodeEnd),
			),
			mentions: []string{"not reachable from start", "can not reach any end node"},
		},
		{
			name: "self loop without end",
			definition: def("selfloop", "s",
				node("s", NodeStart, next("a")),
				node("a", NodeApproval, named("one"), approvers(11), sign(SignAny), next("a")),
				node("e", NodeEnd),
			),
			mentions: []string{"can not reach any end node", "not reachable from start"},
		},
		{
			name: "end node with branches",
			definition: def("endbranch", "s",
				node("s", NodeStart, next("e")),
				node("e", NodeEnd, branch("always", "s")),
			),
			mentions: []string{"must not declare branches"},
		},
		{
			name: "start node with branches",
			definition: def("startbranch", "s",
				node("s", NodeStart, branch("always", "e"), next("e")),
				node("e", NodeEnd),
			),
			mentions: []string{"must not declare branches"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.definition.Validate()
			requireError(t, err, ErrInvalidDefinition, tc.name)
			if len(tc.mentions) == 0 {
				return
			}
			for _, mention := range tc.mentions {
				if contains(err.Error(), mention) {
					return
				}
			}
			t.Fatalf("error %q does not mention any of %v", err.Error(), tc.mentions)
		})
	}
}

func TestValidateAllowsForwardJumpCycleThatCanStillReachEnd(t *testing.T) {
	definition := def("loop", "s",
		node("s", NodeStart, next("a")),
		node("a", NodeApproval, named("leader"), approvers(11), sign(SignAny), next("b")),
		node("b", NodeApproval, named("manager"), approvers(12), sign(SignAny), next("a")),
		node("done", NodeApproval, named("vp"), approvers(13), sign(SignAny), next("e")),
		node("c", NodeCondition, branch("approved==true", "done"), next("a")),
		node("e", NodeEnd),
	)
	definition.Nodes[1].Next = "c"
	err := definition.Validate()
	requireError(t, err, ErrInvalidDefinition, "forward jump cycle")
	if !contains(err.Error(), "not reachable from start") && !contains(err.Error(), "can not reach any end node") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDefinitionNodeLookup(t *testing.T) {
	definition := multiLevelDef()
	first, ok := definition.Node("a")
	if !ok {
		t.Fatal("node a not found")
	}
	second, ok := definition.Node("a")
	if !ok {
		t.Fatal("node a not found on second call")
	}
	if first != second {
		t.Fatal("node lookup must return a stable pointer")
	}
	if first.Name != "leader" || len(first.Approvers) != 2 {
		t.Fatalf("node = %+v", first)
	}
	if _, ok := definition.Node("missing"); ok {
		t.Fatal("missing node reported as found")
	}
	if _, ok := definition.Node(""); ok {
		t.Fatal("empty key reported as found")
	}
	var nilDefinition *Definition
	if _, ok := nilDefinition.Node("a"); ok {
		t.Fatal("nil definition reported a node")
	}
	if _, ok := nilDefinition.StartNode(); ok {
		t.Fatal("nil definition reported a start node")
	}
}

func TestDefinitionCloneIsDeep(t *testing.T) {
	definition := conditionDef()
	clone := definition.Clone()
	clone.Key = "other"
	clone.Nodes[0].Key = "changed"
	clone.Nodes[1].Branches[0].Expr = "changed"
	if definition.Key != "conditional" {
		t.Fatalf("definition key = %s", definition.Key)
	}
	if definition.Nodes[0].Key != "s" {
		t.Fatalf("node key = %s", definition.Nodes[0].Key)
	}
	if definition.Nodes[1].Branches[0].Expr != "amount>1000" {
		t.Fatalf("branch expr = %s", definition.Nodes[1].Branches[0].Expr)
	}
	var nilDefinition *Definition
	if nilDefinition.Clone() != nil {
		t.Fatal("nil clone is not nil")
	}
}

func TestDefinitionSuccessorsAndReachability(t *testing.T) {
	condition := conditionDef().Nodes[1]
	successorsOfCondition := successors(&condition)
	if len(successorsOfCondition) != 3 {
		t.Fatalf("successors = %v", successorsOfCondition)
	}
	if successors(nil) != nil {
		t.Fatal("nil node must have no successors")
	}
	index := map[string]*Node{}
	for i := range conditionDef().Nodes {
		node := conditionDef().Nodes[i]
		index[node.Key] = &node
	}
	forward := reachableForward(index, "s")
	if !forward["s"] || !forward["c"] || !forward["e"] {
		t.Fatalf("forward = %v", forward)
	}
	backward := reachableBackward(index)
	if !backward["s"] || !backward["big"] {
		t.Fatalf("backward = %v", backward)
	}
	if !nodeTargetExists(index, "e") || nodeTargetExists(index, "ghost") || nodeTargetExists(index, "") {
		t.Fatal("nodeTargetExists misbehaves")
	}
	if !branchTargetExists(index, "e") || branchTargetExists(index, "ghost") {
		t.Fatal("branchTargetExists misbehaves")
	}
	if !validNodeType(NodeEnd) || validNodeType(NodeType("timer")) {
		t.Fatal("validNodeType misbehaves")
	}
}

func TestSentinelErrorsAreDistinct(t *testing.T) {
	sentinels := []error{
		ErrVersionConflict,
		ErrInstanceNotFound,
		ErrTaskNotFound,
		ErrTaskHandled,
		ErrNotApprover,
		ErrNotSubmitter,
		ErrNodeNotAllowed,
		ErrInvalidDefinition,
		ErrInvalidState,
		ErrInvalidRequest,
		ErrEvaluatorFailed,
		ErrDefinitionNotFound,
	}
	for i, first := range sentinels {
		for j, second := range sentinels {
			if i == j {
				continue
			}
			if errors.Is(first, second) {
				t.Fatalf("%v must not match %v", first, second)
			}
		}
	}
	wrapped := instanceNotFound("inst-1")
	if !errors.Is(wrapped, ErrInstanceNotFound) {
		t.Fatalf("wrapped error %v is not detectable", wrapped)
	}
	wrappedTask := taskNotFound("task-1")
	if !errors.Is(wrappedTask, ErrTaskNotFound) {
		t.Fatalf("wrapped error %v is not detectable", wrappedTask)
	}
	wrappedVersion := versionConflict("inst-1", 2, 3)
	if !errors.Is(wrappedVersion, ErrVersionConflict) {
		t.Fatalf("wrapped error %v is not detectable", wrappedVersion)
	}
	if !contains(wrappedVersion.Error(), "expected version 2") {
		t.Fatalf("version conflict detail = %s", wrappedVersion.Error())
	}
}
