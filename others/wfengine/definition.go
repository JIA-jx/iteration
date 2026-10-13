package wfengine

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

type NodeType string

const (
	NodeStart     NodeType = "start"
	NodeApproval  NodeType = "approval"
	NodeCondition NodeType = "condition"
	NodeNotify    NodeType = "notify"
	NodeEnd       NodeType = "end"
)

type SignType string

const (
	SignAny      SignType = "any"
	SignAll      SignType = "all"
	SignSequence SignType = "sequence"
)

type Branch struct {
	Expr string
	Next string
}

type Node struct {
	Key           string
	Name          string
	Type          NodeType
	Sign          SignType
	Approvers     []int64
	Next          string
	Branches      []Branch
	Timeout       time.Duration
	AllowTransfer bool
	AllowAddSign  bool
}

type Definition struct {
	Key      string
	Name     string
	Version  int
	Nodes    []Node
	StartKey string

	mu    sync.RWMutex
	index map[string]*Node
}

func (d *Definition) Node(key string) (*Node, bool) {
	if d == nil {
		return nil, false
	}
	d.mu.RLock()
	idx := d.index
	if idx != nil {
		n, ok := idx[key]
		d.mu.RUnlock()
		return n, ok
	}
	d.mu.RUnlock()
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.index == nil {
		d.index = make(map[string]*Node, len(d.Nodes))
		for i := range d.Nodes {
			d.index[d.Nodes[i].Key] = &d.Nodes[i]
		}
	}
	n, ok := d.index[key]
	return n, ok
}

func (d *Definition) StartNode() (*Node, bool) {
	if d == nil {
		return nil, false
	}
	if d.StartKey != "" {
		return d.Node(d.StartKey)
	}
	for i := range d.Nodes {
		if d.Nodes[i].Type == NodeStart {
			return &d.Nodes[i], true
		}
	}
	return nil, false
}

func (d *Definition) Validate() error {
	if d == nil {
		return fmt.Errorf("%w: definition is nil", ErrInvalidDefinition)
	}
	if d.Key == "" {
		return fmt.Errorf("%w: definition key is empty", ErrInvalidDefinition)
	}
	if len(d.Nodes) == 0 {
		return fmt.Errorf("%w: definition %s has no nodes", ErrInvalidDefinition, d.Key)
	}
	index := make(map[string]*Node, len(d.Nodes))
	order := make([]*Node, 0, len(d.Nodes))
	for i := range d.Nodes {
		n := &d.Nodes[i]
		if n.Key == "" {
			return fmt.Errorf("%w: definition %s has a node with empty key", ErrInvalidDefinition, d.Key)
		}
		if _, dup := index[n.Key]; dup {
			return fmt.Errorf("%w: duplicated node key %s", ErrInvalidDefinition, n.Key)
		}
		if !validNodeType(n.Type) {
			return fmt.Errorf("%w: node %s has unknown type %s", ErrInvalidDefinition, n.Key, n.Type)
		}
		index[n.Key] = n
		order = append(order, n)
	}
	start, ok := d.StartNode()
	if !ok {
		return fmt.Errorf("%w: definition %s has no start node", ErrInvalidDefinition, d.Key)
	}
	if start.Type != NodeStart {
		return fmt.Errorf("%w: start key %s is a %s node", ErrInvalidDefinition, start.Key, start.Type)
	}
	for _, n := range order {
		if err := validateNodeShape(n); err != nil {
			return err
		}
		if n.Type == NodeCondition {
			for i, b := range n.Branches {
				if !branchTargetExists(index, b.Next) {
					return fmt.Errorf("%w: node %s branch %d points to missing node %s", ErrInvalidDefinition, n.Key, i, b.Next)
				}
			}
			continue
		}
		if n.Type != NodeEnd && !nodeTargetExists(index, n.Next) {
			return fmt.Errorf("%w: node %s points to missing node %s", ErrInvalidDefinition, n.Key, n.Next)
		}
	}
	forward := reachableForward(index, start.Key)
	for key := range index {
		if !forward[key] {
			return fmt.Errorf("%w: node %s is not reachable from start", ErrInvalidDefinition, key)
		}
	}
	backward := reachableBackward(index)
	for _, n := range order {
		if !backward[n.Key] {
			return fmt.Errorf("%w: node %s can not reach any end node", ErrInvalidDefinition, n.Key)
		}
	}
	return nil
}

func validNodeType(t NodeType) bool {
	switch t {
	case NodeStart, NodeApproval, NodeCondition, NodeNotify, NodeEnd:
		return true
	}
	return false
}

func validateNodeShape(n *Node) error {
	switch n.Type {
	case NodeEnd:
		if len(n.Branches) > 0 {
			return fmt.Errorf("%w: end node %s must not declare branches", ErrInvalidDefinition, n.Key)
		}
		return nil
	case NodeCondition:
		if len(n.Branches) == 0 {
			return fmt.Errorf("%w: condition node %s has no branches", ErrInvalidDefinition, n.Key)
		}
		return nil
	case NodeApproval:
		if len(n.Approvers) == 0 {
			return fmt.Errorf("%w: approval node %s has no approvers", ErrInvalidDefinition, n.Key)
		}
		for i, id := range n.Approvers {
			if id <= 0 {
				return fmt.Errorf("%w: approval node %s approver %d is not a valid member id", ErrInvalidDefinition, n.Key, i)
			}
		}
		seen := make(map[int64]struct{}, len(n.Approvers))
		for _, id := range n.Approvers {
			if _, dup := seen[id]; dup {
				return fmt.Errorf("%w: approval node %s has duplicated approver %d", ErrInvalidDefinition, n.Key, id)
			}
			seen[id] = struct{}{}
		}
		switch n.Sign {
		case SignAny, SignAll, SignSequence:
		default:
			return fmt.Errorf("%w: approval node %s has unknown sign type %s", ErrInvalidDefinition, n.Key, n.Sign)
		}
		if n.Timeout < 0 {
			return fmt.Errorf("%w: approval node %s has negative timeout", ErrInvalidDefinition, n.Key)
		}
		return nil
	case NodeStart:
		if len(n.Branches) > 0 {
			return fmt.Errorf("%w: start node %s must not declare branches", ErrInvalidDefinition, n.Key)
		}
		if n.Next == "" {
			return fmt.Errorf("%w: start node %s has no successor", ErrInvalidDefinition, n.Key)
		}
		return nil
	case NodeNotify:
		if n.Next == "" {
			return fmt.Errorf("%w: notify node %s has no successor", ErrInvalidDefinition, n.Key)
		}
		return nil
	}
	return fmt.Errorf("%w: node %s has unknown type %s", ErrInvalidDefinition, n.Key, n.Type)
}

func nodeTargetExists(index map[string]*Node, next string) bool {
	if next == "" {
		return false
	}
	_, ok := index[next]
	return ok
}

func branchTargetExists(index map[string]*Node, next string) bool {
	if next == "" {
		return false
	}
	_, ok := index[next]
	return ok
}

func reachableForward(index map[string]*Node, start string) map[string]bool {
	seen := make(map[string]bool, len(index))
	stack := []string{start}
	for len(stack) > 0 {
		key := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if seen[key] {
			continue
		}
		n, ok := index[key]
		if !ok {
			continue
		}
		seen[key] = true
		for _, next := range successors(n) {
			if !seen[next] {
				stack = append(stack, next)
			}
		}
	}
	return seen
}

func reachableBackward(index map[string]*Node) map[string]bool {
	reverse := make(map[string][]string, len(index))
	for key, n := range index {
		for _, next := range successors(n) {
			if _, ok := index[next]; ok {
				reverse[next] = append(reverse[next], key)
			}
		}
	}
	seen := make(map[string]bool, len(index))
	stack := make([]string, 0, len(index))
	for key, n := range index {
		if n.Type == NodeEnd {
			stack = append(stack, key)
		}
	}
	for len(stack) > 0 {
		key := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if seen[key] {
			continue
		}
		seen[key] = true
		for _, prev := range reverse[key] {
			if !seen[prev] {
				stack = append(stack, prev)
			}
		}
	}
	return seen
}

func successors(n *Node) []string {
	if n == nil {
		return nil
	}
	out := make([]string, 0, len(n.Branches)+1)
	switch n.Type {
	case NodeCondition:
		for _, b := range n.Branches {
			if b.Next != "" {
				out = append(out, b.Next)
			}
		}
		if n.Next != "" {
			out = append(out, n.Next)
		}
	default:
		if n.Next != "" {
			out = append(out, n.Next)
		}
	}
	return out
}

func (d *Definition) Clone() *Definition {
	if d == nil {
		return nil
	}
	out := &Definition{
		Key:      d.Key,
		Name:     d.Name,
		Version:  d.Version,
		StartKey: d.StartKey,
	}
	if len(d.Nodes) > 0 {
		out.Nodes = make([]Node, len(d.Nodes))
		for i := range d.Nodes {
			out.Nodes[i] = cloneNode(d.Nodes[i])
		}
	}
	return out
}

func cloneNode(n Node) Node {
	out := n
	if len(n.Approvers) > 0 {
		out.Approvers = make([]int64, len(n.Approvers))
		copy(out.Approvers, n.Approvers)
	}
	if len(n.Branches) > 0 {
		out.Branches = make([]Branch, len(n.Branches))
		copy(out.Branches, n.Branches)
	}
	return out
}

func findNode(def *Definition, key string) (*Node, error) {
	n, ok := def.Node(key)
	if !ok {
		return nil, fmt.Errorf("%w: node %s", ErrDefinitionNotFound, key)
	}
	return n, nil
}

func nodeError(node string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("node %s: %w", node, err)
}

func joinErrors(prefix string, errs []error) error {
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("%s: %w", prefix, errors.Join(errs...))
}
