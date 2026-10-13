package wfengine

import (
	"fmt"
	"strconv"
	"strings"
)

func DefaultEvaluator(expr string, vars map[string]string) (bool, error) {
	text := strings.TrimSpace(expr)
	if text == "" {
		return true, nil
	}
	for _, orPart := range strings.Split(text, "||") {
		ok, err := evalAnd(stripUnbalanced(orPart), vars)
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
	}
	return false, nil
}

func evalAnd(expr string, vars map[string]string) (bool, error) {
	parts := strings.Split(expr, "&&")
	for _, part := range parts {
		ok, err := evalAtom(stripUnbalanced(part), vars)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

func evalAtom(expr string, vars map[string]string) (bool, error) {
	text := unwrapParens(expr)
	if text == "" {
		return false, fmt.Errorf("empty condition in expression %q", expr)
	}
	if strings.HasPrefix(text, "!") && !strings.HasPrefix(text, "!=") {
		inner, err := evalAtom(text[1:], vars)
		if err != nil {
			return false, err
		}
		return !inner, nil
	}
	for _, op := range []string{"==", "!=", ">=", "<=", ">", "<"} {
		idx := strings.Index(text, op)
		if idx <= 0 {
			continue
		}
		left := strings.TrimSpace(text[:idx])
		right := strings.TrimSpace(text[idx+len(op):])
		if left == "" || right == "" {
			return false, fmt.Errorf("operator %s needs two operands in %q", op, expr)
		}
		return compare(left, op, right, vars)
	}
	raw, ok := lookupVariable(text, vars)
	if !ok {
		return false, fmt.Errorf("unknown variable %q in condition %q", text, expr)
	}
	return truthy(raw), nil
}

func compare(left, op, right string, vars map[string]string) (bool, error) {
	leftRaw, leftQuoted := resolveOperand(left, vars)
	rightRaw, rightQuoted := resolveOperand(right, vars)
	if leftQuoted || rightQuoted {
		if op == "==" {
			return leftRaw == rightRaw, nil
		}
		if op == "!=" {
			return leftRaw != rightRaw, nil
		}
		return false, fmt.Errorf("operator %s is not supported for text operands in %q", op, left+" "+op+" "+right)
	}
	leftNum, leftIsNum := parseNumber(leftRaw)
	rightNum, rightIsNum := parseNumber(rightRaw)
	if leftIsNum && rightIsNum {
		switch op {
		case "==":
			return leftNum == rightNum, nil
		case "!=":
			return leftNum != rightNum, nil
		case ">":
			return leftNum > rightNum, nil
		case "<":
			return leftNum < rightNum, nil
		case ">=":
			return leftNum >= rightNum, nil
		case "<=":
			return leftNum <= rightNum, nil
		}
	}
	switch op {
	case "==":
		return leftRaw == rightRaw, nil
	case "!=":
		return leftRaw != rightRaw, nil
	}
	return false, fmt.Errorf("operator %s needs numeric operands in %q", op, left+" "+op+" "+right)
}

func resolveOperand(token string, vars map[string]string) (string, bool) {
	text := strings.TrimSpace(token)
	if len(text) >= 2 {
		if (text[0] == '"' && text[len(text)-1] == '"') || (text[0] == '\'' && text[len(text)-1] == '\'') {
			return text[1 : len(text)-1], true
		}
	}
	if raw, ok := lookupVariable(text, vars); ok {
		return raw, false
	}
	return text, false
}

func lookupVariable(name string, vars map[string]string) (string, bool) {
	if vars == nil {
		return "", false
	}
	value, ok := vars[name]
	return value, ok
}

func parseNumber(raw string) (float64, bool) {
	text := strings.TrimSpace(raw)
	if text == "" {
		return 0, false
	}
	value, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return 0, false
	}
	return value, true
}

func truthy(raw string) bool {
	text := strings.TrimSpace(strings.ToLower(raw))
	switch text {
	case "1", "true", "yes", "y", "on", "t":
		return true
	}
	return false
}

func unwrapParens(expr string) string {
	text := strings.TrimSpace(expr)
	for len(text) >= 2 && text[0] == '(' && text[len(text)-1] == ')' && balancedOuter(text) {
		text = strings.TrimSpace(text[1 : len(text)-1])
	}
	return text
}

func stripUnbalanced(expr string) string {
	text := strings.TrimSpace(expr)
	for len(text) > 0 && text[0] == '(' && !balancedOuter(text) {
		text = strings.TrimSpace(text[1:])
	}
	for len(text) > 0 && text[len(text)-1] == ')' && !balancedOuter(text) {
		text = strings.TrimSpace(text[:len(text)-1])
	}
	return text
}

func balancedOuter(text string) bool {
	depth := 0
	for i := 0; i < len(text); i++ {
		switch text[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 && i != len(text)-1 {
				return false
			}
		}
	}
	return depth == 0
}
