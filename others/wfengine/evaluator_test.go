package wfengine

import "testing"

func TestDefaultEvaluatorComparisons(t *testing.T) {
	cases := []struct {
		name      string
		expr      string
		variables map[string]string
		want      bool
	}{
		{name: "empty means true", expr: "", variables: nil, want: true},
		{name: "blank means true", expr: "   ", variables: nil, want: true},
		{name: "number greater", expr: "amount>1000", variables: vars("amount", "5000"), want: true},
		{name: "number greater false", expr: "amount>1000", variables: vars("amount", "20"), want: false},
		{name: "number greater or equal", expr: "amount>=1000", variables: vars("amount", "1000"), want: true},
		{name: "number less", expr: "amount<1000", variables: vars("amount", "999.5"), want: true},
		{name: "number less or equal", expr: "amount<=1000", variables: vars("amount", "1000"), want: true},
		{name: "number not equal", expr: "amount!=1000", variables: vars("amount", "1001"), want: true},
		{name: "number equal", expr: "amount==1000", variables: vars("amount", "1000"), want: true},
		{name: "quoted number compares as text", expr: "amount=='1000'", variables: vars("amount", "1000"), want: true},
		{name: "quoted number mismatch", expr: "amount=='1000'", variables: vars("amount", "1000.0"), want: false},
		{name: "quoted text equal", expr: "dept==\"sales\"", variables: vars("dept", "sales"), want: true},
		{name: "bare text equal", expr: "dept==sales", variables: vars("dept", "sales"), want: true},
		{name: "bare text not equal", expr: "dept!=sales", variables: vars("dept", "it"), want: true},
		{name: "single quoted text", expr: "dept=='sales'", variables: vars("dept", "sales"), want: true},
		{name: "and both true", expr: "amount>100 && dept==sales", variables: vars("amount", "200", "dept", "sales"), want: true},
		{name: "and one false", expr: "amount>100 && dept==sales", variables: vars("amount", "20", "dept", "sales"), want: false},
		{name: "or second true", expr: "amount>100 || dept==sales", variables: vars("amount", "20", "dept", "sales"), want: true},
		{name: "or none true", expr: "amount>100 || dept==sales", variables: vars("amount", "20", "dept", "it"), want: false},
		{name: "negated atom", expr: "!urgent", variables: vars("urgent", "false"), want: true},
		{name: "negated text atom", expr: "!urgent", variables: vars("urgent", "no"), want: true},
		{name: "negated comparison", expr: "!(amount>100)", variables: vars("amount", "20"), want: true},
		{name: "complex expression", expr: "amount>100 && (dept==sales || urgent==true)", variables: vars("amount", "200", "dept", "it", "urgent", "true"), want: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DefaultEvaluator(tc.expr, tc.variables)
			if err != nil {
				t.Fatalf("expr %q: unexpected error %v", tc.expr, err)
			}
			if got != tc.want {
				t.Fatalf("expr %q with %v = %v, want %v", tc.expr, tc.variables, got, tc.want)
			}
		})
	}
}

func TestDefaultEvaluatorTruthyValues(t *testing.T) {
	for _, value := range []string{"1", "true", "TRUE", "yes", "Y", "on", "t"} {
		ok, err := DefaultEvaluator("flag", vars("flag", value))
		if err != nil {
			t.Fatalf("value %q: unexpected error %v", value, err)
		}
		if !ok {
			t.Fatalf("value %q must be truthy", value)
		}
	}
	for _, value := range []string{"0", "false", "", "maybe", "2"} {
		ok, err := DefaultEvaluator("flag", vars("flag", value))
		if err != nil {
			t.Fatalf("value %q: unexpected error %v", value, err)
		}
		if ok {
			t.Fatalf("value %q must be falsy", value)
		}
	}
	if ok, err := DefaultEvaluator("flag", nil); err == nil || ok {
		t.Fatalf("nil variables must fail, got ok=%v err=%v", ok, err)
	}
}

func TestDefaultEvaluatorErrors(t *testing.T) {
	cases := []struct {
		name      string
		expr      string
		variables map[string]string
	}{
		{name: "unknown variable", expr: "missing", variables: vars("amount", "1")},
		{name: "empty right operand", expr: "amount>", variables: vars("amount", "1")},
		{name: "empty left operand", expr: "==1", variables: vars("amount", "1")},
		{name: "text with ordering operator", expr: "dept>sales", variables: vars("dept", "sales")},
		{name: "empty atom in and", expr: "amount>1 &&", variables: vars("amount", "5")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := DefaultEvaluator(tc.expr, tc.variables); err == nil {
				t.Fatalf("expr %q must fail", tc.expr)
			}
		})
	}
}

func TestEvaluatorHelpers(t *testing.T) {
	value, quoted := resolveOperand("12", nil)
	if value != "12" || quoted {
		t.Fatalf("resolveOperand = %q %v", value, quoted)
	}
	value, quoted = resolveOperand("\"12\"", nil)
	if value != "12" || !quoted {
		t.Fatalf("resolveOperand = %q %v", value, quoted)
	}
	if number, ok := parseNumber("10.5"); !ok || number != 10.5 {
		t.Fatalf("parseNumber = %v %v", number, ok)
	}
	if _, ok := parseNumber("ten"); ok {
		t.Fatal("parseNumber must reject text")
	}
	if _, ok := parseNumber(""); ok {
		t.Fatal("parseNumber must reject empty text")
	}
	if _, ok := lookupVariable("k", nil); ok {
		t.Fatal("nil variables must not resolve")
	}
	if raw, ok := lookupVariable("k", vars("k", "v")); !ok || raw != "v" {
		t.Fatalf("lookupVariable = %q %v", raw, ok)
	}
}
