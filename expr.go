package logparser

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

// MetricExpressionCalculator evaluates arithmetic expressions over metrics.
// - Only metrics with the same StartTime are combined.
// - If multiple metrics share the same (StartTime, Name), they are summed first, then used in the expression.
// - Supports +, -, *, / and parentheses, constants, and variable names (metric names).
// - Variable token format: [A-Za-z_][A-Za-z0-9_]* (must match Metric.Name exactly).
// - Constants: decimal numbers like 123, 45.6
// - Division by zero yields 0 (instead of +Inf).
type MetricExpressionCalculator struct{}

// Compute evaluates the given formula across the provided metrics.
// - formula: e.g. "A + B*2 - C/3"
// - outName: the Name to use for the resulting Metric series; if empty, uses the formula string.
func (MetricExpressionCalculator) Compute(metrics []Metric, formula string, outName string) ([]Metric, error) {
	if strings.TrimSpace(formula) == "" {
		return nil, fmt.Errorf("empty formula")
	}
	rpn, vars, err := parseExpressionToRPN(formula)
	if err != nil {
		return nil, err
	}
	if len(vars) == 0 {
		// Constant expression: produce a single metric at the only time present? Better: produce per-time constant if any times exist,
		// else return one sample at zero time. We choose: per-time constant for all times seen in input set.
	}
	// Aggregate values by (time -> name -> sum)
	timeToNameSum := make(map[time.Time]map[string]float64)
	seenTimes := make(map[time.Time]struct{})
	for _, m := range metrics {
		if m.StartTime.IsZero() {
			continue
		}
		name := m.Name
		tt := m.StartTime
		ns, ok := timeToNameSum[tt]
		if !ok {
			ns = make(map[string]float64)
			timeToNameSum[tt] = ns
		}
		ns[name] += m.Value
		seenTimes[tt] = struct{}{}
	}
	// Determine time keys to evaluate on: intersection across variables if any, otherwise all times present.
	var times []time.Time
	if len(vars) > 0 {
		// Build a list of times where every var exists
		timeHasAll := make([]time.Time, 0, len(seenTimes))
		for tt := range seenTimes {
			ns := timeToNameSum[tt]
			okAll := true
			for v := range vars {
				if _, ok := ns[v]; !ok {
					okAll = false
					break
				}
			}
			if okAll {
				timeHasAll = append(timeHasAll, tt)
			}
		}
		times = timeHasAll
	} else {
		// Constant expression: evaluate for all distinct times
		times = make([]time.Time, 0, len(seenTimes))
		for tt := range seenTimes {
			times = append(times, tt)
		}
	}
	if len(times) == 0 {
		return []Metric{}, nil
	}
	sort.Slice(times, func(i, j int) bool { return times[i].Before(times[j]) })
	if outName == "" {
		outName = strings.TrimSpace(formula)
	}
	out := make([]Metric, 0, len(times))
	for _, tt := range times {
		env := timeToNameSum[tt]
			// Evaluate RPN with env
		val, err := evalRPN(rpn, env)
		if err != nil {
			return nil, fmt.Errorf("evaluate at %s: %w", tt.Format("2006/01/02-15:04:05.000000"), err)
		}
		out = append(out, Metric{
			SourceType: "EXPR",
			StartTime:  tt,
			Name:       outName,
			Value:      val,
		})
	}
	return out, nil
}

// Public helper for ad-hoc use.
func ComputeExpression(metrics []Metric, formula, outName string) ([]Metric, error) {
	return (MetricExpressionCalculator{}).Compute(metrics, formula, outName)
}

// ---- Expression parsing (shunting-yard) ----

type tokKind int

const (
	tokInvalid tokKind = iota
	tokNumber
	tokName
	tokOp
	tokLParen
	tokRParen
)

type token struct {
	kind   tokKind
	num    float64
	text   string // operator or name
}

func isNameStart(b byte) bool {
	return (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z') || b == '_'
}
func isNameChar(b byte) bool {
	return isNameStart(b) || (b >= '0' && b <= '9')
}
func isDigit(b byte) bool {
	return b >= '0' && b <= '9'
}

func tokenize(expr string) ([]token, error) {
	s := expr
	toks := make([]token, 0, len(s)/2)
	i := 0
	for i < len(s) {
		ch := s[i]
		if ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' {
			i++
			continue
		}
		switch ch {
		case '+', '-', '*', '/':
			toks = append(toks, token{kind: tokOp, text: string(ch)})
			i++
			continue
		case '(':
			toks = append(toks, token{kind: tokLParen, text: "("})
			i++
			continue
		case ')':
			toks = append(toks, token{kind: tokRParen, text: ")"})
			i++
			continue
		}
		if isNameStart(ch) {
			j := i + 1
			for j < len(s) && isNameChar(s[j]) {
				j++
			}
			toks = append(toks, token{kind: tokName, text: s[i:j]})
			i = j
			continue
		}
		if isDigit(ch) || ch == '.' {
			j := i + 1
			dot := ch == '.'
			for j < len(s) {
				if isDigit(s[j]) {
					j++
				} else if s[j] == '.' && !dot {
					dot = true
					j++
				} else {
					break
				}
			}
			var num float64
			var n int
			fmt.Sscanf(s[i:j], "%f", &num)
			_ = n
			toks = append(toks, token{kind: tokNumber, num: num, text: s[i:j]})
			i = j
			continue
		}
		return nil, fmt.Errorf("unexpected character '%c' at %d", ch, i)
	}
	return toks, nil
}

func precedence(op string) int {
	switch op {
	case "+", "-":
		return 1
	case "*", "/":
		return 2
	default:
		return -1
	}
}

func parseExpressionToRPN(expr string) ([]token, map[string]struct{}, error) {
	toks, err := tokenize(expr)
	if err != nil {
		return nil, nil, err
	}
	output := make([]token, 0, len(toks))
	opstack := make([]token, 0, len(toks))
	vars := make(map[string]struct{})
	for _, tk := range toks {
		switch tk.kind {
		case tokNumber, tokName:
			output = append(output, tk)
			if tk.kind == tokName {
				vars[tk.text] = struct{}{}
			}
		case tokOp:
			for len(opstack) > 0 {
				top := opstack[len(opstack)-1]
				if top.kind == tokOp && precedence(top.text) >= precedence(tk.text) {
					output = append(output, top)
					opstack = opstack[:len(opstack)-1]
				} else {
					break
				}
			}
			opstack = append(opstack, tk)
		case tokLParen:
			opstack = append(opstack, tk)
		case tokRParen:
			found := false
			for len(opstack) > 0 {
				top := opstack[len(opstack)-1]
				opstack = opstack[:len(opstack)-1]
				if top.kind == tokLParen {
					found = true
					break
				}
				output = append(output, top)
			}
			if !found {
				return nil, nil, fmt.Errorf("mismatched parentheses")
			}
		default:
			return nil, nil, fmt.Errorf("invalid token")
		}
	}
	for i := len(opstack) - 1; i >= 0; i-- {
		if opstack[i].kind == tokLParen || opstack[i].kind == tokRParen {
			return nil, nil, fmt.Errorf("mismatched parentheses")
		}
		output = append(output, opstack[i])
	}
	return output, vars, nil
}

func evalRPN(rpn []token, env map[string]float64) (float64, error) {
	stack := make([]float64, 0, len(rpn))
	push := func(v float64) { stack = append(stack, v) }
	pop := func() (float64, error) {
		if len(stack) == 0 {
			return 0, fmt.Errorf("stack underflow")
		}
		v := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		return v, nil
	}
	for _, tk := range rpn {
		switch tk.kind {
		case tokNumber:
			push(tk.num)
		case tokName:
			v, ok := env[tk.text]
			if !ok {
				// Variable missing in this time -> treat as error so caller can decide
				return 0, fmt.Errorf("missing variable %q at time", tk.text)
			}
			push(v)
		case tokOp:
			b, err := pop()
			if err != nil {
				return 0, err
			}
			a, err := pop()
			if err != nil {
				return 0, err
			}
			switch tk.text {
			case "+":
				push(a + b)
			case "-":
				push(a - b)
			case "*":
				push(a * b)
			case "/":
				if b == 0 {
					push(0)
				} else {
					push(a / b)
				}
			default:
				return 0, fmt.Errorf("unknown operator %q", tk.text)
			}
		default:
			return 0, fmt.Errorf("bad token in evaluation")
		}
	}
	if len(stack) != 1 {
		return 0, fmt.Errorf("evaluation error (stack size %d)", len(stack))
	}
	res := stack[0]
	if math.IsInf(res, 0) || math.IsNaN(res) {
		res = 0
	}
	return res, nil
}


