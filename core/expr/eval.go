/*
SPDX-License-Identifier: Apache-2.0

Copyright 2024 The Taxinomia Authors
*/

package expr

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Value represents a runtime value
type Value struct {
	typ    valueType
	numVal float64
	strVal string
	boolVal bool
}

type valueType int

const (
	typeNumber valueType = iota
	typeString
	typeBool
	typeNil
)

// NewNumber creates a number value
func NewNumber(n float64) Value {
	return Value{typ: typeNumber, numVal: n}
}

// NewString creates a string value
func NewString(s string) Value {
	return Value{typ: typeString, strVal: s}
}

// NewBool creates a boolean value
func NewBool(b bool) Value {
	return Value{typ: typeBool, boolVal: b}
}

// NilValue returns a nil value
func NilValue() Value {
	return Value{typ: typeNil}
}

// IsNumber checks if value is a number
func (v Value) IsNumber() bool { return v.typ == typeNumber }

// IsString checks if value is a string
func (v Value) IsString() bool { return v.typ == typeString }

// IsBool checks if value is a boolean
func (v Value) IsBool() bool { return v.typ == typeBool }

// IsNil checks if value is nil
func (v Value) IsNil() bool { return v.typ == typeNil }

// AsNumber returns the number value
func (v Value) AsNumber() float64 { return v.numVal }

// AsString returns the string value
func (v Value) AsString() string {
	switch v.typ {
	case typeString:
		return v.strVal
	case typeNumber:
		if v.numVal == float64(int64(v.numVal)) {
			return strconv.FormatInt(int64(v.numVal), 10)
		}
		return strconv.FormatFloat(v.numVal, 'f', -1, 64)
	case typeBool:
		if v.boolVal {
			return "True"
		}
		return "False"
	default:
		return "None"
	}
}

// AsBool returns the boolean value (truthy evaluation)
func (v Value) AsBool() bool {
	switch v.typ {
	case typeBool:
		return v.boolVal
	case typeNumber:
		return v.numVal != 0
	case typeString:
		return v.strVal != ""
	default:
		return false
	}
}

// ColumnGetter is a function that retrieves a column value by name for a given row
type ColumnGetter func(colName string, rowIndex uint32) (Value, error)

// Evaluator evaluates an expression AST
type Evaluator struct {
	ast       Node
	getColumn ColumnGetter
}

// NewEvaluator creates a new evaluator
func NewEvaluator(ast Node, getColumn ColumnGetter) *Evaluator {
	return &Evaluator{ast: ast, getColumn: getColumn}
}

// Eval evaluates the expression for the given row
func (e *Evaluator) Eval(rowIndex uint32) (Value, error) {
	return e.eval(e.ast, rowIndex)
}

func (e *Evaluator) eval(node Node, row uint32) (Value, error) {
	switch n := node.(type) {
	case *NumberLit:
		return NewNumber(n.Value), nil

	case *StringLit:
		return NewString(n.Value), nil

	case *Ident:
		return e.getColumn(n.Name, row)

	case *UnaryOp:
		val, err := e.eval(n.Expr, row)
		if err != nil {
			return NilValue(), err
		}
		switch n.Op {
		case TOKEN_MINUS:
			if !val.IsNumber() {
				return NilValue(), fmt.Errorf("cannot negate non-number")
			}
			return NewNumber(-val.AsNumber()), nil
		case TOKEN_NOT:
			return NewBool(!val.AsBool()), nil
		}

	case *BinaryOp:
		left, err := e.eval(n.Left, row)
		if err != nil {
			return NilValue(), err
		}

		// Short-circuit for and/or
		if n.Op == TOKEN_AND {
			if !left.AsBool() {
				return NewBool(false), nil
			}
			right, err := e.eval(n.Right, row)
			if err != nil {
				return NilValue(), err
			}
			return NewBool(right.AsBool()), nil
		}
		if n.Op == TOKEN_OR {
			if left.AsBool() {
				return NewBool(true), nil
			}
			right, err := e.eval(n.Right, row)
			if err != nil {
				return NilValue(), err
			}
			return NewBool(right.AsBool()), nil
		}

		right, err := e.eval(n.Right, row)
		if err != nil {
			return NilValue(), err
		}

		return e.evalBinaryOp(n.Op, left, right)

	case *CallExpr:
		return e.evalCall(n, row)

	case *AttrAccess:
		obj, err := e.eval(n.Obj, row)
		if err != nil {
			return NilValue(), err
		}
		return e.evalAttr(obj, n.Attr)
	}

	return NilValue(), fmt.Errorf("unknown node type")
}

func (e *Evaluator) evalBinaryOp(op TokenType, left, right Value) (Value, error) {
	// String concatenation with +
	if op == TOKEN_PLUS && (left.IsString() || right.IsString()) {
		return NewString(left.AsString() + right.AsString()), nil
	}

	// Comparison operators work on both numbers and strings
	switch op {
	case TOKEN_EQ:
		if left.typ != right.typ {
			return NewBool(false), nil
		}
		if left.IsNumber() {
			return NewBool(left.AsNumber() == right.AsNumber()), nil
		}
		if left.IsString() {
			return NewBool(left.AsString() == right.AsString()), nil
		}
		return NewBool(left.AsBool() == right.AsBool()), nil

	case TOKEN_NE:
		if left.typ != right.typ {
			return NewBool(true), nil
		}
		if left.IsNumber() {
			return NewBool(left.AsNumber() != right.AsNumber()), nil
		}
		if left.IsString() {
			return NewBool(left.AsString() != right.AsString()), nil
		}
		return NewBool(left.AsBool() != right.AsBool()), nil

	case TOKEN_LT, TOKEN_GT, TOKEN_LE, TOKEN_GE:
		if left.IsNumber() && right.IsNumber() {
			l, r := left.AsNumber(), right.AsNumber()
			switch op {
			case TOKEN_LT:
				return NewBool(l < r), nil
			case TOKEN_GT:
				return NewBool(l > r), nil
			case TOKEN_LE:
				return NewBool(l <= r), nil
			case TOKEN_GE:
				return NewBool(l >= r), nil
			}
		}
		if left.IsString() && right.IsString() {
			l, r := left.AsString(), right.AsString()
			switch op {
			case TOKEN_LT:
				return NewBool(l < r), nil
			case TOKEN_GT:
				return NewBool(l > r), nil
			case TOKEN_LE:
				return NewBool(l <= r), nil
			case TOKEN_GE:
				return NewBool(l >= r), nil
			}
		}
		return NilValue(), fmt.Errorf("cannot compare %v and %v", left.typ, right.typ)
	}

	// Arithmetic operators require numbers
	if !left.IsNumber() || !right.IsNumber() {
		return NilValue(), fmt.Errorf("arithmetic operations require numbers")
	}

	l, r := left.AsNumber(), right.AsNumber()
	switch op {
	case TOKEN_PLUS:
		return NewNumber(l + r), nil
	case TOKEN_MINUS:
		return NewNumber(l - r), nil
	case TOKEN_STAR:
		return NewNumber(l * r), nil
	case TOKEN_SLASH:
		if r == 0 {
			return NilValue(), fmt.Errorf("division by zero")
		}
		return NewNumber(l / r), nil
	case TOKEN_FLOOR_DIV:
		if r == 0 {
			return NilValue(), fmt.Errorf("division by zero")
		}
		return NewNumber(math.Floor(l / r)), nil
	case TOKEN_PERCENT:
		if r == 0 {
			return NilValue(), fmt.Errorf("modulo by zero")
		}
		return NewNumber(math.Mod(l, r)), nil
	case TOKEN_POWER:
		return NewNumber(math.Pow(l, r)), nil
	}

	return NilValue(), fmt.Errorf("unknown operator")
}

func (e *Evaluator) evalCall(call *CallExpr, row uint32) (Value, error) {
	// Handle method calls
	if call.Func == "__method__" && len(call.Args) >= 2 {
		obj, err := e.eval(call.Args[0], row)
		if err != nil {
			return NilValue(), err
		}
		methodName := call.Args[1].(*StringLit).Value
		args := make([]Value, 0, len(call.Args)-2)
		for _, arg := range call.Args[2:] {
			val, err := e.eval(arg, row)
			if err != nil {
				return NilValue(), err
			}
			args = append(args, val)
		}
		return e.evalMethod(obj, methodName, args)
	}

	// Evaluate arguments
	args := make([]Value, 0, len(call.Args))
	for _, arg := range call.Args {
		val, err := e.eval(arg, row)
		if err != nil {
			return NilValue(), err
		}
		args = append(args, val)
	}

	return e.evalFunc(call.Func, args)
}

func (e *Evaluator) evalFunc(name string, args []Value) (Value, error) {
	switch name {
	case "len":
		if len(args) != 1 {
			return NilValue(), fmt.Errorf("len() takes 1 argument")
		}
		if args[0].IsString() {
			return NewNumber(float64(len(args[0].AsString()))), nil
		}
		return NilValue(), fmt.Errorf("len() argument must be string")

	case "str":
		if len(args) != 1 {
			return NilValue(), fmt.Errorf("str() takes 1 argument")
		}
		return NewString(args[0].AsString()), nil

	case "int":
		if len(args) != 1 {
			return NilValue(), fmt.Errorf("int() takes 1 argument")
		}
		if args[0].IsNumber() {
			return NewNumber(math.Trunc(args[0].AsNumber())), nil
		}
		if args[0].IsString() {
			n, err := strconv.ParseFloat(args[0].AsString(), 64)
			if err != nil {
				return NilValue(), fmt.Errorf("cannot convert '%s' to int", args[0].AsString())
			}
			return NewNumber(math.Trunc(n)), nil
		}
		return NilValue(), fmt.Errorf("int() argument must be number or string")

	case "float":
		if len(args) != 1 {
			return NilValue(), fmt.Errorf("float() takes 1 argument")
		}
		if args[0].IsNumber() {
			return args[0], nil
		}
		if args[0].IsString() {
			n, err := strconv.ParseFloat(args[0].AsString(), 64)
			if err != nil {
				return NilValue(), fmt.Errorf("cannot convert '%s' to float", args[0].AsString())
			}
			return NewNumber(n), nil
		}
		return NilValue(), fmt.Errorf("float() argument must be number or string")

	case "abs":
		if len(args) != 1 {
			return NilValue(), fmt.Errorf("abs() takes 1 argument")
		}
		if !args[0].IsNumber() {
			return NilValue(), fmt.Errorf("abs() argument must be number")
		}
		return NewNumber(math.Abs(args[0].AsNumber())), nil

	case "round":
		if len(args) < 1 || len(args) > 2 {
			return NilValue(), fmt.Errorf("round() takes 1 or 2 arguments")
		}
		if !args[0].IsNumber() {
			return NilValue(), fmt.Errorf("round() first argument must be number")
		}
		digits := 0.0
		if len(args) == 2 {
			if !args[1].IsNumber() {
				return NilValue(), fmt.Errorf("round() second argument must be number")
			}
			digits = args[1].AsNumber()
		}
		mult := math.Pow(10, digits)
		return NewNumber(math.Round(args[0].AsNumber()*mult) / mult), nil

	case "min":
		if len(args) < 1 {
			return NilValue(), fmt.Errorf("min() requires at least 1 argument")
		}
		minVal := args[0]
		for _, arg := range args[1:] {
			if arg.IsNumber() && minVal.IsNumber() {
				if arg.AsNumber() < minVal.AsNumber() {
					minVal = arg
				}
			} else if arg.IsString() && minVal.IsString() {
				if arg.AsString() < minVal.AsString() {
					minVal = arg
				}
			}
		}
		return minVal, nil

	case "max":
		if len(args) < 1 {
			return NilValue(), fmt.Errorf("max() requires at least 1 argument")
		}
		maxVal := args[0]
		for _, arg := range args[1:] {
			if arg.IsNumber() && maxVal.IsNumber() {
				if arg.AsNumber() > maxVal.AsNumber() {
					maxVal = arg
				}
			} else if arg.IsString() && maxVal.IsString() {
				if arg.AsString() > maxVal.AsString() {
					maxVal = arg
				}
			}
		}
		return maxVal, nil

	case "concat":
		var sb strings.Builder
		for _, arg := range args {
			sb.WriteString(arg.AsString())
		}
		return NewString(sb.String()), nil

	case "upper":
		if len(args) != 1 {
			return NilValue(), fmt.Errorf("upper() takes 1 argument")
		}
		return NewString(strings.ToUpper(args[0].AsString())), nil

	case "lower":
		if len(args) != 1 {
			return NilValue(), fmt.Errorf("lower() takes 1 argument")
		}
		return NewString(strings.ToLower(args[0].AsString())), nil

	case "strip", "trim":
		if len(args) != 1 {
			return NilValue(), fmt.Errorf("%s() takes 1 argument", name)
		}
		return NewString(strings.TrimSpace(args[0].AsString())), nil

	case "replace":
		if len(args) != 3 {
			return NilValue(), fmt.Errorf("replace() takes 3 arguments")
		}
		return NewString(strings.ReplaceAll(args[0].AsString(), args[1].AsString(), args[2].AsString())), nil

	case "split":
		if len(args) != 2 {
			return NilValue(), fmt.Errorf("split() takes 2 arguments (string, separator)")
		}
		// For now, return first part
		parts := strings.Split(args[0].AsString(), args[1].AsString())
		if len(parts) > 0 {
			return NewString(parts[0]), nil
		}
		return NewString(""), nil

	case "substr", "substring":
		if len(args) < 2 || len(args) > 3 {
			return NilValue(), fmt.Errorf("substr() takes 2 or 3 arguments")
		}
		s := args[0].AsString()
		start := int(args[1].AsNumber())
		if start < 0 {
			start = len(s) + start
		}
		if start < 0 {
			start = 0
		}
		if start >= len(s) {
			return NewString(""), nil
		}
		if len(args) == 3 {
			end := int(args[2].AsNumber())
			if end < 0 {
				end = len(s) + end
			}
			if end > len(s) {
				end = len(s)
			}
			if end <= start {
				return NewString(""), nil
			}
			return NewString(s[start:end]), nil
		}
		return NewString(s[start:]), nil

	case "bool":
		if len(args) != 1 {
			return NilValue(), fmt.Errorf("bool() takes 1 argument")
		}
		return NewBool(args[0].AsBool()), nil

	default:
		return NilValue(), fmt.Errorf("unknown function: %s", name)
	}
}

func (e *Evaluator) evalMethod(obj Value, method string, args []Value) (Value, error) {
	if obj.IsString() {
		s := obj.AsString()
		switch method {
		case "upper":
			return NewString(strings.ToUpper(s)), nil
		case "lower":
			return NewString(strings.ToLower(s)), nil
		case "strip", "trim":
			return NewString(strings.TrimSpace(s)), nil
		case "lstrip":
			return NewString(strings.TrimLeft(s, " \t\n\r")), nil
		case "rstrip":
			return NewString(strings.TrimRight(s, " \t\n\r")), nil
		case "startswith":
			if len(args) != 1 {
				return NilValue(), fmt.Errorf("startswith() takes 1 argument")
			}
			return NewBool(strings.HasPrefix(s, args[0].AsString())), nil
		case "endswith":
			if len(args) != 1 {
				return NilValue(), fmt.Errorf("endswith() takes 1 argument")
			}
			return NewBool(strings.HasSuffix(s, args[0].AsString())), nil
		case "contains":
			if len(args) != 1 {
				return NilValue(), fmt.Errorf("contains() takes 1 argument")
			}
			return NewBool(strings.Contains(s, args[0].AsString())), nil
		case "replace":
			if len(args) != 2 {
				return NilValue(), fmt.Errorf("replace() takes 2 arguments")
			}
			return NewString(strings.ReplaceAll(s, args[0].AsString(), args[1].AsString())), nil
		case "split":
			if len(args) != 1 {
				return NilValue(), fmt.Errorf("split() takes 1 argument")
			}
			parts := strings.Split(s, args[0].AsString())
			if len(parts) > 0 {
				return NewString(parts[0]), nil
			}
			return NewString(""), nil
		case "capitalize":
			if len(s) == 0 {
				return NewString(""), nil
			}
			return NewString(strings.ToUpper(string(s[0])) + strings.ToLower(s[1:])), nil
		case "title":
			return NewString(strings.Title(s)), nil
		case "count":
			if len(args) != 1 {
				return NilValue(), fmt.Errorf("count() takes 1 argument")
			}
			return NewNumber(float64(strings.Count(s, args[0].AsString()))), nil
		case "find", "index":
			if len(args) != 1 {
				return NilValue(), fmt.Errorf("%s() takes 1 argument", method)
			}
			return NewNumber(float64(strings.Index(s, args[0].AsString()))), nil
		case "rfind", "rindex":
			if len(args) != 1 {
				return NilValue(), fmt.Errorf("%s() takes 1 argument", method)
			}
			return NewNumber(float64(strings.LastIndex(s, args[0].AsString()))), nil
		case "isdigit":
			for _, r := range s {
				if r < '0' || r > '9' {
					return NewBool(false), nil
				}
			}
			return NewBool(len(s) > 0), nil
		case "isalpha":
			for _, r := range s {
				if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') {
					return NewBool(false), nil
				}
			}
			return NewBool(len(s) > 0), nil
		case "isalnum":
			for _, r := range s {
				if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')) {
					return NewBool(false), nil
				}
			}
			return NewBool(len(s) > 0), nil
		case "isupper":
			hasUpper := false
			for _, r := range s {
				if r >= 'a' && r <= 'z' {
					return NewBool(false), nil
				}
				if r >= 'A' && r <= 'Z' {
					hasUpper = true
				}
			}
			return NewBool(hasUpper), nil
		case "islower":
			hasLower := false
			for _, r := range s {
				if r >= 'A' && r <= 'Z' {
					return NewBool(false), nil
				}
				if r >= 'a' && r <= 'z' {
					hasLower = true
				}
			}
			return NewBool(hasLower), nil
		}
	}

	return NilValue(), fmt.Errorf("unknown method: %s", method)
}

func (e *Evaluator) evalAttr(obj Value, attr string) (Value, error) {
	// For now, attributes are treated as potential method calls without args
	// This is mainly for things like checking if an attribute exists
	return NilValue(), fmt.Errorf("attribute access not supported: %s", attr)
}
