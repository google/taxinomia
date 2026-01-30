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
	"time"
)

// Value represents a runtime value
type Value struct {
	typ     valueType
	intVal  int64
	numVal  float64
	strVal  string
	boolVal bool
}

type valueType int

const (
	typeInt      valueType = iota // Integer value
	typeFloat                     // Floating-point value
	typeString                    // String value
	typeBool                      // Boolean value
	typeNil                       // Nil/null value
	typeDuration                  // Duration in nanoseconds
	typeDatetime                  // Datetime as Unix nanoseconds
)

// NewInt creates an integer value
func NewInt(n int64) Value {
	return Value{typ: typeInt, intVal: n}
}

// NewFloat creates a floating-point value
func NewFloat(n float64) Value {
	return Value{typ: typeFloat, numVal: n}
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

// NewDuration creates a duration value (nanoseconds)
func NewDuration(nanos int64) Value {
	return Value{typ: typeDuration, intVal: nanos}
}

// NewDatetime creates a datetime value (Unix nanoseconds)
func NewDatetime(unixNanos int64) Value {
	return Value{typ: typeDatetime, intVal: unixNanos}
}

// IsInt checks if value is an integer
func (v Value) IsInt() bool { return v.typ == typeInt }

// IsFloat checks if value is a floating-point number
func (v Value) IsFloat() bool { return v.typ == typeFloat }

// IsNumeric checks if value is any numeric type (int or float)
func (v Value) IsNumeric() bool { return v.typ == typeInt || v.typ == typeFloat }

// IsString checks if value is a string
func (v Value) IsString() bool { return v.typ == typeString }

// IsBool checks if value is a boolean
func (v Value) IsBool() bool { return v.typ == typeBool }

// IsNil checks if value is nil
func (v Value) IsNil() bool { return v.typ == typeNil }

// IsDuration checks if value is a duration
func (v Value) IsDuration() bool { return v.typ == typeDuration }

// IsDatetime checks if value is a datetime
func (v Value) IsDatetime() bool { return v.typ == typeDatetime }

// AsInt returns the integer value
func (v Value) AsInt() int64 {
	switch v.typ {
	case typeInt:
		return v.intVal
	case typeFloat:
		return int64(v.numVal)
	case typeDuration, typeDatetime:
		return v.intVal
	default:
		return 0
	}
}

// AsFloat returns the floating-point value
func (v Value) AsFloat() float64 {
	switch v.typ {
	case typeFloat:
		return v.numVal
	case typeInt:
		return float64(v.intVal)
	case typeDuration, typeDatetime:
		return float64(v.intVal)
	default:
		return 0
	}
}

// AsDuration returns the duration value in nanoseconds
func (v Value) AsDuration() time.Duration { return time.Duration(v.intVal) }

// AsDatetime returns the datetime value as time.Time
func (v Value) AsDatetime() time.Time { return time.Unix(0, v.intVal) }

// TypeName returns a human-readable name for the value's type
func (v Value) TypeName() string {
	switch v.typ {
	case typeInt:
		return "int"
	case typeFloat:
		return "float"
	case typeString:
		return "string"
	case typeBool:
		return "bool"
	case typeNil:
		return "nil"
	case typeDuration:
		return "duration"
	case typeDatetime:
		return "datetime"
	default:
		return "unknown"
	}
}

// AsString returns the string value
func (v Value) AsString() string {
	switch v.typ {
	case typeString:
		return v.strVal
	case typeInt:
		return strconv.FormatInt(v.intVal, 10)
	case typeFloat:
		if v.numVal == float64(int64(v.numVal)) {
			return strconv.FormatInt(int64(v.numVal), 10)
		}
		return strconv.FormatFloat(v.numVal, 'f', -1, 64)
	case typeBool:
		if v.boolVal {
			return "True"
		}
		return "False"
	case typeDuration:
		return formatDurationCompact(time.Duration(v.intVal))
	case typeDatetime:
		return time.Unix(0, v.intVal).Format(time.RFC3339)
	default:
		return "None"
	}
}

// AsBool returns the boolean value (truthy evaluation)
func (v Value) AsBool() bool {
	switch v.typ {
	case typeBool:
		return v.boolVal
	case typeInt:
		return v.intVal != 0
	case typeFloat:
		return v.numVal != 0
	case typeDuration, typeDatetime:
		return v.intVal != 0
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
	case *IntLit:
		return NewInt(n.Value), nil

	case *NumberLit:
		return NewFloat(n.Value), nil

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
			if val.IsInt() {
				return NewInt(-val.AsInt()), nil
			}
			if val.IsFloat() {
				return NewFloat(-val.AsFloat()), nil
			}
			return NilValue(), fmt.Errorf("cannot negate non-number")
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
		// Allow int == float comparisons
		if left.IsNumeric() && right.IsNumeric() {
			return NewBool(left.AsFloat() == right.AsFloat()), nil
		}
		if left.typ != right.typ {
			return NewBool(false), nil
		}
		if left.IsString() {
			return NewBool(left.AsString() == right.AsString()), nil
		}
		return NewBool(left.AsBool() == right.AsBool()), nil

	case TOKEN_NE:
		// Allow int != float comparisons
		if left.IsNumeric() && right.IsNumeric() {
			return NewBool(left.AsFloat() != right.AsFloat()), nil
		}
		if left.typ != right.typ {
			return NewBool(true), nil
		}
		if left.IsString() {
			return NewBool(left.AsString() != right.AsString()), nil
		}
		return NewBool(left.AsBool() != right.AsBool()), nil

	case TOKEN_LT, TOKEN_GT, TOKEN_LE, TOKEN_GE:
		// Compare numbers (int or float)
		if left.IsNumeric() && right.IsNumeric() {
			l, r := left.AsFloat(), right.AsFloat()
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
		// Compare strings
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
		// Compare datetimes
		if left.IsDatetime() && right.IsDatetime() {
			l, r := left.AsInt(), right.AsInt()
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
		// Compare durations
		if left.IsDuration() && right.IsDuration() {
			l, r := left.AsInt(), right.AsInt()
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

	// Handle datetime arithmetic
	if op == TOKEN_MINUS && left.IsDatetime() && right.IsDatetime() {
		// datetime - datetime = duration
		diff := left.AsInt() - right.AsInt()
		return NewDuration(diff), nil
	}
	if op == TOKEN_PLUS && left.IsDatetime() && right.IsDuration() {
		// datetime + duration = datetime
		result := left.AsInt() + right.AsInt()
		return NewDatetime(result), nil
	}
	if op == TOKEN_PLUS && left.IsDuration() && right.IsDatetime() {
		// duration + datetime = datetime
		result := left.AsInt() + right.AsInt()
		return NewDatetime(result), nil
	}
	if op == TOKEN_MINUS && left.IsDatetime() && right.IsDuration() {
		// datetime - duration = datetime
		result := left.AsInt() - right.AsInt()
		return NewDatetime(result), nil
	}

	// Handle duration arithmetic
	if left.IsDuration() && right.IsDuration() {
		l, r := left.AsInt(), right.AsInt()
		switch op {
		case TOKEN_PLUS:
			return NewDuration(l + r), nil
		case TOKEN_MINUS:
			return NewDuration(l - r), nil
		}
	}

	// Arithmetic operators require numbers
	if !left.IsNumeric() || !right.IsNumeric() {
		return NilValue(), fmt.Errorf("arithmetic operations require numbers, got %v and %v", left.typ, right.typ)
	}

	// If both are ints, preserve int type for most operations
	bothInt := left.IsInt() && right.IsInt()

	switch op {
	case TOKEN_PLUS:
		if bothInt {
			return NewInt(left.AsInt() + right.AsInt()), nil
		}
		return NewFloat(left.AsFloat() + right.AsFloat()), nil
	case TOKEN_MINUS:
		if bothInt {
			return NewInt(left.AsInt() - right.AsInt()), nil
		}
		return NewFloat(left.AsFloat() - right.AsFloat()), nil
	case TOKEN_STAR:
		if bothInt {
			return NewInt(left.AsInt() * right.AsInt()), nil
		}
		return NewFloat(left.AsFloat() * right.AsFloat()), nil
	case TOKEN_SLASH:
		// Division always returns float
		r := right.AsFloat()
		if r == 0 {
			return NilValue(), fmt.Errorf("division by zero")
		}
		return NewFloat(left.AsFloat() / r), nil
	case TOKEN_FLOOR_DIV:
		// Floor division returns int
		r := right.AsFloat()
		if r == 0 {
			return NilValue(), fmt.Errorf("division by zero")
		}
		return NewInt(int64(math.Floor(left.AsFloat() / r))), nil
	case TOKEN_PERCENT:
		if bothInt {
			r := right.AsInt()
			if r == 0 {
				return NilValue(), fmt.Errorf("modulo by zero")
			}
			return NewInt(left.AsInt() % r), nil
		}
		r := right.AsFloat()
		if r == 0 {
			return NilValue(), fmt.Errorf("modulo by zero")
		}
		return NewFloat(math.Mod(left.AsFloat(), r)), nil
	case TOKEN_POWER:
		// Power always returns float
		return NewFloat(math.Pow(left.AsFloat(), right.AsFloat())), nil
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
			return NewInt(int64(len(args[0].AsString()))), nil
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
		if args[0].IsInt() {
			return args[0], nil
		}
		if args[0].IsFloat() {
			return NewInt(int64(args[0].AsFloat())), nil
		}
		if args[0].IsString() {
			// Try parsing as int first
			if n, err := strconv.ParseInt(args[0].AsString(), 10, 64); err == nil {
				return NewInt(n), nil
			}
			// Fall back to float parsing and truncating
			n, err := strconv.ParseFloat(args[0].AsString(), 64)
			if err != nil {
				return NilValue(), fmt.Errorf("cannot convert '%s' to int", args[0].AsString())
			}
			return NewInt(int64(n)), nil
		}
		return NilValue(), fmt.Errorf("int() argument must be number or string")

	case "float":
		if len(args) != 1 {
			return NilValue(), fmt.Errorf("float() takes 1 argument")
		}
		if args[0].IsFloat() {
			return args[0], nil
		}
		if args[0].IsInt() {
			return NewFloat(float64(args[0].AsInt())), nil
		}
		if args[0].IsString() {
			n, err := strconv.ParseFloat(args[0].AsString(), 64)
			if err != nil {
				return NilValue(), fmt.Errorf("cannot convert '%s' to float", args[0].AsString())
			}
			return NewFloat(n), nil
		}
		return NilValue(), fmt.Errorf("float() argument must be number or string")

	case "abs":
		if len(args) != 1 {
			return NilValue(), fmt.Errorf("abs() takes 1 argument")
		}
		if args[0].IsInt() {
			n := args[0].AsInt()
			if n < 0 {
				n = -n
			}
			return NewInt(n), nil
		}
		if args[0].IsFloat() {
			return NewFloat(math.Abs(args[0].AsFloat())), nil
		}
		return NilValue(), fmt.Errorf("abs() argument must be number")

	case "round":
		if len(args) < 1 || len(args) > 2 {
			return NilValue(), fmt.Errorf("round() takes 1 or 2 arguments")
		}
		if !args[0].IsNumeric() {
			return NilValue(), fmt.Errorf("round() first argument must be number")
		}
		digits := 0.0
		if len(args) == 2 {
			if !args[1].IsNumeric() {
				return NilValue(), fmt.Errorf("round() second argument must be number")
			}
			digits = args[1].AsFloat()
		}
		mult := math.Pow(10, digits)
		return NewFloat(math.Round(args[0].AsFloat()*mult) / mult), nil

	case "min":
		if len(args) < 1 {
			return NilValue(), fmt.Errorf("min() requires at least 1 argument")
		}
		minVal := args[0]
		for _, arg := range args[1:] {
			if arg.IsNumeric() && minVal.IsNumeric() {
				if arg.AsFloat() < minVal.AsFloat() {
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
			if arg.IsNumeric() && maxVal.IsNumeric() {
				if arg.AsFloat() > maxVal.AsFloat() {
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
		start := int(args[1].AsInt())
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
			end := int(args[2].AsInt())
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

	// Datetime epoch functions - return int
	case "seconds":
		if len(args) != 1 {
			return NilValue(), fmt.Errorf("seconds() takes 1 argument")
		}
		t, err := parseDatetimeValue(args[0])
		if err != nil {
			return NilValue(), fmt.Errorf("seconds(): %w", err)
		}
		return NewInt(t.Unix()), nil

	case "minutes":
		if len(args) != 1 {
			return NilValue(), fmt.Errorf("minutes() takes 1 argument")
		}
		t, err := parseDatetimeValue(args[0])
		if err != nil {
			return NilValue(), fmt.Errorf("minutes(): %w", err)
		}
		return NewInt(t.Unix() / 60), nil

	case "hours":
		if len(args) != 1 {
			return NilValue(), fmt.Errorf("hours() takes 1 argument")
		}
		t, err := parseDatetimeValue(args[0])
		if err != nil {
			return NilValue(), fmt.Errorf("hours(): %w", err)
		}
		return NewInt(t.Unix() / 3600), nil

	case "days":
		if len(args) != 1 {
			return NilValue(), fmt.Errorf("days() takes 1 argument")
		}
		t, err := parseDatetimeValue(args[0])
		if err != nil {
			return NilValue(), fmt.Errorf("days(): %w", err)
		}
		return NewInt(t.Unix() / 86400), nil

	case "weeks":
		if len(args) != 1 {
			return NilValue(), fmt.Errorf("weeks() takes 1 argument")
		}
		t, err := parseDatetimeValue(args[0])
		if err != nil {
			return NilValue(), fmt.Errorf("weeks(): %w", err)
		}
		return NewInt(t.Unix() / (86400 * 7)), nil

	case "months":
		// Exact months since epoch: (year - 1970) * 12 + (month - 1)
		if len(args) != 1 {
			return NilValue(), fmt.Errorf("months() takes 1 argument")
		}
		t, err := parseDatetimeValue(args[0])
		if err != nil {
			return NilValue(), fmt.Errorf("months(): %w", err)
		}
		months := int64(t.Year()-1970)*12 + int64(t.Month()-1)
		return NewInt(months), nil

	case "quarters":
		// Exact quarters since epoch: (year - 1970) * 4 + ((month - 1) / 3)
		if len(args) != 1 {
			return NilValue(), fmt.Errorf("quarters() takes 1 argument")
		}
		t, err := parseDatetimeValue(args[0])
		if err != nil {
			return NilValue(), fmt.Errorf("quarters(): %w", err)
		}
		quarters := int64(t.Year()-1970)*4 + int64(t.Month()-1)/3
		return NewInt(quarters), nil

	case "years":
		// Years since epoch (1970)
		if len(args) != 1 {
			return NilValue(), fmt.Errorf("years() takes 1 argument")
		}
		t, err := parseDatetimeValue(args[0])
		if err != nil {
			return NilValue(), fmt.Errorf("years(): %w", err)
		}
		return NewInt(int64(t.Year() - 1970)), nil

	// Duration functions
	// Type validation is done at compile time by the TypeChecker.
	case "duration":
		// duration(value, unit) - create duration from value and unit
		// duration(string) - parse Go duration string (e.g., "2h30m", "3d4h")
		if len(args) == 1 {
			d, err := parseDurationValue(args[0])
			if err != nil {
				return NilValue(), fmt.Errorf("duration(): %w", err)
			}
			return NewDuration(int64(d)), nil
		} else if len(args) == 2 {
			value := args[0].AsFloat()
			unit := args[1].AsString()
			d, err := durationFromUnit(value, unit)
			if err != nil {
				return NilValue(), fmt.Errorf("duration(): %w", err)
			}
			return NewDuration(int64(d)), nil
		}
		return NilValue(), fmt.Errorf("duration() takes 1 or 2 arguments")

	case "date_diff":
		// date_diff(end, start) - returns duration (displays as "2h30m" etc.)
		// date_diff(end, start, unit) - returns numeric value in specified unit
		if len(args) < 2 || len(args) > 3 {
			return NilValue(), fmt.Errorf("date_diff() takes 2 or 3 arguments")
		}
		end, err := parseDatetimeValue(args[0])
		if err != nil {
			return NilValue(), fmt.Errorf("date_diff(): first argument: %w", err)
		}
		start, err := parseDatetimeValue(args[1])
		if err != nil {
			return NilValue(), fmt.Errorf("date_diff(): second argument: %w", err)
		}
		diff := end.Sub(start)
		if len(args) == 3 {
			// Return in specified unit as a number
			unit := strings.ToLower(args[2].AsString())
			switch unit {
			case "nanoseconds", "ns":
				return NewFloat(float64(diff.Nanoseconds())), nil
			case "microseconds", "us":
				return NewFloat(float64(diff.Microseconds())), nil
			case "milliseconds", "ms":
				return NewFloat(float64(diff.Milliseconds())), nil
			case "seconds", "s":
				return NewFloat(diff.Seconds()), nil
			case "minutes", "m":
				return NewFloat(diff.Minutes()), nil
			case "hours", "h":
				return NewFloat(diff.Hours()), nil
			case "days", "d":
				return NewFloat(diff.Hours() / 24), nil
			case "weeks", "w":
				return NewFloat(diff.Hours() / (24 * 7)), nil
			default:
				return NilValue(), fmt.Errorf("date_diff(): unknown unit: %s", unit)
			}
		}
		// Return as duration type (will display formatted)
		return NewDuration(int64(diff)), nil

	case "date_add":
		// date_add(datetime, duration) - returns datetime
		if len(args) != 2 {
			return NilValue(), fmt.Errorf("date_add() takes 2 arguments")
		}
		t, err := parseDatetimeValue(args[0])
		if err != nil {
			return NilValue(), fmt.Errorf("date_add(): %w", err)
		}
		d, err := parseDurationValue(args[1])
		if err != nil {
			return NilValue(), fmt.Errorf("date_add(): %w", err)
		}
		return NewDatetime(t.Add(d).UnixNano()), nil

	case "date_sub":
		// date_sub(datetime, duration) - returns datetime
		if len(args) != 2 {
			return NilValue(), fmt.Errorf("date_sub() takes 2 arguments")
		}
		t, err := parseDatetimeValue(args[0])
		if err != nil {
			return NilValue(), fmt.Errorf("date_sub(): %w", err)
		}
		d, err := parseDurationValue(args[1])
		if err != nil {
			return NilValue(), fmt.Errorf("date_sub(): %w", err)
		}
		return NewDatetime(t.Add(-d).UnixNano()), nil

	// Duration extraction functions
	case "as_nanoseconds":
		if len(args) != 1 {
			return NilValue(), fmt.Errorf("as_nanoseconds() takes 1 argument")
		}
		d, err := parseDurationValue(args[0])
		if err != nil {
			return NilValue(), fmt.Errorf("as_nanoseconds(): %w", err)
		}
		return NewFloat(float64(d.Nanoseconds())), nil

	case "as_microseconds":
		if len(args) != 1 {
			return NilValue(), fmt.Errorf("as_microseconds() takes 1 argument")
		}
		d, err := parseDurationValue(args[0])
		if err != nil {
			return NilValue(), fmt.Errorf("as_microseconds(): %w", err)
		}
		return NewFloat(float64(d.Microseconds())), nil

	case "as_milliseconds":
		if len(args) != 1 {
			return NilValue(), fmt.Errorf("as_milliseconds() takes 1 argument")
		}
		d, err := parseDurationValue(args[0])
		if err != nil {
			return NilValue(), fmt.Errorf("as_milliseconds(): %w", err)
		}
		return NewFloat(float64(d.Milliseconds())), nil

	case "as_seconds":
		if len(args) != 1 {
			return NilValue(), fmt.Errorf("as_seconds() takes 1 argument")
		}
		d, err := parseDurationValue(args[0])
		if err != nil {
			return NilValue(), fmt.Errorf("as_seconds(): %w", err)
		}
		return NewFloat(d.Seconds()), nil

	case "as_minutes":
		if len(args) != 1 {
			return NilValue(), fmt.Errorf("as_minutes() takes 1 argument")
		}
		d, err := parseDurationValue(args[0])
		if err != nil {
			return NilValue(), fmt.Errorf("as_minutes(): %w", err)
		}
		return NewFloat(d.Minutes()), nil

	case "as_hours":
		if len(args) != 1 {
			return NilValue(), fmt.Errorf("as_hours() takes 1 argument")
		}
		d, err := parseDurationValue(args[0])
		if err != nil {
			return NilValue(), fmt.Errorf("as_hours(): %w", err)
		}
		return NewFloat(d.Hours()), nil

	case "as_days":
		if len(args) != 1 {
			return NilValue(), fmt.Errorf("as_days() takes 1 argument")
		}
		d, err := parseDurationValue(args[0])
		if err != nil {
			return NilValue(), fmt.Errorf("as_days(): %w", err)
		}
		return NewFloat(d.Hours() / 24), nil

	case "format_duration":
		// format_duration(duration_nanos) - returns human-readable duration string
		if len(args) != 1 {
			return NilValue(), fmt.Errorf("format_duration() takes 1 argument")
		}
		d, err := parseDurationValue(args[0])
		if err != nil {
			return NilValue(), fmt.Errorf("format_duration(): %w", err)
		}
		return NewString(formatDurationCompact(d)), nil

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
			return NewInt(int64(strings.Count(s, args[0].AsString()))), nil
		case "find", "index":
			if len(args) != 1 {
				return NilValue(), fmt.Errorf("%s() takes 1 argument", method)
			}
			return NewInt(int64(strings.Index(s, args[0].AsString()))), nil
		case "rfind", "rindex":
			if len(args) != 1 {
				return NilValue(), fmt.Errorf("%s() takes 1 argument", method)
			}
			return NewInt(int64(strings.LastIndex(s, args[0].AsString()))), nil
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

// dateParseFormats lists formats to try when parsing datetime strings
var dateParseFormats = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05",
	"2006-01-02",
	"2006/01/02",
	"02-Jan-2006",
	"Jan 2, 2006",
	"January 2, 2006",
	"2006-01-02T15:04:05.000",
	"2006-01-02 15:04:05.000",
}

// parseDatetimeValue converts a Value to time.Time.
// Type validation is done at compile time by the TypeChecker.
func parseDatetimeValue(v Value) (time.Time, error) {
	// If already a datetime type, return directly
	if v.IsDatetime() {
		return v.AsDatetime(), nil
	}

	s := v.AsString()
	if s == "" {
		return time.Time{}, nil
	}

	// Try parsing as Unix timestamp if it's a number
	if v.IsNumeric() {
		n := v.AsInt()
		absN := n
		if absN < 0 {
			absN = -absN
		}
		// Determine if seconds, milliseconds, or nanoseconds
		switch {
		case absN > 1e16:
			// Nanoseconds (current epoch ~1.7e18)
			return time.Unix(0, n), nil
		case absN > 1e11:
			// Milliseconds (current epoch ~1.7e12)
			return time.Unix(n/1000, (n%1000)*1e6), nil
		default:
			// Seconds (current epoch ~1.7e9)
			return time.Unix(n, 0), nil
		}
	}

	// Try each format
	for _, format := range dateParseFormats {
		if t, err := time.Parse(format, s); err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("unable to parse datetime: %q", s)
}

// parseDurationValue converts a Value to time.Duration.
// Type validation is done at compile time by the TypeChecker.
func parseDurationValue(v Value) (time.Duration, error) {
	// If already a duration type, return directly
	if v.IsDuration() {
		return v.AsDuration(), nil
	}

	// If it's a number, treat as nanoseconds
	if v.IsNumeric() {
		return time.Duration(v.AsInt()), nil
	}

	// Try to parse as duration string
	s := strings.TrimSpace(v.AsString())
	if s == "" {
		return 0, nil
	}

	return parseDuration(s)
}

// parseDuration parses a duration string, supporting Go format plus days.
func parseDuration(s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}

	// Check for days component (e.g., "3d2h30m")
	negative := false
	if strings.HasPrefix(s, "-") {
		negative = true
		s = s[1:]
	}

	var total time.Duration

	// Look for 'd' for days
	if idx := strings.Index(s, "d"); idx != -1 {
		daysStr := s[:idx]
		days, err := strconv.ParseInt(daysStr, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid days in duration: %s", daysStr)
		}
		total = time.Duration(days) * 24 * time.Hour
		s = s[idx+1:]
	}

	// Parse remaining with Go's time.ParseDuration
	if s != "" {
		d, err := time.ParseDuration(s)
		if err != nil {
			return 0, fmt.Errorf("invalid duration: %w", err)
		}
		total += d
	}

	if negative {
		total = -total
	}

	return total, nil
}

// durationFromUnit creates a duration from a value and unit string.
func durationFromUnit(value float64, unit string) (time.Duration, error) {
	unit = strings.ToLower(strings.TrimSpace(unit))

	var multiplier time.Duration
	switch unit {
	case "nanosecond", "nanoseconds", "ns":
		multiplier = time.Nanosecond
	case "microsecond", "microseconds", "us", "µs":
		multiplier = time.Microsecond
	case "millisecond", "milliseconds", "ms":
		multiplier = time.Millisecond
	case "second", "seconds", "s":
		multiplier = time.Second
	case "minute", "minutes", "m":
		multiplier = time.Minute
	case "hour", "hours", "h":
		multiplier = time.Hour
	case "day", "days", "d":
		multiplier = 24 * time.Hour
	case "week", "weeks", "w":
		multiplier = 7 * 24 * time.Hour
	default:
		return 0, fmt.Errorf("unknown duration unit: %s", unit)
	}

	// Handle fractional values
	nanos := value * float64(multiplier)
	if math.IsInf(nanos, 0) || math.IsNaN(nanos) {
		return 0, fmt.Errorf("duration overflow or invalid value")
	}

	return time.Duration(nanos), nil
}

// formatDurationCompact returns a compact representation like "2h30m" or "3d4h".
func formatDurationCompact(d time.Duration) string {
	if d == 0 {
		return "0s"
	}

	negative := d < 0
	if negative {
		d = -d
	}

	var result strings.Builder
	if negative {
		result.WriteString("-")
	}

	// Handle days specially (not in standard Go duration)
	days := d / (24 * time.Hour)
	d = d % (24 * time.Hour)

	if days > 0 {
		result.WriteString(strconv.FormatInt(int64(days), 10))
		result.WriteString("d")
	}

	// Use Go's standard formatting for the rest
	if d > 0 || days == 0 {
		remaining := d.String()
		// If we have days and the remaining is just "0s", skip it
		if days > 0 && remaining == "0s" {
			return result.String()
		}
		result.WriteString(remaining)
	}

	return result.String()
}
