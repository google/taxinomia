/*
SPDX-License-Identifier: Apache-2.0

Copyright 2024 The Taxinomia Authors
*/

package expr

import "fmt"

// ExprType represents the type of an expression
type ExprType int

const (
	TypeUnknown  ExprType = iota // Unknown type (unresolved column)
	TypeNumber                   // Numeric value
	TypeString                   // String value
	TypeBool                     // Boolean value
	TypeDatetime                 // Datetime value (Unix nanoseconds)
	TypeDuration                 // Duration value (nanoseconds)
	TypeAny                      // Any type (for polymorphic functions)
)

// String returns a human-readable name for the type
func (t ExprType) String() string {
	switch t {
	case TypeNumber:
		return "number"
	case TypeString:
		return "string"
	case TypeBool:
		return "bool"
	case TypeDatetime:
		return "datetime"
	case TypeDuration:
		return "duration"
	case TypeAny:
		return "any"
	default:
		return "unknown"
	}
}

// ColumnTypeGetter returns the type of a column by name
type ColumnTypeGetter func(colName string) ExprType

// TypeChecker performs static type analysis on an AST
type TypeChecker struct {
	getColumnType ColumnTypeGetter
}

// NewTypeChecker creates a new type checker
func NewTypeChecker(getColumnType ColumnTypeGetter) *TypeChecker {
	return &TypeChecker{getColumnType: getColumnType}
}

// Check performs type checking on the AST and returns the result type
func (tc *TypeChecker) Check(node Node) (ExprType, error) {
	return tc.check(node)
}

func (tc *TypeChecker) check(node Node) (ExprType, error) {
	switch n := node.(type) {
	case *NumberLit:
		return TypeNumber, nil

	case *StringLit:
		return TypeString, nil

	case *Ident:
		if tc.getColumnType == nil {
			return TypeUnknown, nil
		}
		return tc.getColumnType(n.Name), nil

	case *UnaryOp:
		exprType, err := tc.check(n.Expr)
		if err != nil {
			return TypeUnknown, err
		}
		return tc.checkUnaryOp(n.Op, exprType)

	case *BinaryOp:
		leftType, err := tc.check(n.Left)
		if err != nil {
			return TypeUnknown, err
		}
		rightType, err := tc.check(n.Right)
		if err != nil {
			return TypeUnknown, err
		}
		return tc.checkBinaryOp(n.Op, leftType, rightType)

	case *CallExpr:
		return tc.checkCall(n)

	case *AttrAccess:
		objType, err := tc.check(n.Obj)
		if err != nil {
			return TypeUnknown, err
		}
		return tc.checkAttrAccess(objType, n.Attr)
	}

	return TypeUnknown, fmt.Errorf("unknown node type")
}

func (tc *TypeChecker) checkUnaryOp(op TokenType, exprType ExprType) (ExprType, error) {
	switch op {
	case TOKEN_MINUS:
		if exprType == TypeNumber || exprType == TypeUnknown {
			return TypeNumber, nil
		}
		if exprType == TypeDuration {
			return TypeDuration, nil
		}
		return TypeUnknown, fmt.Errorf("cannot negate %s", exprType)
	case TOKEN_NOT:
		return TypeBool, nil
	}
	return TypeUnknown, fmt.Errorf("unknown unary operator")
}

func (tc *TypeChecker) checkBinaryOp(op TokenType, left, right ExprType) (ExprType, error) {
	// If either is unknown, we can't fully type check - assume it's ok
	if left == TypeUnknown || right == TypeUnknown {
		return tc.inferBinaryResult(op, left, right), nil
	}

	switch op {
	// Comparison operators return bool
	case TOKEN_EQ, TOKEN_NE:
		// Can compare any two values of same type
		if left == right {
			return TypeBool, nil
		}
		return TypeUnknown, fmt.Errorf("cannot compare %s with %s", left, right)

	case TOKEN_LT, TOKEN_GT, TOKEN_LE, TOKEN_GE:
		// Can compare numbers, strings, datetimes, or durations
		if left == right && (left == TypeNumber || left == TypeString || left == TypeDatetime || left == TypeDuration) {
			return TypeBool, nil
		}
		return TypeUnknown, fmt.Errorf("cannot compare %s with %s", left, right)

	// Logical operators return bool
	case TOKEN_AND, TOKEN_OR:
		return TypeBool, nil

	// String concatenation
	case TOKEN_PLUS:
		if left == TypeString || right == TypeString {
			return TypeString, nil
		}
		// datetime + duration = datetime
		if left == TypeDatetime && right == TypeDuration {
			return TypeDatetime, nil
		}
		// duration + datetime = datetime
		if left == TypeDuration && right == TypeDatetime {
			return TypeDatetime, nil
		}
		// duration + duration = duration
		if left == TypeDuration && right == TypeDuration {
			return TypeDuration, nil
		}
		// number + number = number
		if left == TypeNumber && right == TypeNumber {
			return TypeNumber, nil
		}
		return TypeUnknown, fmt.Errorf("cannot add %s and %s", left, right)

	case TOKEN_MINUS:
		// datetime - datetime = duration
		if left == TypeDatetime && right == TypeDatetime {
			return TypeDuration, nil
		}
		// datetime - duration = datetime
		if left == TypeDatetime && right == TypeDuration {
			return TypeDatetime, nil
		}
		// duration - duration = duration
		if left == TypeDuration && right == TypeDuration {
			return TypeDuration, nil
		}
		// number - number = number
		if left == TypeNumber && right == TypeNumber {
			return TypeNumber, nil
		}
		return TypeUnknown, fmt.Errorf("cannot subtract %s from %s", right, left)

	case TOKEN_STAR, TOKEN_SLASH, TOKEN_FLOOR_DIV, TOKEN_PERCENT, TOKEN_POWER:
		if left == TypeNumber && right == TypeNumber {
			return TypeNumber, nil
		}
		return TypeUnknown, fmt.Errorf("arithmetic operation requires numbers, got %s and %s", left, right)
	}

	return TypeUnknown, fmt.Errorf("unknown binary operator")
}

// inferBinaryResult infers the result type when one operand is unknown
func (tc *TypeChecker) inferBinaryResult(op TokenType, left, right ExprType) ExprType {
	switch op {
	case TOKEN_EQ, TOKEN_NE, TOKEN_LT, TOKEN_GT, TOKEN_LE, TOKEN_GE, TOKEN_AND, TOKEN_OR:
		return TypeBool
	case TOKEN_PLUS:
		if left == TypeString || right == TypeString {
			return TypeString
		}
		if left == TypeDatetime || right == TypeDatetime {
			return TypeDatetime
		}
		if left == TypeDuration && right == TypeDuration {
			return TypeDuration
		}
		return TypeNumber
	case TOKEN_MINUS:
		if left == TypeDatetime && right == TypeDatetime {
			return TypeDuration
		}
		if left == TypeDatetime {
			return TypeDatetime
		}
		if left == TypeDuration && right == TypeDuration {
			return TypeDuration
		}
		return TypeNumber
	default:
		return TypeNumber
	}
}

func (tc *TypeChecker) checkCall(call *CallExpr) (ExprType, error) {
	// Check argument types
	argTypes := make([]ExprType, len(call.Args))
	for i, arg := range call.Args {
		t, err := tc.check(arg)
		if err != nil {
			return TypeUnknown, err
		}
		argTypes[i] = t
	}

	return tc.checkFuncCall(call.Func, argTypes)
}

func (tc *TypeChecker) checkFuncCall(name string, argTypes []ExprType) (ExprType, error) {
	switch name {
	// String functions
	case "len":
		if len(argTypes) != 1 {
			return TypeUnknown, fmt.Errorf("len() takes 1 argument, got %d", len(argTypes))
		}
		if argTypes[0] != TypeString && argTypes[0] != TypeUnknown {
			return TypeUnknown, fmt.Errorf("len() argument must be string, got %s", argTypes[0])
		}
		return TypeNumber, nil

	case "str":
		if len(argTypes) != 1 {
			return TypeUnknown, fmt.Errorf("str() takes 1 argument, got %d", len(argTypes))
		}
		return TypeString, nil

	case "int", "float":
		if len(argTypes) != 1 {
			return TypeUnknown, fmt.Errorf("%s() takes 1 argument, got %d", name, len(argTypes))
		}
		if argTypes[0] != TypeNumber && argTypes[0] != TypeString && argTypes[0] != TypeUnknown {
			return TypeUnknown, fmt.Errorf("%s() argument must be number or string, got %s", name, argTypes[0])
		}
		return TypeNumber, nil

	case "abs":
		if len(argTypes) != 1 {
			return TypeUnknown, fmt.Errorf("abs() takes 1 argument, got %d", len(argTypes))
		}
		if argTypes[0] != TypeNumber && argTypes[0] != TypeUnknown {
			return TypeUnknown, fmt.Errorf("abs() argument must be number, got %s", argTypes[0])
		}
		return TypeNumber, nil

	case "round":
		if len(argTypes) < 1 || len(argTypes) > 2 {
			return TypeUnknown, fmt.Errorf("round() takes 1 or 2 arguments, got %d", len(argTypes))
		}
		if argTypes[0] != TypeNumber && argTypes[0] != TypeUnknown {
			return TypeUnknown, fmt.Errorf("round() first argument must be number, got %s", argTypes[0])
		}
		if len(argTypes) == 2 && argTypes[1] != TypeNumber && argTypes[1] != TypeUnknown {
			return TypeUnknown, fmt.Errorf("round() second argument must be number, got %s", argTypes[1])
		}
		return TypeNumber, nil

	case "min", "max":
		if len(argTypes) < 1 {
			return TypeUnknown, fmt.Errorf("%s() requires at least 1 argument", name)
		}
		// Returns same type as input (or first argument's type)
		return argTypes[0], nil

	case "concat", "upper", "lower", "strip", "trim", "replace", "split", "substr", "substring":
		return TypeString, nil

	case "bool":
		if len(argTypes) != 1 {
			return TypeUnknown, fmt.Errorf("bool() takes 1 argument, got %d", len(argTypes))
		}
		return TypeBool, nil

	// Datetime epoch functions - return number
	case "seconds", "minutes", "hours", "days", "weeks", "months", "quarters", "years":
		if len(argTypes) != 1 {
			return TypeUnknown, fmt.Errorf("%s() takes 1 argument, got %d", name, len(argTypes))
		}
		if argTypes[0] != TypeDatetime && argTypes[0] != TypeNumber && argTypes[0] != TypeString && argTypes[0] != TypeUnknown {
			return TypeUnknown, fmt.Errorf("%s() argument must be datetime, got %s", name, argTypes[0])
		}
		return TypeNumber, nil

	// Duration functions
	case "duration":
		if len(argTypes) == 1 {
			// Parse duration string or return existing duration
			return TypeDuration, nil
		} else if len(argTypes) == 2 {
			// duration(value, unit)
			if argTypes[0] != TypeNumber && argTypes[0] != TypeUnknown {
				return TypeUnknown, fmt.Errorf("duration() first argument must be number, got %s", argTypes[0])
			}
			if argTypes[1] != TypeString && argTypes[1] != TypeUnknown {
				return TypeUnknown, fmt.Errorf("duration() second argument must be string, got %s", argTypes[1])
			}
			return TypeDuration, nil
		}
		return TypeUnknown, fmt.Errorf("duration() takes 1 or 2 arguments, got %d", len(argTypes))

	case "date_diff":
		if len(argTypes) < 2 || len(argTypes) > 3 {
			return TypeUnknown, fmt.Errorf("date_diff() takes 2 or 3 arguments, got %d", len(argTypes))
		}
		// Check first two args are datetime-compatible
		for i := 0; i < 2; i++ {
			if argTypes[i] == TypeDuration {
				return TypeUnknown, fmt.Errorf("date_diff() argument %d must be datetime, got duration", i+1)
			}
		}
		// If 3 args, third must be string (unit)
		if len(argTypes) == 3 {
			if argTypes[2] != TypeString && argTypes[2] != TypeUnknown {
				return TypeUnknown, fmt.Errorf("date_diff() third argument must be string, got %s", argTypes[2])
			}
			return TypeNumber, nil // Returns number when unit specified
		}
		return TypeDuration, nil // Returns duration when no unit

	case "date_add", "date_sub":
		if len(argTypes) != 2 {
			return TypeUnknown, fmt.Errorf("%s() takes 2 arguments, got %d", name, len(argTypes))
		}
		// First arg must be datetime, second must be duration
		if argTypes[0] == TypeDuration {
			return TypeUnknown, fmt.Errorf("%s() first argument must be datetime, got duration", name)
		}
		if argTypes[1] == TypeDatetime {
			return TypeUnknown, fmt.Errorf("%s() second argument must be duration, got datetime", name)
		}
		return TypeDatetime, nil

	// Duration extraction functions - return number
	case "as_nanoseconds", "as_microseconds", "as_milliseconds", "as_seconds", "as_minutes", "as_hours", "as_days":
		if len(argTypes) != 1 {
			return TypeUnknown, fmt.Errorf("%s() takes 1 argument, got %d", name, len(argTypes))
		}
		if argTypes[0] == TypeDatetime {
			return TypeUnknown, fmt.Errorf("%s() argument must be duration, got datetime", name)
		}
		return TypeNumber, nil

	case "format_duration":
		if len(argTypes) != 1 {
			return TypeUnknown, fmt.Errorf("format_duration() takes 1 argument, got %d", len(argTypes))
		}
		if argTypes[0] == TypeDatetime {
			return TypeUnknown, fmt.Errorf("format_duration() argument must be duration, got datetime")
		}
		return TypeString, nil

	// Method call placeholder
	case "__method__":
		// Can't type check methods without knowing the method name
		return TypeUnknown, nil

	default:
		return TypeUnknown, fmt.Errorf("unknown function: %s", name)
	}
}

func (tc *TypeChecker) checkAttrAccess(objType ExprType, attr string) (ExprType, error) {
	// String methods
	if objType == TypeString || objType == TypeUnknown {
		switch attr {
		case "upper", "lower", "strip", "trim", "lstrip", "rstrip", "capitalize", "title":
			return TypeString, nil
		case "startswith", "endswith", "contains", "isdigit", "isalpha", "isalnum", "isupper", "islower":
			return TypeBool, nil
		case "count", "find", "index", "rfind", "rindex":
			return TypeNumber, nil
		}
	}
	return TypeUnknown, fmt.Errorf("unknown attribute: %s", attr)
}
