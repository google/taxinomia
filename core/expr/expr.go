/*
SPDX-License-Identifier: Apache-2.0

Copyright 2024 The Taxinomia Authors

Package expr provides a Python-like expression interpreter for computed columns.
It supports:
  - Column references by internal name (e.g., price, qty)
  - Arithmetic operators: +, -, *, /, //, %, **
  - Comparison operators: ==, !=, <, >, <=, >=
  - Logical operators: and, or, not
  - String concatenation with +
  - String literals: "hello" or 'hello'
  - Number literals: 123, 3.14
  - Built-in functions: len(), str(), int(), float(), abs(), round(), min(), max(),
    concat(), upper(), lower(), strip(), replace(), substr()
  - String methods: .upper(), .lower(), .strip(), .startswith(), .endswith(),
    .contains(), .replace(), .capitalize(), .title(), .count(), .find()
*/
package expr

import "fmt"

// Expression represents a compiled expression ready for evaluation
type Expression struct {
	source    string
	ast       Node
	evaluator *Evaluator
}

// Compile parses and compiles an expression string
func Compile(source string) (*Expression, error) {
	if source == "" {
		return nil, fmt.Errorf("empty expression")
	}

	parser := NewParser(source)
	ast, err := parser.Parse()
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	return &Expression{
		source: source,
		ast:    ast,
	}, nil
}

// Bind creates an evaluator bound to a column getter function
func (e *Expression) Bind(getColumn ColumnGetter) *BoundExpression {
	return &BoundExpression{
		expr:      e,
		evaluator: NewEvaluator(e.ast, getColumn),
	}
}

// Source returns the original expression source
func (e *Expression) Source() string {
	return e.source
}

// BoundExpression is an expression bound to a column getter, ready for row evaluation
type BoundExpression struct {
	expr      *Expression
	evaluator *Evaluator
}

// Eval evaluates the expression for the given row index
func (b *BoundExpression) Eval(rowIndex uint32) (Value, error) {
	return b.evaluator.Eval(rowIndex)
}

// EvalString evaluates the expression and returns the result as a string
func (b *BoundExpression) EvalString(rowIndex uint32) (string, error) {
	val, err := b.evaluator.Eval(rowIndex)
	if err != nil {
		return "", err
	}
	return val.AsString(), nil
}

// EvalNumber evaluates the expression and returns the result as a number
func (b *BoundExpression) EvalNumber(rowIndex uint32) (float64, error) {
	val, err := b.evaluator.Eval(rowIndex)
	if err != nil {
		return 0, err
	}
	if !val.IsNumber() {
		return 0, fmt.Errorf("expression result is not a number")
	}
	return val.AsNumber(), nil
}

// EvalBool evaluates the expression and returns the result as a boolean
func (b *BoundExpression) EvalBool(rowIndex uint32) (bool, error) {
	val, err := b.evaluator.Eval(rowIndex)
	if err != nil {
		return false, err
	}
	return val.AsBool(), nil
}
