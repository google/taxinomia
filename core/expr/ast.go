/*
SPDX-License-Identifier: Apache-2.0

Copyright 2024 The Taxinomia Authors
*/

package expr

// Node is the interface for all AST nodes
type Node interface {
	node()
}

// NumberLit represents a numeric literal
type NumberLit struct {
	Value float64
}

func (n *NumberLit) node() {}

// StringLit represents a string literal
type StringLit struct {
	Value string
}

func (n *StringLit) node() {}

// Ident represents an identifier (column name)
type Ident struct {
	Name string
}

func (n *Ident) node() {}

// BinaryOp represents a binary operation
type BinaryOp struct {
	Op    TokenType
	Left  Node
	Right Node
}

func (n *BinaryOp) node() {}

// UnaryOp represents a unary operation
type UnaryOp struct {
	Op   TokenType
	Expr Node
}

func (n *UnaryOp) node() {}

// CallExpr represents a function call
type CallExpr struct {
	Func string
	Args []Node
}

func (n *CallExpr) node() {}

// AttrAccess represents attribute access (e.g., str.upper())
type AttrAccess struct {
	Obj  Node
	Attr string
}

func (n *AttrAccess) node() {}
