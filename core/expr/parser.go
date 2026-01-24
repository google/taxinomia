/*
SPDX-License-Identifier: Apache-2.0

Copyright 2024 The Taxinomia Authors
*/

package expr

import (
	"fmt"
	"strconv"
)

// Parser parses tokens into an AST
type Parser struct {
	lexer *Lexer
	cur   Token
}

// NewParser creates a new parser
func NewParser(input string) *Parser {
	return &Parser{lexer: NewLexer(input)}
}

func (p *Parser) advance() error {
	tok, err := p.lexer.NextToken()
	if err != nil {
		return err
	}
	p.cur = tok
	return nil
}

// Parse parses the input and returns the AST
func (p *Parser) Parse() (Node, error) {
	if err := p.advance(); err != nil {
		return nil, err
	}
	return p.parseExpr()
}

// Expression parsing with precedence climbing
// Precedence (low to high):
// 1. or
// 2. and
// 3. not
// 4. ==, !=, <, >, <=, >=
// 5. +, -
// 6. *, /, //, %
// 7. ** (right associative)
// 8. unary -, not
// 9. function calls, attribute access

func (p *Parser) parseExpr() (Node, error) {
	return p.parseOr()
}

func (p *Parser) parseOr() (Node, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	for p.cur.Type == TOKEN_OR {
		op := p.cur.Type
		if err := p.advance(); err != nil {
			return nil, err
		}
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &BinaryOp{Op: op, Left: left, Right: right}
	}
	return left, nil
}

func (p *Parser) parseAnd() (Node, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}

	for p.cur.Type == TOKEN_AND {
		op := p.cur.Type
		if err := p.advance(); err != nil {
			return nil, err
		}
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		left = &BinaryOp{Op: op, Left: left, Right: right}
	}
	return left, nil
}

func (p *Parser) parseNot() (Node, error) {
	if p.cur.Type == TOKEN_NOT {
		op := p.cur.Type
		if err := p.advance(); err != nil {
			return nil, err
		}
		expr, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return &UnaryOp{Op: op, Expr: expr}, nil
	}
	return p.parseComparison()
}

func (p *Parser) parseComparison() (Node, error) {
	left, err := p.parseAddSub()
	if err != nil {
		return nil, err
	}

	for p.cur.Type == TOKEN_EQ || p.cur.Type == TOKEN_NE ||
		p.cur.Type == TOKEN_LT || p.cur.Type == TOKEN_GT ||
		p.cur.Type == TOKEN_LE || p.cur.Type == TOKEN_GE {
		op := p.cur.Type
		if err := p.advance(); err != nil {
			return nil, err
		}
		right, err := p.parseAddSub()
		if err != nil {
			return nil, err
		}
		left = &BinaryOp{Op: op, Left: left, Right: right}
	}
	return left, nil
}

func (p *Parser) parseAddSub() (Node, error) {
	left, err := p.parseMulDiv()
	if err != nil {
		return nil, err
	}

	for p.cur.Type == TOKEN_PLUS || p.cur.Type == TOKEN_MINUS {
		op := p.cur.Type
		if err := p.advance(); err != nil {
			return nil, err
		}
		right, err := p.parseMulDiv()
		if err != nil {
			return nil, err
		}
		left = &BinaryOp{Op: op, Left: left, Right: right}
	}
	return left, nil
}

func (p *Parser) parseMulDiv() (Node, error) {
	left, err := p.parsePower()
	if err != nil {
		return nil, err
	}

	for p.cur.Type == TOKEN_STAR || p.cur.Type == TOKEN_SLASH ||
		p.cur.Type == TOKEN_FLOOR_DIV || p.cur.Type == TOKEN_PERCENT {
		op := p.cur.Type
		if err := p.advance(); err != nil {
			return nil, err
		}
		right, err := p.parsePower()
		if err != nil {
			return nil, err
		}
		left = &BinaryOp{Op: op, Left: left, Right: right}
	}
	return left, nil
}

func (p *Parser) parsePower() (Node, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}

	// Power is right-associative
	if p.cur.Type == TOKEN_POWER {
		op := p.cur.Type
		if err := p.advance(); err != nil {
			return nil, err
		}
		right, err := p.parsePower()
		if err != nil {
			return nil, err
		}
		return &BinaryOp{Op: op, Left: left, Right: right}, nil
	}
	return left, nil
}

func (p *Parser) parseUnary() (Node, error) {
	if p.cur.Type == TOKEN_MINUS {
		op := p.cur.Type
		if err := p.advance(); err != nil {
			return nil, err
		}
		expr, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return &UnaryOp{Op: op, Expr: expr}, nil
	}
	return p.parsePostfix()
}

func (p *Parser) parsePostfix() (Node, error) {
	node, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}

	for {
		switch p.cur.Type {
		case TOKEN_LPAREN:
			// Function call
			if ident, ok := node.(*Ident); ok {
				args, err := p.parseArgs()
				if err != nil {
					return nil, err
				}
				node = &CallExpr{Func: ident.Name, Args: args}
			} else if attr, ok := node.(*AttrAccess); ok {
				// Method call like str.upper()
				args, err := p.parseArgs()
				if err != nil {
					return nil, err
				}
				node = &CallExpr{Func: "__method__", Args: append([]Node{attr.Obj, &StringLit{Value: attr.Attr}}, args...)}
			} else {
				return nil, fmt.Errorf("cannot call non-function")
			}
		case TOKEN_DOT:
			// Attribute access
			if err := p.advance(); err != nil {
				return nil, err
			}
			if p.cur.Type != TOKEN_IDENT {
				return nil, fmt.Errorf("expected identifier after '.', got %v", p.cur.Type)
			}
			node = &AttrAccess{Obj: node, Attr: p.cur.Value}
			if err := p.advance(); err != nil {
				return nil, err
			}
		default:
			return node, nil
		}
	}
}

func (p *Parser) parseArgs() ([]Node, error) {
	// Skip '('
	if err := p.advance(); err != nil {
		return nil, err
	}

	var args []Node
	if p.cur.Type != TOKEN_RPAREN {
		arg, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		args = append(args, arg)

		for p.cur.Type == TOKEN_COMMA {
			if err := p.advance(); err != nil {
				return nil, err
			}
			arg, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			args = append(args, arg)
		}
	}

	if p.cur.Type != TOKEN_RPAREN {
		return nil, fmt.Errorf("expected ')' after arguments")
	}
	if err := p.advance(); err != nil {
		return nil, err
	}

	return args, nil
}

func (p *Parser) parsePrimary() (Node, error) {
	switch p.cur.Type {
	case TOKEN_NUMBER:
		val, err := strconv.ParseFloat(p.cur.Value, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid number: %s", p.cur.Value)
		}
		if err := p.advance(); err != nil {
			return nil, err
		}
		return &NumberLit{Value: val}, nil

	case TOKEN_STRING:
		val := p.cur.Value
		if err := p.advance(); err != nil {
			return nil, err
		}
		return &StringLit{Value: val}, nil

	case TOKEN_IDENT:
		name := p.cur.Value
		if err := p.advance(); err != nil {
			return nil, err
		}
		return &Ident{Name: name}, nil

	case TOKEN_LPAREN:
		if err := p.advance(); err != nil {
			return nil, err
		}
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if p.cur.Type != TOKEN_RPAREN {
			return nil, fmt.Errorf("expected ')' after expression")
		}
		if err := p.advance(); err != nil {
			return nil, err
		}
		return expr, nil

	case TOKEN_EOF:
		return nil, fmt.Errorf("unexpected end of expression")

	default:
		return nil, fmt.Errorf("unexpected token: %v", p.cur.Value)
	}
}
