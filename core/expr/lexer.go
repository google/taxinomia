/*
SPDX-License-Identifier: Apache-2.0

Copyright 2024 The Taxinomia Authors
*/

package expr

import (
	"fmt"
	"strings"
	"unicode"
)

// Lexer tokenizes an expression string
type Lexer struct {
	input string
	pos   int
	ch    byte
}

// NewLexer creates a new lexer for the given input
func NewLexer(input string) *Lexer {
	l := &Lexer{input: input}
	if len(input) > 0 {
		l.ch = input[0]
	}
	return l
}

func (l *Lexer) advance() {
	l.pos++
	if l.pos >= len(l.input) {
		l.ch = 0
	} else {
		l.ch = l.input[l.pos]
	}
}

func (l *Lexer) peek() byte {
	if l.pos+1 >= len(l.input) {
		return 0
	}
	return l.input[l.pos+1]
}

func (l *Lexer) skipWhitespace() {
	for l.ch != 0 && (l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r') {
		l.advance()
	}
}

// NextToken returns the next token from the input
func (l *Lexer) NextToken() (Token, error) {
	l.skipWhitespace()

	if l.ch == 0 {
		return Token{Type: TOKEN_EOF, Pos: l.pos}, nil
	}

	startPos := l.pos

	// Numbers
	if isDigit(l.ch) || (l.ch == '.' && isDigit(l.peek())) {
		return l.readNumber(startPos)
	}

	// Strings
	if l.ch == '"' || l.ch == '\'' {
		return l.readString(startPos)
	}

	// Identifiers and keywords
	if isLetter(l.ch) || l.ch == '_' {
		return l.readIdent(startPos)
	}

	// Operators
	switch l.ch {
	case '+':
		l.advance()
		return Token{Type: TOKEN_PLUS, Value: "+", Pos: startPos}, nil
	case '-':
		l.advance()
		return Token{Type: TOKEN_MINUS, Value: "-", Pos: startPos}, nil
	case '*':
		l.advance()
		if l.ch == '*' {
			l.advance()
			return Token{Type: TOKEN_POWER, Value: "**", Pos: startPos}, nil
		}
		return Token{Type: TOKEN_STAR, Value: "*", Pos: startPos}, nil
	case '/':
		l.advance()
		if l.ch == '/' {
			l.advance()
			return Token{Type: TOKEN_FLOOR_DIV, Value: "//", Pos: startPos}, nil
		}
		return Token{Type: TOKEN_SLASH, Value: "/", Pos: startPos}, nil
	case '%':
		l.advance()
		return Token{Type: TOKEN_PERCENT, Value: "%", Pos: startPos}, nil
	case '(':
		l.advance()
		return Token{Type: TOKEN_LPAREN, Value: "(", Pos: startPos}, nil
	case ')':
		l.advance()
		return Token{Type: TOKEN_RPAREN, Value: ")", Pos: startPos}, nil
	case ',':
		l.advance()
		return Token{Type: TOKEN_COMMA, Value: ",", Pos: startPos}, nil
	case '.':
		l.advance()
		return Token{Type: TOKEN_DOT, Value: ".", Pos: startPos}, nil
	case '=':
		l.advance()
		if l.ch == '=' {
			l.advance()
			return Token{Type: TOKEN_EQ, Value: "==", Pos: startPos}, nil
		}
		return Token{}, fmt.Errorf("unexpected '=' at position %d, did you mean '=='?", startPos)
	case '!':
		l.advance()
		if l.ch == '=' {
			l.advance()
			return Token{Type: TOKEN_NE, Value: "!=", Pos: startPos}, nil
		}
		return Token{}, fmt.Errorf("unexpected '!' at position %d", startPos)
	case '<':
		l.advance()
		if l.ch == '=' {
			l.advance()
			return Token{Type: TOKEN_LE, Value: "<=", Pos: startPos}, nil
		}
		return Token{Type: TOKEN_LT, Value: "<", Pos: startPos}, nil
	case '>':
		l.advance()
		if l.ch == '=' {
			l.advance()
			return Token{Type: TOKEN_GE, Value: ">=", Pos: startPos}, nil
		}
		return Token{Type: TOKEN_GT, Value: ">", Pos: startPos}, nil
	}

	return Token{}, fmt.Errorf("unexpected character '%c' at position %d", l.ch, startPos)
}

func (l *Lexer) readNumber(startPos int) (Token, error) {
	var sb strings.Builder
	hasDecimal := false

	for isDigit(l.ch) || l.ch == '.' {
		if l.ch == '.' {
			if hasDecimal {
				break
			}
			hasDecimal = true
		}
		sb.WriteByte(l.ch)
		l.advance()
	}

	return Token{Type: TOKEN_NUMBER, Value: sb.String(), Pos: startPos}, nil
}

func (l *Lexer) readString(startPos int) (Token, error) {
	quote := l.ch
	l.advance()
	var sb strings.Builder

	for l.ch != 0 && l.ch != quote {
		if l.ch == '\\' {
			l.advance()
			switch l.ch {
			case 'n':
				sb.WriteByte('\n')
			case 't':
				sb.WriteByte('\t')
			case 'r':
				sb.WriteByte('\r')
			case '\\':
				sb.WriteByte('\\')
			case '"':
				sb.WriteByte('"')
			case '\'':
				sb.WriteByte('\'')
			default:
				sb.WriteByte(l.ch)
			}
		} else {
			sb.WriteByte(l.ch)
		}
		l.advance()
	}

	if l.ch != quote {
		return Token{}, fmt.Errorf("unterminated string starting at position %d", startPos)
	}
	l.advance()

	return Token{Type: TOKEN_STRING, Value: sb.String(), Pos: startPos}, nil
}

func (l *Lexer) readIdent(startPos int) (Token, error) {
	var sb strings.Builder

	for isLetter(l.ch) || isDigit(l.ch) || l.ch == '_' {
		sb.WriteByte(l.ch)
		l.advance()
	}

	value := sb.String()

	// Check for keywords
	switch value {
	case "and":
		return Token{Type: TOKEN_AND, Value: value, Pos: startPos}, nil
	case "or":
		return Token{Type: TOKEN_OR, Value: value, Pos: startPos}, nil
	case "not":
		return Token{Type: TOKEN_NOT, Value: value, Pos: startPos}, nil
	}

	return Token{Type: TOKEN_IDENT, Value: value, Pos: startPos}, nil
}

func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

func isLetter(ch byte) bool {
	return unicode.IsLetter(rune(ch))
}
