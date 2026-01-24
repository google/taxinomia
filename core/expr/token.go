/*
SPDX-License-Identifier: Apache-2.0

Copyright 2024 The Taxinomia Authors
*/

package expr

// TokenType represents the type of a token
type TokenType int

const (
	TOKEN_EOF TokenType = iota
	TOKEN_NUMBER
	TOKEN_STRING
	TOKEN_IDENT
	TOKEN_PLUS
	TOKEN_MINUS
	TOKEN_STAR
	TOKEN_SLASH
	TOKEN_PERCENT
	TOKEN_POWER      // **
	TOKEN_FLOOR_DIV  // //
	TOKEN_LPAREN
	TOKEN_RPAREN
	TOKEN_COMMA
	TOKEN_EQ         // ==
	TOKEN_NE         // !=
	TOKEN_LT         // <
	TOKEN_GT         // >
	TOKEN_LE         // <=
	TOKEN_GE         // >=
	TOKEN_AND        // and
	TOKEN_OR         // or
	TOKEN_NOT        // not
	TOKEN_DOT
)

// Token represents a lexical token
type Token struct {
	Type    TokenType
	Value   string
	Pos     int
}
