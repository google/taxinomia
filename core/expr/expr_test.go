/*
SPDX-License-Identifier: Apache-2.0

Copyright 2024 The Taxinomia Authors
*/

package expr

import (
	"fmt"
	"testing"
)

// Test data for benchmarks
var testColumns = map[string][]interface{}{
	"price":      {10.5, 20.0, 15.75, 8.99, 100.0},
	"qty":        {5.0, 10.0, 3.0, 20.0, 1.0},
	"name":       {"Apple", "Banana", "Cherry", "Date", "Elderberry"},
	"category":   {"fruit", "fruit", "fruit", "fruit", "fruit"},
	"in_stock":   {1.0, 1.0, 0.0, 1.0, 0.0},
}

func makeColumnGetter(row int) ColumnGetter {
	return func(colName string, rowIndex uint32) (Value, error) {
		col, ok := testColumns[colName]
		if !ok {
			return NilValue(), fmt.Errorf("column not found: %s", colName)
		}
		idx := int(rowIndex) % len(col)
		switch v := col[idx].(type) {
		case float64:
			return NewNumber(v), nil
		case string:
			return NewString(v), nil
		default:
			return NilValue(), fmt.Errorf("unknown type")
		}
	}
}

// Basic functionality tests

func TestArithmetic(t *testing.T) {
	tests := []struct {
		expr     string
		expected float64
	}{
		{"1 + 2", 3},
		{"10 - 3", 7},
		{"4 * 5", 20},
		{"20 / 4", 5},
		{"7 // 2", 3},
		{"7 % 3", 1},
		{"2 ** 3", 8},
		{"(1 + 2) * 3", 9},
		{"-5", -5},
		{"--5", 5},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			compiled, err := Compile(tt.expr)
			if err != nil {
				t.Fatalf("compile error: %v", err)
			}
			bound := compiled.Bind(makeColumnGetter(0))
			val, err := bound.Eval(0)
			if err != nil {
				t.Fatalf("eval error: %v", err)
			}
			if !val.IsNumber() || val.AsNumber() != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, val.AsNumber())
			}
		})
	}
}

func TestStringOperations(t *testing.T) {
	tests := []struct {
		expr     string
		expected string
	}{
		{`"hello" + " " + "world"`, "hello world"},
		{`"HELLO".lower()`, "hello"},
		{`"hello".upper()`, "HELLO"},
		{`"  trim  ".strip()`, "trim"},
		{`"hello".startswith("he")`, "True"},
		{`"hello".endswith("lo")`, "True"},
		{`"hello".contains("ll")`, "True"},
		{`"hello world".replace("world", "there")`, "hello there"},
		{`len("hello")`, "5"},
		{`str(123)`, "123"},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			compiled, err := Compile(tt.expr)
			if err != nil {
				t.Fatalf("compile error: %v", err)
			}
			bound := compiled.Bind(makeColumnGetter(0))
			val, err := bound.Eval(0)
			if err != nil {
				t.Fatalf("eval error: %v", err)
			}
			if val.AsString() != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, val.AsString())
			}
		})
	}
}

func TestComparisons(t *testing.T) {
	tests := []struct {
		expr     string
		expected bool
	}{
		{"1 == 1", true},
		{"1 != 2", true},
		{"1 < 2", true},
		{"2 > 1", true},
		{"1 <= 1", true},
		{"2 >= 2", true},
		{`"a" < "b"`, true},
		{`"hello" == "hello"`, true},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			compiled, err := Compile(tt.expr)
			if err != nil {
				t.Fatalf("compile error: %v", err)
			}
			bound := compiled.Bind(makeColumnGetter(0))
			val, err := bound.Eval(0)
			if err != nil {
				t.Fatalf("eval error: %v", err)
			}
			if val.AsBool() != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, val.AsBool())
			}
		})
	}
}

func TestLogical(t *testing.T) {
	tests := []struct {
		expr     string
		expected bool
	}{
		{"1 and 1", true},
		{"1 and 0", false},
		{"0 or 1", true},
		{"0 or 0", false},
		{"not 0", true},
		{"not 1", false},
		{"1 and 2 and 3", true},
		{"0 or 0 or 1", true},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			compiled, err := Compile(tt.expr)
			if err != nil {
				t.Fatalf("compile error: %v", err)
			}
			bound := compiled.Bind(makeColumnGetter(0))
			val, err := bound.Eval(0)
			if err != nil {
				t.Fatalf("eval error: %v", err)
			}
			if val.AsBool() != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, val.AsBool())
			}
		})
	}
}

func TestColumnAccess(t *testing.T) {
	compiled, err := Compile("price * qty")
	if err != nil {
		t.Fatalf("compile error: %v", err)
	}
	bound := compiled.Bind(makeColumnGetter(0))

	// Row 0: price=10.5, qty=5 -> 52.5
	val, err := bound.Eval(0)
	if err != nil {
		t.Fatalf("eval error: %v", err)
	}
	if val.AsNumber() != 52.5 {
		t.Errorf("expected 52.5, got %v", val.AsNumber())
	}
}

func TestFunctions(t *testing.T) {
	tests := []struct {
		expr     string
		expected string
	}{
		{"abs(-5)", "5"},
		{"round(3.7)", "4"},
		{"round(3.14159, 2)", "3.14"},
		{"min(5, 3, 8)", "3"},
		{"max(5, 3, 8)", "8"},
		{`concat("a", "b", "c")`, "abc"},
		{`upper("hello")`, "HELLO"},
		{`lower("HELLO")`, "hello"},
		{"int(3.9)", "3"},
		{"float(5)", "5"},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			compiled, err := Compile(tt.expr)
			if err != nil {
				t.Fatalf("compile error: %v", err)
			}
			bound := compiled.Bind(makeColumnGetter(0))
			val, err := bound.Eval(0)
			if err != nil {
				t.Fatalf("eval error: %v", err)
			}
			if val.AsString() != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, val.AsString())
			}
		})
	}
}

// Benchmarks

func BenchmarkCompileSimple(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = Compile("price * qty")
	}
}

func BenchmarkCompileComplex(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = Compile("(price * qty) + (price * 0.1) - 5")
	}
}

func BenchmarkCompileString(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = Compile(`name.upper() + " - " + category`)
	}
}

func BenchmarkEvalSimple(b *testing.B) {
	compiled, _ := Compile("price * qty")
	bound := compiled.Bind(makeColumnGetter(0))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = bound.Eval(uint32(i % 5))
	}
}

func BenchmarkEvalComplex(b *testing.B) {
	compiled, _ := Compile("(price * qty) + (price * 0.1) - 5")
	bound := compiled.Bind(makeColumnGetter(0))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = bound.Eval(uint32(i % 5))
	}
}

func BenchmarkEvalString(b *testing.B) {
	compiled, _ := Compile(`name.upper() + " - " + category`)
	bound := compiled.Bind(makeColumnGetter(0))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = bound.Eval(uint32(i % 5))
	}
}

func BenchmarkEvalComparison(b *testing.B) {
	compiled, _ := Compile("price > 10 and qty < 100")
	bound := compiled.Bind(makeColumnGetter(0))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = bound.Eval(uint32(i % 5))
	}
}

func BenchmarkEvalFunction(b *testing.B) {
	compiled, _ := Compile("round(price * qty, 2)")
	bound := compiled.Bind(makeColumnGetter(0))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = bound.Eval(uint32(i % 5))
	}
}

func BenchmarkEvalStringMethod(b *testing.B) {
	compiled, _ := Compile(`name.lower().replace("a", "x")`)
	bound := compiled.Bind(makeColumnGetter(0))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = bound.Eval(uint32(i % 5))
	}
}

// Benchmark with many rows to simulate real usage
func BenchmarkEval1000Rows(b *testing.B) {
	compiled, _ := Compile("price * qty")
	bound := compiled.Bind(makeColumnGetter(0))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for row := 0; row < 1000; row++ {
			_, _ = bound.Eval(uint32(row % 5))
		}
	}
}

func BenchmarkEval10000Rows(b *testing.B) {
	compiled, _ := Compile("price * qty")
	bound := compiled.Bind(makeColumnGetter(0))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for row := 0; row < 10000; row++ {
			_, _ = bound.Eval(uint32(row % 5))
		}
	}
}

// Compare compile+eval vs just eval (showing benefit of pre-compilation)
func BenchmarkCompileAndEval(b *testing.B) {
	getter := makeColumnGetter(0)
	for i := 0; i < b.N; i++ {
		compiled, _ := Compile("price * qty")
		bound := compiled.Bind(getter)
		_, _ = bound.Eval(0)
	}
}

func BenchmarkPrecompiledEval(b *testing.B) {
	compiled, _ := Compile("price * qty")
	bound := compiled.Bind(makeColumnGetter(0))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = bound.Eval(0)
	}
}
