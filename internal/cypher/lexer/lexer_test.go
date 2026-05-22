package lexer

import (
	"reflect"
	"testing"
)

func assertTokensEqual(t *testing.T, got, expected []Token) {
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("Output does not match expected tokens.\nActual:   %#v\nExpected: %#v", got, expected)
	}
}

func TestLexNodeMatch(t *testing.T) {
	inputTokens := []Token{
		{Type: PUNCTUATION, Value: "("},
		{Type: IDENTIFIER, Value: "n"},
		{Type: PUNCTUATION, Value: ")"},
	}

	expectedTokens := []Token{
		{Type: NODE_START, Value: "("},
		{Type: NODE_LABEL, Value: "n"},
		{Type: NODE_END, Value: ")"},
	}

	outputTokens, err := Lex(inputTokens)
	if err != nil {
		t.Fatalf("Unexpected error during lexing: %v", err)
	}

	assertTokensEqual(t, outputTokens, expectedTokens)

	inputTokens = []Token{
		{Type: PUNCTUATION, Value: "("},
		{Type: IDENTIFIER, Value: "p"},
		{Type: PUNCTUATION, Value: ":"},
		{Type: IDENTIFIER, Value: "Person"},
		{Type: PUNCTUATION, Value: ")"},
	}

	expectedTokens = []Token{
		{Type: NODE_START, Value: "("},
		{Type: NODE_LABEL, Value: "p"},
		{Type: NODE_TYPE, Value: "Person"},
		{Type: NODE_END, Value: ")"},
	}

	outputTokens, err = Lex(inputTokens)
	if err != nil {
		t.Fatalf("Unexpected error during lexing: %v", err)
	}

	assertTokensEqual(t, outputTokens, expectedTokens)

	inputTokens = []Token{
		{Type: PUNCTUATION, Value: "("},
		{Type: IDENTIFIER, Value: "n"},
		{Type: PUNCTUATION, Value: ":"},
		{Type: IDENTIFIER, Value: "Movie"},
		{Type: PUNCTUATION, Value: "|"},
		{Type: IDENTIFIER, Value: "Actor"},
		{Type: PUNCTUATION, Value: ")"},
	}

	expectedTokens = []Token{
		{Type: NODE_START, Value: "("},
		{Type: NODE_LABEL, Value: "n"},
		{Type: NODE_TYPE, Value: "Movie"},
		{Type: NODE_TYPE, Value: "Actor"},
		{Type: NODE_END, Value: ")"},
	}

	outputTokens, err = Lex(inputTokens)
	if err != nil {
		t.Fatalf("Unexpected error during lexing: %v", err)
	}

	assertTokensEqual(t, outputTokens, expectedTokens)

	inputTokens = []Token{
		{Type: PUNCTUATION, Value: "("},
		{Type: IDENTIFIER, Value: "actor"},
		{Type: PUNCTUATION, Value: ":"},
		{Type: IDENTIFIER, Value: "Actor"},
		{Type: PUNCTUATION, Value: "{"},
		{Type: IDENTIFIER, Value: "name"},
		{Type: PUNCTUATION, Value: ":"},
		{Type: IDENTIFIER, Value: "Sylvester Stallone"},
		{Type: PUNCTUATION, Value: ","},
		{Type: IDENTIFIER, Value: "birthYear"},
		{Type: PUNCTUATION, Value: ":"},
		{Type: IDENTIFIER, Value: "1946"},
		{Type: PUNCTUATION, Value: "}"},
		{Type: PUNCTUATION, Value: ")"},
	}

	expectedTokens = []Token{
		{Type: NODE_START, Value: "("},
		{Type: NODE_LABEL, Value: "actor"},
		{Type: NODE_TYPE, Value: "Actor"},
		{Type: PROPERTY_START, Value: "{"},
		{Type: PROPERTY_KEY, Value: "name"},
		{Type: PROPERTY_VALUE, Value: "Sylvester Stallone"},
		{Type: PROPERTY_KEY, Value: "birthYear"},
		{Type: PROPERTY_VALUE, Value: "1946"},
		{Type: PROPERTY_END, Value: "}"},
		{Type: NODE_END, Value: ")"},
	}

	outputTokens, err = Lex(inputTokens)
	if err != nil {
		t.Fatalf("Unexpected error during lexing: %v", err)
	}

	assertTokensEqual(t, outputTokens, expectedTokens)
}

func TestLexEdgeMatch(t *testing.T) {
	inputTokens := []Token{
		{Type: PUNCTUATION, Value: "("},
		{Type: IDENTIFIER, Value: "actor"},
		{Type: PUNCTUATION, Value: ")"},
		{Type: PUNCTUATION, Value: "-"},
		{Type: PUNCTUATION, Value: "["},
		{Type: IDENTIFIER, Value: "r"},
		{Type: PUNCTUATION, Value: ":"},
		{Type: IDENTIFIER, Value: "ACTED_IN"},
		{Type: PUNCTUATION, Value: "]"},
		{Type: PUNCTUATION, Value: "-"},
		{Type: PUNCTUATION, Value: ">"},
		{Type: PUNCTUATION, Value: "("},
		{Type: IDENTIFIER, Value: "movie"},
		{Type: PUNCTUATION, Value: ")"},
	}

	expectedTokens := []Token{
		{Type: NODE_START, Value: "("},
		{Type: NODE_LABEL, Value: "actor"},
		{Type: NODE_END, Value: ")"},
		{Type: EDGE_START, Value: "-"},
		{Type: EDGE_LABEL, Value: "r"},
		{Type: EDGE_TYPE, Value: "ACTED_IN"},
		{Type: EDGE_END, Value: ">"},
		{Type: NODE_START, Value: "("},
		{Type: NODE_LABEL, Value: "movie"},
		{Type: NODE_END, Value: ")"},
	}

	outputTokens, err := Lex(inputTokens)
	if err != nil {
		t.Fatalf("Unexpected error during lexing: %v", err)
	}

	assertTokensEqual(t, outputTokens, expectedTokens)

	inputTokens = []Token{
		{Type: PUNCTUATION, Value: "("},
		{Type: IDENTIFIER, Value: "actor"},
		{Type: PUNCTUATION, Value: ")"},
		{Type: PUNCTUATION, Value: "-"},
		{Type: PUNCTUATION, Value: "-"},
		{Type: PUNCTUATION, Value: "("},
		{Type: IDENTIFIER, Value: "n"},
		{Type: PUNCTUATION, Value: ")"},
	}

	expectedTokens = []Token{
		{Type: NODE_START, Value: "("},
		{Type: NODE_LABEL, Value: "actor"},
		{Type: NODE_END, Value: ")"},
		{Type: EDGE_START, Value: "-"},
		{Type: EDGE_END, Value: "-"},
		{Type: NODE_START, Value: "("},
		{Type: NODE_LABEL, Value: "n"},
		{Type: NODE_END, Value: ")"},
	}

	outputTokens, err = Lex(inputTokens)
	if err != nil {
		t.Fatalf("Unexpected error during lexing: %v", err)
	}

	assertTokensEqual(t, outputTokens, expectedTokens)

	inputTokens = []Token{
		{Type: PUNCTUATION, Value: "("},
		{Type: IDENTIFIER, Value: "actor"},
		{Type: PUNCTUATION, Value: ")"},
		{Type: PUNCTUATION, Value: "<"},
		{Type: PUNCTUATION, Value: "-"},
		{Type: PUNCTUATION, Value: "-"},
		{Type: PUNCTUATION, Value: "("},
		{Type: IDENTIFIER, Value: "n"},
		{Type: PUNCTUATION, Value: ")"},
	}

	expectedTokens = []Token{
		{Type: NODE_START, Value: "("},
		{Type: NODE_LABEL, Value: "actor"},
		{Type: NODE_END, Value: ")"},
		{Type: EDGE_START, Value: "<"},
		{Type: EDGE_END, Value: "-"},
		{Type: NODE_START, Value: "("},
		{Type: NODE_LABEL, Value: "n"},
		{Type: NODE_END, Value: ")"},
	}

	outputTokens, err = Lex(inputTokens)
	if err != nil {
		t.Fatalf("Unexpected error during lexing: %v", err)
	}

	assertTokensEqual(t, outputTokens, expectedTokens)

	inputTokens = []Token{
		{Type: PUNCTUATION, Value: "("},
		{Type: IDENTIFIER, Value: "actor"},
		{Type: PUNCTUATION, Value: ")"},
		{Type: PUNCTUATION, Value: "-"},
		{Type: PUNCTUATION, Value: "["},
		{Type: IDENTIFIER, Value: "r"},
		{Type: PUNCTUATION, Value: ":"},
		{Type: IDENTIFIER, Value: "ACTED_IN"},
		{Type: PUNCTUATION, Value: "]"},
		{Type: PUNCTUATION, Value: "-"},
		{Type: PUNCTUATION, Value: ">"},
		{Type: PUNCTUATION, Value: "("},
		{Type: IDENTIFIER, Value: "n"},
		{Type: PUNCTUATION, Value: ")"},
	}

	expectedTokens = []Token{
		{Type: NODE_START, Value: "("},
		{Type: NODE_LABEL, Value: "actor"},
		{Type: NODE_END, Value: ")"},
		{Type: EDGE_START, Value: "-"},
		{Type: EDGE_LABEL, Value: "r"},
		{Type: EDGE_TYPE, Value: "ACTED_IN"},
		{Type: EDGE_END, Value: ">"},
		{Type: NODE_START, Value: "("},
		{Type: NODE_LABEL, Value: "n"},
		{Type: NODE_END, Value: ")"},
	}

	outputTokens, err = Lex(inputTokens)
	if err != nil {
		t.Fatalf("Unexpected error during lexing: %v", err)
	}

	assertTokensEqual(t, outputTokens, expectedTokens)
}
