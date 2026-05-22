package scanner

import (
	"reflect"
	"testing"
)

func assertTokensEqual(t *testing.T, got, expected []Token) {
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("Output does not match expected tokens.\nActual:   %#v\nExpected: %#v", got, expected)
	}
}

func TestExtractTokensQuoteOnly(t *testing.T) {
	query := "'MATCH is a keyword, but this is a string.'"
	tokens, err := Scan(query)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	expectedTokens := []Token{
		{Type: STRING, Value: "'MATCH is a keyword, but this is a string.'"},
	}

	assertTokensEqual(t, tokens, expectedTokens)

	query = "'그래프는 어디에나 있다'"
	tokens, err = Scan(query)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	expectedTokens = []Token{
		{Type: STRING, Value: "'그래프는 어디에나 있다'"},
	}

	assertTokensEqual(t, tokens, expectedTokens)

	query = "'Name: \u004Aohn'"
	tokens, err = Scan(query)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	expectedTokens = []Token{
		{Type: STRING, Value: "'Name: \u004Aohn'"},
	}

	assertTokensEqual(t, tokens, expectedTokens)
}

func TestExtractTokensKeywordsOnly(t *testing.T) {
	queries := Keywords()

	for _, query := range queries {
		expectedTokens := []Token{
			{Type: KEYWORD, Value: query},
		}

		tokens, err := Scan(query)
		if err != nil {
			t.Fatalf("Unexpected error for query '%s': %v", query, err)
		}

		assertTokensEqual(t, tokens, expectedTokens)
	}
}

func TestExtractTokensNumbersOnly(t *testing.T) {
	query := "123 45.67 0x13af 0xFC3A9 -0x66eff 0o1372 -0o5671 6.022E23 1e-9 Inf Infinity NaN"
	expectedTokens := []Token{
		{Type: NUMBER, Value: "123"},
		{Type: NUMBER, Value: "45.67"},
		{Type: NUMBER, Value: "0x13af"},
		{Type: NUMBER, Value: "0xFC3A9"},
		{Type: NEGATIVE_SIGN, Value: "-"},
		{Type: NUMBER, Value: "0x66eff"},
		{Type: NUMBER, Value: "0o1372"},
		{Type: NEGATIVE_SIGN, Value: "-"},
		{Type: NUMBER, Value: "0o5671"},
		{Type: NUMBER, Value: "6.022E23"},
		{Type: NUMBER, Value: "1e-9"},
		{Type: NUMBER, Value: "Inf"},
		{Type: NUMBER, Value: "Infinity"},
		{Type: NUMBER, Value: "NaN"},
	}

	tokens, err := Scan(query)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	assertTokensEqual(t, tokens, expectedTokens)
}

func TestExtractTokensMatch(t *testing.T) {
	query := "MATCH (n:Person {name: 'Alice'}) RETURN n.name"
	expectedTokens := []Token{
		{Type: KEYWORD, Value: "MATCH"},
		{Type: PUNCTUATION, Value: "("},
		{Type: IDENTIFIER, Value: "n"},
		{Type: PUNCTUATION, Value: ":"},
		{Type: IDENTIFIER, Value: "Person"},
		{Type: PUNCTUATION, Value: "{"},
		{Type: IDENTIFIER, Value: "name"},
		{Type: PUNCTUATION, Value: ":"},
		{Type: STRING, Value: "'Alice'"},
		{Type: PUNCTUATION, Value: "}"},
		{Type: PUNCTUATION, Value: ")"},
		{Type: KEYWORD, Value: "RETURN"},
		{Type: IDENTIFIER, Value: "n"},
		{Type: PUNCTUATION, Value: "."},
		{Type: IDENTIFIER, Value: "name"},
	}

	tokens, err := Scan(query)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	assertTokensEqual(t, tokens, expectedTokens)
}
