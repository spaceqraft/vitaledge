package cypher

type TokenType int

const (
	// scanner token types
	KEYWORD TokenType = iota
	IDENTIFIER
	NUMBER
	STRING
	PUNCTUATION
	NEGATIVE_SIGN

	// lexer token types
	VERTEX_START
	VERTEX_END
	VERTEX_LABEL
	VERTEX_TYPE
	VERTEX_NOT_TYPE
	PROPERTY_START
	PROPERTY_KEY
	PROPERTY_VALUE
	PROPERTY_END
	EDGE_START
	EDGE_LABEL
	EDGE_TYPE
	EDGE_END
)

var TokenTypeNames = map[TokenType]string{
	// scanner token types
	KEYWORD:       "KEYWORD",
	IDENTIFIER:    "IDENTIFIER",
	NUMBER:        "NUMBER",
	STRING:        "STRING",
	PUNCTUATION:   "PUNCTUATION",
	NEGATIVE_SIGN: "NEGATIVE_SIGN",

	// lexer token types
	VERTEX_START:    "VERTEX_START",
	VERTEX_END:      "VERTEX_END",
	VERTEX_LABEL:    "VERTEX_LABEL",
	VERTEX_TYPE:     "VERTEX_TYPE",
	VERTEX_NOT_TYPE: "VERTEX_NOT_TYPE",
	PROPERTY_START:  "PROPERTY_START",
	PROPERTY_KEY:    "PROPERTY_KEY",
	PROPERTY_VALUE:  "PROPERTY_VALUE",
	PROPERTY_END:    "PROPERTY_END",
	EDGE_START:      "EDGE_START",
	EDGE_LABEL:      "EDGE_LABEL",
	EDGE_TYPE:       "EDGE_TYPE",
	EDGE_END:        "EDGE_END",
}

type Token struct {
	Type  TokenType
	Value string
}

// BOOLEAN
// true
// false

// INTEGER
// A decimal INTEGER literal: 13, -40000
// A hexadecimal INTEGER literal (prefix 0x): 0x13af, 0xFC3A9, -0x66eff
// An octal INTEGER literal (prefix 0o): 0o1372, -0o5671

// FLOAT
// A FLOAT literal in common notation: 3.14
// A FLOAT literal in scientific notation: 6.022E23, 1e-9
// Literals for special FLOAT values: Inf, Infinity, NaN

// NOTE: Any numeric literal may contain an underscore _ between digits.
// There may be an underscore between the 0x or 0o and the digits for hexadecimal
// and octal literals. For example: 1_000_000, 0x_FC3A9, and 0o_1372.

// STRING
// A STRING quoted with single quotes: 'Hello, 42'
// A STRING quoted with double quotes: "Hello, 42"
// A STRING with whitespace: ' hello '
// A STRING with escape sequences: 'Line 1\nLine 2', 'Tab\tseparated'
// A STRING containing Unicode characters: '그래프는 어디에나 있다'
// A STRING using a Unicode code point: 'Name: \u004Aohn' (produces 'Name: John')

// escaped characters in strings:
// \t tab
// \b backspace
// \n newline
// \r carriage return
// \f form feed
// \' single quote
// \" double quote
// \\ backslash
// \uxxx unicode character
