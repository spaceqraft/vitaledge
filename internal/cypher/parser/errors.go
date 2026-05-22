package parser

import "fmt"

// ParseErrorKind classifies parse failures.
type ParseErrorKind string

const (
	ParseErrorSyntax      ParseErrorKind = "SYNTAX"
	ParseErrorUnsupported ParseErrorKind = "UNSUPPORTED"
	ParseErrorSemantic    ParseErrorKind = "SEMANTIC"
	ParseErrorInternal    ParseErrorKind = "INTERNAL"
)

// ParseError provides fail-fast, user-facing parse diagnostics.
type ParseError struct {
	Kind      ParseErrorKind
	Message   string
	Line      int
	Column    int
	Statement int
}

func (e *ParseError) Error() string {
	if e == nil {
		return "<nil>"
	}

	if e.Line > 0 && e.Column > 0 {
		return fmt.Sprintf("%s error at statement %d (%d:%d): %s", e.Kind, e.Statement, e.Line, e.Column, e.Message)
	}

	if e.Statement > 0 {
		return fmt.Sprintf("%s error at statement %d: %s", e.Kind, e.Statement, e.Message)
	}

	return fmt.Sprintf("%s error: %s", e.Kind, e.Message)
}
