package graph

import (
	"errors"
	"fmt"
)

// ErrorKind classifies graph-layer failures.
type ErrorKind string

const (
	ErrKindParse        ErrorKind = "PARSE"
	ErrKindSemantic     ErrorKind = "SEMANTIC"
	ErrKindStorage      ErrorKind = "STORAGE"
	ErrKindExecution    ErrorKind = "EXECUTION"
	ErrKindConflict     ErrorKind = "CONFLICT"
	ErrKindNotFound     ErrorKind = "NOT_FOUND"
	ErrKindInvalidInput ErrorKind = "INVALID_INPUT"
	ErrKindUnsupported  ErrorKind = "UNSUPPORTED"
	ErrKindTimeout      ErrorKind = "TIMEOUT"
	ErrKindInternal     ErrorKind = "INTERNAL"
)

// Error is a structured graph-layer error.
type Error struct {
	Kind    ErrorKind
	Message string
	Cause   error
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Cause != nil {
		return fmt.Sprintf("%s: %s: %v", e.Kind, e.Message, e.Cause)
	}
	return fmt.Sprintf("%s: %s", e.Kind, e.Message)
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func NewError(kind ErrorKind, message string, cause error) error {
	return &Error{Kind: kind, Message: message, Cause: cause}
}

func IsKind(err error, kind ErrorKind) bool {
	var e *Error
	if !errors.As(err, &e) {
		return false
	}
	return e.Kind == kind
}
