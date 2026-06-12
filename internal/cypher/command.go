package cypher

import (
	"fmt"

	"github.com/spaceqraft/vitaledge/internal/cypher/ast"
	"github.com/spaceqraft/vitaledge/internal/cypher/parser"
)

type Command interface {
	Query() string
}

func ParseCommand(query string) (Command, error) {
	batch, err := parser.ParseBatch(query)
	if err != nil {
		return nil, err
	}
	if len(batch.Statements) != 1 {
		return nil, fmt.Errorf("expected exactly one statement, got %d", len(batch.Statements))
	}

	return &ParsedCommand{query: query, statement: batch.Statements[0]}, nil
}

func ParseBatch(query string) (*ast.Batch, error) {
	return parser.ParseBatch(query)
}

func ParseStatement(query string) (ast.Statement, error) {
	return parser.ParseStatement(query)
}

type SimpleCommand struct {
	query string
}

func (c *SimpleCommand) Query() string {
	return c.query
}

type ParsedCommand struct {
	query     string
	statement ast.Statement
}

func (c *ParsedCommand) Query() string {
	return c.query
}

func (c *ParsedCommand) Statement() ast.Statement {
	return c.statement
}
