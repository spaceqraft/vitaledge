package parser

import (
	"fmt"
	"strings"

	"github.com/antlr4-go/antlr/v4"
	"github.com/paegun/vitaledge/internal/cypher/ast"
	cyphergen "github.com/paegun/vitaledge/internal/cypher/grammar/generated"
)

const GrammarVersion = "openCypher M23"

// ParseBatch parses semicolon-separated statements into typed AST nodes.
func ParseBatch(query string) (*ast.Batch, error) {
	segments := splitStatements(query)
	if len(segments) == 0 {
		return nil, &ParseError{Kind: ParseErrorSemantic, Message: "empty query", Statement: 1}
	}

	result := &ast.Batch{Statements: make([]ast.Statement, 0, len(segments))}
	for _, seg := range segments {
		stmt, err := parseSegment(seg, query)
		if err != nil {
			return nil, err
		}
		result.Statements = append(result.Statements, stmt)
	}

	return result, nil
}

// ParseStatement parses a single statement (or a single-statement batch).
func ParseStatement(query string) (ast.Statement, error) {
	batch, err := ParseBatch(query)
	if err != nil {
		return nil, err
	}
	if len(batch.Statements) != 1 {
		return nil, &ParseError{
			Kind:      ParseErrorSemantic,
			Message:   fmt.Sprintf("expected exactly one statement, got %d", len(batch.Statements)),
			Statement: 1,
		}
	}
	return batch.Statements[0], nil
}

func parseSegment(seg statementSegment, fullQuery string) (ast.Statement, error) {
	input := antlr.NewInputStream(seg.text)
	lexer := cyphergen.NewCypherLexer(input)
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	p := cyphergen.NewCypherParser(stream)

	errListener := &firstSyntaxErrorListener{segment: seg, fullQuery: fullQuery}
	lexer.RemoveErrorListeners()
	p.RemoveErrorListeners()
	lexer.AddErrorListener(errListener)
	p.AddErrorListener(errListener)

	root := p.OC_Cypher()
	if errListener.err != nil {
		return nil, errListener.err
	}
	if p.HasError() {
		return nil, &ParseError{Kind: ParseErrorInternal, Message: "parser failed unexpectedly", Statement: seg.index}
	}

	return buildStatement(root, seg, fullQuery)
}

type firstSyntaxErrorListener struct {
	*antlr.DefaultErrorListener
	segment   statementSegment
	fullQuery string
	err       *ParseError
}

func (l *firstSyntaxErrorListener) SyntaxError(_ antlr.Recognizer, _ interface{}, line int, column int, msg string, _ antlr.RecognitionException) {
	if l.err != nil {
		return
	}

	gLine, gCol := localToGlobal(l.segment, l.fullQuery, line, column)
	l.err = &ParseError{
		Kind:      ParseErrorSyntax,
		Message:   strings.TrimSpace(msg),
		Line:      gLine,
		Column:    gCol,
		Statement: l.segment.index,
	}
}
