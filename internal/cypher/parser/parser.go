package parser

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/antlr4-go/antlr/v4"
	"github.com/spaceqraft/vitaledge/internal/cypher/ast"
	cyphergen "github.com/spaceqraft/vitaledge/internal/cypher/grammar/generated"
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
	if inner, offset, ok := splitExplainPrefix(seg.text); ok {
		if strings.TrimSpace(inner) == "" {
			return nil, &ParseError{Kind: ParseErrorSemantic, Message: "EXPLAIN requires a query", Statement: seg.index}
		}
		innerSeg := seg
		innerSeg.text = inner
		innerSeg.startOffset += offset
		innerStmt, err := parseSegment(innerSeg, fullQuery)
		if err != nil {
			return nil, err
		}
		return &ast.ExplainStatement{Raw: strings.TrimSpace(seg.text), Query: strings.TrimSpace(inner), Statement: innerStmt, SourceSpan: innerStmt.Span()}, nil
	}
	if inner, offset, ok := splitProfilePrefix(seg.text); ok {
		if strings.TrimSpace(inner) == "" {
			return nil, &ParseError{Kind: ParseErrorSemantic, Message: "PROFILE requires a query", Statement: seg.index}
		}
		innerSeg := seg
		innerSeg.text = inner
		innerSeg.startOffset += offset
		innerStmt, err := parseSegment(innerSeg, fullQuery)
		if err != nil {
			return nil, err
		}
		return &ast.ProfileStatement{Raw: strings.TrimSpace(seg.text), Query: strings.TrimSpace(inner), Statement: innerStmt, SourceSpan: innerStmt.Span()}, nil
	}

	existsRewrite := rewriteExistsBlocks(seg.text)
	reduceRewrite := rewriteReduceCalls(existsRewrite.text)
	input := antlr.NewInputStream(reduceRewrite.text)
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

	stmt, err := buildStatement(root, seg, fullQuery)
	if err != nil {
		return nil, err
	}
	restoreExistsBlocks(stmt, existsRewrite.placeholders)
	if err := validatePatternVariableScoping(stmt, seg); err != nil {
		return nil, err
	}
	if err := validateUnexpectedSyntax(stmt, seg); err != nil {
		return nil, err
	}
	restoreExistsBlocks(stmt, reduceRewrite.placeholders)
	return stmt, nil
}

func splitExplainPrefix(raw string) (inner string, offset int, ok bool) {
	return splitStatementPrefix(raw, "EXPLAIN")
}

func splitProfilePrefix(raw string) (inner string, offset int, ok bool) {
	return splitStatementPrefix(raw, "PROFILE")
}

func splitStatementPrefix(raw string, keyword string) (inner string, offset int, ok bool) {
	start := 0
	for start < len(raw) && unicode.IsSpace(rune(raw[start])) {
		start++
	}
	if start+len(keyword) > len(raw) {
		return "", 0, false
	}
	if !strings.EqualFold(raw[start:start+len(keyword)], keyword) {
		return "", 0, false
	}
	end := start + len(keyword)
	if end < len(raw) {
		r := rune(raw[end])
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
			return "", 0, false
		}
	}
	for end < len(raw) && unicode.IsSpace(rune(raw[end])) {
		end++
	}
	return raw[end:], end, true
}

type existsRewrite struct {
	text         string
	placeholders map[string]string
}

func rewriteExistsBlocks(raw string) existsRewrite {
	placeholders := map[string]string{}
	if raw == "" {
		return existsRewrite{text: raw, placeholders: placeholders}
	}

	var out strings.Builder
	n := 0
	for i := 0; i < len(raw); {
		if !hasWordAt(raw, i, "EXISTS") {
			out.WriteByte(raw[i])
			i++
			continue
		}
		j := i + len("EXISTS")
		for j < len(raw) && unicode.IsSpace(rune(raw[j])) {
			j++
		}
		if j < len(raw) && raw[j] == '(' {
			placeholder := fmt.Sprintf("__ve_exists_fn_%d", n)
			n++
			placeholders[placeholder] = raw[i : i+len("EXISTS")]
			out.WriteString(placeholder)
			i += len("EXISTS")
			continue
		}
		if j >= len(raw) || raw[j] != '{' {
			out.WriteByte(raw[i])
			i++
			continue
		}
		end := findMatchingBrace(raw, j)
		if end < 0 {
			out.WriteByte(raw[i])
			i++
			continue
		}

		placeholder := fmt.Sprintf("__ve_exists_%d", n)
		n++
		placeholders[placeholder] = raw[i : end+1]
		out.WriteString(placeholder)
		i = end + 1
	}

	return existsRewrite{text: out.String(), placeholders: placeholders}
}

func rewriteReduceCalls(raw string) existsRewrite {
	placeholders := map[string]string{}
	if raw == "" {
		return existsRewrite{text: raw, placeholders: placeholders}
	}

	var out strings.Builder
	n := 0
	for i := 0; i < len(raw); {
		if !hasWordAt(raw, i, "REDUCE") {
			out.WriteByte(raw[i])
			i++
			continue
		}
		j := i + len("REDUCE")
		for j < len(raw) && unicode.IsSpace(rune(raw[j])) {
			j++
		}
		if j >= len(raw) || raw[j] != '(' {
			out.WriteByte(raw[i])
			i++
			continue
		}
		end := findMatchingParen(raw, j)
		if end < 0 {
			out.WriteByte(raw[i])
			i++
			continue
		}
		placeholder := fmt.Sprintf("__ve_reduce_%d", n)
		n++
		placeholders[placeholder] = raw[i : end+1]
		out.WriteString(placeholder)
		i = end + 1
	}

	return existsRewrite{text: out.String(), placeholders: placeholders}
}

func hasWordAt(raw string, idx int, word string) bool {
	if idx < 0 || idx+len(word) > len(raw) {
		return false
	}
	if !strings.EqualFold(raw[idx:idx+len(word)], word) {
		return false
	}
	if idx > 0 {
		prev := rune(raw[idx-1])
		if unicode.IsLetter(prev) || unicode.IsDigit(prev) || prev == '_' {
			return false
		}
	}
	nextIdx := idx + len(word)
	if nextIdx < len(raw) {
		next := rune(raw[nextIdx])
		if unicode.IsLetter(next) || unicode.IsDigit(next) || next == '_' {
			return false
		}
	}
	return true
}

func findMatchingBrace(raw string, start int) int {
	depth := 0
	for i := start; i < len(raw); i++ {
		switch raw[i] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

func restoreExistsBlocks(stmt ast.Statement, placeholders map[string]string) {
	if len(placeholders) == 0 || stmt == nil {
		return
	}
	replace := func(raw string) string {
		for token, original := range placeholders {
			raw = strings.ReplaceAll(raw, token, original)
		}
		return raw
	}

	switch s := stmt.(type) {
	case *ast.QueryStatement:
		for i := range s.Parts {
			for j := range s.Parts[i].Clauses {
				s.Parts[i].Clauses[j].Raw = replace(s.Parts[i].Clauses[j].Raw)
			}
		}
	case *ast.MatchQueryStatement:
		for i := range s.MatchClauses {
			s.MatchClauses[i].Pattern = replace(s.MatchClauses[i].Pattern)
			if s.MatchClauses[i].Where != nil {
				s.MatchClauses[i].Where.Raw = replace(s.MatchClauses[i].Where.Raw)
			}
		}
		for i := range s.Return.Items {
			s.Return.Items[i].Expression.Raw = replace(s.Return.Items[i].Expression.Raw)
		}
	}
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
