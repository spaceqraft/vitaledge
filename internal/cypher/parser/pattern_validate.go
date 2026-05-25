package parser

import (
	"strings"

	"github.com/paegun/vitaledge/internal/cypher/ast"
)

type patternVarRole string

const (
	patternRoleNode patternVarRole = "node"
	patternRoleRel  patternVarRole = "relationship"
	patternRolePath patternVarRole = "path"
)

type patternBinding struct {
	name string
	role patternVarRole
}

func validatePatternVariableScoping(stmt ast.Statement, seg statementSegment) error {
	bound := map[string]patternVarRole{}

	switch typed := stmt.(type) {
	case *ast.MatchQueryStatement:
		for _, match := range typed.MatchClauses {
			if err := validatePatternBindings(match.Pattern, bound, seg); err != nil {
				return err
			}
			if match.Where != nil {
				if err := validateWherePatternBindings(match.Where.Raw, bound, seg); err != nil {
					return err
				}
			}
		}
	case *ast.QueryStatement:
		for _, part := range typed.Parts {
			for _, clause := range part.Clauses {
				if clause.Kind != ast.ClauseKindMatch && clause.Kind != ast.ClauseKindOptionalMatch {
					continue
				}
				pattern, ok := extractMatchPattern(clause.Raw)
				if !ok {
					continue
				}
				if err := validatePatternBindings(pattern, bound, seg); err != nil {
					return err
				}
				if where, ok := extractMatchWhere(clause.Raw); ok {
					if err := validateWherePatternBindings(where, bound, seg); err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

func validatePatternBindings(pattern string, bound map[string]patternVarRole, seg statementSegment) error {
	bindings := scanPatternBindings(pattern)
	for _, b := range bindings {
		if b.name == "" {
			continue
		}
		if prev, ok := bound[b.name]; ok {
			if prev == b.role {
				continue
			}
			if prev == patternRolePath || b.role == patternRolePath {
				return &ParseError{Kind: ParseErrorUnsupported, Message: "variable already bound", Statement: seg.index}
			}
			return &ParseError{Kind: ParseErrorUnsupported, Message: "variable type conflict", Statement: seg.index}
		}
		bound[b.name] = b.role
	}
	return nil
}

func extractMatchPattern(raw string) (string, bool) {
	text := strings.TrimSpace(raw)
	upper := strings.ToUpper(text)
	if strings.HasPrefix(upper, "OPTIONALMATCH") {
		text = strings.TrimSpace(text[len("OPTIONALMATCH"):])
	} else if strings.HasPrefix(upper, "MATCH") {
		text = strings.TrimSpace(text[len("MATCH"):])
	} else {
		return "", false
	}

	if idx := indexTopLevelKeyword(text, "WHERE"); idx >= 0 {
		text = strings.TrimSpace(text[:idx])
	}
	return text, text != ""
}

func extractMatchWhere(raw string) (string, bool) {
	text := strings.TrimSpace(raw)
	upper := strings.ToUpper(text)
	if strings.HasPrefix(upper, "OPTIONALMATCH") {
		text = strings.TrimSpace(text[len("OPTIONALMATCH"):])
	} else if strings.HasPrefix(upper, "MATCH") {
		text = strings.TrimSpace(text[len("MATCH"):])
	} else {
		return "", false
	}
	idx := indexTopLevelKeyword(text, "WHERE")
	if idx < 0 {
		return "", false
	}
	where := strings.TrimSpace(text[idx+len("WHERE"):])
	return where, where != ""
}

func validateWherePatternBindings(whereRaw string, bound map[string]patternVarRole, seg statementSegment) error {
	for _, b := range scanPatternBindings(whereRaw) {
		if b.name == "" {
			continue
		}
		if _, ok := bound[b.name]; ok {
			continue
		}
		return &ParseError{Kind: ParseErrorUnsupported, Message: "undefined variable", Statement: seg.index}
	}
	return nil
}

func indexTopLevelKeyword(raw, keyword string) int {
	upper := strings.ToUpper(raw)
	keyword = strings.ToUpper(keyword)
	depthParen, depthBracket, depthBrace := 0, 0, 0
	inString := false

	for i := 0; i <= len(raw)-len(keyword); i++ {
		ch := raw[i]
		if ch == '\'' && (i == 0 || raw[i-1] != '\\') {
			inString = !inString
			continue
		}
		if inString {
			continue
		}

		switch ch {
		case '(':
			depthParen++
		case ')':
			if depthParen > 0 {
				depthParen--
			}
		case '[':
			depthBracket++
		case ']':
			if depthBracket > 0 {
				depthBracket--
			}
		case '{':
			depthBrace++
		case '}':
			if depthBrace > 0 {
				depthBrace--
			}
		}

		if depthParen != 0 || depthBracket != 0 || depthBrace != 0 {
			continue
		}
		if strings.HasPrefix(upper[i:], keyword) {
			return i
		}
	}

	return -1
}

func scanPatternBindings(pattern string) []patternBinding {
	bindings := []patternBinding{}

	depthParen, depthBracket, depthBrace := 0, 0, 0
	inString := false

	for i := 0; i < len(pattern); i++ {
		ch := pattern[i]
		if ch == '\'' && (i == 0 || pattern[i-1] != '\\') {
			inString = !inString
			continue
		}
		if inString {
			continue
		}

		if depthParen == 0 && depthBracket == 0 && depthBrace == 0 {
			if name, next, ok := scanTopLevelAssignment(pattern, i); ok {
				bindings = append(bindings, patternBinding{name: name, role: patternRolePath})
				i = next - 1
				continue
			}
		}

		switch ch {
		case '(':
			depthParen++
			if name, next, ok := scanNodeBinding(pattern, i+1); ok {
				bindings = append(bindings, patternBinding{name: name, role: patternRoleNode})
				i = next - 1
			}
		case ')':
			if depthParen > 0 {
				depthParen--
			}
		case '[':
			depthBracket++
			if name, next, ok := scanRelBinding(pattern, i+1); ok {
				bindings = append(bindings, patternBinding{name: name, role: patternRoleRel})
				i = next - 1
			}
		case ']':
			if depthBracket > 0 {
				depthBracket--
			}
		case '{':
			depthBrace++
		case '}':
			if depthBrace > 0 {
				depthBrace--
			}
		}
	}

	return bindings
}

func scanTopLevelAssignment(raw string, start int) (string, int, bool) {
	name, idx, ok := readIdentifier(raw, start)
	if !ok {
		return "", start, false
	}
	idx = skipSpaces(raw, idx)
	if idx >= len(raw) || raw[idx] != '=' {
		return "", start, false
	}
	return name, idx + 1, true
}

func scanNodeBinding(raw string, start int) (string, int, bool) {
	idx := skipSpaces(raw, start)
	if idx >= len(raw) {
		return "", start, false
	}
	if raw[idx] == ')' || raw[idx] == ':' || raw[idx] == '{' {
		return "", start, false
	}
	name, next, ok := readIdentifier(raw, idx)
	if !ok {
		return "", start, false
	}
	next = skipSpaces(raw, next)
	if next >= len(raw) {
		return "", start, false
	}
	if raw[next] != ')' && raw[next] != ':' && raw[next] != '{' {
		return "", start, false
	}
	return name, next, true
}

func scanRelBinding(raw string, start int) (string, int, bool) {
	idx := skipSpaces(raw, start)
	if idx >= len(raw) {
		return "", start, false
	}
	if raw[idx] == ']' || raw[idx] == ':' || raw[idx] == '*' || raw[idx] == '{' {
		return "", start, false
	}
	name, next, ok := readIdentifier(raw, idx)
	if !ok {
		return "", start, false
	}
	next = skipSpaces(raw, next)
	if next >= len(raw) {
		return "", start, false
	}
	if raw[next] != ']' && raw[next] != ':' && raw[next] != '*' && raw[next] != '{' {
		return "", start, false
	}
	return name, next, true
}

func readIdentifier(raw string, start int) (string, int, bool) {
	if start >= len(raw) {
		return "", start, false
	}
	if raw[start] == '`' {
		for i := start + 1; i < len(raw); i++ {
			if raw[i] == '`' {
				return raw[start+1 : i], i + 1, true
			}
		}
		return "", start, false
	}
	if !isIdentifierStart(raw[start]) {
		return "", start, false
	}
	i := start + 1
	for i < len(raw) && isIdentifierPart(raw[i]) {
		i++
	}
	return raw[start:i], i, true
}

func skipSpaces(raw string, start int) int {
	i := start
	for i < len(raw) {
		if raw[i] != ' ' && raw[i] != '\t' && raw[i] != '\n' && raw[i] != '\r' {
			break
		}
		i++
	}
	return i
}

func isIdentifierStart(ch byte) bool {
	return ch == '_' || (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z')
}

func isIdentifierPart(ch byte) bool {
	return isIdentifierStart(ch) || (ch >= '0' && ch <= '9')
}
