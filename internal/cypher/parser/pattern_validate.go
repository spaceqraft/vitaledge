package parser

import (
	"strings"

	"github.com/paegun/vitaledge/internal/cypher/ast"
)

type patternVarRole string

const (
	patternRoleValue patternVarRole = "value"
	patternRoleNode  patternVarRole = "node"
	patternRoleRel   patternVarRole = "relationship"
	patternRolePath  patternVarRole = "path"
)

type patternBinding struct {
	name        string
	role        patternVarRole
	constrained bool
}

func validatePatternVariableScoping(stmt ast.Statement, seg statementSegment) error {
	bound := map[string]patternVarRole{}

	switch typed := stmt.(type) {
	case *ast.MatchQueryStatement:
		for _, match := range typed.MatchClauses {
			clauseIntroduced := map[string]struct{}{}
			if err := validatePatternBindings(match.Pattern, bound, seg, ast.ClauseKindMatch, clauseIntroduced); err != nil {
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
				switch clause.Kind {
				case ast.ClauseKindWith:
					recordWithBindings(clause.Raw, bound)
					continue
				case ast.ClauseKindUnwind:
					recordUnwindBinding(clause.Raw, bound)
					continue
				case ast.ClauseKindMatch, ast.ClauseKindOptionalMatch, ast.ClauseKindCreate, ast.ClauseKindMerge:
					// handled below
				default:
					continue
				}

				patternRaw, ok := extractClausePattern(clause.Raw, clause.Kind)
				if !ok {
					continue
				}

				clauseIntroduced := map[string]struct{}{}
				for _, pattern := range splitTopLevelComma(patternRaw) {
					if err := validatePatternBindings(pattern, bound, seg, clause.Kind, clauseIntroduced); err != nil {
						return err
					}
				}

				if (clause.Kind == ast.ClauseKindMatch || clause.Kind == ast.ClauseKindOptionalMatch) && clause.Kind != ast.ClauseKindCreate {
					where, hasWhere := extractMatchWhere(clause.Raw)
					if !hasWhere {
						continue
					}
					if err := validateWherePatternBindings(where, bound, seg); err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

func validatePatternBindings(pattern string, bound map[string]patternVarRole, seg statementSegment, clauseKind ast.ClauseKind, clauseIntroduced map[string]struct{}) error {
	bindings := scanPatternBindings(pattern)
	patternHasRelationship := strings.Contains(pattern, "-[") || strings.Contains(pattern, "--")
	for _, b := range bindings {
		if b.name == "" {
			continue
		}
		if prev, ok := bound[b.name]; ok {
			if clauseKind == ast.ClauseKindOptionalMatch && prev == patternRoleValue && (b.role == patternRoleNode || b.role == patternRoleRel || b.role == patternRolePath) {
				continue
			}
			if prev == patternRoleValue && b.role == patternRoleRel && isVariableLengthRelationshipBinding(pattern, b.name) {
				// MATCH (a)-[rs*]->(b) can legally reuse a pre-bound list value.
				continue
			}
			if prev == b.role {
				_, seenInClause := clauseIntroduced[b.name]
				if shouldRejectSameRoleRebinding(clauseKind, b, seenInClause, patternHasRelationship) {
					return &ParseError{Kind: ParseErrorUnsupported, Message: "variable already bound", Statement: seg.index}
				}
				continue
			}
			if prev == patternRolePath || b.role == patternRolePath {
				return &ParseError{Kind: ParseErrorUnsupported, Message: "variable already bound", Statement: seg.index}
			}
			if prev == patternRoleValue || b.role == patternRoleValue {
				return &ParseError{Kind: ParseErrorUnsupported, Message: "variable type conflict", Statement: seg.index}
			}
			return &ParseError{Kind: ParseErrorUnsupported, Message: "variable type conflict", Statement: seg.index}
		}
		bound[b.name] = b.role
		if clauseIntroduced != nil {
			clauseIntroduced[b.name] = struct{}{}
		}
	}
	return nil
}

func shouldRejectSameRoleRebinding(clauseKind ast.ClauseKind, b patternBinding, seenInClause bool, patternHasRelationship bool) bool {
	switch clauseKind {
	case ast.ClauseKindCreate:
		if b.role == patternRoleNode {
			if b.constrained {
				return true
			}
			if !patternHasRelationship {
				return true
			}
			return false
		}
		return true
	case ast.ClauseKindMerge:
		if b.role == patternRoleRel || b.role == patternRolePath {
			return true
		}
		if b.role == patternRoleNode {
			if b.constrained {
				return true
			}
			if !patternHasRelationship {
				return true
			}
		}
		return false
	default:
		_ = seenInClause
		_ = patternHasRelationship
		return false
	}
}

func extractClausePattern(raw string, kind ast.ClauseKind) (string, bool) {
	text := strings.TrimSpace(raw)
	upper := strings.ToUpper(text)

	switch kind {
	case ast.ClauseKindMatch, ast.ClauseKindOptionalMatch:
		return extractMatchPattern(raw)
	case ast.ClauseKindCreate:
		if strings.HasPrefix(upper, "CREATE") {
			text = strings.TrimSpace(text[len("CREATE"):])
			return text, text != ""
		}
	case ast.ClauseKindMerge:
		if strings.HasPrefix(upper, "MERGE") {
			text = strings.TrimSpace(text[len("MERGE"):])
			if idx := indexTopLevelKeyword(text, "ONCREATE"); idx >= 0 {
				text = strings.TrimSpace(text[:idx])
			}
			if idx := indexTopLevelKeyword(text, "ONMATCH"); idx >= 0 {
				text = strings.TrimSpace(text[:idx])
			}
			return text, text != ""
		}
	}

	return "", false
}

func splitTopLevelComma(raw string) []string {
	parts := []string{}
	start := 0
	depthParen, depthBracket, depthBrace := 0, 0, 0
	inString := false

	for i := 0; i < len(raw); i++ {
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
		case ',':
			if depthParen == 0 && depthBracket == 0 && depthBrace == 0 {
				part := strings.TrimSpace(raw[start:i])
				if part != "" {
					parts = append(parts, part)
				}
				start = i + 1
			}
		}
	}

	last := strings.TrimSpace(raw[start:])
	if last != "" {
		parts = append(parts, last)
	}

	return parts
}

func recordWithBindings(raw string, bound map[string]patternVarRole) {
	text := strings.TrimSpace(raw)
	upper := strings.ToUpper(text)
	if !strings.HasPrefix(upper, "WITH") {
		return
	}
	text = strings.TrimSpace(text[len("WITH"):])

	if idx := indexTopLevelKeyword(text, "WHERE"); idx >= 0 {
		text = strings.TrimSpace(text[:idx])
	}
	if idx := indexTopLevelKeyword(text, "ORDERBY"); idx >= 0 {
		text = strings.TrimSpace(text[:idx])
	}
	if idx := indexTopLevelKeyword(text, "SKIP"); idx >= 0 {
		text = strings.TrimSpace(text[:idx])
	}
	if idx := indexTopLevelKeyword(text, "LIMIT"); idx >= 0 {
		text = strings.TrimSpace(text[:idx])
	}

	for _, item := range splitTopLevelComma(text) {
		alias, expr, ok := parseProjectionAlias(item)
		if !ok || alias == "" {
			continue
		}
		bound[alias] = roleForProjectionExpr(expr, bound)
	}
}

func recordUnwindBinding(raw string, bound map[string]patternVarRole) {
	text := strings.TrimSpace(raw)
	upper := strings.ToUpper(text)
	if !strings.HasPrefix(upper, "UNWIND") {
		return
	}
	text = strings.TrimSpace(text[len("UNWIND"):])
	idx := indexTopLevelKeyword(text, "AS")
	if idx < 0 {
		return
	}
	aliasRaw := strings.TrimSpace(text[idx+len("AS"):])
	alias, _, ok := readIdentifier(aliasRaw, 0)
	if !ok || alias == "" {
		return
	}
	bound[alias] = patternRoleValue
}

func parseProjectionAlias(item string) (alias string, expr string, ok bool) {
	idx := indexTopLevelKeyword(item, "AS")
	if idx >= 0 {
		expr = strings.TrimSpace(item[:idx])
		aliasRaw := strings.TrimSpace(item[idx+len("AS"):])
		if aliasRaw == "" {
			return "", "", false
		}
		name, _, nameOK := readIdentifier(aliasRaw, 0)
		if !nameOK || name == "" {
			return "", "", false
		}
		return name, expr, true
	}

	name, _, nameOK := readIdentifier(strings.TrimSpace(item), 0)
	if !nameOK || name == "" {
		return "", "", false
	}
	return name, name, true
}

func roleForProjectionExpr(expr string, bound map[string]patternVarRole) patternVarRole {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return patternRoleValue
	}
	name, next, ok := readIdentifier(expr, 0)
	if ok {
		next = skipSpaces(expr, next)
		if next == len(expr) {
			if prev, exists := bound[name]; exists {
				return prev
			}
		}
	}
	return patternRoleValue
}

func extractMatchPattern(raw string) (string, bool) {
	text := strings.TrimSpace(raw)
	upper := strings.ToUpper(text)
	if strings.HasPrefix(upper, "OPTIONAL MATCH") {
		text = strings.TrimSpace(text[len("OPTIONAL MATCH"):])
	} else if strings.HasPrefix(upper, "OPTIONALMATCH") {
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
	if strings.HasPrefix(upper, "OPTIONAL MATCH") {
		text = strings.TrimSpace(text[len("OPTIONAL MATCH"):])
	} else if strings.HasPrefix(upper, "OPTIONALMATCH") {
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
	// Variables introduced inside EXISTS { ... } are scoped to the subquery and
	// should not be treated as unbounded pattern variables in the outer WHERE.
	if strings.Contains(strings.ToUpper(strings.ReplaceAll(whereRaw, " ", "")), "EXISTS{") {
		return nil
	}
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

func isVariableLengthRelationshipBinding(pattern, name string) bool {
	if pattern == "" || name == "" {
		return false
	}
	needle := "[" + name
	searchFrom := 0
	for {
		idx := strings.Index(pattern[searchFrom:], needle)
		if idx < 0 {
			return false
		}
		idx += searchFrom
		after := idx + len(needle)
		if after < len(pattern) && isIdentifierPart(pattern[after]) {
			searchFrom = idx + 1
			continue
		}
		close := strings.IndexByte(pattern[after:], ']')
		if close < 0 {
			return false
		}
		segment := pattern[after : after+close]
		if strings.Contains(segment, "*") {
			return true
		}
		searchFrom = after + close + 1
	}
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
				bindings = append(bindings, patternBinding{name: name, role: patternRolePath, constrained: true})
				i = next - 1
				continue
			}
		}

		switch ch {
		case '(':
			depthParen++
			if name, constrained, next, ok := scanNodeBinding(pattern, i+1); ok {
				bindings = append(bindings, patternBinding{name: name, role: patternRoleNode, constrained: constrained})
				i = next - 1
			}
		case ')':
			if depthParen > 0 {
				depthParen--
			}
		case '[':
			depthBracket++
			if name, constrained, next, ok := scanRelBinding(pattern, i+1); ok {
				bindings = append(bindings, patternBinding{name: name, role: patternRoleRel, constrained: constrained})
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
	next := skipSpaces(raw, idx+1)
	if next >= len(raw) || raw[next] != '(' {
		return "", start, false
	}
	return name, idx + 1, true
}

func scanNodeBinding(raw string, start int) (string, bool, int, bool) {
	idx := skipSpaces(raw, start)
	if idx >= len(raw) {
		return "", false, start, false
	}
	if raw[idx] == ')' || raw[idx] == ':' || raw[idx] == '{' {
		return "", false, start, false
	}
	name, next, ok := readIdentifier(raw, idx)
	if !ok {
		return "", false, start, false
	}
	next = skipSpaces(raw, next)
	if next >= len(raw) {
		return "", false, start, false
	}
	if raw[next] != ')' && raw[next] != ':' && raw[next] != '{' {
		return "", false, start, false
	}
	constrained := raw[next] == ':' || raw[next] == '{'
	return name, constrained, next, true
}

func scanRelBinding(raw string, start int) (string, bool, int, bool) {
	idx := skipSpaces(raw, start)
	if idx >= len(raw) {
		return "", false, start, false
	}
	if raw[idx] == ']' || raw[idx] == ':' || raw[idx] == '*' || raw[idx] == '{' {
		return "", false, start, false
	}
	name, next, ok := readIdentifier(raw, idx)
	if !ok {
		return "", false, start, false
	}
	next = skipSpaces(raw, next)
	if next >= len(raw) {
		return "", false, start, false
	}
	if raw[next] != ']' && raw[next] != ':' && raw[next] != '*' && raw[next] != '{' {
		return "", false, start, false
	}
	constrained := raw[next] == ':' || raw[next] == '*' || raw[next] == '{'
	return name, constrained, next, true
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
