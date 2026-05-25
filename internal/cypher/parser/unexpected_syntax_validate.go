package parser

import (
	"fmt"
	"strings"

	"github.com/paegun/vitaledge/internal/cypher/ast"
)

var supportedExpressionFunctions = map[string]struct{}{
	"all":                       {},
	"any":                       {},
	"avg":                       {},
	"coalesce":                  {},
	"collect":                   {},
	"count":                     {},
	"date":                      {},
	"date.realtime":             {},
	"date.statement":            {},
	"date.transaction":          {},
	"date.truncate":             {},
	"datetime":                  {},
	"datetime.fromepoch":        {},
	"datetime.realtime":         {},
	"datetime.statement":        {},
	"datetime.transaction":      {},
	"datetime.truncate":         {},
	"duration":                  {},
	"duration.between":          {},
	"duration.indays":           {},
	"duration.inmonths":         {},
	"duration.inseconds":        {},
	"labels":                    {},
	"localdatetime":             {},
	"localdatetime.realtime":    {},
	"localdatetime.statement":   {},
	"localdatetime.transaction": {},
	"localdatetime.truncate":    {},
	"localtime":                 {},
	"localtime.realtime":        {},
	"localtime.statement":       {},
	"localtime.transaction":     {},
	"localtime.truncate":        {},
	"max":                       {},
	"min":                       {},
	"none":                      {},
	"properties":                {},
	"rand":                      {},
	"range":                     {},
	"reverse":                   {},
	"single":                    {},
	"size":                      {},
	"split":                     {},
	"substring":                 {},
	"sum":                       {},
	"time":                      {},
	"time.realtime":             {},
	"time.statement":            {},
	"time.transaction":          {},
	"time.truncate":             {},
	"toboolean":                 {},
	"tofloat":                   {},
	"tointeger":                 {},
	"tostring":                  {},
	"type":                      {},
}

var expressionOperatorKeywords = map[string]struct{}{
	"and": {},
	"in":  {},
	"is":  {},
	"not": {},
	"or":  {},
	"xor": {},
}

func validateUnexpectedSyntax(stmt ast.Statement, seg statementSegment) error {
	switch typed := stmt.(type) {
	case *ast.MatchQueryStatement:
		for _, item := range typed.Return.Items {
			if containsForbiddenPatternExpression(item.Expression.Raw) {
				return &ParseError{Kind: ParseErrorUnsupported, Message: "unexpected syntax", Statement: seg.index}
			}
			if name, ok := firstUnknownFunctionCall(item.Expression.Raw); ok {
				return &ParseError{Kind: ParseErrorUnsupported, Message: fmt.Sprintf("unknown function %q", name), Statement: seg.index}
			}
		}
	case *ast.QueryStatement:
		for _, part := range typed.Parts {
			for _, clause := range part.Clauses {
				switch clause.Kind {
				case ast.ClauseKindReturn, ast.ClauseKindWith:
					expressions := extractProjectionExpressions(clause.Raw, clause.Kind)
					for _, expr := range expressions {
						if containsForbiddenPatternExpression(expr) {
							return &ParseError{Kind: ParseErrorUnsupported, Message: "unexpected syntax", Statement: seg.index}
						}
						if name, ok := firstUnknownFunctionCall(expr); ok {
							return &ParseError{Kind: ParseErrorUnsupported, Message: fmt.Sprintf("unknown function %q", name), Statement: seg.index}
						}
					}
				case ast.ClauseKindSet:
					expressions := extractSetValueExpressions(clause.Raw)
					for _, expr := range expressions {
						if containsForbiddenPatternExpression(expr) {
							return &ParseError{Kind: ParseErrorUnsupported, Message: "unexpected syntax", Statement: seg.index}
						}
						if name, ok := firstUnknownFunctionCall(expr); ok {
							return &ParseError{Kind: ParseErrorUnsupported, Message: fmt.Sprintf("unknown function %q", name), Statement: seg.index}
						}
					}
				}
			}
		}
	}

	return nil
}

func extractProjectionExpressions(raw string, kind ast.ClauseKind) []string {
	text := strings.TrimSpace(raw)
	upper := strings.ToUpper(text)

	switch kind {
	case ast.ClauseKindReturn:
		if strings.HasPrefix(upper, "RETURN") {
			text = strings.TrimSpace(text[len("RETURN"):])
		}
		if strings.HasPrefix(strings.ToUpper(text), "DISTINCT") {
			text = strings.TrimSpace(text[len("DISTINCT"):])
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
	case ast.ClauseKindWith:
		if strings.HasPrefix(upper, "WITH") {
			text = strings.TrimSpace(text[len("WITH"):])
		}
		if strings.HasPrefix(strings.ToUpper(text), "DISTINCT") {
			text = strings.TrimSpace(text[len("DISTINCT"):])
		}
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
	default:
		return nil
	}

	if text == "" {
		return nil
	}

	expressions := make([]string, 0)
	for _, item := range splitTopLevelComma(text) {
		entry := strings.TrimSpace(item)
		if entry == "" || entry == "*" {
			continue
		}
		_, expr, ok := parseProjectionAlias(entry)
		if ok {
			entry = strings.TrimSpace(expr)
		}
		expressions = append(expressions, entry)
	}

	return expressions
}

func extractSetValueExpressions(raw string) []string {
	text := strings.TrimSpace(raw)
	upper := strings.ToUpper(text)
	if strings.HasPrefix(upper, "SET") {
		text = strings.TrimSpace(text[len("SET"):])
	}
	if text == "" {
		return nil
	}

	expressions := make([]string, 0)
	for _, item := range splitTopLevelComma(text) {
		idx := indexTopLevelEquals(item)
		if idx < 0 {
			continue
		}
		rhs := strings.TrimSpace(item[idx+1:])
		if rhs != "" {
			expressions = append(expressions, rhs)
		}
	}
	return expressions
}

func indexTopLevelEquals(raw string) int {
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
		case '=':
			if depthParen == 0 && depthBracket == 0 && depthBrace == 0 {
				return i
			}
		}
	}

	return -1
}

func containsForbiddenPatternExpression(expr string) bool {
	inSingle := false
	inDouble := false
	bracketDepth := 0

	for i := 0; i < len(expr); i++ {
		ch := expr[i]
		if inSingle {
			if ch == '\'' && (i == 0 || expr[i-1] != '\\') {
				inSingle = false
			}
			continue
		}
		if inDouble {
			if ch == '"' && (i == 0 || expr[i-1] != '\\') {
				inDouble = false
			}
			continue
		}

		switch ch {
		case '\'':
			inSingle = true
			continue
		case '"':
			inDouble = true
			continue
		case '[':
			bracketDepth++
			continue
		case ']':
			if bracketDepth > 0 {
				bracketDepth--
			}
			continue
		}

		if bracketDepth > 0 {
			continue
		}

		if i+2 < len(expr) {
			if ch == ')' && expr[i+1] == '-' && expr[i+2] == '-' {
				return true
			}
			if ch == ')' && expr[i+1] == '-' && expr[i+2] == '[' {
				return true
			}
			if ch == ')' && expr[i+1] == '<' && expr[i+2] == '-' {
				return true
			}
			if ch == '-' && expr[i+1] == '>' && i+2 < len(expr) && expr[i+2] == '(' {
				return true
			}
			if ch == '<' && expr[i+1] == '-' && expr[i+2] == '(' {
				return true
			}
		}
	}

	return false
}

func firstUnknownFunctionCall(expr string) (string, bool) {
	inSingle := false
	inDouble := false

	for i := 0; i < len(expr); i++ {
		ch := expr[i]
		if inSingle {
			if ch == '\'' && (i == 0 || expr[i-1] != '\\') {
				inSingle = false
			}
			continue
		}
		if inDouble {
			if ch == '"' && (i == 0 || expr[i-1] != '\\') {
				inDouble = false
			}
			continue
		}

		switch ch {
		case '\'':
			inSingle = true
			continue
		case '"':
			inDouble = true
			continue
		}

		if !isFunctionIdentStart(ch) {
			continue
		}
		if i > 0 && isFunctionIdentPart(expr[i-1]) {
			continue
		}

		j := i + 1
		for j < len(expr) && isFunctionIdentPart(expr[j]) {
			j++
		}
		if j <= i+1 {
			continue
		}

		name := expr[i:j]
		if strings.HasSuffix(name, ".") || strings.Contains(name, "..") {
			i = j - 1
			continue
		}

		k := j
		for k < len(expr) && (expr[k] == ' ' || expr[k] == '\t' || expr[k] == '\n' || expr[k] == '\r') {
			k++
		}
		if k >= len(expr) || expr[k] != '(' {
			i = j - 1
			continue
		}

		if _, ok := supportedExpressionFunctions[strings.ToLower(name)]; !ok {
			if _, keyword := expressionOperatorKeywords[strings.ToLower(name)]; keyword {
				i = j - 1
				continue
			}
			return name, true
		}

		i = j - 1
	}

	return "", false
}

func isFunctionIdentStart(ch byte) bool {
	return (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || ch == '_'
}

func isFunctionIdentPart(ch byte) bool {
	return isFunctionIdentStart(ch) || (ch >= '0' && ch <= '9') || ch == '.'
}
