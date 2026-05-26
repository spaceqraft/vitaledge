package parser

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/paegun/vitaledge/internal/cypher/ast"
)

var supportedExpressionFunctions = map[string]struct{}{
	"all":                       {},
	"any":                       {},
	"avg":                       {},
	"abs":                       {},
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
	"datetime.fromepochmillis":  {},
	"datetime.realtime":         {},
	"datetime.statement":        {},
	"datetime.transaction":      {},
	"datetime.truncate":         {},
	"duration":                  {},
	"duration.between":          {},
	"duration.indays":           {},
	"duration.inmonths":         {},
	"duration.inseconds":        {},
	"endnode":                   {},
	"head":                      {},
	"keys":                      {},
	"last":                      {},
	"labels":                    {},
	"length":                    {},
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
	"nodes":                     {},
	"percentilecont":            {},
	"percentiledisc":            {},
	"properties":                {},
	"rand":                      {},
	"range":                     {},
	"relationships":             {},
	"reverse":                   {},
	"sign":                      {},
	"single":                    {},
	"size":                      {},
	"startnode":                 {},
	"split":                     {},
	"substring":                 {},
	"sum":                       {},
	"tail":                      {},
	"time":                      {},
	"time.realtime":             {},
	"time.statement":            {},
	"time.transaction":          {},
	"time.truncate":             {},
	"tolower":                   {},
	"toboolean":                 {},
	"tofloat":                   {},
	"tointeger":                 {},
	"tostring":                  {},
	"toupper":                   {},
	"type":                      {},
}

var expressionOperatorKeywords = map[string]struct{}{
	"and":  {},
	"in":   {},
	"is":   {},
	"not":  {},
	"or":   {},
	"by":   {},
	"with": {},
	"xor":  {},
}

func validateUnexpectedSyntax(stmt ast.Statement, seg statementSegment) error {
	switch typed := stmt.(type) {
	case *ast.MatchQueryStatement:
		bound := map[string]patternVarRole{}
		for _, match := range typed.MatchClauses {
			if err := validatePatternParameterUsage(match.Pattern, seg); err != nil {
				return err
			}
			for _, binding := range scanPatternBindings(match.Pattern) {
				if binding.name == "" {
					continue
				}
				bound[binding.name] = binding.role
			}
		}
		for _, item := range typed.Return.Items {
			if containsForbiddenPatternExpression(item.Expression.Raw) {
				return &ParseError{Kind: ParseErrorUnsupported, Message: "unexpected syntax", Statement: seg.index}
			}
			if hasInvalidInLiteralRHS(item.Expression.Raw) {
				return &ParseError{Kind: ParseErrorUnsupported, Message: "invalid argument type", Statement: seg.index}
			}
			if hasInvalidAggregationInListComprehension(item.Expression.Raw) {
				return &ParseError{Kind: ParseErrorUnsupported, Message: "InvalidAggregation", Statement: seg.index}
			}
			if name, ok := firstUnknownFunctionCall(item.Expression.Raw); ok {
				return &ParseError{Kind: ParseErrorUnsupported, Message: fmt.Sprintf("unknown function %q", name), Statement: seg.index}
			}
			if literal, ok := firstOverflowingHexOrOctalLiteral(item.Expression.Raw); ok {
				return &ParseError{Kind: ParseErrorUnsupported, Message: fmt.Sprintf("integer overflow in literal %q", literal), Statement: seg.index}
			}
			if literal, ok := firstOverflowingFloatLiteral(item.Expression.Raw); ok {
				return &ParseError{Kind: ParseErrorUnsupported, Message: fmt.Sprintf("floating point overflow in literal %q", literal), Statement: seg.index}
			}
			if isInvalidLengthArgumentExpression(item.Expression.Raw, bound) || isInvalidSizeArgumentExpression(item.Expression.Raw, bound) {
				return &ParseError{Kind: ParseErrorUnsupported, Message: "invalid argument type", Statement: seg.index}
			}
		}
		if typed.Return.IncludeAll && len(bound) == 0 {
			return &ParseError{Kind: ParseErrorUnsupported, Message: "no variables in scope", Statement: seg.index}
		}
		if err := validateSkipLimitExpressionForClause(typed.Return.Skip, true, seg); err != nil {
			return err
		}
		if err := validateSkipLimitExpressionForClause(typed.Return.Limit, false, seg); err != nil {
			return err
		}
		if err := validateProjectionClauseNames(typed.Return.Items, typed.Return.IncludeAll, bound, seg); err != nil {
			return err
		}
	case *ast.QueryStatement:
		bound := map[string]patternVarRole{}
		for _, part := range typed.Parts {
			for _, clause := range part.Clauses {
				switch clause.Kind {
				case ast.ClauseKindReturn, ast.ClauseKindWith:
					if err := validateSkipLimitExpressionsInRawClause(clause.Raw, clause.Kind, seg); err != nil {
						return err
					}
					if hasTopLevelStarProjection(clause.Raw, clause.Kind) && len(bound) == 0 {
						return &ParseError{Kind: ParseErrorUnsupported, Message: "no variables in scope", Statement: seg.index}
					}
					if err := validateProjectionClauseNamesFromRaw(clause.Raw, clause.Kind, bound, seg); err != nil {
						return err
					}
					expressions := extractProjectionExpressions(clause.Raw, clause.Kind)
					for _, expr := range expressions {
						if containsForbiddenPatternExpression(expr) {
							return &ParseError{Kind: ParseErrorUnsupported, Message: "unexpected syntax", Statement: seg.index}
						}
						if hasInvalidInLiteralRHS(expr) {
							return &ParseError{Kind: ParseErrorUnsupported, Message: "invalid argument type", Statement: seg.index}
						}
						if hasInvalidAggregationInListComprehension(expr) {
							return &ParseError{Kind: ParseErrorUnsupported, Message: "InvalidAggregation", Statement: seg.index}
						}
						if name, ok := firstUnknownFunctionCall(expr); ok {
							return &ParseError{Kind: ParseErrorUnsupported, Message: fmt.Sprintf("unknown function %q", name), Statement: seg.index}
						}
						if literal, ok := firstOverflowingHexOrOctalLiteral(expr); ok {
							return &ParseError{Kind: ParseErrorUnsupported, Message: fmt.Sprintf("integer overflow in literal %q", literal), Statement: seg.index}
						}
						if literal, ok := firstOverflowingFloatLiteral(expr); ok {
							return &ParseError{Kind: ParseErrorUnsupported, Message: fmt.Sprintf("floating point overflow in literal %q", literal), Statement: seg.index}
						}
						if isInvalidLengthArgumentExpression(expr, bound) || isInvalidSizeArgumentExpression(expr, bound) {
							return &ParseError{Kind: ParseErrorUnsupported, Message: "invalid argument type", Statement: seg.index}
						}
					}
					if clause.Kind == ast.ClauseKindWith {
						recordWithBindings(clause.Raw, bound)
					}
				case ast.ClauseKindSet:
					expressions := extractSetValueExpressions(clause.Raw)
					for _, expr := range expressions {
						if containsForbiddenPatternExpression(expr) {
							return &ParseError{Kind: ParseErrorUnsupported, Message: "unexpected syntax", Statement: seg.index}
						}
						if hasInvalidInLiteralRHS(expr) {
							return &ParseError{Kind: ParseErrorUnsupported, Message: "invalid argument type", Statement: seg.index}
						}
						if hasInvalidAggregationInListComprehension(expr) {
							return &ParseError{Kind: ParseErrorUnsupported, Message: "InvalidAggregation", Statement: seg.index}
						}
						if name, ok := firstUnknownFunctionCall(expr); ok {
							return &ParseError{Kind: ParseErrorUnsupported, Message: fmt.Sprintf("unknown function %q", name), Statement: seg.index}
						}
						if literal, ok := firstOverflowingHexOrOctalLiteral(expr); ok {
							return &ParseError{Kind: ParseErrorUnsupported, Message: fmt.Sprintf("integer overflow in literal %q", literal), Statement: seg.index}
						}
						if literal, ok := firstOverflowingFloatLiteral(expr); ok {
							return &ParseError{Kind: ParseErrorUnsupported, Message: fmt.Sprintf("floating point overflow in literal %q", literal), Statement: seg.index}
						}
						if isInvalidLengthArgumentExpression(expr, bound) || isInvalidSizeArgumentExpression(expr, bound) {
							return &ParseError{Kind: ParseErrorUnsupported, Message: "invalid argument type", Statement: seg.index}
						}
					}
				case ast.ClauseKindUnwind:
					recordUnwindBinding(clause.Raw, bound)
				case ast.ClauseKindMatch, ast.ClauseKindOptionalMatch, ast.ClauseKindCreate, ast.ClauseKindMerge:
					patternRaw, ok := extractClausePattern(clause.Raw, clause.Kind)
					if !ok {
						continue
					}
					for _, pattern := range splitTopLevelComma(patternRaw) {
						if err := validatePatternParameterUsage(pattern, seg); err != nil {
							return err
						}
						for _, binding := range scanPatternBindings(pattern) {
							if binding.name == "" {
								continue
							}
							bound[binding.name] = binding.role
						}
					}
				}
			}
		}
	}

	return nil
}

func validateProjectionClauseNames(items []ast.ProjectionItem, includeAll bool, bound map[string]patternVarRole, seg statementSegment) error {
	seen := map[string]struct{}{}
	if includeAll && len(bound) == 0 {
		return &ParseError{Kind: ParseErrorUnsupported, Message: "no variables in scope", Statement: seg.index}
	}
	for _, item := range items {
		name := projectionOutputName(item)
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			return &ParseError{Kind: ParseErrorUnsupported, Message: "column name conflict", Statement: seg.index}
		}
		seen[name] = struct{}{}
	}
	return nil
}

func validateProjectionClauseNamesFromRaw(raw string, kind ast.ClauseKind, bound map[string]patternVarRole, seg statementSegment) error {
	items := splitProjectionItems(raw, kind)
	seen := map[string]struct{}{}
	if hasTopLevelStarProjection(raw, kind) && len(bound) == 0 {
		return &ParseError{Kind: ParseErrorUnsupported, Message: "no variables in scope", Statement: seg.index}
	}
	for _, item := range items {
		if item == "" || item == "*" {
			continue
		}
		name := projectionOutputNameFromRaw(item)
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			return &ParseError{Kind: ParseErrorUnsupported, Message: "column name conflict", Statement: seg.index}
		}
		seen[name] = struct{}{}
	}
	return nil
}

func splitProjectionItems(raw string, kind ast.ClauseKind) []string {
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
	}
	if text == "" {
		return nil
	}
	return splitTopLevelComma(text)
}

func projectionOutputName(item ast.ProjectionItem) string {
	if strings.TrimSpace(item.Alias) != "" {
		return strings.TrimSpace(item.Alias)
	}
	return projectionOutputNameFromRaw(item.Expression.Raw)
}

func projectionOutputNameFromRaw(raw string) string {
	text := strings.TrimSpace(raw)
	if text == "" || text == "*" {
		return ""
	}
	name, next, ok := readIdentifier(text, 0)
	if !ok {
		return ""
	}
	next = skipSpaces(text, next)
	if next != len(text) {
		return ""
	}
	return name
}

func hasTopLevelStarProjection(raw string, kind ast.ClauseKind) bool {
	for _, item := range splitProjectionItems(raw, kind) {
		if strings.TrimSpace(item) == "*" {
			return true
		}
	}
	return false
}

func validatePatternParameterUsage(pattern string, seg statementSegment) error {
	if !containsTopLevelPatternParameter(pattern) {
		return nil
	}
	return &ParseError{Kind: ParseErrorUnsupported, Message: "invalid parameter use", Statement: seg.index}
}

func containsTopLevelPatternParameter(pattern string) bool {
	depthBrace := 0
	inSingle := false
	inDouble := false
	for i := 0; i < len(pattern); i++ {
		ch := pattern[i]
		if ch == '\'' && (i == 0 || pattern[i-1] != '\\') && !inDouble {
			inSingle = !inSingle
			continue
		}
		if ch == '"' && (i == 0 || pattern[i-1] != '\\') && !inSingle {
			inDouble = !inDouble
			continue
		}
		if inSingle || inDouble {
			continue
		}
		switch ch {
		case '{':
			depthBrace++
		case '}':
			if depthBrace > 0 {
				depthBrace--
			}
		case '$':
			if depthBrace == 0 {
				return true
			}
		}
	}
	return false
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

func hasInvalidInLiteralRHS(expr string) bool {
	_, rhs, ok := splitTopLevelInExpressionForValidation(expr)
	if !ok {
		return false
	}
	return isNonListLiteralExpression(rhs)
}

func splitTopLevelInExpressionForValidation(raw string) (string, string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", false
	}
	upper := strings.ToUpper(raw)
	depthParen := 0
	depthBracket := 0
	depthBrace := 0
	inSingle := false
	inDouble := false
	for i := 0; i <= len(upper)-len("IN"); i++ {
		ch := raw[i]
		if ch == '\'' && !inDouble {
			inSingle = !inSingle
			continue
		}
		if ch == '"' && !inSingle {
			inDouble = !inDouble
			continue
		}
		if inSingle || inDouble {
			continue
		}
		switch upper[i] {
		case '(':
			depthParen++
			continue
		case ')':
			if depthParen > 0 {
				depthParen--
			}
			continue
		case '[':
			depthBracket++
			continue
		case ']':
			if depthBracket > 0 {
				depthBracket--
			}
			continue
		case '{':
			depthBrace++
			continue
		case '}':
			if depthBrace > 0 {
				depthBrace--
			}
			continue
		}
		if depthParen != 0 || depthBracket != 0 || depthBrace != 0 || !strings.HasPrefix(upper[i:], "IN") {
			continue
		}
		if raw[i:i+2] != "IN" {
			continue
		}
		if i >= len("CONTA") && i+2 < len(raw) {
			if strings.EqualFold(raw[i-len("CONTA"):i], "CONTA") {
				next := raw[i+2]
				if next == 's' || next == 'S' {
					continue
				}
			}
		}
		left := strings.TrimSpace(raw[:i])
		right := strings.TrimSpace(raw[i+2:])
		if left == "" || right == "" {
			continue
		}
		beforeWhitespace := i > 0 && strings.ContainsAny(string(raw[i-1]), " \t\n\r")
		afterIdx := i + 2
		afterWhitespace := afterIdx < len(raw) && strings.ContainsAny(string(raw[afterIdx]), " \t\n\r")
		if beforeWhitespace || afterWhitespace {
			return left, right, true
		}
		if !strings.ContainsAny(raw, " \t\n\r") {
			if (len(left) == 1 && len(right) == 1) || strings.HasPrefix(left, "$") || strings.HasPrefix(right, "$") || strings.HasPrefix(left, "[") || strings.HasPrefix(right, "[") || strings.HasPrefix(left, "'") || strings.HasPrefix(left, `"`) || (strings.HasPrefix(left, "(") && strings.HasSuffix(left, ")")) || strings.HasPrefix(right, "(") || isSimpleNumericLiteralForValidation(left) {
				return left, right, true
			}
		}
	}
	return "", "", false
}

func isSimpleNumericLiteralForValidation(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}
	if _, err := strconv.Atoi(raw); err == nil {
		return true
	}
	if _, err := strconv.ParseFloat(raw, 64); err == nil {
		return true
	}
	return false
}

func isNonListLiteralExpression(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}
	if strings.HasPrefix(raw, "[") && strings.HasSuffix(raw, "]") {
		return false
	}
	if strings.HasPrefix(raw, "{") && strings.HasSuffix(raw, "}") {
		return true
	}
	if strings.EqualFold(raw, "true") || strings.EqualFold(raw, "false") || strings.EqualFold(raw, "null") {
		return true
	}
	if strings.HasPrefix(raw, "'") && strings.HasSuffix(raw, "'") && len(raw) >= 2 {
		return true
	}
	if strings.HasPrefix(raw, `"`) && strings.HasSuffix(raw, `"`) && len(raw) >= 2 {
		return true
	}
	if _, err := strconv.Atoi(raw); err == nil {
		return true
	}
	if _, err := strconv.ParseFloat(raw, 64); err == nil {
		return true
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

func validateSkipLimitExpressionsInRawClause(raw string, kind ast.ClauseKind, seg statementSegment) error {
	if kind != ast.ClauseKindReturn && kind != ast.ClauseKindWith {
		return nil
	}

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
	case ast.ClauseKindWith:
		if strings.HasPrefix(upper, "WITH") {
			text = strings.TrimSpace(text[len("WITH"):])
		}
		if strings.HasPrefix(strings.ToUpper(text), "DISTINCT") {
			text = strings.TrimSpace(text[len("DISTINCT"):])
		}
	}

	skipIdx := indexTopLevelKeyword(text, "SKIP")
	limitIdx := indexTopLevelKeyword(text, "LIMIT")

	if skipIdx >= 0 {
		end := len(text)
		if limitIdx > skipIdx {
			end = limitIdx
		}
		skipRaw := strings.TrimSpace(text[skipIdx+len("SKIP") : end])
		if err := validateSkipLimitExpressionRaw(skipRaw, true, seg); err != nil {
			return err
		}
	}

	if limitIdx >= 0 {
		limitRaw := strings.TrimSpace(text[limitIdx+len("LIMIT"):])
		if err := validateSkipLimitExpressionRaw(limitRaw, false, seg); err != nil {
			return err
		}
	}

	return nil
}

func validateSkipLimitExpressionForClause(expr *ast.Expression, isSkip bool, seg statementSegment) error {
	if expr == nil {
		return nil
	}
	return validateSkipLimitExpressionRaw(expr.Raw, isSkip, seg)
}

func validateSkipLimitExpressionRaw(raw string, _ bool, seg statementSegment) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return &ParseError{Kind: ParseErrorUnsupported, Message: "InvalidArgumentType", Statement: seg.index}
	}
	if strings.HasPrefix(raw, "$") {
		return nil
	}
	if n, err := strconv.Atoi(raw); err == nil {
		if n < 0 {
			return &ParseError{Kind: ParseErrorUnsupported, Message: "NegativeIntegerArgument", Statement: seg.index}
		}
		return nil
	}
	if _, err := strconv.ParseFloat(raw, 64); err == nil {
		return &ParseError{Kind: ParseErrorUnsupported, Message: "InvalidArgumentType", Statement: seg.index}
	}
	if hasNonConstantIdentifierInSkipLimitExpr(raw) {
		return &ParseError{Kind: ParseErrorUnsupported, Message: "NonConstantExpression", Statement: seg.index}
	}
	return nil
}

func hasNonConstantIdentifierInSkipLimitExpr(raw string) bool {
	inSingle := false
	inDouble := false
	inBacktick := false

	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		if inSingle {
			if ch == '\'' && (i == 0 || raw[i-1] != '\\') {
				inSingle = false
			}
			continue
		}
		if inDouble {
			if ch == '"' && (i == 0 || raw[i-1] != '\\') {
				inDouble = false
			}
			continue
		}
		if inBacktick {
			if ch == '`' {
				inBacktick = false
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
		case '`':
			inBacktick = true
			continue
		}

		if !isIdentifierStart(ch) {
			continue
		}
		if i > 0 && (isIdentifierPart(raw[i-1]) || raw[i-1] == '.') {
			continue
		}

		j := i + 1
		for j < len(raw) && (isIdentifierPart(raw[j]) || raw[j] == '.') {
			j++
		}

		token := raw[i:j]
		k := skipSpaces(raw, j)
		isFuncCall := k < len(raw) && raw[k] == '('
		if isFuncCall {
			i = j - 1
			continue
		}

		if strings.EqualFold(token, "true") || strings.EqualFold(token, "false") || strings.EqualFold(token, "null") {
			i = j - 1
			continue
		}

		return true
	}

	return false
}

func isInvalidLengthArgumentExpression(expr string, bound map[string]patternVarRole) bool {
	name, ok := lengthSimpleIdentifierArg(expr)
	if !ok {
		return false
	}
	role, exists := bound[name]
	if !exists {
		return false
	}
	return role == patternRoleNode || role == patternRoleRel || role == patternRoleValue
}

func isInvalidSizeArgumentExpression(expr string, bound map[string]patternVarRole) bool {
	name, ok := sizeSimpleIdentifierArg(expr)
	if !ok {
		return false
	}
	role, exists := bound[name]
	if !exists {
		return false
	}
	return role == patternRoleNode || role == patternRoleRel || role == patternRolePath
}

func lengthSimpleIdentifierArg(expr string) (string, bool) {
	text := strings.TrimSpace(expr)
	if len(text) < len("length(")+1 || !strings.HasSuffix(text, ")") {
		return "", false
	}
	if !strings.EqualFold(text[:len("length(")], "length(") {
		return "", false
	}
	inner := strings.TrimSpace(text[len("length(") : len(text)-1])
	if inner == "" {
		return "", false
	}
	name, next, ok := readIdentifier(inner, 0)
	if !ok {
		return "", false
	}
	next = skipSpaces(inner, next)
	if next != len(inner) {
		return "", false
	}
	return name, true
}

func sizeSimpleIdentifierArg(expr string) (string, bool) {
	text := strings.TrimSpace(expr)
	if len(text) < len("size(")+1 || !strings.HasSuffix(text, ")") {
		return "", false
	}
	if !strings.EqualFold(text[:len("size(")], "size(") {
		return "", false
	}
	inner := strings.TrimSpace(text[len("size(") : len(text)-1])
	if inner == "" {
		return "", false
	}
	name, next, ok := readIdentifier(inner, 0)
	if !ok {
		return "", false
	}
	next = skipSpaces(inner, next)
	if next != len(inner) {
		return "", false
	}
	return name, true
}

func hasInvalidAggregationInListComprehension(expr string) bool {
	inSingle := false
	inDouble := false
	for i := 0; i < len(expr); i++ {
		ch := expr[i]
		if ch == '\'' && !inDouble && (i == 0 || expr[i-1] != '\\') {
			inSingle = !inSingle
			continue
		}
		if ch == '"' && !inSingle && (i == 0 || expr[i-1] != '\\') {
			inDouble = !inDouble
			continue
		}
		if inSingle || inDouble || ch != '[' {
			continue
		}
		end := findMatchingBracket(expr, i)
		if end < 0 {
			continue
		}
		body := strings.TrimSpace(expr[i+1 : end])
		if body == "" {
			i = end
			continue
		}
		inIdx := findTopLevelInKeywordIndex(body)
		if inIdx < 0 {
			i = end
			continue
		}
		rest := strings.TrimSpace(body[inIdx+2:])
		if rest == "" {
			i = end
			continue
		}
		whereIdx := indexTopLevelKeyword(rest, "WHERE")
		predicate := ""
		projection := ""
		if whereIdx >= 0 {
			tail := strings.TrimSpace(rest[whereIdx+len("WHERE"):])
			if pipeIdx := findTopLevelPipeIndexInListExpr(tail); pipeIdx >= 0 {
				predicate = strings.TrimSpace(tail[:pipeIdx])
				projection = strings.TrimSpace(tail[pipeIdx+1:])
			} else {
				predicate = tail
			}
		} else if pipeIdx := findTopLevelPipeIndexInListExpr(rest); pipeIdx >= 0 {
			projection = strings.TrimSpace(rest[pipeIdx+1:])
		}

		if containsAggregateFunctionCall(predicate) || containsAggregateFunctionCall(projection) {
			return true
		}
		i = end
	}
	return false
}

func findMatchingBracket(raw string, openIdx int) int {
	depth := 0
	inSingle := false
	inDouble := false
	for i := openIdx; i < len(raw); i++ {
		ch := raw[i]
		if ch == '\'' && !inDouble && (i == 0 || raw[i-1] != '\\') {
			inSingle = !inSingle
			continue
		}
		if ch == '"' && !inSingle && (i == 0 || raw[i-1] != '\\') {
			inDouble = !inDouble
			continue
		}
		if inSingle || inDouble {
			continue
		}
		switch ch {
		case '[':
			depth++
		case ']':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

func findTopLevelPipeIndexInListExpr(raw string) int {
	depthParen, depthBracket, depthBrace := 0, 0, 0
	inSingle := false
	inDouble := false
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		if ch == '\'' && !inDouble && (i == 0 || raw[i-1] != '\\') {
			inSingle = !inSingle
			continue
		}
		if ch == '"' && !inSingle && (i == 0 || raw[i-1] != '\\') {
			inDouble = !inDouble
			continue
		}
		if inSingle || inDouble {
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
		case '|':
			if depthParen == 0 && depthBracket == 0 && depthBrace == 0 {
				return i
			}
		}
	}
	return -1
}

func findTopLevelInKeywordIndex(raw string) int {
	upper := strings.ToUpper(raw)
	depthParen, depthBracket, depthBrace := 0, 0, 0
	inSingle := false
	inDouble := false
	for i := 0; i <= len(raw)-2; i++ {
		ch := raw[i]
		if ch == '\'' && !inDouble && (i == 0 || raw[i-1] != '\\') {
			inSingle = !inSingle
			continue
		}
		if ch == '"' && !inSingle && (i == 0 || raw[i-1] != '\\') {
			inDouble = !inDouble
			continue
		}
		if inSingle || inDouble {
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
		if !strings.HasPrefix(upper[i:], "IN") {
			continue
		}
		beforeOK := i > 0 && strings.ContainsAny(string(raw[i-1]), " \t\n\r")
		afterIdx := i + 2
		afterOK := afterIdx < len(raw) && strings.ContainsAny(string(raw[afterIdx]), " \t\n\r")
		if beforeOK && afterOK {
			return i
		}
	}
	return -1
}

func containsAggregateFunctionCall(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}
	aggFns := map[string]struct{}{
		"count":          {},
		"collect":        {},
		"sum":            {},
		"min":            {},
		"max":            {},
		"avg":            {},
		"percentiledisc": {},
		"percentilecont": {},
	}
	inSingle := false
	inDouble := false
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		if ch == '\'' && !inDouble && (i == 0 || raw[i-1] != '\\') {
			inSingle = !inSingle
			continue
		}
		if ch == '"' && !inSingle && (i == 0 || raw[i-1] != '\\') {
			inDouble = !inDouble
			continue
		}
		if inSingle || inDouble || !isFunctionIdentStart(ch) {
			continue
		}
		if i > 0 && isFunctionIdentPart(raw[i-1]) {
			continue
		}
		j := i + 1
		for j < len(raw) && isFunctionIdentPart(raw[j]) {
			j++
		}
		name := strings.ToLower(raw[i:j])
		k := skipSpaces(raw, j)
		if k < len(raw) && raw[k] == '(' {
			if _, ok := aggFns[name]; ok {
				return true
			}
		}
		i = j - 1
	}
	return false
}

func firstOverflowingHexOrOctalLiteral(expr string) (string, bool) {
	inSingle := false
	inDouble := false
	inBacktick := false

	for i := 0; i < len(expr)-2; i++ {
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
		if inBacktick {
			if ch == '`' {
				inBacktick = false
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
		case '`':
			inBacktick = true
			continue
		}

		if !isHexOrOctalPrefixAt(expr, i) || !isNumericLiteralBoundaryBefore(expr, i) {
			continue
		}

		base := 16
		if expr[i+1] == 'o' || expr[i+1] == 'O' {
			base = 8
		}
		j := i + 2
		for j < len(expr) && isDigitForBase(expr[j], base) {
			j++
		}
		if j == i+2 || !isNumericLiteralBoundaryAfter(expr, j) {
			continue
		}

		negative := hasUnaryMinusBeforeLiteral(expr, i)
		if hexOrOctalLiteralOverflows(expr[i:j], base, negative) {
			if negative {
				return "-" + expr[i:j], true
			}
			return expr[i:j], true
		}
		i = j - 1
	}

	return "", false
}

func isHexOrOctalPrefixAt(expr string, idx int) bool {
	if idx < 0 || idx+2 > len(expr) {
		return false
	}
	if expr[idx] != '0' {
		return false
	}
	return expr[idx+1] == 'x' || expr[idx+1] == 'X' || expr[idx+1] == 'o' || expr[idx+1] == 'O'
}

func isDigitForBase(ch byte, base int) bool {
	if base == 16 {
		return (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F')
	}
	if base == 8 {
		return ch >= '0' && ch <= '7'
	}
	return false
}

func isNumericLiteralBoundaryBefore(expr string, idx int) bool {
	if idx <= 0 {
		return true
	}
	prev := expr[idx-1]
	return !isIdentifierOrDigitChar(prev)
}

func isNumericLiteralBoundaryAfter(expr string, idx int) bool {
	if idx >= len(expr) {
		return true
	}
	next := expr[idx]
	return !isIdentifierOrDigitChar(next)
}

func isIdentifierOrDigitChar(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_' || ch == '.'
}

func hasUnaryMinusBeforeLiteral(expr string, literalStart int) bool {
	i := literalStart - 1
	for i >= 0 && (expr[i] == ' ' || expr[i] == '\t' || expr[i] == '\n' || expr[i] == '\r') {
		i--
	}
	if i < 0 || expr[i] != '-' {
		return false
	}
	j := i - 1
	for j >= 0 && (expr[j] == ' ' || expr[j] == '\t' || expr[j] == '\n' || expr[j] == '\r') {
		j--
	}
	if j < 0 {
		return true
	}
	switch expr[j] {
	case '(', '[', '{', ',', ':', '+', '-', '*', '/', '%', '^', '<', '>', '=', '!':
		return true
	default:
		return false
	}
}

func hexOrOctalLiteralOverflows(raw string, base int, negative bool) bool {
	if len(raw) < 3 {
		return false
	}
	digits := raw[2:]
	if digits == "" {
		return false
	}
	parsed, err := strconv.ParseUint(digits, base, 64)
	if err != nil {
		return true
	}
	if negative {
		return parsed > (uint64(1) << 63)
	}
	return parsed > math.MaxInt64
}

func firstOverflowingFloatLiteral(expr string) (string, bool) {
	inSingle := false
	inDouble := false
	inBacktick := false

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
		if inBacktick {
			if ch == '`' {
				inBacktick = false
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
		case '`':
			inBacktick = true
			continue
		}

		start := i
		if ch == '.' {
			if i+1 >= len(expr) || expr[i+1] < '0' || expr[i+1] > '9' {
				continue
			}
		} else if ch < '0' || ch > '9' {
			continue
		}
		if !isNumericLiteralBoundaryBefore(expr, start) {
			continue
		}

		j := i
		for j < len(expr) && expr[j] >= '0' && expr[j] <= '9' {
			j++
		}
		hasDot := false
		if j < len(expr) && expr[j] == '.' {
			hasDot = true
			j++
			for j < len(expr) && expr[j] >= '0' && expr[j] <= '9' {
				j++
			}
		} else if ch == '.' {
			hasDot = true
		}

		hasExponent := false
		if j < len(expr) && (expr[j] == 'e' || expr[j] == 'E') {
			eStart := j
			j++
			if j < len(expr) && (expr[j] == '+' || expr[j] == '-') {
				j++
			}
			digitStart := j
			for j < len(expr) && expr[j] >= '0' && expr[j] <= '9' {
				j++
			}
			if j == digitStart {
				j = eStart
			} else {
				hasExponent = true
			}
		}

		if !(hasDot || hasExponent) {
			i = j
			continue
		}
		if !isNumericLiteralBoundaryAfter(expr, j) {
			i = j
			continue
		}

		token := expr[start:j]
		if value, err := strconv.ParseFloat(token, 64); err != nil {
			if err == strconv.ErrRange || strings.Contains(strings.ToLower(err.Error()), "value out of range") {
				if hasUnaryMinusBeforeLiteral(expr, start) {
					return "-" + token, true
				}
				return token, true
			}
		} else if math.IsInf(value, 0) {
			if hasUnaryMinusBeforeLiteral(expr, start) {
				return "-" + token, true
			}
			return token, true
		}

		i = j - 1
	}

	return "", false
}
