package parser

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/paegun/vitaledge/internal/cypher/ast"
)

var supportedExpressionFunctions = map[string]struct{}{
	"all":                         {},
	"acos":                        {},
	"any":                         {},
	"asin":                        {},
	"atan":                        {},
	"atan2":                       {},
	"avg":                         {},
	"abs":                         {},
	"btrim":                       {},
	"ceiling":                     {},
	"cot":                         {},
	"char_length":                 {},
	"character_length":            {},
	"coalesce":                    {},
	"collect_list":                {},
	"cos":                         {},
	"collect":                     {},
	"count":                       {},
	"date":                        {},
	"date.realtime":               {},
	"date.statement":              {},
	"date.transaction":            {},
	"date.truncate":               {},
	"datetime":                    {},
	"distance":                    {},
	"datetime.fromepoch":          {},
	"datetime.fromepochmillis":    {},
	"datetime.realtime":           {},
	"datetime.statement":          {},
	"datetime.transaction":        {},
	"datetime.truncate":           {},
	"duration":                    {},
	"duration.between":            {},
	"duration_between":            {},
	"duration.indays":             {},
	"duration.inmonths":           {},
	"duration.inseconds":          {},
	"degrees":                     {},
	"e":                           {},
	"elementid":                   {},
	"endvertex":                   {},
	"exp":                         {},
	"exists":                      {},
	"floor":                       {},
	"head":                        {},
	"haversin":                    {},
	"id":                          {},
	"isnan":                       {},
	"keys":                        {},
	"last":                        {},
	"labels":                      {},
	"left":                        {},
	"length":                      {},
	"log":                         {},
	"ln":                          {},
	"log10":                       {},
	"lower":                       {},
	"normalize":                   {},
	"nodes":                       {},
	"localdatetime":               {},
	"local_datetime":              {},
	"localdatetime.realtime":      {},
	"localdatetime.statement":     {},
	"localdatetime.transaction":   {},
	"localdatetime.truncate":      {},
	"localtime":                   {},
	"local_time":                  {},
	"localtime.realtime":          {},
	"localtime.statement":         {},
	"localtime.transaction":       {},
	"localtime.truncate":          {},
	"max":                         {},
	"min":                         {},
	"none":                        {},
	"vertexes":                    {},
	"nullif":                      {},
	"path_length":                 {},
	"pi":                          {},
	"point":                       {},
	"point.distance":              {},
	"point.withinbbox":            {},
	"percentilecont":              {},
	"percentile_cont":             {},
	"percentiledisc":              {},
	"percentile_disc":             {},
	"properties":                  {},
	"rand":                        {},
	"radians":                     {},
	"range":                       {},
	"reduce":                      {},
	"replace":                     {},
	"relationships":               {},
	"reverse":                     {},
	"randomuuid":                  {},
	"right":                       {},
	"round":                       {},
	"rtrim":                       {},
	"sin":                         {},
	"sign":                        {},
	"single":                      {},
	"size":                        {},
	"startnode":                   {},
	"startvertex":                 {},
	"split":                       {},
	"sqrt":                        {},
	"substring":                   {},
	"stdev":                       {},
	"stdevp":                      {},
	"stdev_pop":                   {},
	"stdev_samp":                  {},
	"sum":                         {},
	"tan":                         {},
	"tail":                        {},
	"time":                        {},
	"tobooleanlist":               {},
	"time.realtime":               {},
	"time.statement":              {},
	"timestamp":                   {},
	"time.transaction":            {},
	"time.truncate":               {},
	"tofloatlist":                 {},
	"tobooleanornull":             {},
	"tolower":                     {},
	"toboolean":                   {},
	"tofloatornull":               {},
	"tofloat":                     {},
	"tointegerlist":               {},
	"tointegerornull":             {},
	"tointeger":                   {},
	"tostringlist":                {},
	"tostringornull":              {},
	"tostring":                    {},
	"trim":                        {},
	"upper":                       {},
	"isempty":                     {},
	"ltrim":                       {},
	"toupper":                     {},
	"type":                        {},
	"valuetype":                   {},
	"endnode":                     {},
	"vector":                      {},
	"vector.similarity.cosine":    {},
	"vector.similarity.euclidean": {},
	"vector_distance":             {},
	"vector_dimension_count":      {},
	"vector_norm":                 {},
	"zoned_datetime":              {},
	"zoned_time":                  {},
}

var expressionOperatorKeywords = map[string]struct{}{
	"and":      {},
	"in":       {},
	"is":       {},
	"exists":   {},
	"not":      {},
	"or":       {},
	"starts":   {},
	"ends":     {},
	"contains": {},
	"by":       {},
	"with":     {},
	"xor":      {},
}

type projectionValueKind int

const (
	projectionValueKindUnknown projectionValueKind = iota
	projectionValueKindMap
	projectionValueKindNonMap
)

func validateUnexpectedSyntax(stmt ast.Statement, seg statementSegment) error {
	switch typed := stmt.(type) {
	case *ast.MatchQueryStatement:
		bound := map[string]patternVarRole{}
		for _, match := range typed.MatchClauses {
			if err := validatePatternParameterUsage(match.Pattern, seg); err != nil {
				return err
			}
			clauseBound := map[string]patternVarRole{}
			for _, binding := range scanPatternBindings(match.Pattern) {
				if binding.name == "" {
					continue
				}
				clauseBound[binding.name] = binding.role
			}
			if match.Where != nil {
				scope := map[string]patternVarRole{}
				for name, role := range bound {
					scope[name] = role
				}
				for name, role := range clauseBound {
					scope[name] = role
				}
				if containsInvalidExistsSubqueryClause(match.Where.Raw) {
					return &ParseError{Kind: ParseErrorUnsupported, Message: "InvalidClauseComposition", Statement: seg.index}
				}
				if hasInvalidWherePathPropertyAccess(match.Where.Raw, scope) {
					return &ParseError{Kind: ParseErrorUnsupported, Message: "InvalidArgumentType", Statement: seg.index}
				}
				if containsAggregateFunctionCall(stripExistsSubqueryBodies(match.Where.Raw)) {
					return &ParseError{Kind: ParseErrorUnsupported, Message: "InvalidAggregation", Statement: seg.index}
				}
				if hasUndefinedWhereIdentifier(match.Where.Raw, scope) {
					return &ParseError{Kind: ParseErrorUnsupported, Message: "UndefinedVariable", Statement: seg.index}
				}
			}
			for name, role := range clauseBound {
				bound[name] = role
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
			if literal, ok := firstOverflowingDecimalIntegerLiteral(item.Expression.Raw); ok {
				return &ParseError{Kind: ParseErrorUnsupported, Message: fmt.Sprintf("integer overflow in literal %q", literal), Statement: seg.index}
			}
			if literal, ok := firstOverflowingFloatLiteral(item.Expression.Raw); ok {
				return &ParseError{Kind: ParseErrorUnsupported, Message: fmt.Sprintf("floating point overflow in literal %q", literal), Statement: seg.index}
			}
			if isInvalidTypeArgumentExpression(item.Expression.Raw, bound) || isInvalidLabelsArgumentExpression(item.Expression.Raw, bound) {
				return &ParseError{Kind: ParseErrorUnsupported, Message: "InvalidArgumentType", Statement: seg.index}
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
		if err := validateProjectionSemanticsFromItems(typed.Return.Items, ast.ClauseKindReturn, bound, seg); err != nil {
			return err
		}
	case *ast.QueryStatement:
		bound := map[string]patternVarRole{}
		valueKinds := map[string]projectionValueKind{}
		for _, part := range typed.Parts {
			for _, clause := range part.Clauses {
				switch clause.Kind {
				case ast.ClauseKindReturn, ast.ClauseKindWith:
					if err := validateSkipLimitExpressionsInRawClause(clause.Raw, clause.Kind, seg); err != nil {
						return err
					}
					if clause.Kind == ast.ClauseKindReturn && hasTopLevelStarProjection(clause.Raw, clause.Kind) && len(bound) == 0 {
						return &ParseError{Kind: ParseErrorUnsupported, Message: "no variables in scope", Statement: seg.index}
					}
					if err := validateProjectionClauseNamesFromRaw(clause.Raw, clause.Kind, bound, seg); err != nil {
						return err
					}
					if err := validateProjectionSemanticsFromRaw(clause.Raw, clause.Kind, bound, seg); err != nil {
						return err
					}
					if err := validateStaticPropertyAccessTypesFromRaw(clause.Raw, clause.Kind, valueKinds, seg); err != nil {
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
						if literal, ok := firstOverflowingDecimalIntegerLiteral(expr); ok {
							return &ParseError{Kind: ParseErrorUnsupported, Message: fmt.Sprintf("integer overflow in literal %q", literal), Statement: seg.index}
						}
						if literal, ok := firstOverflowingFloatLiteral(expr); ok {
							return &ParseError{Kind: ParseErrorUnsupported, Message: fmt.Sprintf("floating point overflow in literal %q", literal), Statement: seg.index}
						}
						if isInvalidTypeArgumentExpression(expr, bound) || isInvalidLabelsArgumentExpression(expr, bound) {
							return &ParseError{Kind: ParseErrorUnsupported, Message: "InvalidArgumentType", Statement: seg.index}
						}
						if isInvalidLengthArgumentExpression(expr, bound) || isInvalidSizeArgumentExpression(expr, bound) {
							return &ParseError{Kind: ParseErrorUnsupported, Message: "invalid argument type", Statement: seg.index}
						}
					}
					if clause.Kind == ast.ClauseKindWith {
						recordWithBindings(clause.Raw, bound)
						recordWithValueKinds(clause.Raw, valueKinds)
					}
				case ast.ClauseKindSet:
					if err := validateSetAssignmentsAndExpressions(clause.Raw, bound, seg); err != nil {
						return err
					}
				case ast.ClauseKindDelete:
					if err := validateDeleteClauseExpressions(clause.Raw, bound, seg); err != nil {
						return err
					}
				case ast.ClauseKindUnwind:
					recordUnwindBinding(clause.Raw, bound)
				case ast.ClauseKindInQueryCall:
					if err := recordInQueryCallBindings(clause.Raw, bound, seg); err != nil {
						return err
					}
				case ast.ClauseKindMatch, ast.ClauseKindOptionalMatch, ast.ClauseKindCreate, ast.ClauseKindMerge:
					patternRaw, ok := extractClausePattern(clause.Raw, clause.Kind)
					if !ok {
						continue
					}
					clauseBound := map[string]patternVarRole{}
					for _, pattern := range splitTopLevelComma(patternRaw) {
						if err := validatePatternParameterUsage(pattern, seg); err != nil {
							return err
						}
						for _, binding := range scanPatternBindings(pattern) {
							if binding.name == "" {
								continue
							}
							clauseBound[binding.name] = binding.role
						}
					}
					if whereExpr, ok := extractMatchClauseWhereExpression(clause.Raw, clause.Kind); ok {
						scope := map[string]patternVarRole{}
						for name, role := range bound {
							scope[name] = role
						}
						for name, role := range clauseBound {
							scope[name] = role
						}
						if containsInvalidExistsSubqueryClause(whereExpr) {
							return &ParseError{Kind: ParseErrorUnsupported, Message: "InvalidClauseComposition", Statement: seg.index}
						}
						if hasInvalidWherePathPropertyAccess(whereExpr, scope) {
							return &ParseError{Kind: ParseErrorUnsupported, Message: "InvalidArgumentType", Statement: seg.index}
						}
						if containsAggregateFunctionCall(stripExistsSubqueryBodies(whereExpr)) {
							return &ParseError{Kind: ParseErrorUnsupported, Message: "InvalidAggregation", Statement: seg.index}
						}
						if hasUndefinedWhereIdentifier(whereExpr, scope) {
							return &ParseError{Kind: ParseErrorUnsupported, Message: "UndefinedVariable", Statement: seg.index}
						}
					}
					if clause.Kind == ast.ClauseKindCreate || clause.Kind == ast.ClauseKindMerge {
						scope := map[string]patternVarRole{}
						for name, role := range bound {
							scope[name] = role
						}
						for name, role := range clauseBound {
							scope[name] = role
						}
						for _, expr := range extractPatternPropertyMapExpressions(patternRaw) {
							if hasUndefinedWhereIdentifier(expr, scope) {
								return &ParseError{Kind: ParseErrorUnsupported, Message: "UndefinedVariable", Statement: seg.index}
							}
						}
						if clause.Kind == ast.ClauseKindMerge {
							if err := validateSetAssignmentsAndExpressions("SET "+strings.TrimSpace(clause.MergeOnCreate), scope, seg); err != nil {
								return err
							}
							if err := validateSetAssignmentsAndExpressions("SET "+strings.TrimSpace(clause.MergeOnMatch), scope, seg); err != nil {
								return err
							}
						}
					}
					for name, role := range clauseBound {
						bound[name] = role
					}
				}
			}
		}
	}

	return nil
}

func validateDeleteClauseExpressions(raw string, bound map[string]patternVarRole, seg statementSegment) error {
	targets, ok := extractDeleteTargetExpressions(raw)
	if !ok || len(targets) == 0 {
		return &ParseError{Kind: ParseErrorUnsupported, Message: "InvalidDelete", Statement: seg.index}
	}

	for _, target := range targets {
		expr := strings.TrimSpace(target)
		if expr == "" {
			continue
		}
		if hasInvalidDeleteTargetSyntax(expr) {
			return &ParseError{Kind: ParseErrorUnsupported, Message: "InvalidDelete", Statement: seg.index}
		}
		if hasUndefinedWhereIdentifier(expr, bound) {
			return &ParseError{Kind: ParseErrorUnsupported, Message: "UndefinedVariable", Statement: seg.index}
		}
		if isDefinitelyInvalidDeleteExpression(expr, bound) {
			return &ParseError{Kind: ParseErrorUnsupported, Message: "InvalidArgumentType", Statement: seg.index}
		}
	}
	return nil
}

func validateSetAssignmentsAndExpressions(raw string, bound map[string]patternVarRole, seg statementSegment) error {
	raw = strings.TrimSpace(raw)
	if raw == "" || strings.EqualFold(raw, "SET") {
		return nil
	}

	for _, target := range extractSetAssignmentTargets(raw) {
		if target == "" {
			continue
		}
		if _, ok := bound[target]; !ok {
			return &ParseError{Kind: ParseErrorUnsupported, Message: "UndefinedVariable", Statement: seg.index}
		}
	}

	expressions := extractSetValueExpressions(raw)
	for _, expr := range expressions {
		if hasUndefinedIdentifierCaseAware(expr, bound) {
			return &ParseError{Kind: ParseErrorUnsupported, Message: "UndefinedVariable", Statement: seg.index}
		}
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
		if literal, ok := firstOverflowingDecimalIntegerLiteral(expr); ok {
			return &ParseError{Kind: ParseErrorUnsupported, Message: fmt.Sprintf("integer overflow in literal %q", literal), Statement: seg.index}
		}
		if literal, ok := firstOverflowingFloatLiteral(expr); ok {
			return &ParseError{Kind: ParseErrorUnsupported, Message: fmt.Sprintf("floating point overflow in literal %q", literal), Statement: seg.index}
		}
		if isInvalidTypeArgumentExpression(expr, bound) || isInvalidLabelsArgumentExpression(expr, bound) {
			return &ParseError{Kind: ParseErrorUnsupported, Message: "InvalidArgumentType", Statement: seg.index}
		}
		if isInvalidLengthArgumentExpression(expr, bound) || isInvalidSizeArgumentExpression(expr, bound) {
			return &ParseError{Kind: ParseErrorUnsupported, Message: "invalid argument type", Statement: seg.index}
		}
	}

	return nil
}

func extractSetAssignmentTargets(raw string) []string {
	text := strings.TrimSpace(raw)
	upper := strings.ToUpper(text)
	if strings.HasPrefix(upper, "SET") {
		text = strings.TrimSpace(text[len("SET"):])
	}
	if text == "" {
		return nil
	}

	targets := make([]string, 0)
	for _, item := range splitTopLevelComma(text) {
		entry := strings.TrimSpace(item)
		if entry == "" {
			continue
		}

		lhs := ""
		if idx := indexTopLevelEquals(entry); idx >= 0 {
			lhs = strings.TrimSpace(entry[:idx])
		} else if idx := indexTopLevelColonForLabelSet(entry); idx >= 0 {
			lhs = strings.TrimSpace(entry[:idx])
		}
		if lhs == "" {
			continue
		}
		if root := extractSetAssignmentRoot(lhs); root != "" {
			targets = append(targets, root)
		}
	}

	return targets
}

func extractSetAssignmentRoot(lhs string) string {
	lhs = strings.TrimSpace(lhs)
	if lhs == "" {
		return ""
	}
	if idx := strings.Index(lhs, "."); idx >= 0 {
		lhs = strings.TrimSpace(lhs[:idx])
	}
	if idx := strings.Index(lhs, "["); idx >= 0 {
		lhs = strings.TrimSpace(lhs[:idx])
	}
	if !isSetAssignmentIdentifier(lhs) {
		return ""
	}
	return lhs
}

func isSetAssignmentIdentifier(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		if i == 0 {
			if !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_') {
				return false
			}
			continue
		}
		if !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_') {
			return false
		}
	}
	return true
}

func indexTopLevelColonForLabelSet(raw string) int {
	depthParen, depthBracket, depthBrace := 0, 0, 0
	inSingle, inDouble := false, false
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
		case ':':
			if depthParen == 0 && depthBracket == 0 && depthBrace == 0 {
				if i > 0 && raw[i-1] == ':' {
					continue
				}
				if i+1 < len(raw) && raw[i+1] == ':' {
					continue
				}
				return i
			}
		}
	}
	return -1
}

func extractDeleteTargetExpressions(raw string) ([]string, bool) {
	text := strings.TrimSpace(raw)
	upper := strings.ToUpper(text)
	switch {
	case strings.HasPrefix(upper, "DETACH DELETE"):
		text = strings.TrimSpace(text[len("DETACH DELETE"):])
	case strings.HasPrefix(upper, "DETACHDELETE"):
		text = strings.TrimSpace(text[len("DETACHDELETE"):])
	case strings.HasPrefix(upper, "DELETE"):
		text = strings.TrimSpace(text[len("DELETE"):])
	default:
		return nil, false
	}
	if text == "" {
		return nil, false
	}
	return splitTopLevelComma(text), true
}

func hasInvalidDeleteTargetSyntax(expr string) bool {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return false
	}
	_, next, ok := readIdentifier(expr, 0)
	if !ok {
		return false
	}
	next = skipSpaces(expr, next)
	if next >= len(expr) || expr[next] != ':' {
		return false
	}
	for {
		next = skipSpaces(expr, next+1)
		_, identEnd, identOK := readIdentifier(expr, next)
		if !identOK {
			return false
		}
		next = skipSpaces(expr, identEnd)
		if next >= len(expr) {
			return true
		}
		if expr[next] != ':' {
			return false
		}
	}
}

func isDefinitelyInvalidDeleteExpression(expr string, bound map[string]patternVarRole) bool {
	trimmed := strings.TrimSpace(expr)
	if trimmed == "" || strings.EqualFold(trimmed, "null") {
		return false
	}
	if _, ok := simpleIdentifierExpression(trimmed); ok {
		return false
	}

	refs := extractIdentifierPropertyReferences(stripExistsSubqueryBodies(trimmed))
	for _, ref := range refs {
		root := ref
		if idx := strings.Index(root, "."); idx >= 0 {
			root = root[:idx]
		}
		root = strings.TrimSpace(root)
		if root == "" || isCypherLiteralKeyword(root) {
			continue
		}
		if _, ok := expressionOperatorKeywords[strings.ToLower(root)]; ok {
			continue
		}
		if _, ok := supportedExpressionFunctions[strings.ToLower(root)]; ok {
			continue
		}
		if _, exists := bound[root]; exists {
			return false
		}
	}

	return len(refs) == 0
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

func recordInQueryCallBindings(raw string, bound map[string]patternVarRole, seg statementSegment) error {
	upper := strings.ToUpper(strings.TrimSpace(raw))
	if !strings.HasPrefix(upper, "CALL") {
		return nil
	}
	yieldIdx := strings.Index(upper, "YIELD")
	if yieldIdx < 0 {
		return nil
	}
	yieldRaw := strings.TrimSpace(raw[yieldIdx+len("YIELD"):])
	if yieldRaw == "" {
		return nil
	}
	if strings.TrimSpace(yieldRaw) == "*" {
		return &ParseError{Kind: ParseErrorUnsupported, Message: "unexpected syntax", Statement: seg.index}
	}
	items := splitTopLevelComma(yieldRaw)
	seen := map[string]struct{}{}
	for _, item := range items {
		entry := strings.TrimSpace(item)
		if entry == "" {
			continue
		}
		alias := entry
		if idx := indexTopLevelAliasKeyword(entry); idx >= 0 {
			alias = strings.TrimSpace(entry[idx+len("AS"):])
		}
		if alias == "" {
			continue
		}
		if _, exists := seen[alias]; exists {
			return &ParseError{Kind: ParseErrorUnsupported, Message: "VariableAlreadyBound", Statement: seg.index}
		}
		if _, exists := bound[alias]; exists {
			return &ParseError{Kind: ParseErrorUnsupported, Message: "VariableAlreadyBound", Statement: seg.index}
		}
		seen[alias] = struct{}{}
		bound[alias] = patternRoleValue
	}
	return nil
}

func validateProjectionClauseNamesFromRaw(raw string, kind ast.ClauseKind, bound map[string]patternVarRole, seg statementSegment) error {
	items := splitProjectionItems(raw, kind)
	seen := map[string]struct{}{}
	if kind == ast.ClauseKindReturn && hasTopLevelStarProjection(raw, kind) && len(bound) == 0 {
		return &ParseError{Kind: ParseErrorUnsupported, Message: "no variables in scope", Statement: seg.index}
	}
	for _, item := range items {
		if item == "" || item == "*" {
			continue
		}
		name := ""
		if idx := indexTopLevelAliasKeyword(item); idx >= 0 {
			aliasRaw := strings.TrimSpace(item[idx+len("AS"):])
			alias, next, ok := readIdentifier(aliasRaw, 0)
			if ok {
				next = skipSpaces(aliasRaw, next)
				if next == len(aliasRaw) {
					name = alias
				}
			}
		}
		if name == "" {
			name = projectionOutputNameFromRaw(item)
		}
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

type projectionSemanticItem struct {
	Expression string
	Alias      string
	HasAlias   bool
	IsStar     bool
}

func validateProjectionSemanticsFromItems(items []ast.ProjectionItem, kind ast.ClauseKind, bound map[string]patternVarRole, seg statementSegment) error {
	semanticItems := make([]projectionSemanticItem, 0, len(items))
	for _, item := range items {
		expr := strings.TrimSpace(item.Expression.Raw)
		semantic := projectionSemanticItem{Expression: expr, Alias: strings.TrimSpace(item.Alias), HasAlias: strings.TrimSpace(item.Alias) != "", IsStar: expr == "*"}
		semanticItems = append(semanticItems, semantic)
	}
	return validateProjectionSemanticItems(semanticItems, kind, bound, seg)
}

func validateProjectionSemanticsFromRaw(raw string, kind ast.ClauseKind, bound map[string]patternVarRole, seg statementSegment) error {
	rawItems := splitProjectionItems(raw, kind)
	semanticItems := make([]projectionSemanticItem, 0, len(rawItems))
	for _, item := range rawItems {
		text := strings.TrimSpace(item)
		if text == "" {
			continue
		}
		if text == "*" {
			semanticItems = append(semanticItems, projectionSemanticItem{Expression: "*", IsStar: true})
			continue
		}

		idx := indexTopLevelAliasKeyword(text)
		if idx >= 0 {
			expr := strings.TrimSpace(text[:idx])
			aliasRaw := strings.TrimSpace(text[idx+len("AS"):])
			alias, next, ok := readIdentifier(aliasRaw, 0)
			if ok {
				next = skipSpaces(aliasRaw, next)
				if next != len(aliasRaw) {
					ok = false
				}
			}
			if !ok || alias == "" {
				semanticItems = append(semanticItems, projectionSemanticItem{Expression: expr, HasAlias: true})
				continue
			}
			semanticItems = append(semanticItems, projectionSemanticItem{Expression: expr, Alias: alias, HasAlias: true})
			continue
		}

		semanticItems = append(semanticItems, projectionSemanticItem{Expression: text})
	}

	return validateProjectionSemanticItems(semanticItems, kind, bound, seg)
}

func validateProjectionSemanticItems(items []projectionSemanticItem, kind ast.ClauseKind, bound map[string]patternVarRole, seg statementSegment) error {
	for _, item := range items {
		if item.IsStar {
			continue
		}
		expr := strings.TrimSpace(item.Expression)
		if expr == "" {
			continue
		}

		if kind == ast.ClauseKindWith && !item.HasAlias && !isSimpleReferenceExpression(expr) {
			return &ParseError{Kind: ParseErrorUnsupported, Message: "NoExpressionAlias", Statement: seg.index}
		}

		if ident, ok := simpleIdentifierExpression(expr); ok {
			if !isCypherLiteralKeyword(ident) {
				if _, exists := bound[ident]; !exists {
					return &ParseError{Kind: ParseErrorUnsupported, Message: "UndefinedVariable", Statement: seg.index}
				}
			}
		}

		if hasNestedAggregateFunctionCall(expr) {
			return &ParseError{Kind: ParseErrorUnsupported, Message: "NestedAggregation", Statement: seg.index}
		}

		if hasUndefinedInlineMapValueIdentifier(expr, bound) {
			return &ParseError{Kind: ParseErrorUnsupported, Message: "UndefinedVariable", Statement: seg.index}
		}
	}

	hasAggregate := false
	allowedRefs := map[string]struct{}{}
	for _, item := range items {
		if item.IsStar {
			continue
		}
		expr := strings.TrimSpace(item.Expression)
		if expr == "" {
			continue
		}
		if containsAggregateFunctionCall(expr) {
			hasAggregate = true
			continue
		}
		if ref, ok := normalizeAllowedGroupingReference(expr); ok {
			allowedRefs[ref] = struct{}{}
		}
		if item.Alias != "" {
			allowedRefs[item.Alias] = struct{}{}
		}
	}
	if !hasAggregate {
		return nil
	}

	for _, item := range items {
		if item.IsStar {
			continue
		}
		expr := strings.TrimSpace(item.Expression)
		if expr == "" || !containsAggregateFunctionCall(expr) {
			continue
		}
		if strings.Contains(expr, "[") && strings.Contains(strings.ToUpper(expr), " IN ") {
			continue
		}
		if containsQuantifiedPredicateWithIn(expr) {
			continue
		}
		for _, ref := range extractNonAggregateReferences(expr) {
			if isCypherLiteralKeyword(ref) {
				continue
			}
			if _, ok := allowedRefs[ref]; ok {
				continue
			}
			return &ParseError{Kind: ParseErrorUnsupported, Message: "AmbiguousAggregationExpression", Statement: seg.index}
		}
	}

	return nil
}

func isSimpleReferenceExpression(expr string) bool {
	_, ok := simpleIdentifierOrPropertyExpression(expr)
	return ok
}

func simpleIdentifierExpression(expr string) (string, bool) {
	name, ok := simpleIdentifierOrPropertyExpression(expr)
	if !ok || strings.Contains(name, ".") {
		return "", false
	}
	return name, true
}

func simpleIdentifierOrPropertyExpression(expr string) (string, bool) {
	text := strings.TrimSpace(expr)
	if text == "" {
		return "", false
	}
	name, next, ok := readIdentifier(text, 0)
	if !ok {
		return "", false
	}
	for {
		next = skipSpaces(text, next)
		if next >= len(text) || text[next] != '.' {
			break
		}
		next = skipSpaces(text, next+1)
		part, partNext, partOK := readIdentifier(text, next)
		if !partOK {
			return "", false
		}
		name += "." + part
		next = partNext
	}
	next = skipSpaces(text, next)
	if next != len(text) {
		return "", false
	}
	return name, true
}

func normalizeAllowedGroupingReference(expr string) (string, bool) {
	return simpleIdentifierOrPropertyExpression(expr)
}

func isCypherLiteralKeyword(name string) bool {
	return strings.EqualFold(name, "null") || strings.EqualFold(name, "true") || strings.EqualFold(name, "false") || strings.HasPrefix(name, "__ve_reduce_")
}

func hasNestedAggregateFunctionCall(expr string) bool {
	for _, call := range findAggregateFunctionCalls(expr) {
		if strings.Contains(call.args, "[") && strings.Contains(call.args, "|") {
			continue
		}
		if containsAggregateFunctionCall(call.args) {
			return true
		}
	}
	return false
}

type aggregateFunctionCall struct {
	start int
	end   int
	args  string
}

func findAggregateFunctionCalls(raw string) []aggregateFunctionCall {
	calls := make([]aggregateFunctionCall, 0)
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

		if !isFunctionIdentStart(ch) {
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
		if !isAggregateFunctionName(name) {
			i = j - 1
			continue
		}

		k := skipSpaces(raw, j)
		if k >= len(raw) || raw[k] != '(' {
			i = j - 1
			continue
		}
		end := findMatchingParen(raw, k)
		if end < 0 {
			i = j - 1
			continue
		}

		calls = append(calls, aggregateFunctionCall{start: i, end: end + 1, args: raw[k+1 : end]})
		i = end
	}

	return calls
}

func isAggregateFunctionName(name string) bool {
	switch canonicalAggregateFunctionName(name) {
	case "count", "collect", "sum", "min", "max", "avg", "percentiledisc", "percentilecont", "stdev", "stdevp":
		return true
	default:
		return false
	}
}

func canonicalAggregateFunctionName(name string) string {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "collect_list":
		return "collect"
	case "percentile_disc":
		return "percentiledisc"
	case "percentile_cont":
		return "percentilecont"
	case "stdev_samp":
		return "stdev"
	case "stdev_pop":
		return "stdevp"
	default:
		return strings.ToLower(strings.TrimSpace(name))
	}
}

func findMatchingParen(raw string, openIdx int) int {
	depth := 0
	inSingle := false
	inDouble := false
	inBacktick := false
	for i := openIdx; i < len(raw); i++ {
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
		case '"':
			inDouble = true
		case '`':
			inBacktick = true
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

func extractNonAggregateReferences(expr string) []string {
	if strings.TrimSpace(expr) == "" {
		return nil
	}
	calls := findAggregateFunctionCalls(expr)
	if len(calls) == 0 {
		return extractIdentifierPropertyReferences(expr)
	}

	stripped := expr
	for i := len(calls) - 1; i >= 0; i-- {
		call := calls[i]
		stripped = stripped[:call.start] + " " + stripped[call.end:]
	}
	return extractIdentifierPropertyReferences(stripped)
}

func extractIdentifierPropertyReferences(raw string) []string {
	refs := make([]string, 0)
	seen := map[string]struct{}{}
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
		if prev, ok := previousNonSpaceChar(raw, i); ok && (prev == ':' || prev == '|') {
			continue
		}
		if i > 0 && (isIdentifierPart(raw[i-1]) || raw[i-1] == '.' || raw[i-1] == '$') {
			continue
		}

		name, next, ok := readIdentifier(raw, i)
		if !ok {
			continue
		}
		if isMapLiteralKeyReference(raw, i, next) {
			i = next - 1
			continue
		}
		cursor := skipSpaces(raw, next)
		if cursor < len(raw) && raw[cursor] == '(' {
			i = next - 1
			continue
		}

		for {
			cursor = skipSpaces(raw, cursor)
			if cursor >= len(raw) || raw[cursor] != '.' {
				break
			}
			cursor = skipSpaces(raw, cursor+1)
			part, partNext, partOK := readIdentifier(raw, cursor)
			if !partOK {
				break
			}
			name += "." + part
			cursor = partNext
		}

		if !isCypherLiteralKeyword(name) {
			if _, exists := seen[name]; !exists {
				seen[name] = struct{}{}
				refs = append(refs, name)
			}
		}
		i = next - 1
	}

	return refs
}

func isMapLiteralKeyReference(raw string, start int, end int) bool {
	if start < 0 || end < 0 || start >= len(raw) || end > len(raw) || start >= end {
		return false
	}
	colon := skipSpaces(raw, end)
	if colon >= len(raw) || raw[colon] != ':' {
		return false
	}
	for i := start - 1; i >= 0; i-- {
		switch raw[i] {
		case ' ', '\t', '\n', '\r':
			continue
		case '{', ',':
			return true
		default:
			return false
		}
	}
	return false
}

func previousNonSpaceChar(raw string, idx int) (byte, bool) {
	for i := idx - 1; i >= 0; i-- {
		switch raw[i] {
		case ' ', '\t', '\n', '\r':
			continue
		default:
			return raw[i], true
		}
	}
	return 0, false
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
		if idx := indexTopLevelOrderBy(text); idx >= 0 {
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
		if idx := indexTopLevelOrderBy(text); idx >= 0 {
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

func extractMatchClauseWhereExpression(raw string, kind ast.ClauseKind) (string, bool) {
	text := strings.TrimSpace(raw)
	upper := strings.ToUpper(text)
	prefix := ""
	switch kind {
	case ast.ClauseKindMatch:
		prefix = "MATCH"
	case ast.ClauseKindOptionalMatch:
		prefix = "OPTIONAL MATCH"
	default:
		return "", false
	}
	if !strings.HasPrefix(upper, prefix) {
		return "", false
	}
	text = strings.TrimSpace(text[len(prefix):])
	idx := indexTopLevelKeyword(text, "WHERE")
	if idx < 0 {
		return "", false
	}
	expr := strings.TrimSpace(text[idx+len("WHERE"):])
	if expr == "" {
		return "", false
	}
	return expr, true
}

func hasUndefinedWhereIdentifier(expr string, bound map[string]patternVarRole) bool {
	listCompScoped := collectListComprehensionVars(stripExistsSubqueryBodies(expr))
	for _, ref := range extractIdentifierPropertyReferences(stripExistsSubqueryBodies(expr)) {
		root := ref
		if idx := strings.Index(root, "."); idx >= 0 {
			root = root[:idx]
		}
		root = strings.TrimSpace(root)
		if root == "" || isCypherLiteralKeyword(root) {
			continue
		}
		if _, ok := expressionOperatorKeywords[strings.ToLower(root)]; ok {
			continue
		}
		if _, ok := supportedExpressionFunctions[strings.ToLower(root)]; ok {
			continue
		}
		if _, ok := listCompScoped[root]; ok {
			continue
		}
		if _, exists := bound[root]; !exists {
			return true
		}
	}
	return false
}

func hasInvalidWherePathPropertyAccess(expr string, bound map[string]patternVarRole) bool {
	for _, ref := range extractIdentifierPropertyReferences(stripExistsSubqueryBodies(expr)) {
		idx := strings.Index(ref, ".")
		if idx <= 0 {
			continue
		}
		root := strings.TrimSpace(ref[:idx])
		if root == "" {
			continue
		}
		if role, exists := bound[root]; exists && role == patternRolePath {
			return true
		}
	}
	return false
}

func containsQuantifiedPredicateWithIn(expr string) bool {
	upper := strings.ToUpper(strings.TrimSpace(expr))
	if upper == "" {
		return false
	}
	if strings.Contains(upper, "ALL(") || strings.Contains(upper, "ANY(") || strings.Contains(upper, "NONE(") || strings.Contains(upper, "SINGLE(") {
		return strings.Contains(upper, " IN ")
	}
	return false
}

func collectListComprehensionVars(expr string) map[string]struct{} {
	out := map[string]struct{}{}
	if expr == "" {
		return out
	}
	for i := 0; i < len(expr); i++ {
		if expr[i] != '[' {
			continue
		}
		end := findMatchingBracketIndex(expr, i)
		if end <= i+1 {
			continue
		}
		body := strings.TrimSpace(expr[i+1 : end])
		if name, ok := parseListComprehensionVar(body); ok {
			out[name] = struct{}{}
		}
		for nested := range collectListComprehensionVars(body) {
			out[nested] = struct{}{}
		}
		i = end
	}
	return out
}

func parseListComprehensionVar(body string) (string, bool) {
	if body == "" {
		return "", false
	}
	name, next, ok := readIdentifier(body, 0)
	if !ok || name == "" {
		return "", false
	}
	next = skipSpaces(body, next)
	if next+1 >= len(body) {
		return "", false
	}
	if !strings.EqualFold(body[next:next+2], "IN") {
		return "", false
	}
	prevOK := next == 0 || !isIdentifierPart(body[next-1])
	post := next + 2
	nextOK := post >= len(body) || !isIdentifierPart(body[post])
	if !prevOK || !nextOK {
		return "", false
	}
	return name, true
}

func findMatchingBracketIndex(raw string, start int) int {
	if start < 0 || start >= len(raw) || raw[start] != '[' {
		return -1
	}
	depth := 0
	inSingle := false
	inDouble := false
	for i := start; i < len(raw); i++ {
		ch := raw[i]
		if ch == '\'' && (i == 0 || raw[i-1] != '\\') && !inDouble {
			inSingle = !inSingle
			continue
		}
		if ch == '"' && (i == 0 || raw[i-1] != '\\') && !inSingle {
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

func containsInvalidExistsSubqueryClause(expr string) bool {
	bodies := extractExistsSubqueryBodies(expr)
	if len(bodies) == 0 {
		return false
	}
	for _, body := range bodies {
		if containsTopLevelKeywordLoose(body, "SET") ||
			containsTopLevelKeywordLoose(body, "CREATE") ||
			containsTopLevelKeywordLoose(body, "MERGE") ||
			containsTopLevelKeywordLoose(body, "DELETE") ||
			containsTopLevelKeywordLoose(body, "REMOVE") {
			return true
		}
	}
	return false
}

func extractExistsSubqueryBodies(expr string) []string {
	bodies := make([]string, 0)
	if expr == "" {
		return bodies
	}
	upper := strings.ToUpper(expr)
	for i := 0; i < len(expr); {
		if i+6 <= len(expr) && upper[i:i+6] == "EXISTS" {
			j := i + 6
			for j < len(expr) && isSpace(expr[j]) {
				j++
			}
			if j < len(expr) && expr[j] == '{' {
				end := findMatchingBraceIndex(expr, j)
				if end > j {
					bodies = append(bodies, strings.TrimSpace(expr[j+1:end]))
					i = end + 1
					continue
				}
			}
		}
		i++
	}
	return bodies
}

func containsTopLevelKeywordLoose(raw, keyword string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}
	upper := strings.ToUpper(raw)
	keyword = strings.ToUpper(strings.TrimSpace(keyword))
	if keyword == "" || len(upper) < len(keyword) {
		return false
	}
	depth := 0
	inSingle := false
	inDouble := false
	inBacktick := false
	for i := 0; i <= len(upper)-len(keyword); i++ {
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
		case '(', '[', '{':
			depth++
			continue
		case ')', ']', '}':
			if depth > 0 {
				depth--
			}
			continue
		}
		if depth != 0 {
			continue
		}
		if strings.HasPrefix(upper[i:], keyword) {
			prevOK := i == 0 || !isIdentifierPart(raw[i-1])
			nextPos := i + len(keyword)
			nextOK := nextPos >= len(raw) || !isIdentifierPart(raw[nextPos])
			if prevOK && nextOK {
				return true
			}
			if prevOK {
				return true
			}
		}
	}
	return false
}

func stripExistsSubqueryBodies(expr string) string {
	if expr == "" {
		return expr
	}
	upper := strings.ToUpper(expr)
	var out strings.Builder
	for i := 0; i < len(expr); {
		if i+6 <= len(expr) && upper[i:i+6] == "EXISTS" {
			j := i + 6
			for j < len(expr) && isSpace(expr[j]) {
				j++
			}
			if j < len(expr) && expr[j] == '{' {
				end := findMatchingBraceIndex(expr, j)
				if end > j {
					out.WriteString("EXISTS { }")
					i = end + 1
					continue
				}
			}
		}
		out.WriteByte(expr[i])
		i++
	}
	return out.String()
}

func findMatchingBraceIndex(raw string, openIdx int) int {
	if openIdx < 0 || openIdx >= len(raw) || raw[openIdx] != '{' {
		return -1
	}
	depth := 0
	inSingle := false
	inDouble := false
	inBacktick := false
	for i := openIdx; i < len(raw); i++ {
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
		case '"':
			inDouble = true
		case '`':
			inBacktick = true
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return i
			}
			if depth < 0 {
				return -1
			}
		}
	}
	return -1
}

func isSpace(ch byte) bool {
	switch ch {
	case ' ', '\t', '\n', '\r':
		return true
	default:
		return false
	}
}

func validateStaticPropertyAccessTypesFromRaw(raw string, kind ast.ClauseKind, kinds map[string]projectionValueKind, seg statementSegment) error {
	for _, expr := range extractProjectionExpressions(raw, kind) {
		base, ok := simplePropertyAccessBase(expr)
		if !ok {
			continue
		}
		if kinds[base] == projectionValueKindNonMap {
			return &ParseError{Kind: ParseErrorUnsupported, Message: "InvalidArgumentType", Statement: seg.index}
		}
	}
	return nil
}

func recordWithValueKinds(raw string, kinds map[string]projectionValueKind) {
	items := splitProjectionItems(raw, ast.ClauseKindWith)
	if len(items) == 0 {
		return
	}

	original := map[string]projectionValueKind{}
	for key, kind := range kinds {
		original[key] = kind
	}

	hasStar := false
	projected := map[string]projectionValueKind{}
	for _, item := range items {
		entry := strings.TrimSpace(item)
		if entry == "" {
			continue
		}
		if entry == "*" {
			hasStar = true
			continue
		}
		alias, expr, ok := parseProjectionAlias(entry)
		if !ok || alias == "" {
			continue
		}
		kind := inferProjectionValueKind(expr, original)
		if kind != projectionValueKindUnknown {
			projected[alias] = kind
		}
	}

	for key := range kinds {
		delete(kinds, key)
	}
	if hasStar {
		for key, kind := range original {
			kinds[key] = kind
		}
	}
	for key, kind := range projected {
		kinds[key] = kind
	}
}

func inferProjectionValueKind(expr string, known map[string]projectionValueKind) projectionValueKind {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return projectionValueKindUnknown
	}
	if kind, ok := known[expr]; ok {
		return kind
	}
	if isMapLiteralExpression(expr) {
		return projectionValueKindMap
	}
	if isNonMapLiteralExpression(expr) {
		return projectionValueKindNonMap
	}
	return projectionValueKindUnknown
}

func simplePropertyAccessBase(expr string) (string, bool) {
	trimmed := strings.TrimSpace(expr)
	if trimmed == "" {
		return "", false
	}
	name, next, ok := readIdentifier(trimmed, 0)
	if !ok {
		return "", false
	}
	next = skipSpaces(trimmed, next)
	if next >= len(trimmed) || trimmed[next] != '.' {
		return "", false
	}
	next++
	for {
		next = skipSpaces(trimmed, next)
		if next >= len(trimmed) {
			return "", false
		}
		if trimmed[next] == '`' {
			closed := false
			next++
			for next < len(trimmed) {
				if trimmed[next] != '`' {
					next++
					continue
				}
				if next+1 < len(trimmed) && trimmed[next+1] == '`' {
					next += 2
					continue
				}
				closed = true
				next++
				break
			}
			if !closed {
				return "", false
			}
		} else {
			_, idNext, idOK := readIdentifier(trimmed, next)
			if !idOK {
				return "", false
			}
			next = idNext
		}
		next = skipSpaces(trimmed, next)
		if next == len(trimmed) {
			return name, true
		}
		if trimmed[next] != '.' {
			return "", false
		}
		next++
	}
}

func isMapLiteralExpression(expr string) bool {
	expr = strings.TrimSpace(expr)
	if len(expr) < 2 || expr[0] != '{' || expr[len(expr)-1] != '}' {
		return false
	}
	return bracesAreBalanced(expr[1 : len(expr)-1])
}

func bracesAreBalanced(raw string) bool {
	depth := 0
	inSingle := false
	inDouble := false
	for i := 0; i < len(raw); i++ {
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
		switch ch {
		case '{':
			depth++
		case '}':
			if depth == 0 {
				return false
			}
			depth--
		}
	}
	return depth == 0 && !inSingle && !inDouble
}

func isNonMapLiteralExpression(expr string) bool {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return false
	}
	if strings.EqualFold(expr, "null") || strings.EqualFold(expr, "true") || strings.EqualFold(expr, "false") {
		return true
	}
	if (strings.HasPrefix(expr, "'") && strings.HasSuffix(expr, "'")) || (strings.HasPrefix(expr, "\"") && strings.HasSuffix(expr, "\"")) {
		return true
	}
	if _, err := strconv.ParseInt(expr, 10, 64); err == nil {
		return true
	}
	if _, err := strconv.ParseFloat(expr, 64); err == nil {
		return true
	}
	if len(expr) >= 2 && expr[0] == '[' && expr[len(expr)-1] == ']' {
		return true
	}
	return false
}

func indexTopLevelOrderBy(raw string) int {
	if idx := indexTopLevelKeyword(raw, "ORDERBY"); idx >= 0 {
		return idx
	}
	return indexTopLevelKeyword(raw, "ORDER BY")
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
		if idx := indexTopLevelOrderBy(text); idx >= 0 {
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
		if idx := indexTopLevelOrderBy(text); idx >= 0 {
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

// hasUndefinedIdentifierCaseAware wraps hasUndefinedWhereIdentifier with
// awareness for CASE...END expressions. Because clause.Raw comes from ANTLR's
// GetText() with all whitespace stripped, a CASE expression token-collapses
// its keywords into the adjacent identifiers (e.g. "CASEp.role" instead of
// "CASE p.role"), causing the plain identifier extractor to flag false
// undefined-variable errors. This function detects top-level CASE expressions,
// expands them into their individual sub-expressions (comparison operand, WHEN
// operands, THEN results, ELSE result), and validates each part independently.
func hasUndefinedIdentifierCaseAware(expr string, bound map[string]patternVarRole) bool {
	if subExprs := extractCaseSubExpressions(expr); subExprs != nil {
		for _, sub := range subExprs {
			if hasUndefinedIdentifierCaseAware(sub, bound) {
				return true
			}
		}
		return false
	}
	return hasUndefinedWhereIdentifier(expr, bound)
}

// extractCaseSubExpressions parses a CASE...END expression (in the
// whitespace-stripped ANTLR form) and returns all constituent sub-expressions:
// the optional comparison operand for simple CASE, each WHEN operand, each
// THEN result expression, and the optional ELSE expression. Returns nil if
// expr is not a top-level CASE expression.
func extractCaseSubExpressions(expr string) []string {
	expr = strings.TrimSpace(expr)
	upper := strings.ToUpper(expr)
	if !strings.HasPrefix(upper, "CASE") || !strings.HasSuffix(upper, "END") {
		return nil
	}
	// "END" must be the trailing keyword, not merely a suffix of the last
	// identifier (e.g. "n.trendEnd" would collapse to "...trendEND" which
	// ends with "END" too). Require that the byte right before the trailing
	// "END" is either a quote, a closing bracket/paren/brace, or another
	// uppercase identifier character that completes a valid identifier — we
	// accept any such form and let the sub-expression validator catch
	// genuinely malformed input.
	body := strings.TrimSpace(expr[len("CASE") : len(expr)-len("END")])
	if body == "" {
		return nil
	}

	var parts []string
	remaining := body

	// Detect simple CASE (comparison operand before first WHEN) vs generic.
	if !strings.HasPrefix(strings.ToUpper(remaining), "WHEN") {
		whenIdx := findCaseKeywordIdx(remaining, "WHEN")
		if whenIdx <= 0 {
			return nil
		}
		compExpr := strings.TrimSpace(remaining[:whenIdx])
		if compExpr != "" {
			parts = append(parts, compExpr)
		}
		remaining = strings.TrimSpace(remaining[whenIdx:])
	}

	for {
		if !strings.HasPrefix(strings.ToUpper(remaining), "WHEN") {
			break
		}
		remaining = strings.TrimSpace(remaining[len("WHEN"):])
		thenIdx := findCaseKeywordIdx(remaining, "THEN")
		if thenIdx < 0 {
			break
		}
		whenExpr := strings.TrimSpace(remaining[:thenIdx])
		if whenExpr != "" {
			parts = append(parts, whenExpr)
		}
		afterThen := strings.TrimSpace(remaining[thenIdx+len("THEN"):])

		nextWhenIdx := findCaseKeywordIdx(afterThen, "WHEN")
		elseIdx := findCaseKeywordIdx(afterThen, "ELSE")
		resultExpr := afterThen
		remaining = ""
		if nextWhenIdx >= 0 && (elseIdx < 0 || nextWhenIdx < elseIdx) {
			resultExpr = strings.TrimSpace(afterThen[:nextWhenIdx])
			remaining = strings.TrimSpace(afterThen[nextWhenIdx:])
		} else if elseIdx >= 0 {
			resultExpr = strings.TrimSpace(afterThen[:elseIdx])
			remaining = strings.TrimSpace(afterThen[elseIdx:])
		}
		if resultExpr != "" {
			parts = append(parts, resultExpr)
		}
	}

	if strings.HasPrefix(strings.ToUpper(remaining), "ELSE") {
		elseExpr := strings.TrimSpace(remaining[len("ELSE"):])
		if elseExpr != "" {
			parts = append(parts, elseExpr)
		}
	}

	return parts
}

// findCaseKeywordIdx returns the index of the first top-level (depth-0,
// outside strings) occurrence of keyword in raw. Used by CASE expression
// parsing to locate WHEN / THEN / ELSE keywords in the whitespace-free
// ANTLR token text.
func findCaseKeywordIdx(raw, keyword string) int {
	upper := strings.ToUpper(raw)
	keyword = strings.ToUpper(strings.TrimSpace(keyword))
	if keyword == "" || len(upper) < len(keyword) {
		return -1
	}
	depth := 0
	inSingle := false
	inDouble := false
	for i := 0; i <= len(upper)-len(keyword); i++ {
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
		case '(', '[', '{':
			depth++
			continue
		case ')', ']', '}':
			if depth > 0 {
				depth--
			}
			continue
		}
		if depth == 0 && strings.HasPrefix(upper[i:], keyword) {
			return i
		}
	}
	return -1
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

func extractPatternPropertyMapExpressions(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}

	mapBodies := make([]string, 0)
	depthBrace := 0
	inSingle := false
	inDouble := false
	start := -1
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
		case '{':
			if depthBrace == 0 {
				start = i + 1
			}
			depthBrace++
		case '}':
			if depthBrace == 0 {
				continue
			}
			depthBrace--
			if depthBrace == 0 && start >= 0 && start <= i {
				mapBodies = append(mapBodies, raw[start:i])
				start = -1
			}
		}
	}

	expressions := make([]string, 0)
	for _, body := range mapBodies {
		for _, pair := range splitTopLevelComma(body) {
			idx := strings.Index(pair, ":")
			if idx < 0 {
				continue
			}
			expr := strings.TrimSpace(pair[idx+1:])
			if expr != "" {
				expressions = append(expressions, expr)
			}
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
	return role == patternRoleVertex || role == patternRoleRel || role == patternRoleValue
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
	return role == patternRoleVertex || role == patternRolePath
}

func isInvalidTypeArgumentExpression(expr string, bound map[string]patternVarRole) bool {
	name, ok := typeSimpleIdentifierArg(expr)
	if !ok {
		return false
	}
	role, exists := bound[name]
	if !exists {
		return false
	}
	return role == patternRoleVertex || role == patternRolePath || role == patternRoleValue
}

func isInvalidLabelsArgumentExpression(expr string, bound map[string]patternVarRole) bool {
	name, ok := labelsSimpleIdentifierArg(expr)
	if !ok {
		return false
	}
	role, exists := bound[name]
	if !exists {
		return false
	}
	return role == patternRolePath
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

func typeSimpleIdentifierArg(expr string) (string, bool) {
	text := strings.TrimSpace(expr)
	if len(text) < len("type(")+1 || !strings.HasSuffix(text, ")") {
		return "", false
	}
	if !strings.EqualFold(text[:len("type(")], "type(") {
		return "", false
	}
	inner := strings.TrimSpace(text[len("type(") : len(text)-1])
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

func labelsSimpleIdentifierArg(expr string) (string, bool) {
	text := strings.TrimSpace(expr)
	if len(text) < len("labels(")+1 || !strings.HasSuffix(text, ")") {
		return "", false
	}
	if !strings.EqualFold(text[:len("labels(")], "labels(") {
		return "", false
	}
	inner := strings.TrimSpace(text[len("labels(") : len(text)-1])
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

func firstOverflowingDecimalIntegerLiteral(expr string) (string, bool) {
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
		signed := false
		if ch == '-' {
			if i+1 >= len(expr) || expr[i+1] < '0' || expr[i+1] > '9' || !hasUnaryMinusBeforeLiteral(expr, i+1) {
				continue
			}
			start = i
			signed = true
			i++
			ch = expr[i]
		}

		if ch < '0' || ch > '9' {
			continue
		}
		if !signed && hasUnaryMinusBeforeLiteral(expr, i) {
			continue
		}
		if !isNumericLiteralBoundaryBefore(expr, start) {
			continue
		}

		j := i
		for j < len(expr) && expr[j] >= '0' && expr[j] <= '9' {
			j++
		}
		if j < len(expr) && (expr[j] == '.' || expr[j] == 'e' || expr[j] == 'E') {
			i = j
			continue
		}
		if !isNumericLiteralBoundaryAfter(expr, j) {
			i = j
			continue
		}

		lit := expr[start:j]
		if _, err := strconv.ParseInt(lit, 10, 64); err != nil {
			if numErr, ok := err.(*strconv.NumError); ok && numErr.Err == strconv.ErrRange {
				return lit, true
			}
		}
		i = j
	}

	return "", false
}

func hasUndefinedInlineMapValueIdentifier(expr string, bound map[string]patternVarRole) bool {
	trimmed := strings.TrimSpace(expr)
	if len(trimmed) < 2 || trimmed[0] != '{' || trimmed[len(trimmed)-1] != '}' {
		return false
	}
	body := strings.TrimSpace(trimmed[1 : len(trimmed)-1])
	if body == "" {
		return false
	}
	for _, pair := range splitTopLevelComma(body) {
		parts := strings.SplitN(pair, ":", 2)
		if len(parts) != 2 {
			continue
		}
		valueExpr := strings.TrimSpace(parts[1])
		if valueExpr == "" {
			continue
		}
		ident, ok := simpleIdentifierExpression(valueExpr)
		if !ok || isCypherLiteralKeyword(ident) {
			continue
		}
		if _, exists := bound[ident]; !exists {
			return true
		}
	}
	return false
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
