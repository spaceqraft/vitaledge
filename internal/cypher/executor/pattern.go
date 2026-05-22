package executor

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/paegun/vitaledge/internal/graph"
)

var anchoredOutPatternRE = regexp.MustCompile(`^\(([A-Za-z_][A-Za-z0-9_]*)(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)-\[:([A-Za-z_][A-Za-z0-9_]*)\]->\(([A-Za-z_][A-Za-z0-9_]*)\)$`)
var nodePatternRE = regexp.MustCompile(`^\(([A-Za-z_][A-Za-z0-9_]*)(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)$`)

type anchoredOutPattern struct {
	SourceVar           string
	SourceLabel         string
	SourcePropertiesRaw string
	SourceIDParam       string
	EdgeType            string
	TargetVar           string
}

type nodePattern struct {
	Var            string
	AnyOfLabels    []string
	AllOfLabels    []string
	ExcludedLabels []string
	PropertiesRaw  string
}

func parseAnchoredOutPattern(raw string) (anchoredOutPattern, error) {
	normalized := normalizeClauseBody(raw)
	m := anchoredOutPatternRE.FindStringSubmatch(normalized)
	if len(m) != 6 {
		return anchoredOutPattern{}, graph.NewError(
			graph.ErrKindUnsupported,
			fmt.Sprintf("pattern %q is not yet supported; expected (src[:Label]{prop:$value})-[:TYPE]->(dst)", raw),
			nil,
		)
	}
	allOf, _, _ := parseLabelFilters(m[2])
	label := ""
	if len(allOf) > 0 {
		label = allOf[0]
	}
	props := m[3]
	sourceIDParam := ""
	if strings.HasPrefix(props, "id:$") && !strings.Contains(props, ",") {
		sourceIDParam = strings.TrimPrefix(props, "id:$")
	}
	return anchoredOutPattern{
		SourceVar:           m[1],
		SourceLabel:         label,
		SourcePropertiesRaw: props,
		SourceIDParam:       sourceIDParam,
		EdgeType:            m[4],
		TargetVar:           m[5],
	}, nil
}

func parseNodePattern(raw string) (nodePattern, error) {
	normalized := normalizeClauseBody(raw)
	m := nodePatternRE.FindStringSubmatch(normalized)
	if len(m) != 4 {
		return nodePattern{}, graph.NewError(
			graph.ErrKindUnsupported,
			fmt.Sprintf("pattern %q is not yet supported", raw),
			nil,
		)
	}
	allOf, anyOf, excluded := parseLabelFilters(m[2])
	pattern := nodePattern{
		Var:            m[1],
		AllOfLabels:    allOf,
		AnyOfLabels:    anyOf,
		ExcludedLabels: excluded,
		PropertiesRaw:  m[3],
	}
	return pattern, nil
}

func parseLabelFilters(raw string) (allOf []string, anyOf []string, excluded []string) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil, nil
	}

	if strings.Contains(raw, "|") {
		parts := strings.Split(raw, "|")
		for _, part := range parts {
			part = strings.TrimSpace(part)
			part = strings.TrimPrefix(part, ":")
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			if strings.HasPrefix(part, "!") {
				excluded = append(excluded, strings.TrimPrefix(part, "!"))
				continue
			}
			anyOf = append(anyOf, part)
		}
		return nil, anyOf, excluded
	}

	parts := strings.Split(raw, ":")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if strings.HasPrefix(part, "!") {
			excluded = append(excluded, strings.TrimPrefix(part, "!"))
			continue
		}
		allOf = append(allOf, part)
	}
	return allOf, nil, excluded
}
