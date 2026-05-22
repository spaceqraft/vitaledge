package executor

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/paegun/vitaledge/internal/graph"
)

var anchoredOutPatternRE = regexp.MustCompile(`^\(([A-Za-z_][A-Za-z0-9_]*)(?::([A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)-\[:([A-Za-z_][A-Za-z0-9_]*)\]->\(([A-Za-z_][A-Za-z0-9_]*)\)$`)
var nodePatternRE = regexp.MustCompile(`^\(([A-Za-z_][A-Za-z0-9_]*)(?::([A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)$`)

type anchoredOutPattern struct {
	SourceVar           string
	SourceLabel         string
	SourcePropertiesRaw string
	SourceIDParam       string
	EdgeType            string
	TargetVar           string
}

type nodePattern struct {
	Var           string
	AnyOfLabels   []string
	AllOfLabels   []string
	PropertiesRaw string
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
	labels := splitLabels(m[2])
	label := ""
	if len(labels) > 0 {
		label = labels[0]
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
	labels := splitLabels(m[2])
	pattern := nodePattern{
		Var:           m[1],
		PropertiesRaw: m[3],
	}
	if strings.Contains(m[2], "|") {
		labels = splitPipeLabels(m[2])
		pattern.AnyOfLabels = labels
		return pattern, nil
	}
	pattern.AllOfLabels = labels
	return pattern, nil
}

func splitPipeLabels(raw string) []string {
	parts := strings.Split(raw, "|")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		part = strings.TrimPrefix(part, ":")
		if part == "" {
			continue
		}
		out = append(out, part)
	}
	return out
}
