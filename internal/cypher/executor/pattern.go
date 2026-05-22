package executor

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/paegun/vitaledge/internal/graph"
)

var anchoredOutPatternRE = regexp.MustCompile(`^\(([A-Za-z_][A-Za-z0-9_]*)(?::([A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)-\[:([A-Za-z_][A-Za-z0-9_]*)\]->\(([A-Za-z_][A-Za-z0-9_]*)\)$`)

type anchoredOutPattern struct {
	SourceVar           string
	SourceLabel         string
	SourcePropertiesRaw string
	SourceIDParam       string
	EdgeType            string
	TargetVar           string
}

func parseAnchoredOutPattern(raw string) (anchoredOutPattern, error) {
	normalized := strings.Join(strings.Fields(raw), "")
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
