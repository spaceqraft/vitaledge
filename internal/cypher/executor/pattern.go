package executor

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/paegun/vitaledge/internal/graph"
)

var anchoredOutPatternRE = regexp.MustCompile(`^\(([A-Za-z_][A-Za-z0-9_]*)(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)-\[:([A-Za-z_][A-Za-z0-9_]*)\]->\(([A-Za-z_][A-Za-z0-9_]*)\)$`)
var nodePatternRE = regexp.MustCompile(`^\(([A-Za-z_][A-Za-z0-9_]*)(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)$`)
var undirectedAdjacentPatternRE = regexp.MustCompile(`^\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)--\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)$`)
var directedAdjacentPatternRE = regexp.MustCompile(`^\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)-->\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)$`)
var reverseDirectedAdjacentPatternRE = regexp.MustCompile(`^\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)<--\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)$`)
var directedRelationshipPatternRE = regexp.MustCompile(`^\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)-\[([^\]]*)\]->\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)$`)
var reverseDirectedRelationshipPatternRE = regexp.MustCompile(`^\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)<-\[([^\]]*)\]-\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)$`)
var undirectedRelationshipPatternRE = regexp.MustCompile(`^\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)-\[([^\]]*)\]-\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)$`)
var twoHopDirectedChainPatternRE = regexp.MustCompile(`^\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)-\[([^\]]*)\]->\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)<-\[([^\]]*)\]-\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)$`)
var identifierRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

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

type undirectedAdjacentPattern struct {
	Left  nodePattern
	Right nodePattern
}

type directedAdjacentPattern struct {
	Left  nodePattern
	Right nodePattern
}

type reverseDirectedAdjacentPattern struct {
	Left  nodePattern
	Right nodePattern
}

type directedRelationshipPattern struct {
	Left      nodePattern
	Right     nodePattern
	EdgeVar   string
	EdgeType  string
	EdgeAnyOf []string
	EdgeProps string
}

type reverseDirectedRelationshipPattern struct {
	Left      nodePattern
	Right     nodePattern
	EdgeVar   string
	EdgeType  string
	EdgeAnyOf []string
	EdgeProps string
}

type undirectedRelationshipPattern struct {
	Left      nodePattern
	Right     nodePattern
	EdgeVar   string
	EdgeType  string
	EdgeAnyOf []string
	EdgeProps string
}

type twoHopDirectedChainPattern struct {
	Left            nodePattern
	Mid             nodePattern
	Right           nodePattern
	FirstEdgeType   string
	FirstEdgeAnyOf  []string
	FirstEdgeProps  string
	SecondEdgeType  string
	SecondEdgeAnyOf []string
	SecondEdgeProps string
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

func parseUndirectedAdjacentPattern(raw string) (undirectedAdjacentPattern, error) {
	normalized := normalizeClauseBody(raw)
	m := undirectedAdjacentPatternRE.FindStringSubmatch(normalized)
	if len(m) != 7 {
		return undirectedAdjacentPattern{}, graph.NewError(
			graph.ErrKindUnsupported,
			fmt.Sprintf("pattern %q is not yet supported", raw),
			nil,
		)
	}

	leftAll, leftAny, leftExcluded := parseLabelFilters(m[2])
	rightAll, rightAny, rightExcluded := parseLabelFilters(m[5])

	return undirectedAdjacentPattern{
		Left: nodePattern{
			Var:            m[1],
			AllOfLabels:    leftAll,
			AnyOfLabels:    leftAny,
			ExcludedLabels: leftExcluded,
			PropertiesRaw:  m[3],
		},
		Right: nodePattern{
			Var:            m[4],
			AllOfLabels:    rightAll,
			AnyOfLabels:    rightAny,
			ExcludedLabels: rightExcluded,
			PropertiesRaw:  m[6],
		},
	}, nil
}

func parseDirectedAdjacentPattern(raw string) (directedAdjacentPattern, error) {
	normalized := normalizeClauseBody(raw)
	m := directedAdjacentPatternRE.FindStringSubmatch(normalized)
	if len(m) != 7 {
		return directedAdjacentPattern{}, graph.NewError(
			graph.ErrKindUnsupported,
			fmt.Sprintf("pattern %q is not yet supported", raw),
			nil,
		)
	}

	leftAll, leftAny, leftExcluded := parseLabelFilters(m[2])
	rightAll, rightAny, rightExcluded := parseLabelFilters(m[5])

	return directedAdjacentPattern{
		Left: nodePattern{
			Var:            m[1],
			AllOfLabels:    leftAll,
			AnyOfLabels:    leftAny,
			ExcludedLabels: leftExcluded,
			PropertiesRaw:  m[3],
		},
		Right: nodePattern{
			Var:            m[4],
			AllOfLabels:    rightAll,
			AnyOfLabels:    rightAny,
			ExcludedLabels: rightExcluded,
			PropertiesRaw:  m[6],
		},
	}, nil
}

func parseReverseDirectedAdjacentPattern(raw string) (reverseDirectedAdjacentPattern, error) {
	normalized := normalizeClauseBody(raw)
	m := reverseDirectedAdjacentPatternRE.FindStringSubmatch(normalized)
	if len(m) != 7 {
		return reverseDirectedAdjacentPattern{}, graph.NewError(
			graph.ErrKindUnsupported,
			fmt.Sprintf("pattern %q is not yet supported", raw),
			nil,
		)
	}

	leftAll, leftAny, leftExcluded := parseLabelFilters(m[2])
	rightAll, rightAny, rightExcluded := parseLabelFilters(m[5])

	return reverseDirectedAdjacentPattern{
		Left: nodePattern{
			Var:            m[1],
			AllOfLabels:    leftAll,
			AnyOfLabels:    leftAny,
			ExcludedLabels: leftExcluded,
			PropertiesRaw:  m[3],
		},
		Right: nodePattern{
			Var:            m[4],
			AllOfLabels:    rightAll,
			AnyOfLabels:    rightAny,
			ExcludedLabels: rightExcluded,
			PropertiesRaw:  m[6],
		},
	}, nil
}

func parseDirectedRelationshipPattern(raw string) (directedRelationshipPattern, error) {
	normalized := normalizeClauseBody(raw)
	m := directedRelationshipPatternRE.FindStringSubmatch(normalized)
	if len(m) != 8 {
		return directedRelationshipPattern{}, graph.NewError(
			graph.ErrKindUnsupported,
			fmt.Sprintf("pattern %q is not yet supported", raw),
			nil,
		)
	}

	leftAll, leftAny, leftExcluded := parseLabelFilters(m[2])
	rightAll, rightAny, rightExcluded := parseLabelFilters(m[6])

	edgeVar, edgeType, edgeAnyOf, edgeProps, err := parseEdgePatternInner(m[4])
	if err != nil {
		return directedRelationshipPattern{}, err
	}

	return directedRelationshipPattern{
		Left: nodePattern{
			Var:            m[1],
			AllOfLabels:    leftAll,
			AnyOfLabels:    leftAny,
			ExcludedLabels: leftExcluded,
			PropertiesRaw:  m[3],
		},
		Right: nodePattern{
			Var:            m[5],
			AllOfLabels:    rightAll,
			AnyOfLabels:    rightAny,
			ExcludedLabels: rightExcluded,
			PropertiesRaw:  m[7],
		},
		EdgeVar:   edgeVar,
		EdgeType:  edgeType,
		EdgeAnyOf: edgeAnyOf,
		EdgeProps: edgeProps,
	}, nil
}

func parseReverseDirectedRelationshipPattern(raw string) (reverseDirectedRelationshipPattern, error) {
	normalized := normalizeClauseBody(raw)
	m := reverseDirectedRelationshipPatternRE.FindStringSubmatch(normalized)
	if len(m) != 8 {
		return reverseDirectedRelationshipPattern{}, graph.NewError(
			graph.ErrKindUnsupported,
			fmt.Sprintf("pattern %q is not yet supported", raw),
			nil,
		)
	}

	leftAll, leftAny, leftExcluded := parseLabelFilters(m[2])
	rightAll, rightAny, rightExcluded := parseLabelFilters(m[6])

	edgeVar, edgeType, edgeAnyOf, edgeProps, err := parseEdgePatternInner(m[4])
	if err != nil {
		return reverseDirectedRelationshipPattern{}, err
	}

	return reverseDirectedRelationshipPattern{
		Left: nodePattern{
			Var:            m[1],
			AllOfLabels:    leftAll,
			AnyOfLabels:    leftAny,
			ExcludedLabels: leftExcluded,
			PropertiesRaw:  m[3],
		},
		Right: nodePattern{
			Var:            m[5],
			AllOfLabels:    rightAll,
			AnyOfLabels:    rightAny,
			ExcludedLabels: rightExcluded,
			PropertiesRaw:  m[7],
		},
		EdgeVar:   edgeVar,
		EdgeType:  edgeType,
		EdgeAnyOf: edgeAnyOf,
		EdgeProps: edgeProps,
	}, nil
}

func parseUndirectedRelationshipPattern(raw string) (undirectedRelationshipPattern, error) {
	normalized := normalizeClauseBody(raw)
	m := undirectedRelationshipPatternRE.FindStringSubmatch(normalized)
	if len(m) != 8 {
		return undirectedRelationshipPattern{}, graph.NewError(
			graph.ErrKindUnsupported,
			fmt.Sprintf("pattern %q is not yet supported", raw),
			nil,
		)
	}

	leftAll, leftAny, leftExcluded := parseLabelFilters(m[2])
	rightAll, rightAny, rightExcluded := parseLabelFilters(m[6])

	edgeVar, edgeType, edgeAnyOf, edgeProps, err := parseEdgePatternInner(m[4])
	if err != nil {
		return undirectedRelationshipPattern{}, err
	}

	return undirectedRelationshipPattern{
		Left: nodePattern{
			Var:            m[1],
			AllOfLabels:    leftAll,
			AnyOfLabels:    leftAny,
			ExcludedLabels: leftExcluded,
			PropertiesRaw:  m[3],
		},
		Right: nodePattern{
			Var:            m[5],
			AllOfLabels:    rightAll,
			AnyOfLabels:    rightAny,
			ExcludedLabels: rightExcluded,
			PropertiesRaw:  m[7],
		},
		EdgeVar:   edgeVar,
		EdgeType:  edgeType,
		EdgeAnyOf: edgeAnyOf,
		EdgeProps: edgeProps,
	}, nil
}

func parseTwoHopDirectedChainPattern(raw string) (twoHopDirectedChainPattern, error) {
	normalized := normalizeClauseBody(raw)
	m := twoHopDirectedChainPatternRE.FindStringSubmatch(normalized)
	if len(m) != 12 {
		return twoHopDirectedChainPattern{}, graph.NewError(
			graph.ErrKindUnsupported,
			fmt.Sprintf("pattern %q is not yet supported", raw),
			nil,
		)
	}

	leftAll, leftAny, leftExcluded := parseLabelFilters(m[2])
	midAll, midAny, midExcluded := parseLabelFilters(m[6])
	rightAll, rightAny, rightExcluded := parseLabelFilters(m[10])

	_, firstType, firstAnyOf, firstProps, err := parseEdgePatternInner(m[4])
	if err != nil {
		return twoHopDirectedChainPattern{}, err
	}
	_, secondType, secondAnyOf, secondProps, err := parseEdgePatternInner(m[8])
	if err != nil {
		return twoHopDirectedChainPattern{}, err
	}

	return twoHopDirectedChainPattern{
		Left: nodePattern{
			Var:            m[1],
			AllOfLabels:    leftAll,
			AnyOfLabels:    leftAny,
			ExcludedLabels: leftExcluded,
			PropertiesRaw:  m[3],
		},
		Mid: nodePattern{
			Var:            m[5],
			AllOfLabels:    midAll,
			AnyOfLabels:    midAny,
			ExcludedLabels: midExcluded,
			PropertiesRaw:  m[7],
		},
		Right: nodePattern{
			Var:            m[9],
			AllOfLabels:    rightAll,
			AnyOfLabels:    rightAny,
			ExcludedLabels: rightExcluded,
			PropertiesRaw:  m[11],
		},
		FirstEdgeType:   firstType,
		FirstEdgeAnyOf:  firstAnyOf,
		FirstEdgeProps:  firstProps,
		SecondEdgeType:  secondType,
		SecondEdgeAnyOf: secondAnyOf,
		SecondEdgeProps: secondProps,
	}, nil
}

func parseEdgePatternInner(raw string) (edgeVar string, edgeType string, edgeAnyOf []string, edgeProps string, err error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", nil, "", nil
	}

	if idx := strings.Index(raw, "{"); idx >= 0 {
		if !strings.HasSuffix(raw, "}") {
			return "", "", nil, "", graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("edge pattern [%s] is not yet supported", raw), nil)
		}
		edgeProps = strings.TrimSpace(raw[idx+1 : len(raw)-1])
		raw = strings.TrimSpace(raw[:idx])
	}

	if strings.HasPrefix(raw, ":") {
		typeRaw := strings.TrimSpace(strings.TrimPrefix(raw, ":"))
		edgeType, edgeAnyOf, err = parseEdgeTypeFilter(typeRaw)
		if err != nil {
			return "", "", nil, "", err
		}
		return "", edgeType, edgeAnyOf, edgeProps, nil
	}
	parts := strings.Split(raw, ":")
	if len(parts) == 1 {
		edgeVar = strings.TrimSpace(parts[0])
		if edgeVar == "" {
			return "", "", nil, edgeProps, nil
		}
		if !identifierRE.MatchString(edgeVar) {
			return "", "", nil, "", graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("edge pattern [%s] is not yet supported", raw), nil)
		}
		return edgeVar, "", nil, edgeProps, nil
	}
	if len(parts) == 2 {
		edgeVar = strings.TrimSpace(parts[0])
		typeRaw := strings.TrimSpace(parts[1])
		if !identifierRE.MatchString(edgeVar) {
			return "", "", nil, "", graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("edge pattern [%s] is not yet supported", raw), nil)
		}
		edgeType, edgeAnyOf, err = parseEdgeTypeFilter(typeRaw)
		if err != nil {
			return "", "", nil, "", err
		}
		return edgeVar, edgeType, edgeAnyOf, edgeProps, nil
	}
	return "", "", nil, "", graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("edge pattern [%s] is not yet supported", raw), nil)
}

func parseEdgeTypeFilter(raw string) (edgeType string, edgeAnyOf []string, err error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil, nil
	}
	parts := strings.Split(raw, "|")
	if len(parts) == 1 {
		typeName := strings.TrimSpace(parts[0])
		if !identifierRE.MatchString(typeName) {
			return "", nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("edge type %q is not yet supported", raw), nil)
		}
		return typeName, nil, nil
	}
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		typeName := strings.TrimSpace(part)
		if !identifierRE.MatchString(typeName) {
			return "", nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("edge type %q is not yet supported", raw), nil)
		}
		out = append(out, typeName)
	}
	return "", out, nil
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
