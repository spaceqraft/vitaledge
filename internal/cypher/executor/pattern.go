package executor

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/paegun/vitaledge/internal/graph"
)

var anchoredOutPatternRE = regexp.MustCompile(`^\(([A-Za-z_][A-Za-z0-9_]*)(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)-\[:([A-Za-z_][A-Za-z0-9_]*)\]->\(([A-Za-z_][A-Za-z0-9_]*)\)$`)
var vertexPatternRE = regexp.MustCompile(`^\((?:([A-Za-z_][A-Za-z0-9_]*))?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)$`)
var undirectedAdjacentPatternRE = regexp.MustCompile(`^\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)--\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)$`)
var directedAdjacentPatternRE = regexp.MustCompile(`^\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)-->\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)$`)
var reverseDirectedAdjacentPatternRE = regexp.MustCompile(`^\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)<--\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)$`)
var directedRelationshipPatternRE = regexp.MustCompile(`^\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)-\[([^\]]*)\]->\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)$`)
var reverseDirectedRelationshipPatternRE = regexp.MustCompile(`^\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)<-\[([^\]]*)\]-\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)$`)
var undirectedRelationshipPatternRE = regexp.MustCompile(`^\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)-\[([^\]]*)\]-\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)$`)
var directedVariableLengthRelationshipPatternRE = regexp.MustCompile(`^\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)-\[([^\]]*)\]->\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)$`)
var directedVariableLengthThenDirectedVariableLengthPatternRE = regexp.MustCompile(`^\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)-\[([^\]]*)\]->\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)-\[([^\]]*)\]->\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)$`)
var undirectedVariableLengthRelationshipPatternRE = regexp.MustCompile(`^\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)-\[([^\]]*)\]-\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)$`)
var directedRelationshipThenAdjacentPatternRE = regexp.MustCompile(`^\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)-\[([^\]]*)\]->\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)-->\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)$`)
var directedThenUndirectedRelationshipChainPatternRE = regexp.MustCompile(`^\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)-\[([^\]]*)\]->\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)-\[([^\]]*)\]-\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)$`)
var twoHopUndirectedRelationshipChainPatternRE = regexp.MustCompile(`^\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)-\[([^\]]*)\]-\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)-\[([^\]]*)\]-\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)$`)
var twoHopForwardChainPatternRE = regexp.MustCompile(`^\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)-\[([^\]]*)\]->\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)-\[([^\]]*)\]->\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)$`)
var twoHopConvergingChainPatternRE = regexp.MustCompile(`^\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)-\[([^\]]*)\]->\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)<-\[([^\]]*)\]-\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)$`)
var reverseRelationshipThenUndirectedVariableLengthPatternRE = regexp.MustCompile(`^\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)<-\[([^\]]*)\]-\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)-\[([^\]]*)\]-\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)$`)
var directedAdjacentThenVariableLengthChainPatternRE = regexp.MustCompile(`^\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)-->\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)-\[([^\]]*)\]->\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)$`)
var chainVertexSegmentRE = regexp.MustCompile(`^\((?:([A-Za-z_][A-Za-z0-9_]*)?)?(?::(!?[A-Za-z_][A-Za-z0-9_]*(?:(?::|\|:?)!?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)$`)
var identifierRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

type anchoredOutPattern struct {
	SourceVar           string
	SourceLabel         string
	SourcePropertiesRaw string
	SourceIDParam       string
	EdgeType            string
	TargetVar           string
}

type vertexPattern struct {
	Var            string
	AnyOfLabels    []string
	AllOfLabels    []string
	ExcludedLabels []string
	PropertiesRaw  string
}

type undirectedAdjacentPattern struct {
	Left  vertexPattern
	Right vertexPattern
}

type directedAdjacentPattern struct {
	Left  vertexPattern
	Right vertexPattern
}

type reverseDirectedAdjacentPattern struct {
	Left  vertexPattern
	Right vertexPattern
}

type directedRelationshipPattern struct {
	Left      vertexPattern
	Right     vertexPattern
	EdgeVar   string
	EdgeType  string
	EdgeAnyOf []string
	EdgeProps string
}

type reverseDirectedRelationshipPattern struct {
	Left      vertexPattern
	Right     vertexPattern
	EdgeVar   string
	EdgeType  string
	EdgeAnyOf []string
	EdgeProps string
}

type undirectedRelationshipPattern struct {
	Left      vertexPattern
	Right     vertexPattern
	EdgeVar   string
	EdgeType  string
	EdgeAnyOf []string
	EdgeProps string
}

type directedVariableLengthRelationshipPattern struct {
	Left      vertexPattern
	Right     vertexPattern
	EdgeVar   string
	EdgeType  string
	EdgeAnyOf []string
	EdgeProps string
	MinHops   int
	MaxHops   int
}

type directedVariableLengthThenDirectedVariableLengthPattern struct {
	Left            vertexPattern
	Mid             vertexPattern
	Right           vertexPattern
	FirstEdgeVar    string
	FirstEdgeType   string
	FirstEdgeAnyOf  []string
	FirstEdgeProps  string
	FirstMinHops    int
	FirstMaxHops    int
	SecondEdgeVar   string
	SecondEdgeType  string
	SecondEdgeAnyOf []string
	SecondEdgeProps string
	SecondMinHops   int
	SecondMaxHops   int
}

type undirectedVariableLengthRelationshipPattern struct {
	Left      vertexPattern
	Right     vertexPattern
	EdgeVar   string
	EdgeType  string
	EdgeAnyOf []string
	EdgeProps string
	MinHops   int
	MaxHops   int
}

type directedAdjacentThenVariableLengthPattern struct {
	Left    vertexPattern
	Mid     vertexPattern
	Right   vertexPattern
	EdgeVar string
}

type directedRelationshipThenAdjacentPattern struct {
	Left           vertexPattern
	Mid            vertexPattern
	Right          vertexPattern
	FirstEdgeVar   string
	FirstEdgeType  string
	FirstEdgeAnyOf []string
	FirstEdgeProps string
}

type twoHopDirectedChainPattern struct {
	Left            vertexPattern
	Mid             vertexPattern
	Right           vertexPattern
	FirstEdgeVar    string
	FirstEdgeType   string
	FirstEdgeAnyOf  []string
	FirstEdgeProps  string
	SecondForward   bool
	SecondEdgeVar   string
	SecondEdgeType  string
	SecondEdgeAnyOf []string
	SecondEdgeProps string
}

type twoHopUndirectedRelationshipChainPattern struct {
	Left            vertexPattern
	Mid             vertexPattern
	Right           vertexPattern
	FirstEdgeVar    string
	FirstEdgeType   string
	FirstEdgeAnyOf  []string
	FirstEdgeProps  string
	SecondEdgeVar   string
	SecondEdgeType  string
	SecondEdgeAnyOf []string
	SecondEdgeProps string
}

type directedThenUndirectedRelationshipChainPattern struct {
	Left            vertexPattern
	Mid             vertexPattern
	Right           vertexPattern
	FirstEdgeVar    string
	FirstEdgeType   string
	FirstEdgeAnyOf  []string
	FirstEdgeProps  string
	SecondEdgeVar   string
	SecondEdgeType  string
	SecondEdgeAnyOf []string
	SecondEdgeProps string
}

type reverseRelationshipThenUndirectedVariableLengthPattern struct {
	Left            vertexPattern
	Mid             vertexPattern
	Right           vertexPattern
	FirstEdgeVar    string
	FirstEdgeType   string
	FirstEdgeAnyOf  []string
	FirstEdgeProps  string
	SecondEdgeVar   string
	SecondEdgeType  string
	SecondEdgeAnyOf []string
	SecondEdgeProps string
	MinHops         int
	MaxHops         int
}

type mixedRelationshipChainSegment struct {
	Direction        string
	IsVariableLength bool
	EdgeVar          string
	EdgeType         string
	EdgeAnyOf        []string
	EdgeProps        string
	MinHops          int
	MaxHops          int
}

type mixedRelationshipChainPattern struct {
	Vertexes []vertexPattern
	Segments []mixedRelationshipChainSegment
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
	if !strings.Contains(props, ",") {
		parts := strings.SplitN(props, ":", 2)
		if len(parts) == 2 && strings.EqualFold(strings.TrimSpace(parts[0]), "id") {
			rhs := strings.TrimSpace(parts[1])
			if strings.HasPrefix(rhs, "$") {
				sourceIDParam = strings.TrimPrefix(rhs, "$")
			}
		}
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

func parseVertexPattern(raw string) (vertexPattern, error) {
	normalized := normalizeClauseBody(raw)
	m := vertexPatternRE.FindStringSubmatch(normalized)
	if len(m) != 4 {
		return vertexPattern{}, graph.NewError(
			graph.ErrKindUnsupported,
			fmt.Sprintf("pattern %q is not yet supported", raw),
			nil,
		)
	}
	allOf, anyOf, excluded := parseLabelFilters(m[2])
	pattern := vertexPattern{
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
		Left: vertexPattern{
			Var:            m[1],
			AllOfLabels:    leftAll,
			AnyOfLabels:    leftAny,
			ExcludedLabels: leftExcluded,
			PropertiesRaw:  m[3],
		},
		Right: vertexPattern{
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
		Left: vertexPattern{
			Var:            m[1],
			AllOfLabels:    leftAll,
			AnyOfLabels:    leftAny,
			ExcludedLabels: leftExcluded,
			PropertiesRaw:  m[3],
		},
		Right: vertexPattern{
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
		Left: vertexPattern{
			Var:            m[1],
			AllOfLabels:    leftAll,
			AnyOfLabels:    leftAny,
			ExcludedLabels: leftExcluded,
			PropertiesRaw:  m[3],
		},
		Right: vertexPattern{
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
		Left: vertexPattern{
			Var:            m[1],
			AllOfLabels:    leftAll,
			AnyOfLabels:    leftAny,
			ExcludedLabels: leftExcluded,
			PropertiesRaw:  m[3],
		},
		Right: vertexPattern{
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
		Left: vertexPattern{
			Var:            m[1],
			AllOfLabels:    leftAll,
			AnyOfLabels:    leftAny,
			ExcludedLabels: leftExcluded,
			PropertiesRaw:  m[3],
		},
		Right: vertexPattern{
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
		Left: vertexPattern{
			Var:            m[1],
			AllOfLabels:    leftAll,
			AnyOfLabels:    leftAny,
			ExcludedLabels: leftExcluded,
			PropertiesRaw:  m[3],
		},
		Right: vertexPattern{
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

func parseDirectedVariableLengthRelationshipPattern(raw string) (directedVariableLengthRelationshipPattern, error) {
	normalized := normalizeClauseBody(raw)
	m := directedVariableLengthRelationshipPatternRE.FindStringSubmatch(normalized)
	if len(m) != 8 {
		return directedVariableLengthRelationshipPattern{}, graph.NewError(
			graph.ErrKindUnsupported,
			fmt.Sprintf("pattern %q is not yet supported", raw),
			nil,
		)
	}

	leftAll, leftAny, leftExcluded := parseLabelFilters(m[2])
	rightAll, rightAny, rightExcluded := parseLabelFilters(m[6])
	edgeVar, edgeType, edgeAnyOf, edgeProps, minHops, maxHops, err := parseDetailedVariableLengthEdgePatternInner(m[4])
	if err != nil {
		return directedVariableLengthRelationshipPattern{}, err
	}

	return directedVariableLengthRelationshipPattern{
		Left: vertexPattern{
			Var:            m[1],
			AllOfLabels:    leftAll,
			AnyOfLabels:    leftAny,
			ExcludedLabels: leftExcluded,
			PropertiesRaw:  m[3],
		},
		Right: vertexPattern{
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
		MinHops:   minHops,
		MaxHops:   maxHops,
	}, nil
}

func parseDirectedVariableLengthThenDirectedVariableLengthPattern(raw string) (directedVariableLengthThenDirectedVariableLengthPattern, error) {
	normalized := normalizeClauseBody(raw)
	m := directedVariableLengthThenDirectedVariableLengthPatternRE.FindStringSubmatch(normalized)
	if len(m) != 12 {
		return directedVariableLengthThenDirectedVariableLengthPattern{}, graph.NewError(
			graph.ErrKindUnsupported,
			fmt.Sprintf("pattern %q is not yet supported", raw),
			nil,
		)
	}

	leftAll, leftAny, leftExcluded := parseLabelFilters(m[2])
	midAll, midAny, midExcluded := parseLabelFilters(m[6])
	rightAll, rightAny, rightExcluded := parseLabelFilters(m[10])

	firstVar, firstType, firstAnyOf, firstProps, firstMinHops, firstMaxHops, err := parseDetailedVariableLengthEdgePatternInner(m[4])
	if err != nil {
		return directedVariableLengthThenDirectedVariableLengthPattern{}, err
	}
	secondVar, secondType, secondAnyOf, secondProps, secondMinHops, secondMaxHops, err := parseDetailedVariableLengthEdgePatternInner(m[8])
	if err != nil {
		return directedVariableLengthThenDirectedVariableLengthPattern{}, err
	}

	return directedVariableLengthThenDirectedVariableLengthPattern{
		Left: vertexPattern{
			Var:            m[1],
			AllOfLabels:    leftAll,
			AnyOfLabels:    leftAny,
			ExcludedLabels: leftExcluded,
			PropertiesRaw:  m[3],
		},
		Mid: vertexPattern{
			Var:            m[5],
			AllOfLabels:    midAll,
			AnyOfLabels:    midAny,
			ExcludedLabels: midExcluded,
			PropertiesRaw:  m[7],
		},
		Right: vertexPattern{
			Var:            m[9],
			AllOfLabels:    rightAll,
			AnyOfLabels:    rightAny,
			ExcludedLabels: rightExcluded,
			PropertiesRaw:  m[11],
		},
		FirstEdgeVar:    firstVar,
		FirstEdgeType:   firstType,
		FirstEdgeAnyOf:  firstAnyOf,
		FirstEdgeProps:  firstProps,
		FirstMinHops:    firstMinHops,
		FirstMaxHops:    firstMaxHops,
		SecondEdgeVar:   secondVar,
		SecondEdgeType:  secondType,
		SecondEdgeAnyOf: secondAnyOf,
		SecondEdgeProps: secondProps,
		SecondMinHops:   secondMinHops,
		SecondMaxHops:   secondMaxHops,
	}, nil
}

func parseUndirectedVariableLengthRelationshipPattern(raw string) (undirectedVariableLengthRelationshipPattern, error) {
	normalized := normalizeClauseBody(raw)
	m := undirectedVariableLengthRelationshipPatternRE.FindStringSubmatch(normalized)
	if len(m) != 8 {
		return undirectedVariableLengthRelationshipPattern{}, graph.NewError(
			graph.ErrKindUnsupported,
			fmt.Sprintf("pattern %q is not yet supported", raw),
			nil,
		)
	}

	leftAll, leftAny, leftExcluded := parseLabelFilters(m[2])
	rightAll, rightAny, rightExcluded := parseLabelFilters(m[6])
	edgeVar, edgeType, edgeAnyOf, edgeProps, minHops, maxHops, err := parseDetailedVariableLengthEdgePatternInner(m[4])
	if err != nil {
		return undirectedVariableLengthRelationshipPattern{}, err
	}

	return undirectedVariableLengthRelationshipPattern{
		Left: vertexPattern{
			Var:            m[1],
			AllOfLabels:    leftAll,
			AnyOfLabels:    leftAny,
			ExcludedLabels: leftExcluded,
			PropertiesRaw:  m[3],
		},
		Right: vertexPattern{
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
		MinHops:   minHops,
		MaxHops:   maxHops,
	}, nil
}

func parseDirectedAdjacentThenVariableLengthPattern(raw string) (directedAdjacentThenVariableLengthPattern, error) {
	normalized := normalizeClauseBody(raw)
	m := directedAdjacentThenVariableLengthChainPatternRE.FindStringSubmatch(normalized)
	if len(m) != 11 {
		return directedAdjacentThenVariableLengthPattern{}, graph.NewError(
			graph.ErrKindUnsupported,
			fmt.Sprintf("pattern %q is not yet supported", raw),
			nil,
		)
	}

	leftAll, leftAny, leftExcluded := parseLabelFilters(m[2])
	midAll, midAny, midExcluded := parseLabelFilters(m[5])
	rightAll, rightAny, rightExcluded := parseLabelFilters(m[9])
	edgeVar, ok := parseVariableLengthEdgePatternInner(m[7])
	if !ok {
		return directedAdjacentThenVariableLengthPattern{}, graph.NewError(
			graph.ErrKindUnsupported,
			fmt.Sprintf("pattern %q is not yet supported", raw),
			nil,
		)
	}

	return directedAdjacentThenVariableLengthPattern{
		Left: vertexPattern{
			Var:            m[1],
			AllOfLabels:    leftAll,
			AnyOfLabels:    leftAny,
			ExcludedLabels: leftExcluded,
			PropertiesRaw:  m[3],
		},
		Mid: vertexPattern{
			Var:            m[4],
			AllOfLabels:    midAll,
			AnyOfLabels:    midAny,
			ExcludedLabels: midExcluded,
			PropertiesRaw:  m[6],
		},
		Right: vertexPattern{
			Var:            m[8],
			AllOfLabels:    rightAll,
			AnyOfLabels:    rightAny,
			ExcludedLabels: rightExcluded,
			PropertiesRaw:  m[10],
		},
		EdgeVar: edgeVar,
	}, nil
}

func parseDirectedRelationshipThenAdjacentPattern(raw string) (directedRelationshipThenAdjacentPattern, error) {
	normalized := normalizeClauseBody(raw)
	m := directedRelationshipThenAdjacentPatternRE.FindStringSubmatch(normalized)
	if len(m) != 11 {
		return directedRelationshipThenAdjacentPattern{}, graph.NewError(
			graph.ErrKindUnsupported,
			fmt.Sprintf("pattern %q is not yet supported", raw),
			nil,
		)
	}

	leftAll, leftAny, leftExcluded := parseLabelFilters(m[2])
	midAll, midAny, midExcluded := parseLabelFilters(m[6])
	rightAll, rightAny, rightExcluded := parseLabelFilters(m[9])

	edgeVar, edgeType, edgeAnyOf, edgeProps, err := parseEdgePatternInner(m[4])
	if err != nil {
		return directedRelationshipThenAdjacentPattern{}, err
	}

	return directedRelationshipThenAdjacentPattern{
		Left: vertexPattern{
			Var:            m[1],
			AllOfLabels:    leftAll,
			AnyOfLabels:    leftAny,
			ExcludedLabels: leftExcluded,
			PropertiesRaw:  m[3],
		},
		Mid: vertexPattern{
			Var:            m[5],
			AllOfLabels:    midAll,
			AnyOfLabels:    midAny,
			ExcludedLabels: midExcluded,
			PropertiesRaw:  m[7],
		},
		Right: vertexPattern{
			Var:            m[8],
			AllOfLabels:    rightAll,
			AnyOfLabels:    rightAny,
			ExcludedLabels: rightExcluded,
			PropertiesRaw:  m[10],
		},
		FirstEdgeVar:   edgeVar,
		FirstEdgeType:  edgeType,
		FirstEdgeAnyOf: edgeAnyOf,
		FirstEdgeProps: edgeProps,
	}, nil
}

func parseVariableLengthEdgePatternInner(raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "*" {
		return "", true
	}
	if strings.HasSuffix(raw, "*") {
		edgeVar := strings.TrimSpace(strings.TrimSuffix(raw, "*"))
		if identifierRE.MatchString(edgeVar) {
			return edgeVar, true
		}
	}
	return "", false
}

func parseDetailedVariableLengthEdgePatternInner(raw string) (edgeVar string, edgeType string, edgeAnyOf []string, edgeProps string, minHops int, maxHops int, err error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", nil, "", 0, 0, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("edge pattern [%s] is not yet supported", raw), nil)
	}

	if idx := strings.Index(raw, "{"); idx >= 0 {
		if !strings.HasSuffix(raw, "}") {
			return "", "", nil, "", 0, 0, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("edge pattern [%s] is not yet supported", raw), nil)
		}
		edgeProps = strings.TrimSpace(raw[idx+1 : len(raw)-1])
		raw = strings.TrimSpace(raw[:idx])
	}

	star := strings.Index(raw, "*")
	if star < 0 {
		return "", "", nil, "", 0, 0, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("edge pattern [%s] is not yet supported", raw), nil)
	}

	prefix := strings.TrimSpace(raw[:star])
	quantifier := strings.TrimSpace(raw[star+1:])
	edgeVar, edgeType, edgeAnyOf, _, err = parseEdgePatternInner(prefix)
	if err != nil {
		return "", "", nil, "", 0, 0, err
	}
	minHops, maxHops, err = parseVariableLengthBounds(quantifier)
	if err != nil {
		return "", "", nil, "", 0, 0, err
	}
	return edgeVar, edgeType, edgeAnyOf, edgeProps, minHops, maxHops, nil
}

func parseVariableLengthBounds(raw string) (int, int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 1, -1, nil
	}
	if !strings.Contains(raw, "..") {
		bound, err := parseNonNegativePatternBound(raw)
		if err != nil {
			return 0, 0, err
		}
		return bound, bound, nil
	}
	parts := strings.SplitN(raw, "..", 2)
	minHops := 1
	maxHops := -1
	if left := strings.TrimSpace(parts[0]); left != "" {
		bound, err := parseNonNegativePatternBound(left)
		if err != nil {
			return 0, 0, err
		}
		minHops = bound
	}
	if right := strings.TrimSpace(parts[1]); right != "" {
		bound, err := parseNonNegativePatternBound(right)
		if err != nil {
			return 0, 0, err
		}
		maxHops = bound
	}
	return minHops, maxHops, nil
}

func parseNonNegativePatternBound(raw string) (int, error) {
	if raw == "" {
		return 0, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("edge bound %q is not yet supported", raw), nil)
	}
	value := 0
	for _, ch := range raw {
		if ch < '0' || ch > '9' {
			return 0, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("edge bound %q is not yet supported", raw), nil)
		}
		value = value*10 + int(ch-'0')
	}
	return value, nil
}

func parseTwoHopDirectedChainPattern(raw string) (twoHopDirectedChainPattern, error) {
	normalized := normalizeClauseBody(raw)
	m := twoHopForwardChainPatternRE.FindStringSubmatch(normalized)
	secondForward := true
	if len(m) != 12 {
		m = twoHopConvergingChainPatternRE.FindStringSubmatch(normalized)
		secondForward = false
	}
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

	firstVar, firstType, firstAnyOf, firstProps, err := parseEdgePatternInner(m[4])
	if err != nil {
		return twoHopDirectedChainPattern{}, err
	}
	secondVar, secondType, secondAnyOf, secondProps, err := parseEdgePatternInner(m[8])
	if err != nil {
		return twoHopDirectedChainPattern{}, err
	}

	return twoHopDirectedChainPattern{
		Left: vertexPattern{
			Var:            m[1],
			AllOfLabels:    leftAll,
			AnyOfLabels:    leftAny,
			ExcludedLabels: leftExcluded,
			PropertiesRaw:  m[3],
		},
		Mid: vertexPattern{
			Var:            m[5],
			AllOfLabels:    midAll,
			AnyOfLabels:    midAny,
			ExcludedLabels: midExcluded,
			PropertiesRaw:  m[7],
		},
		Right: vertexPattern{
			Var:            m[9],
			AllOfLabels:    rightAll,
			AnyOfLabels:    rightAny,
			ExcludedLabels: rightExcluded,
			PropertiesRaw:  m[11],
		},
		FirstEdgeVar:    firstVar,
		FirstEdgeType:   firstType,
		FirstEdgeAnyOf:  firstAnyOf,
		FirstEdgeProps:  firstProps,
		SecondForward:   secondForward,
		SecondEdgeVar:   secondVar,
		SecondEdgeType:  secondType,
		SecondEdgeAnyOf: secondAnyOf,
		SecondEdgeProps: secondProps,
	}, nil
}

func parseTwoHopUndirectedRelationshipChainPattern(raw string) (twoHopUndirectedRelationshipChainPattern, error) {
	normalized := normalizeClauseBody(raw)
	m := twoHopUndirectedRelationshipChainPatternRE.FindStringSubmatch(normalized)
	if len(m) != 12 {
		return twoHopUndirectedRelationshipChainPattern{}, graph.NewError(
			graph.ErrKindUnsupported,
			fmt.Sprintf("pattern %q is not yet supported", raw),
			nil,
		)
	}

	leftAll, leftAny, leftExcluded := parseLabelFilters(m[2])
	midAll, midAny, midExcluded := parseLabelFilters(m[6])
	rightAll, rightAny, rightExcluded := parseLabelFilters(m[10])

	firstVar, firstType, firstAnyOf, firstProps, err := parseEdgePatternInner(m[4])
	if err != nil {
		return twoHopUndirectedRelationshipChainPattern{}, err
	}
	secondVar, secondType, secondAnyOf, secondProps, err := parseEdgePatternInner(m[8])
	if err != nil {
		return twoHopUndirectedRelationshipChainPattern{}, err
	}

	return twoHopUndirectedRelationshipChainPattern{
		Left: vertexPattern{
			Var:            m[1],
			AllOfLabels:    leftAll,
			AnyOfLabels:    leftAny,
			ExcludedLabels: leftExcluded,
			PropertiesRaw:  m[3],
		},
		Mid: vertexPattern{
			Var:            m[5],
			AllOfLabels:    midAll,
			AnyOfLabels:    midAny,
			ExcludedLabels: midExcluded,
			PropertiesRaw:  m[7],
		},
		Right: vertexPattern{
			Var:            m[9],
			AllOfLabels:    rightAll,
			AnyOfLabels:    rightAny,
			ExcludedLabels: rightExcluded,
			PropertiesRaw:  m[11],
		},
		FirstEdgeVar:    firstVar,
		FirstEdgeType:   firstType,
		FirstEdgeAnyOf:  firstAnyOf,
		FirstEdgeProps:  firstProps,
		SecondEdgeVar:   secondVar,
		SecondEdgeType:  secondType,
		SecondEdgeAnyOf: secondAnyOf,
		SecondEdgeProps: secondProps,
	}, nil
}

func parseDirectedThenUndirectedRelationshipChainPattern(raw string) (directedThenUndirectedRelationshipChainPattern, error) {
	normalized := normalizeClauseBody(raw)
	m := directedThenUndirectedRelationshipChainPatternRE.FindStringSubmatch(normalized)
	if len(m) != 12 {
		return directedThenUndirectedRelationshipChainPattern{}, graph.NewError(
			graph.ErrKindUnsupported,
			fmt.Sprintf("pattern %q is not yet supported", raw),
			nil,
		)
	}

	leftAll, leftAny, leftExcluded := parseLabelFilters(m[2])
	midAll, midAny, midExcluded := parseLabelFilters(m[6])
	rightAll, rightAny, rightExcluded := parseLabelFilters(m[10])

	firstVar, firstType, firstAnyOf, firstProps, err := parseEdgePatternInner(m[4])
	if err != nil {
		return directedThenUndirectedRelationshipChainPattern{}, err
	}
	secondVar, secondType, secondAnyOf, secondProps, err := parseEdgePatternInner(m[8])
	if err != nil {
		return directedThenUndirectedRelationshipChainPattern{}, err
	}

	return directedThenUndirectedRelationshipChainPattern{
		Left: vertexPattern{
			Var:            m[1],
			AllOfLabels:    leftAll,
			AnyOfLabels:    leftAny,
			ExcludedLabels: leftExcluded,
			PropertiesRaw:  m[3],
		},
		Mid: vertexPattern{
			Var:            m[5],
			AllOfLabels:    midAll,
			AnyOfLabels:    midAny,
			ExcludedLabels: midExcluded,
			PropertiesRaw:  m[7],
		},
		Right: vertexPattern{
			Var:            m[9],
			AllOfLabels:    rightAll,
			AnyOfLabels:    rightAny,
			ExcludedLabels: rightExcluded,
			PropertiesRaw:  m[11],
		},
		FirstEdgeVar:    firstVar,
		FirstEdgeType:   firstType,
		FirstEdgeAnyOf:  firstAnyOf,
		FirstEdgeProps:  firstProps,
		SecondEdgeVar:   secondVar,
		SecondEdgeType:  secondType,
		SecondEdgeAnyOf: secondAnyOf,
		SecondEdgeProps: secondProps,
	}, nil
}

func parseReverseRelationshipThenUndirectedVariableLengthPattern(raw string) (reverseRelationshipThenUndirectedVariableLengthPattern, error) {
	normalized := normalizeClauseBody(raw)
	m := reverseRelationshipThenUndirectedVariableLengthPatternRE.FindStringSubmatch(normalized)
	if len(m) != 12 {
		return reverseRelationshipThenUndirectedVariableLengthPattern{}, graph.NewError(
			graph.ErrKindUnsupported,
			fmt.Sprintf("pattern %q is not yet supported", raw),
			nil,
		)
	}

	leftAll, leftAny, leftExcluded := parseLabelFilters(m[2])
	midAll, midAny, midExcluded := parseLabelFilters(m[6])
	rightAll, rightAny, rightExcluded := parseLabelFilters(m[10])

	firstVar, firstType, firstAnyOf, firstProps, err := parseEdgePatternInner(m[4])
	if err != nil {
		return reverseRelationshipThenUndirectedVariableLengthPattern{}, err
	}
	secondVar, secondType, secondAnyOf, secondProps, minHops, maxHops, err := parseDetailedVariableLengthEdgePatternInner(m[8])
	if err != nil {
		return reverseRelationshipThenUndirectedVariableLengthPattern{}, err
	}

	return reverseRelationshipThenUndirectedVariableLengthPattern{
		Left: vertexPattern{
			Var:            m[1],
			AllOfLabels:    leftAll,
			AnyOfLabels:    leftAny,
			ExcludedLabels: leftExcluded,
			PropertiesRaw:  m[3],
		},
		Mid: vertexPattern{
			Var:            m[5],
			AllOfLabels:    midAll,
			AnyOfLabels:    midAny,
			ExcludedLabels: midExcluded,
			PropertiesRaw:  m[7],
		},
		Right: vertexPattern{
			Var:            m[9],
			AllOfLabels:    rightAll,
			AnyOfLabels:    rightAny,
			ExcludedLabels: rightExcluded,
			PropertiesRaw:  m[11],
		},
		FirstEdgeVar:    firstVar,
		FirstEdgeType:   firstType,
		FirstEdgeAnyOf:  firstAnyOf,
		FirstEdgeProps:  firstProps,
		SecondEdgeVar:   secondVar,
		SecondEdgeType:  secondType,
		SecondEdgeAnyOf: secondAnyOf,
		SecondEdgeProps: secondProps,
		MinHops:         minHops,
		MaxHops:         maxHops,
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
		typeName = strings.TrimPrefix(typeName, ":")
		if !identifierRE.MatchString(typeName) {
			return "", nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("edge type %q is not yet supported", raw), nil)
		}
		return typeName, nil, nil
	}
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		typeName := strings.TrimSpace(part)
		typeName = strings.TrimPrefix(typeName, ":")
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

// ──────────────────────────────────────────────────────────────────────────
// Multi-hop adjacent chain pattern (no explicit edge types, 2+ hops)
// Handles patterns like:
//   (n)-->(k)<--(n)          forward+reverse
//   (a:Label)<--(:B)--()     reverse+undirected
//   (n)-->(m)--(o)           forward+undirected
//   (n)-->(m)--(o)--(p)      three hops
//   (n)<-->(k)<-->(n)        bidirected (treated as undirected each hop)
// ──────────────────────────────────────────────────────────────────────────

type multiHopAdjacentChainHop struct {
	Direction string // "forward", "reverse", or "undirected"
	Vertex    vertexPattern
}

type multiHopAdjacentChainPattern struct {
	StartVertex vertexPattern
	Hops        []multiHopAdjacentChainHop // len >= 2
}

// consumeVertexSegment reads the leading "(…)" vertex token from s, returning
// (vertexString, remainder, ok).  Handles nested parens (for props).
func consumeVertexSegment(s string) (string, string, bool) {
	if len(s) == 0 || s[0] != '(' {
		return "", "", false
	}
	depth := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return s[:i+1], s[i+1:], true
			}
		}
	}
	return "", "", false
}

// consumeAdjacentArrow reads one of -->, <--, --, <--> from the front of s.
// Returns false when the next token is a bracketed relationship [-…-] or
// anything else.
func consumeAdjacentArrow(s string) (string, string, bool) {
	// Check longest prefixes first so "<-->" doesn't get eaten as "<--".
	if strings.HasPrefix(s, "<-->") {
		return "undirected", s[4:], true
	}
	if strings.HasPrefix(s, "-->") {
		return "forward", s[3:], true
	}
	if strings.HasPrefix(s, "<--") {
		return "reverse", s[3:], true
	}
	// "--" is undirected; reject "--[" (explicit relationship type).
	if strings.HasPrefix(s, "--") && (len(s) < 3 || s[2] != '[') {
		return "undirected", s[2:], true
	}
	return "", "", false
}

// parseChainVertexSegment parses a single "(…)" vertex token that may have an
// anonymous variable (unlike parseVertexPattern which requires a named var).
func parseChainVertexSegment(raw string) (vertexPattern, error) {
	normalized := normalizeClauseBody(raw)
	m := chainVertexSegmentRE.FindStringSubmatch(normalized)
	if len(m) != 4 {
		return vertexPattern{}, fmt.Errorf("not a vertex segment: %q", raw)
	}
	allOf, anyOf, excluded := parseLabelFilters(m[2])
	return vertexPattern{
		Var:            m[1],
		AllOfLabels:    allOf,
		AnyOfLabels:    anyOf,
		ExcludedLabels: excluded,
		PropertiesRaw:  m[3],
	}, nil
}

// parseMultiHopAdjacentChainPattern parses any adjacent chain with 2 or more
// hops and no explicit relationship brackets.  Returns an error if the string
// does not fit (used as a try-parse in applyMatchClause).
func parseMultiHopAdjacentChainPattern(raw string) (multiHopAdjacentChainPattern, error) {
	normalized := normalizeClauseBody(raw)

	vertexStr, rest, ok := consumeVertexSegment(normalized)
	if !ok {
		return multiHopAdjacentChainPattern{}, fmt.Errorf("no leading vertex in %q", raw)
	}
	startVertex, err := parseChainVertexSegment(vertexStr)
	if err != nil {
		return multiHopAdjacentChainPattern{}, err
	}

	s := rest
	var hops []multiHopAdjacentChainHop
	for {
		dir, afterArrow, arrowOK := consumeAdjacentArrow(s)
		if !arrowOK {
			break
		}
		hopStr, afterVertex, vertexOK := consumeVertexSegment(afterArrow)
		if !vertexOK {
			break
		}
		hopVertex, err := parseChainVertexSegment(hopStr)
		if err != nil {
			return multiHopAdjacentChainPattern{}, err
		}
		hops = append(hops, multiHopAdjacentChainHop{Direction: dir, Vertex: hopVertex})
		s = afterVertex
	}

	if len(hops) < 2 || s != "" {
		return multiHopAdjacentChainPattern{}, fmt.Errorf("not a multi-hop adjacent chain: %q (hops=%d, trailing=%q)", raw, len(hops), s)
	}
	return multiHopAdjacentChainPattern{StartVertex: startVertex, Hops: hops}, nil
}

func parseMixedRelationshipChainPattern(raw string) (mixedRelationshipChainPattern, error) {
	normalized := normalizeClauseBody(raw)
	firstVertex, next, ok := consumeVertexSegment(normalized)
	if !ok {
		return mixedRelationshipChainPattern{}, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("pattern %q is not yet supported", raw), nil)
	}
	startVertex, err := parseChainVertexSegment(firstVertex)
	if err != nil {
		return mixedRelationshipChainPattern{}, err
	}

	vertexes := []vertexPattern{startVertex}
	segments := make([]mixedRelationshipChainSegment, 0)
	s := next
	for len(s) > 0 {
		segmentRaw, afterSegment, segmentOK := consumeRelationshipSegment(s)
		segment := mixedRelationshipChainSegment{}
		if segmentOK {
			var err error
			segment, err = parseMixedRelationshipSegment(segmentRaw)
			if err != nil {
				return mixedRelationshipChainPattern{}, err
			}
		} else {
			direction, afterArrow, arrowOK := consumeAdjacentArrow(s)
			if !arrowOK {
				return mixedRelationshipChainPattern{}, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("pattern %q is not yet supported", raw), nil)
			}
			segment = mixedRelationshipChainSegment{Direction: direction}
			afterSegment = afterArrow
		}
		nextVertexRaw, afterVertex, vertexOK := consumeVertexSegment(afterSegment)
		if !vertexOK {
			return mixedRelationshipChainPattern{}, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("pattern %q is not yet supported", raw), nil)
		}
		nextVertex, err := parseChainVertexSegment(nextVertexRaw)
		if err != nil {
			return mixedRelationshipChainPattern{}, err
		}
		segments = append(segments, segment)
		vertexes = append(vertexes, nextVertex)
		s = afterVertex
	}

	if len(segments) < 2 {
		return mixedRelationshipChainPattern{}, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("pattern %q is not yet supported", raw), nil)
	}

	return mixedRelationshipChainPattern{Vertexes: vertexes, Segments: segments}, nil
}

func consumeRelationshipSegment(s string) (string, string, bool) {
	if strings.HasPrefix(s, "<-[") {
		return consumeBracketedRelationshipSegment(s, false)
	}
	if strings.HasPrefix(s, "-[") {
		return consumeBracketedRelationshipSegment(s, true)
	}
	return "", "", false
}

func consumeBracketedRelationshipSegment(s string, forward bool) (string, string, bool) {
	open := strings.IndexByte(s, '[')
	if open < 0 {
		return "", "", false
	}
	depth := 0
	inSingle := false
	inDouble := false
	for i := open; i < len(s); i++ {
		ch := s[i]
		if inSingle {
			if ch == '\'' {
				if i+1 < len(s) && s[i+1] == '\'' {
					i++
					continue
				}
				inSingle = false
			}
			continue
		}
		if inDouble {
			if ch == '\\' {
				i++
				continue
			}
			if ch == '"' {
				inDouble = false
			}
			continue
		}
		switch ch {
		case '\'':
			inSingle = true
		case '"':
			inDouble = true
		case '[':
			depth++
		case ']':
			depth--
			if depth == 0 {
				if forward {
					if i+2 < len(s) && s[i+1] == '-' && s[i+2] == '>' {
						return s[:i+3], s[i+3:], true
					}
					if i+1 < len(s) && s[i+1] == '-' {
						return s[:i+2], s[i+2:], true
					}
					return "", "", false
				}
				if i+2 < len(s) && s[i+1] == '-' && s[i+2] == '>' {
					return s[:i+3], s[i+3:], true
				}
				if i+1 < len(s) && s[i+1] == '-' {
					return s[:i+2], s[i+2:], true
				}
				return "", "", false
			}
		}
	}
	return "", "", false
}

func parseMixedRelationshipSegment(raw string) (mixedRelationshipChainSegment, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return mixedRelationshipChainSegment{}, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("edge pattern [%s] is not yet supported", raw), nil)
	}
	forward := strings.HasPrefix(raw, "-[")
	reverse := strings.HasPrefix(raw, "<-[")
	if !forward && !reverse {
		return mixedRelationshipChainSegment{}, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("edge pattern [%s] is not yet supported", raw), nil)
	}
	open := strings.IndexByte(raw, '[')
	close := strings.LastIndexByte(raw, ']')
	if open < 0 || close <= open {
		return mixedRelationshipChainSegment{}, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("edge pattern [%s] is not yet supported", raw), nil)
	}
	inner := strings.TrimSpace(raw[open+1 : close])
	segment := mixedRelationshipChainSegment{Direction: "forward"}
	if reverse {
		segment.Direction = "reverse"
		if strings.HasSuffix(raw, "->") {
			segment.Direction = "undirected"
		}
	} else if strings.HasSuffix(raw, "-") {
		segment.Direction = "undirected"
	}
	if strings.Contains(inner, "*") {
		edgeVar, edgeType, edgeAnyOf, edgeProps, minHops, maxHops, err := parseDetailedVariableLengthEdgePatternInner(inner)
		if err != nil {
			return mixedRelationshipChainSegment{}, err
		}
		segment.IsVariableLength = true
		segment.EdgeVar = edgeVar
		segment.EdgeType = edgeType
		segment.EdgeAnyOf = edgeAnyOf
		segment.EdgeProps = edgeProps
		segment.MinHops = minHops
		segment.MaxHops = maxHops
		return segment, nil
	}
	edgeVar, edgeType, edgeAnyOf, edgeProps, err := parseEdgePatternInner(inner)
	if err != nil {
		return mixedRelationshipChainSegment{}, err
	}
	segment.EdgeVar = edgeVar
	segment.EdgeType = edgeType
	segment.EdgeAnyOf = edgeAnyOf
	segment.EdgeProps = edgeProps
	return segment, nil
}
