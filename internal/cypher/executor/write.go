package executor

import (
	"bytes"
	"container/heap"
	"context"
	crand "crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode"

	"golang.org/x/text/unicode/norm"

	"github.com/paegun/vitaledge/internal/cypher/ast"
	"github.com/paegun/vitaledge/internal/cypher/parser"
	"github.com/paegun/vitaledge/internal/graph"
)

var (
	createVertexPatternRE            = regexp.MustCompile(`^\((?:([A-Za-z_][A-Za-z0-9_]*))?(?::([A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)$`)
	createEdgePatternRE              = regexp.MustCompile(`^\((?:([A-Za-z_][A-Za-z0-9_]*))?(?::([A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)-\[(?:([A-Za-z_][A-Za-z0-9_]*))?(?::([A-Za-z_][A-Za-z0-9_]*(?:\|:?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\]->\((?:([A-Za-z_][A-Za-z0-9_]*))?(?::([A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)$`)
	createEdgePatternReverseRE       = regexp.MustCompile(`^\((?:([A-Za-z_][A-Za-z0-9_]*))?(?::([A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)<-\[(?:([A-Za-z_][A-Za-z0-9_]*))?(?::([A-Za-z_][A-Za-z0-9_]*(?:\|:?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\]-\((?:([A-Za-z_][A-Za-z0-9_]*))?(?::([A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)$`)
	createEdgePatternUndirectedRE    = regexp.MustCompile(`^\((?:([A-Za-z_][A-Za-z0-9_]*))?(?::([A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)-\[(?:([A-Za-z_][A-Za-z0-9_]*))?(?::([A-Za-z_][A-Za-z0-9_]*(?:\|:?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\]-\((?:([A-Za-z_][A-Za-z0-9_]*))?(?::([A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)$`)
	createEdgePatternTwoDirectionsRE = regexp.MustCompile(`\)<-\[[^\]]*\]->\(`)
	createChainVertexTokenRE         = regexp.MustCompile(`^\((?:([A-Za-z_][A-Za-z0-9_]*))?(?::([A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\)`)
	createChainRelForwardTokenRE     = regexp.MustCompile(`^-\[(?:([A-Za-z_][A-Za-z0-9_]*))?(?::([A-Za-z_][A-Za-z0-9_]*(?:\|:?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\]->`)
	createChainRelReverseTokenRE     = regexp.MustCompile(`^<-\[(?:([A-Za-z_][A-Za-z0-9_]*))?(?::([A-Za-z_][A-Za-z0-9_]*(?:\|:?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\]-`)
	createChainRelUndirTokenRE       = regexp.MustCompile(`^-\[(?:([A-Za-z_][A-Za-z0-9_]*))?(?::([A-Za-z_][A-Za-z0-9_]*(?:\|:?[A-Za-z_][A-Za-z0-9_]*)*))?(?:\{([^{}]*)\})?\]-`)
	createMissingRelTypeForwardRE    = regexp.MustCompile(`^\((?:[A-Za-z_][A-Za-z0-9_]*)?(?::[A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*)?(?:\{[^{}]*\})?\)--?>\((?:[A-Za-z_][A-Za-z0-9_]*)?(?::[A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*)?(?:\{[^{}]*\})?\)$`)
	createMissingRelTypeReverseRE    = regexp.MustCompile(`^\((?:[A-Za-z_][A-Za-z0-9_]*)?(?::[A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*)?(?:\{[^{}]*\})?\)<--\((?:[A-Za-z_][A-Za-z0-9_]*)?(?::[A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*)?(?:\{[^{}]*\})?\)$`)
	createMissingRelTypeUndirRE      = regexp.MustCompile(`^\((?:[A-Za-z_][A-Za-z0-9_]*)?(?::[A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*)?(?:\{[^{}]*\})?\)--\((?:[A-Za-z_][A-Za-z0-9_]*)?(?::[A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*)?(?:\{[^{}]*\})?\)$`)
	createVariableLengthRelRE        = regexp.MustCompile(`\[[^\]]*\*[^\]]*\]`)
	setClauseRE                      = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)=(.+)$`)
	setMapReplaceClauseRE            = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)=(.+)$`)
	setMapAppendClauseRE             = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)\+=(.+)$`)
	setLabelClauseRE                 = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*):([A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*)$`)
	removeClauseRE                   = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)$`)
	removeLabelClauseRE              = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*):([A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)*)$`)
)

type createEdgeDirection string

const (
	createEdgeDirectionForward    createEdgeDirection = "forward"
	createEdgeDirectionReverse    createEdgeDirection = "reverse"
	createEdgeDirectionUndirected createEdgeDirection = "undirected"
)

type createChainVertexPattern struct {
	Var      string
	Labels   []string
	PropsRaw string
}

type createVertexPatternSpec struct {
	Var      string
	Labels   []string
	PropsRaw string
}

type createChainRelPattern struct {
	Var       string
	Type      string
	PropsRaw  string
	Direction createEdgeDirection
}

type createChainPattern struct {
	Vertexes []createChainVertexPattern
	Rels     []createChainRelPattern
}

type deletedVertexBinding struct {
	Tenant string
	ID     string
	Labels []string
}

type deletedEdgeBinding struct {
	Tenant string
	ID     string
	Type   string
}

const mergeCreatedMarkerKey = "__ve_merge_created"
const projectionEvalExecutorParam = "__ve_projection_eval_executor"
const projectionEvalTxParam = "__ve_projection_eval_tx"
const projectionEvalCtxParam = "__ve_projection_eval_ctx"
const wherePatternPredicateCacheParam = "__ve_where_pattern_predicate_cache"
const queryEntityCacheParam = "__ve_query_entity_cache"
const queryAdjacencyCacheParam = "__ve_query_adjacency_cache"
const runtimeCounterStateParam = "__ve_runtime_counter_state"
const edgeRangeIndexCandidateLimit = 200000
const stage2IndexPushdownMaxIndexedCandidates = 512
const stage2IndexPushdownMaxAverageEdgesPerSource = 16
const stage2IndexPushdownProbeCandidateLimit = 1536
const stage2IndexPushdownSourceScopedProbeMaxSources = 8

type wherePatternPredicateCache struct {
	outNeighbors map[string]map[string]struct{}
	inNeighbors  map[string]map[string]struct{}
}

type queryEntityCache struct {
	vertexes map[string]*graph.Vertex
	missing  map[string]struct{}
}

type queryAdjacencyCache struct {
	outEdges map[string][]*graph.Edge
	inEdges  map[string][]*graph.Edge
}

type runtimeCounterState struct {
	counters map[string]int64
}

func ensureRuntimeCounterState(params Params) *runtimeCounterState {
	if params == nil {
		return nil
	}
	if existing, ok := params[runtimeCounterStateParam].(*runtimeCounterState); ok && existing != nil {
		if existing.counters == nil {
			existing.counters = map[string]int64{}
		}
		return existing
	}
	state := &runtimeCounterState{counters: map[string]int64{}}
	params[runtimeCounterStateParam] = state
	return state
}

func resetRuntimeCounterState(params Params) {
	if params == nil {
		return
	}
	delete(params, runtimeCounterStateParam)
}

func buildRuntimeCounterDiagnostic(params Params) (Diagnostic, bool) {
	if params == nil {
		return Diagnostic{}, false
	}
	state, ok := params[runtimeCounterStateParam].(*runtimeCounterState)
	if !ok || state == nil || len(state.counters) == 0 {
		return Diagnostic{}, false
	}
	encoded, err := json.Marshal(state.counters)
	if err != nil {
		return Diagnostic{}, false
	}
	return Diagnostic{Code: "RUNTIME_COUNTERS", Message: string(encoded)}, true
}

func (e *Executor) observeRuntimeCounter(params Params, name string, delta int64) {
	if delta <= 0 || strings.TrimSpace(name) == "" {
		return
	}
	if state := ensureRuntimeCounterState(params); state != nil {
		state.counters[name] += delta
	}
	if e != nil && e.metrics != nil {
		e.metrics.ObserveRuntimeCounter(name, delta)
	}
}

type edgeIDScannerTx interface {
	ScanOutEdgeIDs(ctx context.Context, tenant, srcID, edgeType string, limit int, fn func(string) error) error
	ScanInEdgeIDs(ctx context.Context, tenant, dstID, edgeType string, limit int, fn func(string) error) error
}

func withProjectionEvalRuntime(ctx context.Context, tx graph.Tx, params Params, exec *Executor) Params {
	if params == nil {
		params = Params{}
	}
	runtime := make(Params, len(params)+3)
	for key, value := range params {
		runtime[key] = value
	}
	runtime[projectionEvalExecutorParam] = exec
	runtime[projectionEvalTxParam] = tx
	runtime[projectionEvalCtxParam] = ctx
	return runtime
}

var autoVertexIDSeq uint64
var autoEdgeIDSeq uint64

func (e *Executor) executeQueryStatement(ctx context.Context, stmt *ast.QueryStatement, params Params) (_ *Result, err error) {
	if stmt == nil {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "query statement is required", nil)
	}
	if len(stmt.Parts) == 0 {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "at least one query part is required", nil)
	}
	if len(stmt.Unions) != 0 && len(stmt.Unions) != len(stmt.Parts)-1 {
		return nil, &parser.ParseError{Kind: parser.ParseErrorInternal, Message: "invalid union boundaries", Statement: 1}
	}
	if err := validateUnionKinds(stmt.Unions); err != nil {
		return nil, err
	}
	resetRuntimeCounterState(params)
	_ = ensureRuntimeCounterState(params)
	defer resetRuntimeCounterState(params)

	writeQuery := false
	for _, part := range stmt.Parts {
		if hasWriteClause(part) {
			writeQuery = true
			break
		}
	}

	if !writeQuery {
		if fastResult, ok, fastErr := e.tryFastEdgeCountQuery(ctx, stmt, params); fastErr != nil {
			return nil, fastErr
		} else if ok {
			return fastResult, nil
		}
		if fastResult, ok, fastErr := e.tryFastVertexCountQuery(ctx, stmt, params); fastErr != nil {
			return nil, fastErr
		} else if ok {
			return fastResult, nil
		}
		if fastResult, ok, fastErr := e.tryFastLabelHistogramQuery(ctx, stmt, params); fastErr != nil {
			return nil, fastErr
		} else if ok {
			return fastResult, nil
		}
	} else {
		if fastResult, ok, fastErr := e.tryFastEdgeDeleteQuery(ctx, stmt, params); fastErr != nil {
			return nil, fastErr
		} else if ok {
			return fastResult, nil
		}
	}

	resultRows := []Row{}
	resultColumns := []string{}
	hasAnyReturn := false

	withTx := func(tx graph.Tx) error {
		for idx, part := range stmt.Parts {
			partRows, partColumns, returnSeen, stepErr := e.executeQueryPart(ctx, tx, part, params)
			if stepErr != nil {
				return stepErr
			}
			if !returnSeen {
				if len(stmt.Parts) > 1 {
					return &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "InvalidClauseComposition", Statement: 1}
				}
				continue
			}

			hasAnyReturn = true
			if idx == 0 {
				resultRows = append(resultRows, partRows...)
				resultColumns = append([]string(nil), partColumns...)
				continue
			}

			if !equalStringSlices(resultColumns, partColumns) {
				return &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "DifferentColumnsInUnion", Statement: 1}
			}

			op := stmt.Unions[idx-1]
			if op == ast.UnionKindAll {
				resultRows = append(resultRows, partRows...)
				continue
			}
			resultRows = append(resultRows, partRows...)
			resultRows = distinctProjectionRows(resultRows)
		}
		return nil
	}

	if writeQuery {
		err = e.store.Update(ctx, withTx)
	} else {
		err = e.store.View(ctx, withTx)
	}
	if err != nil {
		return nil, err
	}

	if !hasAnyReturn {
		resultColumns = nil
		resultRows = nil
	}

	result := &Result{Columns: resultColumns, Rows: resultRows, Stats: Stats{RowsReturned: len(resultRows)}}
	result.Rows = normalizeResultRows(result.Rows)
	if diagnostic, ok := buildRuntimeCounterDiagnostic(params); ok {
		result.Warnings = append(result.Warnings, diagnostic)
	}
	return result, nil
}

func (e *Executor) executeQueryPart(ctx context.Context, tx graph.Tx, part ast.QueryPart, params Params) ([]Row, []string, bool, error) {
	rows := []Row{{}}
	resultColumns := []string{}
	returnSeen := false

	for idx := 0; idx < len(part.Clauses); idx++ {
		clause := part.Clauses[idx]
		if idx+2 < len(part.Clauses) && clause.Kind == ast.ClauseKindMatch && part.Clauses[idx+1].Kind == ast.ClauseKindWith && part.Clauses[idx+2].Kind == ast.ClauseKindWith {
			if fastRows, fastColumns, ok, fastErr := e.tryFastTargetSharedPeerAggregationWithTopKClauses(ctx, tx, rows, clause, part.Clauses[idx+1], part.Clauses[idx+2], params, resultColumns); fastErr != nil {
				return nil, nil, false, fastErr
			} else if ok {
				rows = fastRows
				resultColumns = fastColumns
				idx += 2
				continue
			}
		}
		if idx+1 < len(part.Clauses) && clause.Kind == ast.ClauseKindMatch && part.Clauses[idx+1].Kind == ast.ClauseKindWith {
			if fastRows, fastColumns, ok, fastErr := e.tryFastTargetSharedPeerAggregationClausePair(ctx, tx, rows, clause, part.Clauses[idx+1], params, resultColumns); fastErr != nil {
				return nil, nil, false, fastErr
			} else if ok {
				rows = fastRows
				resultColumns = fastColumns
				idx++
				continue
			}
		}
		if idx+1 < len(part.Clauses) && clause.Kind == ast.ClauseKindMatch && part.Clauses[idx+1].Kind == ast.ClauseKindReturn {
			if fastRows, fastColumns, ok, fastErr := e.tryFastPeerCandidateReturnAggregationClausePair(ctx, tx, rows, clause, part.Clauses[idx+1], params, resultColumns); fastErr != nil {
				return nil, nil, false, fastErr
			} else if ok {
				return fastRows, fastColumns, true, nil
			}
		}

		var stepErr error
		switch clause.Kind {
		case ast.ClauseKindMatch:
			rows, stepErr = e.applyMatchClause(ctx, tx, rows, clause, params)
			resultColumns = appendUniqueColumns(resultColumns, inferMatchScopeColumnsForClause(clause)...)
		case ast.ClauseKindOptionalMatch:
			rows, stepErr = e.applyMatchClause(ctx, tx, rows, clause, params)
			resultColumns = appendUniqueColumns(resultColumns, inferMatchScopeColumnsForClause(clause)...)
		case ast.ClauseKindUnwind:
			rows, stepErr = e.applyUnwindClause(rows, clause, params)
			resultColumns = appendUniqueColumns(resultColumns, inferColumnsFromRows(rows)...)
		case ast.ClauseKindWith:
			rows, resultColumns, stepErr = e.applyProjectionClause(ctx, tx, rows, clause, params, resultColumns)
		case ast.ClauseKindCreate:
			rows, stepErr = e.applyCreateClause(ctx, tx, rows, clause, params, false)
		case ast.ClauseKindMerge:
			rows, stepErr = e.applyCreateClause(ctx, tx, rows, clause, params, true)
		case ast.ClauseKindSet:
			rows, stepErr = e.applySetClause(ctx, tx, rows, clause, params)
		case ast.ClauseKindRemove:
			rows, stepErr = e.applyRemoveClause(ctx, tx, rows, clause)
		case ast.ClauseKindDelete:
			rows, stepErr = e.applyDeleteClause(ctx, tx, rows, clause, params)
		case ast.ClauseKindReturn:
			rows, resultColumns, stepErr = e.applyProjectionClause(ctx, tx, rows, clause, params, resultColumns)
			returnSeen = true
			if stepErr != nil {
				return nil, nil, false, stepErr
			}
			return rows, resultColumns, true, nil
		case ast.ClauseKindInQueryCall:
			rows, stepErr = e.applyInQueryCallClause(ctx, rows, clause, params)
		default:
			return nil, nil, false, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("clause %s is not yet supported", clause.Kind), nil)
		}
		if stepErr != nil {
			return nil, nil, false, stepErr
		}
		if isWriteClauseKind(clause.Kind) {
			resetQueryEntityCache(params)
			resetQueryAdjacencyCache(params)
			resetWherePatternPredicateCache(params)
		}
	}

	return rows, resultColumns, returnSeen, nil
}

type fastTargetSharedPeerProjection struct {
	targetExpr         string
	targetKey          string
	peerExpr           string
	peerKey            string
	sharedCountExpr    string
	sharedCountKey     string
	avgDiffExpr        string
	avgDiffKey         string
	firstEdgeProperty  string
	secondEdgeProperty string
}

type fastTargetSharedPeerTopKProjection struct {
	targetExpr     string
	targetKey      string
	peerExpr       string
	peerKey        string
	similarityExpr string
	similarityKey  string
}

type fastTargetSharedPeerTopKSpec struct {
	descending bool
	skip       int
	limit      int
}

type fastTargetSharedPeerAggregate struct {
	target     *graph.Vertex
	peer       *graph.Vertex
	shared     int
	sumAbsDiff float64
}

type firstHopSeed struct {
	target                    *graph.Vertex
	firstValue                float64
	midRow                    Row
	secondWhereConstraints    map[string]edgeNumericRangeConstraint
	hasSecondWhereConstraints bool
}

type stage1WhereShortcutPlan struct {
	enabled                bool
	requirePeerNotTarget   bool
	requireSecondEdgeCover bool
}

type fastTargetSharedPeerRankedRow struct {
	row        Row
	score      float64
	inputIndex int
}

func (e *Executor) tryFastTargetSharedPeerAggregationWithTopKClauses(ctx context.Context, tx graph.Tx, rows []Row, matchClause ast.Clause, withClause ast.Clause, topKWithClause ast.Clause, params Params, priorColumns []string) ([]Row, []string, bool, error) {
	if !e.stage1TopKPushdownEnabled {
		return nil, nil, false, nil
	}
	if feedback, ok := e.fastPathFeedbackSnapshot(stage1TopKPushdownImplementation); ok && stage1TopKPushdownShouldDisableFromFeedback(feedback) {
		e.observeRuntimeCounter(params, "fast_path.stage1.topk_pushdown_skipped_adaptive", 1)
		return nil, nil, false, nil
	}
	aggs, projection, withSpec, ok, err := e.collectFastTargetSharedPeerAggregates(ctx, tx, rows, matchClause, withClause, params)
	if err != nil || !ok {
		return nil, nil, false, err
	}
	if !withSpecHasWhereRaw(withSpec) && len(aggs) == 0 {
		return []Row{}, []string{projection.targetKey, projection.peerKey, projection.sharedCountKey, projection.avgDiffKey}, true, nil
	}
	topKProjection, topKSpec, ok, err := parseFastTargetSharedPeerTopKWithClause(topKWithClause, projection, params)
	if err != nil || !ok {
		return nil, nil, false, err
	}
	out, err := fastTargetSharedPeerTopKRows(aggs, projection, withSpec, topKProjection, topKSpec, ctx, tx, params, e)
	if err != nil {
		return nil, nil, false, err
	}
	e.observeRuntimeCounter(params, "fast_path.stage1.topk_pushdown_applied", 1)
	e.observeRuntimeCounter(params, "fast_path.stage1.rows_output", int64(len(out)))
	columns := []string{topKProjection.targetKey, topKProjection.peerKey, topKProjection.similarityKey}
	if len(columns) == 0 && len(priorColumns) > 0 {
		columns = append([]string(nil), priorColumns...)
	}
	e.recordFastPathFeedback(stage1TopKPushdownImplementation, int64(len(aggs)), int64(len(out)))
	return out, columns, true, nil
}

func withSpecHasWhereRaw(withSpec projectionClauseSpec) bool {
	return strings.TrimSpace(withSpec.WhereRaw) != ""
}

func (e *Executor) collectFastTargetSharedPeerAggregates(ctx context.Context, tx graph.Tx, rows []Row, matchClause ast.Clause, withClause ast.Clause, params Params) (map[string]*fastTargetSharedPeerAggregate, fastTargetSharedPeerProjection, projectionClauseSpec, bool, error) {
	if len(rows) != 1 || len(rows[0]) != 0 {
		return nil, fastTargetSharedPeerProjection{}, projectionClauseSpec{}, false, nil
	}
	if tx == nil {
		return nil, fastTargetSharedPeerProjection{}, projectionClauseSpec{}, false, nil
	}

	matchSpec, err := anchoredMatchSpecFromClause(matchClause)
	if err != nil || matchSpec.Optional {
		return nil, fastTargetSharedPeerProjection{}, projectionClauseSpec{}, false, nil
	}

	chain, err := parseTwoHopDirectedChainPattern(matchSpec.Pattern)
	if err != nil || chain.SecondForward {
		return nil, fastTargetSharedPeerProjection{}, projectionClauseSpec{}, false, nil
	}

	withSpec, err := projectionClauseSpecFromClause(withClause)
	if err != nil {
		return nil, fastTargetSharedPeerProjection{}, projectionClauseSpec{}, false, nil
	}
	if withSpec.Distinct || len(withSpec.OrderBy) != 0 || strings.TrimSpace(withSpec.SkipRaw) != "" || strings.TrimSpace(withSpec.LimitRaw) != "" {
		return nil, fastTargetSharedPeerProjection{}, projectionClauseSpec{}, false, nil
	}
	items, err := parseProjectionItems(withSpec.ProjectionRaw)
	if err != nil {
		return nil, fastTargetSharedPeerProjection{}, projectionClauseSpec{}, false, nil
	}
	projection, ok := parseFastTargetSharedPeerProjection(items, chain)
	if !ok {
		return nil, fastTargetSharedPeerProjection{}, projectionClauseSpec{}, false, nil
	}

	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, fastTargetSharedPeerProjection{}, projectionClauseSpec{}, false, err
	}

	targetCandidates, err := e.resolveVertexPatternCandidates(ctx, tx, tenant, rows[0], chain.Left, params)
	if err != nil {
		return nil, fastTargetSharedPeerProjection{}, projectionClauseSpec{}, false, err
	}
	e.observeRuntimeCounter(params, "fast_path.stage1.target_candidates", int64(len(targetCandidates)))

	aggs := map[string]*fastTargetSharedPeerAggregate{}
	whereShortcut := buildStage1WhereShortcutPlan(matchSpec.Where, chain)
	accumulate := func(target *graph.Vertex, peer *graph.Vertex, firstValue, secondValue float64) {
		if target == nil || peer == nil {
			return
		}
		key := target.ID + "|" + peer.ID
		agg, exists := aggs[key]
		if !exists {
			agg = &fastTargetSharedPeerAggregate{target: target, peer: peer}
			aggs[key] = agg
		}
		agg.shared++
		agg.sumAbsDiff += math.Abs(firstValue - secondValue)
	}

	if !e.stage1SharedSeedExpansionEnabled || len(targetCandidates) <= 1 {
		for _, target := range targetCandidates {
			if target == nil {
				continue
			}
			baseRow := cloneRow(rows[0])
			if chain.Left.Var != "" {
				baseRow[chain.Left.Var] = target
			}

			firstScanType := chain.FirstEdgeType
			if len(chain.FirstEdgeAnyOf) > 0 {
				firstScanType = ""
			}
			if err := scanOutEdgesQueryCached(ctx, tx, tenant, target.ID, firstScanType, params, func(edge1 *graph.Edge) error {
				e.observeRuntimeCounter(params, "fast_path.stage1.first_hop.edges_visited", 1)
				if !edgeTypeMatches(edge1, chain.FirstEdgeType, chain.FirstEdgeAnyOf) {
					return nil
				}
				if !edgePatternMatches(edge1, chain.FirstEdgeProps, params, baseRow) {
					return nil
				}

				shared, err := getVertexQueryCached(ctx, tx, tenant, edge1.DstID, params)
				if err != nil {
					return err
				}
				if shared == nil {
					return nil
				}

				midRow := cloneRow(baseRow)
				if chain.FirstEdgeVar != "" {
					midRow[chain.FirstEdgeVar] = edge1
				}
				if chain.Mid.Var != "" {
					midRow[chain.Mid.Var] = shared
				}
				if !vertexPatternMatches(shared, chain.Mid, params, midRow) {
					return nil
				}

				secondWhereConstraints, hasSecondWhereConstraints := extractEdgeWhereNumericConstraints(matchSpec.Where, chain.SecondEdgeVar, midRow, params)
				secondScanType := chain.SecondEdgeType
				if len(chain.SecondEdgeAnyOf) > 0 {
					secondScanType = ""
				}
				return scanInEdgesQueryCached(ctx, tx, tenant, shared.ID, secondScanType, params, func(edge2 *graph.Edge) error {
					e.observeRuntimeCounter(params, "fast_path.stage1.second_hop.edges_visited", 1)
					if !edgeTypeMatches(edge2, chain.SecondEdgeType, chain.SecondEdgeAnyOf) {
						return nil
					}
					if !edgePatternMatches(edge2, chain.SecondEdgeProps, params, midRow) {
						return nil
					}
					if hasSecondWhereConstraints && !edgeMatchesNumericConstraints(edge2, secondWhereConstraints) {
						e.observeRuntimeCounter(params, "fast_path.stage1.second_hop.numeric_prefilter_drops", 1)
						return nil
					}

					peer, err := getVertexQueryCached(ctx, tx, tenant, edge2.SrcID, params)
					if err != nil {
						return err
					}
					if peer == nil || !vertexBindingMatches(midRow, chain.Right.Var, peer) {
						return nil
					}

					merged := cloneRow(midRow)
					if chain.Right.Var != "" {
						merged[chain.Right.Var] = peer
					}
					if chain.SecondEdgeVar != "" {
						merged[chain.SecondEdgeVar] = edge2
					}
					if !vertexPatternMatches(peer, chain.Right, params, merged) {
						return nil
					}
					if strings.TrimSpace(matchSpec.Where) != "" {
						bypassWhereEval, droppedByShortcut := stage1WhereShortcutDecision(whereShortcut, target, peer, hasSecondWhereConstraints)
						if droppedByShortcut {
							e.observeRuntimeCounter(params, "fast_path.stage1.where_eval_drops", 1)
							return nil
						}
						if bypassWhereEval {
							e.observeRuntimeCounter(params, "fast_path.stage1.where_eval_shortcuts", 1)
						} else {
							e.observeRuntimeCounter(params, "fast_path.stage1.where_eval_checks", 1)
							ok, err := e.evalWhereExpression(ctx, tx, matchSpec.Where, merged, params)
							if err != nil {
								return err
							}
							if !ok {
								e.observeRuntimeCounter(params, "fast_path.stage1.where_eval_drops", 1)
								return nil
							}
						}
					}

					firstValue, firstOK := edgeNumericProperty(edge1, projection.firstEdgeProperty)
					secondValue, secondOK := edgeNumericProperty(edge2, projection.secondEdgeProperty)
					if !firstOK || !secondOK {
						return nil
					}

					accumulate(target, peer, firstValue, secondValue)
					return nil
				})
			}); err != nil {
				return nil, fastTargetSharedPeerProjection{}, projectionClauseSpec{}, false, err
			}
		}
	} else {
		type rangedSeed struct {
			seed       firstHopSeed
			constraint edgeNumericRangeConstraint
		}
		type sharedSeedBuckets struct {
			unconstrained []firstHopSeed
			exactByValue  map[float64][]firstHopSeed
			ranged        []rangedSeed
		}
		sharedSeeds := map[string][]firstHopSeed{}
		sharedSeedBucketsByID := map[string]sharedSeedBuckets{}
		for _, target := range targetCandidates {
			if target == nil {
				continue
			}
			baseRow := cloneRow(rows[0])
			if chain.Left.Var != "" {
				baseRow[chain.Left.Var] = target
			}
			firstScanType := chain.FirstEdgeType
			if len(chain.FirstEdgeAnyOf) > 0 {
				firstScanType = ""
			}
			if err := scanOutEdgesQueryCached(ctx, tx, tenant, target.ID, firstScanType, params, func(edge1 *graph.Edge) error {
				e.observeRuntimeCounter(params, "fast_path.stage1.first_hop.edges_visited", 1)
				if !edgeTypeMatches(edge1, chain.FirstEdgeType, chain.FirstEdgeAnyOf) {
					return nil
				}
				if !edgePatternMatches(edge1, chain.FirstEdgeProps, params, baseRow) {
					return nil
				}
				shared, err := getVertexQueryCached(ctx, tx, tenant, edge1.DstID, params)
				if err != nil {
					return err
				}
				if shared == nil {
					return nil
				}
				midRow := cloneRow(baseRow)
				if chain.FirstEdgeVar != "" {
					midRow[chain.FirstEdgeVar] = edge1
				}
				if chain.Mid.Var != "" {
					midRow[chain.Mid.Var] = shared
				}
				if !vertexPatternMatches(shared, chain.Mid, params, midRow) {
					return nil
				}
				firstValue, firstOK := edgeNumericProperty(edge1, projection.firstEdgeProperty)
				if !firstOK {
					return nil
				}
				secondWhereConstraints, hasSecondWhereConstraints := extractEdgeWhereNumericConstraints(matchSpec.Where, chain.SecondEdgeVar, midRow, params)
				seed := firstHopSeed{target: target, firstValue: firstValue, midRow: midRow, secondWhereConstraints: secondWhereConstraints, hasSecondWhereConstraints: hasSecondWhereConstraints}
				sharedID := strings.TrimSpace(shared.ID)
				if sharedID == "" {
					return nil
				}
				sharedSeeds[sharedID] = append(sharedSeeds[sharedID], seed)
				buckets := sharedSeedBucketsByID[sharedID]
				targetConstraint, hasTargetConstraint := seed.secondWhereConstraints[projection.secondEdgeProperty]
				switch {
				case !seed.hasSecondWhereConstraints || len(seed.secondWhereConstraints) == 0 || !hasTargetConstraint:
					buckets.unconstrained = append(buckets.unconstrained, seed)
				case targetConstraint.isContradictory():
					e.observeRuntimeCounter(params, "fast_path.stage1.seed_rows_bucket_dropped", 1)
				case targetConstraint.lowerSet && targetConstraint.upperSet && targetConstraint.lower == targetConstraint.upper && targetConstraint.lowerInclusive && targetConstraint.upperInclusive:
					if buckets.exactByValue == nil {
						buckets.exactByValue = map[float64][]firstHopSeed{}
					}
					buckets.exactByValue[targetConstraint.lower] = append(buckets.exactByValue[targetConstraint.lower], seed)
				default:
					buckets.ranged = append(buckets.ranged, rangedSeed{seed: seed, constraint: targetConstraint})
				}
				sharedSeedBucketsByID[sharedID] = buckets
				e.observeRuntimeCounter(params, "fast_path.stage1.first_hop.shared_seed_rows", 1)
				return nil
			}); err != nil {
				return nil, fastTargetSharedPeerProjection{}, projectionClauseSpec{}, false, err
			}
		}

		e.observeRuntimeCounter(params, "fast_path.stage1.shared_vertices_seeded", int64(len(sharedSeeds)))
		secondScanType := chain.SecondEdgeType
		if len(chain.SecondEdgeAnyOf) > 0 {
			secondScanType = ""
		}
		for sharedID := range sharedSeeds {
			buckets := sharedSeedBucketsByID[sharedID]
			e.observeRuntimeCounter(params, "fast_path.stage1.second_hop.shared_vertices_expanded", 1)
			if err := scanInEdgesQueryCached(ctx, tx, tenant, sharedID, secondScanType, params, func(edge2 *graph.Edge) error {
				e.observeRuntimeCounter(params, "fast_path.stage1.second_hop.edges_visited", 1)
				if !edgeTypeMatches(edge2, chain.SecondEdgeType, chain.SecondEdgeAnyOf) {
					return nil
				}
				peer, err := getVertexQueryCached(ctx, tx, tenant, edge2.SrcID, params)
				if err != nil {
					return err
				}
				if peer == nil {
					return nil
				}
				secondValue, secondOK := edgeNumericProperty(edge2, projection.secondEdgeProperty)
				if !secondOK {
					return nil
				}
				applySeed := func(seed firstHopSeed) error {
					e.observeRuntimeCounter(params, "fast_path.stage1.seed_rows_considered", 1)
					if seed.target == nil {
						return nil
					}
					if !edgePatternMatches(edge2, chain.SecondEdgeProps, params, seed.midRow) {
						return nil
					}
					if seed.hasSecondWhereConstraints && !edgeMatchesNumericConstraints(edge2, seed.secondWhereConstraints) {
						e.observeRuntimeCounter(params, "fast_path.stage1.second_hop.numeric_prefilter_drops", 1)
						return nil
					}
					if !vertexBindingMatches(seed.midRow, chain.Right.Var, peer) {
						return nil
					}
					merged := cloneRow(seed.midRow)
					if chain.Right.Var != "" {
						merged[chain.Right.Var] = peer
					}
					if chain.SecondEdgeVar != "" {
						merged[chain.SecondEdgeVar] = edge2
					}
					if !vertexPatternMatches(peer, chain.Right, params, merged) {
						return nil
					}
					if strings.TrimSpace(matchSpec.Where) != "" {
						bypassWhereEval, droppedByShortcut := stage1WhereShortcutDecision(whereShortcut, seed.target, peer, seed.hasSecondWhereConstraints)
						if droppedByShortcut {
							e.observeRuntimeCounter(params, "fast_path.stage1.where_eval_drops", 1)
							return nil
						}
						if bypassWhereEval {
							e.observeRuntimeCounter(params, "fast_path.stage1.where_eval_shortcuts", 1)
						} else {
							e.observeRuntimeCounter(params, "fast_path.stage1.where_eval_checks", 1)
							ok, err := e.evalWhereExpression(ctx, tx, matchSpec.Where, merged, params)
							if err != nil {
								return err
							}
							if !ok {
								e.observeRuntimeCounter(params, "fast_path.stage1.where_eval_drops", 1)
								return nil
							}
						}
					}
					accumulate(seed.target, peer, seed.firstValue, secondValue)
					return nil
				}
				for _, seed := range buckets.unconstrained {
					if err := applySeed(seed); err != nil {
						return err
					}
				}
				if exactSeeds, ok := buckets.exactByValue[secondValue]; ok {
					for _, seed := range exactSeeds {
						if err := applySeed(seed); err != nil {
							return err
						}
					}
				}
				for _, ranged := range buckets.ranged {
					if !ranged.constraint.matchesValue(secondValue) {
						e.observeRuntimeCounter(params, "fast_path.stage1.seed_rows_bucket_dropped", 1)
						continue
					}
					if err := applySeed(ranged.seed); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				return nil, fastTargetSharedPeerProjection{}, projectionClauseSpec{}, false, err
			}
		}
	}

	return aggs, projection, withSpec, true, nil
}

func (e *Executor) tryFastTargetSharedPeerAggregationClausePair(ctx context.Context, tx graph.Tx, rows []Row, matchClause ast.Clause, withClause ast.Clause, params Params, priorColumns []string) ([]Row, []string, bool, error) {
	aggs, projection, withSpec, ok, err := e.collectFastTargetSharedPeerAggregates(ctx, tx, rows, matchClause, withClause, params)
	if err != nil || !ok {
		return nil, nil, false, err
	}
	out := make([]Row, 0, len(aggs))
	e.observeRuntimeCounter(params, "fast_path.stage1.peer_groups_before_with_filter", int64(len(aggs)))
	for _, agg := range sortedFastTargetSharedPeerAggregates(aggs) {
		if agg == nil || agg.shared <= 0 {
			continue
		}
		row := Row{}
		row[projection.targetKey] = agg.target
		row[projection.peerKey] = agg.peer
		row[projection.sharedCountKey] = agg.shared
		row[projection.avgDiffKey] = agg.sumAbsDiff / float64(agg.shared)
		out = append(out, row)
	}

	if withSpec.WhereRaw != "" {
		before := len(out)
		filtered := make([]Row, 0, len(out))
		for _, row := range out {
			ok, err := e.evalWhereExpression(ctx, tx, withSpec.WhereRaw, row, params)
			if err != nil {
				return nil, nil, false, err
			}
			if ok {
				filtered = append(filtered, row)
			}
		}
		out = filtered
		e.observeRuntimeCounter(params, "fast_path.stage1.with_filter_drops", int64(before-len(out)))
	}
	e.observeRuntimeCounter(params, "fast_path.stage1.rows_output", int64(len(out)))

	columns := []string{projection.targetKey, projection.peerKey, projection.sharedCountKey, projection.avgDiffKey}
	if len(columns) == 0 && len(priorColumns) > 0 {
		columns = append([]string(nil), priorColumns...)
	}
	return out, columns, true, nil
}

func stage1WhereShortcutDecision(plan stage1WhereShortcutPlan, target *graph.Vertex, peer *graph.Vertex, hasSecondWhereConstraints bool) (bypassWhereEval bool, dropRow bool) {
	if !plan.enabled {
		return false, false
	}
	if plan.requirePeerNotTarget {
		if target == nil || peer == nil {
			return false, true
		}
		if strings.TrimSpace(target.ID) == "" || strings.TrimSpace(peer.ID) == "" {
			return false, true
		}
		if target.ID == peer.ID {
			return false, true
		}
	}
	if plan.requireSecondEdgeCover && !hasSecondWhereConstraints {
		return false, false
	}
	return true, false
}

func buildStage1WhereShortcutPlan(whereRaw string, chain twoHopDirectedChainPattern) stage1WhereShortcutPlan {
	conjuncts, ok := flattenWhereConjuncts(whereRaw)
	if !ok || len(conjuncts) == 0 {
		return stage1WhereShortcutPlan{}
	}

	plan := stage1WhereShortcutPlan{}
	leftVar := strings.TrimSpace(chain.Left.Var)
	rightVar := strings.TrimSpace(chain.Right.Var)
	for _, conjunct := range conjuncts {
		conjunct = strings.TrimSpace(conjunct)
		if conjunct == "" {
			continue
		}
		if isStage1PeerNotTargetConjunct(conjunct, leftVar, rightVar) {
			plan.requirePeerNotTarget = true
			continue
		}
		if isStage1SecondEdgeConstraintConjunct(conjunct, strings.TrimSpace(chain.FirstEdgeVar), strings.TrimSpace(chain.SecondEdgeVar)) {
			plan.requireSecondEdgeCover = true
			continue
		}
		return stage1WhereShortcutPlan{}
	}

	if !plan.requirePeerNotTarget && !plan.requireSecondEdgeCover {
		return stage1WhereShortcutPlan{}
	}
	plan.enabled = true
	return plan
}

func isStage1PeerNotTargetConjunct(conjunct, leftVar, rightVar string) bool {
	if leftVar == "" || rightVar == "" {
		return false
	}
	left, right, op, ok := splitTopLevelComparison(conjunct)
	if !ok {
		return false
	}
	op = strings.TrimSpace(op)
	if op != "<>" && op != "!=" {
		return false
	}
	left = strings.TrimSpace(left)
	right = strings.TrimSpace(right)
	return (left == leftVar && right == rightVar) || (left == rightVar && right == leftVar)
}

func isStage1SecondEdgeConstraintConjunct(conjunct, firstEdgeVar, secondEdgeVar string) bool {
	left, right, _, ok := splitTopLevelComparison(conjunct)
	if !ok {
		return false
	}
	leftIsSecond := isEdgeVarReferenced(left, secondEdgeVar)
	rightIsSecond := isEdgeVarReferenced(right, secondEdgeVar)
	if leftIsSecond != rightIsSecond {
		return true
	}
	if isAbsDifferenceWithSecondEdgeRef(left, firstEdgeVar, secondEdgeVar) {
		return true
	}
	if isAbsDifferenceWithSecondEdgeRef(right, firstEdgeVar, secondEdgeVar) {
		return true
	}
	return false
}

func isEdgeVarReferenced(expr, edgeVar string) bool {
	if strings.TrimSpace(edgeVar) == "" {
		return false
	}
	if _, ok := edgePropertyReference(expr, edgeVar); ok {
		return true
	}
	return false
}

func isAbsDifferenceWithSecondEdgeRef(expr, firstEdgeVar, secondEdgeVar string) bool {
	arg, ok := parseFunctionCall(strings.TrimSpace(expr), "abs")
	if !ok {
		return false
	}
	leftTerm, rightTerm, termOp, ok := splitTopLevelOperatorSetLast(arg, "+", "-")
	if !ok || strings.TrimSpace(termOp) != "-" {
		return false
	}
	leftSecond := isEdgeVarReferenced(leftTerm, secondEdgeVar)
	rightSecond := isEdgeVarReferenced(rightTerm, secondEdgeVar)
	if leftSecond == rightSecond {
		return false
	}
	if strings.TrimSpace(firstEdgeVar) == "" {
		return true
	}
	leftFirst := isEdgeVarReferenced(leftTerm, firstEdgeVar)
	rightFirst := isEdgeVarReferenced(rightTerm, firstEdgeVar)
	return leftFirst || rightFirst
}

func parseFastTargetSharedPeerProjection(items []projectionSpec, chain twoHopDirectedChainPattern) (fastTargetSharedPeerProjection, bool) {
	if len(items) != 4 {
		return fastTargetSharedPeerProjection{}, false
	}
	projection := fastTargetSharedPeerProjection{}
	for _, item := range items {
		if item.Expression == "*" || item.CollectArg != "" {
			return fastTargetSharedPeerProjection{}, false
		}
		key := item.Expression
		if item.Alias != "" {
			key = item.Alias
		}

		switch {
		case item.Expression == chain.Left.Var && item.CountArg == "" && item.AggFunc == "":
			projection.targetExpr = item.Expression
			projection.targetKey = key
		case item.Expression == chain.Right.Var && item.CountArg == "" && item.AggFunc == "":
			projection.peerExpr = item.Expression
			projection.peerKey = key
		case item.CountArg != "":
			countArg, distinct := parseCountDistinctArg(item.CountArg)
			if distinct || strings.TrimSpace(countArg) != chain.Mid.Var {
				return fastTargetSharedPeerProjection{}, false
			}
			projection.sharedCountExpr = item.Expression
			projection.sharedCountKey = key
		case strings.EqualFold(item.AggFunc, "avg"):
			firstProp, secondProp, ok := parseAbsEdgeDifferenceAggregateArg(item.AggArg, chain.FirstEdgeVar, chain.SecondEdgeVar)
			if !ok {
				return fastTargetSharedPeerProjection{}, false
			}
			projection.avgDiffExpr = item.Expression
			projection.avgDiffKey = key
			projection.firstEdgeProperty = firstProp
			projection.secondEdgeProperty = secondProp
		default:
			return fastTargetSharedPeerProjection{}, false
		}
	}

	if projection.targetKey == "" || projection.peerKey == "" || projection.sharedCountKey == "" || projection.avgDiffKey == "" || projection.firstEdgeProperty == "" || projection.secondEdgeProperty == "" {
		return fastTargetSharedPeerProjection{}, false
	}
	return projection, true
}

func parseAbsEdgeDifferenceAggregateArg(raw, firstEdgeVar, secondEdgeVar string) (string, string, bool) {
	inner, ok := parseFunctionCall(strings.TrimSpace(raw), "abs")
	if !ok {
		return "", "", false
	}
	left, right, op, ok := splitTopLevelOperatorSetLast(inner, "+", "-")
	if !ok || strings.TrimSpace(op) != "-" {
		return "", "", false
	}

	if firstProp, ok := edgePropertyReference(left, firstEdgeVar); ok {
		if secondProp, ok := edgePropertyReference(right, secondEdgeVar); ok {
			return firstProp, secondProp, true
		}
	}
	if firstProp, ok := edgePropertyReference(right, firstEdgeVar); ok {
		if secondProp, ok := edgePropertyReference(left, secondEdgeVar); ok {
			return firstProp, secondProp, true
		}
	}
	return "", "", false
}

func edgeNumericProperty(edge *graph.Edge, property string) (float64, bool) {
	if edge == nil || edge.Properties == nil {
		return 0, false
	}
	raw, ok := edge.Properties[strings.TrimSpace(property)]
	if !ok {
		return 0, false
	}
	numeric, ok := comparableNumericValue(decodeStoredPropertyValue(raw))
	if !ok {
		return 0, false
	}
	return numeric, true
}

type fastPeerCandidateReturnProjection struct {
	nonAggregates                []projectionSpec
	avgEdgeProperty              string
	avgKey                       string
	countKey                     string
	sumSimilarityExpr            string
	sumSimilarityKey             string
	orderedOutputKeys            []string
	rightVar                     string
	leftVar                      string
	edgeVar                      string
	lateMaterializeNonAggregates bool
}

type fastPeerCandidateAggregate struct {
	sampleScope     Row
	sampleCandidate *graph.Vertex
	edgeCount       int
	avgSum          float64
	avgCount        int
	similaritySum   float64
}

type fastPeerCandidateTopKSpec struct {
	descending bool
	skip       int
	limit      int
}

type fastPeerCandidateRankedRow struct {
	agg        *fastPeerCandidateAggregate
	score      float64
	inputIndex int
}

func parseFastPeerCandidateReturnProjection(items []projectionSpec, pattern directedRelationshipPattern) (fastPeerCandidateReturnProjection, bool) {
	if len(items) == 0 {
		return fastPeerCandidateReturnProjection{}, false
	}
	out := fastPeerCandidateReturnProjection{rightVar: strings.TrimSpace(pattern.Right.Var), leftVar: strings.TrimSpace(pattern.Left.Var), edgeVar: strings.TrimSpace(pattern.EdgeVar)}
	for _, item := range items {
		if item.Expression == "*" || item.CollectArg != "" {
			return fastPeerCandidateReturnProjection{}, false
		}
		key := item.Expression
		if item.Alias != "" {
			key = item.Alias
		}
		out.orderedOutputKeys = append(out.orderedOutputKeys, key)

		switch {
		case strings.EqualFold(item.AggFunc, "avg"):
			avgProp, ok := edgePropertyReference(item.AggArg, pattern.EdgeVar)
			if !ok || avgProp == "" {
				return fastPeerCandidateReturnProjection{}, false
			}
			out.avgEdgeProperty = avgProp
			out.avgKey = key
		case item.CountArg != "":
			countArg, distinct := parseCountDistinctArg(item.CountArg)
			if distinct || strings.TrimSpace(countArg) != strings.TrimSpace(pattern.EdgeVar) {
				return fastPeerCandidateReturnProjection{}, false
			}
			out.countKey = key
		case strings.EqualFold(item.AggFunc, "sum"):
			if strings.TrimSpace(item.AggArg) == "" {
				return fastPeerCandidateReturnProjection{}, false
			}
			out.sumSimilarityExpr = strings.TrimSpace(item.AggArg)
			out.sumSimilarityKey = key
		default:
			out.nonAggregates = append(out.nonAggregates, item)
		}
	}

	if out.avgEdgeProperty == "" || out.avgKey == "" || out.countKey == "" || out.sumSimilarityExpr == "" || out.sumSimilarityKey == "" {
		return fastPeerCandidateReturnProjection{}, false
	}
	out.lateMaterializeNonAggregates = canLateMaterializePeerCandidateNonAggregates(out.nonAggregates, out)
	return out, true
}

func canLateMaterializePeerCandidateNonAggregates(nonAggregates []projectionSpec, projection fastPeerCandidateReturnProjection) bool {
	rightVar := strings.TrimSpace(projection.rightVar)
	if rightVar == "" {
		return false
	}
	leftVar := strings.TrimSpace(projection.leftVar)
	edgeVar := strings.TrimSpace(projection.edgeVar)

	for _, item := range nonAggregates {
		expr := strings.TrimSpace(item.Expression)
		if expr == "" {
			continue
		}
		if expressionReferencesVariable(expr, leftVar) || expressionReferencesVariable(expr, edgeVar) {
			return false
		}
	}
	return true
}

func expressionReferencesVariable(expr, varName string) bool {
	varName = strings.TrimSpace(varName)
	if varName == "" {
		return false
	}
	expr = strings.TrimSpace(expr)
	if expr == varName {
		return true
	}
	if strings.Contains(expr, varName+".") {
		return true
	}
	if strings.Contains(expr, "("+varName+")") {
		return true
	}
	if strings.Contains(expr, " "+varName+" ") {
		return true
	}
	return false
}

func (e *Executor) tryFastPeerCandidateReturnAggregationClausePair(ctx context.Context, tx graph.Tx, rows []Row, matchClause ast.Clause, returnClause ast.Clause, params Params, priorColumns []string) ([]Row, []string, bool, error) {
	if tx == nil || len(rows) == 0 {
		return nil, nil, false, nil
	}

	matchSpec, err := anchoredMatchSpecFromClause(matchClause)
	if err != nil || matchSpec.Optional {
		return nil, nil, false, nil
	}

	pattern, err := parseDirectedRelationshipPattern(matchSpec.Pattern)
	if err != nil {
		return nil, nil, false, nil
	}
	if strings.TrimSpace(pattern.Left.Var) == "" || strings.TrimSpace(pattern.Right.Var) == "" || strings.TrimSpace(pattern.EdgeVar) == "" {
		return nil, nil, false, nil
	}

	retSpec, err := projectionClauseSpecFromClause(returnClause)
	if err != nil {
		return nil, nil, false, nil
	}
	if retSpec.Distinct || strings.TrimSpace(retSpec.WhereRaw) != "" {
		return nil, nil, false, nil
	}

	items, err := parseProjectionItems(retSpec.ProjectionRaw)
	if err != nil {
		return nil, nil, false, nil
	}
	projection, ok := parseFastPeerCandidateReturnProjection(items, pattern)
	if !ok {
		return nil, nil, false, nil
	}

	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, nil, false, err
	}

	params = withProjectionEvalRuntime(ctx, tx, params, e)
	aggs := map[string]*fastPeerCandidateAggregate{}
	groupOrder := make([]string, 0)
	stage2IndexedEdgeCache := map[string]map[string][]*graph.Edge{}
	stage2IndexPushdownDecisionCache := map[string]bool{}
	stage2IndexPushdownUsed := false
	stage2PredicateShapeSkipNoted := false
	stage2SourceScopeSkipNoted := false
	stage2PredicateShapeEligible, stage2PredicateShapeDecisive := stage2IndexPushdownPredicateShapeDecision(pattern, matchSpec.Where, params)
	stage2IndexProbeLimit := stage2IndexPushdownProbeCandidateLimit
	if maxPushdownCandidates := stage2IndexPushdownMaxIndexedCandidates + 1; stage2IndexProbeLimit > maxPushdownCandidates {
		stage2IndexProbeLimit = maxPushdownCandidates
	}
	type stage2PeerInput struct {
		row  Row
		peer *graph.Vertex
	}
	stage2Inputs := make([]stage2PeerInput, 0, len(rows))
	stage2SourcePeerIDs := map[string]struct{}{}
	for _, row := range rows {
		peer, bound, err := resolveBoundPredicateVertex(ctx, tx, tenant, row, pattern.Left, params)
		if err != nil {
			return nil, nil, false, err
		}
		if !bound {
			return nil, nil, false, nil
		}
		if peer == nil {
			continue
		}
		stage2Inputs = append(stage2Inputs, stage2PeerInput{row: row, peer: peer})
		if peerID := strings.TrimSpace(peer.ID); peerID != "" {
			stage2SourcePeerIDs[peerID] = struct{}{}
		}
	}
	var stage2ProbeSourceFilter map[string]struct{}
	if len(stage2SourcePeerIDs) > 0 && len(stage2SourcePeerIDs) <= stage2IndexPushdownSourceScopedProbeMaxSources {
		stage2ProbeSourceFilter = stage2SourcePeerIDs
	}
	e.observeRuntimeCounter(params, "fast_path.stage2.peer_rows_input", int64(len(rows)))

	for _, input := range stage2Inputs {
		row := input.row
		peer := input.peer

		numericConstraints, hasNumericConstraints := extractEdgeWhereNumericConstraints(matchSpec.Where, pattern.EdgeVar, row, params)
		excludedRightIDs, hasExcludedRightIDs, err := extractDirectedWhereRightExclusionSet(ctx, tx, tenant, matchSpec.Where, pattern.Right.Var, row, params)
		if err != nil {
			return nil, nil, false, err
		}
		skipWhereEval := directedWhereCoveredByExtractedPrefilters(matchSpec.Where, pattern.EdgeVar, pattern.Right.Var, row, params, hasNumericConstraints, hasExcludedRightIDs)
		if skipWhereEval {
			e.observeRuntimeCounter(params, "fast_path.stage2.where_eval_bypass_rows", 1)
		}

		similarityValue, err := evalExpressionWithScope(projection.sumSimilarityExpr, row, params)
		if err != nil {
			return nil, nil, false, nil
		}
		similarityNumeric, similarityNumericOK := comparableNumericValue(similarityValue)

		processCandidateEdge := func(edge *graph.Edge) error {
			e.observeRuntimeCounter(params, "fast_path.stage2.edges_visited", 1)
			if !edgeTypeMatches(edge, pattern.EdgeType, pattern.EdgeAnyOf) {
				return nil
			}
			if !edgePatternMatches(edge, pattern.EdgeProps, params, row) {
				return nil
			}
			if hasNumericConstraints && !edgeMatchesNumericConstraints(edge, numericConstraints) {
				e.observeRuntimeCounter(params, "fast_path.stage2.numeric_prefilter_drops", 1)
				return nil
			}
			if hasExcludedRightIDs {
				if _, blocked := excludedRightIDs[edge.DstID]; blocked {
					e.observeRuntimeCounter(params, "fast_path.stage2.antijoin_prefilter_drops", 1)
					return nil
				}
			}

			groupID := strings.TrimSpace(edge.DstID)
			if groupID == "" {
				return nil
			}
			if agg, exists := aggs[groupID]; exists && skipWhereEval {
				e.observeRuntimeCounter(params, "fast_path.stage2.candidate_group_reuse_hits", 1)
				agg.edgeCount++
				if rating, ok := edgeNumericProperty(edge, projection.avgEdgeProperty); ok {
					agg.avgSum += rating
					agg.avgCount++
				}
				if similarityNumericOK {
					agg.similaritySum += similarityNumeric
				}
				return nil
			}

			candidate, err := getVertexQueryCached(ctx, tx, tenant, edge.DstID, params)
			if err != nil {
				return err
			}
			if candidate == nil {
				return nil
			}

			merged := cloneRow(row)
			if pattern.Left.Var != "" {
				merged[pattern.Left.Var] = peer
			}
			if pattern.Right.Var != "" {
				merged[pattern.Right.Var] = candidate
			}
			if pattern.EdgeVar != "" {
				merged[pattern.EdgeVar] = edge
			}
			if !vertexPatternMatches(candidate, pattern.Right, params, merged) {
				return nil
			}
			if strings.TrimSpace(matchSpec.Where) != "" && !skipWhereEval {
				ok, err := e.evalWhereExpression(ctx, tx, matchSpec.Where, merged, params)
				if err != nil {
					return err
				}
				if !ok {
					e.observeRuntimeCounter(params, "fast_path.stage2.where_eval_drops", 1)
					return nil
				}
			}

			agg, exists := aggs[groupID]
			if !exists {
				agg = &fastPeerCandidateAggregate{}
				if projection.lateMaterializeNonAggregates {
					agg.sampleCandidate = candidate
				} else {
					agg.sampleScope = cloneRow(merged)
				}
				aggs[groupID] = agg
				groupOrder = append(groupOrder, groupID)
				e.observeRuntimeCounter(params, "fast_path.stage2.candidate_groups_created", 1)
				if projection.lateMaterializeNonAggregates {
					e.observeRuntimeCounter(params, "fast_path.stage2.late_materialization_candidates", 1)
				}
			}

			agg.edgeCount++
			if rating, ok := edgeNumericProperty(edge, projection.avgEdgeProperty); ok {
				agg.avgSum += rating
				agg.avgCount++
			}
			if similarityNumericOK {
				agg.similaritySum += similarityNumeric
			}
			if !e.stage2LateMaterializationEnabled {
				e.observeRuntimeCounter(params, "fast_path.stage2.eager_projection_updates", 1)
				if _, err := buildFastPeerCandidateResultRow(agg, projection, params); err != nil {
					return err
				}
			}
			return nil
		}

		if e.stage2EdgeIndexPushdownEnabled {
			predicateShapeEligible := stage2PredicateShapeEligible
			if !stage2PredicateShapeDecisive {
				predicateShapeEligible = stage2IndexPushdownEligibleByPredicateShape(pattern, matchSpec.Where, row, params)
			}
			if !predicateShapeEligible {
				if !stage2PredicateShapeSkipNoted {
					e.observeRuntimeCounter(params, "fast_path.stage2.index_pushdown_skipped_predicate_shape", 1)
					stage2PredicateShapeSkipNoted = true
				}
			} else {
				cacheKey, cacheable := stage2IndexLookupCacheKey(pattern, matchSpec.Where, row, params)
				if cacheable {
					lookupByIndex := false
					if cachedLookup, ok := stage2IndexPushdownDecisionCache[cacheKey]; ok {
						lookupByIndex = cachedLookup
						e.observeRuntimeCounter(params, "fast_path.stage2.index_lookup_cache_hits", 1)
					} else {
						e.observeRuntimeCounter(params, "fast_path.stage2.index_lookup_cache_misses", 1)
						if stage2ProbeSourceFilter == nil && len(stage2SourcePeerIDs) > stage2IndexPushdownSourceScopedProbeMaxSources && !stage2SourceScopeSkipNoted {
							e.observeRuntimeCounter(params, "fast_path.stage2.index_probe_source_scope_skipped_wide", 1)
							stage2SourceScopeSkipNoted = true
						}
						edges, indexed, probeCapExceeded, err := e.resolveEdgesByIndexedProperty(ctx, tx, tenant, pattern.EdgeType, pattern.EdgeAnyOf, pattern.EdgeProps, pattern.EdgeVar, matchSpec.Where, "", row, params, stage2ProbeSourceFilter, stage2IndexProbeLimit)
						if err != nil {
							return nil, nil, false, err
						}
						applyPushdown := false
						if probeCapExceeded {
							e.observeRuntimeCounter(params, "fast_path.stage2.index_probe_cap_exceeded", 1)
							e.observeRuntimeCounter(params, "fast_path.stage2.index_pushdown_skipped_probe_cap", 1)
						}
						if indexed {
							edgesBySource := map[string][]*graph.Edge{}
							for _, candidateEdge := range edges {
								if candidateEdge == nil || strings.TrimSpace(candidateEdge.SrcID) == "" {
									continue
								}
								edgesBySource[candidateEdge.SrcID] = append(edgesBySource[candidateEdge.SrcID], candidateEdge)
							}
							totalCandidates := 0
							for _, sourceEdges := range edgesBySource {
								totalCandidates += len(sourceEdges)
							}
							e.observeRuntimeCounter(params, "fast_path.stage2.index_candidates_total", int64(totalCandidates))
							applyPushdown = shouldApplyStage2IndexPushdown(edgesBySource)
							if applyPushdown {
								stage2IndexedEdgeCache[cacheKey] = edgesBySource
							} else {
								e.observeRuntimeCounter(params, "fast_path.stage2.index_pushdown_skipped_unselective", 1)
							}
						}
						stage2IndexPushdownDecisionCache[cacheKey] = applyPushdown
						lookupByIndex = applyPushdown
					}
					if lookupByIndex {
						if !stage2IndexPushdownUsed {
							e.observeRuntimeCounter(params, "fast_path.stage2.index_pushdown_applied", 1)
							stage2IndexPushdownUsed = true
						}
						e.observeRuntimeCounter(params, "fast_path.stage2.index_pushdown_rows", 1)
						indexedEdges := stage2IndexedEdgeCache[cacheKey][peer.ID]
						for _, edge := range indexedEdges {
							if edge == nil {
								continue
							}
							e.observeRuntimeCounter(params, "fast_path.stage2.index_edges_considered", 1)
							if err := processCandidateEdge(edge); err != nil {
								return nil, nil, false, err
							}
						}
						continue
					}
				}
			}
		}

		scanType := pattern.EdgeType
		if len(pattern.EdgeAnyOf) > 0 {
			scanType = ""
		}
		if err := scanOutEdgesQueryCached(ctx, tx, tenant, peer.ID, scanType, params, processCandidateEdge); err != nil {
			return nil, nil, false, err
		}
	}

	out := make([]Row, 0, len(groupOrder))
	topKSpec, useTopK, err := fastPeerCandidateTopKSpecFromProjection(retSpec, projection, params)
	if err != nil {
		return nil, nil, false, err
	}
	if !e.stage2TopKPushdownEnabled {
		useTopK = false
	}
	e.observeRuntimeCounter(params, "fast_path.stage2.candidate_groups_total", int64(len(groupOrder)))
	if useTopK {
		e.observeRuntimeCounter(params, "fast_path.stage2.topk_pushdown_applied", 1)
		out, err = fastPeerCandidateTopKRows(aggs, groupOrder, projection, topKSpec, params)
		if err != nil {
			return nil, nil, false, err
		}
	} else {
		for _, groupID := range groupOrder {
			agg := aggs[groupID]
			if agg == nil || agg.edgeCount == 0 {
				continue
			}
			resultRow, err := buildFastPeerCandidateResultRow(agg, projection, params)
			if err != nil {
				return nil, nil, false, err
			}
			out = append(out, resultRow)
		}
		out, err = applyProjectionPostProcessing(out, retSpec, params)
		if err != nil {
			return nil, nil, false, err
		}
	}
	e.recordFastPathFeedback("fast_peer_candidate_return_aggregation_clause_pair", int64(len(rows)), int64(len(out)))

	columns := append([]string(nil), projection.orderedOutputKeys...)
	if len(columns) == 0 && len(priorColumns) > 0 {
		columns = append([]string(nil), priorColumns...)
	}
	out = trimProjectionRows(out, columns)
	e.observeRuntimeCounter(params, "fast_path.stage2.rows_output", int64(len(out)))
	return out, columns, true, nil
}

func stage2IndexLookupCacheKey(pattern directedRelationshipPattern, whereRaw string, row Row, params Params) (string, bool) {
	types := edgePatternCandidateTypes(pattern.EdgeType, pattern.EdgeAnyOf)
	if len(types) == 0 {
		return "", false
	}

	parts := make([]string, 0, 4+len(types))
	parts = append(parts, "types="+strings.Join(types, ","))

	if prop, value, ok := edgePropertyEquality(pattern.EdgeProps, params, row); ok {
		parts = append(parts, fmt.Sprintf("eq:%s:%x", prop, valueToBytes(value)))
	}

	constraints, hasConstraints := extractEdgeWhereNumericConstraints(whereRaw, pattern.EdgeVar, row, params)
	if hasConstraints {
		keys := make([]string, 0, len(constraints))
		for prop := range constraints {
			keys = append(keys, prop)
		}
		sort.Strings(keys)
		for _, prop := range keys {
			parts = append(parts, "rng:"+prop+":"+edgeNumericRangeConstraintCacheKey(constraints[prop]))
		}
	}

	if len(parts) <= 1 {
		return "", false
	}
	return strings.Join(parts, "|"), true
}

func stage2IndexPushdownEligibleByPredicateShape(pattern directedRelationshipPattern, whereRaw string, row Row, params Params) bool {
	if _, _, ok := edgePropertyEquality(pattern.EdgeProps, params, row); ok {
		return true
	}
	constraints, hasConstraints := extractEdgeWhereNumericConstraints(whereRaw, pattern.EdgeVar, row, params)
	if !hasConstraints {
		return false
	}
	for _, constraint := range constraints {
		if constraint.isContradictory() {
			return true
		}
		if constraint.lowerSet && constraint.upperSet {
			return true
		}
	}
	return false
}

func stage2IndexPushdownPredicateShapeDecision(pattern directedRelationshipPattern, whereRaw string, params Params) (eligible bool, decisive bool) {
	if strings.TrimSpace(pattern.EdgeProps) != "" {
		if _, _, ok := edgePropertyEquality(pattern.EdgeProps, params, nil); ok {
			return true, true
		}
	}
	constraints, hasConstraints := extractEdgeWhereNumericConstraints(whereRaw, pattern.EdgeVar, nil, params)
	if !hasConstraints {
		return false, false
	}
	for _, constraint := range constraints {
		if constraint.isContradictory() {
			return true, true
		}
		if constraint.lowerSet && constraint.upperSet {
			return true, true
		}
	}
	return false, true
}

func edgeNumericRangeConstraintCacheKey(constraint edgeNumericRangeConstraint) string {
	lower := "*"
	if constraint.lowerSet {
		op := ">"
		if constraint.lowerInclusive {
			op = ">="
		}
		lower = fmt.Sprintf("%s%.9f", op, constraint.lower)
	}
	upper := "*"
	if constraint.upperSet {
		op := "<"
		if constraint.upperInclusive {
			op = "<="
		}
		upper = fmt.Sprintf("%s%.9f", op, constraint.upper)
	}
	return lower + "," + upper
}

func shouldApplyStage2IndexPushdown(indexedEdgesBySource map[string][]*graph.Edge) bool {
	if len(indexedEdgesBySource) == 0 {
		return true
	}
	totalCandidates := 0
	for _, edges := range indexedEdgesBySource {
		totalCandidates += len(edges)
	}
	if totalCandidates <= 0 {
		return true
	}
	if totalCandidates > stage2IndexPushdownMaxIndexedCandidates {
		return false
	}
	averagePerSource := float64(totalCandidates) / float64(len(indexedEdgesBySource))
	if averagePerSource > float64(stage2IndexPushdownMaxAverageEdgesPerSource) {
		return false
	}
	return true
}

func buildFastPeerCandidateResultRow(agg *fastPeerCandidateAggregate, projection fastPeerCandidateReturnProjection, params Params) (Row, error) {
	result := Row{}
	scope := agg.sampleScope
	if projection.lateMaterializeNonAggregates {
		scope = Row{}
		if strings.TrimSpace(projection.rightVar) != "" {
			scope[projection.rightVar] = agg.sampleCandidate
		}
	}
	for _, item := range projection.nonAggregates {
		key := item.Expression
		if item.Alias != "" {
			key = item.Alias
		}
		value, err := evalExpressionWithScope(item.Expression, scope, params)
		if err != nil {
			return nil, err
		}
		result[key] = value
	}
	result[projection.countKey] = agg.edgeCount
	if agg.avgCount == 0 {
		result[projection.avgKey] = nil
	} else {
		result[projection.avgKey] = agg.avgSum / float64(agg.avgCount)
	}
	result[projection.sumSimilarityKey] = agg.similaritySum
	return result, nil
}

func fastPeerCandidateTopKSpecFromProjection(retSpec projectionClauseSpec, projection fastPeerCandidateReturnProjection, params Params) (fastPeerCandidateTopKSpec, bool, error) {
	if len(retSpec.OrderBy) != 1 {
		return fastPeerCandidateTopKSpec{}, false, nil
	}
	if strings.TrimSpace(retSpec.LimitRaw) == "" {
		return fastPeerCandidateTopKSpec{}, false, nil
	}

	orderExpr := strings.TrimSpace(retSpec.OrderBy[0].Expression)
	if !strings.EqualFold(orderExpr, projection.sumSimilarityKey) && !strings.EqualFold(orderExpr, projection.sumSimilarityExpr) {
		return fastPeerCandidateTopKSpec{}, false, nil
	}

	skip, err := evalOptionalInt(rawExpression(retSpec.SkipRaw), params)
	if err != nil {
		return fastPeerCandidateTopKSpec{}, false, err
	}
	limit, err := evalOptionalInt(rawExpression(retSpec.LimitRaw), params)
	if err != nil {
		return fastPeerCandidateTopKSpec{}, false, err
	}
	if limit <= 0 {
		return fastPeerCandidateTopKSpec{descending: retSpec.OrderBy[0].Descending, skip: skip, limit: 0}, true, nil
	}

	return fastPeerCandidateTopKSpec{descending: retSpec.OrderBy[0].Descending, skip: skip, limit: limit}, true, nil
}

func fastPeerCandidateTopKRows(aggs map[string]*fastPeerCandidateAggregate, groupOrder []string, projection fastPeerCandidateReturnProjection, spec fastPeerCandidateTopKSpec, params Params) ([]Row, error) {
	if spec.limit <= 0 {
		return []Row{}, nil
	}

	keep := spec.skip + spec.limit
	if keep <= 0 {
		keep = spec.limit
	}
	top := &fastPeerCandidateTopKHeap{descending: spec.descending, rows: make([]fastPeerCandidateRankedRow, 0, keep)}

	for idx, groupID := range groupOrder {
		agg := aggs[groupID]
		if agg == nil || agg.edgeCount == 0 {
			continue
		}
		candidate := fastPeerCandidateRankedRow{agg: agg, score: agg.similaritySum, inputIndex: idx}
		if top.Len() < keep {
			heap.Push(top, candidate)
			continue
		}
		if compareFastPeerCandidateRank(candidate, top.rows[0], spec.descending) < 0 {
			top.rows[0] = candidate
			heap.Fix(top, 0)
		}
	}
	ranked := top.rows

	sort.Slice(ranked, func(i, j int) bool {
		return compareFastPeerCandidateRank(ranked[i], ranked[j], spec.descending) < 0
	})

	if spec.skip >= len(ranked) {
		return []Row{}, nil
	}
	end := len(ranked)
	if max := spec.skip + spec.limit; max < end {
		end = max
	}
	out := make([]Row, 0, end-spec.skip)
	for _, rankedRow := range ranked[spec.skip:end] {
		row, err := buildFastPeerCandidateResultRow(rankedRow.agg, projection, params)
		if err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, nil
}

type fastPeerCandidateTopKHeap struct {
	rows       []fastPeerCandidateRankedRow
	descending bool
}

func (h fastPeerCandidateTopKHeap) Len() int { return len(h.rows) }

func (h fastPeerCandidateTopKHeap) Less(i, j int) bool {
	// Max-heap by rank quality: root is the worst kept row for replacement checks.
	return compareFastPeerCandidateRank(h.rows[i], h.rows[j], h.descending) > 0
}

func (h fastPeerCandidateTopKHeap) Swap(i, j int) { h.rows[i], h.rows[j] = h.rows[j], h.rows[i] }

func (h *fastPeerCandidateTopKHeap) Push(x any) {
	h.rows = append(h.rows, x.(fastPeerCandidateRankedRow))
}

func (h *fastPeerCandidateTopKHeap) Pop() any {
	last := len(h.rows) - 1
	item := h.rows[last]
	h.rows = h.rows[:last]
	return item
}

func compareFastPeerCandidateRank(left, right fastPeerCandidateRankedRow, descending bool) int {
	if left.score != right.score {
		if descending {
			if left.score > right.score {
				return -1
			}
			return 1
		}
		if left.score < right.score {
			return -1
		}
		return 1
	}
	if left.inputIndex < right.inputIndex {
		return -1
	}
	if left.inputIndex > right.inputIndex {
		return 1
	}
	return 0
}

func parseFastTargetSharedPeerTopKWithClause(clause ast.Clause, prior fastTargetSharedPeerProjection, params Params) (fastTargetSharedPeerTopKProjection, fastTargetSharedPeerTopKSpec, bool, error) {
	withSpec, err := projectionClauseSpecFromClause(clause)
	if err != nil {
		return fastTargetSharedPeerTopKProjection{}, fastTargetSharedPeerTopKSpec{}, false, nil
	}
	if withSpec.Distinct || strings.TrimSpace(withSpec.WhereRaw) != "" || len(withSpec.OrderBy) != 1 || strings.TrimSpace(withSpec.LimitRaw) == "" {
		return fastTargetSharedPeerTopKProjection{}, fastTargetSharedPeerTopKSpec{}, false, nil
	}

	items, err := parseProjectionItems(withSpec.ProjectionRaw)
	if err != nil {
		return fastTargetSharedPeerTopKProjection{}, fastTargetSharedPeerTopKSpec{}, false, nil
	}
	if len(items) != 3 {
		return fastTargetSharedPeerTopKProjection{}, fastTargetSharedPeerTopKSpec{}, false, nil
	}

	projection := fastTargetSharedPeerTopKProjection{}
	for _, item := range items {
		if item.Expression == "*" || item.CollectArg != "" || item.CountArg != "" || item.AggFunc != "" {
			return fastTargetSharedPeerTopKProjection{}, fastTargetSharedPeerTopKSpec{}, false, nil
		}
		key := item.Expression
		if item.Alias != "" {
			key = item.Alias
		}
		expr := strings.TrimSpace(item.Expression)
		switch expr {
		case prior.targetKey, prior.targetExpr:
			projection.targetExpr = expr
			projection.targetKey = key
		case prior.peerKey, prior.peerExpr:
			projection.peerExpr = expr
			projection.peerKey = key
		default:
			if projection.similarityExpr != "" {
				return fastTargetSharedPeerTopKProjection{}, fastTargetSharedPeerTopKSpec{}, false, nil
			}
			projection.similarityExpr = expr
			projection.similarityKey = key
		}
	}
	if projection.targetKey == "" || projection.peerKey == "" || projection.similarityKey == "" || projection.similarityExpr == "" {
		return fastTargetSharedPeerTopKProjection{}, fastTargetSharedPeerTopKSpec{}, false, nil
	}

	orderExpr := strings.TrimSpace(withSpec.OrderBy[0].Expression)
	if !strings.EqualFold(orderExpr, projection.similarityKey) && !strings.EqualFold(orderExpr, projection.similarityExpr) {
		return fastTargetSharedPeerTopKProjection{}, fastTargetSharedPeerTopKSpec{}, false, nil
	}
	skip, err := evalOptionalInt(rawExpression(withSpec.SkipRaw), params)
	if err != nil {
		return fastTargetSharedPeerTopKProjection{}, fastTargetSharedPeerTopKSpec{}, false, err
	}
	limit, err := evalOptionalInt(rawExpression(withSpec.LimitRaw), params)
	if err != nil {
		return fastTargetSharedPeerTopKProjection{}, fastTargetSharedPeerTopKSpec{}, false, err
	}
	return projection, fastTargetSharedPeerTopKSpec{descending: withSpec.OrderBy[0].Descending, skip: skip, limit: limit}, true, nil
}

func fastTargetSharedPeerTopKRows(aggs map[string]*fastTargetSharedPeerAggregate, projection fastTargetSharedPeerProjection, withSpec projectionClauseSpec, topKProjection fastTargetSharedPeerTopKProjection, spec fastTargetSharedPeerTopKSpec, ctx context.Context, tx graph.Tx, params Params, exec *Executor) ([]Row, error) {
	if spec.limit <= 0 {
		return []Row{}, nil
	}

	params = withProjectionEvalRuntime(ctx, tx, params, exec)
	keep := spec.skip + spec.limit
	if keep <= 0 {
		keep = spec.limit
	}
	top := &fastTargetSharedPeerTopKHeap{descending: spec.descending, rows: make([]fastTargetSharedPeerRankedRow, 0, keep)}
	inputIndex := 0
	for _, agg := range sortedFastTargetSharedPeerAggregates(aggs) {
		if agg == nil || agg.shared <= 0 {
			continue
		}
		row := Row{}
		row[projection.targetKey] = agg.target
		row[projection.peerKey] = agg.peer
		row[projection.sharedCountKey] = agg.shared
		row[projection.avgDiffKey] = agg.sumAbsDiff / float64(agg.shared)
		if withSpec.WhereRaw != "" {
			ok, err := exec.evalWhereExpression(ctx, tx, withSpec.WhereRaw, row, params)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
		}
		similarityValue, err := evalExpressionWithScope(topKProjection.similarityExpr, row, params)
		if err != nil {
			return nil, err
		}
		score, ok := comparableNumericValue(similarityValue)
		if !ok {
			continue
		}
		trimmed := Row{}
		trimmed[topKProjection.targetKey] = row[projection.targetKey]
		trimmed[topKProjection.peerKey] = row[projection.peerKey]
		trimmed[topKProjection.similarityKey] = similarityValue
		candidate := fastTargetSharedPeerRankedRow{row: trimmed, score: score, inputIndex: inputIndex}
		inputIndex++
		if top.Len() < keep {
			heap.Push(top, candidate)
			continue
		}
		if compareFastTargetSharedPeerRank(candidate, top.rows[0], spec.descending) < 0 {
			top.rows[0] = candidate
			heap.Fix(top, 0)
		}
	}

	ranked := top.rows
	sort.Slice(ranked, func(i, j int) bool {
		return compareFastTargetSharedPeerRank(ranked[i], ranked[j], spec.descending) < 0
	})
	if spec.skip >= len(ranked) {
		return []Row{}, nil
	}
	end := len(ranked)
	if max := spec.skip + spec.limit; max < end {
		end = max
	}
	out := make([]Row, 0, end-spec.skip)
	for _, rankedRow := range ranked[spec.skip:end] {
		out = append(out, rankedRow.row)
	}
	return out, nil
}

type fastTargetSharedPeerTopKHeap struct {
	rows       []fastTargetSharedPeerRankedRow
	descending bool
}

func (h fastTargetSharedPeerTopKHeap) Len() int { return len(h.rows) }

func (h fastTargetSharedPeerTopKHeap) Less(i, j int) bool {
	return compareFastTargetSharedPeerRank(h.rows[i], h.rows[j], h.descending) > 0
}

func (h fastTargetSharedPeerTopKHeap) Swap(i, j int) { h.rows[i], h.rows[j] = h.rows[j], h.rows[i] }

func (h *fastTargetSharedPeerTopKHeap) Push(x any) {
	h.rows = append(h.rows, x.(fastTargetSharedPeerRankedRow))
}

func (h *fastTargetSharedPeerTopKHeap) Pop() any {
	last := len(h.rows) - 1
	item := h.rows[last]
	h.rows = h.rows[:last]
	return item
}

func compareFastTargetSharedPeerRank(left, right fastTargetSharedPeerRankedRow, descending bool) int {
	if left.score != right.score {
		if descending {
			if left.score > right.score {
				return -1
			}
			return 1
		}
		if left.score < right.score {
			return -1
		}
		return 1
	}
	if left.inputIndex < right.inputIndex {
		return -1
	}
	if left.inputIndex > right.inputIndex {
		return 1
	}
	return 0
}

func sortedFastTargetSharedPeerAggregates(aggs map[string]*fastTargetSharedPeerAggregate) []*fastTargetSharedPeerAggregate {
	out := make([]*fastTargetSharedPeerAggregate, 0, len(aggs))
	for _, agg := range aggs {
		if agg == nil {
			continue
		}
		out = append(out, agg)
	}
	sort.Slice(out, func(i, j int) bool {
		leftTarget := ""
		if out[i].target != nil {
			leftTarget = strings.TrimSpace(out[i].target.ID)
		}
		rightTarget := ""
		if out[j].target != nil {
			rightTarget = strings.TrimSpace(out[j].target.ID)
		}
		if leftTarget != rightTarget {
			return leftTarget < rightTarget
		}
		leftPeer := ""
		if out[i].peer != nil {
			leftPeer = strings.TrimSpace(out[i].peer.ID)
		}
		rightPeer := ""
		if out[j].peer != nil {
			rightPeer = strings.TrimSpace(out[j].peer.ID)
		}
		return leftPeer < rightPeer
	})
	return out
}

func validateUnionKinds(kinds []ast.UnionKind) error {
	if len(kinds) <= 1 {
		return nil
	}
	first := kinds[0]
	for _, kind := range kinds[1:] {
		if kind != first {
			return &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "InvalidClauseComposition", Statement: 1}
		}
	}
	return nil
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func hasWriteClause(part ast.QueryPart) bool {
	for _, clause := range part.Clauses {
		if isWriteClauseKind(clause.Kind) {
			return true
		}
	}
	return false
}

func isWriteClauseKind(kind ast.ClauseKind) bool {
	switch kind {
	case ast.ClauseKindCreate, ast.ClauseKindMerge, ast.ClauseKindSet, ast.ClauseKindRemove, ast.ClauseKindDelete:
		return true
	default:
		return false
	}
}

func (e *Executor) tryFastEdgeCountQuery(ctx context.Context, stmt *ast.QueryStatement, params Params) (*Result, bool, error) {
	if stmt == nil || len(stmt.Parts) != 1 || len(stmt.Unions) != 0 {
		return nil, false, nil
	}
	part := stmt.Parts[0]
	if len(part.Clauses) != 2 {
		return nil, false, nil
	}
	if part.Clauses[0].Kind != ast.ClauseKindMatch || part.Clauses[1].Kind != ast.ClauseKindReturn {
		return nil, false, nil
	}

	matchSpec, err := anchoredMatchSpecFromClause(part.Clauses[0])
	if err != nil || matchSpec.Optional || strings.TrimSpace(matchSpec.Where) != "" {
		return nil, false, nil
	}

	relPattern, err := parseUndirectedRelationshipPattern(matchSpec.Pattern)
	if err != nil {
		return nil, false, nil
	}
	if strings.TrimSpace(relPattern.EdgeVar) == "" || relPattern.EdgeType != "" || len(relPattern.EdgeAnyOf) != 0 || strings.TrimSpace(relPattern.EdgeProps) != "" {
		return nil, false, nil
	}
	if strings.TrimSpace(relPattern.Left.Var) != "" || strings.TrimSpace(relPattern.Right.Var) != "" {
		return nil, false, nil
	}

	leftLabel, leftAny, ok := fastEdgeCountVertexLabelFilter(relPattern.Left)
	if !ok {
		return nil, false, nil
	}
	rightLabel, rightAny, ok := fastEdgeCountVertexLabelFilter(relPattern.Right)
	if !ok {
		return nil, false, nil
	}

	projection, err := projectionClauseSpecFromClause(part.Clauses[1])
	if err != nil {
		return nil, false, nil
	}
	if projection.Distinct || strings.TrimSpace(projection.WhereRaw) != "" || len(projection.OrderBy) != 0 || strings.TrimSpace(projection.SkipRaw) != "" || strings.TrimSpace(projection.LimitRaw) != "" {
		return nil, false, nil
	}
	items, err := parseProjectionItems(projection.ProjectionRaw)
	if err != nil || len(items) != 1 {
		return nil, false, nil
	}
	item := items[0]
	if item.CollectArg != "" || item.AggFunc != "" {
		return nil, false, nil
	}
	countExpr, countDistinct := parseCountDistinctArg(item.CountArg)
	if countDistinct || strings.TrimSpace(countExpr) != relPattern.EdgeVar {
		return nil, false, nil
	}

	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, true, err
	}

	total := 0
	err = e.store.View(ctx, func(tx graph.Tx) error {
		switch {
		case leftAny && rightAny:
			count, err := countUndirectedEdgeMatchesFast(ctx, tx, tenant, "")
			if err != nil {
				return err
			}
			total = count
			return nil
		case !leftAny && rightAny:
			count, err := countUndirectedEdgeMatchesFast(ctx, tx, tenant, leftLabel)
			if err != nil {
				return err
			}
			total = count
			return nil
		case leftAny && !rightAny:
			count, err := countUndirectedEdgeMatchesFast(ctx, tx, tenant, rightLabel)
			if err != nil {
				return err
			}
			total = count
			return nil
		default:
			return graph.NewError(graph.ErrKindUnsupported, "fast edge count unavailable for this label combination", nil)
		}
	})
	if err != nil {
		if graph.IsKind(err, graph.ErrKindUnsupported) {
			return nil, false, nil
		}
		return nil, true, err
	}

	column := item.Expression
	if item.Alias != "" {
		column = item.Alias
	}
	return &Result{
		Columns: []string{column},
		Rows:    []Row{{column: total}},
		Stats:   Stats{RowsReturned: 1},
	}, true, nil
}

func fastEdgeCountVertexLabelFilter(pattern vertexPattern) (label string, any bool, ok bool) {
	if strings.TrimSpace(pattern.PropertiesRaw) != "" {
		return "", false, false
	}
	if len(pattern.AnyOfLabels) != 0 || len(pattern.ExcludedLabels) != 0 {
		return "", false, false
	}
	switch len(pattern.AllOfLabels) {
	case 0:
		return "", true, true
	case 1:
		return strings.TrimSpace(pattern.AllOfLabels[0]), false, true
	default:
		return "", false, false
	}
}

func countUndirectedEdgeMatchesFast(ctx context.Context, tx graph.Tx, tenant, leftLabel string) (int, error) {
	total := 0
	err := tx.ScanVertices(ctx, tenant, 0, func(vertex *graph.Vertex) error {
		if vertex == nil {
			return nil
		}
		if strings.TrimSpace(leftLabel) != "" && !vertexHasLabel(vertex, leftLabel) {
			return nil
		}

		emitted := map[string]struct{}{}
		if err := tx.ScanOutEdges(ctx, tenant, vertex.ID, "", 0, func(edge *graph.Edge) error {
			if edge == nil {
				return nil
			}
			key := edge.ID + "|" + edge.DstID
			if _, seen := emitted[key]; seen {
				return nil
			}
			emitted[key] = struct{}{}
			total++
			return nil
		}); err != nil {
			return err
		}
		return tx.ScanInEdges(ctx, tenant, vertex.ID, "", 0, func(edge *graph.Edge) error {
			if edge == nil {
				return nil
			}
			key := edge.ID + "|" + edge.SrcID
			if _, seen := emitted[key]; seen {
				return nil
			}
			emitted[key] = struct{}{}
			total++
			return nil
		})
	})
	return total, err
}

func (e *Executor) tryFastVertexCountQuery(ctx context.Context, stmt *ast.QueryStatement, params Params) (*Result, bool, error) {
	if stmt == nil || len(stmt.Parts) != 1 || len(stmt.Unions) != 0 {
		return nil, false, nil
	}
	part := stmt.Parts[0]
	if len(part.Clauses) != 2 {
		return nil, false, nil
	}
	if part.Clauses[0].Kind != ast.ClauseKindMatch || part.Clauses[1].Kind != ast.ClauseKindReturn {
		return nil, false, nil
	}

	matchSpec, err := anchoredMatchSpecFromClause(part.Clauses[0])
	if err != nil || matchSpec.Optional || strings.TrimSpace(matchSpec.Where) != "" {
		return nil, false, nil
	}
	vertexPattern, err := parseVertexPattern(matchSpec.Pattern)
	if err != nil {
		return nil, false, nil
	}
	vertexVar := strings.TrimSpace(vertexPattern.Var)
	if vertexVar == "" || strings.TrimSpace(vertexPattern.PropertiesRaw) != "" || len(vertexPattern.AllOfLabels) != 0 || len(vertexPattern.AnyOfLabels) != 0 || len(vertexPattern.ExcludedLabels) != 0 {
		return nil, false, nil
	}

	projection, err := projectionClauseSpecFromClause(part.Clauses[1])
	if err != nil {
		return nil, false, nil
	}
	if projection.Distinct || strings.TrimSpace(projection.WhereRaw) != "" || len(projection.OrderBy) != 0 || strings.TrimSpace(projection.SkipRaw) != "" || strings.TrimSpace(projection.LimitRaw) != "" {
		return nil, false, nil
	}
	items, err := parseProjectionItems(projection.ProjectionRaw)
	if err != nil || len(items) != 1 {
		return nil, false, nil
	}
	item := items[0]
	if item.CountArg == "" || item.CollectArg != "" || item.AggFunc != "" {
		return nil, false, nil
	}
	countExpr, countDistinct := parseCountDistinctArg(item.CountArg)
	if countDistinct || strings.TrimSpace(countExpr) != vertexVar {
		return nil, false, nil
	}

	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, true, err
	}

	total := 0
	err = e.store.View(ctx, func(tx graph.Tx) error {
		return tx.ScanVertices(ctx, tenant, 0, func(vertex *graph.Vertex) error {
			if vertex != nil {
				total++
			}
			return nil
		})
	})
	if err != nil {
		return nil, true, err
	}

	column := item.Expression
	if item.Alias != "" {
		column = item.Alias
	}
	return &Result{Columns: []string{column}, Rows: []Row{{column: total}}, Stats: Stats{RowsReturned: 1}}, true, nil
}

func (e *Executor) tryFastLabelHistogramQuery(ctx context.Context, stmt *ast.QueryStatement, params Params) (*Result, bool, error) {
	if stmt == nil || len(stmt.Parts) != 1 || len(stmt.Unions) != 0 {
		return nil, false, nil
	}
	part := stmt.Parts[0]
	if len(part.Clauses) != 2 {
		return nil, false, nil
	}
	if part.Clauses[0].Kind != ast.ClauseKindMatch || part.Clauses[1].Kind != ast.ClauseKindReturn {
		return nil, false, nil
	}

	matchSpec, err := anchoredMatchSpecFromClause(part.Clauses[0])
	if err != nil || matchSpec.Optional || strings.TrimSpace(matchSpec.Where) != "" {
		return nil, false, nil
	}
	vertexPattern, err := parseVertexPattern(matchSpec.Pattern)
	if err != nil {
		return nil, false, nil
	}
	vertexVar := strings.TrimSpace(vertexPattern.Var)
	if vertexVar == "" || strings.TrimSpace(vertexPattern.PropertiesRaw) != "" || len(vertexPattern.AnyOfLabels) != 0 || len(vertexPattern.ExcludedLabels) != 0 || len(vertexPattern.AllOfLabels) != 0 {
		return nil, false, nil
	}

	projection, err := projectionClauseSpecFromClause(part.Clauses[1])
	if err != nil {
		return nil, false, nil
	}
	if projection.Distinct || strings.TrimSpace(projection.WhereRaw) != "" || len(projection.OrderBy) != 0 || strings.TrimSpace(projection.SkipRaw) != "" || strings.TrimSpace(projection.LimitRaw) != "" {
		return nil, false, nil
	}
	items, err := parseProjectionItems(projection.ProjectionRaw)
	if err != nil || len(items) != 2 {
		return nil, false, nil
	}

	labelsIdx := -1
	countIdx := -1
	for idx, item := range items {
		if arg, ok := parseFunctionCall(item.Expression, "labels"); ok {
			if strings.TrimSpace(arg) == vertexVar && item.CountArg == "" && item.CollectArg == "" && item.AggFunc == "" {
				labelsIdx = idx
				continue
			}
		}
		if item.CountArg == "" || item.CollectArg != "" || item.AggFunc != "" {
			continue
		}
		countExpr, countDistinct := parseCountDistinctArg(item.CountArg)
		if countDistinct {
			continue
		}
		if arg, ok := parseFunctionCall(countExpr, "labels"); ok && strings.TrimSpace(arg) == vertexVar {
			countIdx = idx
		}
	}
	if labelsIdx < 0 || countIdx < 0 || labelsIdx == countIdx {
		return nil, false, nil
	}

	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, true, err
	}

	type labelGroup struct {
		labels []string
		count  int
	}
	groups := map[string]*labelGroup{}
	groupOrder := make([]string, 0)
	err = e.store.View(ctx, func(tx graph.Tx) error {
		return tx.ScanVertices(ctx, tenant, 0, func(vertex *graph.Vertex) error {
			if vertex == nil {
				return nil
			}
			labels := append([]string(nil), vertex.Labels...)
			keyBytes, marshalErr := json.Marshal(normalizeResultValue(labels))
			if marshalErr != nil {
				return graph.NewError(graph.ErrKindUnsupported, "labels histogram key is not serializable", marshalErr)
			}
			key := string(keyBytes)
			group, ok := groups[key]
			if !ok {
				group = &labelGroup{labels: labels}
				groups[key] = group
				groupOrder = append(groupOrder, key)
			}
			group.count++
			return nil
		})
	})
	if err != nil {
		return nil, true, err
	}

	columns := make([]string, len(items))
	for idx, item := range items {
		if item.Alias != "" {
			columns[idx] = item.Alias
		} else {
			columns[idx] = item.Expression
		}
	}

	rows := make([]Row, 0, len(groupOrder))
	for _, key := range groupOrder {
		group := groups[key]
		if group == nil {
			continue
		}
		row := Row{}
		row[columns[labelsIdx]] = append([]string(nil), group.labels...)
		row[columns[countIdx]] = group.count
		rows = append(rows, row)
	}

	return &Result{Columns: columns, Rows: rows, Stats: Stats{RowsReturned: len(rows)}}, true, nil
}

func (e *Executor) tryFastEdgeDeleteQuery(ctx context.Context, stmt *ast.QueryStatement, params Params) (*Result, bool, error) {
	if stmt == nil || len(stmt.Parts) != 1 || len(stmt.Unions) != 0 {
		return nil, false, nil
	}
	part := stmt.Parts[0]
	if len(part.Clauses) != 2 {
		return nil, false, nil
	}
	if part.Clauses[0].Kind != ast.ClauseKindMatch || part.Clauses[1].Kind != ast.ClauseKindDelete {
		return nil, false, nil
	}

	matchSpec, err := anchoredMatchSpecFromClause(part.Clauses[0])
	if err != nil || matchSpec.Optional || strings.TrimSpace(matchSpec.Where) != "" {
		return nil, false, nil
	}

	relPattern, err := parseUndirectedRelationshipPattern(matchSpec.Pattern)
	if err != nil {
		return nil, false, nil
	}
	if strings.TrimSpace(relPattern.EdgeVar) == "" || relPattern.EdgeType != "" || len(relPattern.EdgeAnyOf) != 0 || strings.TrimSpace(relPattern.EdgeProps) != "" {
		return nil, false, nil
	}
	if strings.TrimSpace(relPattern.Left.Var) != "" || strings.TrimSpace(relPattern.Right.Var) != "" {
		return nil, false, nil
	}

	leftLabel, leftAny, ok := fastEdgeCountVertexLabelFilter(relPattern.Left)
	if !ok {
		return nil, false, nil
	}
	rightLabel, rightAny, ok := fastEdgeCountVertexLabelFilter(relPattern.Right)
	if !ok {
		return nil, false, nil
	}
	if !leftAny && !rightAny {
		return nil, false, nil
	}

	deleteTarget, ok := fastDeleteEdgeTarget(part.Clauses[1].Raw)
	if !ok || deleteTarget != relPattern.EdgeVar {
		return nil, false, nil
	}

	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, true, err
	}

	err = e.store.Update(ctx, func(tx graph.Tx) error {
		edgeIDs, err := collectFastEdgeIDsForLabels(ctx, tx, tenant, leftLabel, rightLabel, leftAny, rightAny)
		if err != nil {
			return err
		}
		for edgeID := range edgeIDs {
			if err := tx.DeleteEdge(ctx, tenant, edgeID); err != nil {
				if graph.IsKind(err, graph.ErrKindNotFound) {
					continue
				}
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, true, err
	}

	return &Result{Columns: nil, Rows: nil, Stats: Stats{RowsReturned: 0}}, true, nil
}

func fastDeleteEdgeTarget(raw string) (string, bool) {
	normalized := normalizeClauseBody(raw)
	upper := strings.ToUpper(normalized)
	if strings.HasPrefix(upper, "DETACHDELETE") {
		return "", false
	}
	if !strings.HasPrefix(upper, "DELETE") {
		return "", false
	}
	body := strings.TrimSpace(normalized[len("DELETE"):])
	if body == "" || strings.Contains(body, ",") || !isIdentifierLike(body) {
		return "", false
	}
	return body, true
}

func collectFastEdgeIDsForLabels(ctx context.Context, tx graph.Tx, tenant, leftLabel, rightLabel string, leftAny, rightAny bool) (map[string]struct{}, error) {
	edgeIDs := map[string]struct{}{}
	type edgeIDScannerTx interface {
		ScanOutEdgeIDs(ctx context.Context, tenant, srcID, edgeType string, limit int, fn func(string) error) error
		ScanInEdgeIDs(ctx context.Context, tenant, dstID, edgeType string, limit int, fn func(string) error) error
	}

	collectAllOut := func(scanOut func(vertexID string, fn func(string) error) error) error {
		return tx.ScanVertices(ctx, tenant, 0, func(vertex *graph.Vertex) error {
			if vertex == nil {
				return nil
			}
			return scanOut(vertex.ID, func(edgeID string) error {
				edgeIDs[edgeID] = struct{}{}
				return nil
			})
		})
	}

	collectIncidentForLabel := func(label string, scanOutIn func(vertexID string, fn func(string) error) error) error {
		return tx.ScanVertices(ctx, tenant, 0, func(vertex *graph.Vertex) error {
			if !vertexHasLabel(vertex, label) {
				return nil
			}
			return scanOutIn(vertex.ID, func(edgeID string) error {
				edgeIDs[edgeID] = struct{}{}
				return nil
			})
		})
	}

	if scanner, ok := tx.(edgeIDScannerTx); ok {
		scanOut := func(vertexID string, fn func(string) error) error {
			return scanner.ScanOutEdgeIDs(ctx, tenant, vertexID, "", 0, fn)
		}
		scanOutIn := func(vertexID string, fn func(string) error) error {
			if err := scanner.ScanOutEdgeIDs(ctx, tenant, vertexID, "", 0, fn); err != nil {
				return err
			}
			return scanner.ScanInEdgeIDs(ctx, tenant, vertexID, "", 0, fn)
		}
		switch {
		case leftAny && rightAny:
			if err := collectAllOut(scanOut); err != nil {
				return nil, err
			}
		case !leftAny && rightAny:
			if err := collectIncidentForLabel(leftLabel, scanOutIn); err != nil {
				return nil, err
			}
		case leftAny && !rightAny:
			if err := collectIncidentForLabel(rightLabel, scanOutIn); err != nil {
				return nil, err
			}
		default:
			return nil, graph.NewError(graph.ErrKindUnsupported, "fast edge id collection unavailable for this label combination", nil)
		}
		return edgeIDs, nil
	}

	scanOut := func(vertexID string, fn func(string) error) error {
		return tx.ScanOutEdges(ctx, tenant, vertexID, "", 0, func(edge *graph.Edge) error {
			if edge == nil {
				return nil
			}
			return fn(edge.ID)
		})
	}
	scanOutIn := func(vertexID string, fn func(string) error) error {
		if err := tx.ScanOutEdges(ctx, tenant, vertexID, "", 0, func(edge *graph.Edge) error {
			if edge == nil {
				return nil
			}
			return fn(edge.ID)
		}); err != nil {
			return err
		}
		return tx.ScanInEdges(ctx, tenant, vertexID, "", 0, func(edge *graph.Edge) error {
			if edge == nil {
				return nil
			}
			return fn(edge.ID)
		})
	}

	switch {
	case leftAny && rightAny:
		if err := collectAllOut(scanOut); err != nil {
			return nil, err
		}
	case !leftAny && rightAny:
		if err := collectIncidentForLabel(leftLabel, scanOutIn); err != nil {
			return nil, err
		}
	case leftAny && !rightAny:
		if err := collectIncidentForLabel(rightLabel, scanOutIn); err != nil {
			return nil, err
		}
	default:
		return nil, graph.NewError(graph.ErrKindUnsupported, "fast edge id collection unavailable for this label combination", nil)
	}

	return edgeIDs, nil
}

func (e *Executor) applyMatchClause(ctx context.Context, tx graph.Tx, rows []Row, clause ast.Clause, params Params) ([]Row, error) {
	spec, err := anchoredMatchSpecFromClause(clause)
	if err != nil {
		return nil, err
	}
	patternRaw := spec.Pattern
	expansionSpec := spec
	pathVar := ""
	if boundVar, innerPattern, ok := parseBoundPathPattern(spec.Pattern); ok {
		pathVar = boundVar
		patternRaw = innerPattern
		expansionSpec.Pattern = innerPattern
	}
	if parts := splitTopLevelCommaSeparated(patternRaw); len(parts) > 1 {
		return e.expandCompositeMatch(ctx, tx, rows, spec, parts, params)
	}
	if multi, ok := parseMultiVertexMatchPattern(patternRaw); ok {
		return e.expandMultiVertexMatch(ctx, tx, rows, spec, multi, params)
	}
	if vertex, err := parseVertexPattern(patternRaw); err == nil {
		return e.expandVertexMatch(ctx, tx, rows, spec, vertex, params, pathVar)
	}
	if anchored, err := parseAnchoredOutPattern(patternRaw); err == nil {
		if shouldUseAnchoredOutPath(rows, anchored) {
			return e.expandAnchoredMatch(ctx, tx, rows, expansionSpec, params, pathVar)
		}
	}
	if directed, err := parseDirectedAdjacentPattern(patternRaw); err == nil {
		return e.expandDirectedAdjacentMatch(ctx, tx, rows, spec, directed, params, pathVar)
	}
	if reverseDirected, err := parseReverseDirectedAdjacentPattern(patternRaw); err == nil {
		return e.expandReverseDirectedAdjacentMatch(ctx, tx, rows, spec, reverseDirected, params, pathVar)
	}
	if undirected, err := parseUndirectedAdjacentPattern(patternRaw); err == nil {
		return e.expandUndirectedAdjacentMatch(ctx, tx, rows, spec, undirected, params, pathVar)
	}
	if rel, err := parseDirectedRelationshipPattern(patternRaw); err == nil {
		relForMatch := rel
		leftVar := rel.Left.Var
		rightVar := rel.Right.Var
		edgeVar := rel.EdgeVar
		cleanupVars := []string{}
		if pathVar != "" {
			if leftVar == "" {
				leftVar = "__ve_path_left"
				relForMatch.Left.Var = leftVar
				cleanupVars = append(cleanupVars, leftVar)
			}
			if rightVar == "" {
				rightVar = "__ve_path_right"
				relForMatch.Right.Var = rightVar
				cleanupVars = append(cleanupVars, rightVar)
			}
			if edgeVar == "" {
				edgeVar = "__ve_path_edge"
				relForMatch.EdgeVar = edgeVar
				cleanupVars = append(cleanupVars, edgeVar)
			}
		}
		matched, matchErr := e.expandDirectedRelationshipMatch(ctx, tx, rows, spec, relForMatch, params)
		if matchErr != nil {
			return nil, matchErr
		}
		if pathVar != "" {
			attachRelationshipPathValues(matched, pathVar, leftVar, edgeVar, rightVar, "forward")
			for _, merged := range matched {
				for _, key := range cleanupVars {
					delete(merged, key)
				}
			}
		}
		return matched, nil
	}
	if rel, err := parseReverseDirectedRelationshipPattern(patternRaw); err == nil {
		relForMatch := rel
		leftVar := rel.Left.Var
		rightVar := rel.Right.Var
		edgeVar := rel.EdgeVar
		cleanupVars := []string{}
		if pathVar != "" {
			if leftVar == "" {
				leftVar = "__ve_path_left"
				relForMatch.Left.Var = leftVar
				cleanupVars = append(cleanupVars, leftVar)
			}
			if rightVar == "" {
				rightVar = "__ve_path_right"
				relForMatch.Right.Var = rightVar
				cleanupVars = append(cleanupVars, rightVar)
			}
			if edgeVar == "" {
				edgeVar = "__ve_path_edge"
				relForMatch.EdgeVar = edgeVar
				cleanupVars = append(cleanupVars, edgeVar)
			}
		}
		matched, matchErr := e.expandReverseDirectedRelationshipMatch(ctx, tx, rows, spec, relForMatch, params)
		if matchErr != nil {
			return nil, matchErr
		}
		if pathVar != "" {
			attachRelationshipPathValues(matched, pathVar, leftVar, edgeVar, rightVar, "reverse")
			for _, merged := range matched {
				for _, key := range cleanupVars {
					delete(merged, key)
				}
			}
		}
		return matched, nil
	}
	if rel, err := parseUndirectedRelationshipPattern(patternRaw); err == nil {
		relForMatch := rel
		leftVar := rel.Left.Var
		rightVar := rel.Right.Var
		edgeVar := rel.EdgeVar
		cleanupVars := []string{}
		if pathVar != "" {
			if leftVar == "" {
				leftVar = "__ve_path_left"
				relForMatch.Left.Var = leftVar
				cleanupVars = append(cleanupVars, leftVar)
			}
			if rightVar == "" {
				rightVar = "__ve_path_right"
				relForMatch.Right.Var = rightVar
				cleanupVars = append(cleanupVars, rightVar)
			}
			if edgeVar == "" {
				edgeVar = "__ve_path_edge"
				relForMatch.EdgeVar = edgeVar
				cleanupVars = append(cleanupVars, edgeVar)
			}
		}
		matched, matchErr := e.expandUndirectedRelationshipMatch(ctx, tx, rows, spec, relForMatch, params)
		if matchErr != nil {
			return nil, matchErr
		}
		if pathVar != "" {
			attachRelationshipPathValues(matched, pathVar, leftVar, edgeVar, rightVar, "undirected")
			for _, merged := range matched {
				for _, key := range cleanupVars {
					delete(merged, key)
				}
			}
		}
		return matched, nil
	}
	if chain, err := parseDirectedRelationshipThenAdjacentPattern(patternRaw); err == nil {
		return e.expandDirectedRelationshipThenAdjacentMatch(ctx, tx, rows, spec, chain, params, pathVar)
	}
	if chain, err := parseDirectedThenUndirectedRelationshipChainPattern(patternRaw); err == nil {
		return e.expandDirectedThenUndirectedRelationshipChainMatch(ctx, tx, rows, spec, chain, params, pathVar)
	}
	if chain, err := parseReverseRelationshipThenUndirectedVariableLengthPattern(patternRaw); err == nil {
		return e.expandReverseRelationshipThenUndirectedVariableLengthMatch(ctx, tx, rows, spec, chain, params, pathVar)
	}
	if chain, err := parseDirectedAdjacentThenVariableLengthPattern(patternRaw); err == nil {
		return e.expandDirectedAdjacentThenVariableLengthMatch(ctx, tx, rows, spec, chain, params, pathVar)
	}
	if chain, err := parseDirectedVariableLengthThenDirectedVariableLengthPattern(patternRaw); err == nil {
		return e.expandDirectedVariableLengthThenDirectedVariableLengthMatch(ctx, tx, rows, spec, chain, params, pathVar)
	}
	if rewritten, ok := rewriteReverseVariableLengthPatternPredicate(patternRaw); ok {
		if rel, err := parseDirectedVariableLengthRelationshipPattern(rewritten); err == nil {
			return e.expandVariableLengthDirectedRelationshipMatch(ctx, tx, rows, spec, rel, params, pathVar)
		}
	}
	if rel, err := parseDirectedVariableLengthRelationshipPattern(patternRaw); err == nil {
		return e.expandVariableLengthDirectedRelationshipMatch(ctx, tx, rows, spec, rel, params, pathVar)
	}
	if rel, err := parseUndirectedVariableLengthRelationshipPattern(patternRaw); err == nil {
		return e.expandVariableLengthUndirectedRelationshipMatch(ctx, tx, rows, spec, rel, params, pathVar)
	}
	if chain, err := parseTwoHopDirectedChainPattern(patternRaw); err == nil {
		return e.expandTwoHopDirectedChainMatch(ctx, tx, rows, spec, chain, params, pathVar)
	}
	if chain, err := parseTwoHopUndirectedRelationshipChainPattern(patternRaw); err == nil {
		return e.expandTwoHopUndirectedRelationshipChainMatch(ctx, tx, rows, spec, chain, params, pathVar)
	}
	if chain, err := parseMixedRelationshipChainPattern(patternRaw); err == nil {
		return e.expandMixedRelationshipChainMatch(ctx, tx, rows, spec, chain, params, pathVar)
	}
	if chain, err := parseMultiHopAdjacentChainPattern(patternRaw); err == nil {
		matched, matchErr := e.expandMultiHopAdjacentChainMatch(ctx, tx, rows, spec, chain, params, pathVar)
		if matchErr != nil {
			return nil, matchErr
		}
		ensureOptionalPathBinding(matched, pathVar)
		return matched, nil
	}
	return e.expandAnchoredMatch(ctx, tx, rows, expansionSpec, params, pathVar)
}

func anchoredMatchSpecFromClause(clause ast.Clause) (anchoredMatchSpec, error) {
	if strings.TrimSpace(clause.MatchPattern) != "" {
		spec := anchoredMatchSpec{
			Optional: clause.MatchOptional || clause.Kind == ast.ClauseKindOptionalMatch,
			Pattern:  strings.TrimSpace(clause.MatchPattern),
		}
		if clause.Where != nil {
			spec.Where = strings.TrimSpace(clause.Where.Raw)
		}
		return spec, nil
	}
	return parseAnchoredMatchClauseRaw(clause.Raw)
}

func (e *Executor) expandCompositeMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, parts []string, params Params) ([]Row, error) {
	if len(rows) == 0 {
		rows = []Row{{}}
	}

	raw := strings.TrimSpace(spec.Pattern)
	if raw == "" {
		return rows, nil
	}
	matchVars := inferMatchScopeColumns("MATCH " + raw)

	out := make([]Row, 0)
	for _, row := range rows {
		partials := []Row{cloneRow(row)}
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			next, err := e.applyMatchClause(ctx, tx, partials, ast.Clause{Kind: ast.ClauseKindMatch, Raw: "MATCH " + part, MatchPattern: part, MatchOptional: false}, params)
			if err != nil {
				return nil, err
			}
			partials = next
			if len(partials) == 0 {
				break
			}
		}

		matched := false
		if len(partials) > 0 {
			for _, partial := range partials {
				if spec.Where != "" {
					ok, err := e.evalWhereExpression(ctx, tx, spec.Where, partial, params)
					if err != nil {
						return nil, err
					}
					if !ok {
						continue
					}
				}
				matched = true
				out = append(out, partial)
			}
		}

		if spec.Optional && !matched {
			merged := cloneRow(row)
			for _, name := range matchVars {
				setOptionalNoMatchBinding(merged, row, name)
			}
			out = append(out, merged)
		}
	}

	return out, nil
}

func parseBoundPathPattern(raw string) (string, string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", false
	}
	depthParen := 0
	depthBracket := 0
	depthBrace := 0
	inSingle := false
	inDouble := false
	for i := 0; i < len(raw); i++ {
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
		case '(':
			depthParen++
		case ')':
			depthParen--
		case '[':
			depthBracket++
		case ']':
			depthBracket--
		case '{':
			depthBrace++
		case '}':
			depthBrace--
		case '=':
			if depthParen == 0 && depthBracket == 0 && depthBrace == 0 {
				left := strings.TrimSpace(raw[:i])
				right := strings.TrimSpace(raw[i+1:])
				if !identifierRE.MatchString(left) || !strings.HasPrefix(right, "(") {
					return "", "", false
				}
				return left, right, true
			}
		}
	}
	return "", "", false
}

func attachRelationshipPathValues(rows []Row, pathVar, leftVar, edgeVar, rightVar, direction string) {
	if pathVar == "" {
		return
	}
	for _, row := range rows {
		edge := edgeFromRowBinding(row, edgeVar)
		right := vertexFromRowBinding(row, rightVar)
		if edge == nil || right == nil {
			row[pathVar] = nil
			continue
		}
		left := vertexFromRowBinding(row, leftVar)
		row[pathVar] = cypherPathValue{Left: left, Edge: edge, Right: right, Direction: direction}
	}
}

type cypherPathValue struct {
	Left      *graph.Vertex
	Edge      *graph.Edge
	Right     *graph.Vertex
	Direction string
}

func (p cypherPathValue) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{
		"__path__":   true,
		"vertexes":   []any{vertexToMap(p.Left), vertexToMap(p.Right)},
		"edges":      []any{edgeToMap(p.Edge)},
		"directions": []any{p.Direction},
	})
}

func (p cypherPathValue) String() string {
	left := renderPathVertex(p.Left)
	if p.Edge == nil && p.Right == nil {
		return "<" + left + ">"
	}
	right := renderPathVertex(p.Right)
	edge := renderPathEdge(p.Edge)
	switch p.Direction {
	case "reverse":
		return "<" + left + "<-" + edge + "-" + right + ">"
	case "undirected":
		return "<" + left + "-" + edge + "-" + right + ">"
	default:
		return "<" + left + "-" + edge + "->" + right + ">"
	}
}

func renderPathVertex(v *graph.Vertex) string {
	if v == nil {
		return "()"
	}
	labels := append([]string(nil), v.Labels...)
	b := strings.Builder{}
	b.WriteString("(")
	for _, label := range labels {
		b.WriteString(":")
		b.WriteString(label)
	}
	if len(v.Properties) > 0 {
		parts := make([]string, 0, len(v.Properties))
		for key, raw := range v.Properties {
			parts = append(parts, key+": "+renderPathLiteral(decodeStoredPropertyValue(raw)))
		}
		sort.Strings(parts)
		if len(labels) > 0 {
			b.WriteString(" ")
		}
		b.WriteString("{")
		b.WriteString(strings.Join(parts, ", "))
		b.WriteString("}")
	}
	b.WriteString(")")
	return b.String()
}

func renderPathEdge(e *graph.Edge) string {
	if e == nil {
		return "[]"
	}
	b := strings.Builder{}
	b.WriteString("[")
	if strings.TrimSpace(e.Type) != "" {
		b.WriteString(":")
		b.WriteString(e.Type)
	}
	if len(e.Properties) > 0 {
		parts := make([]string, 0, len(e.Properties))
		for key, raw := range e.Properties {
			parts = append(parts, key+": "+renderPathLiteral(decodeStoredPropertyValue(raw)))
		}
		sort.Strings(parts)
		if strings.TrimSpace(e.Type) != "" {
			b.WriteString(" ")
		}
		b.WriteString("{")
		b.WriteString(strings.Join(parts, ", "))
		b.WriteString("}")
	}
	b.WriteString("]")
	return b.String()
}

func renderPathLiteral(v any) string {
	switch typed := normalizeResultValue(v).(type) {
	case nil:
		return "null"
	case string:
		return "'" + strings.ReplaceAll(typed, "'", "\\'") + "'"
	case bool:
		if typed {
			return "true"
		}
		return "false"
	case []any:
		parts := make([]string, 0, len(typed))
		for _, item := range typed {
			parts = append(parts, renderPathLiteral(item))
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for k := range typed {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		parts := make([]string, 0, len(keys))
		for _, k := range keys {
			parts = append(parts, k+": "+renderPathLiteral(typed[k]))
		}
		return "{" + strings.Join(parts, ", ") + "}"
	default:
		return fmt.Sprint(typed)
	}
}

func vertexFromRowBinding(row Row, key string) *graph.Vertex {
	if strings.TrimSpace(key) == "" || row == nil {
		return nil
	}
	if value, ok := row[key]; ok {
		if vertex, ok := value.(*graph.Vertex); ok {
			return vertex
		}
	}
	return nil
}

func edgeFromRowBinding(row Row, key string) *graph.Edge {
	if row == nil {
		return nil
	}
	if strings.TrimSpace(key) != "" {
		if value, ok := row[key]; ok {
			if edge, ok := value.(*graph.Edge); ok {
				return edge
			}
		}
	}
	if value, ok := row["edge"]; ok {
		if edge, ok := value.(*graph.Edge); ok {
			return edge
		}
	}
	return nil
}

func parseMultiVertexMatchPattern(raw string) ([]vertexPattern, bool) {
	parts := splitTopLevelCommaSeparated(raw)
	if len(parts) <= 1 {
		return nil, false
	}
	out := make([]vertexPattern, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, false
		}
		vertex, err := parseVertexPattern(part)
		if err != nil {
			return nil, false
		}
		out = append(out, vertex)
	}
	return out, true
}

func (e *Executor) expandMultiVertexMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, patterns []vertexPattern, params Params) ([]Row, error) {
	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		rows = []Row{{}}
	}

	out := make([]Row, 0)
	for _, row := range rows {
		partials := []Row{cloneRow(row)}
		for _, pattern := range patterns {
			next := make([]Row, 0)
			for _, partial := range partials {
				candidates, err := e.resolveVertexPatternCandidates(ctx, tx, tenant, partial, pattern, params)
				if err != nil {
					return nil, err
				}
				if len(candidates) == 0 {
					continue
				}
				for _, candidate := range candidates {
					merged := cloneRow(partial)
					if pattern.Var != "" {
						merged[pattern.Var] = candidate
					}
					next = append(next, merged)
				}
			}
			partials = next
			if len(partials) == 0 {
				break
			}
		}

		if len(partials) == 0 {
			if spec.Optional {
				merged := cloneRow(row)
				for _, pattern := range patterns {
					if pattern.Var != "" {
						merged[pattern.Var] = nil
					}
				}
				out = append(out, merged)
			}
			continue
		}

		for _, partial := range partials {
			if spec.Where != "" {
				ok, err := e.evalWhereExpression(ctx, tx, spec.Where, partial, params)
				if err != nil {
					return nil, err
				}
				if !ok {
					continue
				}
			}
			out = append(out, partial)
		}
	}

	return out, nil
}

func shouldUseAnchoredOutPath(rows []Row, pattern anchoredOutPattern) bool {
	if strings.TrimSpace(pattern.SourcePropertiesRaw) != "" {
		return true
	}
	if strings.TrimSpace(pattern.SourceIDParam) != "" {
		return true
	}
	if strings.TrimSpace(pattern.SourceVar) == "" {
		return false
	}
	for _, row := range rows {
		if _, ok := row[pattern.SourceVar]; ok {
			return true
		}
	}
	return false
}

type anchoredMatchSpec struct {
	Optional      bool
	Pattern       string
	SourceVar     string
	SourceIDParam string
	EdgeType      string
	TargetVar     string
	Where         string
}

func parseAnchoredMatchClauseRaw(raw string) (anchoredMatchSpec, error) {
	raw = normalizeClauseBody(raw)
	spec := anchoredMatchSpec{}
	if strings.HasPrefix(raw, "OPTIONALMATCH") {
		spec.Optional = true
		raw = strings.TrimPrefix(raw, "OPTIONALMATCH")
	} else if strings.HasPrefix(raw, "MATCH") {
		raw = strings.TrimPrefix(raw, "MATCH")
	} else {
		return anchoredMatchSpec{}, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("match clause %q is not supported", raw), nil)
	}
	pattern, where, ok := splitTopLevelMatchWhere(raw)
	if !ok {
		spec.Pattern = pattern
		return spec, nil
	}
	spec.Pattern = pattern
	spec.Where = where
	return spec, nil
}

func splitTopLevelMatchWhere(raw string) (string, string, bool) {
	upper := strings.ToUpper(raw)
	depth := 0
	inSingle := false
	inDouble := false
	keyword := "WHERE"

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

		if depth != 0 || !strings.HasPrefix(upper[i:], keyword) {
			continue
		}
		if i > 0 && isAlphaNumericOrUnderscore(raw[i-1]) {
			continue
		}

		left := strings.TrimSpace(raw[:i])
		right := strings.TrimSpace(raw[i+len(keyword):])
		if left == "" || right == "" {
			continue
		}
		return raw[:i], raw[i+len(keyword):], true
	}

	return raw, "", false
}

func isAlphaNumericOrUnderscore(ch byte) bool {
	if ch == '_' {
		return true
	}
	if ch >= '0' && ch <= '9' {
		return true
	}
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}

func (e *Executor) expandAnchoredMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, params Params, pathVar string) ([]Row, error) {
	pattern, err := parseAnchoredOutPattern(spec.Pattern)
	if err != nil {
		return nil, err
	}

	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		rows = []Row{{}}
	}

	out := make([]Row, 0)
	for _, row := range rows {
		sources, err := e.resolveAnchoredSources(ctx, tx, tenant, row, pattern, params)
		if err != nil {
			return nil, err
		}
		if len(sources) == 0 {
			if spec.Optional {
				merged := cloneRow(row)
				setOptionalNoMatchBinding(merged, row, pattern.SourceVar)
				setOptionalNoMatchBinding(merged, row, pattern.TargetVar)
				merged["edge"] = nil
				if pathVar != "" {
					merged[pathVar] = nil
				}
				out = append(out, merged)
			}
			continue
		}

		matched := false
		for _, src := range sources {
			if src == nil {
				continue
			}
			if !vertexBindingMatches(row, pattern.SourceVar, src) {
				continue
			}
			srcID := src.ID
			if err := tx.ScanOutEdges(ctx, tenant, srcID, pattern.EdgeType, 0, func(edge *graph.Edge) error {
				dst, err := tx.GetVertex(ctx, tenant, edge.DstID)
				if err != nil {
					if spec.Optional && graph.IsKind(err, graph.ErrKindNotFound) {
						return nil
					}
					return err
				}
				if !vertexBindingMatches(row, pattern.TargetVar, dst) {
					return nil
				}

				merged := cloneRow(row)
				merged[pattern.SourceVar] = src
				merged[pattern.TargetVar] = dst
				merged["edge"] = edge
				if pathVar != "" {
					merged[pathVar] = cypherPathValue{Left: src, Edge: edge, Right: dst, Direction: "forward"}
				}

				if spec.Where != "" {
					ok, err := e.evalWhereExpression(ctx, tx, spec.Where, merged, params)
					if err != nil {
						return err
					}
					if !ok {
						return nil
					}
				}

				matched = true
				out = append(out, merged)
				return nil
			}); err != nil {
				return nil, err
			}
		}

		if spec.Optional && !matched {
			merged := cloneRow(row)
			setOptionalNoMatchBinding(merged, row, pattern.SourceVar)
			setOptionalNoMatchBinding(merged, row, pattern.TargetVar)
			merged["edge"] = nil
			if pathVar != "" {
				merged[pathVar] = nil
			}
			out = append(out, merged)
		}
	}

	return out, nil
}

func (e *Executor) resolveAnchoredSources(ctx context.Context, tx graph.Tx, tenant string, row Row, pattern anchoredOutPattern, params Params) ([]*graph.Vertex, error) {
	if prop, value, ok := anchoredSourcePropertyEquality(pattern, params, row); ok {
		indexed := e.indexCatalog != nil && pattern.SourceLabel != "" && e.indexCatalog.HasPropertyIndex(tenant, pattern.SourceLabel, prop)
		e.metrics.ObserveIndexCandidate(tenant, pattern.SourceLabel, prop, indexed)
		if !indexed {
			return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("MATCH source property lookup requires configured index on %s.%s", pattern.SourceLabel, prop), nil)
		}

		encoded := valueToBytes(value)
		ids := map[string]struct{}{}
		err := tx.ScanPropertyIndex(ctx, tenant, pattern.SourceLabel, prop, encoded, 0, func(entry *graph.PropertyIndexEntry) error {
			ids[entry.EntityID] = struct{}{}
			return nil
		})
		if err != nil {
			e.metrics.ObserveIndexLookup("property_index", "error", 0)
			return nil, err
		}
		if len(ids) == 0 {
			e.metrics.ObserveIndexLookup("property_index", "miss", 0)
			return nil, nil
		}
		out := make([]*graph.Vertex, 0, len(ids))
		for id := range ids {
			vertex, err := tx.GetVertex(ctx, tenant, id)
			if err != nil {
				if graph.IsKind(err, graph.ErrKindNotFound) {
					continue
				}
				return nil, err
			}
			if !vertexMatchesProperty(vertex, prop, encoded, pattern.SourceLabel) {
				continue
			}
			out = append(out, vertex)
		}
		e.metrics.ObserveIndexLookup("property_index", "hit", len(out))
		return out, nil
	}

	srcID, err := resolvePatternSourceID(row, params, pattern.SourceVar, pattern.SourceIDParam)
	if err != nil {
		e.metrics.ObserveIndexLookup("id_lookup", "error", 0)
		return nil, err
	}
	vertex, err := tx.GetVertex(ctx, tenant, srcID)
	if err != nil {
		e.metrics.ObserveIndexLookup("id_lookup", "error", 0)
		return nil, err
	}
	e.metrics.ObserveIndexLookup("id_lookup", "hit", 1)
	return []*graph.Vertex{vertex}, nil
}

func (e *Executor) expandVertexMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, pattern vertexPattern, params Params, pathVar string) ([]Row, error) {
	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		rows = []Row{{}}
	}

	out := make([]Row, 0)
	for _, row := range rows {
		candidates, err := e.resolveVertexPatternCandidates(ctx, tx, tenant, row, pattern, params)
		if err != nil {
			return nil, err
		}

		matched := false
		for _, candidate := range candidates {
			if candidate == nil {
				continue
			}
			merged := cloneRow(row)
			if pathVar != "" {
				merged[pathVar] = cypherPathValue{Left: candidate}
			}
			if pattern.Var != "" {
				merged[pattern.Var] = candidate
			}

			if spec.Where != "" {
				ok, err := e.evalWhereExpression(ctx, tx, spec.Where, merged, params)
				if err != nil {
					return nil, err
				}
				if !ok {
					continue
				}
			}

			matched = true
			out = append(out, merged)
		}

		if spec.Optional && !matched {
			merged := cloneRow(row)
			if pathVar != "" {
				merged[pathVar] = nil
			}
			setOptionalNoMatchBinding(merged, row, pattern.Var)
			out = append(out, merged)
		}
	}

	return out, nil
}

func (e *Executor) expandUndirectedAdjacentMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, pattern undirectedAdjacentPattern, params Params, pathVar string) ([]Row, error) {
	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		rows = []Row{{}}
	}

	out := make([]Row, 0)
	for _, row := range rows {
		if pathVar != "" {
			row = cloneRow(row)
			row[pathVar] = nil
		}
		leftCandidates, err := e.resolveVertexPatternCandidates(ctx, tx, tenant, row, pattern.Left, params)
		if err != nil {
			return nil, err
		}

		matched := false
		for _, left := range leftCandidates {
			if left == nil {
				continue
			}
			emitted := map[string]struct{}{}
			rowWithLeft := cloneRow(row)
			if pattern.Left.Var != "" {
				rowWithLeft[pattern.Left.Var] = left
			}

			handleAdjacent := func(edge *graph.Edge, otherID string) error {
				if edge == nil {
					return nil
				}
				key := edge.ID + "|" + otherID
				if _, seen := emitted[key]; seen {
					return nil
				}
				emitted[key] = struct{}{}

				neighbor, err := tx.GetVertex(ctx, tenant, otherID)
				if err != nil {
					if graph.IsKind(err, graph.ErrKindNotFound) {
						return nil
					}
					return err
				}
				if !vertexBindingMatches(rowWithLeft, pattern.Right.Var, neighbor) {
					return nil
				}
				if !vertexPatternMatches(neighbor, pattern.Right, params, rowWithLeft) {
					return nil
				}

				merged := cloneRow(rowWithLeft)
				if pattern.Left.Var != "" {
					merged[pattern.Left.Var] = left
				}
				if pattern.Right.Var != "" {
					merged[pattern.Right.Var] = neighbor
				}
				if pathVar != "" {
					direction := "undirected"
					if edge.SrcID == left.ID && edge.DstID == neighbor.ID {
						direction = "forward"
					} else if edge.DstID == left.ID && edge.SrcID == neighbor.ID {
						direction = "reverse"
					}
					merged[pathVar] = cypherPathValue{Left: left, Edge: edge, Right: neighbor, Direction: direction}
				}

				if spec.Where != "" {
					ok, err := e.evalWhereExpression(ctx, tx, spec.Where, merged, params)
					if err != nil {
						return err
					}
					if !ok {
						return nil
					}
				}

				matched = true
				out = append(out, merged)
				return nil
			}

			if err := tx.ScanOutEdges(ctx, tenant, left.ID, "", 0, func(edge *graph.Edge) error {
				return handleAdjacent(edge, edge.DstID)
			}); err != nil {
				return nil, err
			}
			if err := tx.ScanInEdges(ctx, tenant, left.ID, "", 0, func(edge *graph.Edge) error {
				return handleAdjacent(edge, edge.SrcID)
			}); err != nil {
				return nil, err
			}
		}

		if spec.Optional && !matched {
			merged := cloneRow(row)
			setOptionalNoMatchBinding(merged, row, pattern.Left.Var)
			setOptionalNoMatchBinding(merged, row, pattern.Right.Var)
			out = append(out, merged)
		}
	}

	return out, nil
}

func (e *Executor) expandDirectedAdjacentMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, pattern directedAdjacentPattern, params Params, pathVar string) ([]Row, error) {
	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		rows = []Row{{}}
	}

	out := make([]Row, 0)
	for _, row := range rows {
		if pathVar != "" {
			row = cloneRow(row)
			row[pathVar] = nil
		}
		leftCandidates, err := e.resolveVertexPatternCandidates(ctx, tx, tenant, row, pattern.Left, params)
		if err != nil {
			return nil, err
		}

		matched := false
		for _, left := range leftCandidates {
			if left == nil {
				continue
			}
			rowWithLeft := cloneRow(row)
			if pattern.Left.Var != "" {
				rowWithLeft[pattern.Left.Var] = left
			}

			if err := tx.ScanOutEdges(ctx, tenant, left.ID, "", 0, func(edge *graph.Edge) error {
				neighbor, err := tx.GetVertex(ctx, tenant, edge.DstID)
				if err != nil {
					if graph.IsKind(err, graph.ErrKindNotFound) {
						return nil
					}
					return err
				}
				if !vertexBindingMatches(rowWithLeft, pattern.Right.Var, neighbor) {
					return nil
				}
				if !vertexPatternMatches(neighbor, pattern.Right, params, rowWithLeft) {
					return nil
				}

				merged := cloneRow(rowWithLeft)
				if pattern.Left.Var != "" {
					merged[pattern.Left.Var] = left
				}
				if pattern.Right.Var != "" {
					merged[pattern.Right.Var] = neighbor
				}
				if pathVar != "" {
					merged[pathVar] = cypherPathValue{Left: left, Edge: edge, Right: neighbor, Direction: "forward"}
				}

				if spec.Where != "" {
					ok, err := e.evalWhereExpression(ctx, tx, spec.Where, merged, params)
					if err != nil {
						return err
					}
					if !ok {
						return nil
					}
				}

				matched = true
				out = append(out, merged)
				return nil
			}); err != nil {
				return nil, err
			}
		}

		if spec.Optional && !matched {
			merged := cloneRow(row)
			setOptionalNoMatchBinding(merged, row, pattern.Left.Var)
			setOptionalNoMatchBinding(merged, row, pattern.Right.Var)
			out = append(out, merged)
		}
	}

	return out, nil
}

func (e *Executor) expandReverseDirectedAdjacentMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, pattern reverseDirectedAdjacentPattern, params Params, pathVar string) ([]Row, error) {
	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		rows = []Row{{}}
	}

	out := make([]Row, 0)
	for _, row := range rows {
		rightCandidates, err := e.resolveVertexPatternCandidates(ctx, tx, tenant, row, pattern.Right, params)
		if err != nil {
			return nil, err
		}

		matched := false
		for _, right := range rightCandidates {
			if right == nil {
				continue
			}
			rowWithRight := cloneRow(row)
			if pattern.Right.Var != "" {
				rowWithRight[pattern.Right.Var] = right
			}

			if err := tx.ScanOutEdges(ctx, tenant, right.ID, "", 0, func(edge *graph.Edge) error {
				left, err := tx.GetVertex(ctx, tenant, edge.DstID)
				if err != nil {
					if graph.IsKind(err, graph.ErrKindNotFound) {
						return nil
					}
					return err
				}
				if !vertexBindingMatches(rowWithRight, pattern.Left.Var, left) {
					return nil
				}
				if !vertexPatternMatches(left, pattern.Left, params, rowWithRight) {
					return nil
				}

				merged := cloneRow(rowWithRight)
				if pattern.Left.Var != "" {
					merged[pattern.Left.Var] = left
				}
				if pattern.Right.Var != "" {
					merged[pattern.Right.Var] = right
				}
				if pathVar != "" {
					merged[pathVar] = cypherPathValue{Left: left, Edge: edge, Right: right, Direction: "reverse"}
				}

				if spec.Where != "" {
					ok, err := e.evalWhereExpression(ctx, tx, spec.Where, merged, params)
					if err != nil {
						return err
					}
					if !ok {
						return nil
					}
				}

				matched = true
				out = append(out, merged)
				return nil
			}); err != nil {
				return nil, err
			}
		}

		if spec.Optional && !matched {
			merged := cloneRow(row)
			setOptionalNoMatchBinding(merged, row, pattern.Left.Var)
			setOptionalNoMatchBinding(merged, row, pattern.Right.Var)
			out = append(out, merged)
		}
	}

	return out, nil
}

func (e *Executor) expandDirectedRelationshipMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, pattern directedRelationshipPattern, params Params) ([]Row, error) {
	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		rows = []Row{{}}
	}

	out := make([]Row, 0)
	for _, row := range rows {
		numericConstraints, hasNumericConstraints := extractEdgeWhereNumericConstraints(spec.Where, pattern.EdgeVar, row, params)
		excludedRightIDs, hasExcludedRightIDs, err := extractDirectedWhereRightExclusionSet(ctx, tx, tenant, spec.Where, pattern.Right.Var, row, params)
		if err != nil {
			return nil, err
		}
		skipWhereEval := directedWhereCoveredByExtractedPrefilters(spec.Where, pattern.EdgeVar, pattern.Right.Var, row, params, hasNumericConstraints, hasExcludedRightIDs)
		if indexedEdges, indexed, _, err := e.resolveEdgesByIndexedProperty(ctx, tx, tenant, pattern.EdgeType, pattern.EdgeAnyOf, pattern.EdgeProps, pattern.EdgeVar, spec.Where, pattern.Left.Var, row, params, nil, 0); err != nil {
			return nil, err
		} else if indexed {
			matched := false
			for _, edge := range indexedEdges {
				if hasNumericConstraints && !edgeMatchesNumericConstraints(edge, numericConstraints) {
					continue
				}
				if hasExcludedRightIDs {
					if _, blocked := excludedRightIDs[edge.DstID]; blocked {
						continue
					}
				}
				if !edgeBindingMatches(row, pattern.EdgeVar, edge) {
					continue
				}
				left, err := getVertexQueryCached(ctx, tx, tenant, edge.SrcID, params)
				if err != nil {
					return nil, err
				}
				if left == nil {
					continue
				}
				if !vertexBindingMatches(row, pattern.Left.Var, left) {
					continue
				}
				if !vertexPatternMatches(left, pattern.Left, params, row) {
					continue
				}
				rowWithLeft := cloneRow(row)
				if pattern.Left.Var != "" {
					rowWithLeft[pattern.Left.Var] = left
				}

				right, err := getVertexQueryCached(ctx, tx, tenant, edge.DstID, params)
				if err != nil {
					return nil, err
				}
				if right == nil {
					continue
				}
				if !vertexBindingMatches(rowWithLeft, pattern.Right.Var, right) {
					continue
				}
				if !vertexPatternMatches(right, pattern.Right, params, rowWithLeft) {
					continue
				}

				merged := cloneRow(rowWithLeft)
				if pattern.Right.Var != "" {
					merged[pattern.Right.Var] = right
				}
				if pattern.EdgeVar != "" {
					merged[pattern.EdgeVar] = edge
				}
				if spec.Where != "" && !skipWhereEval {
					ok, err := e.evalWhereExpression(ctx, tx, spec.Where, merged, params)
					if err != nil {
						return nil, err
					}
					if !ok {
						continue
					}
				}
				matched = true
				out = append(out, merged)
			}
			if spec.Optional && !matched {
				merged := cloneRow(row)
				setOptionalNoMatchBinding(merged, row, pattern.Left.Var)
				setOptionalNoMatchBinding(merged, row, pattern.Right.Var)
				setOptionalNoMatchBinding(merged, row, pattern.EdgeVar)
				out = append(out, merged)
			}
			continue
		}

		leftCandidates, err := e.resolveVertexPatternCandidates(ctx, tx, tenant, row, pattern.Left, params)
		if err != nil {
			return nil, err
		}

		matched := false
		for _, left := range leftCandidates {
			if left == nil {
				continue
			}
			rowWithLeft := cloneRow(row)
			if pattern.Left.Var != "" {
				rowWithLeft[pattern.Left.Var] = left
			}

			scanType := pattern.EdgeType
			if len(pattern.EdgeAnyOf) > 0 {
				scanType = ""
			}
			if err := scanOutEdgesQueryCached(ctx, tx, tenant, left.ID, scanType, params, func(edge *graph.Edge) error {
				if !edgeTypeMatches(edge, pattern.EdgeType, pattern.EdgeAnyOf) {
					return nil
				}
				if hasNumericConstraints && !edgeMatchesNumericConstraints(edge, numericConstraints) {
					return nil
				}
				if hasExcludedRightIDs {
					if _, blocked := excludedRightIDs[edge.DstID]; blocked {
						return nil
					}
				}
				if !edgeBindingMatches(rowWithLeft, pattern.EdgeVar, edge) {
					return nil
				}
				if !edgePatternMatches(edge, pattern.EdgeProps, params, row) {
					return nil
				}
				neighbor, err := getVertexQueryCached(ctx, tx, tenant, edge.DstID, params)
				if err != nil {
					return err
				}
				if neighbor == nil {
					return nil
				}
				if !vertexBindingMatches(rowWithLeft, pattern.Right.Var, neighbor) {
					return nil
				}
				if !vertexPatternMatches(neighbor, pattern.Right, params, rowWithLeft) {
					return nil
				}

				merged := cloneRow(rowWithLeft)
				if pattern.Left.Var != "" {
					merged[pattern.Left.Var] = left
				}
				if pattern.Right.Var != "" {
					merged[pattern.Right.Var] = neighbor
				}
				if pattern.EdgeVar != "" {
					merged[pattern.EdgeVar] = edge
				}

				if spec.Where != "" && !skipWhereEval {
					ok, err := e.evalWhereExpression(ctx, tx, spec.Where, merged, params)
					if err != nil {
						return err
					}
					if !ok {
						return nil
					}
				}

				matched = true
				out = append(out, merged)
				return nil
			}); err != nil {
				return nil, err
			}
		}

		if spec.Optional && !matched {
			merged := cloneRow(row)
			setOptionalNoMatchBinding(merged, row, pattern.Left.Var)
			setOptionalNoMatchBinding(merged, row, pattern.Right.Var)
			setOptionalNoMatchBinding(merged, row, pattern.EdgeVar)
			out = append(out, merged)
		}
	}

	return out, nil
}

func (e *Executor) expandReverseDirectedRelationshipMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, pattern reverseDirectedRelationshipPattern, params Params) ([]Row, error) {
	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		rows = []Row{{}}
	}

	out := make([]Row, 0)
	for _, row := range rows {
		if indexedEdges, indexed, _, err := e.resolveEdgesByIndexedProperty(ctx, tx, tenant, pattern.EdgeType, pattern.EdgeAnyOf, pattern.EdgeProps, pattern.EdgeVar, spec.Where, pattern.Left.Var, row, params, nil, 0); err != nil {
			return nil, err
		} else if indexed {
			matched := false
			for _, edge := range indexedEdges {
				if !edgeBindingMatches(row, pattern.EdgeVar, edge) {
					continue
				}
				left, err := getVertexQueryCached(ctx, tx, tenant, edge.DstID, params)
				if err != nil {
					return nil, err
				}
				if left == nil {
					continue
				}
				if !vertexBindingMatches(row, pattern.Left.Var, left) {
					continue
				}
				if !vertexPatternMatches(left, pattern.Left, params, row) {
					continue
				}
				rowWithLeft := cloneRow(row)
				if pattern.Left.Var != "" {
					rowWithLeft[pattern.Left.Var] = left
				}

				right, err := getVertexQueryCached(ctx, tx, tenant, edge.SrcID, params)
				if err != nil {
					return nil, err
				}
				if right == nil {
					continue
				}
				if !vertexBindingMatches(rowWithLeft, pattern.Right.Var, right) {
					continue
				}
				if !vertexPatternMatches(right, pattern.Right, params, rowWithLeft) {
					continue
				}

				merged := cloneRow(rowWithLeft)
				if pattern.Right.Var != "" {
					merged[pattern.Right.Var] = right
				}
				if pattern.EdgeVar != "" {
					merged[pattern.EdgeVar] = edge
				}
				if spec.Where != "" {
					ok, err := e.evalWhereExpression(ctx, tx, spec.Where, merged, params)
					if err != nil {
						return nil, err
					}
					if !ok {
						continue
					}
				}
				matched = true
				out = append(out, merged)
			}
			if spec.Optional && !matched {
				merged := cloneRow(row)
				setOptionalNoMatchBinding(merged, row, pattern.Left.Var)
				setOptionalNoMatchBinding(merged, row, pattern.Right.Var)
				setOptionalNoMatchBinding(merged, row, pattern.EdgeVar)
				out = append(out, merged)
			}
			continue
		}

		leftCandidates, err := e.resolveVertexPatternCandidates(ctx, tx, tenant, row, pattern.Left, params)
		if err != nil {
			return nil, err
		}

		matched := false
		for _, left := range leftCandidates {
			if left == nil {
				continue
			}
			rowWithLeft := cloneRow(row)
			if pattern.Left.Var != "" {
				rowWithLeft[pattern.Left.Var] = left
			}

			scanType := pattern.EdgeType
			if len(pattern.EdgeAnyOf) > 0 {
				scanType = ""
			}
			if err := scanInEdgesQueryCached(ctx, tx, tenant, left.ID, scanType, params, func(edge *graph.Edge) error {
				if !edgeTypeMatches(edge, pattern.EdgeType, pattern.EdgeAnyOf) {
					return nil
				}
				if !edgeBindingMatches(rowWithLeft, pattern.EdgeVar, edge) {
					return nil
				}
				if !edgePatternMatches(edge, pattern.EdgeProps, params, rowWithLeft) {
					return nil
				}
				right, err := tx.GetVertex(ctx, tenant, edge.SrcID)
				if err != nil {
					if graph.IsKind(err, graph.ErrKindNotFound) {
						return nil
					}
					return err
				}
				if !vertexBindingMatches(rowWithLeft, pattern.Right.Var, right) {
					return nil
				}
				if !vertexPatternMatches(right, pattern.Right, params, rowWithLeft) {
					return nil
				}

				merged := cloneRow(rowWithLeft)
				if pattern.Left.Var != "" {
					merged[pattern.Left.Var] = left
				}
				if pattern.Right.Var != "" {
					merged[pattern.Right.Var] = right
				}
				if pattern.EdgeVar != "" {
					merged[pattern.EdgeVar] = edge
				}

				if spec.Where != "" {
					ok, err := e.evalWhereExpression(ctx, tx, spec.Where, merged, params)
					if err != nil {
						return err
					}
					if !ok {
						return nil
					}
				}

				matched = true
				out = append(out, merged)
				return nil
			}); err != nil {
				return nil, err
			}
		}

		if spec.Optional && !matched {
			merged := cloneRow(row)
			setOptionalNoMatchBinding(merged, row, pattern.Left.Var)
			setOptionalNoMatchBinding(merged, row, pattern.Right.Var)
			setOptionalNoMatchBinding(merged, row, pattern.EdgeVar)
			out = append(out, merged)
		}
	}

	return out, nil
}

func (e *Executor) expandUndirectedRelationshipMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, pattern undirectedRelationshipPattern, params Params) ([]Row, error) {
	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		rows = []Row{{}}
	}

	out := make([]Row, 0)
	for _, row := range rows {
		leftCandidates, err := e.resolveVertexPatternCandidates(ctx, tx, tenant, row, pattern.Left, params)
		if err != nil {
			return nil, err
		}

		matched := false
		for _, left := range leftCandidates {
			if left == nil {
				continue
			}
			emitted := map[string]struct{}{}
			rowWithLeft := cloneRow(row)
			if pattern.Left.Var != "" {
				rowWithLeft[pattern.Left.Var] = left
			}

			handle := func(edge *graph.Edge, otherID string) error {
				key := edge.ID + "|" + otherID
				if _, seen := emitted[key]; seen {
					return nil
				}
				emitted[key] = struct{}{}

				if !edgeBindingMatches(rowWithLeft, pattern.EdgeVar, edge) {
					return nil
				}

				if !edgePatternMatches(edge, pattern.EdgeProps, params, rowWithLeft) {
					return nil
				}
				neighbor, err := tx.GetVertex(ctx, tenant, otherID)
				if err != nil {
					if graph.IsKind(err, graph.ErrKindNotFound) {
						return nil
					}
					return err
				}
				if !vertexBindingMatches(rowWithLeft, pattern.Right.Var, neighbor) {
					return nil
				}
				if !vertexPatternMatches(neighbor, pattern.Right, params, rowWithLeft) {
					return nil
				}

				merged := cloneRow(rowWithLeft)
				if pattern.Left.Var != "" {
					merged[pattern.Left.Var] = left
				}
				if pattern.Right.Var != "" {
					merged[pattern.Right.Var] = neighbor
				}
				if pattern.EdgeVar != "" {
					merged[pattern.EdgeVar] = edge
				}

				if spec.Where != "" {
					ok, err := e.evalWhereExpression(ctx, tx, spec.Where, merged, params)
					if err != nil {
						return err
					}
					if !ok {
						return nil
					}
				}

				matched = true
				out = append(out, merged)
				return nil
			}

			scanType := pattern.EdgeType
			if len(pattern.EdgeAnyOf) > 0 {
				scanType = ""
			}
			if err := scanOutEdgesQueryCached(ctx, tx, tenant, left.ID, scanType, params, func(edge *graph.Edge) error {
				if !edgeTypeMatches(edge, pattern.EdgeType, pattern.EdgeAnyOf) {
					return nil
				}
				return handle(edge, edge.DstID)
			}); err != nil {
				return nil, err
			}
			if err := scanInEdgesQueryCached(ctx, tx, tenant, left.ID, scanType, params, func(edge *graph.Edge) error {
				if !edgeTypeMatches(edge, pattern.EdgeType, pattern.EdgeAnyOf) {
					return nil
				}
				return handle(edge, edge.SrcID)
			}); err != nil {
				return nil, err
			}
		}

		if spec.Optional && !matched {
			merged := cloneRow(row)
			setOptionalNoMatchBinding(merged, row, pattern.Left.Var)
			setOptionalNoMatchBinding(merged, row, pattern.Right.Var)
			setOptionalNoMatchBinding(merged, row, pattern.EdgeVar)
			out = append(out, merged)
		}
	}

	return out, nil
}

func edgeSequenceBindingMatches(row Row, varName string, edges []*graph.Edge) bool {
	if strings.TrimSpace(varName) == "" {
		return true
	}
	binding, ok := row[varName]
	if !ok {
		return true
	}
	if binding == nil {
		return false
	}

	sameIDs := func(bound []*graph.Edge) bool {
		if len(bound) != len(edges) {
			return false
		}
		for i := range bound {
			if bound[i] == nil || edges[i] == nil {
				return false
			}
			if bound[i].ID != edges[i].ID {
				return false
			}
		}
		return true
	}

	switch typed := binding.(type) {
	case []*graph.Edge:
		return sameIDs(typed)
	case []any:
		if len(typed) != len(edges) {
			return false
		}
		for i, item := range typed {
			edge, ok := item.(*graph.Edge)
			if !ok || edge == nil || edges[i] == nil || edge.ID != edges[i].ID {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func edgeSequenceToAny(edges []*graph.Edge) []any {
	out := make([]any, len(edges))
	for i, edge := range edges {
		out[i] = edge
	}
	return out
}

func (e *Executor) expandVariableLengthDirectedRelationshipMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, pattern directedVariableLengthRelationshipPattern, params Params, pathVar string) ([]Row, error) {
	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		rows = []Row{{}}
	}

	out := make([]Row, 0)
	for _, row := range rows {
		leftCandidates, err := e.resolveVertexPatternCandidates(ctx, tx, tenant, row, pattern.Left, params)
		if err != nil {
			return nil, err
		}

		matched := false
		for _, left := range leftCandidates {
			if left == nil {
				continue
			}

			baseRow := cloneRow(row)
			if pattern.Left.Var != "" {
				baseRow[pattern.Left.Var] = left
			}

			emitMatch := func(current *graph.Vertex, vertexes []*graph.Vertex, edges []*graph.Edge, dirs []string) error {
				depth := len(edges)
				if depth < pattern.MinHops {
					return nil
				}
				if pattern.MaxHops >= 0 && depth > pattern.MaxHops {
					return nil
				}
				if !vertexBindingMatches(baseRow, pattern.Right.Var, current) {
					return nil
				}

				merged := cloneRow(baseRow)
				if pattern.Right.Var != "" {
					merged[pattern.Right.Var] = current
				}
				if !edgeSequenceBindingMatches(baseRow, pattern.EdgeVar, edges) {
					return nil
				}
				if pattern.EdgeVar != "" {
					merged[pattern.EdgeVar] = edgeSequenceToAny(edges)
				}
				if pathVar != "" {
					merged[pathVar] = multiHopPathValue(vertexes, edges, dirs)
				}
				if !vertexPatternMatches(current, pattern.Right, params, merged) {
					return nil
				}
				if spec.Where != "" {
					ok, err := e.evalWhereExpression(ctx, tx, spec.Where, merged, params)
					if err != nil {
						return err
					}
					if !ok {
						return nil
					}
				}

				matched = true
				out = append(out, merged)
				return nil
			}

			if err := emitMatch(left, []*graph.Vertex{left}, []*graph.Edge{}, []string{}); err != nil {
				return nil, err
			}

			var walk func(current *graph.Vertex, vertexes []*graph.Vertex, edges []*graph.Edge, dirs []string, used map[string]bool) error
			walk = func(current *graph.Vertex, vertexes []*graph.Vertex, edges []*graph.Edge, dirs []string, used map[string]bool) error {
				if pattern.MaxHops >= 0 && len(edges) >= pattern.MaxHops {
					return nil
				}
				scanType := pattern.EdgeType
				if len(pattern.EdgeAnyOf) > 0 {
					scanType = ""
				}
				return tx.ScanOutEdges(ctx, tenant, current.ID, scanType, 0, func(edge *graph.Edge) error {
					if edge == nil || used[edge.ID] {
						return nil
					}
					if !edgeTypeMatches(edge, pattern.EdgeType, pattern.EdgeAnyOf) {
						return nil
					}
					if !edgePatternMatches(edge, pattern.EdgeProps, params, baseRow) {
						return nil
					}
					right, err := tx.GetVertex(ctx, tenant, edge.DstID)
					if err != nil {
						if graph.IsKind(err, graph.ErrKindNotFound) {
							return nil
						}
						return err
					}

					nextVertexes := append(append([]*graph.Vertex{}, vertexes...), right)
					nextEdges := append(append([]*graph.Edge{}, edges...), edge)
					nextDirs := append(append([]string{}, dirs...), "forward")

					nextUsed := make(map[string]bool, len(used)+1)
					for key := range used {
						nextUsed[key] = true
					}
					nextUsed[edge.ID] = true

					if err := emitMatch(right, nextVertexes, nextEdges, nextDirs); err != nil {
						return err
					}

					return walk(right, nextVertexes, nextEdges, nextDirs, nextUsed)
				})
			}

			if err := walk(left, []*graph.Vertex{left}, nil, nil, map[string]bool{}); err != nil {
				return nil, err
			}
		}

		if spec.Optional && !matched {
			merged := cloneRow(row)
			setOptionalNoMatchBinding(merged, row, pattern.Left.Var)
			setOptionalNoMatchBinding(merged, row, pattern.Right.Var)
			setOptionalNoMatchBinding(merged, row, pattern.EdgeVar)
			if pathVar != "" {
				merged[pathVar] = nil
			}
			out = append(out, merged)
		}
	}

	return out, nil
}

func (e *Executor) expandDirectedVariableLengthThenDirectedVariableLengthMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, pattern directedVariableLengthThenDirectedVariableLengthPattern, params Params, pathVar string) ([]Row, error) {
	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		rows = []Row{{}}
	}

	out := make([]Row, 0)
	for _, row := range rows {
		leftCandidates, err := e.resolveVertexPatternCandidates(ctx, tx, tenant, row, pattern.Left, params)
		if err != nil {
			return nil, err
		}

		matched := false
		for _, left := range leftCandidates {
			if left == nil {
				continue
			}

			baseRow := cloneRow(row)
			if pattern.Left.Var != "" {
				baseRow[pattern.Left.Var] = left
			}

			var walkSecond func(current *graph.Vertex, vertexes []*graph.Vertex, edges []*graph.Edge, dirs []string, used map[string]bool, midRow Row, firstEdgeCount int) error
			walkSecond = func(current *graph.Vertex, vertexes []*graph.Vertex, edges []*graph.Edge, dirs []string, used map[string]bool, midRow Row, firstEdgeCount int) error {
				depthSecond := len(edges) - firstEdgeCount
				if depthSecond >= pattern.SecondMinHops && (pattern.SecondMaxHops < 0 || depthSecond <= pattern.SecondMaxHops) {
					if vertexBindingMatches(midRow, pattern.Right.Var, current) {
						merged := cloneRow(midRow)
						if pattern.Right.Var != "" {
							merged[pattern.Right.Var] = current
						}
						if pathVar != "" {
							merged[pathVar] = multiHopPathValue(vertexes, edges, dirs)
						}
						if vertexPatternMatches(current, pattern.Right, params, merged) {
							if spec.Where != "" {
								ok, err := e.evalWhereExpression(ctx, tx, spec.Where, merged, params)
								if err != nil {
									return err
								}
								if ok {
									matched = true
									out = append(out, merged)
								}
							} else {
								matched = true
								out = append(out, merged)
							}
						}
					}
				}
				if pattern.SecondMaxHops >= 0 && depthSecond >= pattern.SecondMaxHops {
					return nil
				}
				scanType := pattern.SecondEdgeType
				if len(pattern.SecondEdgeAnyOf) > 0 {
					scanType = ""
				}
				return tx.ScanOutEdges(ctx, tenant, current.ID, scanType, 0, func(edge *graph.Edge) error {
					if edge == nil || used[edge.ID] {
						return nil
					}
					if !edgeTypeMatches(edge, pattern.SecondEdgeType, pattern.SecondEdgeAnyOf) {
						return nil
					}
					if !edgePatternMatches(edge, pattern.SecondEdgeProps, params, midRow) {
						return nil
					}
					next, err := tx.GetVertex(ctx, tenant, edge.DstID)
					if err != nil {
						if graph.IsKind(err, graph.ErrKindNotFound) {
							return nil
						}
						return err
					}
					nextVertexes := append(append([]*graph.Vertex{}, vertexes...), next)
					nextEdges := append(append([]*graph.Edge{}, edges...), edge)
					nextDirs := append(append([]string{}, dirs...), "forward")
					nextUsed := make(map[string]bool, len(used)+1)
					for key := range used {
						nextUsed[key] = true
					}
					nextUsed[edge.ID] = true
					return walkSecond(next, nextVertexes, nextEdges, nextDirs, nextUsed, midRow, firstEdgeCount)
				})
			}

			var walkFirst func(current *graph.Vertex, vertexes []*graph.Vertex, edges []*graph.Edge, dirs []string, used map[string]bool) error
			walkFirst = func(current *graph.Vertex, vertexes []*graph.Vertex, edges []*graph.Edge, dirs []string, used map[string]bool) error {
				depthFirst := len(edges)
				if depthFirst >= pattern.FirstMinHops {
					if pattern.Mid.Var == "" || vertexBindingMatches(baseRow, pattern.Mid.Var, current) {
						midRow := cloneRow(baseRow)
						if pattern.Mid.Var != "" {
							midRow[pattern.Mid.Var] = current
						}
						if vertexPatternMatches(current, pattern.Mid, params, midRow) {
							usedForSecond := make(map[string]bool, len(used))
							for key := range used {
								usedForSecond[key] = true
							}
							if err := walkSecond(current, vertexes, edges, dirs, usedForSecond, midRow, depthFirst); err != nil {
								return err
							}
						}
					}
				}

				if pattern.FirstMaxHops >= 0 && depthFirst >= pattern.FirstMaxHops {
					return nil
				}
				scanType := pattern.FirstEdgeType
				if len(pattern.FirstEdgeAnyOf) > 0 {
					scanType = ""
				}
				return tx.ScanOutEdges(ctx, tenant, current.ID, scanType, 0, func(edge *graph.Edge) error {
					if edge == nil || used[edge.ID] {
						return nil
					}
					if !edgeTypeMatches(edge, pattern.FirstEdgeType, pattern.FirstEdgeAnyOf) {
						return nil
					}
					if !edgePatternMatches(edge, pattern.FirstEdgeProps, params, baseRow) {
						return nil
					}
					next, err := tx.GetVertex(ctx, tenant, edge.DstID)
					if err != nil {
						if graph.IsKind(err, graph.ErrKindNotFound) {
							return nil
						}
						return err
					}
					nextVertexes := append(append([]*graph.Vertex{}, vertexes...), next)
					nextEdges := append(append([]*graph.Edge{}, edges...), edge)
					nextDirs := append(append([]string{}, dirs...), "forward")
					nextUsed := make(map[string]bool, len(used)+1)
					for key := range used {
						nextUsed[key] = true
					}
					nextUsed[edge.ID] = true
					return walkFirst(next, nextVertexes, nextEdges, nextDirs, nextUsed)
				})
			}

			if err := walkFirst(left, []*graph.Vertex{left}, nil, nil, map[string]bool{}); err != nil {
				return nil, err
			}
		}

		if spec.Optional && !matched {
			merged := cloneRow(row)
			setOptionalNoMatchBinding(merged, row, pattern.Left.Var)
			setOptionalNoMatchBinding(merged, row, pattern.Mid.Var)
			setOptionalNoMatchBinding(merged, row, pattern.Right.Var)
			if pathVar != "" {
				merged[pathVar] = nil
			}
			out = append(out, merged)
		}
	}

	return out, nil
}

func (e *Executor) expandVariableLengthUndirectedRelationshipMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, pattern undirectedVariableLengthRelationshipPattern, params Params, pathVar string) ([]Row, error) {
	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		rows = []Row{{}}
	}

	out := make([]Row, 0)
	for _, row := range rows {
		leftCandidates, err := e.resolveVertexPatternCandidates(ctx, tx, tenant, row, pattern.Left, params)
		if err != nil {
			return nil, err
		}

		matched := false
		for _, left := range leftCandidates {
			if left == nil {
				continue
			}

			baseRow := cloneRow(row)
			if pattern.Left.Var != "" {
				baseRow[pattern.Left.Var] = left
			}

			emitMatch := func(current *graph.Vertex, vertexes []*graph.Vertex, edges []*graph.Edge, dirs []string) error {
				depth := len(edges)
				if depth < pattern.MinHops {
					return nil
				}
				if pattern.MaxHops >= 0 && depth > pattern.MaxHops {
					return nil
				}
				if !vertexBindingMatches(baseRow, pattern.Right.Var, current) {
					return nil
				}

				merged := cloneRow(baseRow)
				if pattern.Right.Var != "" {
					merged[pattern.Right.Var] = current
				}
				if !edgeSequenceBindingMatches(baseRow, pattern.EdgeVar, edges) {
					return nil
				}
				if pattern.EdgeVar != "" {
					merged[pattern.EdgeVar] = edgeSequenceToAny(edges)
				}
				if pathVar != "" {
					merged[pathVar] = multiHopPathValue(vertexes, edges, dirs)
				}
				if !vertexPatternMatches(current, pattern.Right, params, merged) {
					return nil
				}
				if spec.Where != "" {
					ok, err := e.evalWhereExpression(ctx, tx, spec.Where, merged, params)
					if err != nil {
						return err
					}
					if !ok {
						return nil
					}
				}

				matched = true
				out = append(out, merged)
				return nil
			}

			if err := emitMatch(left, []*graph.Vertex{left}, []*graph.Edge{}, []string{}); err != nil {
				return nil, err
			}

			var walk func(current *graph.Vertex, vertexes []*graph.Vertex, edges []*graph.Edge, dirs []string, used map[string]bool) error
			walk = func(current *graph.Vertex, vertexes []*graph.Vertex, edges []*graph.Edge, dirs []string, used map[string]bool) error {
				if pattern.MaxHops >= 0 && len(edges) >= pattern.MaxHops {
					return nil
				}
				type neighborEdge struct {
					edge     *graph.Edge
					neighbor *graph.Vertex
					dir      string
				}
				neighbors := make([]neighborEdge, 0)
				seen := map[string]struct{}{}
				collect := func(edge *graph.Edge, neighborID string, dir string) error {
					if edge == nil || used[edge.ID] {
						return nil
					}
					if !edgeTypeMatches(edge, pattern.EdgeType, pattern.EdgeAnyOf) {
						return nil
					}
					if !edgePatternMatches(edge, pattern.EdgeProps, params, baseRow) {
						return nil
					}
					key := edge.ID + "|" + neighborID
					if _, ok := seen[key]; ok {
						return nil
					}
					seen[key] = struct{}{}
					neighbor, err := tx.GetVertex(ctx, tenant, neighborID)
					if err != nil {
						if graph.IsKind(err, graph.ErrKindNotFound) {
							return nil
						}
						return err
					}
					neighbors = append(neighbors, neighborEdge{edge: edge, neighbor: neighbor, dir: dir})
					return nil
				}

				scanType := pattern.EdgeType
				if len(pattern.EdgeAnyOf) > 0 {
					scanType = ""
				}
				if err := tx.ScanOutEdges(ctx, tenant, current.ID, scanType, 0, func(edge *graph.Edge) error {
					return collect(edge, edge.DstID, "forward")
				}); err != nil {
					return err
				}
				if err := tx.ScanInEdges(ctx, tenant, current.ID, scanType, 0, func(edge *graph.Edge) error {
					return collect(edge, edge.SrcID, "reverse")
				}); err != nil {
					return err
				}

				for _, candidate := range neighbors {
					nextVertexes := append(append([]*graph.Vertex{}, vertexes...), candidate.neighbor)
					nextEdges := append(append([]*graph.Edge{}, edges...), candidate.edge)
					nextDirs := append(append([]string{}, dirs...), candidate.dir)

					nextUsed := make(map[string]bool, len(used)+1)
					for key := range used {
						nextUsed[key] = true
					}
					nextUsed[candidate.edge.ID] = true

					if err := emitMatch(candidate.neighbor, nextVertexes, nextEdges, nextDirs); err != nil {
						return err
					}

					if err := walk(candidate.neighbor, nextVertexes, nextEdges, nextDirs, nextUsed); err != nil {
						return err
					}
				}
				return nil
			}

			if err := walk(left, []*graph.Vertex{left}, nil, nil, map[string]bool{}); err != nil {
				return nil, err
			}
		}

		if spec.Optional && !matched {
			merged := cloneRow(row)
			setOptionalNoMatchBinding(merged, row, pattern.Left.Var)
			setOptionalNoMatchBinding(merged, row, pattern.Right.Var)
			setOptionalNoMatchBinding(merged, row, pattern.EdgeVar)
			if pathVar != "" {
				merged[pathVar] = nil
			}
			out = append(out, merged)
		}
	}

	return out, nil
}

func (e *Executor) expandDirectedAdjacentThenVariableLengthMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, pattern directedAdjacentThenVariableLengthPattern, params Params, pathVar string) ([]Row, error) {
	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		rows = []Row{{}}
	}

	out := make([]Row, 0)
	for _, row := range rows {
		leftCandidates, err := e.resolveVertexPatternCandidates(ctx, tx, tenant, row, pattern.Left, params)
		if err != nil {
			return nil, err
		}

		matched := false
		for _, left := range leftCandidates {
			if left == nil {
				continue
			}

			if err := tx.ScanOutEdges(ctx, tenant, left.ID, "", 0, func(edge1 *graph.Edge) error {
				mid, err := tx.GetVertex(ctx, tenant, edge1.DstID)
				if err != nil {
					if graph.IsKind(err, graph.ErrKindNotFound) {
						return nil
					}
					return err
				}

				midRow := cloneRow(row)
				if pattern.Left.Var != "" {
					midRow[pattern.Left.Var] = left
				}
				if !vertexBindingMatches(midRow, pattern.Mid.Var, mid) {
					return nil
				}
				if pattern.Mid.Var != "" {
					midRow[pattern.Mid.Var] = mid
				}
				if !vertexPatternMatches(mid, pattern.Mid, params, midRow) {
					return nil
				}

				var walk func(current *graph.Vertex, vertexes []*graph.Vertex, edges []*graph.Edge, dirs []string, used map[string]bool) error
				walk = func(current *graph.Vertex, vertexes []*graph.Vertex, edges []*graph.Edge, dirs []string, used map[string]bool) error {
					return tx.ScanOutEdges(ctx, tenant, current.ID, "", 0, func(edge *graph.Edge) error {
						if edge == nil || used[edge.ID] {
							return nil
						}
						right, err := tx.GetVertex(ctx, tenant, edge.DstID)
						if err != nil {
							if graph.IsKind(err, graph.ErrKindNotFound) {
								return nil
							}
							return err
						}

						nextVertexes := append(append([]*graph.Vertex{}, vertexes...), right)
						nextEdges := append(append([]*graph.Edge{}, edges...), edge)
						nextDirs := append(append([]string{}, dirs...), "forward")

						nextUsed := make(map[string]bool, len(used)+1)
						for key := range used {
							nextUsed[key] = true
						}
						nextUsed[edge.ID] = true

						if vertexBindingMatches(midRow, pattern.Right.Var, right) {
							merged := cloneRow(midRow)
							if pattern.Right.Var != "" {
								merged[pattern.Right.Var] = right
							}
							if edgeSequenceBindingMatches(midRow, pattern.EdgeVar, nextEdges[1:]) {
								if pattern.EdgeVar != "" {
									merged[pattern.EdgeVar] = edgeSequenceToAny(nextEdges[1:])
								}
								if pathVar != "" {
									merged[pathVar] = multiHopPathValue(nextVertexes, nextEdges, nextDirs)
								}
								if vertexPatternMatches(right, pattern.Right, params, merged) {
									if spec.Where != "" {
										ok, err := e.evalWhereExpression(ctx, tx, spec.Where, merged, params)
										if err != nil {
											return err
										}
										if ok {
											matched = true
											out = append(out, merged)
										}
									} else {
										matched = true
										out = append(out, merged)
									}
								}
							}
						}

						return walk(right, nextVertexes, nextEdges, nextDirs, nextUsed)
					})
				}

				initialEdges := []*graph.Edge{edge1}
				initialDirs := []string{"forward"}
				used := map[string]bool{edge1.ID: true}
				return walk(mid, []*graph.Vertex{left, mid}, initialEdges, initialDirs, used)
			}); err != nil {
				return nil, err
			}
		}

		if spec.Optional && !matched {
			merged := cloneRow(row)
			setOptionalNoMatchBinding(merged, row, pattern.Left.Var)
			setOptionalNoMatchBinding(merged, row, pattern.Mid.Var)
			setOptionalNoMatchBinding(merged, row, pattern.Right.Var)
			setOptionalNoMatchBinding(merged, row, pattern.EdgeVar)
			if pathVar != "" {
				merged[pathVar] = nil
			}
			out = append(out, merged)
		}
	}

	return out, nil
}

func (e *Executor) expandTwoHopDirectedChainMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, pattern twoHopDirectedChainPattern, params Params, pathVar string) ([]Row, error) {
	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		rows = []Row{{}}
	}

	type cachedTwoHopCandidate struct {
		edge  *graph.Edge
		right *graph.Vertex
	}
	secondHopCache := map[string][]cachedTwoHopCandidate{}

	out := make([]Row, 0)
	for _, row := range rows {
		leftCandidates, err := e.resolveVertexPatternCandidates(ctx, tx, tenant, row, pattern.Left, params)
		if err != nil {
			return nil, err
		}

		matched := false
		for _, left := range leftCandidates {
			if left == nil {
				continue
			}

			firstScanType := pattern.FirstEdgeType
			if len(pattern.FirstEdgeAnyOf) > 0 {
				firstScanType = ""
			}

			if err := scanOutEdgesQueryCached(ctx, tx, tenant, left.ID, firstScanType, params, func(edge1 *graph.Edge) error {
				if !edgeTypeMatches(edge1, pattern.FirstEdgeType, pattern.FirstEdgeAnyOf) {
					return nil
				}
				if !edgePatternMatches(edge1, pattern.FirstEdgeProps, params, row) {
					return nil
				}

				mid, err := getVertexQueryCached(ctx, tx, tenant, edge1.DstID, params)
				if err != nil {
					return err
				}
				if mid == nil {
					return nil
				}

				mergedMid := cloneRow(row)
				if pattern.Left.Var != "" {
					mergedMid[pattern.Left.Var] = left
				}
				if pattern.FirstEdgeVar != "" {
					mergedMid[pattern.FirstEdgeVar] = edge1
				}
				if !vertexBindingMatches(mergedMid, pattern.Mid.Var, mid) {
					return nil
				}
				if pattern.Mid.Var != "" {
					mergedMid[pattern.Mid.Var] = mid
				}
				if !vertexPatternMatches(mid, pattern.Mid, params, mergedMid) {
					return nil
				}

				secondScanType := pattern.SecondEdgeType
				if len(pattern.SecondEdgeAnyOf) > 0 {
					secondScanType = ""
				}
				secondWhereConstraints, hasSecondWhereConstraints := extractEdgeWhereNumericConstraints(spec.Where, pattern.SecondEdgeVar, mergedMid, params)

				cacheKey := tenant + "|" + mid.ID + "|"
				if pattern.SecondForward {
					cacheKey += "out|"
				} else {
					cacheKey += "in|"
				}
				cacheKey += secondScanType

				candidates, cacheHit := secondHopCache[cacheKey]
				if !cacheHit {
					candidates = make([]cachedTwoHopCandidate, 0)
					collectCandidate := func(edge2 *graph.Edge, rightID string) error {
						right, err := getVertexQueryCached(ctx, tx, tenant, rightID, params)
						if err != nil {
							return err
						}
						if right == nil {
							return nil
						}
						candidates = append(candidates, cachedTwoHopCandidate{edge: edge2, right: right})
						return nil
					}
					if pattern.SecondForward {
						if err := scanOutEdgesQueryCached(ctx, tx, tenant, mid.ID, secondScanType, params, func(edge2 *graph.Edge) error {
							return collectCandidate(edge2, edge2.DstID)
						}); err != nil {
							return err
						}
					} else {
						if err := scanInEdgesQueryCached(ctx, tx, tenant, mid.ID, secondScanType, params, func(edge2 *graph.Edge) error {
							return collectCandidate(edge2, edge2.SrcID)
						}); err != nil {
							return err
						}
					}
					secondHopCache[cacheKey] = candidates
				}

				collectRight := func(edge2 *graph.Edge, right *graph.Vertex) error {
					if !edgeTypeMatches(edge2, pattern.SecondEdgeType, pattern.SecondEdgeAnyOf) {
						return nil
					}
					if !edgePatternMatches(edge2, pattern.SecondEdgeProps, params, mergedMid) {
						return nil
					}
					if hasSecondWhereConstraints && !edgeMatchesNumericConstraints(edge2, secondWhereConstraints) {
						return nil
					}
					if right == nil {
						return nil
					}
					if !vertexBindingMatches(mergedMid, pattern.Right.Var, right) {
						return nil
					}

					merged := cloneRow(mergedMid)
					if pattern.Right.Var != "" {
						merged[pattern.Right.Var] = right
					}
					if pattern.SecondEdgeVar != "" {
						merged[pattern.SecondEdgeVar] = edge2
					}
					if pathVar != "" {
						directions := []string{"forward"}
						if pattern.SecondForward {
							directions = append(directions, "forward")
						} else {
							directions = append(directions, "reverse")
						}
						merged[pathVar] = multiHopPathValue([]*graph.Vertex{left, mid, right}, []*graph.Edge{edge1, edge2}, directions)
					}
					if !vertexPatternMatches(right, pattern.Right, params, merged) {
						return nil
					}

					if spec.Where != "" {
						ok, err := e.evalWhereExpression(ctx, tx, spec.Where, merged, params)
						if err != nil {
							return err
						}
						if !ok {
							return nil
						}
					}

					matched = true
					out = append(out, merged)
					return nil
				}

				for _, candidate := range candidates {
					if err := collectRight(candidate.edge, candidate.right); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				return nil, err
			}
		}

		if spec.Optional && !matched {
			merged := cloneRow(row)
			if pathVar != "" {
				merged[pathVar] = nil
			}
			setOptionalNoMatchBinding(merged, row, pattern.Left.Var)
			setOptionalNoMatchBinding(merged, row, pattern.Mid.Var)
			setOptionalNoMatchBinding(merged, row, pattern.Right.Var)
			setOptionalNoMatchBinding(merged, row, pattern.FirstEdgeVar)
			setOptionalNoMatchBinding(merged, row, pattern.SecondEdgeVar)
			out = append(out, merged)
		}
	}

	return out, nil
}

func (e *Executor) expandDirectedRelationshipThenAdjacentMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, pattern directedRelationshipThenAdjacentPattern, params Params, pathVar string) ([]Row, error) {
	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		rows = []Row{{}}
	}

	out := make([]Row, 0)
	for _, row := range rows {
		leftCandidates, err := e.resolveVertexPatternCandidates(ctx, tx, tenant, row, pattern.Left, params)
		if err != nil {
			return nil, err
		}

		matched := false
		for _, left := range leftCandidates {
			if left == nil {
				continue
			}

			rowWithLeft := cloneRow(row)
			if pattern.Left.Var != "" {
				rowWithLeft[pattern.Left.Var] = left
			}

			scanType := pattern.FirstEdgeType
			if len(pattern.FirstEdgeAnyOf) > 0 {
				scanType = ""
			}

			if err := tx.ScanOutEdges(ctx, tenant, left.ID, scanType, 0, func(edge1 *graph.Edge) error {
				if !edgeTypeMatches(edge1, pattern.FirstEdgeType, pattern.FirstEdgeAnyOf) {
					return nil
				}
				if !edgePatternMatches(edge1, pattern.FirstEdgeProps, params, rowWithLeft) {
					return nil
				}

				mid, err := tx.GetVertex(ctx, tenant, edge1.DstID)
				if err != nil {
					if graph.IsKind(err, graph.ErrKindNotFound) {
						return nil
					}
					return err
				}
				if !vertexBindingMatches(rowWithLeft, pattern.Mid.Var, mid) {
					return nil
				}

				mergedMid := cloneRow(rowWithLeft)
				if pattern.Mid.Var != "" {
					mergedMid[pattern.Mid.Var] = mid
				}
				if pattern.FirstEdgeVar != "" {
					mergedMid[pattern.FirstEdgeVar] = edge1
				}
				if !vertexPatternMatches(mid, pattern.Mid, params, mergedMid) {
					return nil
				}

				if err := tx.ScanOutEdges(ctx, tenant, mid.ID, "", 0, func(edge2 *graph.Edge) error {
					if edge2 == nil || edge2.ID == edge1.ID {
						return nil
					}

					right, err := tx.GetVertex(ctx, tenant, edge2.DstID)
					if err != nil {
						if graph.IsKind(err, graph.ErrKindNotFound) {
							return nil
						}
						return err
					}
					if !vertexBindingMatches(mergedMid, pattern.Right.Var, right) {
						return nil
					}

					merged := cloneRow(mergedMid)
					if pattern.Right.Var != "" {
						merged[pattern.Right.Var] = right
					}
					if pathVar != "" {
						merged[pathVar] = multiHopPathValue([]*graph.Vertex{left, mid, right}, []*graph.Edge{edge1, edge2}, []string{"forward", "forward"})
					}
					if !vertexPatternMatches(right, pattern.Right, params, merged) {
						return nil
					}

					if spec.Where != "" {
						ok, err := e.evalWhereExpression(ctx, tx, spec.Where, merged, params)
						if err != nil {
							return err
						}
						if !ok {
							return nil
						}
					}

					matched = true
					out = append(out, merged)
					return nil
				}); err != nil {
					return err
				}

				return nil
			}); err != nil {
				return nil, err
			}
		}

		if spec.Optional && !matched {
			merged := cloneRow(row)
			if pathVar != "" {
				merged[pathVar] = nil
			}
			setOptionalNoMatchBinding(merged, row, pattern.Left.Var)
			setOptionalNoMatchBinding(merged, row, pattern.Mid.Var)
			setOptionalNoMatchBinding(merged, row, pattern.Right.Var)
			setOptionalNoMatchBinding(merged, row, pattern.FirstEdgeVar)
			out = append(out, merged)
		}
	}

	return out, nil
}

func (e *Executor) expandTwoHopUndirectedRelationshipChainMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, pattern twoHopUndirectedRelationshipChainPattern, params Params, pathVar string) ([]Row, error) {
	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		rows = []Row{{}}
	}

	out := make([]Row, 0)
	for _, row := range rows {
		leftCandidates, err := e.resolveVertexPatternCandidates(ctx, tx, tenant, row, pattern.Left, params)
		if err != nil {
			return nil, err
		}

		matched := false
		for _, left := range leftCandidates {
			if left == nil {
				continue
			}

			rowWithLeft := cloneRow(row)
			if pattern.Left.Var != "" {
				rowWithLeft[pattern.Left.Var] = left
			}

			firstScanType := pattern.FirstEdgeType
			if len(pattern.FirstEdgeAnyOf) > 0 {
				firstScanType = ""
			}

			emittedFirst := map[string]struct{}{}
			collectFirst := func(edge1 *graph.Edge, midID string) error {
				if edge1 == nil {
					return nil
				}
				key := edge1.ID + "|" + midID
				if _, seen := emittedFirst[key]; seen {
					return nil
				}
				emittedFirst[key] = struct{}{}

				if !edgeTypeMatches(edge1, pattern.FirstEdgeType, pattern.FirstEdgeAnyOf) {
					return nil
				}
				if !edgePatternMatches(edge1, pattern.FirstEdgeProps, params, rowWithLeft) {
					return nil
				}

				mid, err := tx.GetVertex(ctx, tenant, midID)
				if err != nil {
					if graph.IsKind(err, graph.ErrKindNotFound) {
						return nil
					}
					return err
				}

				if !vertexBindingMatches(rowWithLeft, pattern.Mid.Var, mid) {
					return nil
				}
				mergedMid := cloneRow(rowWithLeft)
				if pattern.Mid.Var != "" {
					mergedMid[pattern.Mid.Var] = mid
				}
				if pattern.FirstEdgeVar != "" {
					mergedMid[pattern.FirstEdgeVar] = edge1
				}
				if !vertexPatternMatches(mid, pattern.Mid, params, mergedMid) {
					return nil
				}

				secondScanType := pattern.SecondEdgeType
				if len(pattern.SecondEdgeAnyOf) > 0 {
					secondScanType = ""
				}

				emittedSecond := map[string]struct{}{}
				collectSecond := func(edge2 *graph.Edge, rightID string) error {
					if edge2 == nil {
						return nil
					}
					if edge2.ID == edge1.ID {
						return nil
					}
					secondKey := edge2.ID + "|" + rightID
					if _, seen := emittedSecond[secondKey]; seen {
						return nil
					}
					emittedSecond[secondKey] = struct{}{}

					if !edgeTypeMatches(edge2, pattern.SecondEdgeType, pattern.SecondEdgeAnyOf) {
						return nil
					}
					if !edgePatternMatches(edge2, pattern.SecondEdgeProps, params, mergedMid) {
						return nil
					}

					right, err := tx.GetVertex(ctx, tenant, rightID)
					if err != nil {
						if graph.IsKind(err, graph.ErrKindNotFound) {
							return nil
						}
						return err
					}
					if !vertexBindingMatches(mergedMid, pattern.Right.Var, right) {
						return nil
					}

					merged := cloneRow(mergedMid)
					if pattern.Right.Var != "" {
						merged[pattern.Right.Var] = right
					}
					if pattern.SecondEdgeVar != "" {
						merged[pattern.SecondEdgeVar] = edge2
					}
					if pathVar != "" {
						merged[pathVar] = multiHopPathValue([]*graph.Vertex{left, mid, right}, []*graph.Edge{edge1, edge2}, []string{"undirected", "undirected"})
					}
					if !vertexPatternMatches(right, pattern.Right, params, merged) {
						return nil
					}

					if spec.Where != "" {
						ok, err := e.evalWhereExpression(ctx, tx, spec.Where, merged, params)
						if err != nil {
							return err
						}
						if !ok {
							return nil
						}
					}

					matched = true
					out = append(out, merged)
					return nil
				}

				if err := tx.ScanOutEdges(ctx, tenant, mid.ID, secondScanType, 0, func(edge2 *graph.Edge) error {
					return collectSecond(edge2, edge2.DstID)
				}); err != nil {
					return err
				}
				if err := tx.ScanInEdges(ctx, tenant, mid.ID, secondScanType, 0, func(edge2 *graph.Edge) error {
					return collectSecond(edge2, edge2.SrcID)
				}); err != nil {
					return err
				}

				return nil
			}

			if err := tx.ScanOutEdges(ctx, tenant, left.ID, firstScanType, 0, func(edge1 *graph.Edge) error {
				return collectFirst(edge1, edge1.DstID)
			}); err != nil {
				return nil, err
			}
			if err := tx.ScanInEdges(ctx, tenant, left.ID, firstScanType, 0, func(edge1 *graph.Edge) error {
				return collectFirst(edge1, edge1.SrcID)
			}); err != nil {
				return nil, err
			}
		}

		if spec.Optional && !matched {
			merged := cloneRow(row)
			if pathVar != "" {
				merged[pathVar] = nil
			}
			setOptionalNoMatchBinding(merged, row, pattern.Left.Var)
			setOptionalNoMatchBinding(merged, row, pattern.Mid.Var)
			setOptionalNoMatchBinding(merged, row, pattern.Right.Var)
			setOptionalNoMatchBinding(merged, row, pattern.FirstEdgeVar)
			setOptionalNoMatchBinding(merged, row, pattern.SecondEdgeVar)
			out = append(out, merged)
		}
	}

	return out, nil
}

func (e *Executor) expandDirectedThenUndirectedRelationshipChainMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, pattern directedThenUndirectedRelationshipChainPattern, params Params, pathVar string) ([]Row, error) {
	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		rows = []Row{{}}
	}

	out := make([]Row, 0)
	for _, row := range rows {
		leftCandidates, err := e.resolveVertexPatternCandidates(ctx, tx, tenant, row, pattern.Left, params)
		if err != nil {
			return nil, err
		}

		matched := false
		for _, left := range leftCandidates {
			if left == nil {
				continue
			}

			rowWithLeft := cloneRow(row)
			if pattern.Left.Var != "" {
				rowWithLeft[pattern.Left.Var] = left
			}

			firstScanType := pattern.FirstEdgeType
			if len(pattern.FirstEdgeAnyOf) > 0 {
				firstScanType = ""
			}

			if err := tx.ScanOutEdges(ctx, tenant, left.ID, firstScanType, 0, func(edge1 *graph.Edge) error {
				if !edgeTypeMatches(edge1, pattern.FirstEdgeType, pattern.FirstEdgeAnyOf) {
					return nil
				}
				if !edgePatternMatches(edge1, pattern.FirstEdgeProps, params, rowWithLeft) {
					return nil
				}

				mid, err := tx.GetVertex(ctx, tenant, edge1.DstID)
				if err != nil {
					if graph.IsKind(err, graph.ErrKindNotFound) {
						return nil
					}
					return err
				}
				if !vertexBindingMatches(rowWithLeft, pattern.Mid.Var, mid) {
					return nil
				}

				mergedMid := cloneRow(rowWithLeft)
				if pattern.Mid.Var != "" {
					mergedMid[pattern.Mid.Var] = mid
				}
				if pattern.FirstEdgeVar != "" {
					mergedMid[pattern.FirstEdgeVar] = edge1
				}
				if !vertexPatternMatches(mid, pattern.Mid, params, mergedMid) {
					return nil
				}

				secondScanType := pattern.SecondEdgeType
				if len(pattern.SecondEdgeAnyOf) > 0 {
					secondScanType = ""
				}

				emitted := map[string]struct{}{}
				collectSecond := func(edge2 *graph.Edge, rightID string, dir string) error {
					if edge2 == nil || edge2.ID == edge1.ID {
						return nil
					}
					key := edge2.ID + "|" + rightID
					if _, seen := emitted[key]; seen {
						return nil
					}
					emitted[key] = struct{}{}

					if !edgeTypeMatches(edge2, pattern.SecondEdgeType, pattern.SecondEdgeAnyOf) {
						return nil
					}
					if !edgePatternMatches(edge2, pattern.SecondEdgeProps, params, mergedMid) {
						return nil
					}

					right, err := tx.GetVertex(ctx, tenant, rightID)
					if err != nil {
						if graph.IsKind(err, graph.ErrKindNotFound) {
							return nil
						}
						return err
					}
					if !vertexBindingMatches(mergedMid, pattern.Right.Var, right) {
						return nil
					}

					merged := cloneRow(mergedMid)
					if pattern.Right.Var != "" {
						merged[pattern.Right.Var] = right
					}
					if pattern.SecondEdgeVar != "" {
						merged[pattern.SecondEdgeVar] = edge2
					}
					if pathVar != "" {
						merged[pathVar] = multiHopPathValue([]*graph.Vertex{left, mid, right}, []*graph.Edge{edge1, edge2}, []string{"forward", dir})
					}
					if !vertexPatternMatches(right, pattern.Right, params, merged) {
						return nil
					}
					if spec.Where != "" {
						ok, err := e.evalWhereExpression(ctx, tx, spec.Where, merged, params)
						if err != nil {
							return err
						}
						if !ok {
							return nil
						}
					}

					matched = true
					out = append(out, merged)
					return nil
				}

				if err := tx.ScanOutEdges(ctx, tenant, mid.ID, secondScanType, 0, func(edge2 *graph.Edge) error {
					return collectSecond(edge2, edge2.DstID, "forward")
				}); err != nil {
					return err
				}
				if err := tx.ScanInEdges(ctx, tenant, mid.ID, secondScanType, 0, func(edge2 *graph.Edge) error {
					return collectSecond(edge2, edge2.SrcID, "reverse")
				}); err != nil {
					return err
				}

				return nil
			}); err != nil {
				return nil, err
			}
		}

		if spec.Optional && !matched {
			merged := cloneRow(row)
			if pathVar != "" {
				merged[pathVar] = nil
			}
			setOptionalNoMatchBinding(merged, row, pattern.Left.Var)
			setOptionalNoMatchBinding(merged, row, pattern.Mid.Var)
			setOptionalNoMatchBinding(merged, row, pattern.Right.Var)
			setOptionalNoMatchBinding(merged, row, pattern.FirstEdgeVar)
			setOptionalNoMatchBinding(merged, row, pattern.SecondEdgeVar)
			out = append(out, merged)
		}
	}

	return out, nil
}

func (e *Executor) expandReverseRelationshipThenUndirectedVariableLengthMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, pattern reverseRelationshipThenUndirectedVariableLengthPattern, params Params, pathVar string) ([]Row, error) {
	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		rows = []Row{{}}
	}

	out := make([]Row, 0)
	for _, row := range rows {
		leftCandidates, err := e.resolveVertexPatternCandidates(ctx, tx, tenant, row, pattern.Left, params)
		if err != nil {
			return nil, err
		}

		matched := false
		for _, left := range leftCandidates {
			if left == nil {
				continue
			}

			baseRow := cloneRow(row)
			if pattern.Left.Var != "" {
				baseRow[pattern.Left.Var] = left
			}

			firstScanType := pattern.FirstEdgeType
			if len(pattern.FirstEdgeAnyOf) > 0 {
				firstScanType = ""
			}

			if err := tx.ScanInEdges(ctx, tenant, left.ID, firstScanType, 0, func(edge1 *graph.Edge) error {
				if !edgeTypeMatches(edge1, pattern.FirstEdgeType, pattern.FirstEdgeAnyOf) {
					return nil
				}
				if !edgePatternMatches(edge1, pattern.FirstEdgeProps, params, baseRow) {
					return nil
				}

				mid, err := tx.GetVertex(ctx, tenant, edge1.SrcID)
				if err != nil {
					if graph.IsKind(err, graph.ErrKindNotFound) {
						return nil
					}
					return err
				}
				if !vertexBindingMatches(baseRow, pattern.Mid.Var, mid) {
					return nil
				}

				midRow := cloneRow(baseRow)
				if pattern.Mid.Var != "" {
					midRow[pattern.Mid.Var] = mid
				}
				if pattern.FirstEdgeVar != "" {
					midRow[pattern.FirstEdgeVar] = edge1
				}
				if !vertexPatternMatches(mid, pattern.Mid, params, midRow) {
					return nil
				}

				emitMatch := func(current *graph.Vertex, varVertexes []*graph.Vertex, varEdges []*graph.Edge, varDirs []string) error {
					depth := len(varEdges)
					if depth < pattern.MinHops {
						return nil
					}
					if pattern.MaxHops >= 0 && depth > pattern.MaxHops {
						return nil
					}
					if !vertexBindingMatches(midRow, pattern.Right.Var, current) {
						return nil
					}
					if !edgeSequenceBindingMatches(midRow, pattern.SecondEdgeVar, varEdges) {
						return nil
					}

					merged := cloneRow(midRow)
					if pattern.Right.Var != "" {
						merged[pattern.Right.Var] = current
					}
					if pattern.SecondEdgeVar != "" {
						merged[pattern.SecondEdgeVar] = edgeSequenceToAny(varEdges)
					}
					pathVertexes := append([]*graph.Vertex{left, mid}, varVertexes...)
					pathEdges := append([]*graph.Edge{edge1}, varEdges...)
					pathDirs := append([]string{"reverse"}, varDirs...)
					if pathVar != "" {
						merged[pathVar] = multiHopPathValue(pathVertexes, pathEdges, pathDirs)
					}
					if !vertexPatternMatches(current, pattern.Right, params, merged) {
						return nil
					}
					if spec.Where != "" {
						ok, err := e.evalWhereExpression(ctx, tx, spec.Where, merged, params)
						if err != nil {
							return err
						}
						if !ok {
							return nil
						}
					}

					matched = true
					out = append(out, merged)
					return nil
				}

				if err := emitMatch(mid, []*graph.Vertex{}, []*graph.Edge{}, []string{}); err != nil {
					return err
				}

				var walk func(current *graph.Vertex, vertexes []*graph.Vertex, edges []*graph.Edge, dirs []string, used map[string]bool) error
				walk = func(current *graph.Vertex, vertexes []*graph.Vertex, edges []*graph.Edge, dirs []string, used map[string]bool) error {
					if pattern.MaxHops >= 0 && len(edges) >= pattern.MaxHops {
						return nil
					}

					type neighborEdge struct {
						edge     *graph.Edge
						neighbor *graph.Vertex
						dir      string
					}
					neighbors := make([]neighborEdge, 0)
					seen := map[string]struct{}{}
					collect := func(edge *graph.Edge, neighborID string, dir string) error {
						if edge == nil || used[edge.ID] {
							return nil
						}
						if !edgeTypeMatches(edge, pattern.SecondEdgeType, pattern.SecondEdgeAnyOf) {
							return nil
						}
						if !edgePatternMatches(edge, pattern.SecondEdgeProps, params, midRow) {
							return nil
						}
						key := edge.ID + "|" + neighborID
						if _, ok := seen[key]; ok {
							return nil
						}
						seen[key] = struct{}{}
						neighbor, err := tx.GetVertex(ctx, tenant, neighborID)
						if err != nil {
							if graph.IsKind(err, graph.ErrKindNotFound) {
								return nil
							}
							return err
						}
						neighbors = append(neighbors, neighborEdge{edge: edge, neighbor: neighbor, dir: dir})
						return nil
					}

					scanType := pattern.SecondEdgeType
					if len(pattern.SecondEdgeAnyOf) > 0 {
						scanType = ""
					}
					if err := tx.ScanOutEdges(ctx, tenant, current.ID, scanType, 0, func(edge *graph.Edge) error {
						return collect(edge, edge.DstID, "forward")
					}); err != nil {
						return err
					}
					if err := tx.ScanInEdges(ctx, tenant, current.ID, scanType, 0, func(edge *graph.Edge) error {
						return collect(edge, edge.SrcID, "reverse")
					}); err != nil {
						return err
					}

					for _, candidate := range neighbors {
						nextVertexes := append(append([]*graph.Vertex{}, vertexes...), candidate.neighbor)
						nextEdges := append(append([]*graph.Edge{}, edges...), candidate.edge)
						nextDirs := append(append([]string{}, dirs...), candidate.dir)

						nextUsed := make(map[string]bool, len(used)+1)
						for key := range used {
							nextUsed[key] = true
						}
						nextUsed[candidate.edge.ID] = true

						if err := emitMatch(candidate.neighbor, nextVertexes, nextEdges, nextDirs); err != nil {
							return err
						}
						if err := walk(candidate.neighbor, nextVertexes, nextEdges, nextDirs, nextUsed); err != nil {
							return err
						}
					}

					return nil
				}

				return walk(mid, []*graph.Vertex{}, []*graph.Edge{}, []string{}, map[string]bool{edge1.ID: true})
			}); err != nil {
				return nil, err
			}
		}

		if spec.Optional && !matched {
			merged := cloneRow(row)
			if pathVar != "" {
				merged[pathVar] = nil
			}
			setOptionalNoMatchBinding(merged, row, pattern.Left.Var)
			setOptionalNoMatchBinding(merged, row, pattern.Mid.Var)
			setOptionalNoMatchBinding(merged, row, pattern.Right.Var)
			setOptionalNoMatchBinding(merged, row, pattern.FirstEdgeVar)
			setOptionalNoMatchBinding(merged, row, pattern.SecondEdgeVar)
			out = append(out, merged)
		}
	}

	return out, nil
}

func (e *Executor) expandMixedRelationshipChainMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, pattern mixedRelationshipChainPattern, params Params, pathVar string) ([]Row, error) {
	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		rows = []Row{{}}
	}

	cloneUsed := func(src map[string]bool) map[string]bool {
		dst := make(map[string]bool, len(src)+1)
		for key := range src {
			dst[key] = true
		}
		return dst
	}

	out := make([]Row, 0)
	for _, row := range rows {
		startCandidates, err := e.resolveVertexPatternCandidates(ctx, tx, tenant, row, pattern.Vertexes[0], params)
		if err != nil {
			return nil, err
		}

		matched := false
		for _, start := range startCandidates {
			if start == nil {
				continue
			}

			baseRow := cloneRow(row)
			if pattern.Vertexes[0].Var != "" {
				baseRow[pattern.Vertexes[0].Var] = start
			}
			if !vertexPatternMatches(start, pattern.Vertexes[0], params, baseRow) {
				continue
			}

			var walk func(index int, current *graph.Vertex, currentRow Row, vertexes []*graph.Vertex, edges []*graph.Edge, dirs []string, used map[string]bool) error
			walk = func(index int, current *graph.Vertex, currentRow Row, vertexes []*graph.Vertex, edges []*graph.Edge, dirs []string, used map[string]bool) error {
				if index == len(pattern.Segments) {
					merged := cloneRow(currentRow)
					if pathVar != "" {
						merged[pathVar] = multiHopPathValue(vertexes, edges, dirs)
					}
					if spec.Where != "" {
						ok, err := e.evalWhereExpression(ctx, tx, spec.Where, merged, params)
						if err != nil {
							return err
						}
						if !ok {
							return nil
						}
					}
					matched = true
					out = append(out, merged)
					return nil
				}

				segment := pattern.Segments[index]
				nextPattern := pattern.Vertexes[index+1]
				segmentWhereConstraints, hasSegmentWhereConstraints := extractEdgeWhereNumericConstraints(spec.Where, segment.EdgeVar, currentRow, params)

				minHops := segment.MinHops
				maxHops := segment.MaxHops
				if !segment.IsVariableLength {
					minHops = 1
					maxHops = 1
				}
				baseEdgeCount := len(edges)
				var explore func(vertex *graph.Vertex, pathVertexes []*graph.Vertex, pathEdges []*graph.Edge, pathDirs []string, pathUsed map[string]bool) error
				explore = func(vertex *graph.Vertex, pathVertexes []*graph.Vertex, pathEdges []*graph.Edge, pathDirs []string, pathUsed map[string]bool) error {
					segmentEdges := pathEdges[baseEdgeCount:]
					depth := len(segmentEdges)
					if depth >= minHops {
						if vertexBindingMatches(currentRow, nextPattern.Var, vertex) {
							nextRow := cloneRow(currentRow)
							if nextPattern.Var != "" {
								nextRow[nextPattern.Var] = vertex
							}
							segmentBindingOK := true
							if segment.IsVariableLength {
								segmentBindingOK = edgeSequenceBindingMatches(currentRow, segment.EdgeVar, segmentEdges)
								if segmentBindingOK && segment.EdgeVar != "" {
									nextRow[segment.EdgeVar] = edgeSequenceToAny(segmentEdges)
								}
							} else {
								segmentBindingOK = len(segmentEdges) == 1 && edgeBindingMatches(currentRow, segment.EdgeVar, segmentEdges[0])
								if segmentBindingOK && segment.EdgeVar != "" {
									nextRow[segment.EdgeVar] = segmentEdges[0]
								}
							}
							if segmentBindingOK && vertexPatternMatches(vertex, nextPattern, params, nextRow) {
								if err := walk(index+1, vertex, nextRow, pathVertexes, pathEdges, pathDirs, pathUsed); err != nil {
									return err
								}
							}
						}
					}
					if maxHops >= 0 && depth >= maxHops {
						return nil
					}

					scanType := segment.EdgeType
					if len(segment.EdgeAnyOf) > 0 {
						scanType = ""
					}
					emitted := map[string]struct{}{}
					collect := func(edge *graph.Edge, neighborID string, direction string) error {
						if edge == nil || used[edge.ID] || pathUsed[edge.ID] {
							return nil
						}
						key := edge.ID + "|" + neighborID
						if _, ok := emitted[key]; ok {
							return nil
						}
						emitted[key] = struct{}{}
						if !edgeTypeMatches(edge, segment.EdgeType, segment.EdgeAnyOf) {
							return nil
						}
						if !edgePatternMatches(edge, segment.EdgeProps, params, currentRow) {
							return nil
						}
						if hasSegmentWhereConstraints && !edgeMatchesNumericConstraints(edge, segmentWhereConstraints) {
							return nil
						}
						neighbor, err := tx.GetVertex(ctx, tenant, neighborID)
						if err != nil {
							if graph.IsKind(err, graph.ErrKindNotFound) {
								return nil
							}
							return err
						}
						nextVertexes := append(append([]*graph.Vertex{}, pathVertexes...), neighbor)
						nextEdges := append(append([]*graph.Edge{}, pathEdges...), edge)
						nextDirs := append(append([]string{}, pathDirs...), direction)
						nextUsed := cloneUsed(pathUsed)
						nextUsed[edge.ID] = true
						return explore(neighbor, nextVertexes, nextEdges, nextDirs, nextUsed)
					}

					if segment.Direction == "reverse" {
						return tx.ScanInEdges(ctx, tenant, vertex.ID, scanType, 0, func(edge *graph.Edge) error {
							return collect(edge, edge.SrcID, "reverse")
						})
					}
					if segment.Direction == "undirected" {
						if err := tx.ScanOutEdges(ctx, tenant, vertex.ID, scanType, 0, func(edge *graph.Edge) error {
							return collect(edge, edge.DstID, "forward")
						}); err != nil {
							return err
						}
						return tx.ScanInEdges(ctx, tenant, vertex.ID, scanType, 0, func(edge *graph.Edge) error {
							return collect(edge, edge.SrcID, "reverse")
						})
					}
					return tx.ScanOutEdges(ctx, tenant, vertex.ID, scanType, 0, func(edge *graph.Edge) error {
						return collect(edge, edge.DstID, "forward")
					})
				}

				return explore(current, vertexes, edges, dirs, used)
			}

			if err := walk(0, start, baseRow, []*graph.Vertex{start}, nil, nil, map[string]bool{}); err != nil {
				return nil, err
			}
		}

		if spec.Optional && !matched {
			merged := cloneRow(row)
			if pathVar != "" {
				merged[pathVar] = nil
			}
			for _, vertex := range pattern.Vertexes {
				setOptionalNoMatchBinding(merged, row, vertex.Var)
			}
			for _, segment := range pattern.Segments {
				setOptionalNoMatchBinding(merged, row, segment.EdgeVar)
			}
			out = append(out, merged)
		}
	}

	return out, nil
}

func ensureOptionalPathBinding(rows []Row, pathVar string) {
	if pathVar == "" {
		return
	}
	for _, row := range rows {
		if _, ok := row[pathVar]; !ok {
			row[pathVar] = nil
		}
	}
}

func setOptionalNoMatchBinding(dst Row, src Row, varName string) {
	if varName == "" {
		return
	}
	if _, bound := src[varName]; bound {
		dst[varName] = src[varName]
	} else {
		dst[varName] = nil
	}
}

func multiHopPathValue(vertexes []*graph.Vertex, edges []*graph.Edge, directions []string) any {
	if len(vertexes) == 0 {
		return nil
	}
	if len(vertexes) == 1 {
		return cypherPathValue{Left: vertexes[0]}
	}
	// Build the path as a serialized string similar to cypherPathValue.
	// For multi-hop, return a multiHopCypherPath struct.
	return multiHopCypherPath{Vertexes: vertexes, Edges: edges, Directions: directions}
}

type multiHopCypherPath struct {
	Vertexes   []*graph.Vertex
	Edges      []*graph.Edge
	Directions []string
}

func (p multiHopCypherPath) MarshalJSON() ([]byte, error) {
	vertexes := make([]any, len(p.Vertexes))
	for i, v := range p.Vertexes {
		vertexes[i] = vertexToMap(v)
	}
	edges := make([]any, len(p.Edges))
	for i, e := range p.Edges {
		edges[i] = edgeToMap(e)
	}
	directions := make([]any, len(p.Directions))
	for i, d := range p.Directions {
		directions[i] = d
	}
	return json.Marshal(map[string]any{
		"__path__":   true,
		"vertexes":   vertexes,
		"edges":      edges,
		"directions": directions,
	})
}

type multiHopPartialPath struct {
	Vertexes   []*graph.Vertex
	Edges      []*graph.Edge
	Directions []string
	AccRow     Row
	UsedEdges  map[string]bool
}

func (p multiHopCypherPath) String() string {
	if len(p.Vertexes) == 0 {
		return "<>"
	}
	b := strings.Builder{}
	b.WriteString("<")
	b.WriteString(renderPathVertex(p.Vertexes[0]))
	for i, edge := range p.Edges {
		dir := "forward"
		if i < len(p.Directions) {
			dir = p.Directions[i]
		}
		edgeStr := renderPathEdge(edge)
		switch dir {
		case "reverse":
			b.WriteString("<-")
			b.WriteString(edgeStr)
			b.WriteString("-")
		case "undirected":
			b.WriteString("-")
			b.WriteString(edgeStr)
			b.WriteString("-")
		default:
			b.WriteString("-")
			b.WriteString(edgeStr)
			b.WriteString("->")
		}
		if i+1 < len(p.Vertexes) {
			b.WriteString(renderPathVertex(p.Vertexes[i+1]))
		}
	}
	b.WriteString(">")
	return b.String()
}

func (e *Executor) expandMultiHopAdjacentChainMatch(ctx context.Context, tx graph.Tx, rows []Row, spec anchoredMatchSpec, chain multiHopAdjacentChainPattern, params Params, pathVar string) ([]Row, error) {
	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		rows = []Row{{}}
	}

	out := make([]Row, 0)
	for _, row := range rows {
		startCandidates, err := e.resolveVertexPatternCandidates(ctx, tx, tenant, row, chain.StartVertex, params)
		if err != nil {
			return nil, err
		}

		matched := false
		for _, start := range startCandidates {
			if start == nil {
				continue
			}

			accRow := cloneRow(row)
			if chain.StartVertex.Var != "" {
				accRow[chain.StartVertex.Var] = start
			}

			current := []multiHopPartialPath{{
				Vertexes:  []*graph.Vertex{start},
				AccRow:    accRow,
				UsedEdges: make(map[string]bool),
			}}

			var hopErr error
			for _, hop := range chain.Hops {
				var next []multiHopPartialPath
				for _, partial := range current {
					last := partial.Vertexes[len(partial.Vertexes)-1]

					type edgeNeighbor struct {
						edge      *graph.Edge
						neighbor  *graph.Vertex
						direction string
					}
					var candidates []edgeNeighbor
					seenCandidate := map[string]struct{}{}

					collectFn := func(edge *graph.Edge, neighborID, dir string) error {
						if edge == nil {
							return nil
						}
						key := edge.ID + "|" + neighborID
						if _, seen := seenCandidate[key]; seen {
							return nil
						}
						neighbor, nerr := tx.GetVertex(ctx, tenant, neighborID)
						if nerr != nil {
							if graph.IsKind(nerr, graph.ErrKindNotFound) {
								return nil
							}
							return nerr
						}
						seenCandidate[key] = struct{}{}
						candidates = append(candidates, edgeNeighbor{edge, neighbor, dir})
						return nil
					}

					if hop.Direction == "forward" || hop.Direction == "undirected" {
						if scanErr := tx.ScanOutEdges(ctx, tenant, last.ID, "", 0, func(edge *graph.Edge) error {
							return collectFn(edge, edge.DstID, "forward")
						}); scanErr != nil {
							hopErr = scanErr
							break
						}
					}
					if hop.Direction == "reverse" || hop.Direction == "undirected" {
						if scanErr := tx.ScanInEdges(ctx, tenant, last.ID, "", 0, func(edge *graph.Edge) error {
							return collectFn(edge, edge.SrcID, "reverse")
						}); scanErr != nil {
							hopErr = scanErr
							break
						}
					}
					if hopErr != nil {
						break
					}

					for _, c := range candidates {
						// Cypher path-uniqueness: each edge may only appear once per path.
						if partial.UsedEdges[c.edge.ID] {
							continue
						}
						if !vertexPatternMatches(c.neighbor, hop.Vertex, params, partial.AccRow) {
							continue
						}

						newVertexes := make([]*graph.Vertex, len(partial.Vertexes)+1)
						copy(newVertexes, partial.Vertexes)
						newVertexes[len(partial.Vertexes)] = c.neighbor

						newEdges := make([]*graph.Edge, len(partial.Edges)+1)
						copy(newEdges, partial.Edges)
						newEdges[len(partial.Edges)] = c.edge

						newDirs := make([]string, len(partial.Directions)+1)
						copy(newDirs, partial.Directions)
						newDirs[len(partial.Directions)] = c.direction

						newAccRow := cloneRow(partial.AccRow)
						if hop.Vertex.Var != "" {
							newAccRow[hop.Vertex.Var] = c.neighbor
						}

						newUsed := make(map[string]bool, len(partial.UsedEdges)+1)
						for k := range partial.UsedEdges {
							newUsed[k] = true
						}
						newUsed[c.edge.ID] = true

						next = append(next, multiHopPartialPath{
							Vertexes:   newVertexes,
							Edges:      newEdges,
							Directions: newDirs,
							AccRow:     newAccRow,
							UsedEdges:  newUsed,
						})
					}
				}
				if hopErr != nil {
					break
				}
				current = next
			}

			if hopErr != nil {
				return nil, hopErr
			}

			for _, path := range current {
				merged := cloneRow(path.AccRow)
				if pathVar != "" {
					merged[pathVar] = multiHopPathValue(path.Vertexes, path.Edges, path.Directions)
				}

				if spec.Where != "" {
					ok, err := e.evalWhereExpression(ctx, tx, spec.Where, merged, params)
					if err != nil {
						return nil, err
					}
					if !ok {
						continue
					}
				}

				matched = true
				out = append(out, merged)
			}
		}

		if spec.Optional && !matched {
			merged := cloneRow(row)
			if chain.StartVertex.Var != "" {
				setOptionalNoMatchBinding(merged, row, chain.StartVertex.Var)
			}
			for _, hop := range chain.Hops {
				if hop.Vertex.Var != "" {
					setOptionalNoMatchBinding(merged, row, hop.Vertex.Var)
				}
			}
			out = append(out, merged)
		}
	}

	return out, nil
}

func vertexBindingMatches(row Row, varName string, candidate *graph.Vertex) bool {
	if strings.TrimSpace(varName) == "" {
		return true
	}
	binding, ok := row[varName]
	if !ok {
		return true
	}
	switch typed := binding.(type) {
	case nil:
		return candidate == nil
	case *graph.Vertex:
		return candidate != nil && typed.ID == candidate.ID
	case string:
		return candidate != nil && typed == candidate.ID
	default:
		return false
	}
}

func edgeBindingMatches(row Row, varName string, candidate *graph.Edge) bool {
	if strings.TrimSpace(varName) == "" {
		return true
	}
	binding, ok := row[varName]
	if !ok {
		return true
	}
	switch typed := binding.(type) {
	case nil:
		return candidate == nil
	case *graph.Edge:
		return candidate != nil && typed.ID == candidate.ID
	case string:
		return candidate != nil && typed == candidate.ID
	default:
		return false
	}
}

func edgePatternMatches(edge *graph.Edge, propsRaw string, params Params, row Row) bool {
	if edge == nil {
		return false
	}
	propsRaw = strings.TrimSpace(propsRaw)
	if propsRaw == "" {
		return true
	}
	parsed, err := parsePropertyMap(propsRaw, params, row)
	if err != nil {
		return false
	}
	for key, value := range parsed {
		if strings.EqualFold(key, "id") {
			if edge.ID != stringFromProperty(map[string]any{"id": value}, "id") {
				return false
			}
			continue
		}
		if strings.EqualFold(key, "type") {
			if edge.Type != stringFromProperty(map[string]any{"type": value}, "type") {
				return false
			}
			continue
		}
		if edge.Properties == nil {
			return false
		}
		current, ok := edge.Properties[key]
		if !ok {
			return false
		}
		if !bytes.Equal(current, valueToBytes(value)) {
			return false
		}
	}
	return true
}

func edgeTypeMatches(edge *graph.Edge, edgeType string, edgeAnyOf []string) bool {
	if edge == nil {
		return false
	}
	if len(edgeAnyOf) == 0 {
		if edgeType == "" {
			return true
		}
		return edge.Type == edgeType
	}
	for _, candidate := range edgeAnyOf {
		if edge.Type == candidate {
			return true
		}
	}
	return false
}

func (e *Executor) resolveVertexPatternCandidates(ctx context.Context, tx graph.Tx, tenant string, row Row, pattern vertexPattern, params Params) ([]*graph.Vertex, error) {
	if binding, ok := row[pattern.Var]; ok {
		switch typed := binding.(type) {
		case *graph.Vertex:
			if vertexPatternMatches(typed, pattern, params, row) {
				return []*graph.Vertex{typed}, nil
			}
			return nil, nil
		case string:
			vertex, err := tx.GetVertex(ctx, tenant, typed)
			if err != nil {
				if graph.IsKind(err, graph.ErrKindNotFound) {
					return nil, nil
				}
				return nil, err
			}
			if vertexPatternMatches(vertex, pattern, params, row) {
				return []*graph.Vertex{vertex}, nil
			}
			return nil, nil
		}
	}

	if plan, ok, err := e.planVertexPatternPropertyIndexLookup(tenant, pattern, params, row); err != nil {
		return nil, err
	} else if ok {
		matches, err := e.lookupVertexPatternCandidatesByPropertyIndex(ctx, tx, tenant, pattern, params, row, plan)
		if err != nil {
			return nil, err
		}
		return matches, nil
	}

	out := make([]*graph.Vertex, 0)
	if err := tx.ScanVertices(ctx, tenant, 0, func(vertex *graph.Vertex) error {
		if vertexPatternMatches(vertex, pattern, params, row) {
			out = append(out, vertex)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return out, nil
}

type vertexPropertyIndexLookupPlan struct {
	schema   string
	property string
	value    any
}

func (e *Executor) planVertexPatternPropertyIndexLookup(tenant string, pattern vertexPattern, params Params, row Row) (vertexPropertyIndexLookupPlan, bool, error) {
	if e == nil || e.indexCatalog == nil {
		return vertexPropertyIndexLookupPlan{}, false, nil
	}
	if len(pattern.AllOfLabels) == 0 && len(pattern.AnyOfLabels) == 0 {
		return vertexPropertyIndexLookupPlan{}, false, nil
	}

	props, err := parsePropertyMap(pattern.PropertiesRaw, params, row)
	if err != nil || len(props) == 0 {
		return vertexPropertyIndexLookupPlan{}, false, nil
	}

	labels := append([]string{}, pattern.AllOfLabels...)
	if len(labels) == 0 {
		labels = append(labels, pattern.AnyOfLabels...)
	}
	if len(labels) == 0 {
		return vertexPropertyIndexLookupPlan{}, false, nil
	}

	propKeys := make([]string, 0, len(props))
	for prop := range props {
		if strings.EqualFold(prop, "id") {
			continue
		}
		propKeys = append(propKeys, prop)
	}
	if len(propKeys) == 0 {
		return vertexPropertyIndexLookupPlan{}, false, nil
	}
	sort.Strings(propKeys)

	for _, label := range labels {
		for _, prop := range propKeys {
			indexed := e.indexCatalog.HasPropertyIndex(tenant, label, prop)
			e.metrics.ObserveIndexCandidate(tenant, label, prop, indexed)
			if !indexed {
				continue
			}
			return vertexPropertyIndexLookupPlan{schema: label, property: prop, value: props[prop]}, true, nil
		}
	}

	return vertexPropertyIndexLookupPlan{}, false, nil
}

func (e *Executor) lookupVertexPatternCandidatesByPropertyIndex(ctx context.Context, tx graph.Tx, tenant string, pattern vertexPattern, params Params, row Row, plan vertexPropertyIndexLookupPlan) ([]*graph.Vertex, error) {
	encoded := valueToBytes(plan.value)
	ids := map[string]struct{}{}
	err := tx.ScanPropertyIndex(ctx, tenant, plan.schema, plan.property, encoded, 0, func(entry *graph.PropertyIndexEntry) error {
		ids[entry.EntityID] = struct{}{}
		return nil
	})
	if err != nil {
		e.metrics.ObserveIndexLookup("property_index", "error", 0)
		return nil, err
	}
	if len(ids) == 0 {
		e.metrics.ObserveIndexLookup("property_index", "miss", 0)
		return nil, nil
	}

	matches := make([]*graph.Vertex, 0, len(ids))
	for id := range ids {
		vertex, err := tx.GetVertex(ctx, tenant, id)
		if err != nil {
			if graph.IsKind(err, graph.ErrKindNotFound) {
				continue
			}
			e.metrics.ObserveIndexLookup("property_index", "error", 0)
			return nil, err
		}
		if !vertexPatternMatches(vertex, pattern, params, row) {
			continue
		}
		matches = append(matches, vertex)
	}
	if len(matches) == 0 {
		e.metrics.ObserveIndexLookup("property_index", "miss", 0)
		return nil, nil
	}
	sort.Slice(matches, func(i, j int) bool {
		return matches[i].ID < matches[j].ID
	})
	e.metrics.ObserveIndexLookup("property_index", "hit", len(matches))
	return matches, nil
}

func vertexPatternMatches(vertex *graph.Vertex, pattern vertexPattern, params Params, row Row) bool {
	if vertex == nil {
		return false
	}
	if !vertexBindingMatches(row, pattern.Var, vertex) {
		return false
	}
	if len(pattern.AnyOfLabels) > 0 {
		matched := false
		for _, want := range pattern.AnyOfLabels {
			if vertexHasLabel(vertex, want) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	if len(pattern.AllOfLabels) > 0 {
		for _, want := range pattern.AllOfLabels {
			if !vertexHasLabel(vertex, want) {
				return false
			}
		}
	}
	if len(pattern.ExcludedLabels) > 0 {
		for _, want := range pattern.ExcludedLabels {
			if vertexHasLabel(vertex, want) {
				return false
			}
		}
	}
	if len(pattern.AllOfLabels) > 0 && len(pattern.AnyOfLabels) == 0 && len(pattern.ExcludedLabels) == 0 {
		if pattern.Var == "" {
			vertex.Labels = reorderLabelsByPattern(vertex.Labels, pattern.AllOfLabels)
		} else if _, alreadyBound := row[pattern.Var]; !alreadyBound {
			vertex.Labels = reorderLabelsByPattern(vertex.Labels, pattern.AllOfLabels)
		}
	}

	props := strings.TrimSpace(pattern.PropertiesRaw)
	if props == "" {
		return true
	}

	parsed, err := parsePropertyMap(props, params, row)
	if err != nil {
		return false
	}
	for key, value := range parsed {
		if vertex.Properties == nil {
			return false
		}
		current, ok := vertex.Properties[key]
		if !ok {
			return false
		}
		if !bytes.Equal(current, valueToBytes(value)) {
			return false
		}
	}

	return true
}

func vertexHasLabel(vertex *graph.Vertex, label string) bool {
	if vertex == nil || strings.TrimSpace(label) == "" {
		return false
	}
	for _, current := range vertex.Labels {
		if current == label {
			return true
		}
	}
	return false
}

func reorderLabelsByPattern(labels []string, ordered []string) []string {
	if len(labels) == 0 || len(ordered) == 0 {
		return labels
	}
	seen := make(map[string]struct{}, len(labels))
	out := make([]string, 0, len(labels))
	for _, want := range ordered {
		for _, label := range labels {
			if label != want {
				continue
			}
			if _, ok := seen[label]; ok {
				break
			}
			seen[label] = struct{}{}
			out = append(out, label)
			break
		}
	}
	for _, label := range labels {
		if _, ok := seen[label]; ok {
			continue
		}
		out = append(out, label)
	}
	return out
}

func anchoredSourcePropertyEquality(pattern anchoredOutPattern, params Params, row Row) (string, any, bool) {
	props := strings.TrimSpace(pattern.SourcePropertiesRaw)
	if props == "" {
		return "", nil, false
	}
	parsed, err := parsePropertyMap(props, params, row)
	if err != nil || len(parsed) != 1 {
		return "", nil, false
	}
	for key, value := range parsed {
		if strings.EqualFold(key, "id") {
			return "", nil, false
		}
		return key, value, true
	}
	return "", nil, false
}

func edgePropertyEquality(raw string, params Params, row Row) (string, any, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil, false
	}
	parsed, err := parsePropertyMap(raw, params, row)
	if err != nil || len(parsed) != 1 {
		return "", nil, false
	}
	for key, value := range parsed {
		if strings.EqualFold(key, "id") {
			return "", nil, false
		}
		return key, value, true
	}
	return "", nil, false
}

func edgePatternCandidateTypes(edgeType string, edgeAnyOf []string) []string {
	edgeType = strings.TrimSpace(edgeType)
	if edgeType != "" {
		return []string{edgeType}
	}
	if len(edgeAnyOf) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	types := make([]string, 0, len(edgeAnyOf))
	for _, t := range edgeAnyOf {
		t = strings.TrimSpace(t)
		if t == "" {
			continue
		}
		if _, ok := seen[t]; ok {
			continue
		}
		seen[t] = struct{}{}
		types = append(types, t)
	}
	sort.Strings(types)
	return types
}

type edgeNumericRangeConstraint struct {
	lower          float64
	lowerSet       bool
	lowerInclusive bool
	upper          float64
	upperSet       bool
	upperInclusive bool
}

func (c *edgeNumericRangeConstraint) applyLower(value float64, inclusive bool) bool {
	if !c.lowerSet {
		c.lower = value
		c.lowerSet = true
		c.lowerInclusive = inclusive
		return true
	}
	if value > c.lower {
		c.lower = value
		c.lowerInclusive = inclusive
		return true
	}
	if value == c.lower {
		c.lowerInclusive = c.lowerInclusive && inclusive
	}
	return true
}

func (c *edgeNumericRangeConstraint) applyUpper(value float64, inclusive bool) bool {
	if !c.upperSet {
		c.upper = value
		c.upperSet = true
		c.upperInclusive = inclusive
		return true
	}
	if value < c.upper {
		c.upper = value
		c.upperInclusive = inclusive
		return true
	}
	if value == c.upper {
		c.upperInclusive = c.upperInclusive && inclusive
	}
	return true
}

func (c edgeNumericRangeConstraint) isContradictory() bool {
	if !c.lowerSet || !c.upperSet {
		return false
	}
	if c.lower < c.upper {
		return false
	}
	if c.lower > c.upper {
		return true
	}
	return !(c.lowerInclusive && c.upperInclusive)
}

func (c edgeNumericRangeConstraint) matchesValue(value any) bool {
	numeric, ok := comparableNumericValue(value)
	if !ok {
		return false
	}
	if c.lowerSet {
		if numeric < c.lower {
			return false
		}
		if numeric == c.lower && !c.lowerInclusive {
			return false
		}
	}
	if c.upperSet {
		if numeric > c.upper {
			return false
		}
		if numeric == c.upper && !c.upperInclusive {
			return false
		}
	}
	return true
}

func flattenWhereConjuncts(raw string) ([]string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, false
	}
	if strings.HasPrefix(raw, "(") && strings.HasSuffix(raw, ")") && parensAreBalanced(raw[1:len(raw)-1]) {
		return flattenWhereConjuncts(raw[1 : len(raw)-1])
	}
	if _, _, ok := splitTopLevelCompressedBoolean(raw, "OR"); ok {
		return nil, false
	}
	if _, _, ok := splitTopLevelKeyword(raw, "OR"); ok {
		return nil, false
	}
	if _, _, ok := splitTopLevelCompressedBoolean(raw, "XOR"); ok {
		return nil, false
	}
	if _, _, ok := splitTopLevelKeyword(raw, "XOR"); ok {
		return nil, false
	}
	if left, right, ok := splitTopLevelCompressedBoolean(raw, "AND"); ok {
		leftConjuncts, leftOK := flattenWhereConjuncts(left)
		rightConjuncts, rightOK := flattenWhereConjuncts(right)
		if !leftOK || !rightOK {
			return nil, false
		}
		return append(leftConjuncts, rightConjuncts...), true
	}
	if left, right, ok := splitTopLevelKeyword(raw, "AND"); ok {
		leftConjuncts, leftOK := flattenWhereConjuncts(left)
		rightConjuncts, rightOK := flattenWhereConjuncts(right)
		if !leftOK || !rightOK {
			return nil, false
		}
		return append(leftConjuncts, rightConjuncts...), true
	}
	return []string{raw}, true
}

func edgePropertyReference(expr string, edgeVar string) (string, bool) {
	edgeVar = strings.TrimSpace(edgeVar)
	if edgeVar == "" {
		return "", false
	}
	base, fields, ok := splitTopLevelFieldAccess(expr)
	if !ok || len(fields) != 1 {
		return "", false
	}
	base = strings.TrimSpace(base)
	if inner, wrapped := unwrapOuterParentheses(base); wrapped {
		base = strings.TrimSpace(inner)
	}
	if base != edgeVar {
		return "", false
	}
	property := strings.TrimSpace(fields[0])
	if property == "" || strings.EqualFold(property, "id") || strings.EqualFold(property, "type") {
		return "", false
	}
	return property, true
}

func reverseComparisonOperator(op string) string {
	switch strings.TrimSpace(op) {
	case "<":
		return ">"
	case "<=":
		return ">="
	case ">":
		return "<"
	case ">=":
		return "<="
	case "=":
		return "="
	default:
		return ""
	}
}

func mergeEdgeNumericRangeConstraint(dst *edgeNumericRangeConstraint, src edgeNumericRangeConstraint) {
	if dst == nil {
		return
	}
	if src.lowerSet {
		dst.applyLower(src.lower, src.lowerInclusive)
	}
	if src.upperSet {
		dst.applyUpper(src.upper, src.upperInclusive)
	}
}

func contradictoryEdgeNumericRangeConstraint() edgeNumericRangeConstraint {
	constraint := edgeNumericRangeConstraint{}
	constraint.applyLower(1, true)
	constraint.applyUpper(0, true)
	return constraint
}

func extractEdgeAbsDifferenceConstraint(conjunct, edgeVar string, row Row, params Params) (string, edgeNumericRangeConstraint, bool) {
	left, right, op, ok := splitTopLevelComparison(strings.TrimSpace(conjunct))
	if !ok {
		return "", edgeNumericRangeConstraint{}, false
	}

	comparisonOp := strings.TrimSpace(op)
	absExpr := ""
	scalarExpr := ""
	if _, ok := parseFunctionCall(left, "abs"); ok {
		absExpr = left
		scalarExpr = right
	} else if _, ok := parseFunctionCall(right, "abs"); ok {
		absExpr = right
		scalarExpr = left
		comparisonOp = reverseComparisonOperator(comparisonOp)
		if comparisonOp == "" {
			return "", edgeNumericRangeConstraint{}, false
		}
	} else {
		return "", edgeNumericRangeConstraint{}, false
	}

	arg, ok := parseFunctionCall(absExpr, "abs")
	if !ok {
		return "", edgeNumericRangeConstraint{}, false
	}

	leftTerm, rightTerm, termOp, ok := splitTopLevelOperatorSetLast(arg, "+", "-")
	if !ok || strings.TrimSpace(termOp) != "-" {
		return "", edgeNumericRangeConstraint{}, false
	}

	leftProp, leftIsEdge := edgePropertyReference(leftTerm, edgeVar)
	rightProp, rightIsEdge := edgePropertyReference(rightTerm, edgeVar)
	if leftIsEdge == rightIsEdge {
		return "", edgeNumericRangeConstraint{}, false
	}

	property := leftProp
	anchorExpr := rightTerm
	if rightIsEdge {
		property = rightProp
		anchorExpr = leftTerm
	}

	anchorValue, err := evalExpressionWithScope(anchorExpr, row, params)
	if err != nil {
		return "", edgeNumericRangeConstraint{}, false
	}
	anchor, ok := comparableNumericValue(anchorValue)
	if !ok {
		return "", edgeNumericRangeConstraint{}, false
	}

	scalarValue, err := evalExpressionWithScope(scalarExpr, row, params)
	if err != nil {
		return "", edgeNumericRangeConstraint{}, false
	}
	radius, ok := comparableNumericValue(scalarValue)
	if !ok {
		return "", edgeNumericRangeConstraint{}, false
	}

	constraint := edgeNumericRangeConstraint{}
	switch comparisonOp {
	case "<=":
		if radius < 0 {
			return property, contradictoryEdgeNumericRangeConstraint(), true
		}
		constraint.applyLower(anchor-radius, true)
		constraint.applyUpper(anchor+radius, true)
		return property, constraint, true
	case "<":
		if radius <= 0 {
			return property, contradictoryEdgeNumericRangeConstraint(), true
		}
		constraint.applyLower(anchor-radius, false)
		constraint.applyUpper(anchor+radius, false)
		return property, constraint, true
	default:
		return "", edgeNumericRangeConstraint{}, false
	}
}

func edgeMatchesNumericConstraints(edge *graph.Edge, constraints map[string]edgeNumericRangeConstraint) bool {
	if edge == nil {
		return false
	}
	for property, constraint := range constraints {
		if edge.Properties == nil {
			return false
		}
		raw, ok := edge.Properties[property]
		if !ok {
			return false
		}
		if !constraint.matchesValue(decodeStoredPropertyValue(raw)) {
			return false
		}
	}
	return true
}

func extractDirectedWhereRightExclusionSet(ctx context.Context, tx graph.Tx, tenant, whereRaw, rightVar string, row Row, params Params) (map[string]struct{}, bool, error) {
	rightVar = strings.TrimSpace(rightVar)
	if rightVar == "" {
		return nil, false, nil
	}
	conjuncts, ok := flattenWhereConjuncts(whereRaw)
	if !ok || len(conjuncts) == 0 {
		return nil, false, nil
	}
	for _, conjunct := range conjuncts {
		conjunct = strings.TrimSpace(conjunct)
		if !hasLogicalNotPrefix(conjunct) {
			continue
		}
		operand := strings.TrimSpace(conjunct[3:])
		pattern, err := parseDirectedRelationshipPattern(operand)
		if err != nil {
			continue
		}
		if strings.TrimSpace(pattern.EdgeVar) != "" || strings.TrimSpace(pattern.EdgeProps) != "" || len(pattern.EdgeAnyOf) != 0 {
			continue
		}
		if strings.TrimSpace(pattern.Right.Var) != rightVar {
			continue
		}
		src, bound, err := resolveBoundPredicateVertex(ctx, tx, tenant, row, pattern.Left, params)
		if err != nil {
			return nil, false, err
		}
		if !bound || src == nil {
			continue
		}
		neighbors, err := loadWherePatternPredicateNeighbors(ctx, tx, tenant, src.ID, strings.TrimSpace(pattern.EdgeType), true, params)
		if err != nil {
			return nil, false, err
		}
		if neighbors == nil {
			neighbors = map[string]struct{}{}
		}
		return neighbors, true, nil
	}
	return nil, false, nil
}

func directedWhereCoveredByExtractedPrefilters(whereRaw, edgeVar, rightVar string, row Row, params Params, hasNumericConstraints, hasExcludedRightIDs bool) bool {
	conjuncts, ok := flattenWhereConjuncts(whereRaw)
	if !ok || len(conjuncts) == 0 {
		return false
	}

	for _, conjunct := range conjuncts {
		conjunct = strings.TrimSpace(conjunct)
		if conjunct == "" {
			continue
		}

		if hasLogicalNotPrefix(conjunct) {
			if !hasExcludedRightIDs {
				return false
			}
			operand := strings.TrimSpace(conjunct[3:])
			pattern, err := parseDirectedRelationshipPattern(operand)
			if err != nil {
				return false
			}
			if strings.TrimSpace(pattern.EdgeVar) != "" || strings.TrimSpace(pattern.EdgeProps) != "" || len(pattern.EdgeAnyOf) != 0 {
				return false
			}
			if strings.TrimSpace(pattern.Right.Var) != strings.TrimSpace(rightVar) {
				return false
			}
			continue
		}

		left, right, _, ok := splitTopLevelComparison(conjunct)
		if !ok {
			return false
		}
		leftProp, leftIsEdge := edgePropertyReference(left, edgeVar)
		rightProp, rightIsEdge := edgePropertyReference(right, edgeVar)
		if leftIsEdge == rightIsEdge {
			return false
		}
		if !hasNumericConstraints {
			return false
		}

		scalarExpr := right
		if rightIsEdge {
			scalarExpr = left
		}
		if strings.TrimSpace(leftProp) == "" && strings.TrimSpace(rightProp) == "" {
			return false
		}
		scalarValue, err := evalExpressionWithScope(scalarExpr, row, params)
		if err != nil {
			return false
		}
		if _, numericOK := comparableNumericValue(scalarValue); !numericOK {
			return false
		}
	}
	return true
}

func extractEdgeWhereNumericConstraints(whereRaw, edgeVar string, row Row, params Params) (map[string]edgeNumericRangeConstraint, bool) {
	edgeVar = strings.TrimSpace(edgeVar)
	if edgeVar == "" {
		return nil, false
	}
	conjuncts, ok := flattenWhereConjuncts(whereRaw)
	if !ok || len(conjuncts) == 0 {
		return nil, false
	}
	constraints := map[string]edgeNumericRangeConstraint{}
	for _, conjunct := range conjuncts {
		if property, absConstraint, ok := extractEdgeAbsDifferenceConstraint(conjunct, edgeVar, row, params); ok {
			merged := constraints[property]
			mergeEdgeNumericRangeConstraint(&merged, absConstraint)
			constraints[property] = merged
			continue
		}

		left, right, op, ok := splitTopLevelComparison(strings.TrimSpace(conjunct))
		if !ok {
			continue
		}
		leftProp, leftIsEdge := edgePropertyReference(left, edgeVar)
		rightProp, rightIsEdge := edgePropertyReference(right, edgeVar)
		if leftIsEdge && rightIsEdge {
			continue
		}
		if !leftIsEdge && !rightIsEdge {
			continue
		}

		property := leftProp
		scalarExpr := right
		normalizedOp := strings.TrimSpace(op)
		if rightIsEdge {
			property = rightProp
			scalarExpr = left
			normalizedOp = reverseComparisonOperator(op)
			if normalizedOp == "" {
				continue
			}
		}

		scalarValue, err := evalExpressionWithScope(scalarExpr, row, params)
		if err != nil {
			continue
		}
		numeric, numericOK := comparableNumericValue(scalarValue)
		if !numericOK {
			continue
		}

		constraint := constraints[property]
		switch normalizedOp {
		case ">":
			constraint.applyLower(numeric, false)
		case ">=":
			constraint.applyLower(numeric, true)
		case "<":
			constraint.applyUpper(numeric, false)
		case "<=":
			constraint.applyUpper(numeric, true)
		case "=":
			constraint.applyLower(numeric, true)
			constraint.applyUpper(numeric, true)
		default:
			continue
		}
		merged := constraints[property]
		mergeEdgeNumericRangeConstraint(&merged, constraint)
		constraints[property] = merged
	}
	if len(constraints) == 0 {
		return nil, false
	}
	return constraints, true
}

func (e *Executor) resolveEdgesByIndexedProperty(ctx context.Context, tx graph.Tx, tenant, edgeType string, edgeAnyOf []string, edgeProps, edgeVar, whereRaw string, leftVar string, row Row, params Params, sourceVertexIDs map[string]struct{}, candidateLimit int) ([]*graph.Edge, bool, bool, error) {
	if e.indexCatalog == nil {
		return nil, false, false, nil
	}
	if candidateLimit <= 0 {
		candidateLimit = edgeRangeIndexCandidateLimit
	}
	types := edgePatternCandidateTypes(edgeType, edgeAnyOf)
	if len(types) == 0 {
		return nil, false, false, nil
	}
	hasSourceFilter := len(sourceVertexIDs) > 0

	strategy := "edge_property_index"
	ids := map[string]struct{}{}
	preloadedEdges := map[string]*graph.Edge{}
	errEdgeIndexCapReached := errors.New("edge index candidate limit reached")

	if prop, value, ok := edgePropertyEquality(edgeProps, params, row); ok {
		for _, candidateType := range types {
			indexed := e.indexCatalog.HasEdgePropertyIndex(tenant, candidateType, prop)
			e.metrics.ObserveIndexCandidate(tenant, candidateType, prop, indexed)
			if !indexed {
				return nil, false, false, nil
			}
		}

		encoded := valueToBytes(value)
		for _, candidateType := range types {
			err := tx.ScanPropertyIndex(ctx, tenant, candidateType, prop, encoded, 0, func(entry *graph.PropertyIndexEntry) error {
				if entry == nil || entry.EntityClass != "edge" {
					return nil
				}
				if hasSourceFilter {
					edge, err := tx.GetEdge(ctx, tenant, entry.EntityID)
					if err != nil {
						if graph.IsKind(err, graph.ErrKindNotFound) {
							return nil
						}
						return err
					}
					if edge == nil {
						return nil
					}
					if _, ok := sourceVertexIDs[strings.TrimSpace(edge.SrcID)]; !ok {
						return nil
					}
					preloadedEdges[entry.EntityID] = edge
				}
				ids[entry.EntityID] = struct{}{}
				if len(ids) > candidateLimit {
					return errEdgeIndexCapReached
				}
				return nil
			})
			if err != nil {
				if errors.Is(err, errEdgeIndexCapReached) {
					return nil, false, true, nil
				}
				e.metrics.ObserveIndexLookup(strategy, "error", 0)
				return nil, true, false, err
			}
		}
	} else {
		// When the source vertex is already bound in the current row, a per-vertex
		// adjacency scan + residual filter is far cheaper than a global property
		// index range scan (which iterates the full edge index before narrowing by
		// source). Skip range pushdown so the caller uses the adjacency path.
		if leftVar != "" {
			if v, ok := row[leftVar]; ok && v != nil {
				return nil, false, false, nil
			}
		}
		constraints, ok := extractEdgeWhereNumericConstraints(whereRaw, edgeVar, row, params)
		if !ok {
			return nil, false, false, nil
		}

		props := make([]string, 0, len(constraints))
		for property := range constraints {
			props = append(props, property)
		}
		sort.Strings(props)

		selectedProp := ""
		selectedConstraint := edgeNumericRangeConstraint{}
		for _, property := range props {
			allIndexed := true
			for _, candidateType := range types {
				indexed := e.indexCatalog.HasEdgePropertyIndex(tenant, candidateType, property)
				e.metrics.ObserveIndexCandidate(tenant, candidateType, property, indexed)
				if !indexed {
					allIndexed = false
				}
			}
			if allIndexed {
				selectedProp = property
				selectedConstraint = constraints[property]
				break
			}
		}
		if selectedProp == "" {
			return nil, false, false, nil
		}
		if selectedConstraint.isContradictory() {
			e.metrics.ObserveIndexLookup("edge_property_index_range", "miss", 0)
			return nil, true, false, nil
		}

		strategy = "edge_property_index_range"
		for _, candidateType := range types {
			err := tx.ScanPropertyIndexAll(ctx, tenant, candidateType, selectedProp, 0, func(entry *graph.PropertyIndexEntry) error {
				if entry == nil || entry.EntityClass != "edge" {
					return nil
				}
				if !selectedConstraint.matchesValue(decodeStoredPropertyValue(entry.Value)) {
					return nil
				}
				if hasSourceFilter {
					edge, err := tx.GetEdge(ctx, tenant, entry.EntityID)
					if err != nil {
						if graph.IsKind(err, graph.ErrKindNotFound) {
							return nil
						}
						return err
					}
					if edge == nil {
						return nil
					}
					if _, ok := sourceVertexIDs[strings.TrimSpace(edge.SrcID)]; !ok {
						return nil
					}
					preloadedEdges[entry.EntityID] = edge
				}
				ids[entry.EntityID] = struct{}{}
				if len(ids) > candidateLimit {
					return errEdgeIndexCapReached
				}
				return nil
			})
			if err != nil {
				if errors.Is(err, errEdgeIndexCapReached) {
					return nil, false, true, nil
				}
				e.metrics.ObserveIndexLookup(strategy, "error", 0)
				return nil, true, false, err
			}
		}
	}

	out := make([]*graph.Edge, 0, len(ids))
	for id := range ids {
		edge := preloadedEdges[id]
		if edge == nil {
			var err error
			edge, err = tx.GetEdge(ctx, tenant, id)
			if err != nil {
				if graph.IsKind(err, graph.ErrKindNotFound) {
					continue
				}
				e.metrics.ObserveIndexLookup("edge_property_index", "error", 0)
				return nil, true, false, err
			}
		}
		if !edgeTypeMatches(edge, edgeType, edgeAnyOf) {
			continue
		}
		if !edgePatternMatches(edge, edgeProps, params, row) {
			continue
		}
		out = append(out, edge)
	}
	if len(out) == 0 {
		e.metrics.ObserveIndexLookup(strategy, "miss", 0)
		return nil, true, false, nil
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].ID < out[j].ID
	})
	e.metrics.ObserveIndexLookup(strategy, "hit", len(out))
	return out, true, false, nil
}

func vertexMatchesProperty(vertex *graph.Vertex, prop string, encoded []byte, label string) bool {
	if vertex == nil {
		return false
	}
	if label != "" {
		matched := false
		for _, current := range vertex.Labels {
			if current == label {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	if vertex.Properties == nil {
		return false
	}
	value, ok := vertex.Properties[prop]
	if !ok {
		return false
	}
	return bytes.Equal(value, encoded)
}

func (e *Executor) applyCreateClause(ctx context.Context, tx graph.Tx, rows []Row, clause ast.Clause, params Params, merge bool) ([]Row, error) {
	rawClause := stripLeadingClauseKeyword(clause.Raw, string(clause.Kind))
	raw := normalizeClauseBody(stripCypherLineComments(rawClause))
	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return nil, err
	}
	if rows == nil {
		rows = []Row{{}}
	}
	parts := splitTopLevelCommaSeparated(raw)
	if merge {
		parts = []string{raw}
	}
	out := rows
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		patternRaw := part
		onCreateSet := ""
		onMatchSet := ""
		if merge {
			patternRaw = normalizeClauseBody(stripCypherLineComments(clause.MergePattern))
			onCreateSet = normalizeClauseBody(stripCypherLineComments(clause.MergeOnCreate))
			onMatchSet = normalizeClauseBody(stripCypherLineComments(clause.MergeOnMatch))
			if patternRaw == "" {
				return nil, graph.NewError(graph.ErrKindUnsupported, "MERGE clause requires structured pattern metadata", nil)
			}
		}

		out, err = e.applyCreatePattern(ctx, tx, out, patternRaw, params, tenant, merge)
		if err != nil {
			return nil, err
		}
		if merge {
			out, err = e.applyMergeActions(ctx, tx, out, onCreateSet, onMatchSet, params)
			if err != nil {
				return nil, err
			}
		}
	}
	if len(out) == 0 {
		return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("CREATE/MERGE pattern %q is not yet supported", raw), nil)
	}
	out = clearMergeCreatedMarker(out)
	return out, nil
}

func (e *Executor) applyMergeActions(ctx context.Context, tx graph.Tx, rows []Row, onCreateSet string, onMatchSet string, params Params) ([]Row, error) {
	onCreateSet = strings.TrimSpace(onCreateSet)
	onMatchSet = strings.TrimSpace(onMatchSet)
	if onCreateSet == "" && onMatchSet == "" {
		return rows, nil
	}
	if err := validateMergeActionAssignmentTargets(rows, onCreateSet, onMatchSet); err != nil {
		return nil, err
	}

	updated := make([]Row, 0, len(rows))
	for _, row := range rows {
		created := false
		if marker, ok := row[mergeCreatedMarkerKey]; ok {
			if flagged, ok := marker.(bool); ok {
				created = flagged
			}
		}

		current := cloneRow(row)
		if created && onCreateSet != "" {
			setClause := ast.Clause{Kind: ast.ClauseKindSet, Raw: "SET" + onCreateSet}
			nextRows, err := e.applySetClause(ctx, tx, []Row{current}, setClause, params)
			if err != nil {
				return nil, err
			}
			if len(nextRows) > 0 {
				current = nextRows[0]
			}
		}
		if !created && onMatchSet != "" {
			setClause := ast.Clause{Kind: ast.ClauseKindSet, Raw: "SET" + onMatchSet}
			nextRows, err := e.applySetClause(ctx, tx, []Row{current}, setClause, params)
			if err != nil {
				return nil, err
			}
			if len(nextRows) > 0 {
				current = nextRows[0]
			}
		}
		updated = append(updated, current)
	}
	return updated, nil
}

func validateMergeActionAssignmentTargets(rows []Row, onCreateSet string, onMatchSet string) error {
	if len(rows) == 0 {
		return nil
	}

	bound := map[string]struct{}{}
	for _, row := range rows {
		for key := range row {
			bound[key] = struct{}{}
		}
	}

	validateOne := func(raw string) error {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			return nil
		}
		for _, item := range splitTopLevelCommaSeparated(raw) {
			item = strings.TrimSpace(item)
			if item == "" {
				continue
			}
			varName := ""
			if m := setClauseRE.FindStringSubmatch(item); len(m) == 4 {
				varName = m[1]
			} else if m := setMapAppendClauseRE.FindStringSubmatch(item); len(m) == 3 {
				varName = m[1]
			} else if m := setMapReplaceClauseRE.FindStringSubmatch(item); len(m) == 3 {
				varName = m[1]
			} else if m := setLabelClauseRE.FindStringSubmatch(item); len(m) == 3 {
				varName = m[1]
			}
			if varName == "" {
				continue
			}
			if _, ok := bound[varName]; !ok {
				return &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "UndefinedVariable"}
			}
		}
		return nil
	}

	if err := validateOne(onCreateSet); err != nil {
		return err
	}
	if err := validateOne(onMatchSet); err != nil {
		return err
	}
	return nil
}

func clearMergeCreatedMarker(rows []Row) []Row {
	clean := make([]Row, 0, len(rows))
	for _, row := range rows {
		next := cloneRow(row)
		delete(next, mergeCreatedMarkerKey)
		clean = append(clean, next)
	}
	return clean
}

func (e *Executor) applyCreatePattern(ctx context.Context, tx graph.Tx, rows []Row, raw string, params Params, tenant string, merge bool) ([]Row, error) {
	if pathVar, innerPattern, ok := parseBoundPathPattern(raw); ok {
		createdRows, err := e.applyCreatePattern(ctx, tx, rows, innerPattern, params, tenant, merge)
		if err != nil {
			return nil, err
		}
		if err := bindCreatePatternPathValues(ctx, tx, createdRows, pathVar, innerPattern); err != nil {
			return nil, err
		}
		return createdRows, nil
	}

	if isMissingRelationshipTypePattern(raw) {
		return nil, &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "NoSingleRelationshipType"}
	}
	if createVariableLengthRelRE.MatchString(raw) {
		return nil, &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "CreatingVarLength"}
	}
	if createEdgePatternTwoDirectionsRE.MatchString(raw) {
		return nil, &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "RequiresDirectedRelationship"}
	}
	if m := createEdgePatternRE.FindStringSubmatch(raw); len(m) == 10 {
		return e.applyCreateEdge(ctx, tx, rows, m, params, tenant, merge, createEdgeDirectionForward)
	}
	if m := createEdgePatternReverseRE.FindStringSubmatch(raw); len(m) == 10 {
		return e.applyCreateEdge(ctx, tx, rows, m, params, tenant, merge, createEdgeDirectionReverse)
	}
	if m := createEdgePatternUndirectedRE.FindStringSubmatch(raw); len(m) == 10 {
		if !merge {
			return nil, &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "RequiresDirectedRelationship"}
		}
		return e.applyCreateEdge(ctx, tx, rows, m, params, tenant, merge, createEdgeDirectionUndirected)
	}
	if chain, ok := parseCreateChainPattern(raw); ok {
		return e.applyCreateChainPattern(ctx, tx, rows, chain, params, tenant, merge)
	}
	if m := createVertexPatternRE.FindStringSubmatch(raw); len(m) == 4 {
		return e.applyCreateVertex(ctx, tx, rows, m, params, tenant, merge)
	}
	if spec, ok := parseCreateVertexPatternSpec(raw); ok {
		return e.applyCreateVertexSpec(ctx, tx, rows, spec, params, tenant, merge)
	}
	return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("CREATE/MERGE pattern %q is not yet supported", raw), nil)
}

func bindCreatePatternPathValues(ctx context.Context, tx graph.Tx, rows []Row, pathVar, patternRaw string) error {
	if pathVar == "" {
		return nil
	}

	if spec, ok := parseCreateVertexPatternSpec(patternRaw); ok {
		if spec.Var == "" {
			for _, row := range rows {
				row[pathVar] = nil
			}
			return nil
		}
		for _, row := range rows {
			row[pathVar] = cypherPathValue{Left: vertexFromRowBinding(row, spec.Var)}
		}
		return nil
	}

	if m := createEdgePatternRE.FindStringSubmatch(patternRaw); len(m) == 10 {
		return bindCreateSingleEdgePathValues(ctx, tx, rows, pathVar, m, createEdgeDirectionForward)
	}
	if m := createEdgePatternReverseRE.FindStringSubmatch(patternRaw); len(m) == 10 {
		return bindCreateSingleEdgePathValues(ctx, tx, rows, pathVar, m, createEdgeDirectionReverse)
	}
	if m := createEdgePatternUndirectedRE.FindStringSubmatch(patternRaw); len(m) == 10 {
		return bindCreateSingleEdgePathValues(ctx, tx, rows, pathVar, m, createEdgeDirectionUndirected)
	}

	if chain, ok := parseCreateChainPattern(patternRaw); ok {
		return bindCreateChainPathValues(ctx, tx, rows, pathVar, chain)
	}

	for _, row := range rows {
		row[pathVar] = nil
	}
	return nil
}

func bindCreateSingleEdgePathValues(ctx context.Context, tx graph.Tx, rows []Row, pathVar string, m []string, direction createEdgeDirection) error {
	leftVar := strings.TrimSpace(m[1])
	edgeVar := strings.TrimSpace(m[4])
	rightVar := strings.TrimSpace(m[7])
	edgeType, err := normalizeCreateRelationshipType(m[5])
	if err != nil {
		return err
	}

	for _, row := range rows {
		left := vertexFromRowBinding(row, leftVar)
		right := vertexFromRowBinding(row, rightVar)
		edge := edgeFromRowBinding(row, edgeVar)
		if edge == nil {
			edge, err = lookupBoundPathEdge(ctx, tx, left, right, edgeType, direction)
			if err != nil {
				return err
			}
		}
		row[pathVar] = cypherPathValue{Left: left, Edge: edge, Right: right, Direction: createEdgeDirectionName(direction)}
	}
	return nil
}

func bindCreateChainPathValues(ctx context.Context, tx graph.Tx, rows []Row, pathVar string, pattern createChainPattern) error {
	for _, row := range rows {
		vertexes := make([]*graph.Vertex, 0, len(pattern.Vertexes))
		edges := make([]*graph.Edge, 0, len(pattern.Rels))
		dirs := make([]string, 0, len(pattern.Rels))
		for _, vertex := range pattern.Vertexes {
			vertexes = append(vertexes, vertexFromRowBinding(row, vertex.Var))
		}
		for i, rel := range pattern.Rels {
			edge := edgeFromRowBinding(row, rel.Var)
			if edge == nil && i+1 < len(vertexes) {
				edgeType, err := normalizeCreateRelationshipType(rel.Type)
				if err != nil {
					return err
				}
				edge, err = lookupBoundPathEdge(ctx, tx, vertexes[i], vertexes[i+1], edgeType, rel.Direction)
				if err != nil {
					return err
				}
			}
			edges = append(edges, edge)
			dirs = append(dirs, createEdgeDirectionName(rel.Direction))
		}
		if len(edges) <= 1 {
			var edge *graph.Edge
			if len(edges) == 1 {
				edge = edges[0]
			}
			left := (*graph.Vertex)(nil)
			right := (*graph.Vertex)(nil)
			if len(vertexes) > 0 {
				left = vertexes[0]
			}
			if len(vertexes) > 1 {
				right = vertexes[1]
			}
			row[pathVar] = cypherPathValue{Left: left, Edge: edge, Right: right, Direction: firstOrDefault(dirs, "forward")}
			continue
		}
		row[pathVar] = multiHopPathValue(vertexes, edges, dirs)
	}
	return nil
}

func lookupBoundPathEdge(ctx context.Context, tx graph.Tx, left, right *graph.Vertex, edgeType string, direction createEdgeDirection) (*graph.Edge, error) {
	if left == nil || right == nil || strings.TrimSpace(edgeType) == "" {
		return nil, nil
	}

	matches, err := findMergeEdges(ctx, tx, left, right, edgeType, map[string]any{}, direction)
	if err != nil {
		return nil, err
	}
	if len(matches) == 0 {
		return nil, nil
	}
	return matches[0], nil
}

func createEdgeDirectionName(direction createEdgeDirection) string {
	switch direction {
	case createEdgeDirectionReverse:
		return "reverse"
	case createEdgeDirectionUndirected:
		return "undirected"
	default:
		return "forward"
	}
}

func firstOrDefault(values []string, fallback string) string {
	if len(values) == 0 || strings.TrimSpace(values[0]) == "" {
		return fallback
	}
	return values[0]
}

func parseCreateChainPattern(raw string) (createChainPattern, bool) {
	remaining := strings.TrimSpace(raw)
	if remaining == "" {
		return createChainPattern{}, false
	}

	vertexMatch := createChainVertexTokenRE.FindStringSubmatch(remaining)
	if len(vertexMatch) != 4 {
		return createChainPattern{}, false
	}
	pattern := createChainPattern{
		Vertexes: []createChainVertexPattern{{Var: vertexMatch[1], Labels: splitLabels(vertexMatch[2]), PropsRaw: vertexMatch[3]}},
		Rels:     make([]createChainRelPattern, 0),
	}
	remaining = strings.TrimSpace(remaining[len(vertexMatch[0]):])

	for remaining != "" {
		rel, consumed, ok := parseCreateChainRelToken(remaining)
		if !ok {
			return createChainPattern{}, false
		}
		pattern.Rels = append(pattern.Rels, rel)
		remaining = strings.TrimSpace(remaining[consumed:])

		nextVertex := createChainVertexTokenRE.FindStringSubmatch(remaining)
		if len(nextVertex) != 4 {
			return createChainPattern{}, false
		}
		pattern.Vertexes = append(pattern.Vertexes, createChainVertexPattern{Var: nextVertex[1], Labels: splitLabels(nextVertex[2]), PropsRaw: nextVertex[3]})
		remaining = strings.TrimSpace(remaining[len(nextVertex[0]):])
	}

	if len(pattern.Rels) == 0 || len(pattern.Vertexes) != len(pattern.Rels)+1 {
		return createChainPattern{}, false
	}
	return pattern, true
}

func parseCreateChainRelToken(raw string) (createChainRelPattern, int, bool) {
	if m := createChainRelForwardTokenRE.FindStringSubmatch(raw); len(m) == 4 {
		return createChainRelPattern{Var: m[1], Type: m[2], PropsRaw: m[3], Direction: createEdgeDirectionForward}, len(m[0]), true
	}
	if m := createChainRelReverseTokenRE.FindStringSubmatch(raw); len(m) == 4 {
		return createChainRelPattern{Var: m[1], Type: m[2], PropsRaw: m[3], Direction: createEdgeDirectionReverse}, len(m[0]), true
	}
	if m := createChainRelUndirTokenRE.FindStringSubmatch(raw); len(m) == 4 {
		return createChainRelPattern{Var: m[1], Type: m[2], PropsRaw: m[3], Direction: createEdgeDirectionUndirected}, len(m[0]), true
	}
	return createChainRelPattern{}, 0, false
}

func (e *Executor) applyCreateVertex(ctx context.Context, tx graph.Tx, rows []Row, m []string, params Params, tenant string, merge bool) ([]Row, error) {
	return e.applyCreateVertexSpec(ctx, tx, rows, createVertexPatternSpec{Var: m[1], Labels: splitLabels(m[2]), PropsRaw: m[3]}, params, tenant, merge)
}

func (e *Executor) applyCreateVertexSpec(ctx context.Context, tx graph.Tx, rows []Row, spec createVertexPatternSpec, params Params, tenant string, merge bool) ([]Row, error) {
	varName := spec.Var
	labels := spec.Labels

	out := make([]Row, 0, len(rows))
	for _, row := range rows {
		props, err := parsePropertyMap(spec.PropsRaw, params, row)
		if err != nil {
			return nil, err
		}
		if merge && hasNilPropertyValue(props) {
			return nil, graph.NewError(graph.ErrKindSemantic, "MergeReadOwnWrites", nil)
		}
		normalizedProps := normalizeVertexProperties(props)
		vertexID := ""
		if strings.TrimSpace(varName) != "" {
			if derivedID, ok := vertexIDFromPatternProperties(props); ok {
				vertexID = derivedID
			}
		}
		if vertexID == "" {
			if varName != "" {
				if existing, ok := row[varName].(*graph.Vertex); ok {
					vertexID = existing.ID
				}
			}
		}

		if merge && vertexID == "" {
			matches, err := e.findMergeVerticesByPattern(ctx, tx, tenant, labels, props)
			if err != nil {
				return nil, err
			}
			if len(matches) == 0 {
				vertex := &graph.Vertex{Tenant: tenant, ID: nextAutoVertexID(varName), Labels: labels, Properties: normalizedProps}
				if err := e.putVertexWithIndexes(ctx, tx, vertex); err != nil {
					return nil, err
				}
				merged := cloneRow(row)
				merged[mergeCreatedMarkerKey] = true
				if varName != "" {
					merged[varName] = vertex
				}
				out = append(out, merged)
				continue
			}
			for _, match := range matches {
				merged := cloneRow(row)
				merged[mergeCreatedMarkerKey] = false
				if varName != "" {
					merged[varName] = match
				}
				out = append(out, merged)
			}
			continue
		}

		if vertexID == "" {
			vertexID = nextAutoVertexID(varName)
		}

		vertex := &graph.Vertex{Tenant: tenant, ID: vertexID, Labels: labels, Properties: normalizedProps}
		created := true
		if merge {
			if existing, err := tx.GetVertex(ctx, vertex.Tenant, vertex.ID); err == nil {
				vertex = existing
				created = false
			}
		}
		if err := e.putVertexWithIndexes(ctx, tx, vertex); err != nil {
			return nil, err
		}
		merged := cloneRow(row)
		if merge {
			merged[mergeCreatedMarkerKey] = created
		}
		if varName != "" {
			merged[varName] = vertex
		}
		out = append(out, merged)
	}
	return out, nil
}

func hasNilPropertyValue(props map[string]any) bool {
	for _, value := range props {
		if value == nil {
			return true
		}
	}
	return false
}

func (e *Executor) findMergeVerticesByPattern(ctx context.Context, tx graph.Tx, tenant string, labels []string, props map[string]any) ([]*graph.Vertex, error) {
	type lookupPlan struct {
		schema   string
		property string
		value    any
	}

	plan := lookupPlan{}
	hasPlan := false
	if len(labels) > 0 && len(props) > 0 {
		propKeys := make([]string, 0, len(props))
		for prop := range props {
			propKeys = append(propKeys, prop)
		}
		sort.Strings(propKeys)

		for _, label := range labels {
			for _, prop := range propKeys {
				indexed := e.indexCatalog != nil && e.indexCatalog.HasPropertyIndex(tenant, label, prop)
				e.metrics.ObserveIndexCandidate(tenant, label, prop, indexed)
				if hasPlan || !indexed {
					continue
				}
				plan = lookupPlan{schema: label, property: prop, value: props[prop]}
				hasPlan = true
			}
		}
	}

	if hasPlan {
		encoded := valueToBytes(plan.value)
		ids := map[string]struct{}{}
		err := tx.ScanPropertyIndex(ctx, tenant, plan.schema, plan.property, encoded, 0, func(entry *graph.PropertyIndexEntry) error {
			ids[entry.EntityID] = struct{}{}
			return nil
		})
		if err != nil {
			e.metrics.ObserveIndexLookup("property_index", "error", 0)
			return nil, err
		}

		matches := make([]*graph.Vertex, 0, len(ids))
		for id := range ids {
			vertex, err := tx.GetVertex(ctx, tenant, id)
			if err != nil {
				if graph.IsKind(err, graph.ErrKindNotFound) {
					continue
				}
				e.metrics.ObserveIndexLookup("property_index", "error", 0)
				return nil, err
			}
			if !vertexMatchesMergePattern(vertex, labels, props) {
				continue
			}
			matches = append(matches, vertex)
		}
		if len(matches) == 0 {
			e.metrics.ObserveIndexLookup("property_index", "miss", 0)
			return nil, nil
		}
		e.metrics.ObserveIndexLookup("property_index", "hit", len(matches))
		return matches, nil
	}

	matches := make([]*graph.Vertex, 0)
	err := tx.ScanVertices(ctx, tenant, 0, func(vertex *graph.Vertex) error {
		if !vertexMatchesMergePattern(vertex, labels, props) {
			return nil
		}
		matches = append(matches, vertex)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return matches, nil
}

func vertexMatchesMergePattern(vertex *graph.Vertex, labels []string, props map[string]any) bool {
	if vertex == nil {
		return false
	}
	if len(labels) > 0 {
		for _, want := range labels {
			found := false
			for _, current := range vertex.Labels {
				if current == want {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
	}
	for key, expected := range props {
		stored, ok := vertex.Properties[key]
		if !ok {
			return false
		}
		actual := decodeStoredPropertyValue(stored)
		if !mergePropertyValueEqual(expected, actual) {
			return false
		}
	}
	return true
}

func (e *Executor) putVertexWithIndexes(ctx context.Context, tx graph.Tx, vertex *graph.Vertex) error {
	if err := tx.PutVertex(ctx, vertex); err != nil {
		return err
	}
	if e.indexCatalog == nil || vertex == nil {
		return nil
	}
	for _, label := range vertex.Labels {
		for property, encoded := range vertex.Properties {
			if !e.indexCatalog.HasPropertyIndex(vertex.Tenant, label, property) {
				continue
			}
			valueCopy := append([]byte(nil), encoded...)
			if err := tx.PutPropertyIndex(ctx, &graph.PropertyIndexEntry{
				Tenant:      vertex.Tenant,
				Schema:      label,
				Property:    property,
				Value:       valueCopy,
				EntityID:    vertex.ID,
				EntityClass: "vertex",
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *Executor) putEdgeWithIndexes(ctx context.Context, tx graph.Tx, edge *graph.Edge) error {
	if err := tx.PutEdge(ctx, edge); err != nil {
		return err
	}
	if e.indexCatalog == nil || edge == nil || strings.TrimSpace(edge.Type) == "" {
		return nil
	}
	for property, encoded := range edge.Properties {
		if !e.indexCatalog.HasEdgePropertyIndex(edge.Tenant, edge.Type, property) {
			continue
		}
		valueCopy := append([]byte(nil), encoded...)
		if err := tx.PutPropertyIndex(ctx, &graph.PropertyIndexEntry{
			Tenant:      edge.Tenant,
			Schema:      edge.Type,
			Property:    property,
			Value:       valueCopy,
			EntityID:    edge.ID,
			EntityClass: "edge",
		}); err != nil {
			return err
		}
	}
	return nil
}

func mergePropertyValueEqual(expected, actual any) bool {
	if reflect.DeepEqual(expected, actual) {
		return true
	}

	switch exp := expected.(type) {
	case int:
		switch typed := actual.(type) {
		case int64:
			return int64(exp) == typed
		case json.Number:
			if i, err := typed.Int64(); err == nil {
				return int64(exp) == i
			}
		}
	case int64:
		switch typed := actual.(type) {
		case int:
			return exp == int64(typed)
		case json.Number:
			if i, err := typed.Int64(); err == nil {
				return exp == i
			}
		}
	case float64:
		if num, ok := actual.(json.Number); ok {
			if f, err := num.Float64(); err == nil {
				return exp == f
			}
		}
	case json.Number:
		switch typed := actual.(type) {
		case int:
			if i, err := exp.Int64(); err == nil {
				return i == int64(typed)
			}
		case int64:
			if i, err := exp.Int64(); err == nil {
				return i == typed
			}
		case float64:
			if f, err := exp.Float64(); err == nil {
				return f == typed
			}
		}
	}

	return fmt.Sprint(expected) == fmt.Sprint(actual)
}

func parseCreateVertexPatternSpec(raw string) (createVertexPatternSpec, bool) {
	raw = strings.TrimSpace(raw)
	if !strings.HasPrefix(raw, "(") || !strings.HasSuffix(raw, ")") {
		return createVertexPatternSpec{}, false
	}
	body := strings.TrimSpace(raw[1 : len(raw)-1])
	if body == "" {
		return createVertexPatternSpec{}, true
	}

	head := body
	propsRaw := ""
	if idx := strings.Index(body, "{"); idx >= 0 {
		end, ok := findMatchingDelimiter(body, idx, '{', '}')
		if !ok {
			return createVertexPatternSpec{}, false
		}
		if strings.TrimSpace(body[end+1:]) != "" {
			return createVertexPatternSpec{}, false
		}
		head = strings.TrimSpace(body[:idx])
		propsRaw = strings.TrimSpace(body[idx+1 : end])
	}

	varName := ""
	labelsRaw := ""
	if strings.HasPrefix(head, ":") {
		labelsRaw = strings.TrimPrefix(head, ":")
	} else if strings.Contains(head, ":") {
		parts := strings.SplitN(head, ":", 2)
		varName = strings.TrimSpace(parts[0])
		labelsRaw = strings.TrimSpace(parts[1])
	} else {
		varName = strings.TrimSpace(head)
	}

	if varName != "" && !identifierRE.MatchString(varName) {
		return createVertexPatternSpec{}, false
	}
	return createVertexPatternSpec{Var: varName, Labels: splitLabels(labelsRaw), PropsRaw: propsRaw}, true
}

func findMatchingDelimiter(raw string, start int, open byte, close byte) (int, bool) {
	if start < 0 || start >= len(raw) || raw[start] != open {
		return -1, false
	}
	depth := 0
	inSingle := false
	inDouble := false
	for i := start; i < len(raw); i++ {
		ch := raw[i]
		switch ch {
		case '\'':
			if !inDouble {
				inSingle = !inSingle
			}
		case '"':
			if !inSingle {
				inDouble = !inDouble
			}
		}
		if inSingle || inDouble {
			continue
		}
		if ch == open {
			depth++
			continue
		}
		if ch == close {
			depth--
			if depth == 0 {
				return i, true
			}
		}
	}
	return -1, false
}

func (e *Executor) applyCreateEdge(ctx context.Context, tx graph.Tx, rows []Row, m []string, params Params, tenant string, merge bool, direction createEdgeDirection) ([]Row, error) {
	leftVar := m[1]
	leftLabels := splitLabels(m[2])
	edgeVar := m[4]
	edgeType, err := normalizeCreateRelationshipType(m[5])
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(edgeType) == "" {
		return nil, &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "NoSingleRelationshipType"}
	}
	rightVar := m[7]
	rightLabels := splitLabels(m[8])
	undirectedSeenEdges := map[string]struct{}{}

	out := make([]Row, 0, len(rows))
	for _, row := range rows {
		workRow := cloneRow(row)

		leftProps, err := parsePropertyMap(m[3], params, workRow)
		if err != nil {
			return nil, err
		}
		leftVertex, leftCreated, err := e.resolveOrCreateVertex(ctx, tx, tenant, workRow, leftVar, leftLabels, leftProps, merge)
		if err != nil {
			return nil, err
		}
		if leftVar != "" {
			workRow[leftVar] = leftVertex
		}

		edgeProps, err := parsePropertyMap(m[6], params, workRow)
		if err != nil {
			return nil, err
		}
		rightProps, err := parsePropertyMap(m[9], params, workRow)
		if err != nil {
			return nil, err
		}
		rightVertex, rightCreated, err := e.resolveOrCreateVertex(ctx, tx, tenant, workRow, rightVar, rightLabels, rightProps, merge)
		if err != nil {
			return nil, err
		}
		if rightVar != "" {
			workRow[rightVar] = rightVertex
		}

		if merge {
			matchedEdges, err := findMergeEdges(ctx, tx, leftVertex, rightVertex, edgeType, edgeProps, direction)
			if err != nil {
				return nil, err
			}
			if len(matchedEdges) > 0 {
				seenEdges := map[string]struct{}{}
				for _, edge := range matchedEdges {
					if edge == nil {
						continue
					}
					if _, seen := seenEdges[edge.ID]; seen {
						continue
					}
					if direction == createEdgeDirectionUndirected {
						if _, seen := undirectedSeenEdges[edge.ID]; seen {
							continue
						}
						undirectedSeenEdges[edge.ID] = struct{}{}
					}
					seenEdges[edge.ID] = struct{}{}
					merged := cloneRow(workRow)
					merged[mergeCreatedMarkerKey] = leftCreated || rightCreated
					if leftVar != "" {
						merged[leftVar] = leftVertex
					}
					if rightVar != "" {
						merged[rightVar] = rightVertex
					}
					if edgeVar != "" {
						merged[edgeVar] = edge
					}
					out = append(out, merged)
				}
				continue
			}
		}

		edge, edgeCreated, err := e.createOrMergeEdge(ctx, tx, leftVertex, rightVertex, edgeType, edgeProps, merge, direction)
		if err != nil {
			return nil, err
		}

		merged := cloneRow(workRow)
		if merge {
			merged[mergeCreatedMarkerKey] = leftCreated || rightCreated || edgeCreated
		}
		if leftVar != "" {
			merged[leftVar] = leftVertex
		}
		if rightVar != "" {
			merged[rightVar] = rightVertex
		}
		if edgeVar != "" {
			merged[edgeVar] = edge
		}
		out = append(out, merged)
	}
	return out, nil
}

func (e *Executor) applyCreateChainPattern(ctx context.Context, tx graph.Tx, rows []Row, pattern createChainPattern, params Params, tenant string, merge bool) ([]Row, error) {
	out := make([]Row, 0, len(rows))
	for _, row := range rows {
		merged := cloneRow(row)
		vertices := make([]*graph.Vertex, len(pattern.Vertexes))
		createdAny := false

		for i, vertexPattern := range pattern.Vertexes {
			props, err := parsePropertyMap(vertexPattern.PropsRaw, params, merged)
			if err != nil {
				return nil, err
			}
			vertex, vertexCreated, err := e.resolveOrCreateVertex(ctx, tx, tenant, merged, vertexPattern.Var, vertexPattern.Labels, props, merge)
			if err != nil {
				return nil, err
			}
			if merge && vertexCreated {
				createdAny = true
			}
			vertices[i] = vertex
			if vertexPattern.Var != "" {
				merged[vertexPattern.Var] = vertex
			}
		}

		for i, rel := range pattern.Rels {
			relType, err := normalizeCreateRelationshipType(rel.Type)
			if err != nil {
				return nil, err
			}
			if strings.TrimSpace(relType) == "" {
				return nil, &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "NoSingleRelationshipType"}
			}
			relProps, err := parsePropertyMap(rel.PropsRaw, params, merged)
			if err != nil {
				return nil, err
			}
			edge, edgeCreated, err := e.createOrMergeEdge(ctx, tx, vertices[i], vertices[i+1], relType, relProps, merge, rel.Direction)
			if err != nil {
				return nil, err
			}
			if merge && edgeCreated {
				createdAny = true
			}
			if rel.Var != "" {
				merged[rel.Var] = edge
			}
		}
		if merge {
			merged[mergeCreatedMarkerKey] = createdAny
		}

		out = append(out, merged)
	}
	return out, nil
}

func (e *Executor) createOrMergeEdge(ctx context.Context, tx graph.Tx, leftVertex, rightVertex *graph.Vertex, edgeType string, edgeProps map[string]any, merge bool, direction createEdgeDirection) (*graph.Edge, bool, error) {
	if merge && hasNilPropertyValue(edgeProps) {
		return nil, false, graph.NewError(graph.ErrKindSemantic, "MergeReadOwnWrites", nil)
	}

	srcVertex := leftVertex
	dstVertex := rightVertex
	switch direction {
	case createEdgeDirectionReverse:
		srcVertex = rightVertex
		dstVertex = leftVertex
	case createEdgeDirectionForward, createEdgeDirectionUndirected:
		// Keep CREATE default direction left-to-right.
	default:
		return nil, false, graph.NewError(graph.ErrKindInvalidInput, fmt.Sprintf("unknown edge direction %q", direction), nil)
	}

	if merge {
		matches, err := findMergeEdges(ctx, tx, leftVertex, rightVertex, edgeType, edgeProps, direction)
		if err != nil {
			return nil, false, err
		}
		if len(matches) > 0 {
			return matches[0], false, nil
		}
	}

	edgeID := ""
	if merge {
		edgeID = nextAutoEdgeID(srcVertex.Tenant, srcVertex.ID, edgeType, dstVertex.ID)
	} else {
		edgeID = syntheticEdgeID(srcVertex.Tenant, srcVertex.ID, edgeType, dstVertex.ID)
		if _, err := tx.GetEdge(ctx, srcVertex.Tenant, edgeID); err == nil {
			edgeID = nextAutoEdgeID(srcVertex.Tenant, srcVertex.ID, edgeType, dstVertex.ID)
		} else if !graph.IsKind(err, graph.ErrKindNotFound) {
			return nil, false, err
		}
	}

	edge := &graph.Edge{
		Tenant:     srcVertex.Tenant,
		ID:         edgeID,
		Type:       edgeType,
		SrcID:      srcVertex.ID,
		DstID:      dstVertex.ID,
		Properties: normalizeEdgeProperties(edgeProps),
	}
	if err := e.putEdgeWithIndexes(ctx, tx, edge); err != nil {
		return nil, false, err
	}
	return edge, true, nil
}

func findMergeEdges(ctx context.Context, tx graph.Tx, leftVertex, rightVertex *graph.Vertex, edgeType string, edgeProps map[string]any, direction createEdgeDirection) ([]*graph.Edge, error) {
	if leftVertex == nil || rightVertex == nil {
		return nil, nil
	}

	appendMatches := func(out []*graph.Edge, seen map[string]struct{}, srcID, dstID string) ([]*graph.Edge, error) {
		err := tx.ScanOutEdges(ctx, leftVertex.Tenant, srcID, edgeType, 0, func(edge *graph.Edge) error {
			if edge.DstID != dstID {
				return nil
			}
			if !edgeMatchesMergePattern(edge, edgeProps) {
				return nil
			}
			if _, ok := seen[edge.ID]; ok {
				return nil
			}
			seen[edge.ID] = struct{}{}
			out = append(out, edge)
			return nil
		})
		if err != nil {
			return nil, err
		}
		return out, nil
	}

	matches := make([]*graph.Edge, 0)
	seen := map[string]struct{}{}
	switch direction {
	case createEdgeDirectionForward:
		return appendMatches(matches, seen, leftVertex.ID, rightVertex.ID)
	case createEdgeDirectionReverse:
		return appendMatches(matches, seen, rightVertex.ID, leftVertex.ID)
	case createEdgeDirectionUndirected:
		out, err := appendMatches(matches, seen, leftVertex.ID, rightVertex.ID)
		if err != nil {
			return nil, err
		}
		return appendMatches(out, seen, rightVertex.ID, leftVertex.ID)
	default:
		return nil, graph.NewError(graph.ErrKindInvalidInput, fmt.Sprintf("unknown edge direction %q", direction), nil)
	}
}

func edgeMatchesMergePattern(edge *graph.Edge, props map[string]any) bool {
	if edge == nil {
		return false
	}
	for key, expected := range props {
		if strings.EqualFold(key, "id") {
			if edge.ID != stringFromProperty(map[string]any{"id": expected}, "id") {
				return false
			}
			continue
		}
		if strings.EqualFold(key, "type") {
			if edge.Type != stringFromProperty(map[string]any{"type": expected}, "type") {
				return false
			}
			continue
		}
		if edge.Properties == nil {
			return false
		}
		stored, ok := edge.Properties[key]
		if !ok {
			return false
		}
		actual := decodeStoredPropertyValue(stored)
		if !mergePropertyValueEqual(expected, actual) {
			return false
		}
	}
	return true
}

func normalizeCreateRelationshipType(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil
	}
	edgeType, edgeAnyOf, err := parseEdgeTypeFilter(strings.ReplaceAll(raw, ":", ""))
	if err != nil {
		return "", err
	}
	if len(edgeAnyOf) > 0 {
		return "", &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "NoSingleRelationshipType"}
	}
	return edgeType, nil
}

func isMissingRelationshipTypePattern(raw string) bool {
	raw = strings.TrimSpace(raw)
	if strings.Contains(raw, "[") || strings.Contains(raw, "]") {
		return false
	}
	if createMissingRelTypeForwardRE.MatchString(raw) {
		return true
	}
	if createMissingRelTypeReverseRE.MatchString(raw) {
		return true
	}
	if createMissingRelTypeUndirRE.MatchString(raw) {
		return true
	}
	return false
}

func (e *Executor) applySetClause(ctx context.Context, tx graph.Tx, rows []Row, clause ast.Clause, params Params) ([]Row, error) {
	raw := normalizeClauseBody(clause.Raw)
	raw = stripNormalizedPrefix(raw, "SET")
	items := splitTopLevelCommaSeparated(raw)
	if len(items) == 0 {
		return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("SET clause %q is not yet supported", raw), nil)
	}

	out := make([]Row, 0, len(rows))
	for _, row := range rows {
		working := row
		for _, item := range items {
			item = strings.TrimSpace(item)
			if item == "" {
				continue
			}

			if varName, field, exprRaw, ok := parseSetPropertyAssignment(item); ok {
				binding, ok := working[varName]
				if !ok {
					return nil, graph.NewError(graph.ErrKindInvalidInput, fmt.Sprintf("unknown binding %q", varName), nil)
				}
				value, err := evalExpressionWithScope(exprRaw, working, params)
				if err != nil {
					value, err = evalWriteValue(exprRaw, params, working)
				}
				if err != nil {
					return nil, err
				}
				switch typed := binding.(type) {
				case *graph.Vertex:
					if field == "id" {
						return nil, graph.NewError(graph.ErrKindUnsupported, "setting vertex id is not supported", nil)
					}
					current, err := loadCurrentVertexForWrite(ctx, tx, typed)
					if err != nil {
						return nil, err
					}
					mutated := cloneVertex(current)
					ensureProperties(mutated)
					if value == nil {
						delete(mutated.Properties, field)
					} else {
						encoded, err := valueToPropertyBytes(value)
						if err != nil {
							return nil, err
						}
						mutated.Properties[field] = encoded
					}
					if err := e.putVertexWithIndexes(ctx, tx, mutated); err != nil {
						return nil, err
					}
					working = cloneRow(working)
					working[varName] = mutated
				case *graph.Edge:
					if field == "id" {
						return nil, graph.NewError(graph.ErrKindUnsupported, "setting edge id is not supported", nil)
					}
					current, err := loadCurrentEdgeForWrite(ctx, tx, typed)
					if err != nil {
						return nil, err
					}
					mutated := cloneEdge(current)
					ensurePropertiesEdge(mutated)
					if value == nil {
						delete(mutated.Properties, field)
					} else {
						encoded, err := valueToPropertyBytes(value)
						if err != nil {
							return nil, err
						}
						mutated.Properties[field] = encoded
					}
					if err := e.putEdgeWithIndexes(ctx, tx, mutated); err != nil {
						return nil, err
					}
					working = cloneRow(working)
					working[varName] = mutated
				case nil:
					continue
				default:
					return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("SET on %T is not supported", binding), nil)
				}
				continue
			}

			if match := setMapAppendClauseRE.FindStringSubmatch(item); len(match) == 3 {
				varName, exprRaw := match[1], match[2]
				binding, ok := working[varName]
				if !ok {
					return nil, graph.NewError(graph.ErrKindInvalidInput, fmt.Sprintf("unknown binding %q", varName), nil)
				}
				mapValue, err := evalSetMapValue(exprRaw, working, params)
				if err != nil {
					return nil, err
				}
				switch typed := binding.(type) {
				case *graph.Vertex:
					current, err := loadCurrentVertexForWrite(ctx, tx, typed)
					if err != nil {
						return nil, err
					}
					mutated := cloneVertex(current)
					ensureProperties(mutated)
					for key, value := range mapValue {
						if value == nil {
							delete(mutated.Properties, key)
							continue
						}
						encoded, err := valueToPropertyBytes(value)
						if err != nil {
							return nil, err
						}
						mutated.Properties[key] = encoded
					}
					if err := e.putVertexWithIndexes(ctx, tx, mutated); err != nil {
						return nil, err
					}
					working = cloneRow(working)
					working[varName] = mutated
				case *graph.Edge:
					current, err := loadCurrentEdgeForWrite(ctx, tx, typed)
					if err != nil {
						return nil, err
					}
					mutated := cloneEdge(current)
					ensurePropertiesEdge(mutated)
					for key, value := range mapValue {
						if value == nil {
							delete(mutated.Properties, key)
							continue
						}
						encoded, err := valueToPropertyBytes(value)
						if err != nil {
							return nil, err
						}
						mutated.Properties[key] = encoded
					}
					if err := e.putEdgeWithIndexes(ctx, tx, mutated); err != nil {
						return nil, err
					}
					working = cloneRow(working)
					working[varName] = mutated
				case nil:
					continue
				default:
					return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("SET on %T is not supported", binding), nil)
				}
				continue
			}

			if match := setMapReplaceClauseRE.FindStringSubmatch(item); len(match) == 3 {
				varName, exprRaw := match[1], match[2]
				binding, ok := working[varName]
				if !ok {
					return nil, graph.NewError(graph.ErrKindInvalidInput, fmt.Sprintf("unknown binding %q", varName), nil)
				}
				mapValue, err := evalSetMapValue(exprRaw, working, params)
				if err != nil {
					return nil, err
				}
				switch typed := binding.(type) {
				case *graph.Vertex:
					current, err := loadCurrentVertexForWrite(ctx, tx, typed)
					if err != nil {
						return nil, err
					}
					mutated := cloneVertex(current)
					mutated.Properties = map[string][]byte{}
					for key, value := range mapValue {
						if value == nil {
							continue
						}
						encoded, err := valueToPropertyBytes(value)
						if err != nil {
							return nil, err
						}
						mutated.Properties[key] = encoded
					}
					if err := e.putVertexWithIndexes(ctx, tx, mutated); err != nil {
						return nil, err
					}
					working = cloneRow(working)
					working[varName] = mutated
				case *graph.Edge:
					current, err := loadCurrentEdgeForWrite(ctx, tx, typed)
					if err != nil {
						return nil, err
					}
					mutated := cloneEdge(current)
					mutated.Properties = map[string][]byte{}
					for key, value := range mapValue {
						if value == nil {
							continue
						}
						encoded, err := valueToPropertyBytes(value)
						if err != nil {
							return nil, err
						}
						mutated.Properties[key] = encoded
					}
					if err := e.putEdgeWithIndexes(ctx, tx, mutated); err != nil {
						return nil, err
					}
					working = cloneRow(working)
					working[varName] = mutated
				case nil:
					continue
				default:
					return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("SET on %T is not supported", binding), nil)
				}
				continue
			}

			if match := setLabelClauseRE.FindStringSubmatch(item); len(match) == 3 {
				varName := match[1]
				labels := splitLabels(match[2])
				binding, ok := working[varName]
				if !ok {
					return nil, graph.NewError(graph.ErrKindInvalidInput, fmt.Sprintf("unknown binding %q", varName), nil)
				}
				vertex, ok := binding.(*graph.Vertex)
				if !ok {
					if binding == nil {
						continue
					}
					return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("SET on %T is not supported", binding), nil)
				}
				current, err := loadCurrentVertexForWrite(ctx, tx, vertex)
				if err != nil {
					return nil, err
				}
				mutated := cloneVertex(current)
				for _, label := range labels {
					if !vertexHasLabel(mutated, label) {
						mutated.Labels = append(mutated.Labels, label)
					}
				}
				if err := e.putVertexWithIndexes(ctx, tx, mutated); err != nil {
					return nil, err
				}
				working = cloneRow(working)
				working[varName] = mutated
				continue
			}

			return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("SET clause %q is not yet supported", raw), nil)
		}
		out = append(out, working)
	}
	return out, nil
}

func parseSetPropertyAssignment(item string) (string, string, string, bool) {
	item = strings.TrimSpace(item)
	idx := indexTopLevelEqualsInSetItem(item)
	if idx < 0 {
		return "", "", "", false
	}
	lhs := strings.TrimSpace(item[:idx])
	rhs := strings.TrimSpace(item[idx+1:])
	if lhs == "" || rhs == "" {
		return "", "", "", false
	}

	base, fields, ok := splitTopLevelFieldAccess(lhs)
	if !ok || len(fields) != 1 {
		return "", "", "", false
	}
	base = strings.TrimSpace(base)
	if inner, wrapped := unwrapOuterParentheses(base); wrapped {
		base = strings.TrimSpace(inner)
	}
	if !isIdentifierLike(base) || !isIdentifierLike(fields[0]) {
		return "", "", "", false
	}

	return base, fields[0], rhs, true
}

func indexTopLevelEqualsInSetItem(raw string) int {
	depthParen, depthBracket, depthBrace := 0, 0, 0
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

func loadCurrentVertexForWrite(ctx context.Context, tx graph.Tx, vertex *graph.Vertex) (*graph.Vertex, error) {
	if vertex == nil {
		return nil, nil
	}
	current, err := tx.GetVertex(ctx, vertex.Tenant, vertex.ID)
	if err == nil {
		return current, nil
	}
	if graph.IsKind(err, graph.ErrKindNotFound) {
		return vertex, nil
	}
	return nil, err
}

func loadCurrentEdgeForWrite(ctx context.Context, tx graph.Tx, edge *graph.Edge) (*graph.Edge, error) {
	if edge == nil {
		return nil, nil
	}
	current, err := tx.GetEdge(ctx, edge.Tenant, edge.ID)
	if err == nil {
		return current, nil
	}
	if graph.IsKind(err, graph.ErrKindNotFound) {
		return edge, nil
	}
	return nil, err
}

func evalSetMapValue(exprRaw string, row Row, params Params) (map[string]any, error) {
	value, err := evalExpressionWithScope(exprRaw, row, params)
	if err != nil {
		value, err = evalWriteValue(exprRaw, params, row)
	}
	if err != nil {
		return nil, err
	}

	switch typed := value.(type) {
	case nil:
		return map[string]any{}, nil
	case map[string]any:
		return typed, nil
	case *graph.Vertex:
		return decodePropertyMap(typed.Properties), nil
	case *graph.Edge:
		return decodePropertyMap(typed.Properties), nil
	default:
		return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
	}
}

func decodePropertyMap(raw map[string][]byte) map[string]any {
	decoded := make(map[string]any, len(raw))
	for key, value := range raw {
		decoded[key] = decodeStoredPropertyValue(value)
	}
	return decoded
}

func (e *Executor) applyRemoveClause(ctx context.Context, tx graph.Tx, rows []Row, clause ast.Clause) ([]Row, error) {
	raw := normalizeClauseBody(clause.Raw)
	raw = stripNormalizedPrefix(raw, "REMOVE")
	items := splitTopLevelCommaSeparated(raw)
	if len(items) == 0 {
		return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("REMOVE clause %q is not yet supported", raw), nil)
	}

	out := make([]Row, 0, len(rows))
	for _, row := range rows {
		working := row
		for _, item := range items {
			item = strings.TrimSpace(item)
			if item == "" {
				continue
			}

			if match := removeClauseRE.FindStringSubmatch(item); len(match) == 3 {
				varName, field := match[1], match[2]
				binding, ok := working[varName]
				if !ok {
					return nil, graph.NewError(graph.ErrKindInvalidInput, fmt.Sprintf("unknown binding %q", varName), nil)
				}
				switch typed := binding.(type) {
				case *graph.Vertex:
					mutated := cloneVertex(typed)
					ensureProperties(mutated)
					delete(mutated.Properties, field)
					if err := e.putVertexWithIndexes(ctx, tx, mutated); err != nil {
						return nil, err
					}
					working = cloneRow(working)
					working[varName] = mutated
				case *graph.Edge:
					mutated := cloneEdge(typed)
					ensurePropertiesEdge(mutated)
					delete(mutated.Properties, field)
					if err := e.putEdgeWithIndexes(ctx, tx, mutated); err != nil {
						return nil, err
					}
					working = cloneRow(working)
					working[varName] = mutated
				case nil:
					continue
				default:
					return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("REMOVE on %T is not supported", binding), nil)
				}
				continue
			}

			if match := removeLabelClauseRE.FindStringSubmatch(item); len(match) == 3 {
				varName := match[1]
				labelsToRemove := splitLabels(match[2])
				binding, ok := working[varName]
				if !ok {
					return nil, graph.NewError(graph.ErrKindInvalidInput, fmt.Sprintf("unknown binding %q", varName), nil)
				}
				vertex, ok := binding.(*graph.Vertex)
				if !ok {
					if binding == nil {
						continue
					}
					return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("REMOVE on %T is not supported", binding), nil)
				}
				removeSet := map[string]struct{}{}
				for _, label := range labelsToRemove {
					removeSet[label] = struct{}{}
				}
				mutated := cloneVertex(vertex)
				kept := make([]string, 0, len(mutated.Labels))
				for _, label := range mutated.Labels {
					if _, remove := removeSet[label]; remove {
						continue
					}
					kept = append(kept, label)
				}
				mutated.Labels = kept
				if err := e.putVertexWithIndexes(ctx, tx, mutated); err != nil {
					return nil, err
				}
				working = cloneRow(working)
				working[varName] = mutated
				continue
			}

			return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("REMOVE clause %q is not yet supported", raw), nil)
		}
		out = append(out, working)
	}
	return out, nil
}

func (e *Executor) applyDeleteClause(ctx context.Context, tx graph.Tx, rows []Row, clause ast.Clause, params Params) ([]Row, error) {
	raw := normalizeClauseBody(clause.Raw)
	detach := false
	switch {
	case strings.HasPrefix(strings.ToUpper(raw), "DETACHDELETE"):
		detach = true
		raw = strings.TrimSpace(raw[len("DETACHDELETE"):])
	case strings.HasPrefix(strings.ToUpper(raw), "DELETE"):
		raw = strings.TrimSpace(raw[len("DELETE"):])
	default:
		return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("DELETE clause %q is not yet supported", raw), nil)
	}
	targets := splitTopLevelCommaSeparated(raw)
	if len(targets) == 0 {
		return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("DELETE clause %q is not yet supported", raw), nil)
	}

	out := make([]Row, 0, len(rows))
	e.metrics.ObserveDeleteCounter("rows_seen", int64(len(rows)))
	for _, row := range rows {
		working := row
		for _, target := range targets {
			target = strings.TrimSpace(target)
			if target == "" {
				continue
			}

			value, err := evalExpressionWithScope(target, working, params)
			if err != nil {
				value, err = evalWriteValue(target, params, working)
			}
			if err != nil {
				return nil, err
			}

			replacement, err := e.deleteValue(ctx, tx, value, detach)
			if err != nil {
				return nil, err
			}

			if isIdentifierLike(target) {
				if _, bound := working[target]; bound {
					working = cloneRow(working)
					working[target] = replacement
				}
			}
		}
		out = append(out, working)
	}
	return out, nil
}

func stripNormalizedPrefix(raw, prefix string) string {
	raw = strings.TrimSpace(raw)
	if strings.HasPrefix(strings.ToUpper(raw), prefix) {
		return strings.TrimSpace(raw[len(prefix):])
	}
	return raw
}

func (e *Executor) deleteValue(ctx context.Context, tx graph.Tx, value any, detach bool) (any, error) {
	switch typed := value.(type) {
	case nil:
		return nil, nil
	case *graph.Vertex:
		e.metrics.ObserveDeleteCounter("vertex_targets", 1)
		if detach {
			if err := deleteVertexWithEdges(ctx, tx, typed.Tenant, typed.ID, e.metrics); err != nil && !graph.IsKind(err, graph.ErrKindNotFound) {
				return nil, err
			}
		} else {
			hasEdges, err := vertexHasAnyEdges(ctx, tx, typed.Tenant, typed.ID)
			if err != nil {
				return nil, err
			}
			if hasEdges {
				return nil, graph.NewError(graph.ErrKindConflict, "DeleteConnectedVertex", nil)
			}
			if err := tx.DeleteVertex(ctx, typed.Tenant, typed.ID); err != nil && !graph.IsKind(err, graph.ErrKindNotFound) {
				return nil, err
			}
			e.metrics.ObserveDeleteCounter("vertices_deleted", 1)
		}
		return deletedVertexBinding{Tenant: typed.Tenant, ID: typed.ID, Labels: append([]string(nil), typed.Labels...)}, nil
	case *graph.Edge:
		e.metrics.ObserveDeleteCounter("edge_targets", 1)
		if err := tx.DeleteEdge(ctx, typed.Tenant, typed.ID); err != nil && !graph.IsKind(err, graph.ErrKindNotFound) {
			return nil, err
		}
		e.metrics.ObserveDeleteCounter("edges_deleted", 1)
		return deletedEdgeBinding{Tenant: typed.Tenant, ID: typed.ID, Type: typed.Type}, nil
	case deletedVertexBinding, deletedEdgeBinding:
		return typed, nil
	case cypherPathValue:
		return e.deletePathValue(ctx, tx, typed)
	case multiHopCypherPath:
		return e.deletePathValue(ctx, tx, typed)
	case []any:
		for _, item := range typed {
			if _, err := e.deleteValue(ctx, tx, item, detach); err != nil {
				return nil, err
			}
		}
		return nil, nil
	default:
		rv := reflect.ValueOf(value)
		if rv.IsValid() && (rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array) {
			for i := 0; i < rv.Len(); i++ {
				if _, err := e.deleteValue(ctx, tx, rv.Index(i).Interface(), detach); err != nil {
					return nil, err
				}
			}
			return nil, nil
		}
		return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("DELETE on %T is not supported", value), nil)
	}
}

func (e *Executor) deletePathValue(ctx context.Context, tx graph.Tx, value any) (any, error) {
	deleteEdge := func(edge *graph.Edge) error {
		if edge == nil {
			return nil
		}
		if err := tx.DeleteEdge(ctx, edge.Tenant, edge.ID); err != nil && !graph.IsKind(err, graph.ErrKindNotFound) {
			return err
		}
		return nil
	}

	deleteVertex := func(vertex *graph.Vertex) error {
		if vertex == nil {
			return nil
		}
		// Deleting a path should remove entities reachable by that path.
		if err := deleteVertexWithEdges(ctx, tx, vertex.Tenant, vertex.ID, e.metrics); err != nil && !graph.IsKind(err, graph.ErrKindNotFound) {
			return err
		}
		return nil
	}

	switch typed := value.(type) {
	case cypherPathValue:
		if err := deleteEdge(typed.Edge); err != nil {
			return nil, err
		}
		if err := deleteVertex(typed.Left); err != nil {
			return nil, err
		}
		if err := deleteVertex(typed.Right); err != nil {
			return nil, err
		}
	case multiHopCypherPath:
		for _, edge := range typed.Edges {
			if err := deleteEdge(edge); err != nil {
				return nil, err
			}
		}
		for _, vertex := range typed.Vertexes {
			if err := deleteVertex(vertex); err != nil {
				return nil, err
			}
		}
	}
	return nil, nil
}

func (e *Executor) applyUnwindClause(rows []Row, clause ast.Clause, params Params) ([]Row, error) {
	raw := normalizeClauseBody(stripLeadingClauseKeyword(clause.Raw, "UNWIND"))
	parts := strings.SplitN(raw, "AS", 2)
	if len(parts) != 2 {
		return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("UNWIND clause %q is not yet supported", raw), nil)
	}

	exprRaw := strings.TrimSpace(parts[0])
	varName := strings.TrimSpace(parts[1])
	if varName == "" {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "UNWIND target variable is required", nil)
	}

	if len(rows) == 0 {
		rows = []Row{{}}
	}

	out := make([]Row, 0)
	for _, row := range rows {
		values, err := evalUnwindValues(exprRaw, params, row)
		if err != nil {
			return nil, err
		}
		for _, value := range values {
			merged := cloneRow(row)
			merged[varName] = value
			out = append(out, merged)
		}
	}
	return out, nil
}

func (e *Executor) applyProjectionClause(ctx context.Context, tx graph.Tx, rows []Row, clause ast.Clause, params Params, priorColumns []string) ([]Row, []string, error) {
	params = withProjectionEvalRuntime(ctx, tx, params, e)
	projection, err := projectionClauseSpecFromClause(clause)
	if err != nil {
		return nil, nil, err
	}
	items, err := parseProjectionItems(projection.ProjectionRaw)
	if err != nil {
		return nil, nil, err
	}
	if len(items) == 0 {
		return nil, nil, graph.NewError(graph.ErrKindInvalidInput, fmt.Sprintf("%s clause requires at least one projection item", clause.Kind), nil)
	}

	if err := validateProjectionOrderBy(items, projection.OrderBy, rows, projection.Distinct); err != nil {
		return nil, nil, err
	}
	projection.OrderBy = rewriteOrderByAggregateReferences(projection.OrderBy, items)

	filterProjectedRows := func(in []Row) ([]Row, error) {
		if projection.WhereRaw == "" {
			return in, nil
		}
		filtered := make([]Row, 0, len(in))
		for _, row := range in {
			ok, err := e.evalWhereExpression(ctx, tx, projection.WhereRaw, row, params)
			if err != nil {
				return nil, err
			}
			if ok {
				filtered = append(filtered, row)
			}
		}
		return filtered, nil
	}

	out := make([]Row, 0, len(rows))
	columns := make([]string, 0, len(items))
	hasAggregate := false
	hasStar := false
	for _, item := range items {
		if item.Expression == "*" {
			hasStar = true
			continue
		}
		if item.Alias != "" {
			columns = append(columns, item.Alias)
		} else {
			columns = append(columns, item.Expression)
		}
		if item.CountArg != "" || item.CollectArg != "" || item.AggFunc != "" || len(extractAggregateCalls(item.Expression)) > 0 {
			hasAggregate = true
		}
	}

	if !hasAggregate {
		for _, row := range rows {
			projected := Row{}
			if len(projection.OrderBy) > 0 && !hasStar {
				for key, value := range row {
					projected[key] = value
				}
			}
			for _, item := range items {
				if item.Expression == "*" {
					for key, value := range row {
						projected[key] = value
					}
					continue
				}
				value, ok, err := e.evalProjectionPatternComprehension(ctx, tx, item.Expression, row, params)
				if err != nil {
					return nil, nil, err
				}
				if !ok {
					value, err = evalExpressionWithScope(item.Expression, row, params)
				}
				if err != nil {
					return nil, nil, err
				}
				key := item.Expression
				if item.Alias != "" {
					key = item.Alias
				}
				projected[key] = value
			}
			if projection.WhereRaw != "" {
				scope := cloneRow(row)
				for key, value := range projected {
					scope[key] = value
				}
				ok, err := e.evalWhereExpression(ctx, tx, projection.WhereRaw, scope, params)
				if err != nil {
					return nil, nil, err
				}
				if !ok {
					continue
				}
			}
			out = append(out, projected)
		}
		if hasStar {
			columns = inferProjectionColumns(out)
			if len(columns) == 0 && len(priorColumns) > 0 {
				columns = append([]string(nil), priorColumns...)
			}
		}
		if projection.WhereRaw == "" {
			out, err = filterProjectedRows(out)
			if err != nil {
				return nil, nil, err
			}
		}
		out, err = applyProjectionPostProcessing(out, projection, params)
		if err != nil {
			return nil, nil, err
		}
		if len(projection.OrderBy) > 0 && !hasStar {
			out = trimProjectionRows(out, columns)
		}
		return out, columns, nil
	}

	type projectionAggregate struct {
		funcName string
		count    int
		sum      float64
		intSum   int64
		intOnly  bool
		min      any
		max      any
		values   []float64
		sumSq    float64
		pValue   *float64
		hasValue bool
	}

	type projectionGroup struct {
		projected          Row
		source             Row
		counts             map[int]int
		countSeen          map[int]map[string]struct{}
		collects           map[int][]any
		collectSeen        map[int]map[string]struct{}
		aggs               map[int]*projectionAggregate
		aggExprCounts      map[string]int
		aggExprCountSeen   map[string]map[string]struct{}
		aggExprCollects    map[string][]any
		aggExprCollectSeen map[string]map[string]struct{}
		aggExprAggs        map[string]*projectionAggregate
	}

	nonAggregateCount := 0
	for _, item := range items {
		if item.CountArg == "" && item.CollectArg == "" && item.AggFunc == "" && len(extractAggregateCalls(item.Expression)) == 0 {
			nonAggregateCount++
		}
	}

	groups := map[string]*projectionGroup{}
	groupOrder := make([]string, 0)
	for _, row := range rows {
		projected := Row{}
		keyValues := make([]any, 0, nonAggregateCount)
		for _, item := range items {
			if item.CountArg != "" || item.CollectArg != "" || item.AggFunc != "" || len(extractAggregateCalls(item.Expression)) > 0 {
				continue
			}
			value, ok, err := e.evalProjectionPatternComprehension(ctx, tx, item.Expression, row, params)
			if err != nil {
				return nil, nil, err
			}
			if !ok {
				value, err = evalExpressionWithScope(item.Expression, row, params)
			}
			if err != nil {
				return nil, nil, err
			}
			key := item.Expression
			if item.Alias != "" {
				key = item.Alias
			}
			projected[key] = value
			keyValues = append(keyValues, value)
		}

		groupKey, err := projectionAggregateGroupKey(keyValues)
		if err != nil {
			return nil, nil, graph.NewError(graph.ErrKindUnsupported, "aggregation key is not serializable", err)
		}
		group, ok := groups[groupKey]
		if !ok {
			group = &projectionGroup{projected: projected, source: cloneRow(row), counts: map[int]int{}, countSeen: map[int]map[string]struct{}{}, collects: map[int][]any{}, collectSeen: map[int]map[string]struct{}{}, aggs: map[int]*projectionAggregate{}, aggExprCounts: map[string]int{}, aggExprCountSeen: map[string]map[string]struct{}{}, aggExprCollects: map[string][]any{}, aggExprCollectSeen: map[string]map[string]struct{}{}, aggExprAggs: map[string]*projectionAggregate{}}
			groups[groupKey] = group
			groupOrder = append(groupOrder, groupKey)
		}
		for idx, item := range items {
			calls := extractAggregateCalls(item.Expression)
			if len(calls) > 0 && item.CountArg == "" && item.CollectArg == "" && item.AggFunc == "" {
				seenCalls := map[string]struct{}{}
				for _, call := range calls {
					normalized := normalizeAggregateExprCall(call)
					if _, seen := seenCalls[normalized]; seen {
						continue
					}
					seenCalls[normalized] = struct{}{}
					fn := aggregateFuncNameFromCall(call)
					rawArg, ok := parseAggregateCallArg(call)
					if !ok {
						continue
					}
					switch fn {
					case "count":
						arg := strings.TrimSpace(rawArg)
						if arg == "*" {
							group.aggExprCounts[normalized]++
							continue
						}
						countExpr, countDistinct := parseCountDistinctArg(arg)
						if countExpr == "" {
							countExpr = arg
						}
						value, err := evalExpressionWithScope(countExpr, row, params)
						if err != nil {
							return nil, nil, err
						}
						if value == nil {
							continue
						}
						if countDistinct {
							if group.aggExprCountSeen[normalized] == nil {
								group.aggExprCountSeen[normalized] = map[string]struct{}{}
							}
							keyBytes, err := json.Marshal(normalizeResultValue(value))
							if err != nil {
								keyBytes = []byte(fmt.Sprintf("%v", value))
							}
							key := string(keyBytes)
							if _, ok := group.aggExprCountSeen[normalized][key]; ok {
								continue
							}
							group.aggExprCountSeen[normalized][key] = struct{}{}
						}
						group.aggExprCounts[normalized]++
					case "collect":
						collectExpr, collectDistinct := parseCollectDistinctArg(rawArg)
						value, err := evalExpressionWithScope(collectExpr, row, params)
						if err != nil {
							return nil, nil, err
						}
						if value == nil {
							continue
						}
						if collectDistinct {
							if group.aggExprCollectSeen[normalized] == nil {
								group.aggExprCollectSeen[normalized] = map[string]struct{}{}
							}
							keyBytes, err := json.Marshal(normalizeResultValue(value))
							if err != nil {
								keyBytes = []byte(fmt.Sprintf("%v", value))
							}
							key := string(keyBytes)
							if _, ok := group.aggExprCollectSeen[normalized][key]; ok {
								continue
							}
							group.aggExprCollectSeen[normalized][key] = struct{}{}
						}
						group.aggExprCollects[normalized] = append(group.aggExprCollects[normalized], value)
					case "sum", "avg", "stdev", "stdevp", "min", "max", "percentiledisc", "percentilecont":
						agg := group.aggExprAggs[normalized]
						if agg == nil {
							agg = &projectionAggregate{funcName: fn, intOnly: true}
							group.aggExprAggs[normalized] = agg
						}
						switch fn {
						case "sum", "avg":
							value, err := evalExpressionWithScope(rawArg, row, params)
							if err != nil {
								return nil, nil, err
							}
							if value == nil {
								continue
							}
							n, ok := numericValue(value)
							if !ok {
								continue
							}
							agg.sum += n
							if agg.intOnly {
								integer, ok := exactIntegerAggregateValue(value)
								if ok && !isFloatLikeNumeric(value) {
									agg.intSum += integer
								} else {
									agg.intOnly = false
								}
							}
							agg.count++
							agg.hasValue = true
						case "stdev", "stdevp":
							value, err := evalExpressionWithScope(rawArg, row, params)
							if err != nil {
								return nil, nil, err
							}
							if value == nil {
								continue
							}
							n, ok := numericValue(value)
							if !ok {
								continue
							}
							agg.sum += n
							agg.sumSq += n * n
							agg.count++
							agg.hasValue = true
						case "min":
							value, err := evalExpressionWithScope(rawArg, row, params)
							if err != nil {
								return nil, nil, err
							}
							if value == nil {
								continue
							}
							if !agg.hasValue {
								agg.min = value
								agg.hasValue = true
								continue
							}
							if cmp, ok := compareCypherValues(value, agg.min); ok && cmp < 0 {
								agg.min = value
							}
						case "max":
							value, err := evalExpressionWithScope(rawArg, row, params)
							if err != nil {
								return nil, nil, err
							}
							if value == nil {
								continue
							}
							if !agg.hasValue {
								agg.max = value
								agg.hasValue = true
								continue
							}
							if cmp, ok := compareCypherValues(value, agg.max); ok && cmp > 0 {
								agg.max = value
							}
						case "percentiledisc", "percentilecont":
							valueExpr, percentileExpr, ok := parsePercentileAggregateArgs(rawArg)
							if !ok {
								return nil, nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
							}
							percentileRaw, err := evalExpressionWithScope(percentileExpr, row, params)
							if err != nil {
								return nil, nil, err
							}
							p, ok := numericValue(percentileRaw)
							if !ok || p < 0 || p > 1 {
								return nil, nil, graph.NewError(graph.ErrKindInvalidInput, "NumberOutOfRange", nil)
							}
							agg.pValue = &p

							valueRaw, err := evalExpressionWithScope(valueExpr, row, params)
							if err != nil {
								return nil, nil, err
							}
							if valueRaw == nil {
								continue
							}
							n, ok := numericValue(valueRaw)
							if !ok {
								continue
							}
							agg.values = append(agg.values, n)
							agg.hasValue = true
						}
					}
				}
			}
			if item.CountArg != "" {
				if item.CountArg == "*" {
					group.counts[idx]++
					continue
				}
				countExpr, countDistinct := parseCountDistinctArg(item.CountArg)
				if countExpr == "" {
					countExpr = item.CountArg
				}
				value, err := evalExpressionWithScope(countExpr, row, params)
				if err != nil {
					return nil, nil, err
				}
				if value != nil {
					if countDistinct {
						if group.countSeen[idx] == nil {
							group.countSeen[idx] = map[string]struct{}{}
						}
						keyBytes, err := json.Marshal(normalizeResultValue(value))
						if err != nil {
							keyBytes = []byte(fmt.Sprintf("%v", value))
						}
						key := string(keyBytes)
						if _, ok := group.countSeen[idx][key]; ok {
							continue
						}
						group.countSeen[idx][key] = struct{}{}
					}
					group.counts[idx]++
				}
				continue
			}
			if item.CollectArg != "" {
				collectExpr, collectDistinct := parseCollectDistinctArg(item.CollectArg)
				value, err := evalExpressionWithScope(collectExpr, row, params)
				if err != nil {
					return nil, nil, err
				}
				if value == nil {
					continue
				}
				if collectDistinct {
					if group.collectSeen[idx] == nil {
						group.collectSeen[idx] = map[string]struct{}{}
					}
					keyBytes, err := json.Marshal(normalizeResultValue(value))
					if err != nil {
						keyBytes = []byte(fmt.Sprintf("%v", value))
					}
					key := string(keyBytes)
					if _, ok := group.collectSeen[idx][key]; ok {
						continue
					}
					group.collectSeen[idx][key] = struct{}{}
				}
				group.collects[idx] = append(group.collects[idx], value)
				continue
			}
			if item.AggFunc != "" {
				agg := group.aggs[idx]
				if agg == nil {
					agg = &projectionAggregate{funcName: item.AggFunc, intOnly: true}
					group.aggs[idx] = agg
				}
				switch item.AggFunc {
				case "sum", "avg":
					value, err := evalExpressionWithScope(item.AggArg, row, params)
					if err != nil {
						return nil, nil, err
					}
					if value == nil {
						continue
					}
					n, ok := numericValue(value)
					if !ok {
						continue
					}
					agg.sum += n
					if agg.intOnly {
						integer, ok := exactIntegerAggregateValue(value)
						if ok && !isFloatLikeNumeric(value) {
							agg.intSum += integer
						} else {
							agg.intOnly = false
						}
					}
					agg.count++
					agg.hasValue = true
				case "stdev", "stdevp":
					value, err := evalExpressionWithScope(item.AggArg, row, params)
					if err != nil {
						return nil, nil, err
					}
					if value == nil {
						continue
					}
					n, ok := numericValue(value)
					if !ok {
						continue
					}
					agg.sum += n
					agg.sumSq += n * n
					agg.count++
					agg.hasValue = true
				case "min":
					value, err := evalExpressionWithScope(item.AggArg, row, params)
					if err != nil {
						return nil, nil, err
					}
					if value == nil {
						continue
					}
					if !agg.hasValue {
						agg.min = value
						agg.hasValue = true
						continue
					}
					if cmp, ok := compareCypherValues(value, agg.min); ok && cmp < 0 {
						agg.min = value
					}
				case "max":
					value, err := evalExpressionWithScope(item.AggArg, row, params)
					if err != nil {
						return nil, nil, err
					}
					if value == nil {
						continue
					}
					if !agg.hasValue {
						agg.max = value
						agg.hasValue = true
						continue
					}
					if cmp, ok := compareCypherValues(value, agg.max); ok && cmp > 0 {
						agg.max = value
					}
				case "percentiledisc", "percentilecont":
					valueExpr, percentileExpr, ok := parsePercentileAggregateArgs(item.AggArg)
					if !ok {
						return nil, nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
					}
					percentileRaw, err := evalExpressionWithScope(percentileExpr, row, params)
					if err != nil {
						return nil, nil, err
					}
					p, ok := numericValue(percentileRaw)
					if !ok || p < 0 || p > 1 {
						return nil, nil, graph.NewError(graph.ErrKindInvalidInput, "NumberOutOfRange", nil)
					}
					agg.pValue = &p

					valueRaw, err := evalExpressionWithScope(valueExpr, row, params)
					if err != nil {
						return nil, nil, err
					}
					if valueRaw == nil {
						continue
					}
					n, ok := numericValue(valueRaw)
					if !ok {
						continue
					}
					agg.values = append(agg.values, n)
					agg.hasValue = true
				}
			}
		}
	}

	if len(rows) == 0 && nonAggregateCount == 0 {
		projected := Row{}
		for _, item := range items {
			key := item.Expression
			if item.Alias != "" {
				key = item.Alias
			}
			calls := extractAggregateCalls(item.Expression)
			if item.CountArg != "" {
				projected[key] = 0
			} else if item.CollectArg != "" {
				projected[key] = []any{}
			} else if item.AggFunc != "" {
				projected[key] = nil
			} else if len(calls) > 0 {
				evalRow := Row{}
				rewritten := item.Expression
				seenCalls := map[string]string{}
				for idx, call := range calls {
					normalized := normalizeAggregateExprCall(call)
					alias, ok := seenCalls[normalized]
					if !ok {
						alias = fmt.Sprintf("__agg_expr_%d", idx)
						seenCalls[normalized] = alias
						switch aggregateFuncNameFromCall(call) {
						case "count":
							evalRow[alias] = 0
						case "collect":
							evalRow[alias] = []any{}
						default:
							evalRow[alias] = nil
						}
					}
					rewritten = strings.ReplaceAll(rewritten, call, alias)
				}
				value, err := evalExpressionWithScope(rewritten, evalRow, params)
				if err != nil {
					return nil, nil, err
				}
				projected[key] = value
			} else {
				projected[key] = nil
			}
		}
		out = append(out, projected)
		out, err = filterProjectedRows(out)
		if err != nil {
			return nil, nil, err
		}
		out, err = applyProjectionPostProcessing(out, projection, params)
		if err != nil {
			return nil, nil, err
		}
		return out, columns, nil
	}

	for _, groupKey := range groupOrder {
		group := groups[groupKey]
		projected := cloneRow(group.projected)
		if len(projection.OrderBy) > 0 && !hasStar && group.source != nil {
			for key, value := range group.source {
				if _, exists := projected[key]; !exists {
					projected[key] = value
				}
			}
		}
		for idx, item := range items {
			key := item.Expression
			if item.Alias != "" {
				key = item.Alias
			}
			calls := extractAggregateCalls(item.Expression)
			if item.CountArg != "" {
				projected[key] = group.counts[idx]
				continue
			}
			if item.CollectArg != "" {
				if values, ok := group.collects[idx]; ok {
					projected[key] = append([]any(nil), values...)
				} else {
					projected[key] = []any{}
				}
				continue
			}
			if item.AggFunc != "" {
				agg := group.aggs[idx]
				if agg == nil || !agg.hasValue {
					projected[key] = nil
					continue
				}
				switch item.AggFunc {
				case "sum":
					if agg.intOnly {
						projected[key] = agg.intSum
					} else {
						projected[key] = agg.sum
					}
				case "avg":
					if agg.count == 0 {
						projected[key] = nil
					} else {
						projected[key] = json.Number(formatFloatResult(agg.sum / float64(agg.count)))
					}
				case "stdev":
					projected[key] = standardDeviationResult(agg.sum, agg.sumSq, agg.count, true)
				case "stdevp":
					projected[key] = standardDeviationResult(agg.sum, agg.sumSq, agg.count, false)
				case "min":
					projected[key] = agg.min
				case "max":
					projected[key] = agg.max
				case "percentiledisc":
					if agg.pValue == nil || len(agg.values) == 0 {
						projected[key] = nil
						continue
					}
					values := append([]float64(nil), agg.values...)
					sort.Float64s(values)
					idx := int(math.Ceil(*agg.pValue*float64(len(values)))) - 1
					if idx < 0 {
						idx = 0
					}
					if idx >= len(values) {
						idx = len(values) - 1
					}
					projected[key] = json.Number(formatFloatResult(values[idx]))
				case "percentilecont":
					if agg.pValue == nil || len(agg.values) == 0 {
						projected[key] = nil
						continue
					}
					values := append([]float64(nil), agg.values...)
					sort.Float64s(values)
					if len(values) == 1 {
						projected[key] = json.Number(formatFloatResult(values[0]))
						continue
					}
					pos := *agg.pValue * float64(len(values)-1)
					low := int(math.Floor(pos))
					high := int(math.Ceil(pos))
					if low == high {
						projected[key] = json.Number(formatFloatResult(values[low]))
						continue
					}
					frac := pos - float64(low)
					interpolated := values[low] + (values[high]-values[low])*frac
					projected[key] = json.Number(formatFloatResult(interpolated))
				}
				continue
			}
			if len(calls) > 0 {
				evalRow := Row{}
				for k, v := range projected {
					evalRow[k] = v
				}
				for k, v := range group.source {
					if _, exists := evalRow[k]; !exists {
						evalRow[k] = v
					}
				}
				rewritten := item.Expression
				seenCalls := map[string]string{}
				for idx, call := range calls {
					normalized := normalizeAggregateExprCall(call)
					alias, ok := seenCalls[normalized]
					if !ok {
						alias = fmt.Sprintf("__agg_expr_%d", idx)
						seenCalls[normalized] = alias
						switch aggregateFuncNameFromCall(call) {
						case "count":
							evalRow[alias] = group.aggExprCounts[normalized]
						case "collect":
							if values, ok := group.aggExprCollects[normalized]; ok {
								evalRow[alias] = append([]any(nil), values...)
							} else {
								evalRow[alias] = []any{}
							}
						case "sum":
							agg := group.aggExprAggs[normalized]
							if agg == nil || !agg.hasValue {
								evalRow[alias] = nil
							} else if agg.intOnly {
								evalRow[alias] = agg.intSum
							} else {
								evalRow[alias] = agg.sum
							}
						case "avg":
							agg := group.aggExprAggs[normalized]
							if agg == nil || !agg.hasValue || agg.count == 0 {
								evalRow[alias] = nil
							} else {
								evalRow[alias] = json.Number(formatFloatResult(agg.sum / float64(agg.count)))
							}
						case "stdev":
							agg := group.aggExprAggs[normalized]
							if agg == nil || !agg.hasValue {
								evalRow[alias] = nil
							} else {
								evalRow[alias] = standardDeviationResult(agg.sum, agg.sumSq, agg.count, true)
							}
						case "stdevp":
							agg := group.aggExprAggs[normalized]
							if agg == nil || !agg.hasValue {
								evalRow[alias] = nil
							} else {
								evalRow[alias] = standardDeviationResult(agg.sum, agg.sumSq, agg.count, false)
							}
						case "min":
							agg := group.aggExprAggs[normalized]
							if agg == nil || !agg.hasValue {
								evalRow[alias] = nil
							} else {
								evalRow[alias] = agg.min
							}
						case "max":
							agg := group.aggExprAggs[normalized]
							if agg == nil || !agg.hasValue {
								evalRow[alias] = nil
							} else {
								evalRow[alias] = agg.max
							}
						case "percentiledisc":
							agg := group.aggExprAggs[normalized]
							if agg == nil || !agg.hasValue || agg.pValue == nil || len(agg.values) == 0 {
								evalRow[alias] = nil
							} else {
								values := append([]float64(nil), agg.values...)
								sort.Float64s(values)
								idx := int(math.Ceil(*agg.pValue*float64(len(values)))) - 1
								if idx < 0 {
									idx = 0
								}
								if idx >= len(values) {
									idx = len(values) - 1
								}
								evalRow[alias] = json.Number(formatFloatResult(values[idx]))
							}
						case "percentilecont":
							agg := group.aggExprAggs[normalized]
							if agg == nil || !agg.hasValue || agg.pValue == nil || len(agg.values) == 0 {
								evalRow[alias] = nil
							} else {
								values := append([]float64(nil), agg.values...)
								sort.Float64s(values)
								if len(values) == 1 {
									evalRow[alias] = json.Number(formatFloatResult(values[0]))
								} else {
									pos := *agg.pValue * float64(len(values)-1)
									low := int(math.Floor(pos))
									high := int(math.Ceil(pos))
									if low == high {
										evalRow[alias] = json.Number(formatFloatResult(values[low]))
									} else {
										frac := pos - float64(low)
										interpolated := values[low] + (values[high]-values[low])*frac
										evalRow[alias] = json.Number(formatFloatResult(interpolated))
									}
								}
							}
						default:
							evalRow[alias] = nil
						}
					}
					rewritten = strings.ReplaceAll(rewritten, call, alias)
				}
				value, err := evalExpressionWithScope(rewritten, evalRow, params)
				if err != nil {
					return nil, nil, err
				}
				projected[key] = value
			}
		}
		out = append(out, projected)
	}
	if hasStar {
		columns = inferProjectionColumns(out)
		if len(columns) == 0 && len(priorColumns) > 0 {
			columns = append([]string(nil), priorColumns...)
		}
	}
	out, err = filterProjectedRows(out)
	if err != nil {
		return nil, nil, err
	}
	out, err = applyProjectionPostProcessing(out, projection, params)
	if err != nil {
		return nil, nil, err
	}
	if len(projection.OrderBy) > 0 && !hasStar {
		out = trimProjectionRows(out, columns)
	}
	return out, columns, nil
}

func projectionClauseSpecFromClause(clause ast.Clause) (projectionClauseSpec, error) {
	if clause.Projection != nil {
		return projectionClauseSpecFromAST(clause.Projection, clause.Where), nil
	}
	if clause.Kind == ast.ClauseKindWith || clause.Kind == ast.ClauseKindReturn {
		return projectionClauseSpec{}, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("%s clause projection metadata is unavailable", clause.Kind), nil)
	}
	raw := strings.TrimSpace(stripLeadingClauseKeyword(clause.Raw, string(clause.Kind)))
	return parseProjectionClauseSpec(raw)
}

func projectionClauseSpecFromAST(ret *ast.ReturnClause, where *ast.Expression) projectionClauseSpec {
	out := projectionClauseSpec{Distinct: ret.Distinct, ProjectionRaw: projectionItemsRawFromAST(ret), OrderBy: projectionOrderByFromAST(ret.OrderBy)}
	if where != nil {
		out.WhereRaw = strings.TrimSpace(where.Raw)
	}
	if ret.Skip != nil {
		out.SkipRaw = strings.TrimSpace(ret.Skip.Raw)
	}
	if ret.Limit != nil {
		out.LimitRaw = strings.TrimSpace(ret.Limit.Raw)
	}
	return out
}

func projectionItemsRawFromAST(ret *ast.ReturnClause) string {
	parts := make([]string, 0, len(ret.Items)+1)
	if ret.IncludeAll {
		parts = append(parts, "*")
	}
	for _, item := range ret.Items {
		expr := strings.TrimSpace(item.Expression.Raw)
		if expr == "" {
			continue
		}
		if alias := strings.TrimSpace(item.Alias); alias != "" {
			expr += " AS " + alias
		}
		parts = append(parts, expr)
	}
	return strings.TrimSpace(strings.Join(parts, ", "))
}

func projectionOrderByFromAST(sortItems []ast.SortItem) []projectionOrderBySpec {
	if len(sortItems) == 0 {
		return nil
	}
	out := make([]projectionOrderBySpec, 0, len(sortItems))
	for _, item := range sortItems {
		expr := strings.TrimSpace(item.Expression.Raw)
		if expr == "" {
			continue
		}
		out = append(out, projectionOrderBySpec{Expression: expr, Descending: item.Direction == ast.SortDirectionDesc})
	}
	return out
}

type projectionSpec struct {
	Expression string
	Alias      string
	CountArg   string
	CollectArg string
	AggFunc    string
	AggArg     string
}

type projectionClauseSpec struct {
	Distinct      bool
	ProjectionRaw string
	WhereRaw      string
	OrderBy       []projectionOrderBySpec
	SkipRaw       string
	LimitRaw      string
}

type projectionOrderBySpec struct {
	Expression string
	Descending bool
}

func parseProjectionClauseSpec(raw string) (projectionClauseSpec, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return projectionClauseSpec{}, nil
	}

	orderByIdx := findTopLevelKeywordIndex(raw, "ORDERBY")
	skipIdx := findTopLevelKeywordIndex(raw, "SKIP")
	limitIdx := findTopLevelKeywordIndex(raw, "LIMIT")

	firstTail := minPositiveIndex(orderByIdx, skipIdx, limitIdx)
	projectionRaw := raw
	if firstTail >= 0 {
		projectionRaw = raw[:firstTail]
	}
	projectionDistinct := false
	if strings.HasPrefix(strings.ToUpper(projectionRaw), "DISTINCT") {
		projectionDistinct = true
		projectionRaw = strings.TrimSpace(projectionRaw[len("DISTINCT"):])
	}

	out := projectionClauseSpec{Distinct: projectionDistinct, ProjectionRaw: strings.TrimSpace(projectionRaw)}
	if whereIdx := findTopLevelKeywordIndex(out.ProjectionRaw, "WHERE"); whereIdx >= 0 {
		out.WhereRaw = strings.TrimSpace(out.ProjectionRaw[whereIdx+len("WHERE"):])
		out.ProjectionRaw = strings.TrimSpace(out.ProjectionRaw[:whereIdx])
	}

	if orderByIdx >= 0 {
		end := minPositiveIndex(greaterIndex(skipIdx, orderByIdx), greaterIndex(limitIdx, orderByIdx))
		if end < 0 {
			end = len(raw)
		}
		orderByWidth := len("ORDERBY")
		if strings.HasPrefix(strings.ToUpper(raw[orderByIdx:]), "ORDER BY") {
			orderByWidth = len("ORDER BY")
		}
		orderByRaw := strings.TrimSpace(raw[orderByIdx+orderByWidth : end])
		items, err := parseProjectionOrderBy(orderByRaw)
		if err != nil {
			return projectionClauseSpec{}, err
		}
		out.OrderBy = items
	}

	if skipIdx >= 0 {
		end := greaterIndex(limitIdx, skipIdx)
		if end < 0 {
			end = len(raw)
		}
		out.SkipRaw = strings.TrimSpace(raw[skipIdx+len("SKIP") : end])
	}

	if limitIdx >= 0 {
		out.LimitRaw = strings.TrimSpace(raw[limitIdx+len("LIMIT"):])
	}

	if out.ProjectionRaw == "" {
		return projectionClauseSpec{}, graph.NewError(graph.ErrKindInvalidInput, "projection clause requires at least one item", nil)
	}

	return out, nil
}

func parseProjectionOrderBy(raw string) ([]projectionOrderBySpec, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "ORDER BY requires at least one expression", nil)
	}

	parts := splitTopLevelCommaSeparated(raw)
	out := make([]projectionOrderBySpec, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		upper := strings.ToUpper(part)
		spec := projectionOrderBySpec{}
		switch {
		case strings.HasSuffix(upper, "DESCENDING"):
			spec.Descending = true
			spec.Expression = strings.TrimSpace(part[:len(part)-len("DESCENDING")])
		case strings.HasSuffix(upper, "ASCENDING"):
			spec.Expression = strings.TrimSpace(part[:len(part)-len("ASCENDING")])
		case strings.HasSuffix(upper, "DESC"):
			spec.Descending = true
			spec.Expression = strings.TrimSpace(part[:len(part)-len("DESC")])
		case strings.HasSuffix(upper, "ASC"):
			spec.Expression = strings.TrimSpace(part[:len(part)-len("ASC")])
		default:
			spec.Expression = strings.TrimSpace(part)
		}
		if spec.Expression == "" {
			return nil, graph.NewError(graph.ErrKindInvalidInput, fmt.Sprintf("ORDER BY expression %q is invalid", part), nil)
		}
		out = append(out, spec)
	}

	if len(out) == 0 {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "ORDER BY requires at least one expression", nil)
	}

	return out, nil
}

func validateProjectionOrderBy(items []projectionSpec, orderBy []projectionOrderBySpec, rows []Row, distinct bool) error {
	if len(orderBy) == 0 {
		return nil
	}

	hasProjectionAggregate := false
	projectedAggFuncs := map[string]struct{}{}
	for _, item := range items {
		if item.CountArg != "" || item.CollectArg != "" || item.AggFunc != "" {
			hasProjectionAggregate = true
			if item.CountArg != "" {
				projectedAggFuncs["count"] = struct{}{}
			}
			if item.CollectArg != "" {
				projectedAggFuncs["collect"] = struct{}{}
			}
			if item.AggFunc != "" {
				projectedAggFuncs[strings.ToLower(item.AggFunc)] = struct{}{}
			}
		}
	}

	inScope := map[string]struct{}{}
	distinctScope := map[string]struct{}{}
	distinctExpandableRoots := map[string]struct{}{}
	if !distinct && len(rows) > 0 {
		for key := range rows[0] {
			inScope[key] = struct{}{}
		}
	}
	for _, item := range items {
		rawExpr := strings.TrimSpace(item.Expression)
		distinctScope[normalizeProjectionExpr(item.Expression)] = struct{}{}
		if item.Alias != "" {
			inScope[item.Alias] = struct{}{}
			distinctScope[normalizeProjectionExpr(item.Alias)] = struct{}{}
		}
		if ident, ok := parseSimpleIdentifierRoot(item.Expression); ok {
			inScope[ident] = struct{}{}
			if rawExpr == ident {
				distinctExpandableRoots[ident] = struct{}{}
			}
		}
	}

	for _, spec := range orderBy {
		expr := strings.TrimSpace(spec.Expression)
		if expr == "" {
			continue
		}
		hasAgg := containsAggregationExpression(expr)
		if hasAgg && !hasProjectionAggregate {
			return &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "invalid aggregation expression"}
		}
		if hasAgg {
			calls := extractAggregateCalls(expr)
			for _, call := range calls {
				fn := aggregateFuncNameFromCall(call)
				if _, ok := projectedAggFuncs[fn]; !ok {
					return &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "undefined variable"}
				}
			}
			stripped := stripAggregateCalls(expr)
			for _, ident := range extractIdentifierRoots(stripped) {
				if _, ok := inScope[ident]; !ok {
					return &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "undefined variable"}
				}
			}
			continue
		}
		if distinct {
			if _, ok := distinctScope[normalizeProjectionExpr(expr)]; ok {
				continue
			}
			if ident, ok := parseSimpleIdentifierRoot(expr); ok {
				if _, in := distinctExpandableRoots[ident]; in {
					continue
				}
			}
			return &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "undefined variable"}
		}
		if ident, ok := parseSimpleIdentifierRoot(expr); ok {
			if _, ok := inScope[ident]; !ok {
				return &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "undefined variable"}
			}
		}
	}

	return nil
}

func normalizeProjectionExpr(raw string) string {
	return strings.ToUpper(normalizeClauseBody(strings.TrimSpace(raw)))
}

func containsAggregationExpression(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}
	return len(extractAggregateCalls(raw)) > 0
}

func normalizeAggregateExprCall(call string) string {
	call = strings.TrimSpace(call)
	idx := strings.Index(call, "(")
	if idx < 0 || !strings.HasSuffix(call, ")") {
		return canonicalAggregateFunctionName(strings.ToLower(call))
	}
	fn := canonicalAggregateFunctionName(strings.ToLower(strings.TrimSpace(call[:idx])))
	arg := strings.ToLower(strings.TrimSpace(call[idx+1 : len(call)-1]))
	return fn + "(" + arg + ")"
}

func projectionKey(item projectionSpec) string {
	if item.Alias != "" {
		return item.Alias
	}
	return item.Expression
}

func rewriteOrderByAggregateReferences(orderBy []projectionOrderBySpec, items []projectionSpec) []projectionOrderBySpec {
	if len(orderBy) == 0 {
		return orderBy
	}
	aggMap := map[string]string{}
	for _, item := range items {
		key := projectionKey(item)
		if item.CountArg != "" {
			aggMap[normalizeAggregateExprCall("count("+item.CountArg+")")] = key
		}
		if item.CollectArg != "" {
			aggMap[normalizeAggregateExprCall("collect("+item.CollectArg+")")] = key
		}
		if item.AggFunc != "" {
			aggMap[normalizeAggregateExprCall(item.AggFunc+"("+item.AggArg+")")] = key
		}
	}
	if len(aggMap) == 0 {
		return orderBy
	}
	out := make([]projectionOrderBySpec, 0, len(orderBy))
	for _, spec := range orderBy {
		expr := spec.Expression
		for _, call := range extractAggregateCalls(expr) {
			if repl, ok := aggMap[normalizeAggregateExprCall(call)]; ok {
				expr = strings.ReplaceAll(expr, call, repl)
			}
		}
		out = append(out, projectionOrderBySpec{Expression: expr, Descending: spec.Descending})
	}
	return out
}

func aggregateFuncNameFromCall(call string) string {
	call = strings.TrimSpace(call)
	idx := strings.Index(call, "(")
	if idx < 0 || !strings.HasSuffix(call, ")") {
		return canonicalAggregateFunctionName(strings.ToLower(call))
	}
	return canonicalAggregateFunctionName(strings.ToLower(strings.TrimSpace(call[:idx])))
}

func extractAggregateCalls(raw string) []string {
	calls := []string{}
	for i := 0; i < len(raw); {
		if !isIdentifierStart(raw[i]) {
			i++
			continue
		}
		if i > 0 && raw[i-1] == '$' {
			j := i + 1
			for j < len(raw) && isIdentifierPart(raw[j]) {
				j++
			}
			i = j
			continue
		}
		j := i + 1
		for j < len(raw) && isIdentifierPart(raw[j]) {
			j++
		}
		name := strings.ToLower(strings.TrimSpace(raw[i:j]))
		k := skipSpaces(raw, j)
		if k >= len(raw) || raw[k] != '(' || !isAggregateFunctionName(name) {
			i = j
			continue
		}
		end := findClosingParen(raw, k)
		if end < 0 {
			break
		}
		calls = append(calls, strings.TrimSpace(raw[i:end+1]))
		i = end + 1
	}
	return calls
}

func stripAggregateCalls(raw string) string {
	var out strings.Builder
	for i := 0; i < len(raw); {
		if !isIdentifierStart(raw[i]) {
			out.WriteByte(raw[i])
			i++
			continue
		}
		j := i + 1
		for j < len(raw) && isIdentifierPart(raw[j]) {
			j++
		}
		name := strings.ToLower(strings.TrimSpace(raw[i:j]))
		k := skipSpaces(raw, j)
		if k >= len(raw) || raw[k] != '(' || !isAggregateFunctionName(name) {
			out.WriteString(raw[i:j])
			i = j
			continue
		}
		end := findClosingParen(raw, k)
		if end < 0 {
			out.WriteString(raw[i:])
			break
		}
		out.WriteString("0")
		i = end + 1
	}
	return out.String()
}

func isAggregateFunctionName(name string) bool {
	switch canonicalAggregateFunctionName(strings.ToLower(strings.TrimSpace(name))) {
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

func parsePercentileAggregateArgs(raw string) (string, string, bool) {
	parts := splitTopLevelCommaSeparated(raw)
	if len(parts) != 2 {
		return "", "", false
	}
	valueExpr := strings.TrimSpace(parts[0])
	percentileExpr := strings.TrimSpace(parts[1])
	if valueExpr == "" || percentileExpr == "" {
		return "", "", false
	}
	return valueExpr, percentileExpr, true
}

func findClosingParen(raw string, openIdx int) int {
	depth := 0
	inSingle := false
	for i := openIdx; i < len(raw); i++ {
		ch := raw[i]
		if ch == '\'' && (i == 0 || raw[i-1] != '\\') {
			inSingle = !inSingle
			continue
		}
		if inSingle {
			continue
		}
		switch ch {
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

func skipSpaces(raw string, i int) int {
	for i < len(raw) {
		if raw[i] != ' ' && raw[i] != '\t' && raw[i] != '\n' && raw[i] != '\r' {
			break
		}
		i++
	}
	return i
}

func extractIdentifierRoots(raw string) []string {
	seen := map[string]struct{}{}
	out := []string{}
	for i := 0; i < len(raw); {
		if !isIdentifierStart(raw[i]) {
			i++
			continue
		}
		if i > 0 && raw[i-1] == '$' {
			j := i + 1
			for j < len(raw) && isIdentifierPart(raw[j]) {
				j++
			}
			i = j
			continue
		}
		j := i + 1
		for j < len(raw) && isIdentifierPart(raw[j]) {
			j++
		}
		name := raw[i:j]
		lower := strings.ToLower(name)
		if lower == "true" || lower == "false" || lower == "null" || isAggregateFunctionName(lower) {
			i = j
			continue
		}
		if _, ok := seen[name]; !ok {
			seen[name] = struct{}{}
			out = append(out, name)
		}
		i = j
	}
	return out
}

func parseSimpleIdentifierRoot(raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", false
	}
	if strings.ContainsAny(raw, "()+-*/%<>=,![]{}") {
		return "", false
	}
	root := raw
	if idx := strings.Index(root, "."); idx >= 0 {
		root = root[:idx]
	}
	root = strings.TrimSpace(root)
	if root == "" {
		return "", false
	}
	if !isIdentifierLike(root) {
		return "", false
	}
	return root, true
}

func isIdentifierLike(raw string) bool {
	if raw == "" {
		return false
	}
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_' || (i > 0 && ch >= '0' && ch <= '9') {
			continue
		}
		return false
	}
	return true
}

func isIdentifierStart(ch byte) bool {
	return ch == '_' || (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z')
}

func isIdentifierPart(ch byte) bool {
	return isIdentifierStart(ch) || (ch >= '0' && ch <= '9')
}

func projectionAggregateGroupKey(values []any) (string, error) {
	if len(values) == 0 {
		return "", nil
	}
	var b strings.Builder
	b.Grow(len(values) * 16)
	for _, value := range values {
		if err := appendProjectionAggregateGroupKeyValue(&b, value); err != nil {
			return "", err
		}
		b.WriteByte('\x1f')
	}
	return b.String(), nil
}

func appendProjectionAggregateGroupKeyValue(b *strings.Builder, value any) error {
	switch typed := value.(type) {
	case nil:
		b.WriteString("n")
		return nil
	case bool:
		if typed {
			b.WriteString("b1")
		} else {
			b.WriteString("b0")
		}
		return nil
	case string:
		b.WriteString("s")
		b.WriteString(typed)
		return nil
	case json.Number:
		b.WriteString("j")
		b.WriteString(typed.String())
		return nil
	case int:
		b.WriteString("i")
		b.WriteString(strconv.FormatInt(int64(typed), 10))
		return nil
	case int8:
		b.WriteString("i")
		b.WriteString(strconv.FormatInt(int64(typed), 10))
		return nil
	case int16:
		b.WriteString("i")
		b.WriteString(strconv.FormatInt(int64(typed), 10))
		return nil
	case int32:
		b.WriteString("i")
		b.WriteString(strconv.FormatInt(int64(typed), 10))
		return nil
	case int64:
		b.WriteString("i")
		b.WriteString(strconv.FormatInt(typed, 10))
		return nil
	case uint:
		b.WriteString("u")
		b.WriteString(strconv.FormatUint(uint64(typed), 10))
		return nil
	case uint8:
		b.WriteString("u")
		b.WriteString(strconv.FormatUint(uint64(typed), 10))
		return nil
	case uint16:
		b.WriteString("u")
		b.WriteString(strconv.FormatUint(uint64(typed), 10))
		return nil
	case uint32:
		b.WriteString("u")
		b.WriteString(strconv.FormatUint(uint64(typed), 10))
		return nil
	case uint64:
		b.WriteString("u")
		b.WriteString(strconv.FormatUint(typed, 10))
		return nil
	case float32:
		b.WriteString("f")
		b.WriteString(strconv.FormatFloat(float64(typed), 'g', -1, 64))
		return nil
	case float64:
		b.WriteString("f")
		b.WriteString(strconv.FormatFloat(typed, 'g', -1, 64))
		return nil
	case *graph.Vertex:
		b.WriteString("v")
		if typed != nil {
			b.WriteString(typed.Tenant)
			b.WriteByte('\x1e')
			b.WriteString(typed.ID)
		}
		return nil
	case *graph.Edge:
		b.WriteString("e")
		if typed != nil {
			b.WriteString(typed.Tenant)
			b.WriteByte('\x1e')
			b.WriteString(typed.ID)
		}
		return nil
	}

	normalized := normalizeResultValue(value)
	keyBytes, err := json.Marshal(normalized)
	if err != nil {
		return err
	}
	b.WriteString("c")
	b.Write(keyBytes)
	return nil
}

func applyProjectionPostProcessing(rows []Row, clause projectionClauseSpec, params Params) ([]Row, error) {
	if clause.Distinct {
		rows = distinctProjectionRows(rows)
	}

	skip, err := evalOptionalInt(rawExpression(clause.SkipRaw), params)
	if err != nil {
		return nil, err
	}
	limit, err := evalOptionalInt(rawExpression(clause.LimitRaw), params)
	if err != nil {
		return nil, err
	}

	hasLimit := strings.TrimSpace(clause.LimitRaw) != ""
	if hasLimit && limit == 0 {
		return []Row{}, nil
	}

	if len(clause.OrderBy) > 0 && len(rows) > 1 {
		sortLimit := 0
		if hasLimit && limit > 0 {
			sortLimit = skip + limit
		}
		sorted, err := sortProjectedRows(rows, clause.OrderBy, params, sortLimit)
		if err != nil {
			return nil, err
		}
		rows = sorted
	}

	return applySkipLimit(rows, skip, limit, hasLimit), nil
}

func distinctProjectionRows(rows []Row) []Row {
	if len(rows) <= 1 {
		return rows
	}
	seen := map[string]struct{}{}
	out := make([]Row, 0, len(rows))
	for _, row := range rows {
		canonical := map[string]any{}
		for key, value := range row {
			canonical[key] = normalizeResultValue(value)
		}
		keyBytes, err := json.Marshal(canonical)
		if err != nil {
			keyBytes = []byte(fmt.Sprintf("%v", canonical))
		}
		key := string(keyBytes)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, row)
	}
	return out
}

func trimProjectionRows(rows []Row, columns []string) []Row {
	if len(rows) == 0 || len(columns) == 0 {
		return rows
	}
	out := make([]Row, 0, len(rows))
	for _, row := range rows {
		trimmed := Row{}
		for _, col := range columns {
			trimmed[col] = row[col]
		}
		out = append(out, trimmed)
	}
	return out
}

type projectionSortRow struct {
	row        Row
	keys       []any
	inputIndex int
}

func sortProjectedRows(rows []Row, orderBy []projectionOrderBySpec, params Params, sortLimit int) ([]Row, error) {
	indexed := make([]projectionSortRow, 0, len(rows))
	for idx, row := range rows {
		keys := make([]any, 0, len(orderBy))
		for _, item := range orderBy {
			value, err := evalExpressionWithScope(item.Expression, row, params)
			if err != nil {
				return nil, err
			}
			keys = append(keys, value)
		}
		indexed = append(indexed, projectionSortRow{row: row, keys: keys, inputIndex: idx})
	}

	if sortLimit > 0 && sortLimit < len(indexed) {
		top := &projectionTopKHeap{orderBy: orderBy, rows: make([]projectionSortRow, 0, sortLimit)}
		for _, item := range indexed {
			if len(top.rows) < sortLimit {
				heap.Push(top, item)
				continue
			}
			if compareProjectionSortRows(item, top.rows[0], orderBy) < 0 {
				top.rows[0] = item
				heap.Fix(top, 0)
			}
		}
		indexed = top.rows
	}

	sort.Slice(indexed, func(i, j int) bool {
		return compareProjectionSortRows(indexed[i], indexed[j], orderBy) < 0
	})

	out := make([]Row, 0, len(indexed))
	for _, item := range indexed {
		out = append(out, item.row)
	}

	return out, nil
}

func compareProjectionSortRows(left, right projectionSortRow, orderBy []projectionOrderBySpec) int {
	for idx, item := range orderBy {
		cmp := compareSortValues(left.keys[idx], right.keys[idx])
		if cmp == 0 {
			continue
		}
		if item.Descending {
			cmp = -cmp
		}
		return cmp
	}
	if left.inputIndex < right.inputIndex {
		return -1
	}
	if left.inputIndex > right.inputIndex {
		return 1
	}
	return 0
}

type projectionTopKHeap struct {
	rows    []projectionSortRow
	orderBy []projectionOrderBySpec
}

func (h projectionTopKHeap) Len() int { return len(h.rows) }

func (h projectionTopKHeap) Less(i, j int) bool {
	return compareProjectionSortRows(h.rows[i], h.rows[j], h.orderBy) > 0
}

func (h projectionTopKHeap) Swap(i, j int) { h.rows[i], h.rows[j] = h.rows[j], h.rows[i] }

func (h *projectionTopKHeap) Push(x any) {
	h.rows = append(h.rows, x.(projectionSortRow))
}

func (h *projectionTopKHeap) Pop() any {
	last := len(h.rows) - 1
	item := h.rows[last]
	h.rows = h.rows[:last]
	return item
}

func compareSortValues(left, right any) int {
	cmp, ok := compareCypherValues(left, right)
	if ok {
		return cmp
	}
	leftText := fmt.Sprint(left)
	rightText := fmt.Sprint(right)
	switch {
	case leftText < rightText:
		return -1
	case leftText > rightText:
		return 1
	default:
		return 0
	}
}

func compareCypherValues(lhs, rhs any) (int, bool) {
	if lhs == nil && rhs == nil {
		return 0, true
	}
	if lhs == nil {
		// Cypher ORDER BY places null values after non-null values.
		return 1, true
	}
	if rhs == nil {
		return -1, true
	}

	leftRank := cypherSortRank(lhs)
	rightRank := cypherSortRank(rhs)
	if leftRank != rightRank {
		if leftRank < rightRank {
			return -1, true
		}
		return 1, true
	}

	if leftMap, leftTemporal := temporalMapValue(lhs); leftTemporal {
		if rightMap, rightTemporal := temporalMapValue(rhs); rightTemporal {
			if equal, ok := compareTemporalMaps(leftMap, rightMap, "="); ok && equal {
				return 0, true
			}
			if less, ok := compareTemporalMaps(leftMap, rightMap, "<"); ok {
				if less {
					return -1, true
				}
				return 1, true
			}
		}
	}

	if lf, lok := comparableNumericValue(lhs); lok {
		if rf, rok := comparableNumericValue(rhs); rok {
			leftNaN := math.IsNaN(lf)
			rightNaN := math.IsNaN(rf)
			if leftNaN || rightNaN {
				switch {
				case leftNaN && rightNaN:
					return 0, true
				case leftNaN:
					return 1, true
				default:
					return -1, true
				}
			}
			switch {
			case lf < rf:
				return -1, true
			case lf > rf:
				return 1, true
			default:
				return 0, true
			}
		}
	}

	if lb, lok := lhs.(bool); lok {
		if rb, rok := rhs.(bool); rok {
			switch {
			case !lb && rb:
				return -1, true
			case lb && !rb:
				return 1, true
			default:
				return 0, true
			}
		}
	}

	if ls, lok := lhs.(string); lok {
		if rs, rok := rhs.(string); rok {
			switch {
			case ls < rs:
				return -1, true
			case ls > rs:
				return 1, true
			default:
				return 0, true
			}
		}
	}

	if _, lhsString := lhs.(string); lhsString {
		if _, rhsNumeric := comparableNumericValue(rhs); rhsNumeric {
			return -1, true
		}
	}
	if _, rhsString := rhs.(string); rhsString {
		if _, lhsNumeric := comparableNumericValue(lhs); lhsNumeric {
			return 1, true
		}
	}

	if ll, lok := asAnySlice(lhs); lok {
		if rl, rok := asAnySlice(rhs); rok {
			limit := len(ll)
			if len(rl) < limit {
				limit = len(rl)
			}
			for i := 0; i < limit; i++ {
				cmp, ok := compareCypherValues(ll[i], rl[i])
				if !ok {
					return 0, false
				}
				if cmp != 0 {
					return cmp, true
				}
			}
			switch {
			case len(ll) < len(rl):
				return -1, true
			case len(ll) > len(rl):
				return 1, true
			default:
				return 0, true
			}
		}
	}

	return 0, false
}

func cypherSortRank(value any) int {
	if value == nil {
		return 90
	}
	if f, ok := comparableNumericValue(value); ok {
		if math.IsNaN(f) {
			return 80
		}
		return 70
	}
	switch typed := value.(type) {
	case map[string]any:
		if isRelationshipMapShape(typed) {
			return 20
		}
		if isVertexMapShape(typed) {
			return 10
		}
		return 0
	case *graph.Vertex:
		return 10
	case *graph.Edge:
		return 20
	case []any, []string:
		return 30
	case cypherPathValue:
		return 40
	case string:
		return 50
	case bool:
		return 60
	default:
		if _, ok := asAnySlice(value); ok {
			return 30
		}
		if rv := reflect.ValueOf(value); rv.IsValid() {
			if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
				return 30
			}
		}
		return 85
	}
}

func isVertexMapShape(value map[string]any) bool {
	if value == nil {
		return false
	}
	_, hasLabels := value["labels"]
	_, hasProps := value["properties"]
	_, hasType := value["type"]
	return hasLabels && hasProps && !hasType
}

func isRelationshipMapShape(value map[string]any) bool {
	if value == nil {
		return false
	}
	_, hasType := value["type"]
	_, hasProps := value["properties"]
	_, hasSrc := value["src"]
	_, hasDst := value["dst"]
	return hasType && hasProps && hasSrc && hasDst
}

func asAnySlice(value any) ([]any, bool) {
	switch typed := value.(type) {
	case []any:
		return typed, true
	case []string:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, item)
		}
		return out, true
	default:
		return nil, false
	}
}

func rawExpression(raw string) *ast.Expression {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	return &ast.Expression{Raw: raw}
}

func greaterIndex(value int, floor int) int {
	if value > floor {
		return value
	}
	return -1
}

func minPositiveIndex(values ...int) int {
	best := -1
	for _, value := range values {
		if value < 0 {
			continue
		}
		if best == -1 || value < best {
			best = value
		}
	}
	return best
}

func findTopLevelKeywordIndex(raw, keyword string) int {
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
		if depth == 0 {
			if strings.HasPrefix(upper[i:], keyword) {
				return i
			}
			if keyword == "ORDERBY" && strings.HasPrefix(upper[i:], "ORDER BY") {
				return i
			}
		}
	}

	return -1
}

func parseProjectionItems(raw string) ([]projectionSpec, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	parts := splitTopLevelCommaSeparated(raw)
	items := make([]projectionSpec, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if part == "*" {
			items = append(items, projectionSpec{Expression: "*"})
			continue
		}
		alias := ""
		if idx := findTopLevelAliasIndex(part); idx > 0 {
			expr := strings.TrimSpace(part[:idx])
			alias = normalizeProjectionIdentifier(strings.TrimSpace(part[idx+2:]))
			if expr == "" || alias == "" {
				return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("projection item %q is not supported", part), nil)
			}
			countArg, _ := parseCountExpression(expr)
			collectArg, _ := parseCollectExpression(expr)
			aggFunc, aggArg, _ := parseNamedAggregateExpression(expr)
			if err := validateAggregateArgumentConstant(countArg); err != nil {
				return nil, err
			}
			if err := validateAggregateArgumentConstant(collectArg); err != nil {
				return nil, err
			}
			if err := validateAggregateArgumentConstant(aggArg); err != nil {
				return nil, err
			}
			items = append(items, projectionSpec{Expression: expr, Alias: alias, CountArg: countArg, CollectArg: collectArg, AggFunc: aggFunc, AggArg: aggArg})
			continue
		}
		countArg, _ := parseCountExpression(part)
		collectArg, _ := parseCollectExpression(part)
		aggFunc, aggArg, _ := parseNamedAggregateExpression(part)
		if err := validateAggregateArgumentConstant(countArg); err != nil {
			return nil, err
		}
		if err := validateAggregateArgumentConstant(collectArg); err != nil {
			return nil, err
		}
		if err := validateAggregateArgumentConstant(aggArg); err != nil {
			return nil, err
		}
		items = append(items, projectionSpec{Expression: part, CountArg: countArg, CollectArg: collectArg, AggFunc: aggFunc, AggArg: aggArg})
	}
	return items, nil
}

func validateAggregateArgumentConstant(raw string) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	if strings.Contains(strings.ToLower(raw), "rand(") {
		return &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "NonConstantExpression"}
	}
	return nil
}

func findTopLevelAliasIndex(raw string) int {
	upper := strings.ToUpper(raw)
	depthParen, depthBracket, depthBrace := 0, 0, 0
	inSingle := false
	inDouble := false
	candidates := make([]int, 0, 2)

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
		if upper[i:i+2] != "AS" {
			continue
		}
		if i == 0 || !strings.ContainsAny(string(raw[i-1]), " \t\n\r") {
			continue
		}
		after := i + 2
		if after >= len(raw) || !strings.ContainsAny(string(raw[after]), " \t\n\r") {
			continue
		}
		candidates = append(candidates, i)
	}

	for i := len(candidates) - 1; i >= 0; i-- {
		idx := candidates[i]
		lhs := strings.TrimSpace(raw[:idx])
		rhs := strings.TrimSpace(raw[idx+2:])
		if lhs == "" || rhs == "" {
			continue
		}
		if strings.HasPrefix(rhs, "`") && strings.HasSuffix(rhs, "`") && len(rhs) >= 2 {
			return idx
		}
		if isIdentifierLike(rhs) {
			return idx
		}
	}

	return -1
}

func inferProjectionColumns(rows []Row) []string {
	keySet := map[string]struct{}{}
	for _, row := range rows {
		for key := range row {
			keySet[key] = struct{}{}
		}
	}
	columns := make([]string, 0, len(keySet))
	for key := range keySet {
		columns = append(columns, key)
	}
	sort.Strings(columns)
	return columns
}

func parseFunctionCall(raw string, name string) (string, bool) {
	raw = strings.TrimSpace(raw)
	name = strings.TrimSpace(name)
	if raw == "" || name == "" {
		return "", false
	}
	prefix := name + "("
	if len(raw) <= len(prefix) || !strings.HasSuffix(raw, ")") {
		return "", false
	}
	if !strings.EqualFold(raw[:len(prefix)], prefix) {
		return "", false
	}
	arg := strings.TrimSpace(raw[len(prefix) : len(raw)-1])
	return arg, true
}

func parseAggregateCallArg(call string) (string, bool) {
	call = strings.TrimSpace(call)
	if call == "" {
		return "", false
	}
	openIdx := strings.Index(call, "(")
	if openIdx <= 0 {
		return "", false
	}
	closeIdx := findClosingParen(call, openIdx)
	if closeIdx < 0 || closeIdx != len(call)-1 {
		return "", false
	}
	return strings.TrimSpace(call[openIdx+1 : closeIdx]), true
}

func normalizeProjectionIdentifier(raw string) string {
	raw = strings.TrimSpace(raw)
	if strings.HasPrefix(raw, "`") && strings.HasSuffix(raw, "`") && len(raw) >= 2 {
		return raw[1 : len(raw)-1]
	}
	return raw
}

func parseCountExpression(raw string) (string, bool) {
	arg, ok := parseFunctionCall(raw, "count")
	if !ok || arg == "" {
		return "", false
	}
	return arg, true
}

func parseCountDistinctArg(raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	upper := strings.ToUpper(raw)
	if strings.HasPrefix(upper, "DISTINCT") {
		return strings.TrimSpace(raw[len("DISTINCT"):]), true
	}
	return raw, false
}

func parseCollectExpression(raw string) (string, bool) {
	arg, ok := parseFunctionCall(raw, "collect")
	if !ok {
		arg, ok = parseFunctionCall(raw, "collect_list")
	}
	if !ok || arg == "" {
		return "", false
	}
	return arg, true
}

func parseCollectDistinctArg(raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	upper := strings.ToUpper(raw)
	if strings.HasPrefix(upper, "DISTINCT") {
		return strings.TrimSpace(raw[len("DISTINCT"):]), true
	}
	return raw, false
}

func parseNamedAggregateExpression(raw string) (string, string, bool) {
	aggFuncs := []string{"sum", "min", "max", "avg", "percentileDisc", "percentileCont", "percentile_disc", "percentile_cont", "stDev", "stDevP", "stdev_samp", "stdev_pop"}
	for _, fn := range aggFuncs {
		arg, ok := parseFunctionCall(raw, fn)
		if !ok || strings.TrimSpace(arg) == "" {
			continue
		}
		return canonicalAggregateFunctionName(fn), strings.TrimSpace(arg), true
	}
	return "", "", false
}

func splitTopLevelCommaSeparated(raw string) []string {
	parts := []string{}
	depthParen := 0
	depthBracket := 0
	depthBrace := 0
	inSingle := false
	inDouble := false
	start := 0
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		if ch == '\'' && !inDouble && (i == 0 || raw[i-1] != '\\') {
			if inSingle && i+1 < len(raw) && raw[i+1] == '\'' {
				i++
				continue
			}
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
		case ',':
			if depthParen == 0 && depthBracket == 0 && depthBrace == 0 {
				parts = append(parts, raw[start:i])
				start = i + 1
			}
		}
	}
	parts = append(parts, raw[start:])
	return parts
}

func evalExpressionWithScope(raw string, row Row, params Params) (any, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, graph.NewError(graph.ErrKindSemantic, "empty expression", nil)
	}
	if strings.EqualFold(raw, "true") {
		return true, nil
	}
	if strings.EqualFold(raw, "false") {
		return false, nil
	}
	if strings.EqualFold(raw, "null") {
		return nil, nil
	}
	if value, ok := resolveBareIdentifier(raw, row, params); ok {
		return value, nil
	}
	if inner, ok := unwrapOuterParentheses(raw); ok {
		return evalExpressionWithScope(inner, row, params)
	}
	if value, ok, err := evalCaseExpression(raw, row, params); ok {
		return value, err
	}
	if left, right, ok := splitTopLevelCompressedBoolean(raw, "OR"); ok {
		lhs, err := evalExpressionWithScope(left, row, params)
		if err != nil {
			return nil, err
		}
		rhs, err := evalExpressionWithScope(right, row, params)
		if err != nil {
			return nil, err
		}
		return evalBooleanBinary("OR", lhs, rhs)
	}
	if left, right, ok := splitTopLevelKeyword(raw, "OR"); ok {
		lhs, err := evalExpressionWithScope(left, row, params)
		if err != nil {
			return nil, err
		}
		rhs, err := evalExpressionWithScope(right, row, params)
		if err != nil {
			return nil, err
		}
		return evalBooleanBinary("OR", lhs, rhs)
	}
	if left, right, ok := splitTopLevelCompressedBoolean(raw, "XOR"); ok {
		lhs, err := evalExpressionWithScope(left, row, params)
		if err != nil {
			return nil, err
		}
		rhs, err := evalExpressionWithScope(right, row, params)
		if err != nil {
			return nil, err
		}
		return evalBooleanBinary("XOR", lhs, rhs)
	}
	if left, right, ok := splitTopLevelKeyword(raw, "XOR"); ok {
		lhs, err := evalExpressionWithScope(left, row, params)
		if err != nil {
			return nil, err
		}
		rhs, err := evalExpressionWithScope(right, row, params)
		if err != nil {
			return nil, err
		}
		return evalBooleanBinary("XOR", lhs, rhs)
	}
	if left, right, ok := splitTopLevelCompressedBoolean(raw, "AND"); ok {
		lhs, err := evalExpressionWithScope(left, row, params)
		if err != nil {
			return nil, err
		}
		rhs, err := evalExpressionWithScope(right, row, params)
		if err != nil {
			return nil, err
		}
		return evalBooleanBinary("AND", lhs, rhs)
	}
	if left, right, ok := splitTopLevelKeyword(raw, "AND"); ok {
		lhs, err := evalExpressionWithScope(left, row, params)
		if err != nil {
			return nil, err
		}
		rhs, err := evalExpressionWithScope(right, row, params)
		if err != nil {
			return nil, err
		}
		return evalBooleanBinary("AND", lhs, rhs)
	}
	if hasLogicalNotPrefix(raw) {
		value, err := evalExpressionWithScope(strings.TrimSpace(raw[3:]), row, params)
		if err != nil {
			return nil, err
		}
		return evalBooleanNot(value)
	}
	if left, right, ok := splitTopLevelKeyword(raw, "STARTS WITH"); ok {
		return evalStringPredicateExpression(left, right, "STARTS WITH", row, params)
	}
	if left, right, ok := splitTopLevelCompactKeyword(raw, "STARTSWITH"); ok {
		return evalStringPredicateExpression(left, right, "STARTS WITH", row, params)
	}
	if left, right, ok := splitTopLevelKeyword(raw, "ENDS WITH"); ok {
		return evalStringPredicateExpression(left, right, "ENDS WITH", row, params)
	}
	if left, right, ok := splitTopLevelCompactKeyword(raw, "ENDSWITH"); ok {
		return evalStringPredicateExpression(left, right, "ENDS WITH", row, params)
	}
	if left, right, ok := splitTopLevelKeyword(raw, "CONTAINS"); ok {
		return evalStringPredicateExpression(left, right, "CONTAINS", row, params)
	}
	if left, right, ok := splitTopLevelCompactKeyword(raw, "CONTAINS"); ok {
		return evalStringPredicateExpression(left, right, "CONTAINS", row, params)
	}
	if strings.HasPrefix(raw, "(") && strings.HasSuffix(raw, ")") && parensAreBalanced(raw[1:len(raw)-1]) {
		return evalExpressionWithScope(raw[1:len(raw)-1], row, params)
	}
	if left, labels, ok := splitTopLevelLabelPredicate(raw); ok {
		return evalLabelPredicateExpression(left, labels, row, params)
	}
	if strings.HasPrefix(raw, "-(") && strings.HasSuffix(raw, ")") && parensAreBalanced(raw[2:len(raw)-1]) {
		value, err := evalExpressionWithScope(raw[2:len(raw)-1], row, params)
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, nil
		}
		if integer, err := toInt(value); err == nil {
			return -integer, nil
		}
		if numeric, ok := numericValue(value); ok {
			return json.Number(formatFloatResult(-numeric)), nil
		}
		return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("expression %q is not yet supported", raw), nil)
	}
	if left, right, ok := splitTopLevelOperator(raw, ">="); ok {
		lhs, err := evalExpressionWithScope(left, row, params)
		if err != nil {
			return nil, err
		}
		rhs, err := evalExpressionWithScope(right, row, params)
		if err != nil {
			return nil, err
		}
		return compareExpressionValues(lhs, rhs, ">=")
	}
	if left, right, ok := splitTopLevelOperator(raw, "<="); ok {
		lhs, err := evalExpressionWithScope(left, row, params)
		if err != nil {
			return nil, err
		}
		rhs, err := evalExpressionWithScope(right, row, params)
		if err != nil {
			return nil, err
		}
		return compareExpressionValues(lhs, rhs, "<=")
	}
	if left, right, ok := splitTopLevelOperator(raw, "<>"); ok {
		lhs, err := evalExpressionWithScope(left, row, params)
		if err != nil {
			return nil, err
		}
		rhs, err := evalExpressionWithScope(right, row, params)
		if err != nil {
			return nil, err
		}
		return compareExpressionValuesWithRaw(lhs, rhs, "<>", left, right)
	}
	if left, right, ok := splitTopLevelOperator(raw, "="); ok {
		lhs, err := evalExpressionWithScope(left, row, params)
		if err != nil {
			return nil, err
		}
		rhs, err := evalExpressionWithScope(right, row, params)
		if err != nil {
			return nil, err
		}
		return compareExpressionValuesWithRaw(lhs, rhs, "=", left, right)
	}
	if left, right, ok := splitTopLevelOperator(raw, ">"); ok {
		lhs, err := evalExpressionWithScope(left, row, params)
		if err != nil {
			return nil, err
		}
		rhs, err := evalExpressionWithScope(right, row, params)
		if err != nil {
			return nil, err
		}
		return compareExpressionValues(lhs, rhs, ">")
	}
	if left, right, ok := splitTopLevelOperator(raw, "<"); ok {
		lhs, err := evalExpressionWithScope(left, row, params)
		if err != nil {
			return nil, err
		}
		rhs, err := evalExpressionWithScope(right, row, params)
		if err != nil {
			return nil, err
		}
		return compareExpressionValues(lhs, rhs, "<")
	}
	if left, right, ok := splitTopLevelInExpression(raw); ok {
		lhs, err := evalExpressionWithScope(left, row, params)
		if err != nil {
			return nil, err
		}
		rhs, err := evalExpressionWithScope(right, row, params)
		if err != nil {
			return nil, err
		}
		return evalInExpression(lhs, rhs)
	}
	if left, right, op, ok := splitTopLevelOperatorSetLast(raw, "+", "-"); ok {
		return evalAdditiveExpression(op, left, right, raw, row, params)
	}
	if left, right, op, ok := splitTopLevelOperatorSetLast(raw, "*", "/", "%"); ok {
		return evalMultiplicativeExpression(op, left, right, raw, row, params)
	}
	if left, right, ok := splitTopLevelOperatorLast(raw, "^"); ok {
		lhs, err := evalExpressionWithScope(left, row, params)
		if err != nil {
			return nil, err
		}
		rhs, err := evalExpressionWithScope(right, row, params)
		if err != nil {
			return nil, err
		}
		if lhs == nil || rhs == nil {
			return nil, nil
		}
		lf, lok := numericValue(lhs)
		rf, rok := numericValue(rhs)
		if lok && rok {
			return json.Number(formatFloatResult(math.Pow(lf, rf))), nil
		}
		return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("expression %q is not yet supported", raw), nil)
	}
	if left, isNull, ok := splitTopLevelNullPredicate(raw); ok {
		value, err := evalExpressionWithScope(left, row, params)
		if err != nil {
			return nil, err
		}
		if isNull {
			return value == nil, nil
		}
		return value != nil, nil
	}
	if arg, ok := parseFunctionCall(raw, "rand"); ok {
		if strings.TrimSpace(arg) != "" {
			return nil, graph.NewError(graph.ErrKindSemantic, "rand() expects no arguments", nil)
		}
		return rand.Float64(), nil
	}
	if arg, ok := parseFunctionCall(raw, "point"); ok {
		return evalPointFunction(arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "vector"); ok {
		return evalVectorFunction(arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "vector_dimension_count"); ok {
		return evalVectorDimensionCountFunction(arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "vector_distance"); ok {
		return evalVectorDistanceFunction(arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "vector_norm"); ok {
		return evalVectorNormFunction(arg, row, params)
	}
	if value, ok, err := evalVectorNamespaceFunction(raw, row, params); ok {
		return value, err
	}
	if value, ok, err := evalSpatialNamespaceFunction(raw, row, params); ok {
		return value, err
	}
	if arg, ok := parseFunctionCall(raw, "distance"); ok {
		return evalDistanceFunction(arg, row, params)
	}
	if value, ok, err := evalTemporalNamespaceFunction(raw, row, params); ok {
		return value, err
	}
	if value, ok, err := evalListPredicateFunction(raw, row, params); ok {
		return value, err
	}
	if value, ok, err := evalListComprehension(raw, row, params); ok {
		return value, err
	}
	if arg, ok := parseFunctionCall(raw, "size"); ok {
		if patternValue, handled, err := evalPatternComprehensionFromRuntime(arg, row, params); handled {
			if err != nil {
				return nil, err
			}
			switch typed := patternValue.(type) {
			case nil:
				return nil, nil
			case []any:
				return len(typed), nil
			case []string:
				return len(typed), nil
			default:
				return nil, graph.NewError(graph.ErrKindSemantic, "size() requires a list, map, or string", nil)
			}
		}
		value, err := evalExpressionWithScope(arg, row, params)
		if err != nil {
			return nil, err
		}
		switch typed := value.(type) {
		case nil:
			return nil, nil
		case map[string]any:
			if vector, ok := vectorValue(typed); ok {
				if dimension, err := toInt(vector["dimension"]); err == nil {
					return dimension, nil
				}
			}
			return len(typed), nil
		case []any:
			return len(typed), nil
		case []string:
			return len(typed), nil
		case string:
			return len([]rune(typed)), nil
		default:
			return nil, graph.NewError(graph.ErrKindSemantic, "size() requires a list, map, or string", nil)
		}
	}
	if arg, ok := parseFunctionCall(raw, "range"); ok {
		parts := splitTopLevelCommaSeparated(arg)
		if len(parts) < 2 || len(parts) > 3 {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "range() expects 2 or 3 arguments", nil)
		}
		startVal, err := evalExpressionWithScope(parts[0], row, params)
		if err != nil {
			return nil, err
		}
		endVal, err := evalExpressionWithScope(parts[1], row, params)
		if err != nil {
			return nil, err
		}
		start, err := toInt(startVal)
		if err != nil {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "range() start must be an integer", err)
		}
		end, err := toInt(endVal)
		if err != nil {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "range() end must be an integer", err)
		}
		step := 1
		if len(parts) == 3 {
			stepVal, err := evalExpressionWithScope(parts[2], row, params)
			if err != nil {
				return nil, err
			}
			step, err = toInt(stepVal)
			if err != nil {
				return nil, graph.NewError(graph.ErrKindInvalidInput, "range() step must be an integer", err)
			}
			if step == 0 {
				return nil, graph.NewError(graph.ErrKindInvalidInput, "range() step cannot be zero", nil)
			}
		}
		out := []any{}
		if step > 0 {
			for i := start; i <= end; i += step {
				out = append(out, i)
			}
		} else {
			for i := start; i >= end; i += step {
				out = append(out, i)
			}
		}
		return out, nil
	}
	if arg, ok := parseFunctionCall(raw, "toString"); ok {
		value, err := evalExpressionWithScope(arg, row, params)
		if err != nil {
			return nil, err
		}
		return evalToStringValue(value)
	}
	if arg, ok := parseFunctionCall(raw, "toInteger"); ok {
		value, err := evalExpressionWithScope(arg, row, params)
		if err != nil {
			return nil, err
		}
		return evalToIntegerValue(value)
	}
	if arg, ok := parseFunctionCall(raw, "toBoolean"); ok {
		value, err := evalExpressionWithScope(arg, row, params)
		if err != nil {
			return nil, err
		}
		return evalToBooleanValue(value)
	}
	if arg, ok := parseFunctionCall(raw, "toFloat"); ok {
		value, err := evalExpressionWithScope(arg, row, params)
		if err != nil {
			return nil, err
		}
		return evalToFloatValue(value)
	}
	if arg, ok := parseFunctionCall(raw, "ceil"); ok {
		value, err := evalExpressionWithScope(arg, row, params)
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, nil
		}
		numeric, ok := numericValue(value)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		return json.Number(formatFloatResult(math.Ceil(numeric))), nil
	}
	if arg, ok := parseFunctionCall(raw, "coalesce"); ok {
		parts := splitTopLevelCommaSeparated(arg)
		if len(parts) == 0 {
			return nil, graph.NewError(graph.ErrKindSemantic, "coalesce() expects at least one argument", nil)
		}
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			value, err := evalExpressionWithScope(part, row, params)
			if err != nil {
				return nil, err
			}
			if value != nil {
				return value, nil
			}
		}
		return nil, nil
	}
	if arg, ok := parseFunctionCall(raw, "reverse"); ok {
		value, err := evalExpressionWithScope(arg, row, params)
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, nil
		}
		if list, ok := normalizeListValue(value); ok {
			out := make([]any, len(list))
			for i := range list {
				out[i] = list[len(list)-1-i]
			}
			return out, nil
		}
		if str, ok := value.(string); ok {
			runes := []rune(str)
			for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
				runes[i], runes[j] = runes[j], runes[i]
			}
			return string(runes), nil
		}
		return nil, graph.NewError(graph.ErrKindSemantic, "reverse() requires a list or string", nil)
	}
	if arg, ok := parseFunctionCall(raw, "split"); ok {
		parts := splitTopLevelCommaSeparated(arg)
		if len(parts) != 2 {
			return nil, graph.NewError(graph.ErrKindSemantic, "split() expects exactly two arguments", nil)
		}
		input, err := evalExpressionWithScope(parts[0], row, params)
		if err != nil {
			return nil, err
		}
		delim, err := evalExpressionWithScope(parts[1], row, params)
		if err != nil {
			return nil, err
		}
		if input == nil || delim == nil {
			return nil, nil
		}
		inputStr, ok := input.(string)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		delimStr, ok := delim.(string)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		split := strings.Split(inputStr, delimStr)
		out := make([]any, 0, len(split))
		for _, s := range split {
			out = append(out, s)
		}
		return out, nil
	}
	if arg, ok := parseFunctionCall(raw, "substring"); ok {
		parts := splitTopLevelCommaSeparated(arg)
		if len(parts) != 2 && len(parts) != 3 {
			return nil, graph.NewError(graph.ErrKindSemantic, "substring() expects two or three arguments", nil)
		}
		input, err := evalExpressionWithScope(parts[0], row, params)
		if err != nil {
			return nil, err
		}
		startVal, err := evalExpressionWithScope(parts[1], row, params)
		if err != nil {
			return nil, err
		}
		if input == nil || startVal == nil {
			return nil, nil
		}
		inputStr, ok := input.(string)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		start, err := toInt(startVal)
		if err != nil {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", err)
		}
		runes := []rune(inputStr)
		if start < 0 {
			start = len(runes) + start
		}
		if start < 0 {
			start = 0
		}
		if start > len(runes) {
			return "", nil
		}
		end := len(runes)
		if len(parts) == 3 {
			lengthVal, err := evalExpressionWithScope(parts[2], row, params)
			if err != nil {
				return nil, err
			}
			if lengthVal == nil {
				return nil, nil
			}
			length, err := toInt(lengthVal)
			if err != nil {
				return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", err)
			}
			if length <= 0 {
				return "", nil
			}
			end = start + length
			if end > len(runes) {
				end = len(runes)
			}
		}
		if start > end {
			return "", nil
		}
		return string(runes[start:end]), nil
	}
	if arg, ok := parseFunctionCall(raw, "toLower"); ok {
		value, err := evalExpressionWithScope(arg, row, params)
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, nil
		}
		text, ok := value.(string)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		return strings.ToLower(text), nil
	}
	if arg, ok := parseFunctionCall(raw, "lower"); ok {
		value, err := evalExpressionWithScope(arg, row, params)
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, nil
		}
		text, ok := value.(string)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		return strings.ToLower(text), nil
	}
	if arg, ok := parseFunctionCall(raw, "toUpper"); ok {
		value, err := evalExpressionWithScope(arg, row, params)
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, nil
		}
		text, ok := value.(string)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		return strings.ToUpper(text), nil
	}
	if arg, ok := parseFunctionCall(raw, "upper"); ok {
		value, err := evalExpressionWithScope(arg, row, params)
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, nil
		}
		text, ok := value.(string)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		return strings.ToUpper(text), nil
	}
	if arg, ok := parseFunctionCall(raw, "left"); ok {
		parts := splitTopLevelCommaSeparated(arg)
		if len(parts) != 2 {
			return nil, graph.NewError(graph.ErrKindSemantic, "left() expects exactly two arguments", nil)
		}
		input, err := evalExpressionWithScope(parts[0], row, params)
		if err != nil {
			return nil, err
		}
		lengthValue, err := evalExpressionWithScope(parts[1], row, params)
		if err != nil {
			return nil, err
		}
		if input == nil || lengthValue == nil {
			return nil, nil
		}
		inputStr, ok := input.(string)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		length, err := toInt(lengthValue)
		if err != nil {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", err)
		}
		if length < 0 {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
		}
		runes := []rune(inputStr)
		if length > len(runes) {
			length = len(runes)
		}
		return string(runes[:length]), nil
	}
	if arg, ok := parseFunctionCall(raw, "right"); ok {
		parts := splitTopLevelCommaSeparated(arg)
		if len(parts) != 2 {
			return nil, graph.NewError(graph.ErrKindSemantic, "right() expects exactly two arguments", nil)
		}
		input, err := evalExpressionWithScope(parts[0], row, params)
		if err != nil {
			return nil, err
		}
		lengthValue, err := evalExpressionWithScope(parts[1], row, params)
		if err != nil {
			return nil, err
		}
		if input == nil || lengthValue == nil {
			return nil, nil
		}
		inputStr, ok := input.(string)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		length, err := toInt(lengthValue)
		if err != nil {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", err)
		}
		if length < 0 {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
		}
		runes := []rune(inputStr)
		if length > len(runes) {
			length = len(runes)
		}
		return string(runes[len(runes)-length:]), nil
	}
	if arg, ok := parseFunctionCall(raw, "replace"); ok {
		parts := splitTopLevelCommaSeparated(arg)
		if len(parts) != 3 {
			return nil, graph.NewError(graph.ErrKindSemantic, "replace() expects exactly three arguments", nil)
		}
		input, err := evalExpressionWithScope(parts[0], row, params)
		if err != nil {
			return nil, err
		}
		search, err := evalExpressionWithScope(parts[1], row, params)
		if err != nil {
			return nil, err
		}
		replacement, err := evalExpressionWithScope(parts[2], row, params)
		if err != nil {
			return nil, err
		}
		if input == nil || search == nil || replacement == nil {
			return nil, nil
		}
		inputStr, ok := input.(string)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		searchStr, ok := search.(string)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		replacementStr, ok := replacement.(string)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		return strings.ReplaceAll(inputStr, searchStr, replacementStr), nil
	}
	if arg, ok := parseFunctionCall(raw, "ltrim"); ok {
		parts := splitTopLevelCommaSeparated(arg)
		if len(parts) != 1 && len(parts) != 2 {
			return nil, graph.NewError(graph.ErrKindSemantic, "ltrim() expects one or two arguments", nil)
		}
		input, err := evalExpressionWithScope(parts[0], row, params)
		if err != nil {
			return nil, err
		}
		if input == nil {
			return nil, nil
		}
		inputStr, ok := input.(string)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		if len(parts) == 1 {
			return strings.TrimLeftFunc(inputStr, unicode.IsSpace), nil
		}
		trimChars, err := evalExpressionWithScope(parts[1], row, params)
		if err != nil {
			return nil, err
		}
		if trimChars == nil {
			return nil, nil
		}
		trimCharsStr, ok := trimChars.(string)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		return strings.TrimLeft(inputStr, trimCharsStr), nil
	}
	if arg, ok := parseFunctionCall(raw, "rtrim"); ok {
		parts := splitTopLevelCommaSeparated(arg)
		if len(parts) != 1 && len(parts) != 2 {
			return nil, graph.NewError(graph.ErrKindSemantic, "rtrim() expects one or two arguments", nil)
		}
		input, err := evalExpressionWithScope(parts[0], row, params)
		if err != nil {
			return nil, err
		}
		if input == nil {
			return nil, nil
		}
		inputStr, ok := input.(string)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		if len(parts) == 1 {
			return strings.TrimRightFunc(inputStr, unicode.IsSpace), nil
		}
		trimChars, err := evalExpressionWithScope(parts[1], row, params)
		if err != nil {
			return nil, err
		}
		if trimChars == nil {
			return nil, nil
		}
		trimCharsStr, ok := trimChars.(string)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		return strings.TrimRight(inputStr, trimCharsStr), nil
	}
	if arg, ok := parseFunctionCall(raw, "trim"); ok {
		parts := splitTopLevelCommaSeparated(arg)
		if len(parts) != 1 && len(parts) != 2 {
			return nil, graph.NewError(graph.ErrKindSemantic, "trim() expects one or two arguments", nil)
		}
		input, err := evalExpressionWithScope(parts[0], row, params)
		if err != nil {
			return nil, err
		}
		if input == nil {
			return nil, nil
		}
		inputStr, ok := input.(string)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		if len(parts) == 1 {
			return strings.TrimSpace(inputStr), nil
		}
		trimChars, err := evalExpressionWithScope(parts[1], row, params)
		if err != nil {
			return nil, err
		}
		if trimChars == nil {
			return nil, nil
		}
		trimCharsStr, ok := trimChars.(string)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		return strings.Trim(inputStr, trimCharsStr), nil
	}
	if arg, ok := parseFunctionCall(raw, "btrim"); ok {
		return evalTrimFunction(arg, row, params, "btrim")
	}
	if arg, ok := parseFunctionCall(raw, "char_length"); ok {
		value, err := evalExpressionWithScope(arg, row, params)
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, nil
		}
		text, ok := value.(string)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		return len([]rune(text)), nil
	}
	if arg, ok := parseFunctionCall(raw, "character_length"); ok {
		value, err := evalExpressionWithScope(arg, row, params)
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, nil
		}
		text, ok := value.(string)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		return len([]rune(text)), nil
	}
	if arg, ok := parseFunctionCall(raw, "keys"); ok {
		return evalKeysFunction(arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "head"); ok {
		return evalHeadFunction(arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "tail"); ok {
		return evalTailFunction(arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "abs"); ok {
		return evalAbsFunction(arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "sqrt"); ok {
		return evalSqrtFunction(arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "vertexes"); ok {
		return evalVertexesFunction(arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "nodes"); ok {
		return evalVertexesFunction(arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "relationships"); ok {
		return evalRelationshipsFunction(arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "length"); ok {
		return evalLengthFunction(arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "path_length"); ok {
		return evalLengthFunction(arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "last"); ok {
		return evalLastFunction(arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "sign"); ok {
		return evalSignFunction(arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "startVertex"); ok {
		return evalStartVertexFunction(arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "startNode"); ok {
		return evalStartVertexFunction(arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "endVertex"); ok {
		return evalEndVertexFunction(arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "endNode"); ok {
		return evalEndVertexFunction(arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "date.truncate"); ok {
		return evalTemporalTruncateFunction("date", arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "duration_between"); ok {
		parts := splitTopLevelCommaSeparated(arg)
		if len(parts) != 2 {
			return nil, graph.NewError(graph.ErrKindSemantic, "duration_between() expects exactly two arguments", nil)
		}
		args := make([]any, 0, 2)
		for _, part := range parts {
			value, err := evalExpressionWithScope(part, row, params)
			if err != nil {
				return nil, err
			}
			args = append(args, value)
		}
		return evalDurationMethod("between", args)
	}
	if arg, ok := parseFunctionCall(raw, "time.truncate"); ok {
		return evalTemporalTruncateFunction("time", arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "datetime.truncate"); ok {
		return evalTemporalTruncateFunction("datetime", arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "localtime.truncate"); ok {
		return evalTemporalTruncateFunction("localtime", arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "localdatetime.truncate"); ok {
		return evalTemporalTruncateFunction("localdatetime", arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "labels"); ok {
		return evalLabelsFunction(arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "type"); ok {
		return evalTypeFunction(arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "properties"); ok {
		return evalPropertiesFunction(arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "exists"); ok {
		arg = strings.TrimSpace(arg)
		if arg == "" {
			return nil, graph.NewError(graph.ErrKindSemantic, "exists() requires one argument", nil)
		}
		value, err := evalExpressionWithScope(arg, row, params)
		if err != nil {
			return nil, err
		}
		return value != nil, nil
	}
	if arg, ok := parseFunctionCall(raw, "elementId"); ok {
		return evalElementIDFunction(arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "id"); ok {
		return evalIDFunction(arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "valueType"); ok {
		return evalValueTypeFunction(arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "randomUUID"); ok {
		if strings.TrimSpace(arg) != "" {
			return nil, graph.NewError(graph.ErrKindSemantic, "randomUUID() expects no arguments", nil)
		}
		return randomUUIDv4(), nil
	}
	if arg, ok := parseFunctionCall(raw, "timestamp"); ok {
		if strings.TrimSpace(arg) != "" {
			return nil, graph.NewError(graph.ErrKindSemantic, "timestamp() expects no arguments", nil)
		}
		return time.Now().UnixMilli(), nil
	}
	if arg, ok := parseFunctionCall(raw, "reduce"); ok {
		return evalReduceFunction(arg, row, params)
	}
	if arg, ok := parseFunctionCall(raw, "isEmpty"); ok {
		value, err := evalExpressionWithScope(arg, row, params)
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, nil
		}
		switch typed := value.(type) {
		case string:
			return len([]rune(typed)) == 0, nil
		case map[string]any:
			return len(typed) == 0, nil
		default:
			if list, ok := normalizeListValue(value); ok {
				return len(list) == 0, nil
			}
		}
		return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
	}
	if arg, ok := parseFunctionCall(raw, "nullIf"); ok {
		parts := splitTopLevelCommaSeparated(arg)
		if len(parts) != 2 {
			return nil, graph.NewError(graph.ErrKindSemantic, "nullIf() expects exactly two arguments", nil)
		}
		left, err := evalExpressionWithScope(parts[0], row, params)
		if err != nil {
			return nil, err
		}
		right, err := evalExpressionWithScope(parts[1], row, params)
		if err != nil {
			return nil, err
		}
		equal, isNull := cypherNullableEqual(left, right)
		if isNull {
			return left, nil
		}
		if equal {
			return nil, nil
		}
		return left, nil
	}
	if arg, ok := parseFunctionCall(raw, "ceiling"); ok {
		value, err := evalExpressionWithScope(arg, row, params)
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, nil
		}
		numeric, ok := numericValue(value)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		return json.Number(formatFloatResult(math.Ceil(numeric))), nil
	}
	if arg, ok := parseFunctionCall(raw, "floor"); ok {
		value, err := evalExpressionWithScope(arg, row, params)
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, nil
		}
		numeric, ok := numericValue(value)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		return json.Number(formatFloatResult(math.Floor(numeric))), nil
	}
	if arg, ok := parseFunctionCall(raw, "round"); ok {
		parts := splitTopLevelCommaSeparated(arg)
		if len(parts) < 1 || len(parts) > 3 {
			return nil, graph.NewError(graph.ErrKindSemantic, "round() expects 1 to 3 arguments", nil)
		}
		value, err := evalExpressionWithScope(parts[0], row, params)
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, nil
		}
		numeric, ok := numericValue(value)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		precision := 0
		if len(parts) >= 2 {
			precisionValue, err := evalExpressionWithScope(parts[1], row, params)
			if err != nil {
				return nil, err
			}
			if precisionValue == nil {
				return nil, nil
			}
			precision, err = toInt(precisionValue)
			if err != nil {
				return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", err)
			}
		}
		mode := "HALF_UP"
		if len(parts) == 3 {
			modeValue, err := evalExpressionWithScope(parts[2], row, params)
			if err != nil {
				return nil, err
			}
			if modeValue == nil {
				return nil, nil
			}
			modeString, ok := modeValue.(string)
			if !ok {
				return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
			}
			mode = strings.ToUpper(strings.TrimSpace(modeString))
		}

		scale := math.Pow(10, float64(precision))
		shifted := numeric * scale
		var rounded float64
		switch mode {
		case "UP":
			rounded = math.Ceil(math.Abs(shifted))
			if shifted < 0 {
				rounded = -rounded
			}
		case "DOWN":
			rounded = math.Trunc(shifted)
		case "CEILING":
			rounded = math.Ceil(shifted)
		case "FLOOR":
			rounded = math.Floor(shifted)
		case "HALF_EVEN":
			rounded = math.RoundToEven(shifted)
		case "HALF_DOWN":
			frac := math.Abs(shifted) - math.Floor(math.Abs(shifted))
			if math.Abs(frac-0.5) <= 1e-12 {
				rounded = math.Trunc(shifted)
			} else {
				rounded = math.Round(shifted)
			}
		case "HALF_UP", "":
			rounded = math.Round(shifted)
		default:
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
		}
		result := rounded / scale
		return json.Number(formatFloatResult(result)), nil
	}
	if arg, ok := parseFunctionCall(raw, "exp"); ok {
		value, err := evalExpressionWithScope(arg, row, params)
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, nil
		}
		numeric, ok := numericValue(value)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		return json.Number(formatFloatResult(math.Exp(numeric))), nil
	}
	if arg, ok := parseFunctionCall(raw, "log"); ok {
		value, err := evalExpressionWithScope(arg, row, params)
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, nil
		}
		numeric, ok := numericValue(value)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		return json.Number(formatFloatResult(math.Log(numeric))), nil
	}
	if arg, ok := parseFunctionCall(raw, "ln"); ok {
		value, err := evalExpressionWithScope(arg, row, params)
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, nil
		}
		numeric, ok := numericValue(value)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		return json.Number(formatFloatResult(math.Log(numeric))), nil
	}
	if arg, ok := parseFunctionCall(raw, "log10"); ok {
		value, err := evalExpressionWithScope(arg, row, params)
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, nil
		}
		numeric, ok := numericValue(value)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		return json.Number(formatFloatResult(math.Log10(numeric))), nil
	}
	if arg, ok := parseFunctionCall(raw, "e"); ok {
		if strings.TrimSpace(arg) != "" {
			return nil, graph.NewError(graph.ErrKindSemantic, "e() expects no arguments", nil)
		}
		return json.Number(formatFloatResult(math.E)), nil
	}
	if arg, ok := parseFunctionCall(raw, "pi"); ok {
		if strings.TrimSpace(arg) != "" {
			return nil, graph.NewError(graph.ErrKindSemantic, "pi() expects no arguments", nil)
		}
		return json.Number(formatFloatResult(math.Pi)), nil
	}
	if arg, ok := parseFunctionCall(raw, "isNaN"); ok {
		value, err := evalExpressionWithScope(arg, row, params)
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, nil
		}
		numeric, ok := numericValue(value)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		return math.IsNaN(numeric), nil
	}
	if arg, ok := parseFunctionCall(raw, "sin"); ok {
		value, err := evalExpressionWithScope(arg, row, params)
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, nil
		}
		numeric, ok := numericValue(value)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		return json.Number(formatFloatResult(math.Sin(numeric))), nil
	}
	if arg, ok := parseFunctionCall(raw, "cos"); ok {
		value, err := evalExpressionWithScope(arg, row, params)
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, nil
		}
		numeric, ok := numericValue(value)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		return json.Number(formatFloatResult(math.Cos(numeric))), nil
	}
	if arg, ok := parseFunctionCall(raw, "tan"); ok {
		value, err := evalExpressionWithScope(arg, row, params)
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, nil
		}
		numeric, ok := numericValue(value)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		return json.Number(formatFloatResult(math.Tan(numeric))), nil
	}
	if arg, ok := parseFunctionCall(raw, "asin"); ok {
		value, err := evalExpressionWithScope(arg, row, params)
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, nil
		}
		numeric, ok := numericValue(value)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		return json.Number(formatFloatResult(math.Asin(numeric))), nil
	}
	if arg, ok := parseFunctionCall(raw, "acos"); ok {
		value, err := evalExpressionWithScope(arg, row, params)
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, nil
		}
		numeric, ok := numericValue(value)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		return json.Number(formatFloatResult(math.Acos(numeric))), nil
	}
	if arg, ok := parseFunctionCall(raw, "atan"); ok {
		value, err := evalExpressionWithScope(arg, row, params)
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, nil
		}
		numeric, ok := numericValue(value)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		return json.Number(formatFloatResult(math.Atan(numeric))), nil
	}
	if arg, ok := parseFunctionCall(raw, "atan2"); ok {
		parts := splitTopLevelCommaSeparated(arg)
		if len(parts) != 2 {
			return nil, graph.NewError(graph.ErrKindSemantic, "atan2() expects exactly two arguments", nil)
		}
		yRaw, err := evalExpressionWithScope(parts[0], row, params)
		if err != nil {
			return nil, err
		}
		xRaw, err := evalExpressionWithScope(parts[1], row, params)
		if err != nil {
			return nil, err
		}
		if yRaw == nil || xRaw == nil {
			return nil, nil
		}
		y, ok := numericValue(yRaw)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		x, ok := numericValue(xRaw)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		return json.Number(formatFloatResult(math.Atan2(y, x))), nil
	}
	if arg, ok := parseFunctionCall(raw, "degrees"); ok {
		value, err := evalExpressionWithScope(arg, row, params)
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, nil
		}
		numeric, ok := numericValue(value)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		return json.Number(formatFloatResult(numeric * 180 / math.Pi)), nil
	}
	if arg, ok := parseFunctionCall(raw, "radians"); ok {
		value, err := evalExpressionWithScope(arg, row, params)
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, nil
		}
		numeric, ok := numericValue(value)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		return json.Number(formatFloatResult(numeric * math.Pi / 180)), nil
	}
	if arg, ok := parseFunctionCall(raw, "cot"); ok {
		value, err := evalExpressionWithScope(arg, row, params)
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, nil
		}
		numeric, ok := numericValue(value)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		tan := math.Tan(numeric)
		return json.Number(formatFloatResult(1 / tan)), nil
	}
	if arg, ok := parseFunctionCall(raw, "haversin"); ok {
		value, err := evalExpressionWithScope(arg, row, params)
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, nil
		}
		numeric, ok := numericValue(value)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		return json.Number(formatFloatResult((1 - math.Cos(numeric)) / 2)), nil
	}
	if arg, ok := parseFunctionCall(raw, "toBooleanOrNull"); ok {
		value, err := evalExpressionWithScope(arg, row, params)
		if err != nil {
			return nil, err
		}
		converted, convErr := evalToBooleanValue(value)
		if convErr != nil {
			return nil, nil
		}
		return converted, nil
	}
	if arg, ok := parseFunctionCall(raw, "toBooleanList"); ok {
		return evalConvertedListFunction(arg, row, params, evalToBooleanValue)
	}
	if arg, ok := parseFunctionCall(raw, "toIntegerOrNull"); ok {
		value, err := evalExpressionWithScope(arg, row, params)
		if err != nil {
			return nil, err
		}
		converted, convErr := evalToIntegerValue(value)
		if convErr != nil {
			return nil, nil
		}
		return converted, nil
	}
	if arg, ok := parseFunctionCall(raw, "toIntegerList"); ok {
		return evalConvertedListFunction(arg, row, params, evalToIntegerValue)
	}
	if arg, ok := parseFunctionCall(raw, "toFloatOrNull"); ok {
		value, err := evalExpressionWithScope(arg, row, params)
		if err != nil {
			return nil, err
		}
		converted, convErr := evalToFloatValue(value)
		if convErr != nil {
			return nil, nil
		}
		return converted, nil
	}
	if arg, ok := parseFunctionCall(raw, "toFloatList"); ok {
		return evalConvertedListFunction(arg, row, params, evalToFloatValue)
	}
	if arg, ok := parseFunctionCall(raw, "toStringOrNull"); ok {
		value, err := evalExpressionWithScope(arg, row, params)
		if err != nil {
			return nil, err
		}
		converted, convErr := evalToStringValue(value)
		if convErr != nil {
			return nil, nil
		}
		return converted, nil
	}
	if arg, ok := parseFunctionCall(raw, "toStringList"); ok {
		return evalConvertedListFunction(arg, row, params, evalToStringValue)
	}
	if arg, ok := parseFunctionCall(raw, "normalize"); ok {
		return evalNormalizeFunction(arg, row, params)
	}
	if baseExpr, indexExpr, ok := splitTrailingSubscript(raw); ok {
		base, err := evalExpressionWithScope(baseExpr, row, params)
		if err != nil {
			base, err = evalWriteValue(baseExpr, params, row)
		}
		if err != nil {
			return nil, err
		}
		if startExpr, endExpr, ok := splitTopLevelSliceBounds(indexExpr); ok {
			start, hasStart, startIsNull, err := evalSliceBound(startExpr, row, params)
			if err != nil {
				return nil, err
			}
			end, hasEnd, endIsNull, err := evalSliceBound(endExpr, row, params)
			if err != nil {
				return nil, err
			}
			if startIsNull || endIsNull {
				return nil, nil
			}
			switch typed := base.(type) {
			case nil:
				return nil, nil
			case []any:
				return applySliceAny(typed, start, end, hasStart, hasEnd), nil
			case []string:
				return applySliceStringList(typed, start, end, hasStart, hasEnd), nil
			case string:
				return applySliceString(typed, start, end, hasStart, hasEnd), nil
			default:
				return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
			}
		}
		indexValue, err := evalExpressionWithScope(indexExpr, row, params)
		if err != nil {
			indexValue, err = evalWriteValue(indexExpr, params, row)
		}
		if err != nil {
			return nil, err
		}
		switch typed := base.(type) {
		case nil:
			return nil, nil
		case *graph.Vertex:
			if indexValue == nil {
				return nil, nil
			}
			key, ok := indexValue.(string)
			if !ok {
				return nil, graph.NewError(graph.ErrKindInvalidInput, "MapElementAccessByNonString", nil)
			}
			if key == "id" {
				if !shouldExposeEntityID(typed.ID) {
					return nil, nil
				}
				return typed.ID, nil
			}
			if typed.Properties == nil {
				return nil, nil
			}
			raw, ok := typed.Properties[key]
			if !ok {
				return nil, nil
			}
			return decodeStoredPropertyValue(raw), nil
		case *graph.Edge:
			if indexValue == nil {
				return nil, nil
			}
			key, ok := indexValue.(string)
			if !ok {
				return nil, graph.NewError(graph.ErrKindInvalidInput, "MapElementAccessByNonString", nil)
			}
			if key == "id" {
				if !shouldExposeEntityID(typed.ID) {
					return nil, nil
				}
				return typed.ID, nil
			}
			if typed.Properties == nil {
				return nil, nil
			}
			raw, ok := typed.Properties[key]
			if !ok {
				return nil, nil
			}
			return decodeStoredPropertyValue(raw), nil
		case []any:
			idx, err := listIndexToInt(indexValue)
			if err != nil {
				return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", err)
			}
			if idx < 0 {
				idx = len(typed) + idx
			}
			if idx < 0 || idx >= len(typed) {
				return nil, nil
			}
			return typed[idx], nil
		case []string:
			idx, err := listIndexToInt(indexValue)
			if err != nil {
				return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", err)
			}
			if idx < 0 {
				idx = len(typed) + idx
			}
			if idx < 0 || idx >= len(typed) {
				return nil, nil
			}
			return typed[idx], nil
		case map[string]any:
			if indexValue == nil {
				return nil, nil
			}
			key, ok := indexValue.(string)
			if !ok {
				return nil, graph.NewError(graph.ErrKindInvalidInput, "MapElementAccessByNonString", nil)
			}
			if value, ok := typed[key]; ok {
				return value, nil
			}
			return nil, nil
		default:
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
	}
	if value, err := evalWriteValue(raw, params, row); err == nil {
		return value, nil
	}
	if value, handled, err := evalPatternComprehensionFromRuntime(raw, row, params); handled {
		if err != nil {
			return nil, err
		}
		return value, nil
	}
	baseExpr, fields, ok := splitTopLevelFieldAccess(raw)
	if ok && len(fields) >= 1 {
		var base any
		if isIdentifierLike(baseExpr) {
			if value, exists := row[baseExpr]; exists {
				base = value
			} else if value, exists := params[baseExpr]; exists {
				base = value
			} else if value, err := evalExpressionWithScope(baseExpr, row, params); err == nil {
				base = value
			} else {
				return nil, graph.NewError(graph.ErrKindSemantic, fmt.Sprintf("unknown identifier %q", baseExpr), nil)
			}
		} else {
			value, err := evalExpressionWithScope(baseExpr, row, params)
			if err != nil {
				return nil, err
			}
			base = value
		}
		for i := 0; i < len(fields); i++ {
			if base == nil {
				return nil, nil
			}
			next, err := evalFieldAccessValue(base, fields[i])
			if err != nil {
				return nil, err
			}
			base = next
		}
		return base, nil
	}
	return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("expression %q is not yet supported", raw), nil)
}

func listIndexToInt(v any) (int, error) {
	switch n := v.(type) {
	case int:
		return n, nil
	case int64:
		return int(n), nil
	case int32:
		return int(n), nil
	case uint:
		return int(n), nil
	case uint64:
		return int(n), nil
	case uint32:
		return int(n), nil
	case json.Number:
		s := strings.TrimSpace(n.String())
		if strings.ContainsAny(s, ".eE") {
			return 0, fmt.Errorf("non-integer json.Number")
		}
		parsed, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return 0, err
		}
		return int(parsed), nil
	default:
		return 0, fmt.Errorf("unsupported list index type %T", v)
	}
}

func evalPatternComprehensionFromRuntime(raw string, row Row, params Params) (any, bool, error) {
	execRaw, ok := params[projectionEvalExecutorParam]
	if !ok || execRaw == nil {
		return nil, false, nil
	}
	exec, ok := execRaw.(*Executor)
	if !ok || exec == nil {
		return nil, false, nil
	}
	txRaw, ok := params[projectionEvalTxParam]
	if !ok || txRaw == nil {
		return nil, false, nil
	}
	tx, ok := txRaw.(graph.Tx)
	if !ok || tx == nil {
		return nil, false, nil
	}
	ctxRaw, ok := params[projectionEvalCtxParam]
	if !ok || ctxRaw == nil {
		return nil, false, nil
	}
	ctx, ok := ctxRaw.(context.Context)
	if !ok || ctx == nil {
		return nil, false, nil
	}
	return exec.evalProjectionPatternComprehension(ctx, tx, raw, row, params)
}

func evalFieldAccessValue(base any, field string) (any, error) {
	field = normalizeFieldAccessPart(field)
	switch typed := base.(type) {
	case *graph.Vertex:
		return evalVertexField(typed, field)
	case *graph.Edge:
		return evalEdgeField(typed, field)
	case deletedVertexBinding:
		return nil, graph.NewError(graph.ErrKindNotFound, "DeletedEntityAccess", nil)
	case deletedEdgeBinding:
		return nil, graph.NewError(graph.ErrKindNotFound, "DeletedEntityAccess", nil)
	case map[string]any:
		if value, ok := evalTemporalAccessor(typed, field); ok {
			return value, nil
		}
		if value, ok := typed[field]; ok {
			return value, nil
		}
		return nil, nil
	case string:
		if mapped, ok := parseStoredMapString(typed); ok {
			if value, ok := evalTemporalAccessor(mapped, field); ok {
				return value, nil
			}
			if value, ok := mapped[field]; ok {
				return value, nil
			}
			return nil, nil
		}
		return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentTypePropertyAccess", nil)
	default:
		return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentTypePropertyAccess", nil)
	}
}

func splitTopLevelFieldAccess(raw string) (string, []string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil, false
	}

	depthParen, depthBracket, depthBrace := 0, 0, 0
	inSingle := false
	inDouble := false
	firstDot := -1
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
		case '.':
			if depthParen == 0 && depthBracket == 0 && depthBrace == 0 {
				firstDot = i
				i = len(raw)
			}
		}
	}
	if firstDot <= 0 {
		return "", nil, false
	}

	baseExpr := strings.TrimSpace(raw[:firstDot])
	tail := raw[firstDot+1:]
	if baseExpr == "" || strings.TrimSpace(tail) == "" {
		return "", nil, false
	}

	readIdentifier := func(text string, idx int) (string, int, bool) {
		if idx >= len(text) || !isIdentifierStart(text[idx]) {
			return "", idx, false
		}
		start := idx
		idx++
		for idx < len(text) && isIdentifierPart(text[idx]) {
			idx++
		}
		return text[start:idx], idx, true
	}

	readDelimited := func(text string, start int) (string, int, bool) {
		if start >= len(text) || text[start] != '`' {
			return "", start, false
		}
		var b strings.Builder
		for i := start + 1; i < len(text); i++ {
			if text[i] != '`' {
				b.WriteByte(text[i])
				continue
			}
			if i+1 < len(text) && text[i+1] == '`' {
				b.WriteByte('`')
				i++
				continue
			}
			return b.String(), i + 1, true
		}
		return "", start, false
	}

	parts := make([]string, 0, 3)
	idx := 0
	for {
		idx = skipInlineSpaces(tail, idx)
		if idx >= len(tail) || !isIdentifierStart(tail[idx]) {
			if idx >= len(tail) {
				break
			}
			if tail[idx] == '`' {
				field, next, ok := readDelimited(tail, idx)
				if !ok {
					return "", nil, false
				}
				parts = append(parts, field)
				idx = skipInlineSpaces(tail, next)
				if idx >= len(tail) {
					break
				}
				if tail[idx] != '.' {
					return "", nil, false
				}
				idx++
				continue
			}
			return "", nil, false
		}

		field, next, ok := readIdentifier(tail, idx)
		if !ok {
			return "", nil, false
		}
		parts = append(parts, field)
		idx = skipInlineSpaces(tail, next)
		if idx >= len(tail) {
			break
		}
		if tail[idx] != '.' {
			return "", nil, false
		}
		idx++
	}

	if len(parts) == 0 {
		return "", nil, false
	}
	return baseExpr, parts, true
}

func skipInlineSpaces(raw string, idx int) int {
	for idx < len(raw) {
		switch raw[idx] {
		case ' ', '\t', '\n', '\r':
			idx++
		default:
			return idx
		}
	}
	return idx
}

func normalizeFieldAccessPart(part string) string {
	part = strings.TrimSpace(part)
	if len(part) >= 2 && part[0] == '`' && part[len(part)-1] == '`' {
		return strings.ReplaceAll(part[1:len(part)-1], "``", "`")
	}
	return part
}

func resolveBareIdentifier(raw string, row Row, params Params) (any, bool) {
	if !isIdentifierLike(raw) {
		return nil, false
	}
	if row != nil {
		if value, ok := row[raw]; ok {
			return value, true
		}
	}
	if params != nil {
		if value, ok := params[raw]; ok {
			return value, true
		}
	}
	return nil, false
}

func evalCaseExpression(raw string, row Row, params Params) (any, bool, error) {
	raw = strings.TrimSpace(raw)
	upper := strings.ToUpper(raw)
	if !strings.HasPrefix(upper, "CASE") || !strings.HasSuffix(upper, "END") {
		return nil, false, nil
	}
	body := strings.TrimSpace(raw[len("CASE") : len(raw)-len("END")])
	if body == "" {
		return nil, false, nil
	}
	comparisonExpr := ""
	remaining := body
	if !strings.HasPrefix(strings.ToUpper(remaining), "WHEN") {
		whenIdx := findTopLevelKeywordIndex(remaining, "WHEN")
		if whenIdx <= 0 {
			return nil, true, graph.NewError(graph.ErrKindSemantic, "CASE expression is missing WHEN", nil)
		}
		comparisonExpr = strings.TrimSpace(remaining[:whenIdx])
		remaining = strings.TrimSpace(remaining[whenIdx:])
	}

	testValue := any(nil)
	if comparisonExpr != "" {
		value, err := evalExpressionWithScope(comparisonExpr, row, params)
		if err != nil {
			return nil, true, err
		}
		testValue = value
	}

	for {
		if !strings.HasPrefix(strings.ToUpper(remaining), "WHEN") {
			break
		}
		remaining = strings.TrimSpace(remaining[len("WHEN"):])
		thenIdx := findTopLevelKeywordIndex(remaining, "THEN")
		if thenIdx < 0 {
			return nil, true, graph.NewError(graph.ErrKindSemantic, "CASE expression is missing THEN", nil)
		}
		whenExpr := strings.TrimSpace(remaining[:thenIdx])
		afterThen := strings.TrimSpace(remaining[thenIdx+len("THEN"):])
		if whenExpr == "" || afterThen == "" {
			return nil, true, graph.NewError(graph.ErrKindSemantic, "CASE expression is malformed", nil)
		}

		nextWhenIdx := findTopLevelKeywordIndex(afterThen, "WHEN")
		elseIdx := findTopLevelKeywordIndex(afterThen, "ELSE")
		resultExpr := afterThen
		remaining = ""
		if nextWhenIdx >= 0 && (elseIdx < 0 || nextWhenIdx < elseIdx) {
			resultExpr = strings.TrimSpace(afterThen[:nextWhenIdx])
			remaining = strings.TrimSpace(afterThen[nextWhenIdx:])
		} else if elseIdx >= 0 {
			resultExpr = strings.TrimSpace(afterThen[:elseIdx])
			remaining = strings.TrimSpace(afterThen[elseIdx:])
		}

		matched := false
		if comparisonExpr == "" {
			conditionValue, err := evalExpressionWithScope(whenExpr, row, params)
			if err != nil {
				return nil, true, err
			}
			condition, ok := conditionValue.(bool)
			if !ok {
				return nil, true, graph.NewError(graph.ErrKindSemantic, "CASE condition must evaluate to a boolean", nil)
			}
			matched = condition
		} else {
			whenValue, err := evalExpressionWithScope(whenExpr, row, params)
			if err != nil {
				return nil, true, err
			}
			matched = simpleCaseValuesMatch(testValue, whenValue)
		}

		if matched {
			value, err := evalExpressionWithScope(resultExpr, row, params)
			return value, true, err
		}
	}

	if strings.HasPrefix(strings.ToUpper(remaining), "ELSE") {
		elseExpr := strings.TrimSpace(remaining[len("ELSE"):])
		if elseExpr == "" {
			return nil, true, nil
		}
		value, err := evalExpressionWithScope(elseExpr, row, params)
		return value, true, err
	}
	return nil, true, nil
}

func simpleCaseValuesMatch(lhs, rhs any) bool {
	if lhs == nil || rhs == nil {
		return false
	}
	if ls, ok := lhs.(string); ok {
		rs, ok := rhs.(string)
		return ok && ls == rs
	}
	if _, ok := rhs.(string); ok {
		return false
	}
	if lb, ok := lhs.(bool); ok {
		rb, ok := rhs.(bool)
		return ok && lb == rb
	}
	if _, ok := rhs.(bool); ok {
		return false
	}
	if isStrictNumericType(lhs) && isStrictNumericType(rhs) {
		lf, _ := numericValue(lhs)
		rf, _ := numericValue(rhs)
		return lf == rf
	}
	equal, isNull := cypherNullableEqual(lhs, rhs)
	return equal && !isNull
}

func isStrictNumericType(v any) bool {
	switch v.(type) {
	case int, int64, float32, float64, json.Number:
		return true
	default:
		return false
	}
}

func splitTopLevelNullPredicate(raw string) (string, bool, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", false, false
	}
	if strings.ContainsAny(raw, " \t\n\r") {
		if left, right, ok := splitTopLevelKeyword(raw, "IS"); ok {
			rightUpper := strings.ToUpper(strings.TrimSpace(right))
			if rightUpper == "NULL" {
				return left, true, true
			}
			if rightUpper == "NOT NULL" {
				return left, false, true
			}
		}
	}
	if left, ok := splitTopLevelSuffixKeyword(raw, "ISNOTNULL"); ok {
		return left, false, true
	}
	if left, ok := splitTopLevelSuffixKeyword(raw, "ISNULL"); ok {
		return left, true, true
	}
	return "", false, false
}

func splitTopLevelInExpression(raw string) (string, string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", false
	}
	upper := strings.ToUpper(raw)
	depth := 0
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
		case '(', '[', '{':
			depth++
			continue
		case ')', ']', '}':
			if depth > 0 {
				depth--
			}
			continue
		}
		if depth != 0 || !strings.HasPrefix(upper[i:], "IN") {
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
			if (len(left) == 1 && len(right) == 1) || strings.HasPrefix(left, "$") || strings.HasPrefix(right, "$") || strings.HasPrefix(left, "[") || strings.HasPrefix(right, "[") || strings.HasPrefix(left, "'") || strings.HasPrefix(left, `"`) || (strings.HasPrefix(left, "(") && strings.HasSuffix(left, ")")) || strings.HasPrefix(right, "(") || isSimpleNumericToken(left) {
				return left, right, true
			}
		}
	}
	return "", "", false
}

func splitTopLevelCompactKeyword(raw, keyword string) (string, string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", false
	}
	upper := strings.ToUpper(raw)
	keyword = strings.ToUpper(strings.TrimSpace(keyword))
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
		if depth != 0 || !strings.HasPrefix(upper[i:], keyword) {
			continue
		}
		left := strings.TrimSpace(raw[:i])
		right := strings.TrimSpace(raw[i+len(keyword):])
		if left != "" && right != "" {
			return left, right, true
		}
	}
	return "", "", false
}

func evalStringPredicateExpression(leftExpr, rightExpr, op string, row Row, params Params) (any, error) {
	left, err := evalExpressionWithScope(leftExpr, row, params)
	if err != nil {
		return nil, err
	}
	right, err := evalExpressionWithScope(rightExpr, row, params)
	if err != nil {
		return nil, err
	}
	if left == nil || right == nil {
		return nil, nil
	}
	ls, ok := left.(string)
	if !ok {
		return nil, nil
	}
	rs, ok := right.(string)
	if !ok {
		return nil, nil
	}
	switch op {
	case "STARTS WITH":
		return strings.HasPrefix(ls, rs), nil
	case "ENDS WITH":
		return strings.HasSuffix(ls, rs), nil
	default:
		return strings.Contains(ls, rs), nil
	}
}

func splitTopLevelSliceBounds(raw string) (string, string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", false
	}
	depth := 0
	inSingle := false
	inDouble := false
	for i := 0; i < len(raw)-1; i++ {
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
		case '(', '[', '{':
			depth++
		case ')', ']', '}':
			if depth > 0 {
				depth--
			}
		}
		if depth == 0 && raw[i] == '.' && raw[i+1] == '.' {
			left := strings.TrimSpace(raw[:i])
			right := strings.TrimSpace(raw[i+2:])
			return left, right, true
		}
	}
	return "", "", false
}

func evalSliceBound(expr string, row Row, params Params) (int, bool, bool, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return 0, false, false, nil
	}
	value, err := evalExpressionWithScope(expr, row, params)
	if err != nil {
		value, err = evalWriteValue(expr, params, row)
	}
	if err != nil {
		return 0, false, false, err
	}
	if value == nil {
		return 0, false, true, nil
	}
	bound, err := toInt(value)
	if err != nil {
		return 0, false, false, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", err)
	}
	return bound, true, false, nil
}

func applySliceAny(values []any, start, end int, hasStart, hasEnd bool) []any {
	length := len(values)
	startIdx := 0
	endIdx := length
	if hasStart {
		startIdx = start
		if startIdx < 0 {
			startIdx = length + startIdx
		}
	}
	if hasEnd {
		endIdx = end
		if endIdx < 0 {
			endIdx = length + endIdx
		}
	}
	if startIdx < 0 {
		startIdx = 0
	}
	if endIdx < 0 {
		endIdx = 0
	}
	if startIdx > length {
		startIdx = length
	}
	if endIdx > length {
		endIdx = length
	}
	if endIdx < startIdx {
		return []any{}
	}
	return append([]any(nil), values[startIdx:endIdx]...)
}

func applySliceStringList(values []string, start, end int, hasStart, hasEnd bool) []any {
	anyValues := make([]any, 0, len(values))
	for _, value := range values {
		anyValues = append(anyValues, value)
	}
	return applySliceAny(anyValues, start, end, hasStart, hasEnd)
}

func applySliceString(value string, start, end int, hasStart, hasEnd bool) string {
	runes := []rune(value)
	length := len(runes)
	startIdx := 0
	endIdx := length
	if hasStart {
		startIdx = start
		if startIdx < 0 {
			startIdx = length + startIdx
		}
	}
	if hasEnd {
		endIdx = end
		if endIdx < 0 {
			endIdx = length + endIdx
		}
	}
	if startIdx < 0 {
		startIdx = 0
	}
	if endIdx < 0 {
		endIdx = 0
	}
	if startIdx > length {
		startIdx = length
	}
	if endIdx > length {
		endIdx = length
	}
	if endIdx < startIdx {
		return ""
	}
	return string(runes[startIdx:endIdx])
}

func isSimpleNumericToken(raw string) bool {
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

func evalListPredicateFunction(raw string, row Row, params Params) (any, bool, error) {
	raw = strings.TrimSpace(raw)
	for _, name := range []string{"all", "any", "none", "single"} {
		arg, ok := parseFunctionCall(raw, name)
		if !ok {
			continue
		}
		body := strings.TrimSpace(arg)
		if body == "" {
			return nil, true, graph.NewError(graph.ErrKindSemantic, name+"() requires arguments", nil)
		}
		whereIdx := findTopLevelKeywordIndex(body, "WHERE")
		if whereIdx < 0 {
			return nil, true, graph.NewError(graph.ErrKindSemantic, name+"() requires WHERE", nil)
		}
		head := strings.TrimSpace(body[:whereIdx])
		predicateExpr := strings.TrimSpace(body[whereIdx+len("WHERE"):])
		if head == "" || predicateExpr == "" {
			return nil, true, graph.NewError(graph.ErrKindSemantic, name+"() requires a list and a predicate", nil)
		}
		varName, listExpr, ok := splitTopLevelListPredicateHeader(head)
		if !ok {
			return nil, true, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("expression %q is not yet supported", raw), nil)
		}
		if !isIdentifierLike(varName) {
			return nil, true, graph.NewError(graph.ErrKindSemantic, "list predicate variable must be an identifier", nil)
		}
		listValue, err := evalExpressionWithScope(listExpr, row, params)
		if err != nil {
			return nil, true, err
		}
		values, ok := normalizeListValue(listValue)
		if !ok {
			return nil, true, graph.NewError(graph.ErrKindSemantic, "list predicate requires a list source", nil)
		}
		anyNull := false
		anyTrue := false
		anyFalse := false
		trueCount := 0
		for _, value := range values {
			scope := cloneRow(row)
			scope[varName] = value
			predValue, err := evalExpressionWithScope(predicateExpr, scope, params)
			if err != nil {
				return nil, true, err
			}
			if predValue == nil {
				anyNull = true
				continue
			}
			boolValue := truthyWhereValue(predValue)
			if boolValue {
				anyTrue = true
				trueCount++
			} else {
				anyFalse = true
			}
		}
		switch name {
		case "all":
			if anyFalse {
				return false, true, nil
			}
			if anyNull {
				return nil, true, nil
			}
			return true, true, nil
		case "any":
			if anyTrue {
				return true, true, nil
			}
			if anyNull {
				return nil, true, nil
			}
			return false, true, nil
		case "none":
			if anyTrue {
				return false, true, nil
			}
			if anyNull {
				return nil, true, nil
			}
			return true, true, nil
		case "single":
			if trueCount > 1 {
				return false, true, nil
			}
			if trueCount == 1 {
				if anyNull {
					return nil, true, nil
				}
				return true, true, nil
			}
			if anyNull {
				return nil, true, nil
			}
			return false, true, nil
		}
	}
	return nil, false, nil
}

func splitTopLevelListPredicateHeader(raw string) (string, string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", false
	}
	if left, right, ok := splitTopLevelInExpression(raw); ok {
		return left, right, true
	}
	upper := strings.ToUpper(raw)
	depth := 0
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
		case '(', '[', '{':
			depth++
			continue
		case ')', ']', '}':
			if depth > 0 {
				depth--
			}
			continue
		}
		if depth == 0 && strings.HasPrefix(upper[i:], "IN") {
			left := strings.TrimSpace(raw[:i])
			right := strings.TrimSpace(raw[i+2:])
			if left != "" && right != "" {
				return left, right, true
			}
		}
	}
	return "", "", false
}

func splitTopLevelCompressedBoolean(raw, keyword string) (string, string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" || strings.ContainsAny(raw, " \t\n\r") {
		return "", "", false
	}
	keyword = strings.ToUpper(strings.TrimSpace(keyword))
	if keyword == "" {
		return "", "", false
	}
	upper := strings.ToUpper(raw)
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
		if depth != 0 || !strings.HasPrefix(upper[i:], keyword) {
			continue
		}
		if keyword == "OR" && i > 0 && strings.EqualFold(raw[i-1:i+len(keyword)], "XOR") {
			continue
		}
		if raw[i:i+len(keyword)] != keyword {
			continue
		}
		left := strings.TrimSpace(raw[:i])
		right := strings.TrimSpace(raw[i+len(keyword):])
		if left == "" || right == "" {
			continue
		}
		return left, right, true
	}
	return "", "", false
}

func splitTopLevelSuffixKeyword(raw, suffix string) (string, bool) {
	raw = strings.TrimSpace(raw)
	suffix = strings.ToUpper(strings.TrimSpace(suffix))
	if raw == "" || suffix == "" || len(raw) <= len(suffix) {
		return "", false
	}
	upper := strings.ToUpper(raw)
	depth := 0
	inSingle := false
	inDouble := false
	for i := 0; i <= len(upper)-len(suffix); i++ {
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
		if depth == 0 && i+len(suffix) == len(upper) && strings.HasPrefix(upper[i:], suffix) {
			left := strings.TrimSpace(raw[:i])
			if left != "" {
				return left, true
			}
		}
	}
	return "", false
}

func evalInExpression(lhs, rhs any) (any, error) {
	values, ok := normalizeListValue(rhs)
	if !ok {
		if rhs == nil {
			return nil, nil
		}
		return nil, graph.NewError(graph.ErrKindSemantic, "IN requires a list on the right-hand side", nil)
	}
	if lhs == nil {
		if len(values) == 0 {
			return false, nil
		}
		return nil, nil
	}
	matchedNull := false
	for _, candidate := range values {
		equal, isNull := cypherNullableEqualForIn(lhs, candidate)
		if isNull {
			matchedNull = true
			continue
		}
		if equal {
			return true, nil
		}
	}
	if matchedNull {
		return nil, nil
	}
	return false, nil
}

func cypherNullableEqualForIn(lhs, rhs any) (equal bool, isNull bool) {
	if lhs == nil || rhs == nil {
		return false, true
	}

	if lf, lok := lhs.(float64); lok && math.IsNaN(lf) {
		_, rok := rhs.(float64)
		if rok {
			return false, false
		}
	}

	if isStrictNumericType(lhs) && isStrictNumericType(rhs) {
		lf, _ := numericValue(lhs)
		rf, _ := numericValue(rhs)
		return lf == rf, false
	}

	if (isStrictNumericType(lhs) && isStringType(rhs)) || (isStrictNumericType(rhs) && isStringType(lhs)) {
		return false, false
	}

	if lb, lok := lhs.(bool); lok {
		rb, rok := rhs.(bool)
		if !rok {
			return false, false
		}
		return lb == rb, false
	}

	if ls, lok := lhs.(string); lok {
		rs, rok := rhs.(string)
		if !rok {
			return false, false
		}
		return ls == rs, false
	}

	if ll, lok := asAnySlice(lhs); lok {
		rl, rok := asAnySlice(rhs)
		if !rok {
			return false, false
		}
		if len(ll) != len(rl) {
			return false, false
		}
		unknown := false
		for i := range ll {
			eq, isNull := cypherNullableEqualForIn(ll[i], rl[i])
			if isNull {
				unknown = true
				continue
			}
			if !eq {
				return false, false
			}
		}
		if unknown {
			return false, true
		}
		return true, false
	}

	if lm, lok := lhs.(map[string]any); lok {
		rm, rok := rhs.(map[string]any)
		if !rok {
			return false, false
		}
		if len(lm) != len(rm) {
			return false, false
		}
		unknown := false
		for key, lv := range lm {
			rv, ok := rm[key]
			if !ok {
				return false, false
			}
			eq, isNull := cypherNullableEqualForIn(lv, rv)
			if isNull {
				unknown = true
				continue
			}
			if !eq {
				return false, false
			}
		}
		if unknown {
			return false, true
		}
		return true, false
	}

	return reflect.DeepEqual(lhs, rhs), false
}

func isStringType(value any) bool {
	_, ok := value.(string)
	return ok
}

func normalizeListValue(value any) ([]any, bool) {
	if vector, ok := vectorValue(value); ok {
		if typed, ok := vector["values"].([]any); ok {
			return append([]any(nil), typed...), true
		}
	}
	switch typed := value.(type) {
	case []any:
		return typed, true
	case []string:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, item)
		}
		return out, true
	case string:
		if parsed, ok := parseStoredListString(typed); ok {
			return parsed, true
		}
	}
	return nil, false
}

func isFloatLikeNumeric(v any) bool {
	switch typed := v.(type) {
	case float64, float32:
		return true
	case json.Number:
		s := strings.TrimSpace(typed.String())
		return strings.ContainsAny(s, ".eE")
	case string:
		s := strings.TrimSpace(typed)
		return strings.ContainsAny(s, ".eE")
	default:
		return false
	}
}

func splitTrailingSubscript(raw string) (string, string, bool) {
	raw = strings.TrimSpace(raw)
	if !strings.HasSuffix(raw, "]") {
		return "", "", false
	}
	depthParen := 0
	depthBracket := 0
	depthBrace := 0
	inSingle := false
	inDouble := false
	for i := len(raw) - 1; i >= 0; i-- {
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
		case ']':
			depthBracket++
		case '[':
			depthBracket--
			if depthBracket == 0 {
				base := strings.TrimSpace(raw[:i])
				index := strings.TrimSpace(raw[i+1 : len(raw)-1])
				if base == "" || index == "" {
					return "", "", false
				}
				return base, index, true
			}
		case ')':
			depthParen++
		case '(':
			depthParen--
		case '}':
			depthBrace++
		case '{':
			depthBrace--
		}
		if depthParen < 0 || depthBracket < 0 || depthBrace < 0 {
			return "", "", false
		}
	}
	return "", "", false
}

func evalListComprehension(raw string, row Row, params Params) (any, bool, error) {
	raw = strings.TrimSpace(raw)
	if len(raw) < 2 || raw[0] != '[' || raw[len(raw)-1] != ']' {
		return nil, false, nil
	}
	body := strings.TrimSpace(raw[1 : len(raw)-1])
	pipeIdx := findTopLevelPipeIndex(body)
	projectionExpr := ""
	if pipeIdx >= 0 {
		projectionExpr = strings.TrimSpace(body[pipeIdx+1:])
		body = strings.TrimSpace(body[:pipeIdx])
	}
	upper := strings.ToUpper(body)
	inIdx := strings.Index(upper, "IN")
	if inIdx <= 0 {
		return nil, false, nil
	}
	varName := strings.TrimSpace(body[:inIdx])
	if !isIdentifierLike(varName) {
		return nil, false, nil
	}
	rest := strings.TrimSpace(body[inIdx+2:])
	if rest == "" {
		return nil, true, graph.NewError(graph.ErrKindSemantic, "list comprehension source is required", nil)
	}

	whereIdx := findTopLevelKeywordIndex(rest, "WHERE")
	listExpr := rest
	predicate := ""
	if whereIdx >= 0 {
		listExpr = strings.TrimSpace(rest[:whereIdx])
		predicate = strings.TrimSpace(rest[whereIdx+len("WHERE"):])
	}

	listValue, err := evalExpressionWithScope(listExpr, row, params)
	if err != nil {
		return nil, true, err
	}
	values, ok := listValue.([]any)
	if !ok {
		if typed, ok := listValue.([]string); ok {
			values = make([]any, 0, len(typed))
			for _, v := range typed {
				values = append(values, v)
			}
		} else {
			return nil, true, graph.NewError(graph.ErrKindSemantic, "list comprehension requires a list source", nil)
		}
	}

	out := make([]any, 0, len(values))
	for _, v := range values {
		scope := cloneRow(row)
		scope[varName] = v
		include := true
		if predicate != "" {
			predValue, err := evalExpressionWithScope(predicate, scope, params)
			if err != nil {
				return nil, true, err
			}
			if predValue == nil {
				include = false
			} else {
				include = truthyWhereValue(predValue)
			}
		}
		if include {
			if projectionExpr == "" {
				out = append(out, v)
				continue
			}
			projected, err := evalExpressionWithScope(projectionExpr, scope, params)
			if err != nil {
				return nil, true, err
			}
			out = append(out, projected)
		}
	}

	return out, true, nil
}

func evalLabelsFunction(arg string, row Row, params Params) (any, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return nil, graph.NewError(graph.ErrKindSemantic, "labels() requires one argument", nil)
	}
	base, err := evalExpressionWithScope(arg, row, params)
	if err != nil {
		base, err = evalWriteValue(arg, params, row)
	}
	if err != nil {
		return nil, err
	}
	if base == nil {
		return nil, nil
	}
	if _, _, ok := pathComponents(base); ok {
		return nil, &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "InvalidArgumentType"}
	}
	switch typed := base.(type) {
	case *graph.Vertex:
		return append([]string(nil), typed.Labels...), nil
	case deletedVertexBinding:
		return nil, graph.NewError(graph.ErrKindNotFound, "DeletedEntityAccess", nil)
	case map[string]any:
		if labels, ok := typed["labels"]; ok {
			switch l := labels.(type) {
			case []string:
				return append([]string(nil), l...), nil
			case []any:
				out := make([]string, 0, len(l))
				for _, item := range l {
					out = append(out, fmt.Sprint(item))
				}
				return out, nil
			}
		}
	}
	return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
}

func evalTypeFunction(arg string, row Row, params Params) (any, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return nil, graph.NewError(graph.ErrKindSemantic, "type() requires one argument", nil)
	}
	base, err := evalExpressionWithScope(arg, row, params)
	if err != nil {
		base, err = evalWriteValue(arg, params, row)
	}
	if err != nil {
		return nil, err
	}
	if base == nil {
		return nil, nil
	}
	switch typed := base.(type) {
	case *graph.Edge:
		return typed.Type, nil
	case *graph.Vertex:
		return nil, &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "InvalidArgumentType"}
	case deletedEdgeBinding:
		return typed.Type, nil
	case map[string]any:
		if _, hasLabels := typed["labels"]; hasLabels {
			return nil, &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "InvalidArgumentType"}
		}
		if relType, ok := typed["type"]; ok {
			return fmt.Sprint(relType), nil
		}
	}
	return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
}

func evalPropertiesFunction(arg string, row Row, params Params) (any, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return nil, graph.NewError(graph.ErrKindSemantic, "properties() requires one argument", nil)
	}
	value, err := evalExpressionWithScope(arg, row, params)
	if err != nil {
		value, err = evalWriteValue(arg, params, row)
	}
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}

	switch typed := value.(type) {
	case *graph.Vertex:
		return clonePropertyMap(typed.Properties), nil
	case *graph.Edge:
		return clonePropertyMap(typed.Properties), nil
	case deletedVertexBinding:
		return nil, graph.NewError(graph.ErrKindNotFound, "DeletedEntityAccess", nil)
	case deletedEdgeBinding:
		return nil, graph.NewError(graph.ErrKindNotFound, "DeletedEntityAccess", nil)
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[key] = item
		}
		return out, nil
	default:
		return nil, graph.NewError(graph.ErrKindSemantic, "invalid argument type", nil)
	}
}

func evalElementIDFunction(arg string, row Row, params Params) (any, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return nil, graph.NewError(graph.ErrKindSemantic, "elementId() requires one argument", nil)
	}
	value, err := evalExpressionWithScope(arg, row, params)
	if err != nil {
		value, err = evalWriteValue(arg, params, row)
	}
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	switch typed := value.(type) {
	case *graph.Vertex:
		return typed.ID, nil
	case *graph.Edge:
		return typed.ID, nil
	case deletedVertexBinding:
		return typed.ID, nil
	case deletedEdgeBinding:
		return typed.ID, nil
	case map[string]any:
		if idValue, ok := typed["id"]; ok {
			return fmt.Sprint(idValue), nil
		}
	}
	return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
}

func evalIDFunction(arg string, row Row, params Params) (any, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return nil, graph.NewError(graph.ErrKindSemantic, "id() requires one argument", nil)
	}
	value, err := evalExpressionWithScope(arg, row, params)
	if err != nil {
		value, err = evalWriteValue(arg, params, row)
	}
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}

	idText := ""
	switch typed := value.(type) {
	case *graph.Vertex:
		idText = typed.ID
	case *graph.Edge:
		idText = typed.ID
	case deletedVertexBinding:
		idText = typed.ID
	case deletedEdgeBinding:
		idText = typed.ID
	case map[string]any:
		if idValue, ok := typed["id"]; ok {
			idText = fmt.Sprint(idValue)
		}
	default:
		return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
	}

	idText = strings.TrimSpace(idText)
	if idText == "" {
		return nil, nil
	}
	if parsed, err := strconv.ParseInt(idText, 10, 64); err == nil {
		return parsed, nil
	}
	if f, ok := numericValue(idText); ok {
		return int64(truncTowardZero(f)), nil
	}
	return nil, nil
}

func evalValueTypeFunction(arg string, row Row, params Params) (any, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return nil, graph.NewError(graph.ErrKindSemantic, "valueType() requires one argument", nil)
	}
	value, err := evalExpressionWithScope(arg, row, params)
	if err != nil {
		value, err = evalWriteValue(arg, params, row)
	}
	if err != nil {
		return nil, err
	}
	if value == nil {
		return "NULL", nil
	}

	if temporal, ok := temporalMapValue(value); ok {
		typeName := strings.ToUpper(strings.TrimSpace(fmt.Sprint(temporal["__temporal_type"])))
		if typeName != "" {
			return typeName, nil
		}
	}
	if _, ok := spatialPointValue(value); ok {
		return "POINT", nil
	}
	if _, ok := vectorValue(value); ok {
		return "VECTOR", nil
	}

	switch typed := value.(type) {
	case bool:
		return "BOOLEAN", nil
	case string:
		return "STRING", nil
	case int, int64, int32, uint, uint64, uint32:
		return "INTEGER", nil
	case float64, float32:
		return "FLOAT", nil
	case json.Number:
		s := strings.TrimSpace(typed.String())
		if strings.ContainsAny(s, ".eE") {
			return "FLOAT", nil
		}
		return "INTEGER", nil
	case []any, []string:
		return "LIST", nil
	case *graph.Vertex, deletedVertexBinding:
		return "VERTEX", nil
	case *graph.Edge, deletedEdgeBinding:
		return "RELATIONSHIP", nil
	case cypherPathValue, multiHopCypherPath:
		return "PATH", nil
	case map[string]any:
		if _, hasLabels := typed["labels"]; hasLabels {
			return "VERTEX", nil
		}
		if _, hasType := typed["type"]; hasType {
			return "RELATIONSHIP", nil
		}
		return "MAP", nil
	default:
		if _, _, ok := pathComponents(value); ok {
			return "PATH", nil
		}
		if _, ok := normalizeListValue(value); ok {
			return "LIST", nil
		}
	}

	return "ANY", nil
}

func randomUUIDv4() string {
	var b [16]byte
	if _, err := crand.Read(b[:]); err != nil {
		// Fallback keeps function total (non-erroring), while still producing UUID-like text.
		seed := time.Now().UnixNano()
		for i := range b {
			b[i] = byte(seed >> ((i % 8) * 8))
		}
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		uint32(b[0])<<24|uint32(b[1])<<16|uint32(b[2])<<8|uint32(b[3]),
		uint16(b[4])<<8|uint16(b[5]),
		uint16(b[6])<<8|uint16(b[7]),
		uint16(b[8])<<8|uint16(b[9]),
		uint64(b[10])<<40|uint64(b[11])<<32|uint64(b[12])<<24|uint64(b[13])<<16|uint64(b[14])<<8|uint64(b[15]),
	)
}

func evalKeysFunction(arg string, row Row, params Params) (any, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return nil, graph.NewError(graph.ErrKindSemantic, "keys() requires one argument", nil)
	}

	value, err := evalExpressionWithScope(arg, row, params)
	if err != nil {
		value, err = evalWriteValue(arg, params, row)
	}
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}

	props := map[string]any{}
	switch typed := value.(type) {
	case *graph.Vertex:
		for key, raw := range typed.Properties {
			if isStoredNullProperty(raw) {
				continue
			}
			props[key] = true
		}
	case *graph.Edge:
		for key, raw := range typed.Properties {
			if isStoredNullProperty(raw) {
				continue
			}
			props[key] = true
		}
	case deletedVertexBinding:
		return nil, graph.NewError(graph.ErrKindNotFound, "DeletedEntityAccess", nil)
	case deletedEdgeBinding:
		return nil, graph.NewError(graph.ErrKindNotFound, "DeletedEntityAccess", nil)
	case map[string]any:
		props = typed
	case string:
		if mapped, ok := parseStoredMapString(typed); ok {
			props = mapped
		} else {
			return nil, graph.NewError(graph.ErrKindSemantic, "invalid argument type", nil)
		}
	default:
		return nil, graph.NewError(graph.ErrKindSemantic, "invalid argument type", nil)
	}

	keys := make([]string, 0, len(props))
	for key := range props {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	out := make([]any, 0, len(keys))
	for _, key := range keys {
		out = append(out, key)
	}
	return out, nil
}

func isStoredNullProperty(raw []byte) bool {
	text := strings.TrimSpace(string(raw))
	if strings.EqualFold(text, "null") {
		return true
	}
	return text == "<nil>"
}

func evalHeadFunction(arg string, row Row, params Params) (any, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return nil, graph.NewError(graph.ErrKindSemantic, "head() requires one argument", nil)
	}

	value, err := evalExpressionWithScope(arg, row, params)
	if err != nil {
		value, err = evalWriteValue(arg, params, row)
	}
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}

	list, ok := normalizeListValue(value)
	if !ok {
		return nil, graph.NewError(graph.ErrKindSemantic, "invalid argument type", nil)
	}
	if len(list) == 0 {
		return nil, nil
	}
	return list[0], nil
}

func evalTailFunction(arg string, row Row, params Params) (any, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return nil, graph.NewError(graph.ErrKindSemantic, "tail() requires one argument", nil)
	}

	value, err := evalExpressionWithScope(arg, row, params)
	if err != nil {
		value, err = evalWriteValue(arg, params, row)
	}
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}

	list, ok := normalizeListValue(value)
	if !ok {
		return nil, graph.NewError(graph.ErrKindSemantic, "invalid argument type", nil)
	}
	if len(list) <= 1 {
		return []any{}, nil
	}

	out := make([]any, 0, len(list)-1)
	out = append(out, list[1:]...)
	return out, nil
}

func evalAbsFunction(arg string, row Row, params Params) (any, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return nil, graph.NewError(graph.ErrKindSemantic, "abs() requires one argument", nil)
	}

	value, err := evalExpressionWithScope(arg, row, params)
	if err != nil {
		value, err = evalWriteValue(arg, params, row)
	}
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}

	if n, err := toInt(value); err == nil {
		if n == math.MinInt {
			return json.Number(formatFloatResult(math.Abs(float64(n)))), nil
		}
		if n < 0 {
			return -n, nil
		}
		return n, nil
	}

	f, ok := numericValue(value)
	if !ok {
		return nil, graph.NewError(graph.ErrKindSemantic, "invalid argument type", nil)
	}
	return json.Number(formatFloatResult(math.Abs(f))), nil
}

func evalSqrtFunction(arg string, row Row, params Params) (any, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return nil, graph.NewError(graph.ErrKindSemantic, "sqrt() requires one argument", nil)
	}

	value, err := evalExpressionWithScope(arg, row, params)
	if err != nil {
		value, err = evalWriteValue(arg, params, row)
	}
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}

	f, ok := numericValue(value)
	if !ok {
		return nil, graph.NewError(graph.ErrKindSemantic, "invalid argument type", nil)
	}
	return json.Number(formatFloatResult(math.Sqrt(f))), nil
}

func evalVertexesFunction(arg string, row Row, params Params) (any, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return nil, graph.NewError(graph.ErrKindSemantic, "vertexes() requires one argument", nil)
	}
	value, err := evalExpressionWithScope(arg, row, params)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	if isNullPathValue(value) {
		return nil, nil
	}
	vertexes, _, ok := pathComponents(value)
	if !ok {
		return nil, graph.NewError(graph.ErrKindSemantic, "invalid argument type", nil)
	}
	out := make([]any, 0, len(vertexes))
	for _, vertex := range vertexes {
		out = append(out, vertex)
	}
	return out, nil
}

func evalRelationshipsFunction(arg string, row Row, params Params) (any, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return nil, graph.NewError(graph.ErrKindSemantic, "relationships() requires one argument", nil)
	}
	value, err := evalExpressionWithScope(arg, row, params)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	if isNullPathValue(value) {
		return nil, nil
	}
	_, edges, ok := pathComponents(value)
	if !ok {
		return nil, graph.NewError(graph.ErrKindSemantic, "invalid argument type", nil)
	}
	out := make([]any, 0, len(edges))
	for _, edge := range edges {
		out = append(out, edge)
	}
	return out, nil
}

func evalLengthFunction(arg string, row Row, params Params) (any, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return nil, graph.NewError(graph.ErrKindSemantic, "length() requires one argument", nil)
	}
	value, err := evalExpressionWithScope(arg, row, params)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	if isNullPathValue(value) {
		return nil, nil
	}
	_, edges, ok := pathComponents(value)
	if !ok {
		return nil, graph.NewError(graph.ErrKindSemantic, "invalid argument type", nil)
	}
	return len(edges), nil
}

func evalLastFunction(arg string, row Row, params Params) (any, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return nil, graph.NewError(graph.ErrKindSemantic, "last() requires one argument", nil)
	}
	value, err := evalExpressionWithScope(arg, row, params)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	list, ok := normalizeListValue(value)
	if !ok {
		return nil, graph.NewError(graph.ErrKindSemantic, "invalid argument type", nil)
	}
	if len(list) == 0 {
		return nil, nil
	}
	return list[len(list)-1], nil
}

func evalSignFunction(arg string, row Row, params Params) (any, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return nil, graph.NewError(graph.ErrKindSemantic, "sign() requires one argument", nil)
	}
	value, err := evalExpressionWithScope(arg, row, params)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	n, ok := numericValue(value)
	if !ok {
		return nil, graph.NewError(graph.ErrKindSemantic, "invalid argument type", nil)
	}
	if n > 0 {
		return 1, nil
	}
	if n < 0 {
		return -1, nil
	}
	return 0, nil
}

func evalStartVertexFunction(arg string, row Row, params Params) (any, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return nil, graph.NewError(graph.ErrKindSemantic, "startVertex() requires one argument", nil)
	}
	value, err := evalExpressionWithScope(arg, row, params)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	edge, ok := edgeValueFromAny(value)
	if !ok {
		return nil, graph.NewError(graph.ErrKindSemantic, "invalid argument type", nil)
	}
	if vertex, ok := findBoundVertexByID(row, edge.SrcID); ok {
		return vertex, nil
	}
	return map[string]any{"id": edge.SrcID}, nil
}

func evalEndVertexFunction(arg string, row Row, params Params) (any, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return nil, graph.NewError(graph.ErrKindSemantic, "endVertex() requires one argument", nil)
	}
	value, err := evalExpressionWithScope(arg, row, params)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	edge, ok := edgeValueFromAny(value)
	if !ok {
		return nil, graph.NewError(graph.ErrKindSemantic, "invalid argument type", nil)
	}
	if vertex, ok := findBoundVertexByID(row, edge.DstID); ok {
		return vertex, nil
	}
	return map[string]any{"id": edge.DstID}, nil
}

func pathComponents(value any) ([]*graph.Vertex, []*graph.Edge, bool) {
	switch typed := value.(type) {
	case cypherPathValue:
		vertexes := make([]*graph.Vertex, 0, 2)
		edges := make([]*graph.Edge, 0, 1)
		if typed.Left != nil {
			vertexes = append(vertexes, typed.Left)
		}
		if typed.Edge != nil {
			edges = append(edges, typed.Edge)
		}
		if typed.Right != nil {
			vertexes = append(vertexes, typed.Right)
		}
		return vertexes, edges, true
	case multiHopCypherPath:
		vertexes := append([]*graph.Vertex(nil), typed.Vertexes...)
		edges := append([]*graph.Edge(nil), typed.Edges...)
		return vertexes, edges, true
	default:
		return nil, nil, false
	}
}

func isNullPathValue(value any) bool {
	switch typed := value.(type) {
	case cypherPathValue:
		return typed.Left == nil && typed.Edge == nil && typed.Right == nil
	case multiHopCypherPath:
		return len(typed.Vertexes) == 0 && len(typed.Edges) == 0
	default:
		return false
	}
}

func edgeValueFromAny(value any) (*graph.Edge, bool) {
	switch typed := value.(type) {
	case *graph.Edge:
		return typed, true
	case map[string]any:
		src, sok := typed["src"]
		dst, dok := typed["dst"]
		if !sok || !dok {
			return nil, false
		}
		return &graph.Edge{SrcID: fmt.Sprint(src), DstID: fmt.Sprint(dst)}, true
	default:
		return nil, false
	}
}

func findBoundVertexByID(row Row, vertexID string) (*graph.Vertex, bool) {
	if row == nil || vertexID == "" {
		return nil, false
	}
	for _, value := range row {
		vertex, ok := value.(*graph.Vertex)
		if !ok || vertex == nil {
			continue
		}
		if vertex.ID == vertexID {
			return vertex, true
		}
	}
	return nil, false
}

func evalToBooleanValue(value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	if b, ok := value.(bool); ok {
		return b, nil
	}
	if s, ok := value.(string); ok {
		s = strings.TrimSpace(s)
		s = strings.ToLower(s)
		switch s {
		case "true":
			return true, nil
		case "false":
			return false, nil
		default:
			return nil, nil
		}
	}
	return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
}

func evalToStringValue(value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	if temporal, ok := temporalMapValue(value); ok {
		if rendered, ok := formatTemporalToString(temporal); ok {
			return rendered, nil
		}
	}
	if vector, ok := vectorValue(value); ok {
		if rendered, ok := formatVectorToString(vector); ok {
			return rendered, nil
		}
	}
	if spatial, ok := spatialPointValue(value); ok {
		if rendered, ok := formatSpatialPointToString(spatial); ok {
			return rendered, nil
		}
	}
	if isInvalidTypeConversionValue(value) {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
	}
	return fmt.Sprint(normalizeResultValue(value)), nil
}

func evalPointFunction(arg string, row Row, params Params) (any, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return nil, graph.NewError(graph.ErrKindSemantic, "point() requires one argument", nil)
	}
	value, err := evalExpressionWithScope(arg, row, params)
	if err != nil {
		value, err = evalWriteValue(arg, params, row)
	}
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	mapped, ok := value.(map[string]any)
	if !ok {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
	}
	for _, item := range mapped {
		if item == nil {
			return nil, nil
		}
	}
	return normalizeSpatialPointMap(mapped)
}

func evalVectorFunction(arg string, row Row, params Params) (any, error) {
	parts := splitTopLevelCommaSeparated(arg)
	if len(parts) != 3 {
		return nil, graph.NewError(graph.ErrKindSemantic, "vector() expects exactly three arguments", nil)
	}

	vectorValueRaw, err := evalExpressionWithScope(parts[0], row, params)
	if err != nil {
		vectorValueRaw, err = evalWriteValue(parts[0], params, row)
	}
	if err != nil {
		return nil, err
	}

	dimensionRaw, err := evalExpressionWithScope(parts[1], row, params)
	if err != nil {
		dimensionRaw, err = evalWriteValue(parts[1], params, row)
	}
	if err != nil {
		return nil, err
	}
	if vectorValueRaw == nil || dimensionRaw == nil {
		return nil, nil
	}

	dimension, err := toInt(dimensionRaw)
	if err != nil || dimension <= 0 || dimension > 4096 {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", err)
	}

	coordinateType, err := parseVectorCoordinateType(parts[2], row, params)
	if err != nil {
		return nil, err
	}

	values, err := vectorCoordinateList(vectorValueRaw)
	if err != nil {
		return nil, err
	}
	if len(values) != dimension {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
	}

	out := map[string]any{
		"__vector_type":  "vector",
		"coordinateType": coordinateType,
		"dimension":      dimension,
		"values":         make([]any, 0, len(values)),
	}
	stored := out["values"].([]any)
	for _, value := range values {
		stored = append(stored, json.Number(formatFloatResult(value)))
	}
	out["values"] = stored
	return out, nil
}

func parseVectorCoordinateType(rawArg string, row Row, params Params) (string, error) {
	rawArg = strings.TrimSpace(rawArg)
	if rawArg == "" {
		return "", graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
	}
	if (strings.HasPrefix(rawArg, "'") && strings.HasSuffix(rawArg, "'")) || (strings.HasPrefix(rawArg, "\"") && strings.HasSuffix(rawArg, "\"")) {
		unquoted := strings.TrimSpace(rawArg[1 : len(rawArg)-1])
		if unquoted == "" {
			return "", graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
		}
		rawArg = unquoted
	} else if value, err := evalExpressionWithScope(rawArg, row, params); err == nil {
		if text, ok := value.(string); ok && strings.TrimSpace(text) != "" {
			rawArg = text
		}
	}

	coordinateType := strings.ToUpper(strings.TrimSpace(rawArg))
	coordinateType = canonicalVectorCoordinateType(coordinateType)
	switch coordinateType {
	case "INTEGER64", "INTEGER32", "INTEGER16", "INTEGER8", "FLOAT64", "FLOAT32", "INTEGER", "FLOAT":
		return coordinateType, nil
	default:
		return "", graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
	}
}

func canonicalVectorCoordinateType(raw string) string {
	switch raw {
	case "INTEGER":
		return "INTEGER64"
	case "FLOAT":
		return "FLOAT64"
	default:
		return raw
	}
}

func vectorCoordinateList(raw any) ([]float64, error) {
	if raw == nil {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
	}
	if mapped, ok := vectorValue(raw); ok {
		values, ok := normalizeListValue(mapped["values"])
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		return numericVectorValues(values)
	}
	if text, ok := raw.(string); ok {
		text = strings.TrimSpace(text)
		if strings.HasPrefix(text, "[") && strings.HasSuffix(text, "]") {
			var parsed []any
			if err := json.Unmarshal([]byte(text), &parsed); err != nil {
				return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", err)
			}
			return numericVectorValues(parsed)
		}
	}
	list, ok := normalizeListValue(raw)
	if !ok {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
	}
	return numericVectorValues(list)
}

func numericVectorValues(values []any) ([]float64, error) {
	out := make([]float64, 0, len(values))
	for _, item := range values {
		if item == nil {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
		}
		numeric, ok := numericValue(item)
		if !ok || math.IsNaN(numeric) || math.IsInf(numeric, 0) {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
		}
		out = append(out, numeric)
	}
	return out, nil
}

func evalVectorNamespaceFunction(raw string, row Row, params Params) (any, bool, error) {
	if arg, ok := parseFunctionCall(raw, "vector.similarity.cosine"); ok {
		value, err := evalVectorSimilarityFunction(arg, row, params, "cosine")
		return value, true, err
	}
	if arg, ok := parseFunctionCall(raw, "vector.similarity.euclidean"); ok {
		value, err := evalVectorSimilarityFunction(arg, row, params, "euclidean")
		return value, true, err
	}
	return nil, false, nil
}

func evalVectorSimilarityFunction(arg string, row Row, params Params, metric string) (any, error) {
	parts := splitTopLevelCommaSeparated(arg)
	if len(parts) != 2 {
		return nil, graph.NewError(graph.ErrKindSemantic, "vector similarity functions expect exactly two arguments", nil)
	}

	leftRaw, err := evalExpressionWithScope(parts[0], row, params)
	if err != nil {
		leftRaw, err = evalWriteValue(parts[0], params, row)
	}
	if err != nil {
		return nil, err
	}
	rightRaw, err := evalExpressionWithScope(parts[1], row, params)
	if err != nil {
		rightRaw, err = evalWriteValue(parts[1], params, row)
	}
	if err != nil {
		return nil, err
	}
	if leftRaw == nil || rightRaw == nil {
		return nil, nil
	}

	leftValues, err := vectorCoordinateList(leftRaw)
	if err != nil {
		return nil, err
	}
	rightValues, err := vectorCoordinateList(rightRaw)
	if err != nil {
		return nil, err
	}
	if len(leftValues) != len(rightValues) || len(leftValues) == 0 {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
	}

	var result float64
	switch metric {
	case "cosine":
		dot := 0.0
		leftNorm := 0.0
		rightNorm := 0.0
		for idx := range leftValues {
			dot += leftValues[idx] * rightValues[idx]
			leftNorm += leftValues[idx] * leftValues[idx]
			rightNorm += rightValues[idx] * rightValues[idx]
		}
		if leftNorm == 0 || rightNorm == 0 {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
		}
		cosine := dot / (math.Sqrt(leftNorm) * math.Sqrt(rightNorm))
		result = (1 + cosine) / 2
		if result < 0 {
			result = 0
		}
		if result > 1 {
			result = 1
		}
	case "euclidean":
		sumSquares := 0.0
		for idx := range leftValues {
			delta := leftValues[idx] - rightValues[idx]
			sumSquares += delta * delta
		}
		result = 1 / (1 + sumSquares)
	default:
		return nil, graph.NewError(graph.ErrKindUnsupported, "unsupported vector similarity metric", nil)
	}

	return json.Number(formatFloatResult(result)), nil
}

func evalVectorDimensionCountFunction(arg string, row Row, params Params) (any, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return nil, graph.NewError(graph.ErrKindSemantic, "vector_dimension_count() requires one argument", nil)
	}
	value, err := evalExpressionWithScope(arg, row, params)
	if err != nil {
		value, err = evalWriteValue(arg, params, row)
	}
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	if mapped, ok := vectorValue(value); ok {
		if dimension, err := toInt(mapped["dimension"]); err == nil {
			return dimension, nil
		}
	}
	if list, ok := normalizeListValue(value); ok {
		return len(list), nil
	}
	return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
}

func evalVectorDistanceFunction(arg string, row Row, params Params) (any, error) {
	parts := splitTopLevelCommaSeparated(arg)
	if len(parts) != 3 {
		return nil, graph.NewError(graph.ErrKindSemantic, "vector_distance() expects exactly three arguments", nil)
	}

	leftRaw, err := evalExpressionWithScope(parts[0], row, params)
	if err != nil {
		leftRaw, err = evalWriteValue(parts[0], params, row)
	}
	if err != nil {
		return nil, err
	}
	rightRaw, err := evalExpressionWithScope(parts[1], row, params)
	if err != nil {
		rightRaw, err = evalWriteValue(parts[1], params, row)
	}
	if err != nil {
		return nil, err
	}
	if leftRaw == nil || rightRaw == nil {
		return nil, nil
	}

	leftValues, err := vectorCoordinateList(leftRaw)
	if err != nil {
		return nil, err
	}
	rightValues, err := vectorCoordinateList(rightRaw)
	if err != nil {
		return nil, err
	}
	if len(leftValues) != len(rightValues) || len(leftValues) == 0 {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
	}

	metric, err := parseVectorDistanceMetric(parts[2], row, params)
	if err != nil {
		return nil, err
	}

	result, err := vectorDistanceMetricValue(leftValues, rightValues, metric)
	if err != nil {
		return nil, err
	}
	return json.Number(formatFloatResult(result)), nil
}

func evalVectorNormFunction(arg string, row Row, params Params) (any, error) {
	parts := splitTopLevelCommaSeparated(arg)
	if len(parts) != 2 {
		return nil, graph.NewError(graph.ErrKindSemantic, "vector_norm() expects exactly two arguments", nil)
	}

	vectorRaw, err := evalExpressionWithScope(parts[0], row, params)
	if err != nil {
		vectorRaw, err = evalWriteValue(parts[0], params, row)
	}
	if err != nil {
		return nil, err
	}
	if vectorRaw == nil {
		return nil, nil
	}

	values, err := vectorCoordinateList(vectorRaw)
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
	}

	metric, err := parseVectorNormMetric(parts[1], row, params)
	if err != nil {
		return nil, err
	}

	result := 0.0
	switch metric {
	case "EUCLIDEAN":
		for _, value := range values {
			result += value * value
		}
		result = math.Sqrt(result)
	case "MANHATTAN":
		for _, value := range values {
			result += math.Abs(value)
		}
	default:
		return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
	}

	return json.Number(formatFloatResult(result)), nil
}

func parseVectorDistanceMetric(raw string, row Row, params Params) (string, error) {
	metric, err := parseVectorMetricToken(raw, row, params)
	if err != nil {
		return "", err
	}
	switch metric {
	case "EUCLIDEAN", "EUCLIDEAN_SQUARED", "MANHATTAN", "COSINE", "DOT", "HAMMING":
		return metric, nil
	default:
		return "", graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
	}
}

func parseVectorNormMetric(raw string, row Row, params Params) (string, error) {
	metric, err := parseVectorMetricToken(raw, row, params)
	if err != nil {
		return "", err
	}
	switch metric {
	case "EUCLIDEAN", "MANHATTAN":
		return metric, nil
	default:
		return "", graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
	}
}

func parseVectorMetricToken(raw string, row Row, params Params) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
	}
	if (strings.HasPrefix(raw, "'") && strings.HasSuffix(raw, "'")) || (strings.HasPrefix(raw, "\"") && strings.HasSuffix(raw, "\"")) {
		return strings.ToUpper(strings.TrimSpace(raw[1 : len(raw)-1])), nil
	}
	if value, err := evalExpressionWithScope(raw, row, params); err == nil {
		if text, ok := value.(string); ok && strings.TrimSpace(text) != "" {
			return strings.ToUpper(strings.TrimSpace(text)), nil
		}
	}
	return strings.ToUpper(raw), nil
}

func vectorDistanceMetricValue(leftValues []float64, rightValues []float64, metric string) (float64, error) {
	result := 0.0
	switch metric {
	case "EUCLIDEAN":
		for idx := range leftValues {
			delta := leftValues[idx] - rightValues[idx]
			result += delta * delta
		}
		return math.Sqrt(result), nil
	case "EUCLIDEAN_SQUARED":
		for idx := range leftValues {
			delta := leftValues[idx] - rightValues[idx]
			result += delta * delta
		}
		return result, nil
	case "MANHATTAN":
		for idx := range leftValues {
			result += math.Abs(leftValues[idx] - rightValues[idx])
		}
		return result, nil
	case "DOT":
		for idx := range leftValues {
			result += leftValues[idx] * rightValues[idx]
		}
		return -result, nil
	case "COSINE":
		dot := 0.0
		leftNorm := 0.0
		rightNorm := 0.0
		for idx := range leftValues {
			dot += leftValues[idx] * rightValues[idx]
			leftNorm += leftValues[idx] * leftValues[idx]
			rightNorm += rightValues[idx] * rightValues[idx]
		}
		if leftNorm == 0 || rightNorm == 0 {
			return 0, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
		}
		cosine := dot / (math.Sqrt(leftNorm) * math.Sqrt(rightNorm))
		return 1 - cosine, nil
	case "HAMMING":
		count := 0
		for idx := range leftValues {
			if leftValues[idx] != rightValues[idx] {
				count++
			}
		}
		return float64(count), nil
	default:
		return 0, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
	}
}

func evalDistanceFunction(arg string, row Row, params Params) (any, error) {
	parts := splitTopLevelCommaSeparated(arg)
	if len(parts) != 2 {
		return nil, graph.NewError(graph.ErrKindSemantic, "distance() expects exactly two arguments", nil)
	}
	left, err := evalExpressionWithScope(parts[0], row, params)
	if err != nil {
		left, err = evalWriteValue(parts[0], params, row)
	}
	if err != nil {
		return nil, err
	}
	right, err := evalExpressionWithScope(parts[1], row, params)
	if err != nil {
		right, err = evalWriteValue(parts[1], params, row)
	}
	if err != nil {
		return nil, err
	}
	if left == nil || right == nil {
		return nil, nil
	}
	leftPoint, ok := spatialPointValue(left)
	if !ok {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
	}
	rightPoint, ok := spatialPointValue(right)
	if !ok {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
	}
	distance, err := pointDistance(leftPoint, rightPoint)
	if err != nil {
		return nil, err
	}
	return json.Number(formatFloatResult(distance)), nil
}

func normalizeSpatialPointMap(in map[string]any) (map[string]any, error) {
	if existing, ok := spatialPointValue(in); ok {
		out := map[string]any{}
		for key, value := range existing {
			out[key] = value
		}
		return out, nil
	}
	if len(in) == 0 {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
	}

	out := map[string]any{"__spatial_type": "point"}
	sridFromCRS, hasCRS, err := spatialCRSValue(in["crs"])
	if err != nil {
		return nil, err
	}
	srid, hasSRID, err := spatialSRIDValue(in["srid"])
	if err != nil {
		return nil, err
	}
	if hasCRS {
		if hasSRID && srid != sridFromCRS {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
		}
		srid = sridFromCRS
		hasSRID = true
	}

	hasGeographic := hasSpatialKey(in, "longitude") || hasSpatialKey(in, "latitude") || hasSpatialKey(in, "height")
	hasCartesian := hasSpatialKey(in, "x") || hasSpatialKey(in, "y") || hasSpatialKey(in, "z")
	if hasGeographic && hasCartesian {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
	}

	targetGeographic := hasGeographic
	if !targetGeographic && hasSRID && (srid == 4326 || srid == 4979) {
		targetGeographic = true
	}

	if targetGeographic {
		if hasGeographic && hasSRID && (srid == 7203 || srid == 9157) {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
		}
		longitudeKey := "longitude"
		latitudeKey := "latitude"
		heightKey := "height"
		if !hasGeographic {
			longitudeKey = "x"
			latitudeKey = "y"
			heightKey = "z"
		}
		lon, ok, err := spatialCoordinate(in, longitudeKey)
		if err != nil || !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", err)
		}
		lat, ok, err := spatialCoordinate(in, latitudeKey)
		if err != nil || !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", err)
		}
		lon = wrapLongitude(lon)
		if err := validateGeographicCoordinateRange(lon, lat); err != nil {
			return nil, err
		}
		out["longitude"] = json.Number(formatFloatResult(lon))
		out["latitude"] = json.Number(formatFloatResult(lat))
		out["x"] = json.Number(formatFloatResult(lon))
		out["y"] = json.Number(formatFloatResult(lat))
		if height, ok, err := spatialCoordinate(in, heightKey); err != nil {
			return nil, err
		} else if ok {
			out["height"] = json.Number(formatFloatResult(height))
			out["z"] = json.Number(formatFloatResult(height))
			if !hasSRID {
				srid = 4979
			}
		} else if !hasSRID {
			srid = 4326
		}
		if err := validateSpatialSRIDShape(srid, true, hasSpatialKey(out, "height")); err != nil {
			return nil, err
		}
		out["srid"] = srid
		out["crs"] = spatialCRSNameFromSRID(srid)
		return out, nil
	}
	if hasGeographic && hasSRID {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
	}

	x, ok, err := spatialCoordinate(in, "x")
	if err != nil || !ok {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", err)
	}
	y, ok, err := spatialCoordinate(in, "y")
	if err != nil || !ok {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", err)
	}
	out["x"] = json.Number(formatFloatResult(x))
	out["y"] = json.Number(formatFloatResult(y))
	if z, ok, err := spatialCoordinate(in, "z"); err != nil {
		return nil, err
	} else if ok {
		out["z"] = json.Number(formatFloatResult(z))
		if !hasSRID {
			srid = 9157
		}
	} else if !hasSRID {
		srid = 7203
	}
	if err := validateSpatialSRIDShape(srid, false, hasSpatialKey(out, "z")); err != nil {
		return nil, err
	}
	out["srid"] = srid
	out["crs"] = spatialCRSNameFromSRID(srid)
	return out, nil
}

func wrapLongitude(value float64) float64 {
	if value >= -180 && value <= 180 {
		return value
	}
	wrapped := math.Mod(value+180, 360)
	if wrapped < 0 {
		wrapped += 360
	}
	wrapped -= 180
	if wrapped == -180 && value > 0 {
		return 180
	}
	return wrapped
}

func hasSpatialKey(in map[string]any, key string) bool {
	_, ok := in[key]
	return ok
}

func spatialCoordinate(in map[string]any, key string) (float64, bool, error) {
	raw, ok := in[key]
	if !ok || raw == nil {
		return 0, false, nil
	}
	value, ok := numericValue(raw)
	if !ok {
		return 0, false, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
	}
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, false, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
	}
	return value, true, nil
}

func spatialSRIDValue(raw any) (int, bool, error) {
	if raw == nil {
		return 0, false, nil
	}
	value, err := toInt(raw)
	if err != nil {
		return 0, false, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", err)
	}
	return value, true, nil
}

func spatialCRSValue(raw any) (int, bool, error) {
	if raw == nil {
		return 0, false, nil
	}
	text, ok := raw.(string)
	if !ok {
		return 0, false, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
	}
	switch strings.ToLower(strings.TrimSpace(text)) {
	case "wgs-84", "wgs-84-2d":
		return 4326, true, nil
	case "wgs-84-3d":
		return 4979, true, nil
	case "cartesian":
		return 7203, true, nil
	case "cartesian-3d":
		return 9157, true, nil
	default:
		return 0, false, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
	}
}

func spatialCRSNameFromSRID(srid int) string {
	switch srid {
	case 4326:
		return "wgs-84"
	case 4979:
		return "wgs-84-3d"
	case 7203:
		return "cartesian"
	case 9157:
		return "cartesian-3d"
	default:
		return ""
	}
}

func spatialPointValue(v any) (map[string]any, bool) {
	mapped, ok := v.(map[string]any)
	if !ok {
		return nil, false
	}
	if strings.EqualFold(strings.TrimSpace(fmt.Sprint(mapped["__spatial_type"])), "point") {
		return mapped, true
	}
	return nil, false
}

func vectorValue(v any) (map[string]any, bool) {
	mapped, ok := v.(map[string]any)
	if !ok {
		return nil, false
	}
	if strings.EqualFold(strings.TrimSpace(fmt.Sprint(mapped["__vector_type"])), "vector") {
		return mapped, true
	}
	return nil, false
}

func formatVectorToString(vector map[string]any) (string, bool) {
	if vector == nil {
		return "", false
	}
	values, ok := normalizeListValue(vector["values"])
	if !ok {
		return "", false
	}
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, fmt.Sprint(normalizeResultValue(value)))
	}
	dimension, err := toInt(vector["dimension"])
	if err != nil || dimension <= 0 {
		return "", false
	}
	coordinateType := strings.ToUpper(strings.TrimSpace(fmt.Sprint(vector["coordinateType"])))
	if coordinateType == "" {
		coordinateType = "FLOAT32"
	}
	return fmt.Sprintf("vector([%s], %d, %s)", strings.Join(parts, ", "), dimension, coordinateType), true
}

func formatSpatialPointToString(point map[string]any) (string, bool) {
	if point == nil {
		return "", false
	}
	parts := make([]string, 0, 4)
	if srid, ok := point["srid"]; ok {
		parts = append(parts, fmt.Sprintf("srid: %v", normalizeResultValue(srid)))
	}
	if x, ok := point["x"]; ok {
		parts = append(parts, fmt.Sprintf("x: %v", normalizeResultValue(x)))
		parts = append(parts, fmt.Sprintf("y: %v", normalizeResultValue(point["y"])))
		if z, ok := point["z"]; ok {
			parts = append(parts, fmt.Sprintf("z: %v", normalizeResultValue(z)))
		}
		return "point({" + strings.Join(parts, ", ") + "})", true
	}
	return "", false
}

func evalSpatialNamespaceFunction(raw string, row Row, params Params) (any, bool, error) {
	if arg, ok := parseFunctionCall(raw, "point.distance"); ok {
		value, err := evalDistanceFunction(arg, row, params)
		return value, true, err
	}
	if arg, ok := parseFunctionCall(raw, "point.withinBBox"); ok {
		value, err := evalWithinBBoxFunction(arg, row, params)
		return value, true, err
	}
	return nil, false, nil
}

func evalWithinBBoxFunction(arg string, row Row, params Params) (any, error) {
	parts := splitTopLevelCommaSeparated(arg)
	if len(parts) != 3 {
		return nil, graph.NewError(graph.ErrKindSemantic, "point.withinBBox() expects exactly three arguments", nil)
	}
	values := make([]any, 0, 3)
	for _, part := range parts {
		value, err := evalExpressionWithScope(part, row, params)
		if err != nil {
			value, err = evalWriteValue(part, params, row)
		}
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, nil
		}
		values = append(values, value)
	}
	point, ok := spatialPointValue(values[0])
	if !ok {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
	}
	lowerLeft, ok := spatialPointValue(values[1])
	if !ok {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
	}
	upperRight, ok := spatialPointValue(values[2])
	if !ok {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
	}
	within, comparable, err := pointWithinBBox(point, lowerLeft, upperRight)
	if err != nil {
		return nil, err
	}
	if !comparable {
		return nil, nil
	}
	return within, nil
}

func pointWithinBBox(point, lowerLeft, upperRight map[string]any) (bool, bool, error) {
	pointSRID, _, err := spatialSRIDValue(point["srid"])
	if err != nil {
		return false, false, err
	}
	lowerSRID, _, err := spatialSRIDValue(lowerLeft["srid"])
	if err != nil {
		return false, false, err
	}
	upperSRID, _, err := spatialSRIDValue(upperRight["srid"])
	if err != nil {
		return false, false, err
	}
	if pointSRID != lowerSRID || pointSRID != upperSRID {
		return false, false, nil
	}
	if _, ok := point["longitude"]; ok {
		return geographicPointWithinBBox(point, lowerLeft, upperRight)
	}
	return cartesianPointWithinBBox(point, lowerLeft, upperRight)
}

func cartesianPointWithinBBox(point, lowerLeft, upperRight map[string]any) (bool, bool, error) {
	px, _, err := spatialCoordinate(point, "x")
	if err != nil {
		return false, false, err
	}
	py, _, err := spatialCoordinate(point, "y")
	if err != nil {
		return false, false, err
	}
	lx, _, err := spatialCoordinate(lowerLeft, "x")
	if err != nil {
		return false, false, err
	}
	ly, _, err := spatialCoordinate(lowerLeft, "y")
	if err != nil {
		return false, false, err
	}
	ux, _, err := spatialCoordinate(upperRight, "x")
	if err != nil {
		return false, false, err
	}
	uy, _, err := spatialCoordinate(upperRight, "y")
	if err != nil {
		return false, false, err
	}
	if lx > ux || ly > uy {
		return false, true, nil
	}
	if pz, pok, err := spatialCoordinate(point, "z"); err != nil {
		return false, false, err
	} else if lz, lok, err := spatialCoordinate(lowerLeft, "z"); err != nil {
		return false, false, err
	} else if uz, uok, err := spatialCoordinate(upperRight, "z"); err != nil {
		return false, false, err
	} else if pok != lok || pok != uok {
		return false, false, nil
	} else if pok {
		if lz > uz {
			return false, true, nil
		}
		return px >= lx && px <= ux && py >= ly && py <= uy && pz >= lz && pz <= uz, true, nil
	}
	return px >= lx && px <= ux && py >= ly && py <= uy, true, nil
}

func geographicPointWithinBBox(point, lowerLeft, upperRight map[string]any) (bool, bool, error) {
	plon, _, err := spatialCoordinate(point, "longitude")
	if err != nil {
		return false, false, err
	}
	plat, _, err := spatialCoordinate(point, "latitude")
	if err != nil {
		return false, false, err
	}
	llon, _, err := spatialCoordinate(lowerLeft, "longitude")
	if err != nil {
		return false, false, err
	}
	llat, _, err := spatialCoordinate(lowerLeft, "latitude")
	if err != nil {
		return false, false, err
	}
	ulon, _, err := spatialCoordinate(upperRight, "longitude")
	if err != nil {
		return false, false, err
	}
	ulat, _, err := spatialCoordinate(upperRight, "latitude")
	if err != nil {
		return false, false, err
	}
	if llat > ulat {
		return false, true, nil
	}
	inLon := false
	if llon <= ulon {
		inLon = plon >= llon && plon <= ulon
	} else {
		inLon = plon >= llon || plon <= ulon
	}
	if ph, pok, err := spatialCoordinate(point, "height"); err != nil {
		return false, false, err
	} else if lh, lok, err := spatialCoordinate(lowerLeft, "height"); err != nil {
		return false, false, err
	} else if uh, uok, err := spatialCoordinate(upperRight, "height"); err != nil {
		return false, false, err
	} else if pok != lok || pok != uok {
		return false, false, nil
	} else if pok {
		if lh > uh {
			return false, true, nil
		}
		return inLon && plat >= llat && plat <= ulat && ph >= lh && ph <= uh, true, nil
	}
	return inLon && plat >= llat && plat <= ulat, true, nil
}

func pointDistance(left, right map[string]any) (float64, error) {
	leftSRID, _, err := spatialSRIDValue(left["srid"])
	if err != nil {
		return 0, err
	}
	rightSRID, _, err := spatialSRIDValue(right["srid"])
	if err != nil {
		return 0, err
	}
	if leftSRID != rightSRID {
		return 0, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
	}
	leftGeographic := hasSpatialKey(left, "longitude")
	rightGeographic := hasSpatialKey(right, "longitude")
	if leftGeographic != rightGeographic {
		return 0, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
	}
	if leftGeographic {
		return geographicPointDistance(left, right)
	}
	return cartesianPointDistance(left, right)
}

func validateGeographicCoordinateRange(longitude float64, latitude float64) error {
	if longitude < -180 || longitude > 180 {
		return graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
	}
	if latitude < -90 || latitude > 90 {
		return graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
	}
	return nil
}

func validateSpatialSRIDShape(srid int, geographic bool, threeD bool) error {
	switch srid {
	case 4326:
		if !geographic || threeD {
			return graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
		}
	case 4979:
		if !geographic || !threeD {
			return graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
		}
	case 7203:
		if geographic || threeD {
			return graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
		}
	case 9157:
		if geographic || !threeD {
			return graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
		}
	default:
		return graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
	}
	return nil
}

func cartesianPointDistance(left, right map[string]any) (float64, error) {
	lx, _, err := spatialCoordinate(left, "x")
	if err != nil {
		return 0, err
	}
	ly, _, err := spatialCoordinate(left, "y")
	if err != nil {
		return 0, err
	}
	rx, _, err := spatialCoordinate(right, "x")
	if err != nil {
		return 0, err
	}
	ry, _, err := spatialCoordinate(right, "y")
	if err != nil {
		return 0, err
	}
	dx := lx - rx
	dy := ly - ry
	dz := 0.0
	if lz, lok, err := spatialCoordinate(left, "z"); err != nil {
		return 0, err
	} else if rz, rok, err := spatialCoordinate(right, "z"); err != nil {
		return 0, err
	} else if lok != rok {
		return 0, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
	} else if lok {
		dz = lz - rz
	}
	return math.Sqrt(dx*dx + dy*dy + dz*dz), nil
}

func geographicPointDistance(left, right map[string]any) (float64, error) {
	llon, _, err := spatialCoordinate(left, "longitude")
	if err != nil {
		return 0, err
	}
	llat, _, err := spatialCoordinate(left, "latitude")
	if err != nil {
		return 0, err
	}
	rlon, _, err := spatialCoordinate(right, "longitude")
	if err != nil {
		return 0, err
	}
	rlat, _, err := spatialCoordinate(right, "latitude")
	if err != nil {
		return 0, err
	}
	lat1 := llat * math.Pi / 180
	lat2 := rlat * math.Pi / 180
	dLat := (rlat - llat) * math.Pi / 180
	dLon := (rlon - llon) * math.Pi / 180
	a := math.Sin(dLat/2)*math.Sin(dLat/2) + math.Cos(lat1)*math.Cos(lat2)*math.Sin(dLon/2)*math.Sin(dLon/2)
	surface := 2 * 6371008.8 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	if lh, lok, err := spatialCoordinate(left, "height"); err != nil {
		return 0, err
	} else if rh, rok, err := spatialCoordinate(right, "height"); err != nil {
		return 0, err
	} else if lok != rok {
		return 0, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
	} else if lok {
		dh := lh - rh
		return math.Sqrt(surface*surface + dh*dh), nil
	}
	return surface, nil
}

func evalToIntegerValue(value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	if isInvalidTypeConversionValue(value) {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
	}
	normalized := normalizeResultValue(value)
	switch typed := normalized.(type) {
	case string:
		s := strings.TrimSpace(typed)
		if s == "" {
			return nil, nil
		}
		if f, ok := numericValue(s); ok {
			return int(truncTowardZero(f)), nil
		}
		return nil, nil
	case json.Number:
		f, err := typed.Float64()
		if err != nil {
			return nil, nil
		}
		return int(truncTowardZero(f)), nil
	default:
		if f, ok := numericValue(normalized); ok {
			return int(truncTowardZero(f)), nil
		}
		return nil, nil
	}
}

func evalToFloatValue(value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	switch typed := value.(type) {
	case bool, []any, []string, map[string]any, *graph.Vertex, *graph.Edge, deletedVertexBinding, deletedEdgeBinding:
		return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
	case string:
		s := strings.TrimSpace(typed)
		if s == "" {
			return nil, nil
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, nil
		}
		return json.Number(formatFloatResult(f)), nil
	case json.Number:
		f, err := typed.Float64()
		if err != nil {
			return nil, nil
		}
		return json.Number(formatFloatResult(f)), nil
	case int:
		return json.Number(formatFloatResult(float64(typed))), nil
	case int64:
		return json.Number(formatFloatResult(float64(typed))), nil
	case int32:
		return json.Number(formatFloatResult(float64(typed))), nil
	case uint:
		return json.Number(formatFloatResult(float64(typed))), nil
	case uint64:
		return json.Number(formatFloatResult(float64(typed))), nil
	case uint32:
		return json.Number(formatFloatResult(float64(typed))), nil
	case float64:
		return json.Number(formatFloatResult(typed)), nil
	case float32:
		return json.Number(formatFloatResult(float64(typed))), nil
	default:
		if f, ok := numericValue(value); ok {
			return json.Number(formatFloatResult(f)), nil
		}
		return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
	}
}

func clonePropertyMap(props graph.PropertyMap) map[string]any {
	if len(props) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(props))
	for key, raw := range props {
		out[key] = decodeStoredPropertyValue(raw)
	}
	return out
}

func evalConvertedListFunction(arg string, row Row, params Params, converter func(any) (any, error)) (any, error) {
	value, err := evalExpressionWithScope(arg, row, params)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	list, ok := normalizeListValue(value)
	if !ok {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
	}
	out := make([]any, 0, len(list))
	for _, item := range list {
		converted, err := converter(item)
		if err != nil {
			out = append(out, nil)
			continue
		}
		out = append(out, converted)
	}
	return out, nil
}

func evalTrimFunction(arg string, row Row, params Params, name string) (any, error) {
	parts := splitTopLevelCommaSeparated(arg)
	if len(parts) != 1 && len(parts) != 2 {
		return nil, graph.NewError(graph.ErrKindSemantic, fmt.Sprintf("%s() expects one or two arguments", name), nil)
	}
	input, err := evalExpressionWithScope(parts[0], row, params)
	if err != nil {
		return nil, err
	}
	if input == nil {
		return nil, nil
	}
	inputStr, ok := input.(string)
	if !ok {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
	}
	if len(parts) == 1 {
		return strings.TrimSpace(inputStr), nil
	}
	trimChars, err := evalExpressionWithScope(parts[1], row, params)
	if err != nil {
		return nil, err
	}
	if trimChars == nil {
		return nil, nil
	}
	trimCharsStr, ok := trimChars.(string)
	if !ok {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
	}
	return strings.Trim(inputStr, trimCharsStr), nil
}

func evalNormalizeFunction(arg string, row Row, params Params) (any, error) {
	parts := splitTopLevelCommaSeparated(arg)
	if len(parts) != 1 && len(parts) != 2 {
		return nil, graph.NewError(graph.ErrKindSemantic, "normalize() expects one or two arguments", nil)
	}
	value, err := evalExpressionWithScope(parts[0], row, params)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	input, ok := value.(string)
	if !ok {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
	}
	form := norm.NFC
	if len(parts) == 2 {
		formValue, err := evalExpressionWithScope(parts[1], row, params)
		if err != nil {
			return nil, err
		}
		if formValue == nil {
			return nil, nil
		}
		formName, ok := formValue.(string)
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
		}
		switch strings.ToUpper(strings.TrimSpace(formName)) {
		case "NFC":
			form = norm.NFC
		case "NFD":
			form = norm.NFD
		case "NFKC":
			form = norm.NFKC
		case "NFKD":
			form = norm.NFKD
		default:
			return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil)
		}
	}
	return form.String(input), nil
}

func evalReduceFunction(arg string, row Row, params Params) (any, error) {
	accumulatorName, initialExpr, variableName, listExpr, bodyExpr, ok := parseReduceFunctionArgs(arg)
	if !ok {
		return nil, graph.NewError(graph.ErrKindSemantic, "reduce() expects reduce(acc = initial, item IN list | expression)", nil)
	}
	accumulator, err := evalExpressionWithScope(initialExpr, row, params)
	if err != nil {
		return nil, err
	}
	listValue, err := evalExpressionWithScope(listExpr, row, params)
	if err != nil {
		return nil, err
	}
	if listValue == nil {
		return nil, nil
	}
	list, ok := normalizeListValue(listValue)
	if !ok {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil)
	}
	for _, item := range list {
		scope := cloneRow(row)
		scope[accumulatorName] = accumulator
		scope[variableName] = item
		accumulator, err = evalExpressionWithScope(bodyExpr, scope, params)
		if err != nil {
			return nil, err
		}
	}
	return accumulator, nil
}

func parseReduceFunctionArgs(raw string) (string, string, string, string, string, bool) {
	parts := splitTopLevelCommaSeparated(raw)
	if len(parts) != 2 {
		return "", "", "", "", "", false
	}
	assign := strings.TrimSpace(parts[0])
	idx := findTopLevelEqualsIndex(assign)
	if idx < 0 {
		return "", "", "", "", "", false
	}
	accumulatorName := strings.TrimSpace(assign[:idx])
	initialExpr := strings.TrimSpace(assign[idx+1:])
	if !isIdentifierLike(accumulatorName) || initialExpr == "" {
		return "", "", "", "", "", false
	}
	loop := strings.TrimSpace(parts[1])
	inIdx := findTopLevelKeywordIndex(loop, "IN")
	pipeIdx := findTopLevelPipeIndex(loop)
	if inIdx < 0 || pipeIdx < 0 || inIdx > pipeIdx {
		return "", "", "", "", "", false
	}
	variableName := strings.TrimSpace(loop[:inIdx])
	listExpr := strings.TrimSpace(loop[inIdx+len("IN") : pipeIdx])
	bodyExpr := strings.TrimSpace(loop[pipeIdx+1:])
	if !isIdentifierLike(variableName) || listExpr == "" || bodyExpr == "" {
		return "", "", "", "", "", false
	}
	return accumulatorName, initialExpr, variableName, listExpr, bodyExpr, true
}

func (e *Executor) evalWhereExpression(ctx context.Context, tx graph.Tx, raw string, row Row, params Params) (bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false, graph.NewError(graph.ErrKindSemantic, "empty WHERE expression", nil)
	}
	if body, ok := parseExistsSubqueryBody(raw); ok {
		return e.evalExistsSubquery(ctx, tx, body, row, params)
	}

	if left, right, ok := splitTopLevelCompressedBoolean(raw, "OR"); ok {
		lhs, err := e.evalWhereExpression(ctx, tx, left, row, params)
		if err != nil {
			return false, err
		}
		if lhs {
			return true, nil
		}
		return e.evalWhereExpression(ctx, tx, right, row, params)
	}
	if left, right, ok := splitTopLevelKeyword(raw, "OR"); ok {
		lhs, err := e.evalWhereExpression(ctx, tx, left, row, params)
		if err != nil {
			return false, err
		}
		if lhs {
			return true, nil
		}
		return e.evalWhereExpression(ctx, tx, right, row, params)
	}

	if left, right, ok := splitTopLevelCompressedBoolean(raw, "AND"); ok {
		lhs, err := e.evalWhereExpression(ctx, tx, left, row, params)
		if err != nil {
			return false, err
		}
		if !lhs {
			return false, nil
		}
		return e.evalWhereExpression(ctx, tx, right, row, params)
	}
	if left, right, ok := splitTopLevelKeyword(raw, "AND"); ok {
		lhs, err := e.evalWhereExpression(ctx, tx, left, row, params)
		if err != nil {
			return false, err
		}
		if !lhs {
			return false, nil
		}
		return e.evalWhereExpression(ctx, tx, right, row, params)
	}

	if left, right, ok := splitTopLevelCompressedBoolean(raw, "XOR"); ok {
		lhs, err := e.evalWhereExpression(ctx, tx, left, row, params)
		if err != nil {
			return false, err
		}
		rhs, err := e.evalWhereExpression(ctx, tx, right, row, params)
		if err != nil {
			return false, err
		}
		return lhs != rhs, nil
	}
	if left, right, ok := splitTopLevelKeyword(raw, "XOR"); ok {
		lhs, err := e.evalWhereExpression(ctx, tx, left, row, params)
		if err != nil {
			return false, err
		}
		rhs, err := e.evalWhereExpression(ctx, tx, right, row, params)
		if err != nil {
			return false, err
		}
		return lhs != rhs, nil
	}

	if hasLogicalNotPrefix(raw) {
		operand := strings.TrimSpace(raw[3:])
		if body, ok := parseExistsSubqueryBody(operand); ok {
			matched, err := e.evalExistsSubquery(ctx, tx, body, row, params)
			if err != nil {
				return false, err
			}
			return !matched, nil
		}
		if matched, handled, err := e.evalWhereRelationshipPatternPredicate(ctx, tx, operand, row, params); handled {
			if err != nil {
				return false, err
			}
			return !matched, nil
		}
		value, err := evalExpressionWithScope(operand, row, params)
		if err != nil {
			return false, err
		}
		b, isNull, err := asNullableBoolean(value)
		if err != nil {
			return false, err
		}
		if isNull {
			return false, nil
		}
		return !b, nil
	}

	if matched, handled, err := e.evalWhereRelationshipPatternPredicate(ctx, tx, raw, row, params); handled {
		if err != nil {
			return false, err
		}
		return matched, nil
	}

	if strings.HasPrefix(raw, "(") && strings.HasSuffix(raw, ")") && parensAreBalanced(raw[1:len(raw)-1]) {
		return e.evalWhereExpression(ctx, tx, raw[1:len(raw)-1], row, params)
	}
	if operands, operators, ok := splitTopLevelComparisonChain(raw); ok {
		var sawNull bool
		for i := 0; i < len(operators); i++ {
			lhs, err := evalExpressionWithScope(operands[i], row, params)
			if err != nil {
				return false, err
			}
			rhs, err := evalExpressionWithScope(operands[i+1], row, params)
			if err != nil {
				return false, err
			}
			result, err := compareExpressionValues(lhs, rhs, operators[i])
			if err != nil {
				return false, err
			}
			if result == nil {
				sawNull = true
				continue
			}
			truth, ok := result.(bool)
			if !ok {
				return false, nil
			}
			if !truth {
				return false, nil
			}
		}
		if sawNull {
			return false, nil
		}
		return true, nil
	}

	if left, right, op, ok := splitTopLevelComparison(raw); ok {
		lhs, err := evalExpressionWithScope(left, row, params)
		if err != nil {
			return false, err
		}
		rhs, err := evalExpressionWithScope(right, row, params)
		if err != nil {
			return false, err
		}
		return compareWhereValues(lhs, rhs, op)
	}
	if left, right, ok := splitTopLevelInExpression(raw); ok {
		lhs, err := evalExpressionWithScope(left, row, params)
		if err != nil {
			return false, err
		}
		rhs, err := evalExpressionWithScope(right, row, params)
		if err != nil {
			return false, err
		}
		value, err := evalInExpression(lhs, rhs)
		if err != nil {
			return false, err
		}
		return truthyWhereValue(value), nil
	}
	if left, isNull, ok := splitTopLevelNullPredicate(raw); ok {
		value, err := evalExpressionWithScope(left, row, params)
		if err != nil {
			return false, err
		}
		if isNull {
			return value == nil, nil
		}
		return value != nil, nil
	}

	value, err := evalExpressionWithScope(raw, row, params)
	if err != nil {
		return false, err
	}
	return truthyWhereValue(value), nil
}

func parseExistsSubqueryBody(raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	if len(raw) < len("EXISTS{}") {
		return "", false
	}
	if !strings.EqualFold(raw[:6], "EXISTS") {
		return "", false
	}
	rest := strings.TrimSpace(raw[6:])
	if len(rest) < 2 || !strings.HasPrefix(rest, "{") || !strings.HasSuffix(rest, "}") {
		return "", false
	}
	if !bracesAreBalanced(rest[1 : len(rest)-1]) {
		return "", false
	}
	body := strings.TrimSpace(rest[1 : len(rest)-1])
	if body == "" {
		return "", false
	}
	return body, true
}

func bracesAreBalanced(raw string) bool {
	depth := 0
	for _, r := range raw {
		switch r {
		case '{':
			depth++
		case '}':
			if depth == 0 {
				return false
			}
			depth--
		}
	}
	return depth == 0
}

func (e *Executor) evalExistsSubquery(ctx context.Context, tx graph.Tx, body string, row Row, params Params) (bool, error) {
	if tx == nil {
		return false, graph.NewError(graph.ErrKindUnsupported, "EXISTS subquery requires transactional context", nil)
	}
	body = strings.TrimSpace(body)
	if result, ok, err := e.evalExistsQueryBody(ctx, tx, body, row, params); ok {
		return result, err
	}
	if patternBody, whereBody, ok := splitExistsPatternBody(body); ok {
		matches, err := e.applyMatchClause(ctx, tx, []Row{cloneRow(row)}, ast.Clause{Kind: ast.ClauseKindMatch, Raw: "MATCH " + patternBody, MatchPattern: patternBody, MatchOptional: false}, params)
		if err != nil {
			return false, err
		}
		if whereBody == "" {
			return len(matches) > 0, nil
		}
		for _, matched := range matches {
			ok, err := e.evalWhereExpression(ctx, tx, whereBody, matched, params)
			if err != nil {
				return false, err
			}
			if ok {
				return true, nil
			}
		}
		return false, nil
	}
	if !strings.HasPrefix(strings.ToUpper(body), "MATCH") && !strings.HasPrefix(strings.ToUpper(body), "OPTIONALMATCH") {
		return false, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("EXISTS subquery %q is not yet supported", body), nil)
	}

	rows, err := e.applyMatchClause(ctx, tx, []Row{cloneRow(row)}, ast.Clause{Kind: ast.ClauseKindMatch, Raw: body}, params)
	if err != nil {
		return false, err
	}
	return len(rows) > 0, nil
}

func (e *Executor) evalExistsQueryBody(ctx context.Context, tx graph.Tx, body string, row Row, params Params) (bool, bool, error) {
	body = strings.TrimSpace(stripCypherLineComments(body))
	upper := strings.ToUpper(body)
	matchKeyword := ""
	if strings.HasPrefix(upper, "OPTIONAL MATCH") {
		matchKeyword = "OPTIONAL MATCH"
	} else if strings.HasPrefix(upper, "OPTIONALMATCH") {
		matchKeyword = "OPTIONALMATCH"
	} else if strings.HasPrefix(upper, "MATCH") {
		matchKeyword = "MATCH"
	} else {
		return false, false, nil
	}
	rest := strings.TrimSpace(body[len(matchKeyword):])
	nextClauseIdx := minPositiveIndex(
		findTopLevelKeywordIndex(rest, "WITH"),
		findTopLevelKeywordIndex(rest, "RETURN"),
	)
	matchExpr := rest
	remaining := ""
	if nextClauseIdx >= 0 {
		matchExpr = strings.TrimSpace(rest[:nextClauseIdx])
		remaining = strings.TrimSpace(rest[nextClauseIdx:])
	}
	if matchExpr == "" {
		return false, true, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("EXISTS subquery %q is not yet supported", body), nil)
	}
	matchRaw := "MATCH " + matchExpr
	matchKind := ast.ClauseKindMatch
	if matchKeyword == "OPTIONALMATCH" || matchKeyword == "OPTIONAL MATCH" {
		matchRaw = "OPTIONAL MATCH " + matchExpr
		matchKind = ast.ClauseKindOptionalMatch
	}
	rows := []Row{cloneRow(row)}
	resultColumns := []string{}
	rows, err := e.applyMatchClause(ctx, tx, rows, ast.Clause{Kind: matchKind, Raw: matchRaw}, params)
	if err != nil {
		return false, true, err
	}
	if remaining == "" {
		return len(rows) > 0, true, nil
	}
	upperRemaining := strings.ToUpper(remaining)
	if strings.HasPrefix(upperRemaining, "WITH") {
		returnIdx := findTopLevelKeywordIndex(remaining, "RETURN")
		withRaw := remaining
		next := ""
		if returnIdx >= 0 {
			withRaw = strings.TrimSpace(remaining[:returnIdx])
			next = strings.TrimSpace(remaining[returnIdx:])
		}
		withClause, err := buildStructuredProjectionClause(ast.ClauseKindWith, withRaw, inferColumnsFromRows(rows))
		if err != nil {
			return false, true, err
		}
		var stepErr error
		rows, resultColumns, stepErr = e.applyProjectionClause(ctx, tx, rows, withClause, params, resultColumns)
		if stepErr != nil {
			return false, true, stepErr
		}
		remaining = next
		upperRemaining = strings.ToUpper(remaining)
	}
	if strings.HasPrefix(upperRemaining, "RETURN") {
		returnClause, err := buildStructuredProjectionClause(ast.ClauseKindReturn, remaining, inferColumnsFromRows(rows))
		if err != nil {
			return false, true, err
		}
		var stepErr error
		rows, resultColumns, stepErr = e.applyProjectionClause(ctx, tx, rows, returnClause, params, resultColumns)
		if stepErr != nil {
			return false, true, stepErr
		}
		return len(rows) > 0, true, nil
	}
	if remaining != "" {
		return false, true, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("EXISTS subquery %q is not yet supported", body), nil)
	}
	return len(rows) > 0, true, nil
}

func buildStructuredProjectionClause(kind ast.ClauseKind, raw string, scopeVars []string) (ast.Clause, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ast.Clause{}, graph.NewError(graph.ErrKindInvalidInput, "projection clause is required", nil)
	}

	query := raw
	prelude := buildProjectionScopePrelude(scopeVars)
	switch kind {
	case ast.ClauseKindWith:
		if !strings.HasPrefix(strings.ToUpper(query), "WITH") {
			query = "WITH " + query
		}
		query = prelude + query + " RETURN 1"
	case ast.ClauseKindReturn:
		if !strings.HasPrefix(strings.ToUpper(query), "RETURN") {
			query = "RETURN " + query
		}
		query = prelude + query
	default:
		return ast.Clause{}, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("projection clause kind %s is not supported", kind), nil)
	}

	stmt, err := parser.ParseStatement(query)
	if err != nil {
		compactBody := strings.TrimSpace(stripLeadingClauseKeyword(raw, string(kind)))
		spec, parseErr := parseProjectionClauseSpec(compactBody)
		if parseErr != nil {
			return ast.Clause{}, err
		}
		projection, convErr := projectionClauseFromSpec(spec)
		if convErr != nil {
			return ast.Clause{}, convErr
		}
		clause := ast.Clause{Kind: kind, Raw: raw, Projection: &projection}
		if kind == ast.ClauseKindWith && strings.TrimSpace(spec.WhereRaw) != "" {
			expr := ast.Expression{Raw: strings.TrimSpace(spec.WhereRaw)}
			clause.Where = &expr
		}
		return clause, nil
	}
	typed, ok := stmt.(*ast.QueryStatement)
	if !ok || len(typed.Parts) == 0 || len(typed.Parts[0].Clauses) == 0 {
		return ast.Clause{}, graph.NewError(graph.ErrKindUnsupported, "unable to build structured projection clause", nil)
	}

	for i := len(typed.Parts[0].Clauses) - 1; i >= 0; i-- {
		clause := typed.Parts[0].Clauses[i]
		if clause.Kind == kind {
			clause.Raw = raw
			return clause, nil
		}
	}

	return ast.Clause{}, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("projection clause kind %s not found", kind), nil)
}

func projectionClauseFromSpec(spec projectionClauseSpec) (ast.ReturnClause, error) {
	items, err := parseProjectionItems(spec.ProjectionRaw)
	if err != nil {
		return ast.ReturnClause{}, err
	}
	out := ast.ReturnClause{Distinct: spec.Distinct, Items: make([]ast.ProjectionItem, 0, len(items)), OrderBy: make([]ast.SortItem, 0, len(spec.OrderBy))}
	for _, item := range items {
		expr := strings.TrimSpace(item.Expression)
		if expr == "" {
			continue
		}
		if expr == "*" {
			out.IncludeAll = true
			continue
		}
		out.Items = append(out.Items, ast.ProjectionItem{Expression: ast.Expression{Raw: expr}, Alias: strings.TrimSpace(item.Alias)})
	}
	for _, order := range spec.OrderBy {
		direction := ast.SortDirectionAsc
		if order.Descending {
			direction = ast.SortDirectionDesc
		}
		out.OrderBy = append(out.OrderBy, ast.SortItem{Expression: ast.Expression{Raw: strings.TrimSpace(order.Expression)}, Direction: direction})
	}
	if raw := strings.TrimSpace(spec.SkipRaw); raw != "" {
		expr := ast.Expression{Raw: raw}
		out.Skip = &expr
	}
	if raw := strings.TrimSpace(spec.LimitRaw); raw != "" {
		expr := ast.Expression{Raw: raw}
		out.Limit = &expr
	}
	return out, nil
}

func buildProjectionScopePrelude(scopeVars []string) string {
	decls := make([]string, 0, len(scopeVars))
	seen := map[string]struct{}{}
	for _, raw := range scopeVars {
		name := strings.TrimSpace(raw)
		if !isIdentifierLike(name) {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		decls = append(decls, "0 AS "+name)
	}
	if len(decls) == 0 {
		return ""
	}
	return "WITH " + strings.Join(decls, ", ") + " "
}

func splitExistsPatternBody(body string) (patternRaw string, whereRaw string, ok bool) {
	body = strings.TrimSpace(body)
	if body == "" || !strings.HasPrefix(body, "(") {
		return "", "", false
	}
	if idx := findTopLevelExistsWhereIndex(body); idx >= 0 {
		patternRaw = strings.TrimSpace(body[:idx])
		whereRaw = strings.TrimSpace(body[idx+len("WHERE"):])
	} else {
		patternRaw = body
		whereRaw = ""
	}
	if patternRaw == "" {
		return "", "", false
	}
	return patternRaw, whereRaw, true
}

func findTopLevelExistsWhereIndex(raw string) int {
	upper := strings.ToUpper(raw)
	keyword := "WHERE"
	depth := 0
	inSingle := false
	inDouble := false
	inBacktick := false
	for i := 0; i <= len(raw)-len(keyword); i++ {
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
			return i
		}
	}
	return -1
}

func (e *Executor) evalWhereRelationshipPatternPredicate(ctx context.Context, tx graph.Tx, raw string, row Row, params Params) (bool, bool, error) {
	if tx == nil {
		return false, false, nil
	}
	patternRaw := strings.TrimSpace(raw)
	if rewritten, ok := rewriteReverseVariableLengthPatternPredicate(patternRaw); ok {
		patternRaw = rewritten
	}
	if !isWhereRelationshipPatternPredicate(patternRaw) {
		return false, false, nil
	}

	if matched, handled, err := e.evalFastWhereRelationshipPatternPredicate(ctx, tx, patternRaw, row, params); handled {
		return matched, true, err
	}

	matches, err := e.applyMatchClause(ctx, tx, []Row{cloneRow(row)}, ast.Clause{Kind: ast.ClauseKindMatch, Raw: "MATCH " + patternRaw}, params)
	if err != nil {
		return false, true, err
	}
	return len(matches) > 0, true, nil
}

func (e *Executor) evalFastWhereRelationshipPatternPredicate(ctx context.Context, tx graph.Tx, patternRaw string, row Row, params Params) (bool, bool, error) {
	tenant, err := requireStringParam(params, "tenant")
	if err != nil {
		return false, true, err
	}

	if pattern, err := parseDirectedRelationshipPattern(patternRaw); err == nil {
		if strings.TrimSpace(pattern.EdgeVar) != "" || strings.TrimSpace(pattern.EdgeProps) != "" || len(pattern.EdgeAnyOf) != 0 {
			return false, false, nil
		}
		src, bound, err := resolveBoundPredicateVertex(ctx, tx, tenant, row, pattern.Left, params)
		if err != nil {
			return false, true, err
		}
		if !bound {
			return false, false, nil
		}
		dst, bound, err := resolveBoundPredicateVertex(ctx, tx, tenant, row, pattern.Right, params)
		if err != nil {
			return false, true, err
		}
		if !bound {
			return false, false, nil
		}
		if src == nil || dst == nil {
			return false, true, nil
		}
		matched, err := relationshipExistsByDirection(ctx, tx, params, tenant, src.ID, dst.ID, strings.TrimSpace(pattern.EdgeType), true)
		return matched, true, err
	}

	if pattern, err := parseReverseDirectedRelationshipPattern(patternRaw); err == nil {
		if strings.TrimSpace(pattern.EdgeVar) != "" || strings.TrimSpace(pattern.EdgeProps) != "" || len(pattern.EdgeAnyOf) != 0 {
			return false, false, nil
		}
		src, bound, err := resolveBoundPredicateVertex(ctx, tx, tenant, row, pattern.Right, params)
		if err != nil {
			return false, true, err
		}
		if !bound {
			return false, false, nil
		}
		dst, bound, err := resolveBoundPredicateVertex(ctx, tx, tenant, row, pattern.Left, params)
		if err != nil {
			return false, true, err
		}
		if !bound {
			return false, false, nil
		}
		if src == nil || dst == nil {
			return false, true, nil
		}
		matched, err := relationshipExistsByDirection(ctx, tx, params, tenant, src.ID, dst.ID, strings.TrimSpace(pattern.EdgeType), true)
		return matched, true, err
	}

	return false, false, nil
}

func resolveBoundPredicateVertex(ctx context.Context, tx graph.Tx, tenant string, row Row, pattern vertexPattern, params Params) (*graph.Vertex, bool, error) {
	varName := strings.TrimSpace(pattern.Var)
	if varName == "" {
		return nil, false, nil
	}

	binding, ok := row[varName]
	if !ok {
		return nil, false, nil
	}
	if binding == nil {
		return nil, true, nil
	}

	var vertex *graph.Vertex
	switch typed := binding.(type) {
	case *graph.Vertex:
		vertex = typed
	case string:
		resolved, err := getVertexQueryCached(ctx, tx, tenant, typed, params)
		if err != nil {
			return nil, true, err
		}
		if resolved == nil {
			return nil, true, nil
		}
		vertex = resolved
	default:
		return nil, false, nil
	}

	if vertex == nil || !vertexPatternMatches(vertex, pattern, params, row) {
		return nil, true, nil
	}

	return vertex, true, nil
}

func relationshipExistsByDirection(ctx context.Context, tx graph.Tx, params Params, tenant, srcID, dstID, edgeType string, forward bool) (bool, error) {
	srcID = strings.TrimSpace(srcID)
	dstID = strings.TrimSpace(dstID)
	if srcID == "" || dstID == "" {
		return false, nil
	}

	cache := ensureWherePatternPredicateCache(params)
	cacheKey := wherePatternPredicateCacheKey(tenant, srcID, edgeType)

	neighbors := cache.outNeighbors[cacheKey]
	if !forward {
		neighbors = cache.inNeighbors[cacheKey]
	}
	if neighbors == nil {
		loaded, err := loadWherePatternPredicateNeighbors(ctx, tx, tenant, srcID, edgeType, forward, params)
		if err != nil {
			return false, err
		}
		neighbors = loaded
		if forward {
			cache.outNeighbors[cacheKey] = neighbors
		} else {
			cache.inNeighbors[cacheKey] = neighbors
		}
	}

	_, ok := neighbors[dstID]
	return ok, nil
}

func ensureWherePatternPredicateCache(params Params) *wherePatternPredicateCache {
	if params == nil {
		return &wherePatternPredicateCache{outNeighbors: map[string]map[string]struct{}{}, inNeighbors: map[string]map[string]struct{}{}}
	}
	if existing, ok := params[wherePatternPredicateCacheParam].(*wherePatternPredicateCache); ok && existing != nil {
		if existing.outNeighbors == nil {
			existing.outNeighbors = map[string]map[string]struct{}{}
		}
		if existing.inNeighbors == nil {
			existing.inNeighbors = map[string]map[string]struct{}{}
		}
		return existing
	}
	cache := &wherePatternPredicateCache{outNeighbors: map[string]map[string]struct{}{}, inNeighbors: map[string]map[string]struct{}{}}
	params[wherePatternPredicateCacheParam] = cache
	return cache
}

func resetWherePatternPredicateCache(params Params) {
	if params == nil {
		return
	}
	delete(params, wherePatternPredicateCacheParam)
}

func ensureQueryEntityCache(params Params) *queryEntityCache {
	if params == nil {
		return &queryEntityCache{vertexes: map[string]*graph.Vertex{}, missing: map[string]struct{}{}}
	}
	if existing, ok := params[queryEntityCacheParam].(*queryEntityCache); ok && existing != nil {
		if existing.vertexes == nil {
			existing.vertexes = map[string]*graph.Vertex{}
		}
		if existing.missing == nil {
			existing.missing = map[string]struct{}{}
		}
		return existing
	}
	cache := &queryEntityCache{vertexes: map[string]*graph.Vertex{}, missing: map[string]struct{}{}}
	params[queryEntityCacheParam] = cache
	return cache
}

func resetQueryEntityCache(params Params) {
	if params == nil {
		return
	}
	delete(params, queryEntityCacheParam)
}

func ensureQueryAdjacencyCache(params Params) *queryAdjacencyCache {
	if params == nil {
		return &queryAdjacencyCache{outEdges: map[string][]*graph.Edge{}, inEdges: map[string][]*graph.Edge{}}
	}
	if existing, ok := params[queryAdjacencyCacheParam].(*queryAdjacencyCache); ok && existing != nil {
		if existing.outEdges == nil {
			existing.outEdges = map[string][]*graph.Edge{}
		}
		if existing.inEdges == nil {
			existing.inEdges = map[string][]*graph.Edge{}
		}
		return existing
	}
	cache := &queryAdjacencyCache{outEdges: map[string][]*graph.Edge{}, inEdges: map[string][]*graph.Edge{}}
	params[queryAdjacencyCacheParam] = cache
	return cache
}

func resetQueryAdjacencyCache(params Params) {
	if params == nil {
		return
	}
	delete(params, queryAdjacencyCacheParam)
}

func queryAdjacencyCacheKey(tenant, vertexID, edgeType string) string {
	return strings.TrimSpace(tenant) + "\x00" + strings.TrimSpace(vertexID) + "\x00" + strings.TrimSpace(edgeType)
}

func cloneEdgeForCache(edge *graph.Edge) *graph.Edge {
	if edge == nil {
		return nil
	}
	cloned := *edge
	if edge.Properties != nil {
		props := make(graph.PropertyMap, len(edge.Properties))
		for key, raw := range edge.Properties {
			copied := make([]byte, len(raw))
			copy(copied, raw)
			props[key] = copied
		}
		cloned.Properties = props
	}
	return &cloned
}

func scanOutEdgesQueryCached(ctx context.Context, tx graph.Tx, tenant, vertexID, edgeType string, params Params, fn func(*graph.Edge) error) error {
	if params == nil {
		return tx.ScanOutEdges(ctx, tenant, vertexID, edgeType, 0, fn)
	}
	cache := ensureQueryAdjacencyCache(params)
	key := queryAdjacencyCacheKey(tenant, vertexID, edgeType)
	edges, ok := cache.outEdges[key]
	if !ok {
		edges = make([]*graph.Edge, 0)
		if err := tx.ScanOutEdges(ctx, tenant, vertexID, edgeType, 0, func(edge *graph.Edge) error {
			if edge != nil {
				edges = append(edges, cloneEdgeForCache(edge))
			}
			return nil
		}); err != nil {
			return err
		}
		cache.outEdges[key] = edges
	}
	for _, edge := range edges {
		if err := fn(edge); err != nil {
			return err
		}
	}
	return nil
}

func scanInEdgesQueryCached(ctx context.Context, tx graph.Tx, tenant, vertexID, edgeType string, params Params, fn func(*graph.Edge) error) error {
	if params == nil {
		return tx.ScanInEdges(ctx, tenant, vertexID, edgeType, 0, fn)
	}
	cache := ensureQueryAdjacencyCache(params)
	key := queryAdjacencyCacheKey(tenant, vertexID, edgeType)
	edges, ok := cache.inEdges[key]
	if !ok {
		edges = make([]*graph.Edge, 0)
		if err := tx.ScanInEdges(ctx, tenant, vertexID, edgeType, 0, func(edge *graph.Edge) error {
			if edge != nil {
				edges = append(edges, cloneEdgeForCache(edge))
			}
			return nil
		}); err != nil {
			return err
		}
		cache.inEdges[key] = edges
	}
	for _, edge := range edges {
		if err := fn(edge); err != nil {
			return err
		}
	}
	return nil
}

func queryEntityCacheKey(tenant, vertexID string) string {
	return strings.TrimSpace(tenant) + "\x00" + strings.TrimSpace(vertexID)
}

func getVertexQueryCached(ctx context.Context, tx graph.Tx, tenant, vertexID string, params Params) (*graph.Vertex, error) {
	vertexID = strings.TrimSpace(vertexID)
	if vertexID == "" {
		return nil, nil
	}
	if params == nil {
		vertex, err := tx.GetVertex(ctx, tenant, vertexID)
		if err != nil {
			if graph.IsKind(err, graph.ErrKindNotFound) {
				return nil, nil
			}
			return nil, err
		}
		return vertex, nil
	}

	cache := ensureQueryEntityCache(params)
	key := queryEntityCacheKey(tenant, vertexID)
	if vertex, ok := cache.vertexes[key]; ok {
		return vertex, nil
	}
	if _, missing := cache.missing[key]; missing {
		return nil, nil
	}

	vertex, err := tx.GetVertex(ctx, tenant, vertexID)
	if err != nil {
		if graph.IsKind(err, graph.ErrKindNotFound) {
			cache.missing[key] = struct{}{}
			return nil, nil
		}
		return nil, err
	}
	cache.vertexes[key] = vertex
	return vertex, nil
}

func wherePatternPredicateCacheKey(tenant, vertexID, edgeType string) string {
	return strings.TrimSpace(tenant) + "\x00" + strings.TrimSpace(vertexID) + "\x00" + strings.TrimSpace(edgeType)
}

func loadWherePatternPredicateNeighbors(ctx context.Context, tx graph.Tx, tenant, vertexID, edgeType string, forward bool, params Params) (map[string]struct{}, error) {
	neighbors := map[string]struct{}{}

	if forward {
		if err := scanOutEdgesQueryCached(ctx, tx, tenant, vertexID, edgeType, params, func(edge *graph.Edge) error {
			if edge != nil {
				neighbors[edge.DstID] = struct{}{}
			}
			return nil
		}); err != nil {
			return nil, err
		}
		return neighbors, nil
	}

	if err := scanInEdgesQueryCached(ctx, tx, tenant, vertexID, edgeType, params, func(edge *graph.Edge) error {
		if edge != nil {
			neighbors[edge.SrcID] = struct{}{}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return neighbors, nil
}

func rewriteReverseVariableLengthPatternPredicate(raw string) (string, bool) {
	m := regexp.MustCompile(`^\(([^()]*)\)<-\[([^\]]*\*)\]-\(([^()]*)\)$`).FindStringSubmatch(raw)
	if len(m) != 4 {
		return "", false
	}
	left := strings.TrimSpace(m[1])
	edge := strings.TrimSpace(m[2])
	right := strings.TrimSpace(m[3])
	return "(" + right + ")-[" + edge + "]->(" + left + ")", true
}

func isWhereRelationshipPatternPredicate(raw string) bool {
	if raw == "" {
		return false
	}
	if _, err := parseDirectedRelationshipPattern(raw); err == nil {
		return true
	}
	if _, err := parseReverseDirectedRelationshipPattern(raw); err == nil {
		return true
	}
	if _, err := parseUndirectedRelationshipPattern(raw); err == nil {
		return true
	}
	if _, err := parseDirectedVariableLengthRelationshipPattern(raw); err == nil {
		return true
	}
	if _, err := parseUndirectedVariableLengthRelationshipPattern(raw); err == nil {
		return true
	}
	if _, err := parseMixedRelationshipChainPattern(raw); err == nil {
		return true
	}
	if _, err := parseTwoHopDirectedChainPattern(raw); err == nil {
		return true
	}
	if _, err := parseTwoHopUndirectedRelationshipChainPattern(raw); err == nil {
		return true
	}
	return false
}

func splitTopLevelKeyword(raw, keyword string) (string, string, bool) {
	upper := strings.ToUpper(raw)
	keyword = strings.ToUpper(keyword)
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
			if keyword == "OR" && i > 0 && strings.EqualFold(raw[i-1:i+len(keyword)], "XOR") {
				continue
			}
			beforeIsWord := i > 0 && isAlphaOrUnderscore(raw[i-1])
			afterIdx := i + len(keyword)
			afterIsWord := afterIdx < len(raw) && isAlphaOrUnderscore(raw[afterIdx])
			if beforeIsWord || afterIsWord {
				// Only allow compact boolean tokens (for example `aORb`) when the
				// token is explicitly uppercase in the raw expression. This avoids
				// splitting identifier substrings like `threat_score` on `or`.
				if raw[i:i+len(keyword)] != keyword {
					continue
				}
				if !shouldSplitCompressedKeyword(raw, i, len(keyword)) {
					continue
				}
			}
			left := strings.TrimSpace(raw[:i])
			right := strings.TrimSpace(raw[i+len(keyword):])
			if left == "" || right == "" {
				continue
			}
			return raw[:i], raw[i+len(keyword):], true
		}
	}
	return raw, "", false
}

func hasLogicalNotPrefix(raw string) bool {
	return len(raw) >= 3 && strings.EqualFold(raw[:3], "NOT")
}

func shouldSplitCompressedKeyword(raw string, idx, kwLen int) bool {
	if idx <= 0 || idx+kwLen >= len(raw) {
		return false
	}
	left := raw[:idx]
	right := raw[idx+kwLen:]
	if left == "" || right == "" {
		return false
	}
	leftHasExprMarker := strings.ContainsAny(left, ".)]}")
	rightHasExprMarker := strings.ContainsAny(right, ".[({$")
	return leftHasExprMarker && rightHasExprMarker
}

func isAlphaOrUnderscore(ch byte) bool {
	if ch == '_' {
		return true
	}
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}

func splitTopLevelComparison(raw string) (string, string, string, bool) {
	op := []string{"<=", ">=", "<>", "=", "<", ">"}
	depth := 0
	depthBracket := 0
	depthBrace := 0
	inSingle := false
	inDouble := false
	upper := strings.ToUpper(raw)
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
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
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
		if depth == 0 && depthBracket == 0 && depthBrace == 0 && strings.HasPrefix(upper[i:], "CASE") {
			if endIdx, ok := findCaseExpressionEnd(raw, i); ok {
				i = endIdx
				continue
			}
		}
		if depth != 0 || depthBracket != 0 || depthBrace != 0 {
			continue
		}
		for _, candidate := range op {
			if strings.HasPrefix(raw[i:], candidate) {
				return strings.TrimSpace(raw[:i]), strings.TrimSpace(raw[i+len(candidate):]), candidate, true
			}
		}
	}
	return "", "", "", false
}

func splitTopLevelComparisonChain(raw string) ([]string, []string, bool) {
	operators := []string{"<=", ">=", "<>", "=", "<", ">"}
	depthParen := 0
	depthBracket := 0
	depthBrace := 0
	inSingle := false
	inDouble := false
	upper := strings.ToUpper(raw)

	parts := make([]string, 0, 4)
	ops := make([]string, 0, 3)
	start := 0

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
		if depthParen == 0 && depthBracket == 0 && depthBrace == 0 && strings.HasPrefix(upper[i:], "CASE") {
			if endIdx, ok := findCaseExpressionEnd(raw, i); ok {
				i = endIdx
				continue
			}
		}
		if depthParen != 0 || depthBracket != 0 || depthBrace != 0 {
			continue
		}

		for _, op := range operators {
			if strings.HasPrefix(raw[i:], op) {
				left := strings.TrimSpace(raw[start:i])
				if left == "" {
					return nil, nil, false
				}
				parts = append(parts, left)
				ops = append(ops, op)
				i += len(op) - 1
				start = i + 1
				break
			}
		}
	}

	if len(ops) < 2 {
		return nil, nil, false
	}
	last := strings.TrimSpace(raw[start:])
	if last == "" {
		return nil, nil, false
	}
	parts = append(parts, last)
	if len(parts) != len(ops)+1 {
		return nil, nil, false
	}
	return parts, ops, true
}

func splitTopLevelLabelPredicate(raw string) (string, []string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil, false
	}
	depthParen := 0
	depthBracket := 0
	depthBrace := 0
	inSingle := false
	inDouble := false
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		switch ch {
		case '\'':
			if !inDouble {
				inSingle = !inSingle
			}
		case '"':
			if !inSingle {
				inDouble = !inDouble
			}
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
			if depthParen != 0 || depthBracket != 0 || depthBrace != 0 {
				continue
			}
			left := strings.TrimSpace(raw[:i])
			right := strings.TrimSpace(raw[i+1:])
			if left == "" || right == "" {
				return "", nil, false
			}
			labels := splitLabels(right)
			if len(labels) == 0 {
				return "", nil, false
			}
			return left, labels, true
		}
	}
	return "", nil, false
}

func evalLabelPredicateExpression(left string, labels []string, row Row, params Params) (any, error) {
	value, err := evalExpressionWithScope(left, row, params)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	switch typed := value.(type) {
	case *graph.Vertex:
		for _, label := range labels {
			if !vertexHasLabel(typed, label) {
				return false, nil
			}
		}
		return true, nil
	case *graph.Edge:
		for _, label := range labels {
			if typed.Type != label {
				return false, nil
			}
		}
		return true, nil
	case map[string]any:
		if relType, ok := typed["type"]; ok {
			current := fmt.Sprint(relType)
			for _, label := range labels {
				if current != label {
					return false, nil
				}
			}
			return true, nil
		}
		labelValue, ok := typed["labels"]
		if !ok {
			return false, nil
		}
		labelSet := map[string]struct{}{}
		switch current := labelValue.(type) {
		case []string:
			for _, label := range current {
				labelSet[label] = struct{}{}
			}
		case []any:
			for _, rawLabel := range current {
				labelSet[fmt.Sprint(rawLabel)] = struct{}{}
			}
		default:
			return false, nil
		}
		for _, label := range labels {
			if _, ok := labelSet[label]; !ok {
				return false, nil
			}
		}
		return true, nil
	default:
		return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("expression %q is not yet supported", left+":"+strings.Join(labels, ":")), nil)
	}
}

func compareWhereValues(lhs, rhs any, op string) (bool, error) {
	value, err := compareExpressionValues(lhs, rhs, op)
	if err != nil {
		return false, err
	}
	return truthyWhereValue(value), nil
}

func compareExpressionValues(lhs, rhs any, op string) (any, error) {
	op = strings.TrimSpace(op)
	if lhs == nil || rhs == nil {
		return nil, nil
	}

	switch op {
	case "=", "<>":
		equal, isNull := cypherNullableEqual(lhs, rhs)
		if isNull {
			return nil, nil
		}
		if op == "=" {
			return equal, nil
		}
		return !equal, nil
	case "<", "<=", ">", ">=":
		if ll, lok := asAnySlice(lhs); lok {
			if rl, rok := asAnySlice(rhs); rok {
				return compareOrderedLists(ll, rl, op), nil
			}
		}
		if lf, lok := comparableNumericValue(lhs); lok {
			if rf, rok := comparableNumericValue(rhs); rok {
				if math.IsNaN(lf) || math.IsNaN(rf) {
					return false, nil
				}
			}
		}
		cmp, ok := compareCypherValues(lhs, rhs)
		if !ok {
			return nil, nil
		}
		sameKind := cypherSortRank(lhs) == cypherSortRank(rhs)
		bothNumeric := isNumericType(lhs) && isNumericType(rhs)
		if !sameKind && !bothNumeric {
			return nil, nil
		}
		switch op {
		case "<":
			return cmp < 0, nil
		case "<=":
			return cmp <= 0, nil
		case ">":
			return cmp > 0, nil
		case ">=":
			return cmp >= 0, nil
		}
	default:
		return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("WHERE operator %q is not supported", op), nil)
	}
	return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("WHERE operator %q is not supported", op), nil)
}

func compareOrderedLists(lhs, rhs []any, op string) any {
	limit := len(lhs)
	if len(rhs) < limit {
		limit = len(rhs)
	}
	for i := 0; i < limit; i++ {
		if lhs[i] == nil || rhs[i] == nil {
			if lhs[i] == nil && rhs[i] == nil {
				continue
			}
			return nil
		}
		cmp, ok := compareCypherValues(lhs[i], rhs[i])
		if !ok {
			return nil
		}
		if cmp != 0 {
			switch op {
			case "<":
				return cmp < 0
			case "<=":
				return cmp < 0
			case ">":
				return cmp > 0
			case ">=":
				return cmp > 0
			}
		}
	}

	cmp := 0
	switch {
	case len(lhs) < len(rhs):
		cmp = -1
	case len(lhs) > len(rhs):
		cmp = 1
	default:
		cmp = 0
	}

	switch op {
	case "<":
		return cmp < 0
	case "<=":
		return cmp <= 0
	case ">":
		return cmp > 0
	case ">=":
		return cmp >= 0
	default:
		return nil
	}
}

func compareExpressionValuesWithRaw(lhs, rhs any, op, leftRaw, rightRaw string) (any, error) {
	op = strings.TrimSpace(op)
	if lhs == nil && rhs == nil && (op == "=" || op == "<>") {
		if shouldTreatDoubleNullAsLogicalEquality(leftRaw, rightRaw) {
			if op == "=" {
				return true, nil
			}
			return false, nil
		}
	}
	return compareExpressionValues(lhs, rhs, op)
}

func shouldTreatDoubleNullAsLogicalEquality(leftRaw, rightRaw string) bool {
	left := strings.ToUpper(strings.TrimSpace(leftRaw))
	right := strings.ToUpper(strings.TrimSpace(rightRaw))
	if left == "NULL" && right == "NULL" {
		return false
	}
	return isCompositeTruthExpression(left) || isCompositeTruthExpression(right)
}

func isCompositeTruthExpression(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}
	for _, marker := range []string{" OR ", " AND ", " XOR ", "NOT ", " IS NULL", " IS NOT NULL", "ISNULL", "ISNOTNULL", " IN "} {
		if strings.Contains(raw, marker) {
			return true
		}
	}
	if strings.ContainsAny(raw, "<>=") {
		return true
	}
	return strings.Contains(raw, "(") || strings.Contains(raw, ")")
}

func cypherNullableEqual(lhs, rhs any) (equal bool, isNull bool) {
	if lhs == nil || rhs == nil {
		return false, true
	}

	if leftMap, leftTemporal := temporalMapValue(lhs); leftTemporal {
		if rightMap, rightTemporal := temporalMapValue(rhs); rightTemporal {
			if result, ok := compareTemporalMaps(leftMap, rightMap, "="); ok {
				return result, false
			}
		}
	}

	if lf, lok := comparableNumericValue(lhs); lok {
		if rf, rok := comparableNumericValue(rhs); rok {
			if li, lokInt := exactIntegerValue(lhs); lokInt {
				if ri, rokInt := exactIntegerValue(rhs); rokInt {
					return li == ri, false
				}
			}
			return lf == rf, false
		}
	}

	if lb, lok := lhs.(bool); lok {
		rb, rok := rhs.(bool)
		if !rok {
			return false, false
		}
		return lb == rb, false
	}

	if ls, lok := lhs.(string); lok {
		rs, rok := rhs.(string)
		if !rok {
			return false, false
		}
		return ls == rs, false
	}

	if ll, lok := asAnySlice(lhs); lok {
		rl, rok := asAnySlice(rhs)
		if !rok {
			return false, false
		}
		if len(ll) != len(rl) {
			return false, false
		}
		unknown := false
		for i := range ll {
			eq, isNull := cypherNullableEqual(ll[i], rl[i])
			if isNull {
				unknown = true
				continue
			}
			if !eq {
				return false, false
			}
		}
		if unknown {
			return false, true
		}
		return true, false
	}

	lm, lok := lhs.(map[string]any)
	rm, rok := rhs.(map[string]any)
	if lok || rok {
		if !lok || !rok {
			return false, false
		}
		if len(lm) != len(rm) {
			return false, false
		}
		unknown := false
		for k, lv := range lm {
			rv, ok := rm[k]
			if !ok {
				return false, false
			}
			eq, isNull := cypherNullableEqual(lv, rv)
			if isNull {
				unknown = true
				continue
			}
			if !eq {
				return false, false
			}
		}
		if unknown {
			return false, true
		}
		return true, false
	}

	if equal, handled := comparePathEquality(lhs, rhs); handled {
		return equal, false
	}

	return reflect.DeepEqual(lhs, rhs), false
}

func comparePathEquality(lhs, rhs any) (bool, bool) {
	leftVertexes, leftEdges, ok := pathValueComponents(lhs)
	if !ok {
		return false, false
	}
	rightVertexes, rightEdges, ok := pathValueComponents(rhs)
	if !ok {
		return false, false
	}
	if len(leftVertexes) != len(rightVertexes) || len(leftEdges) != len(rightEdges) {
		return false, true
	}
	for i := 0; i < len(leftVertexes); i++ {
		if leftVertexes[i] != rightVertexes[i] {
			return false, true
		}
	}
	for i := 0; i < len(leftEdges); i++ {
		if leftEdges[i] != rightEdges[i] {
			return false, true
		}
	}
	return true, true
}

func pathValueComponents(value any) ([]string, []string, bool) {
	vertexID := func(v *graph.Vertex) string {
		if v == nil {
			return ""
		}
		return v.ID
	}
	edgeID := func(e *graph.Edge) string {
		if e == nil {
			return ""
		}
		return e.ID
	}

	switch typed := value.(type) {
	case cypherPathValue:
		vertexes := []string{vertexID(typed.Left)}
		edges := []string{}
		if typed.Edge != nil || typed.Right != nil {
			edges = append(edges, edgeID(typed.Edge))
			vertexes = append(vertexes, vertexID(typed.Right))
		}
		return vertexes, edges, true
	case multiHopCypherPath:
		vertexes := make([]string, 0, len(typed.Vertexes))
		for _, vertex := range typed.Vertexes {
			vertexes = append(vertexes, vertexID(vertex))
		}
		edges := make([]string, 0, len(typed.Edges))
		for _, edge := range typed.Edges {
			edges = append(edges, edgeID(edge))
		}
		return vertexes, edges, true
	default:
		return nil, nil, false
	}
}

func exactIntegerValue(value any) (int64, bool) {
	switch typed := value.(type) {
	case int:
		return int64(typed), true
	case int64:
		return typed, true
	case float32:
		f := float64(typed)
		if math.IsNaN(f) || math.IsInf(f, 0) || math.Trunc(f) != f || f < math.MinInt64 || f > math.MaxInt64 {
			return 0, false
		}
		return int64(f), true
	case float64:
		if math.IsNaN(typed) || math.IsInf(typed, 0) || math.Trunc(typed) != typed || typed < math.MinInt64 || typed > math.MaxInt64 {
			return 0, false
		}
		return int64(typed), true
	case json.Number:
		if i, err := typed.Int64(); err == nil {
			return i, true
		}
		f, err := typed.Float64()
		if err != nil || math.IsNaN(f) || math.IsInf(f, 0) || math.Trunc(f) != f || f < math.MinInt64 || f > math.MaxInt64 {
			return 0, false
		}
		return int64(f), true
	default:
		return 0, false
	}
}

func isNumericType(value any) bool {
	_, ok := comparableNumericValue(value)
	return ok
}

func compareTemporalMaps(lhs, rhs map[string]any, op string) (bool, bool) {
	leftType := strings.ToLower(strings.TrimSpace(fmt.Sprint(lhs["__temporal_type"])))
	rightType := strings.ToLower(strings.TrimSpace(fmt.Sprint(rhs["__temporal_type"])))
	if leftType == "" || rightType == "" {
		return false, false
	}

	if leftType == "duration" && rightType == "duration" {
		leftDur := durationComponentsFromMap(lhs)
		rightDur := durationComponentsFromMap(rhs)
		switch op {
		case "=":
			return durationComponentsEqual(leftDur, rightDur), true
		case "<>":
			return !durationComponentsEqual(leftDur, rightDur), true
		case "<", "<=", ">", ">=":
			return compareDurationComponents(leftDur, rightDur, op), true
		}
		return false, false
	}

	leftInstant, ok1 := coerceDurationInstant(lhs)
	rightInstant, ok2 := coerceDurationInstant(rhs)
	if !ok1 || !ok2 {
		return false, false
	}
	lt, ok1 := durationInstantToTime(leftInstant)
	rt, ok2 := durationInstantToTime(rightInstant)
	if !ok1 || !ok2 {
		return false, false
	}

	switch op {
	case "=":
		return lt.Equal(rt), true
	case "<>":
		return !lt.Equal(rt), true
	case "<":
		return lt.Before(rt), true
	case "<=":
		return lt.Before(rt) || lt.Equal(rt), true
	case ">":
		return lt.After(rt), true
	case ">=":
		return lt.After(rt) || lt.Equal(rt), true
	default:
		return false, false
	}
}

func durationComponentsEqual(left, right durationComponents) bool {
	const epsilon = 1e-9
	return math.Abs(left.months-right.months) < epsilon && math.Abs(left.days-right.days) < epsilon && math.Abs(left.seconds-right.seconds) < epsilon
}

func compareDurationComponents(left, right durationComponents, op string) bool {
	if durationComponentsEqual(left, right) {
		switch op {
		case "<", ">":
			return false
		case "<=", ">=":
			return true
		}
	}
	if left.months != right.months {
		switch op {
		case "<":
			return left.months < right.months
		case "<=":
			return left.months < right.months
		case ">":
			return left.months > right.months
		case ">=":
			return left.months > right.months
		}
	}
	if left.days != right.days {
		switch op {
		case "<":
			return left.days < right.days
		case "<=":
			return left.days < right.days
		case ">":
			return left.days > right.days
		case ">=":
			return left.days > right.days
		}
	}
	switch op {
	case "<":
		return left.seconds < right.seconds
	case "<=":
		return left.seconds <= right.seconds
	case ">":
		return left.seconds > right.seconds
	case ">=":
		return left.seconds >= right.seconds
	default:
		return false
	}
}

func truthyWhereValue(value any) bool {
	switch typed := value.(type) {
	case nil:
		return false
	case bool:
		return typed
	case string:
		return typed != ""
	case int:
		return typed != 0
	case int64:
		return typed != 0
	case float32:
		return typed != 0
	case float64:
		return typed != 0
	default:
		return true
	}
}

func evalBooleanNot(value any) (any, error) {
	b, isNull, err := asNullableBoolean(value)
	if err != nil {
		return nil, err
	}
	if isNull {
		return nil, nil
	}
	return !b, nil
}

func evalBooleanBinary(op string, lhs, rhs any) (any, error) {
	l, lNull, err := asNullableBoolean(lhs)
	if err != nil {
		return nil, err
	}
	r, rNull, err := asNullableBoolean(rhs)
	if err != nil {
		return nil, err
	}

	switch strings.ToUpper(strings.TrimSpace(op)) {
	case "AND":
		if (!lNull && !l) || (!rNull && !r) {
			return false, nil
		}
		if lNull || rNull {
			return nil, nil
		}
		return true, nil
	case "OR":
		if (!lNull && l) || (!rNull && r) {
			return true, nil
		}
		if lNull || rNull {
			return nil, nil
		}
		return false, nil
	case "XOR":
		if lNull || rNull {
			return nil, nil
		}
		return l != r, nil
	default:
		return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("boolean operator %q is not supported", op), nil)
	}
}

func asNullableBoolean(value any) (bool, bool, error) {
	if value == nil {
		return false, true, nil
	}
	b, ok := value.(bool)
	if !ok {
		return false, false, graph.NewError(graph.ErrKindSemantic, "invalid argument type", nil)
	}
	return b, false, nil
}

func parensAreBalanced(raw string) bool {
	depth := 0
	for i := 0; i < len(raw); i++ {
		switch raw[i] {
		case '(', '[', '{':
			depth++
		case ')', ']', '}':
			depth--
			if depth < 0 {
				return false
			}
		}
	}
	return depth == 0
}

func evalUnwindValues(raw string, params Params, row Row) ([]any, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, graph.NewError(graph.ErrKindSemantic, "empty UNWIND expression", nil)
	}
	if strings.HasPrefix(raw, "[") && strings.HasSuffix(raw, "]") {
		inner := strings.TrimSpace(raw[1 : len(raw)-1])
		if inner == "" {
			return []any{}, nil
		}
		parts := splitTopLevelCommaSeparated(inner)
		values := make([]any, 0, len(parts))
		for _, part := range parts {
			part = strings.TrimSpace(part)
			value, err := evalExpressionWithScope(part, row, params)
			if err != nil {
				value, err = evalWriteValue(part, params, row)
			}
			if err != nil {
				return nil, err
			}
			values = append(values, value)
		}
		return values, nil
	}
	if strings.HasPrefix(raw, "$") {
		name := strings.TrimPrefix(raw, "$")
		value, ok := params[name]
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, fmt.Sprintf("missing parameter %q", name), nil)
		}
		if value == nil {
			return []any{}, nil
		}
		return flattenListValue(value)
	}
	if row != nil {
		if value, ok := row[raw]; ok {
			if value == nil {
				return []any{}, nil
			}
			return flattenListValue(value)
		}
	}
	if value, err := evalExpressionWithScope(raw, row, params); err == nil {
		if value == nil {
			return []any{}, nil
		}
		return flattenListValue(value)
	}
	value, err := evalWriteValue(raw, params, row)
	if err == nil {
		if value == nil {
			return []any{}, nil
		}
		return []any{value}, nil
	}
	return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("UNWIND expression %q is not yet supported", raw), nil)
}

func (e *Executor) evalProjectionPatternComprehension(ctx context.Context, tx graph.Tx, raw string, row Row, params Params) (any, bool, error) {
	if tx == nil {
		return nil, false, nil
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, false, nil
	}
	wrapSize := false
	if arg, ok := parseFunctionCall(raw, "size"); ok {
		raw = strings.TrimSpace(arg)
		wrapSize = true
	}

	patternExpr, projectionExpr, ok := parsePatternComprehension(raw)
	if !ok {
		return nil, false, nil
	}
	if strings.TrimSpace(patternExpr) == "" || strings.TrimSpace(projectionExpr) == "" {
		return nil, true, graph.NewError(graph.ErrKindSemantic, "pattern comprehension variables are required", nil)
	}

	matches, err := e.applyMatchClause(ctx, tx, []Row{cloneRow(row)}, ast.Clause{Kind: ast.ClauseKindMatch, Raw: "MATCH " + patternExpr}, params)
	if err != nil {
		return nil, true, err
	}
	out := make([]any, 0)
	for _, matchRow := range matches {
		projected, err := evalExpressionWithScope(projectionExpr, matchRow, params)
		if err != nil {
			if nested, nestedOK, nestedErr := e.evalProjectionPatternComprehension(ctx, tx, projectionExpr, matchRow, params); nestedOK {
				if nestedErr != nil {
					return nil, true, nestedErr
				}
				projected = nested
			} else {
				return nil, true, err
			}
		}
		out = append(out, projected)
	}
	if wrapSize {
		return len(out), true, nil
	}
	return out, true, nil
}

func parsePatternComprehension(raw string) (patternExpr string, projectionExpr string, ok bool) {
	raw = strings.TrimSpace(raw)
	if len(raw) < 2 || raw[0] != '[' || raw[len(raw)-1] != ']' {
		return "", "", false
	}
	body := strings.TrimSpace(raw[1 : len(raw)-1])
	pipeIdx := findTopLevelPipeIndex(body)
	if pipeIdx <= 0 {
		return "", "", false
	}
	left := strings.TrimSpace(body[:pipeIdx])
	projectionExpr = strings.TrimSpace(body[pipeIdx+1:])
	if left == "" || projectionExpr == "" {
		return "", "", false
	}

	eqIdx := findTopLevelEqualsIndex(left)
	if eqIdx >= 0 {
		pathVar := strings.TrimSpace(left[:eqIdx])
		if pathVar == "" || !isIdentifierLike(pathVar) {
			return "", "", false
		}
		patternExpr = strings.TrimSpace(left[eqIdx+1:])
		if !strings.HasPrefix(patternExpr, "(") {
			return "", "", false
		}
		return left, projectionExpr, true
	}

	if !strings.HasPrefix(left, "(") {
		return "", "", false
	}
	return left, projectionExpr, true
}

func findTopLevelEqualsIndex(raw string) int {
	depthParen := 0
	depthBracket := 0
	depthBrace := 0
	inSingle := false
	inDouble := false
	for i := 0; i < len(raw); i++ {
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

func findTopLevelPipeIndex(raw string) int {
	depthParen := 0
	depthBracket := 0
	depthBrace := 0
	inSingle := false
	inDouble := false
	for i := 0; i < len(raw); i++ {
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

func flattenListValue(value any) ([]any, error) {
	if value == nil {
		return []any{nil}, nil
	}
	rv := reflect.ValueOf(value)
	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		out := make([]any, 0, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			out = append(out, rv.Index(i).Interface())
		}
		return out, nil
	}
	return []any{value}, nil
}

func deleteVertexWithEdges(ctx context.Context, tx graph.Tx, tenant, vertexID string, metrics Metrics) error {
	if metrics != nil {
		metrics.ObserveDeleteCounter("vertex_detach_targets", 1)
	}
	type edgeIDScannerTx interface {
		ScanOutEdgeIDs(ctx context.Context, tenant, srcID, edgeType string, limit int, fn func(string) error) error
		ScanInEdgeIDs(ctx context.Context, tenant, dstID, edgeType string, limit int, fn func(string) error) error
	}
	edgeIDs := map[string]struct{}{}
	if scanner, ok := tx.(edgeIDScannerTx); ok {
		if err := scanner.ScanOutEdgeIDs(ctx, tenant, vertexID, "", 0, func(edgeID string) error {
			edgeIDs[edgeID] = struct{}{}
			if metrics != nil {
				metrics.ObserveDeleteCounter("out_edges_scanned", 1)
			}
			return nil
		}); err != nil {
			return err
		}
		if err := scanner.ScanInEdgeIDs(ctx, tenant, vertexID, "", 0, func(edgeID string) error {
			edgeIDs[edgeID] = struct{}{}
			if metrics != nil {
				metrics.ObserveDeleteCounter("in_edges_scanned", 1)
			}
			return nil
		}); err != nil {
			return err
		}
	} else {
		if err := tx.ScanOutEdges(ctx, tenant, vertexID, "", 0, func(edge *graph.Edge) error {
			edgeIDs[edge.ID] = struct{}{}
			if metrics != nil {
				metrics.ObserveDeleteCounter("out_edges_scanned", 1)
			}
			return nil
		}); err != nil {
			return err
		}
		if err := tx.ScanInEdges(ctx, tenant, vertexID, "", 0, func(edge *graph.Edge) error {
			edgeIDs[edge.ID] = struct{}{}
			if metrics != nil {
				metrics.ObserveDeleteCounter("in_edges_scanned", 1)
			}
			return nil
		}); err != nil {
			return err
		}
	}
	for edgeID := range edgeIDs {
		if err := tx.DeleteEdge(ctx, tenant, edgeID); err != nil {
			return err
		}
		if metrics != nil {
			metrics.ObserveDeleteCounter("edges_deleted", 1)
		}
	}
	if err := tx.DeleteVertex(ctx, tenant, vertexID); err != nil {
		return err
	}
	if metrics != nil {
		metrics.ObserveDeleteCounter("vertices_deleted", 1)
	}
	return nil
}

func resolvePatternSourceID(row Row, params Params, varName, paramName string) (string, error) {
	if binding, ok := row[varName]; ok {
		switch typed := binding.(type) {
		case *graph.Vertex:
			return typed.ID, nil
		case string:
			return typed, nil
		}
	}
	return requireStringParam(params, paramName)
}

func (e *Executor) resolveOrCreateVertex(ctx context.Context, tx graph.Tx, tenant string, row Row, varName string, labels []string, props map[string]any, merge bool) (*graph.Vertex, bool, error) {
	if binding, ok := row[varName]; ok {
		if v, ok := binding.(*graph.Vertex); ok {
			return v, false, nil
		}
		if s, ok := binding.(string); ok && s != "" {
			v, err := tx.GetVertex(ctx, tenant, s)
			if err == nil {
				return v, false, nil
			}
		}
	}
	if merge && hasNilPropertyValue(props) {
		return nil, false, graph.NewError(graph.ErrKindSemantic, "MergeReadOwnWrites", nil)
	}

	vertexID := ""
	if strings.TrimSpace(varName) != "" {
		if derivedID, ok := vertexIDFromPatternProperties(props); ok {
			vertexID = derivedID
		}
	}
	if vertexID == "" {
		if merge {
			matches, err := e.findMergeVerticesByPattern(ctx, tx, tenant, labels, props)
			if err != nil {
				return nil, false, err
			}
			if len(matches) > 0 {
				return matches[0], false, nil
			}
		}
		vertexID = nextAutoVertexID(varName)
	}

	vertex, err := tx.GetVertex(ctx, tenant, vertexID)
	if err == nil {
		return vertex, false, nil
	}
	if !graph.IsKind(err, graph.ErrKindNotFound) {
		return nil, false, err
	}

	vertex = &graph.Vertex{Tenant: tenant, ID: vertexID, Labels: labels, Properties: normalizeVertexProperties(props)}
	if err := e.putVertexWithIndexes(ctx, tx, vertex); err != nil {
		return nil, false, err
	}
	return vertex, true, nil
}

func vertexIDFromPatternProperties(props map[string]any) (string, bool) {
	for key, value := range props {
		if !strings.EqualFold(strings.TrimSpace(key), "id") {
			continue
		}
		if value == nil {
			return "", false
		}
		id := strings.TrimSpace(fmt.Sprint(value))
		if id == "" {
			return "", false
		}
		return id, true
	}
	return "", false
}

func nextAutoVertexID(varName string) string {
	n := atomic.AddUint64(&autoVertexIDSeq, 1)
	if strings.TrimSpace(varName) == "" {
		return fmt.Sprintf("auto-v-%d", n)
	}
	return fmt.Sprintf("auto-%s-%d", varName, n)
}

func nextAutoEdgeID(tenant, srcID, edgeType, dstID string) string {
	n := atomic.AddUint64(&autoEdgeIDSeq, 1)
	return fmt.Sprintf("%s|%s|%s|%s|%d", tenant, srcID, edgeType, dstID, n)
}

func parsePropertyMap(raw string, params Params, row Row) (map[string]any, error) {
	out := map[string]any{}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return out, nil
	}
	for _, pair := range splitTopLevelCommaSeparated(raw) {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		parts := strings.SplitN(pair, ":", 2)
		if len(parts) != 2 {
			return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("property pair %q is not supported", pair), nil)
		}
		key := strings.TrimSpace(parts[0])
		valueExpr := strings.TrimSpace(parts[1])
		value, err := evalExpressionWithScope(valueExpr, row, params)
		if err != nil {
			value, err = evalWriteValue(valueExpr, params, row)
		}
		if err != nil {
			if isIdentifierLike(valueExpr) {
				return nil, &parser.ParseError{Kind: parser.ParseErrorUnsupported, Message: "UndefinedVariable"}
			}
			return nil, err
		}
		out[key] = value
	}
	return out, nil
}

func vertexHasAnyEdges(ctx context.Context, tx graph.Tx, tenant, vertexID string) (bool, error) {
	hasEdges := false
	if err := tx.ScanOutEdges(ctx, tenant, vertexID, "", 1, func(edge *graph.Edge) error {
		hasEdges = true
		return nil
	}); err != nil {
		return false, err
	}
	if hasEdges {
		return true, nil
	}
	if err := tx.ScanInEdges(ctx, tenant, vertexID, "", 1, func(edge *graph.Edge) error {
		hasEdges = true
		return nil
	}); err != nil {
		return false, err
	}
	return hasEdges, nil
}

func evalWriteValue(raw string, params Params, row Row) (any, error) {
	raw = strings.TrimSpace(raw)
	if strings.EqualFold(raw, "null") {
		return nil, nil
	}
	if strings.EqualFold(raw, "true") {
		return true, nil
	}
	if strings.EqualFold(raw, "false") {
		return false, nil
	}
	if arg, ok := parseFunctionCall(raw, "date"); ok {
		return evalTemporalConstructor("date", arg, params, row)
	}
	if arg, ok := parseFunctionCall(raw, "time"); ok {
		return evalTemporalConstructor("time", arg, params, row)
	}
	if arg, ok := parseFunctionCall(raw, "zoned_time"); ok {
		return evalTemporalConstructor("time", arg, params, row)
	}
	if arg, ok := parseFunctionCall(raw, "datetime"); ok {
		return evalTemporalConstructor("datetime", arg, params, row)
	}
	if arg, ok := parseFunctionCall(raw, "zoned_datetime"); ok {
		return evalTemporalConstructor("datetime", arg, params, row)
	}
	if arg, ok := parseFunctionCall(raw, "localtime"); ok {
		return evalTemporalConstructor("localtime", arg, params, row)
	}
	if arg, ok := parseFunctionCall(raw, "local_time"); ok {
		return evalTemporalConstructor("localtime", arg, params, row)
	}
	if arg, ok := parseFunctionCall(raw, "localdatetime"); ok {
		return evalTemporalConstructor("localdatetime", arg, params, row)
	}
	if arg, ok := parseFunctionCall(raw, "local_datetime"); ok {
		return evalTemporalConstructor("localdatetime", arg, params, row)
	}
	if arg, ok := parseFunctionCall(raw, "duration"); ok {
		return evalTemporalConstructor("duration", arg, params, row)
	}
	if strings.HasPrefix(raw, "$") {
		name := strings.TrimPrefix(raw, "$")
		v, ok := params[name]
		if !ok {
			return nil, graph.NewError(graph.ErrKindInvalidInput, fmt.Sprintf("missing parameter %q", name), nil)
		}
		return v, nil
	}
	if strings.HasPrefix(raw, "[") && strings.HasSuffix(raw, "]") {
		return parseListLiteral(raw, params, row)
	}
	if strings.HasPrefix(raw, "{") && strings.HasSuffix(raw, "}") {
		return parseInlineMapLiteral(raw, params, row)
	}
	if row != nil {
		if v, ok := row[raw]; ok {
			return v, nil
		}
	}
	if strings.HasPrefix(raw, "'") || strings.HasPrefix(raw, `"`) {
		unquoted, err := unquoteCypherString(raw)
		if err != nil {
			return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("string literal %q is not supported", raw), err)
		}
		return unquoted, nil
	}
	if raw == "true" || raw == "false" {
		return raw == "true", nil
	}
	if value, ok, err := parseHexOrOctalIntegerLiteral(raw); ok {
		if err != nil {
			return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("write value %q is not supported", raw), err)
		}
		return value, nil
	}
	if n, err := strconv.Atoi(raw); err == nil {
		return n, nil
	}
	if f, err := strconv.ParseFloat(raw, 64); err == nil {
		return json.Number(formatFloatResult(f)), nil
	}
	return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("write value %q is not supported", raw), nil)
}

func parseHexOrOctalIntegerLiteral(raw string) (int, bool, error) {
	if raw == "" {
		return 0, false, nil
	}
	negative := false
	unsigned := raw
	if strings.HasPrefix(unsigned, "+") {
		unsigned = unsigned[1:]
	} else if strings.HasPrefix(unsigned, "-") {
		negative = true
		unsigned = unsigned[1:]
	}
	if len(unsigned) < 3 || unsigned[0] != '0' {
		return 0, false, nil
	}
	base := 0
	switch unsigned[1] {
	case 'x', 'X':
		base = 16
	case 'o', 'O':
		base = 8
	default:
		return 0, false, nil
	}

	digits := unsigned[2:]
	if digits == "" {
		return 0, true, fmt.Errorf("missing integer literal digits")
	}

	parsed, err := strconv.ParseUint(digits, base, 64)
	if err != nil {
		return 0, true, err
	}

	if negative {
		const minIntAbs = uint64(1) << 63
		if parsed > minIntAbs {
			return 0, true, fmt.Errorf("integer overflow")
		}
		if parsed == minIntAbs {
			return int(math.MinInt64), true, nil
		}
		return int(-int64(parsed)), true, nil
	}

	if parsed > math.MaxInt64 {
		return 0, true, fmt.Errorf("integer overflow")
	}
	return int(parsed), true, nil
}

func unwrapOuterParentheses(raw string) (string, bool) {
	if len(raw) < 2 || raw[0] != '(' || raw[len(raw)-1] != ')' {
		return "", false
	}
	depth := 0
	inSingle := false
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		if ch == '\'' && (i == 0 || raw[i-1] != '\\') {
			inSingle = !inSingle
			continue
		}
		if inSingle {
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 && i < len(raw)-1 {
				return "", false
			}
			if depth < 0 {
				return "", false
			}
		}
	}
	if depth != 0 {
		return "", false
	}
	inner := strings.TrimSpace(raw[1 : len(raw)-1])
	if inner == "" {
		return "", false
	}
	return inner, true
}

func parseListLiteral(raw string, params Params, row Row) ([]any, error) {
	body := strings.TrimSpace(raw[1 : len(raw)-1])
	if body == "" {
		return []any{}, nil
	}
	parts := splitTopLevelCommaSeparated(body)
	out := make([]any, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		var (
			value any
			err   error
		)
		if isQuotedCypherString(part) {
			value, err = evalWriteValue(part, params, row)
		} else {
			value, err = evalExpressionWithScope(part, row, params)
			if err != nil {
				value, err = evalWriteValue(part, params, row)
			}
		}
		if err != nil {
			return nil, err
		}
		out = append(out, value)
	}
	return out, nil
}

func isQuotedCypherString(raw string) bool {
	if len(raw) < 2 {
		return false
	}
	first := raw[0]
	last := raw[len(raw)-1]
	if first != last {
		return false
	}
	return first == '\'' || first == '"'
}

func parseInlineMapLiteral(raw string, params Params, row Row) (map[string]any, error) {
	body := strings.TrimSpace(raw[1 : len(raw)-1])
	if body == "" {
		return map[string]any{}, nil
	}
	out := map[string]any{}
	for _, pair := range splitTopLevelCommaSeparated(body) {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		parts := strings.SplitN(pair, ":", 2)
		if len(parts) != 2 {
			return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("property pair %q is not supported", pair), nil)
		}
		key := strings.TrimSpace(parts[0])
		valueExpr := strings.TrimSpace(parts[1])
		value, err := evalExpressionWithScope(valueExpr, row, params)
		if err != nil {
			value, err = evalWriteValue(valueExpr, params, row)
		}
		if err != nil {
			return nil, err
		}
		out[key] = value
	}
	return out, nil
}

func evalTemporalConstructor(name, arg string, params Params, row Row) (any, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return map[string]any{"__temporal_type": name}, nil
	}
	value, err := evalExpressionWithScope(arg, row, params)
	if err != nil {
		value, err = evalWriteValue(arg, params, row)
	}
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	out := map[string]any{"__temporal_type": name}
	switch typed := value.(type) {
	case map[string]any:
		normalized, normErr := normalizeTemporalConstructorMap(name, typed)
		if normErr != nil {
			return nil, normErr
		}
		for key, v := range normalized {
			out[key] = v
		}
		out["__temporal_type"] = name
	case string:
		if parsed, ok := parseTemporalLiteralToMap(name, typed); ok {
			for key, v := range parsed {
				out[key] = v
			}
		} else {
			out["value"] = typed
		}
	default:
		out["value"] = typed
	}
	return out, nil
}

func normalizeTemporalConstructorMap(name string, in map[string]any) (map[string]any, error) {
	typeName := strings.ToLower(strings.TrimSpace(name))
	out := map[string]any{}
	for k, v := range in {
		out[k] = v
	}

	if typeName == "duration" {
		return out, nil
	}

	if _, hasDate := out["date"]; !hasDate {
		if embeddedDateTime, ok := out["datetime"]; ok {
			out["date"] = embeddedDateTime
		}
	}

	if embeddedDate, ok := parseEmbeddedDate(out["date"]); ok {
		if _, ok := out["year"]; !ok {
			out["year"] = embeddedDate.Year()
		}
		if _, ok := out["month"]; !ok {
			out["month"] = int(embeddedDate.Month())
		}
		if _, ok := out["day"]; !ok {
			out["day"] = embeddedDate.Day()
		}
	}

	if typeName == "localtime" || typeName == "time" || typeName == "localdatetime" || typeName == "datetime" {
		timeSource := out["time"]
		sourceTZ := ""
		if timeSource == nil {
			timeSource = out["datetime"]
		}
		if h, m, s, n, tz, ok := parseEmbeddedTime(timeSource); ok {
			sourceTZ = tz
			if sourceMap, ok := temporalMapValue(timeSource); ok {
				sourceType := strings.ToLower(strings.TrimSpace(fmt.Sprint(sourceMap["__temporal_type"])))
				if sourceType != "time" && sourceType != "datetime" {
					sourceTZ = ""
				}
			}
			if _, exists := out["hour"]; !exists {
				out["hour"] = h
			}
			if _, exists := out["minute"]; !exists {
				out["minute"] = m
			}
			if _, exists := out["second"]; !exists {
				out["second"] = s
			}
			if _, exists := out["nanosecond"]; !exists {
				out["nanosecond"] = n
			}
			if tz != "" {
				if _, exists := out["timezone"]; !exists {
					out["timezone"] = tz
				}
			}
		}

		if typeName == "time" || typeName == "datetime" {
			targetTZ := strings.TrimSpace(fmt.Sprint(out["timezone"]))
			if sourceTZ != "" && targetTZ != "" && sourceTZ != targetTZ {
				year, month, day := 1970, 1, 1
				if typeName == "datetime" {
					if y, mo, d, ok := resolveDateFromTemporalMap(out); ok {
						year, month, day = y, mo, d
					}
				}
				hour, _ := mapInt(out, "hour")
				minute, _ := mapInt(out, "minute")
				second, _ := mapInt(out, "second")
				nano := combineNanoseconds(out)
				if converted, ok := convertTemporalClockTimezone(year, month, day, hour, minute, second, nano, sourceTZ, targetTZ); ok {
					out["hour"] = converted.Hour()
					out["minute"] = converted.Minute()
					out["second"] = converted.Second()
					out["nanosecond"] = converted.Nanosecond()
					if typeName == "datetime" {
						out["year"] = converted.Year()
						out["month"] = int(converted.Month())
						out["day"] = converted.Day()
					}
				}
			}
		}
	}

	if typeName == "date" || typeName == "localdatetime" || typeName == "datetime" {
		y, m, d, ok := resolveDateFromTemporalMap(out)
		if ok {
			out["year"] = y
			out["month"] = m
			out["day"] = d
		}
	}

	if typeName == "localtime" || typeName == "time" || typeName == "localdatetime" || typeName == "datetime" {
		hour, _ := mapInt(out, "hour")
		minute, _ := mapInt(out, "minute")
		second, _ := mapInt(out, "second")
		nano := combineNanoseconds(out)
		out["hour"] = hour
		out["minute"] = minute
		out["second"] = second
		out["nanosecond"] = nano
		delete(out, "microsecond")
		delete(out, "millisecond")
	}

	if typeName == "time" || typeName == "datetime" {
		tz := strings.TrimSpace(fmt.Sprint(out["timezone"]))
		if tz == "" {
			out["timezone"] = "Z"
		}
	}

	return out, nil
}

func resolveDateFromTemporalMap(in map[string]any) (int, int, int, bool) {
	if y, ord, ok := yearAndOrdinal(in); ok {
		base := time.Date(y, 1, 1, 0, 0, 0, 0, time.UTC)
		resolved := base.AddDate(0, 0, ord-1)
		return resolved.Year(), int(resolved.Month()), resolved.Day(), true
	}
	if y, q, doq, ok := yearQuarterDayOfQuarter(in); ok {
		month := (q-1)*3 + 1
		base := time.Date(y, time.Month(month), 1, 0, 0, 0, 0, time.UTC)
		resolved := base.AddDate(0, 0, doq-1)
		return resolved.Year(), int(resolved.Month()), resolved.Day(), true
	}
	if week, ok := mapInt(in, "week"); ok {
		weekYear, hasWeekYear := mapInt(in, "year")
		baseDate, hasBaseDate := parseEmbeddedDate(in["date"])
		if !hasWeekYear {
			if hasBaseDate {
				isoYear, _ := baseDate.ISOWeek()
				weekYear = isoYear
				hasWeekYear = true
			}
		}
		if hasWeekYear {
			dayOfWeek, hasDOW := mapInt(in, "dayOfWeek")
			if !hasDOW {
				if hasBaseDate {
					wd := int(baseDate.Weekday())
					if wd == 0 {
						wd = 7
					}
					dayOfWeek = wd
				} else {
					dayOfWeek = 1
				}
			}
			if resolved, ok := isoWeekDate(weekYear, week, dayOfWeek); ok {
				return resolved.Year(), int(resolved.Month()), resolved.Day(), true
			}
		}
	}
	if y, m, d, ok := directYMD(in); ok {
		return y, m, d, true
	}
	if y, ok := mapInt(in, "year"); ok {
		return y, 1, 1, true
	}
	if embedded, ok := parseEmbeddedDate(in["date"]); ok {
		return embedded.Year(), int(embedded.Month()), embedded.Day(), true
	}
	return 0, 0, 0, false
}

func directYMD(in map[string]any) (int, int, int, bool) {
	y, yOK := mapInt(in, "year")
	m, mOK := mapInt(in, "month")
	if yOK && mOK {
		d, dOK := mapInt(in, "day")
		if !dOK {
			d = 1
		}
		return y, m, d, true
	}
	return 0, 0, 0, false
}

func yearAndOrdinal(in map[string]any) (int, int, bool) {
	y, yOK := mapInt(in, "year")
	ord, ordOK := mapInt(in, "ordinalDay")
	if !yOK || !ordOK {
		return 0, 0, false
	}
	return y, ord, true
}

func yearQuarterDayOfQuarter(in map[string]any) (int, int, int, bool) {
	y, yOK := mapInt(in, "year")
	q, qOK := mapInt(in, "quarter")
	if !yOK || !qOK {
		return 0, 0, 0, false
	}
	doq, doqOK := mapInt(in, "dayOfQuarter")
	if !doqOK {
		if m, mOK := mapInt(in, "month"); mOK {
			if d, dOK := mapInt(in, "day"); dOK {
				base := time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC)
				qStartMonth := ((int(base.Month())-1)/3)*3 + 1
				qStart := time.Date(base.Year(), time.Month(qStartMonth), 1, 0, 0, 0, 0, time.UTC)
				doq = int(base.Sub(qStart).Hours()/24) + 1
				doqOK = true
			}
		}
	}
	if !doqOK {
		doq = 1
	}
	return y, q, doq, true
}

func parseEmbeddedDate(raw any) (time.Time, bool) {
	switch typed := raw.(type) {
	case map[string]any:
		if y, m, d, ok := resolveDateFromTemporalMap(typed); ok {
			return time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC), true
		}
		if v, ok := typed["value"]; ok {
			if s := strings.TrimSpace(fmt.Sprint(v)); s != "" {
				if idx := strings.IndexAny(s, "Tt"); idx > 0 {
					s = strings.TrimSpace(s[:idx])
				}
				if t, err := time.Parse("2006-01-02", s); err == nil {
					return t, true
				}
			}
		}
	case string:
		s := strings.TrimSpace(typed)
		if s == "" {
			return time.Time{}, false
		}
		if idx := strings.IndexAny(s, "Tt"); idx > 0 {
			s = strings.TrimSpace(s[:idx])
		}
		if t, err := time.Parse("2006-01-02", s); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

func parseEmbeddedTime(raw any) (int, int, int, int, string, bool) {
	switch typed := raw.(type) {
	case map[string]any:
		mapped := typed
		if _, hasTemporal := mapped["__temporal_type"]; !hasTemporal {
			if v, ok := mapped["value"]; ok {
				if parsed, ok := parseTemporalLiteralToMap("time", fmt.Sprint(v)); ok {
					mapped = parsed
				}
			}
		}
		hour, hasHour := mapInt(mapped, "hour")
		minute, hasMinute := mapInt(mapped, "minute")
		second, _ := mapInt(mapped, "second")
		nano := combineNanoseconds(mapped)
		tz := ""
		if tzRaw, ok := mapped["timezone"]; ok {
			tz = strings.TrimSpace(fmt.Sprint(tzRaw))
		}
		if hasHour || hasMinute || second != 0 || nano != 0 {
			return hour, minute, second, nano, tz, true
		}
		if valueRaw, ok := mapped["value"]; ok {
			if parsed, ok := parseTemporalLiteralToMap("time", fmt.Sprint(valueRaw)); ok {
				hour, _ = mapInt(parsed, "hour")
				minute, _ = mapInt(parsed, "minute")
				second, _ = mapInt(parsed, "second")
				nano = combineNanoseconds(parsed)
				tz = ""
				if tzRaw, ok := parsed["timezone"]; ok {
					tz = strings.TrimSpace(fmt.Sprint(tzRaw))
				}
				return hour, minute, second, nano, tz, true
			}
			if parsed, ok := parseTemporalLiteralToMap("localtime", fmt.Sprint(valueRaw)); ok {
				hour, _ = mapInt(parsed, "hour")
				minute, _ = mapInt(parsed, "minute")
				second, _ = mapInt(parsed, "second")
				nano = combineNanoseconds(parsed)
				return hour, minute, second, nano, "", true
			}
		}
	case string:
		if parsed, ok := parseTemporalLiteralToMap("time", typed); ok {
			h, _ := mapInt(parsed, "hour")
			m, _ := mapInt(parsed, "minute")
			s, _ := mapInt(parsed, "second")
			n := combineNanoseconds(parsed)
			tz := ""
			if tzRaw, ok := parsed["timezone"]; ok {
				tz = strings.TrimSpace(fmt.Sprint(tzRaw))
			}
			return h, m, s, n, tz, true
		}
		if parsed, ok := parseTemporalLiteralToMap("localtime", typed); ok {
			h, _ := mapInt(parsed, "hour")
			m, _ := mapInt(parsed, "minute")
			s, _ := mapInt(parsed, "second")
			n := combineNanoseconds(parsed)
			return h, m, s, n, "", true
		}
	}
	return 0, 0, 0, 0, "", false
}

func convertTemporalClockTimezone(year, month, day, hour, minute, second, nanosecond int, sourceTZ, targetTZ string) (time.Time, bool) {
	srcLoc := time.UTC
	if off, err := parseOffsetSeconds(sourceTZ); err == nil {
		srcLoc = time.FixedZone("", off)
	} else if l, err := time.LoadLocation(sourceTZ); err == nil {
		srcLoc = l
	}
	dstLoc := time.UTC
	if off, err := parseOffsetSeconds(targetTZ); err == nil {
		dstLoc = time.FixedZone("", off)
	} else if l, err := time.LoadLocation(targetTZ); err == nil {
		dstLoc = l
	}
	src := time.Date(year, time.Month(month), day, hour, minute, second, nanosecond, srcLoc)
	return src.In(dstLoc), true
}

func combineNanoseconds(in map[string]any) int {
	nano, _ := mapInt(in, "nanosecond")
	micro, _ := mapInt(in, "microsecond")
	milli, _ := mapInt(in, "millisecond")
	total := nano + micro*1_000 + milli*1_000_000
	if total < 0 {
		return 0
	}
	if total >= 1_000_000_000 {
		total = total % 1_000_000_000
	}
	return total
}

func evalTemporalTruncateFunction(namespace string, argList string, row Row, params Params) (any, error) {
	args := splitTopLevelCommaSeparated(argList)
	if len(args) < 2 {
		return nil, graph.NewError(graph.ErrKindSemantic, "truncate() requires at least 2 arguments", nil)
	}
	unit, err := evalWriteValue(args[0], params, row)
	if err != nil {
		return nil, err
	}
	targetExpr := strings.TrimSpace(args[1])
	target, err := evalExpressionWithScope(targetExpr, row, params)
	if err != nil {
		target, err = evalWriteValue(targetExpr, params, row)
		if err != nil {
			return nil, err
		}
	}
	if target == nil {
		return nil, nil
	}
	if mapped, ok := target.(map[string]any); ok {
		out := map[string]any{}
		for key, value := range mapped {
			out[key] = value
		}
		if namespace != "" {
			out["__temporal_type"] = namespace
		}
		out["truncated"] = fmt.Sprint(unit)
		if len(args) >= 3 {
			overrideExpr := strings.TrimSpace(args[2])
			overrideValue, err := evalExpressionWithScope(overrideExpr, row, params)
			if err != nil {
				overrideValue, err = evalWriteValue(overrideExpr, params, row)
				if err != nil {
					return nil, err
				}
			}
			if overrideValue == nil {
				return nil, nil
			}
			if overrideMap, ok := overrideValue.(map[string]any); ok {
				out["truncate_overrides"] = overrideMap
			}
		}
		return out, nil
	}
	return target, nil
}

func evalTemporalNamespaceFunction(raw string, row Row, params Params) (any, bool, error) {
	idx := strings.Index(raw, "(")
	if idx <= 0 || !strings.HasSuffix(raw, ")") {
		return nil, false, nil
	}
	funcName := strings.TrimSpace(raw[:idx])
	if !strings.Contains(funcName, ".") {
		return nil, false, nil
	}
	parts := strings.SplitN(funcName, ".", 2)
	if len(parts) != 2 {
		return nil, false, nil
	}
	namespace := strings.ToLower(strings.TrimSpace(parts[0]))
	method := strings.TrimSpace(parts[1])
	switch namespace {
	case "date", "time", "datetime", "localtime", "localdatetime", "duration":
	default:
		return nil, false, nil
	}

	argsRaw := strings.TrimSpace(raw[idx+1 : len(raw)-1])
	if strings.EqualFold(method, "truncate") {
		value, err := evalTemporalTruncateFunction(namespace, argsRaw, row, params)
		return value, true, err
	}

	argExprs := []string{}
	if argsRaw != "" {
		argExprs = splitTopLevelCommaSeparated(argsRaw)
	}
	args := make([]any, 0, len(argExprs))
	for _, argExpr := range argExprs {
		argExpr = strings.TrimSpace(argExpr)
		value, err := evalExpressionWithScope(argExpr, row, params)
		if err != nil {
			value, err = evalWriteValue(argExpr, params, row)
			if err != nil {
				return nil, true, err
			}
		}
		args = append(args, value)
	}

	for _, arg := range args {
		if arg == nil {
			return nil, true, nil
		}
	}

	if namespace != "duration" {
		if strings.EqualFold(method, "transaction") || strings.EqualFold(method, "statement") || strings.EqualFold(method, "realtime") {
			if len(args) == 0 {
				return map[string]any{"__temporal_type": namespace}, true, nil
			}
			if len(args) == 1 {
				value, err := temporalFromConstructedValue(namespace, args[0])
				return value, true, err
			}
		}
	}

	if namespace == "duration" && (strings.EqualFold(method, "indays") || strings.EqualFold(method, "inmonths") || strings.EqualFold(method, "inseconds") || strings.EqualFold(method, "between")) {
		value, err := evalDurationMethod(method, args)
		return value, true, err
	}

	if namespace == "datetime" {
		switch strings.ToLower(strings.TrimSpace(method)) {
		case "fromepoch":
			if len(args) < 1 || len(args) > 2 {
				return nil, true, graph.NewError(graph.ErrKindSemantic, "datetime.fromepoch requires 1 or 2 arguments", nil)
			}
			seconds, ok := numericValue(args[0])
			if !ok {
				return nil, true, graph.NewError(graph.ErrKindInvalidInput, "datetime.fromepoch requires numeric seconds", nil)
			}
			nanos := 0.0
			if len(args) == 2 {
				if v, ok := numericValue(args[1]); ok {
					nanos = v
				} else {
					return nil, true, graph.NewError(graph.ErrKindInvalidInput, "datetime.fromepoch requires numeric nanoseconds", nil)
				}
			}
			t := time.Unix(int64(seconds), int64(nanos)).UTC()
			return map[string]any{
				"__temporal_type": "datetime",
				"year":            t.Year(),
				"month":           int(t.Month()),
				"day":             t.Day(),
				"hour":            t.Hour(),
				"minute":          t.Minute(),
				"second":          t.Second(),
				"nanosecond":      t.Nanosecond(),
				"timezone":        "Z",
			}, true, nil
		case "fromepochmillis":
			if len(args) != 1 {
				return nil, true, graph.NewError(graph.ErrKindSemantic, "datetime.fromepochmillis requires 1 argument", nil)
			}
			millis, ok := numericValue(args[0])
			if !ok {
				return nil, true, graph.NewError(graph.ErrKindInvalidInput, "datetime.fromepochmillis requires numeric milliseconds", nil)
			}
			t := time.Unix(0, int64(millis*1_000_000)).UTC()
			return map[string]any{
				"__temporal_type": "datetime",
				"year":            t.Year(),
				"month":           int(t.Month()),
				"day":             t.Day(),
				"hour":            t.Hour(),
				"minute":          t.Minute(),
				"second":          t.Second(),
				"nanosecond":      t.Nanosecond(),
				"timezone":        "Z",
			}, true, nil
		}
	}

	return map[string]any{"__temporal_type": namespace, "method": method, "args": args}, true, nil
}

func temporalFromConstructedValue(name string, value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	out := map[string]any{"__temporal_type": name}
	switch typed := value.(type) {
	case map[string]any:
		normalized, normErr := normalizeTemporalConstructorMap(name, typed)
		if normErr != nil {
			return nil, normErr
		}
		for key, v := range normalized {
			out[key] = v
		}
	case string:
		if parsed, ok := parseTemporalLiteralToMap(name, typed); ok {
			for key, v := range parsed {
				out[key] = v
			}
		} else {
			out["value"] = typed
		}
	default:
		out["value"] = typed
	}
	return out, nil
}

type durationInstant struct {
	kind     string
	year     int
	month    int
	day      int
	hour     int
	minute   int
	second   int
	nano     int
	timezone string
	hasDate  bool
	hasTime  bool
	hasZone  bool
}

type durationClock struct {
	secondOfDay float64
	hasZone     bool
	offset      int
}

func evalDurationMethod(method string, args []any) (any, error) {
	if len(args) < 2 {
		return nil, graph.NewError(graph.ErrKindSemantic, "duration method requires 2 arguments", nil)
	}
	left, ok := coerceDurationInstant(args[0])
	if !ok {
		return map[string]any{"__temporal_type": "duration"}, nil
	}
	right, ok := coerceDurationInstant(args[1])
	if !ok {
		return map[string]any{"__temporal_type": "duration"}, nil
	}

	// Mixed zoned/local values are interpreted in the zoned operand's zone.
	if left.hasZone != right.hasZone {
		if left.hasZone {
			right.timezone = left.timezone
			right.hasZone = true
		} else {
			left.timezone = right.timezone
			left.hasZone = true
		}
	}

	methodKey := strings.ToLower(strings.TrimSpace(method))
	if methodKey == "inseconds" || methodKey == "between" {
		if left.hasDate && !right.hasDate {
			right.year, right.month, right.day = left.year, left.month, left.day
			right.hasDate = true
		}
		if right.hasDate && !left.hasDate {
			left.year, left.month, left.day = right.year, right.month, right.day
			left.hasDate = true
		}
	}

	if methodKey == "inseconds" && !left.hasDate && !right.hasDate {
		lClock := durationInstantClock(left)
		rClock := durationInstantClock(right)
		delta := rClock.secondOfDay - lClock.secondOfDay
		if lClock.hasZone && rClock.hasZone {
			delta += float64(lClock.offset - rClock.offset)
		}
		result := map[string]any{"__temporal_type": "duration"}
		setDurationFields(result, durationComponents{seconds: delta})
		return result, nil
	}

	if methodKey == "between" && !left.hasDate && !right.hasDate {
		lClock := durationInstantClock(left)
		rClock := durationInstantClock(right)
		delta := rClock.secondOfDay - lClock.secondOfDay
		if lClock.hasZone && rClock.hasZone {
			delta += float64(lClock.offset - rClock.offset)
		}
		result := map[string]any{"__temporal_type": "duration"}
		setDurationFields(result, durationComponents{seconds: delta})
		return result, nil
	}

	if methodKey == "inseconds" {
		if whole, nanos, ok := durationSecondsBetweenExact(left, right); ok {
			result := map[string]any{"__temporal_type": "duration"}
			result["years"] = 0
			result["months"] = 0
			result["days"] = 0
			result["seconds"] = whole
			result["nanoseconds"] = nanos
			result["nanosecondsOfSecond"] = nanos
			result["__duration_exact"] = true
			return result, nil
		}
		if secs, ok := durationSecondsBetweenWithoutTimeDateOverflow(left, right); ok {
			result := map[string]any{"__temporal_type": "duration"}
			setDurationFields(result, durationComponents{seconds: secs})
			return result, nil
		}
	}

	t1, ok1 := durationInstantToTime(left)
	t2, ok2 := durationInstantToTime(right)
	if !ok1 || !ok2 {
		return map[string]any{"__temporal_type": "duration"}, nil
	}

	var dur durationComponents
	switch methodKey {
	case "inseconds":
		dur = durationComponents{seconds: t2.Sub(t1).Seconds()}
	case "indays":
		if !(left.hasDate && right.hasDate) {
			dur = durationComponents{}
			break
		}
		dur = durationComponents{days: truncTowardZero(t2.Sub(t1).Hours() / 24)}
	case "inmonths":
		if !(left.hasDate && right.hasDate) {
			dur = durationComponents{}
			break
		}
		months := (right.year-left.year)*12 + (right.month - left.month)
		anchor := t1.AddDate(0, months, 0)
		if months > 0 && anchor.After(t2) {
			months--
		}
		if months < 0 && anchor.Before(t2) {
			months++
		}
		dur = durationComponents{months: float64(months)}
	case "between":
		if left.hasDate && right.hasDate {
			months := (right.year-left.year)*12 + (right.month - left.month)
			anchor := t1.AddDate(0, months, 0)
			if months > 0 && anchor.After(t2) {
				months--
				anchor = t1.AddDate(0, months, 0)
			}
			if months < 0 && anchor.Before(t2) {
				months++
				anchor = t1.AddDate(0, months, 0)
			}
			days := int(truncTowardZero(t2.Sub(anchor).Hours() / 24))
			anchor = anchor.AddDate(0, 0, days)
			dur = durationComponents{
				months:  float64(months),
				days:    float64(days),
				seconds: t2.Sub(anchor).Seconds(),
			}
		} else {
			dur = durationComponents{seconds: t2.Sub(t1).Seconds()}
		}
	default:
		return map[string]any{"__temporal_type": "duration"}, nil
	}

	result := map[string]any{"__temporal_type": "duration"}
	setDurationFields(result, dur)
	return result, nil
}

func setDurationFields(out map[string]any, dur durationComponents) {
	totalMonths := int(truncTowardZero(dur.months))
	years := int(truncTowardZero(float64(totalMonths) / 12))
	months := totalMonths - years*12

	days := int(truncTowardZero(dur.days))
	secondsWhole, nanos := splitSecondsAndNanoseconds(dur.seconds)

	out["years"] = years
	out["months"] = months
	out["days"] = days
	out["seconds"] = secondsWhole
	out["nanoseconds"] = nanos
	out["nanosecondsOfSecond"] = nanos
}

func splitSecondsAndNanoseconds(seconds float64) (int, int) {
	if math.IsNaN(seconds) || math.IsInf(seconds, 0) {
		return 0, 0
	}
	whole := int(math.Floor(seconds))
	frac := seconds - float64(whole)
	rawNanos := frac * 1_000_000_000
	nanos := int(math.Round(rawNanos))
	if nanos == 0 {
		if rawNanos > 0 {
			nanos = 1
		} else if rawNanos < 0 {
			nanos = -1
		}
	}
	if nanos >= 1_000_000_000 {
		whole++
		nanos -= 1_000_000_000
	}
	if nanos < 0 {
		nanos = 0
	}
	return whole, nanos
}

func durationInstantClock(v durationInstant) durationClock {
	sec := float64(v.hour*3600+v.minute*60+v.second) + float64(v.nano)/1_000_000_000
	off, _ := durationInstantOffsetSeconds(v)
	return durationClock{secondOfDay: sec, hasZone: v.hasZone, offset: off}
}

func durationInstantOffsetSeconds(v durationInstant) (int, bool) {
	if !v.hasZone {
		return 0, false
	}
	if parsed, err := parseOffsetSeconds(v.timezone); err == nil {
		return parsed, true
	}
	if v.hasDate {
		if loc, err := time.LoadLocation(v.timezone); err == nil {
			hour, minute, second, nano := 0, 0, 0, 0
			if v.hasTime {
				hour, minute, second, nano = v.hour, v.minute, v.second, v.nano
			}
			t := time.Date(v.year, time.Month(v.month), v.day, hour, minute, second, nano, loc)
			_, off := t.Zone()
			return off, true
		}
	}
	return 0, false
}

func durationSecondsBetweenWithoutTimeDateOverflow(left, right durationInstant) (float64, bool) {
	if !(left.hasDate && right.hasDate) {
		return 0, false
	}
	leftDays, ok := daysSinceEpoch(left.year, left.month, left.day)
	if !ok {
		return 0, false
	}
	rightDays, ok := daysSinceEpoch(right.year, right.month, right.day)
	if !ok {
		return 0, false
	}
	leftClock := durationInstantClock(left)
	rightClock := durationInstantClock(right)
	seconds := float64(rightDays-leftDays)*86400 + (rightClock.secondOfDay - leftClock.secondOfDay)
	if leftClock.hasZone && rightClock.hasZone {
		seconds += float64(leftClock.offset - rightClock.offset)
	}
	return seconds, true
}

func durationSecondsBetweenExact(left, right durationInstant) (int64, int, bool) {
	if !(left.hasDate && right.hasDate) {
		return 0, 0, false
	}
	leftDays, ok := daysSinceEpoch(left.year, left.month, left.day)
	if !ok {
		return 0, 0, false
	}
	rightDays, ok := daysSinceEpoch(right.year, right.month, right.day)
	if !ok {
		return 0, 0, false
	}

	leftSec := int64(left.hour*3600 + left.minute*60 + left.second)
	rightSec := int64(right.hour*3600 + right.minute*60 + right.second)
	leftNanos := left.nano
	rightNanos := right.nano

	whole := (rightDays-leftDays)*86400 + (rightSec - leftSec)
	if left.hasZone && right.hasZone {
		leftOffset, _ := durationInstantOffsetSeconds(left)
		rightOffset, _ := durationInstantOffsetSeconds(right)
		whole += int64(leftOffset - rightOffset)
	}

	nanos := rightNanos - leftNanos
	if nanos < 0 {
		nanos += 1_000_000_000
		whole--
	}
	if nanos >= 1_000_000_000 {
		nanos -= 1_000_000_000
		whole++
	}
	return whole, nanos, true
}

func daysSinceEpoch(year, month, day int) (int64, bool) {
	if month < 1 || month > 12 || day < 1 || day > 31 {
		return 0, false
	}
	a := (14 - month) / 12
	y := year + 4800 - a
	m := month + 12*a - 3
	jd := day + (153*m+2)/5 + 365*y + y/4 - y/100 + y/400 - 32045
	const unixEpochJDN = 2440588
	return int64(jd - unixEpochJDN), true
}

func durationInstantToTime(v durationInstant) (time.Time, bool) {
	year := 1970
	month := 1
	day := 1
	if v.hasDate {
		year = v.year
		month = v.month
		day = v.day
	}
	hour, minute, second, nano := 0, 0, 0, 0
	if v.hasTime {
		hour, minute, second, nano = v.hour, v.minute, v.second, v.nano
	}
	loc := time.UTC
	if v.hasZone {
		if off, err := parseOffsetSeconds(v.timezone); err == nil {
			loc = time.FixedZone("", off)
		} else if l, err := time.LoadLocation(v.timezone); err == nil {
			loc = l
		}
	}
	return time.Date(year, time.Month(month), day, hour, minute, second, nano, loc), true
}

func coerceDurationInstant(raw any) (durationInstant, bool) {
	mapped, ok := temporalMapValue(raw)
	if !ok {
		return durationInstant{}, false
	}
	typeName := strings.ToLower(strings.TrimSpace(fmt.Sprint(mapped["__temporal_type"])))
	if typeName == "" || typeName == "duration" {
		return durationInstant{}, false
	}
	if valueRaw, ok := mapped["value"]; ok {
		if parsed, ok := parseTemporalLiteralToMap(typeName, fmt.Sprint(valueRaw)); ok {
			parsed["__temporal_type"] = typeName
			mapped = parsed
		}
	}

	if y, m, d, ok := resolveDateFromTemporalMap(mapped); ok {
		mapped["year"] = y
		mapped["month"] = m
		mapped["day"] = d
	}

	inst := durationInstant{kind: typeName}
	if y, yOK := mapInt(mapped, "year"); yOK {
		if m, mOK := mapInt(mapped, "month"); mOK {
			if d, dOK := mapInt(mapped, "day"); dOK {
				inst.year = y
				inst.month = m
				inst.day = d
				inst.hasDate = true
			}
		}
	}
	if h, hOK := mapInt(mapped, "hour"); hOK {
		inst.hour = h
		inst.hasTime = true
	}
	if m, mOK := mapInt(mapped, "minute"); mOK {
		inst.minute = m
		inst.hasTime = true
	}
	if s, sOK := mapInt(mapped, "second"); sOK {
		inst.second = s
		inst.hasTime = true
	}
	inst.nano = combineNanoseconds(mapped)
	if inst.nano != 0 {
		inst.hasTime = true
	}
	if tzRaw, ok := mapped["timezone"]; ok {
		tz := strings.TrimSpace(fmt.Sprint(tzRaw))
		if tz != "" {
			inst.timezone = tz
			inst.hasZone = true
		}
	}

	if typeName == "date" {
		inst.hasTime = false
	}
	if typeName == "localtime" || typeName == "time" {
		inst.hasDate = false
	}
	if typeName == "time" {
		inst.hasZone = true
		if inst.timezone == "" {
			inst.timezone = "Z"
		}
	}
	if typeName == "datetime" && inst.timezone == "" {
		inst.timezone = "Z"
		inst.hasZone = true
	}
	return inst, true
}

func parseTemporalLiteralToMap(typeName, raw string) (map[string]any, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, false
	}
	switch typeName {
	case "date":
		y, m, d, ok := parseDateParts(raw)
		if !ok {
			return nil, false
		}
		return map[string]any{"year": y, "month": m, "day": d}, true
	case "localtime":
		h, m, s, n, _, ok := parseClockAndZone(raw)
		if !ok {
			return nil, false
		}
		return map[string]any{"hour": h, "minute": m, "second": s, "nanosecond": n}, true
	case "time":
		h, m, s, n, tz, ok := parseClockAndZone(raw)
		if !ok {
			return nil, false
		}
		if tz == "" {
			tz = "Z"
		}
		return map[string]any{"hour": h, "minute": m, "second": s, "nanosecond": n, "timezone": tz}, true
	case "localdatetime":
		if datePart, timePart, ok := strings.Cut(raw, "T"); ok {
			y, mo, d, ok := parseDateParts(datePart)
			if !ok {
				return nil, false
			}
			h, mi, s, n, _, ok := parseClockAndZone(timePart)
			if !ok {
				return nil, false
			}
			return map[string]any{"year": y, "month": mo, "day": d, "hour": h, "minute": mi, "second": s, "nanosecond": n}, true
		}
		y, mo, d, ok := parseDateParts(raw)
		if !ok {
			return nil, false
		}
		return map[string]any{"year": y, "month": mo, "day": d, "hour": 0, "minute": 0, "second": 0, "nanosecond": 0}, true
	case "datetime":
		if datePart, timePart, ok := strings.Cut(raw, "T"); ok {
			y, mo, d, ok := parseDateParts(datePart)
			if !ok {
				return nil, false
			}
			h, mi, s, n, tz, ok := parseClockAndZone(timePart)
			if !ok {
				return nil, false
			}
			if tz == "" {
				tz = "Z"
			}
			return map[string]any{"year": y, "month": mo, "day": d, "hour": h, "minute": mi, "second": s, "nanosecond": n, "timezone": tz}, true
		}
		y, mo, d, ok := parseDateParts(raw)
		if !ok {
			return nil, false
		}
		return map[string]any{"year": y, "month": mo, "day": d, "hour": 0, "minute": 0, "second": 0, "nanosecond": 0, "timezone": "Z"}, true
	case "duration":
		return parseDurationLiteralToMap(raw)
	default:
		return nil, false
	}
}

func parseDateParts(raw string) (int, int, int, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, 0, 0, false
	}
	sign := 1
	if strings.HasPrefix(raw, "+") {
		raw = raw[1:]
	} else if strings.HasPrefix(raw, "-") {
		sign = -1
		raw = raw[1:]
	}
	if idx := strings.IndexAny(raw, "Ww"); idx > 0 {
		yearPart := strings.TrimSuffix(raw[:idx], "-")
		rest := raw[idx+1:]
		year, err := strconv.Atoi(yearPart)
		if err != nil {
			return 0, 0, 0, false
		}
		year *= sign
		dayOfWeek := 1
		rest = strings.TrimPrefix(rest, "-")
		weekPart := rest
		if dash := strings.Index(rest, "-"); dash >= 0 {
			weekPart = rest[:dash]
			if parsedDay, err := strconv.Atoi(rest[dash+1:]); err == nil {
				dayOfWeek = parsedDay
			}
		} else if len(rest) > 2 {
			weekPart = rest[:2]
			if parsedDay, err := strconv.Atoi(rest[2:]); err == nil {
				dayOfWeek = parsedDay
			}
		}
		week, err := strconv.Atoi(weekPart)
		if err != nil {
			return 0, 0, 0, false
		}
		if resolved, ok := isoWeekDate(year, week, dayOfWeek); ok {
			return resolved.Year(), int(resolved.Month()), resolved.Day(), true
		}
		return 0, 0, 0, false
	}
	if strings.Contains(raw, "-") {
		parts := strings.Split(raw, "-")
		if len(parts) == 3 {
			y, err := strconv.Atoi(parts[0])
			if err != nil {
				return 0, 0, 0, false
			}
			m, err := strconv.Atoi(parts[1])
			if err != nil {
				return 0, 0, 0, false
			}
			d, err := strconv.Atoi(parts[2])
			if err != nil {
				return 0, 0, 0, false
			}
			return sign * y, m, d, true
		}
		if len(parts) == 2 {
			y, err := strconv.Atoi(parts[0])
			if err != nil {
				return 0, 0, 0, false
			}
			if len(parts[1]) == 3 {
				ord, err := strconv.Atoi(parts[1])
				if err != nil {
					return 0, 0, 0, false
				}
				resolved := time.Date(sign*y, 1, ord, 0, 0, 0, 0, time.UTC)
				return resolved.Year(), int(resolved.Month()), resolved.Day(), true
			}
			if m, err := strconv.Atoi(parts[1]); err == nil {
				return sign * y, m, 1, true
			}
		}
	}
	if len(raw) == 8 {
		y, err := strconv.Atoi(raw[:4])
		if err != nil {
			return 0, 0, 0, false
		}
		m, err := strconv.Atoi(raw[4:6])
		if err != nil {
			return 0, 0, 0, false
		}
		d, err := strconv.Atoi(raw[6:8])
		if err != nil {
			return 0, 0, 0, false
		}
		return sign * y, m, d, true
	}
	if len(raw) == 7 {
		y, err := strconv.Atoi(raw[:4])
		if err != nil {
			return 0, 0, 0, false
		}
		ord, err := strconv.Atoi(raw[4:])
		if err != nil {
			return 0, 0, 0, false
		}
		resolved := time.Date(sign*y, 1, ord, 0, 0, 0, 0, time.UTC)
		return resolved.Year(), int(resolved.Month()), resolved.Day(), true
	}
	if len(raw) == 6 {
		y, err := strconv.Atoi(raw[:4])
		if err != nil {
			return 0, 0, 0, false
		}
		m, err := strconv.Atoi(raw[4:6])
		if err != nil {
			return 0, 0, 0, false
		}
		return sign * y, m, 1, true
	}
	if len(raw) == 4 {
		y, err := strconv.Atoi(raw)
		if err != nil {
			return 0, 0, 0, false
		}
		return sign * y, 1, 1, true
	}
	parts := strings.Split(raw, "-")
	if len(parts) == 3 {
		y, err := strconv.Atoi(parts[0])
		if err != nil {
			return 0, 0, 0, false
		}
		m, err := strconv.Atoi(parts[1])
		if err != nil {
			return 0, 0, 0, false
		}
		d, err := strconv.Atoi(parts[2])
		if err != nil {
			return 0, 0, 0, false
		}
		return sign * y, m, d, true
	}
	return 0, 0, 0, false
}

func parseClockAndZone(raw string) (int, int, int, int, string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, 0, 0, 0, "", false
	}
	tz := ""
	hasNamedZone := false
	if idx := strings.LastIndex(raw, "["); idx > 0 && strings.HasSuffix(raw, "]") {
		tz = strings.TrimSpace(raw[idx+1 : len(raw)-1])
		hasNamedZone = tz != ""
		raw = strings.TrimSpace(raw[:idx])
	}

	offsetIdx := -1
	for i := len(raw) - 1; i >= 0; i-- {
		if raw[i] == '+' || raw[i] == '-' {
			offsetIdx = i
			break
		}
	}
	if strings.HasSuffix(raw, "Z") || strings.HasSuffix(raw, "z") {
		tz = "Z"
		raw = raw[:len(raw)-1]
	} else if offsetIdx > 0 {
		offset := raw[offsetIdx:]
		norm, ok := normalizeOffsetToken(offset)
		if ok {
			if !hasNamedZone {
				tz = norm
			}
			raw = raw[:offsetIdx]
		}
	}

	clock := strings.SplitN(raw, ".", 2)
	h := 0
	m := 0
	s := 0
	var err error
	if strings.Contains(clock[0], ":") {
		hms := strings.Split(clock[0], ":")
		if len(hms) < 2 || len(hms) > 3 {
			return 0, 0, 0, 0, "", false
		}
		h, err = strconv.Atoi(hms[0])
		if err != nil {
			return 0, 0, 0, 0, "", false
		}
		m, err = strconv.Atoi(hms[1])
		if err != nil {
			return 0, 0, 0, 0, "", false
		}
		if len(hms) == 3 {
			s, err = strconv.Atoi(hms[2])
			if err != nil {
				return 0, 0, 0, 0, "", false
			}
		}
	} else {
		digits := clock[0]
		if len(digits) != 2 && len(digits) != 4 && len(digits) != 6 {
			return 0, 0, 0, 0, "", false
		}
		h, err = strconv.Atoi(digits[:2])
		if err != nil {
			return 0, 0, 0, 0, "", false
		}
		if len(digits) >= 4 {
			m, err = strconv.Atoi(digits[2:4])
			if err != nil {
				return 0, 0, 0, 0, "", false
			}
		}
		if len(digits) == 6 {
			s, err = strconv.Atoi(digits[4:6])
			if err != nil {
				return 0, 0, 0, 0, "", false
			}
		}
	}
	n := 0
	if len(clock) == 2 {
		frac := strings.TrimSpace(clock[1])
		if frac == "" {
			return 0, 0, 0, 0, "", false
		}
		if len(frac) > 9 {
			frac = frac[:9]
		}
		for len(frac) < 9 {
			frac += "0"
		}
		n, err = strconv.Atoi(frac)
		if err != nil {
			return 0, 0, 0, 0, "", false
		}
	}
	return h, m, s, n, tz, true
}

func parseDurationLiteralToMap(raw string) (map[string]any, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" || !(strings.HasPrefix(raw, "P") || strings.HasPrefix(raw, "p")) {
		return nil, false
	}
	raw = raw[1:]
	out := map[string]any{}
	hasValue := false
	datePart := raw
	timePart := ""
	if idx := strings.IndexAny(raw, "Tt"); idx >= 0 {
		datePart = raw[:idx]
		timePart = raw[idx+1:]
	}
	if datePart != "" {
		if strings.ContainsAny(datePart, "YyMmWwDd") {
			if parsed, ok := parseDurationUnitSection(datePart, false); ok {
				for k, v := range parsed {
					out[k] = v
					hasValue = true
				}
			}
		} else if y, m, d, ok := parseDateParts(datePart); ok {
			out["years"] = float64(y)
			out["months"] = float64(m)
			out["days"] = float64(d)
			hasValue = true
		}
	}
	if timePart != "" {
		if strings.Contains(timePart, ":") {
			h, m, s, n, _, ok := parseClockAndZone(timePart)
			if ok {
				out["hours"] = float64(h)
				out["minutes"] = float64(m)
				out["seconds"] = float64(s) + float64(n)/1_000_000_000
				hasValue = true
			}
		} else if strings.ContainsAny(timePart, "HhMmSs") {
			if parsed, ok := parseDurationUnitSection(timePart, true); ok {
				for k, v := range parsed {
					out[k] = v
					hasValue = true
				}
			}
		}
	}
	if !hasValue {
		return nil, false
	}
	return out, true
}

func parseDurationUnitSection(raw string, timeSection bool) (map[string]float64, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, false
	}
	result := map[string]float64{}
	pattern := regexp.MustCompile(`([+-]?\d+(?:\.\d+)?)([YMWDHSymwdhs])`)
	matches := pattern.FindAllStringSubmatch(raw, -1)
	if len(matches) == 0 {
		return nil, false
	}
	for _, match := range matches {
		value, err := strconv.ParseFloat(match[1], 64)
		if err != nil {
			return nil, false
		}
		switch strings.ToUpper(match[2]) {
		case "Y":
			result["years"] += value
		case "M":
			if timeSection {
				result["minutes"] += value
			} else if strings.ContainsAny(raw, "YyWwDd") {
				result["months"] += value
			} else {
				wholeMonths := truncTowardZero(value)
				result["months"] += wholeMonths
				fracMonths := value - wholeMonths
				if fracMonths != 0 {
					monthSeconds := fracMonths * 2629746.0
					wholeDays := truncTowardZero(monthSeconds / 86400)
					result["days"] += wholeDays
					result["seconds"] += monthSeconds - wholeDays*86400
				}
			}
		case "W":
			result["weeks"] += value
		case "D":
			result["days"] += value
		case "H":
			result["hours"] += value
		case "S":
			result["seconds"] += value
		}
	}
	return result, true
}

func normalizeOffsetToken(raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", false
	}
	if raw == "Z" || raw == "z" {
		return "Z", true
	}
	if raw[0] != '+' && raw[0] != '-' {
		return "", false
	}
	body := raw[1:]
	if len(body) == 4 {
		return string(raw[0]) + body[:2] + ":" + body[2:], true
	}
	if len(body) == 2 {
		return string(raw[0]) + body + ":00", true
	}
	if len(body) == 6 {
		return string(raw[0]) + body[:2] + ":" + body[2:4] + ":" + body[4:], true
	}
	if len(body) == 5 && body[2] == ':' {
		return raw, true
	}
	if len(body) == 8 && body[2] == ':' && body[5] == ':' {
		return raw, true
	}
	return "", false
}

func splitTopLevelOperator(raw string, op string) (string, string, bool) {
	if op == "" {
		return "", "", false
	}
	depthParen := 0
	depthBracket := 0
	depthBrace := 0
	inSingle := false
	inDouble := false
	upper := strings.ToUpper(raw)
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		switch ch {
		case '\'':
			if !inDouble {
				inSingle = !inSingle
			}
		case '"':
			if !inSingle {
				inDouble = !inDouble
			}
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
		if depthParen == 0 && depthBracket == 0 && depthBrace == 0 && strings.HasPrefix(upper[i:], "CASE") {
			if endIdx, ok := findCaseExpressionEnd(raw, i); ok {
				i = endIdx
				continue
			}
		}
		if depthParen == 0 && depthBracket == 0 && depthBrace == 0 && strings.HasPrefix(raw[i:], op) {
			if (op == "+" || op == "-") && isUnarySignPosition(raw, i) {
				continue
			}
			left := strings.TrimSpace(raw[:i])
			right := strings.TrimSpace(raw[i+len(op):])
			if left == "" || right == "" {
				return "", "", false
			}
			return left, right, true
		}
	}
	return "", "", false
}

func splitTopLevelOperatorLast(raw string, op string) (string, string, bool) {
	if op == "" {
		return "", "", false
	}
	depthParen := 0
	depthBracket := 0
	depthBrace := 0
	inSingle := false
	inDouble := false
	upper := strings.ToUpper(raw)
	matchIdx := -1
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		switch ch {
		case '\'':
			if !inDouble {
				inSingle = !inSingle
			}
		case '"':
			if !inSingle {
				inDouble = !inDouble
			}
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
		if depthParen == 0 && depthBracket == 0 && depthBrace == 0 && strings.HasPrefix(upper[i:], "CASE") {
			if endIdx, ok := findCaseExpressionEnd(raw, i); ok {
				i = endIdx
				continue
			}
		}
		if depthParen == 0 && depthBracket == 0 && depthBrace == 0 && strings.HasPrefix(raw[i:], op) {
			left := strings.TrimSpace(raw[:i])
			right := strings.TrimSpace(raw[i+len(op):])
			if left != "" && right != "" {
				matchIdx = i
			}
		}
	}
	if matchIdx == -1 {
		return "", "", false
	}
	left := strings.TrimSpace(raw[:matchIdx])
	right := strings.TrimSpace(raw[matchIdx+len(op):])
	if left == "" || right == "" {
		return "", "", false
	}
	return left, right, true
}

func splitTopLevelOperatorSetLast(raw string, ops ...string) (string, string, string, bool) {
	if len(ops) == 0 {
		return "", "", "", false
	}
	depthParen := 0
	depthBracket := 0
	depthBrace := 0
	inSingle := false
	inDouble := false
	upper := strings.ToUpper(raw)
	matchIdx := -1
	matchOp := ""
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		switch ch {
		case '\'':
			if !inDouble {
				inSingle = !inSingle
			}
		case '"':
			if !inSingle {
				inDouble = !inDouble
			}
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
		if depthParen == 0 && depthBracket == 0 && depthBrace == 0 && strings.HasPrefix(upper[i:], "CASE") {
			if endIdx, ok := findCaseExpressionEnd(raw, i); ok {
				i = endIdx
				continue
			}
		}
		if depthParen != 0 || depthBracket != 0 || depthBrace != 0 {
			continue
		}
		for _, op := range ops {
			if strings.HasPrefix(raw[i:], op) {
				if (op == "+" || op == "-") && isUnarySignPosition(raw, i) {
					continue
				}
				if (op == "+" || op == "-") && isExponentSignPosition(raw, i) {
					continue
				}
				left := strings.TrimSpace(raw[:i])
				right := strings.TrimSpace(raw[i+len(op):])
				if left != "" && right != "" {
					matchIdx = i
					matchOp = op
				}
				break
			}
		}
	}
	if matchIdx == -1 {
		return "", "", "", false
	}
	left := strings.TrimSpace(raw[:matchIdx])
	right := strings.TrimSpace(raw[matchIdx+len(matchOp):])
	if left == "" || right == "" {
		return "", "", "", false
	}
	return left, right, matchOp, true
}

func findCaseExpressionEnd(raw string, start int) (int, bool) {
	if start < 0 || start >= len(raw) {
		return -1, false
	}
	upper := strings.ToUpper(raw)
	if !strings.HasPrefix(upper[start:], "CASE") {
		return -1, false
	}
	depthParen := 0
	depthBracket := 0
	depthBrace := 0
	inSingle := false
	inDouble := false
	caseDepth := 0
	for i := start; i < len(raw); i++ {
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
		if strings.HasPrefix(upper[i:], "CASE") {
			caseDepth++
			i += len("CASE") - 1
			continue
		}
		if caseDepth > 0 && strings.HasPrefix(upper[i:], "END") {
			caseDepth--
			if caseDepth == 0 {
				return i + len("END") - 1, true
			}
			i += len("END") - 1
		}
	}
	return -1, false
}

func isUnarySignPosition(raw string, idx int) bool {
	if idx == 0 {
		return true
	}
	for i := idx - 1; i >= 0; i-- {
		ch := raw[i]
		if ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' {
			continue
		}
		switch ch {
		case '(', '[', '{', ',', '+', '-', '*', '/', '%', '=', '<', '>', '!':
			return true
		default:
			return false
		}
	}
	return true
}

func isExponentSignPosition(raw string, idx int) bool {
	if idx <= 0 || idx >= len(raw) {
		return false
	}
	sign := raw[idx]
	if sign != '+' && sign != '-' {
		return false
	}
	if idx+1 >= len(raw) || raw[idx+1] < '0' || raw[idx+1] > '9' {
		return false
	}
	prevIdx := idx - 1
	for prevIdx >= 0 && (raw[prevIdx] == ' ' || raw[prevIdx] == '\t' || raw[prevIdx] == '\n' || raw[prevIdx] == '\r') {
		prevIdx--
	}
	if prevIdx < 0 {
		return false
	}
	if raw[prevIdx] != 'e' && raw[prevIdx] != 'E' {
		return false
	}
	if prevIdx == 0 {
		return false
	}
	basePrev := raw[prevIdx-1]
	return (basePrev >= '0' && basePrev <= '9') || basePrev == '.'
}

func numericValue(v any) (float64, bool) {
	switch typed := v.(type) {
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case float64:
		return typed, true
	case json.Number:
		f, err := typed.Float64()
		if err == nil {
			return f, true
		}
		return 0, false
	case float32:
		return float64(typed), true
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		if err == nil {
			return f, true
		}
	}
	return 0, false
}

func exactIntegerAggregateValue(v any) (int64, bool) {
	switch typed := v.(type) {
	case int:
		return int64(typed), true
	case int64:
		return typed, true
	case int32:
		return int64(typed), true
	case uint:
		return int64(typed), true
	case uint64:
		if typed > math.MaxInt64 {
			return 0, false
		}
		return int64(typed), true
	case uint32:
		return int64(typed), true
	case json.Number:
		s := strings.TrimSpace(typed.String())
		if s == "" || strings.ContainsAny(s, ".eE") {
			return 0, false
		}
		parsed, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return 0, false
		}
		return parsed, true
	case string:
		s := strings.TrimSpace(typed)
		if s == "" || strings.ContainsAny(s, ".eE") {
			return 0, false
		}
		parsed, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return 0, false
		}
		return parsed, true
	default:
		return 0, false
	}
}

func comparableNumericValue(v any) (float64, bool) {
	switch typed := v.(type) {
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case float64:
		return typed, true
	case json.Number:
		f, err := typed.Float64()
		if err == nil {
			return f, true
		}
		return 0, false
	case float32:
		return float64(typed), true
	default:
		return 0, false
	}
}

func evalAdditiveExpression(op, left, right, raw string, row Row, params Params) (any, error) {
	lhs, err := evalExpressionWithScope(left, row, params)
	if err != nil {
		return nil, err
	}
	rhs, err := evalExpressionWithScope(right, row, params)
	if err != nil {
		return nil, err
	}
	if lhs == nil || rhs == nil {
		return nil, nil
	}
	lf, lok := numericValue(lhs)
	rf, rok := numericValue(rhs)
	if lok && rok {
		if isFloatLikeNumeric(lhs) || isFloatLikeNumeric(rhs) {
			switch op {
			case "+":
				return json.Number(formatFloatResult(lf + rf)), nil
			case "-":
				return json.Number(formatFloatResult(lf - rf)), nil
			}
		}
		if li, err := toInt(lhs); err == nil {
			if ri, err := toInt(rhs); err == nil {
				switch op {
				case "+":
					return li + ri, nil
				case "-":
					return li - ri, nil
				}
			}
		}
		switch op {
		case "+":
			return lf + rf, nil
		case "-":
			return lf - rf, nil
		}
	}
	if op == "+" {
		if list, ok := normalizeListValue(lhs); ok {
			out := append([]any{}, list...)
			if rhsList, ok := normalizeListValue(rhs); ok {
				out = append(out, rhsList...)
			} else {
				out = append(out, rhs)
			}
			return out, nil
		}
		if rhsList, ok := normalizeListValue(rhs); ok {
			out := make([]any, 0, len(rhsList)+1)
			out = append(out, lhs)
			out = append(out, rhsList...)
			return out, nil
		}
	}
	if value, ok := evalTemporalArithmetic(lhs, rhs, op); ok {
		return value, nil
	}
	if op == "+" {
		return fmt.Sprint(lhs) + fmt.Sprint(rhs), nil
	}
	return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("expression %q is not yet supported", raw), nil)
}

func evalMultiplicativeExpression(op, left, right, raw string, row Row, params Params) (any, error) {
	lhs, err := evalExpressionWithScope(left, row, params)
	if err != nil {
		return nil, err
	}
	rhs, err := evalExpressionWithScope(right, row, params)
	if err != nil {
		return nil, err
	}
	if lhs == nil || rhs == nil {
		return nil, nil
	}
	lf, lok := numericValue(lhs)
	rf, rok := numericValue(rhs)
	if lok && rok {
		if isFloatLikeNumeric(lhs) || isFloatLikeNumeric(rhs) {
			if (op == "/" || op == "%") && rf == 0 {
				if op == "/" {
					return json.Number(formatFloatResult(lf / rf)), nil
				}
				return nil, graph.NewError(graph.ErrKindInvalidInput, "modulo by zero", nil)
			}
			switch op {
			case "*":
				return json.Number(formatFloatResult(lf * rf)), nil
			case "/":
				return json.Number(formatFloatResult(lf / rf)), nil
			case "%":
				return json.Number(formatFloatResult(math.Mod(lf, rf))), nil
			}
		}
		li, lerr := toInt(lhs)
		ri, rerr := toInt(rhs)
		if lerr == nil && rerr == nil {
			switch op {
			case "*":
				return li * ri, nil
			case "/":
				if ri == 0 {
					return nil, graph.NewError(graph.ErrKindInvalidInput, "division by zero", nil)
				}
				return li / ri, nil
			case "%":
				if ri == 0 {
					return nil, graph.NewError(graph.ErrKindInvalidInput, "modulo by zero", nil)
				}
				return li % ri, nil
			}
		}
		switch op {
		case "*":
			return lf * rf, nil
		case "/":
			if rf == 0 {
				return nil, graph.NewError(graph.ErrKindInvalidInput, "division by zero", nil)
			}
			return json.Number(formatFloatResult(lf / rf)), nil
		case "%":
			if rf == 0 {
				return nil, graph.NewError(graph.ErrKindInvalidInput, "modulo by zero", nil)
			}
			return json.Number(formatFloatResult(math.Mod(lf, rf))), nil
		}
	}
	if value, ok := evalTemporalArithmetic(lhs, rhs, op); ok {
		return value, nil
	}
	return nil, graph.NewError(graph.ErrKindUnsupported, fmt.Sprintf("expression %q is not yet supported", compactExpression(raw)), nil)
}

func compactExpression(raw string) string {
	var b strings.Builder
	b.Grow(len(raw))
	for i := 0; i < len(raw); i++ {
		switch raw[i] {
		case ' ', '\t', '\n', '\r':
			continue
		default:
			b.WriteByte(raw[i])
		}
	}
	return b.String()
}

func formatFloatResult(value float64) string {
	if math.IsNaN(value) {
		return "NaN"
	}
	if math.IsInf(value, 1) {
		return "Inf"
	}
	if math.IsInf(value, -1) {
		return "-Inf"
	}
	if value == 0 {
		return "0.0"
	}
	abs := math.Abs(value)
	if abs >= 1e15 || abs < 1e-8 {
		formatted := strconv.FormatFloat(value, 'e', -1, 64)
		parts := strings.SplitN(formatted, "e", 2)
		if len(parts) != 2 {
			return formatted
		}
		exp := parts[1]
		expSign := ""
		if strings.HasPrefix(exp, "+") || strings.HasPrefix(exp, "-") {
			expSign = exp[:1]
			exp = exp[1:]
		}
		exp = strings.TrimLeft(exp, "0")
		if exp == "" {
			exp = "0"
		}
		if expSign == "+" {
			expSign = ""
		}
		return parts[0] + "e" + expSign + exp
	}
	formatted := strconv.FormatFloat(value, 'f', -1, 64)
	if strings.HasPrefix(formatted, ".") {
		formatted = "0" + formatted
	}
	if strings.HasPrefix(formatted, "-.") {
		formatted = "-0" + formatted[1:]
	}
	if !strings.ContainsAny(formatted, ".eE") {
		formatted += ".0"
	}
	return formatted
}

func parseStoredMapString(raw string) (map[string]any, bool) {
	raw = strings.TrimSpace(raw)
	if !strings.HasPrefix(raw, "map[") || !strings.HasSuffix(raw, "]") {
		return nil, false
	}
	body := strings.TrimSpace(raw[len("map[") : len(raw)-1])
	if body == "" {
		return map[string]any{}, true
	}
	out := map[string]any{}
	for _, part := range strings.Fields(body) {
		pair := strings.SplitN(part, ":", 2)
		if len(pair) != 2 {
			continue
		}
		out[pair[0]] = pair[1]
	}
	return out, true
}

func evalTemporalArithmetic(lhs, rhs any, op string) (any, bool) {
	leftMap, leftTemporal := temporalMapValue(lhs)
	rightMap, rightTemporal := temporalMapValue(rhs)

	if leftTemporal && rightTemporal {
		leftType := strings.ToLower(fmt.Sprint(leftMap["__temporal_type"]))
		rightType := strings.ToLower(fmt.Sprint(rightMap["__temporal_type"]))
		if leftType == "duration" && rightType == "duration" {
			leftDur := durationComponentsFromMap(leftMap)
			rightDur := durationComponentsFromMap(rightMap)
			switch op {
			case "+":
				return formatDurationComponents(leftDur.add(rightDur)), true
			case "-":
				return formatDurationComponents(leftDur.sub(rightDur)), true
			}
		}

		if rightType == "duration" {
			if value, ok := applyTemporalAndDuration(leftMap, durationComponentsFromMap(rightMap), op); ok {
				return value, true
			}
		}
	}

	if leftTemporal {
		leftType := strings.ToLower(fmt.Sprint(leftMap["__temporal_type"]))
		if leftType == "duration" {
			leftDur := durationComponentsFromMap(leftMap)
			if factor, ok := numericValue(rhs); ok {
				switch op {
				case "*":
					return formatDurationComponents(leftDur.scale(factor)), true
				case "/":
					if factor == 0 {
						return nil, false
					}
					return formatDurationComponents(leftDur.scale(1 / factor)), true
				}
			}
		}
	}

	if rightTemporal && op == "*" {
		rightType := strings.ToLower(fmt.Sprint(rightMap["__temporal_type"]))
		if rightType == "duration" {
			if factor, ok := numericValue(lhs); ok {
				return formatDurationComponents(durationComponentsFromMap(rightMap).scale(factor)), true
			}
		}
	}

	return nil, false
}

type durationComponents struct {
	months  float64
	days    float64
	seconds float64
}

func (d durationComponents) add(other durationComponents) durationComponents {
	return durationComponents{months: d.months + other.months, days: d.days + other.days, seconds: d.seconds + other.seconds}
}

func (d durationComponents) sub(other durationComponents) durationComponents {
	return durationComponents{months: d.months - other.months, days: d.days - other.days, seconds: d.seconds - other.seconds}
}

func (d durationComponents) scale(factor float64) durationComponents {
	return durationComponents{months: d.months * factor, days: d.days * factor, seconds: d.seconds * factor}
}

func temporalMapValue(v any) (map[string]any, bool) {
	switch typed := v.(type) {
	case map[string]any:
		if _, ok := typed["__temporal_type"]; ok {
			return typed, true
		}
	case string:
		if mapped, ok := parseStoredMapString(typed); ok {
			if _, hasTemporal := mapped["__temporal_type"]; hasTemporal {
				return mapped, true
			}
		}
	}
	return nil, false
}

func durationComponentsFromMap(value map[string]any) durationComponents {
	seconds := 3600*mapFloat(value, "hours") + 60*mapFloat(value, "minutes") + mapFloat(value, "seconds")
	months := 12*mapFloat(value, "years") + mapFloat(value, "months")
	days := 7*mapFloat(value, "weeks") + mapFloat(value, "days")

	// Fractional month components are converted into day/second components before
	// arithmetic so operations across durations preserve openCypher expectations.
	const avgMonthSeconds = 2629746.0
	wholeMonths := truncTowardZero(months)
	fracMonths := months - wholeMonths
	if fracMonths != 0 {
		monthSeconds := fracMonths * avgMonthSeconds
		wholeDays := truncTowardZero(monthSeconds / 86400)
		days += wholeDays
		seconds += monthSeconds - wholeDays*86400
	}
	wholeDays := truncTowardZero(days)
	fracDays := days - wholeDays
	if fracDays != 0 {
		seconds += fracDays * 86400
		days = wholeDays
	}

	var nanosAcc int64
	if ms, ok := mapWholeInt64(value, "milliseconds"); ok {
		nanosAcc += ms * 1_000_000
	} else {
		seconds += mapFloat(value, "milliseconds") / 1_000
	}
	if us, ok := mapWholeInt64(value, "microseconds"); ok {
		nanosAcc += us * 1_000
	} else {
		seconds += mapFloat(value, "microseconds") / 1_000_000
	}
	if ns, ok := mapWholeInt64(value, "nanoseconds"); ok {
		nanosAcc += ns
	} else {
		seconds += mapFloat(value, "nanoseconds") / 1_000_000_000
	}
	seconds += float64(nanosAcc) / 1_000_000_000
	secWhole, secNanos := splitSecondsAndNanoseconds(seconds)
	seconds = float64(secWhole) + float64(secNanos)/1_000_000_000

	return durationComponents{
		months:  wholeMonths,
		days:    days,
		seconds: seconds,
	}
}

func mapFloat(value map[string]any, key string) float64 {
	raw, ok := value[key]
	if !ok {
		return 0
	}
	if f, ok := numericValue(raw); ok {
		return f
	}
	return 0
}

func mapWholeInt64(value map[string]any, key string) (int64, bool) {
	raw, ok := value[key]
	if !ok {
		return 0, false
	}
	intVal, err := toInt(raw)
	if err != nil {
		return 0, false
	}
	if f, ok := numericValue(raw); ok {
		if math.Abs(f-float64(intVal)) > 1e-12 {
			return 0, false
		}
	}
	return int64(intVal), true
}

func applyTemporalAndDuration(temporal map[string]any, dur durationComponents, op string) (any, bool) {
	if op != "+" && op != "-" {
		return nil, false
	}
	if op == "-" {
		dur = dur.scale(-1)
	}

	temporalType := strings.ToLower(fmt.Sprint(temporal["__temporal_type"]))
	year, yOk := mapInt(temporal, "year")
	month, mOk := mapInt(temporal, "month")
	day, dOk := mapInt(temporal, "day")
	hour, _ := mapInt(temporal, "hour")
	minute, _ := mapInt(temporal, "minute")
	second, _ := mapInt(temporal, "second")
	nanosecond, _ := mapInt(temporal, "nanosecond")

	loc := time.UTC
	if tzRaw, ok := temporal["timezone"]; ok {
		tz := strings.TrimSpace(fmt.Sprint(tzRaw))
		if offset, err := parseOffsetSeconds(tz); err == nil {
			loc = time.FixedZone("", offset)
		}
	}

	baseYear, baseMonth, baseDay := 2000, 1, 1
	if yOk {
		baseYear = year
	}
	if mOk {
		baseMonth = month
	}
	if dOk {
		baseDay = day
	}

	base := time.Date(baseYear, time.Month(baseMonth), baseDay, hour, minute, second, nanosecond, loc)
	addY, addM, addD, addSeconds := decomposeDuration(dur)
	dateAdjusted := base.AddDate(addY, addM, addD)
	adjusted := dateAdjusted.Add(secondsToDuration(addSeconds))

	switch temporalType {
	case "date":
		dayCarry := int(truncTowardZero(addSeconds / 86400))
		dateAdjusted = base.AddDate(addY, addM, addD+dayCarry)
		return temporalResultFromTime("date", dateAdjusted, temporal), true
	case "localtime":
		return temporalResultFromTime("localtime", adjusted, temporal), true
	case "time":
		return temporalResultFromTime("time", adjusted, temporal), true
	case "localdatetime":
		return temporalResultFromTime("localdatetime", adjusted, temporal), true
	case "datetime":
		return temporalResultFromTime("datetime", adjusted, temporal), true
	default:
		return nil, false
	}
}

func temporalResultFromTime(typeName string, t time.Time, source map[string]any) map[string]any {
	out := map[string]any{"__temporal_type": typeName}
	switch typeName {
	case "date":
		out["year"] = t.Year()
		out["month"] = int(t.Month())
		out["day"] = t.Day()
	case "localtime":
		out["hour"] = t.Hour()
		out["minute"] = t.Minute()
		out["second"] = t.Second()
		out["nanosecond"] = t.Nanosecond()
	case "time":
		out["hour"] = t.Hour()
		out["minute"] = t.Minute()
		out["second"] = t.Second()
		out["nanosecond"] = t.Nanosecond()
		if tzRaw, ok := source["timezone"]; ok {
			tz := strings.TrimSpace(fmt.Sprint(tzRaw))
			if tz != "" {
				out["timezone"] = tz
			} else {
				_, off := t.Zone()
				out["timezone"] = formatOffsetString(off)
			}
		} else {
			_, off := t.Zone()
			out["timezone"] = formatOffsetString(off)
		}
	case "localdatetime":
		out["year"] = t.Year()
		out["month"] = int(t.Month())
		out["day"] = t.Day()
		out["hour"] = t.Hour()
		out["minute"] = t.Minute()
		out["second"] = t.Second()
		out["nanosecond"] = t.Nanosecond()
	case "datetime":
		out["year"] = t.Year()
		out["month"] = int(t.Month())
		out["day"] = t.Day()
		out["hour"] = t.Hour()
		out["minute"] = t.Minute()
		out["second"] = t.Second()
		out["nanosecond"] = t.Nanosecond()
		if tzRaw, ok := source["timezone"]; ok {
			tz := strings.TrimSpace(fmt.Sprint(tzRaw))
			if tz != "" {
				out["timezone"] = tz
			} else {
				_, off := t.Zone()
				out["timezone"] = formatOffsetString(off)
			}
		} else {
			_, off := t.Zone()
			out["timezone"] = formatOffsetString(off)
		}
	}
	return out
}

func decomposeDuration(dur durationComponents) (int, int, int, float64) {
	const avgMonthSeconds = 2629746.0
	totalMonths := dur.months
	years := int(truncTowardZero(totalMonths / 12))
	remainingMonths := totalMonths - float64(years*12)
	months := int(truncTowardZero(remainingMonths))
	fracMonths := remainingMonths - float64(months)

	totalDays := dur.days + (fracMonths*avgMonthSeconds)/86400
	days := int(truncTowardZero(totalDays))
	fracDays := totalDays - float64(days)

	seconds := dur.seconds + fracDays*86400
	return years, months, days, seconds
}

func formatDurationComponents(dur durationComponents) string {
	years, months, days, seconds := decomposeDuration(dur)
	wholeSeconds := int64(truncTowardZero(seconds))
	frac := seconds - float64(wholeSeconds)

	hours := int(wholeSeconds / 3600)
	remainingSeconds := wholeSeconds - int64(hours*3600)
	minutes := int(remainingSeconds / 60)
	secInt := int(remainingSeconds - int64(minutes*60))

	fracSign := 1
	if frac < 0 {
		fracSign = -1
	}
	absNanosFloat := math.Abs(frac) * 1_000_000_000
	absNanos := int(math.Floor(absNanosFloat))
	nearest := math.Round(absNanosFloat)
	// Snap values that are very close to integral nanoseconds to avoid binary drift.
	if math.Abs(absNanosFloat-nearest) < 0.02 {
		absNanos = int(nearest)
	}
	if absNanos >= 1_000_000_000 {
		if fracSign > 0 {
			secInt++
		} else {
			secInt--
		}
		absNanos -= 1_000_000_000
	}
	nanos := fracSign * absNanos

	if nanos >= 1_000_000_000 {
		secInt++
		nanos -= 1_000_000_000
	}
	if nanos <= -1_000_000_000 {
		secInt--
		nanos += 1_000_000_000
	}

	b := strings.Builder{}
	b.WriteString("P")
	if years != 0 {
		b.WriteString(fmt.Sprintf("%dY", years))
	}
	if months != 0 {
		b.WriteString(fmt.Sprintf("%dM", months))
	}
	if days != 0 {
		b.WriteString(fmt.Sprintf("%dD", days))
	}

	hasTime := hours != 0 || minutes != 0 || secInt != 0 || nanos != 0
	if hasTime || (years == 0 && months == 0 && days == 0) {
		b.WriteString("T")
		if hours != 0 {
			b.WriteString(fmt.Sprintf("%dH", hours))
		}
		if minutes != 0 {
			b.WriteString(fmt.Sprintf("%dM", minutes))
		}
		if secInt != 0 || nanos != 0 || (hours == 0 && minutes == 0) {
			if nanos == 0 {
				b.WriteString(fmt.Sprintf("%dS", secInt))
			} else {
				sign := ""
				if secInt < 0 || (secInt == 0 && nanos < 0) {
					sign = "-"
				}
				absSec := secInt
				if absSec < 0 {
					absSec = -absSec
				}
				absNanos := nanos
				if absNanos < 0 {
					absNanos = -absNanos
				}
				frac := strings.TrimRight(fmt.Sprintf("%09d", absNanos), "0")
				b.WriteString(fmt.Sprintf("%s%d.%sS", sign, absSec, frac))
			}
		}
	}
	return b.String()
}

func truncTowardZero(v float64) float64 {
	if v < 0 {
		return math.Ceil(v)
	}
	return math.Floor(v)
}

func standardDeviationResult(sum float64, sumSq float64, count int, sample bool) any {
	if count == 0 {
		return nil
	}
	if sample && count == 1 {
		return json.Number("0.0")
	}
	divisor := float64(count)
	if sample {
		divisor = float64(count - 1)
	}
	variance := (sumSq - ((sum * sum) / float64(count))) / divisor
	if variance < 0 && variance > -1e-12 {
		variance = 0
	}
	return json.Number(formatFloatResult(math.Sqrt(variance)))
}

func mapInt(value map[string]any, key string) (int, bool) {
	raw, ok := value[key]
	if !ok {
		return 0, false
	}
	if iv, err := toInt(raw); err == nil {
		return iv, true
	}
	if fv, ok := numericValue(raw); ok {
		return int(truncTowardZero(fv)), true
	}
	return 0, false
}

func secondsToDuration(seconds float64) time.Duration {
	return time.Duration(seconds * float64(time.Second))
}

func formatTimeString(t time.Time, includeZone bool) string {
	hms := t.Format("15:04")
	sec := t.Second()
	nanos := t.Nanosecond()
	frac := ""
	if sec != 0 || nanos != 0 {
		hms += fmt.Sprintf(":%02d", sec)
	}
	if nanos != 0 {
		frac = "." + strings.TrimRight(fmt.Sprintf("%09d", nanos), "0")
	}
	if includeZone {
		_, off := t.Zone()
		return hms + frac + formatOffsetString(off)
	}
	return hms + frac
}

func formatDateTimeString(t time.Time, includeZone bool) string {
	base := t.Format("2006-01-02T15:04")
	sec := t.Second()
	nanos := t.Nanosecond()
	if sec != 0 || nanos != 0 {
		base += fmt.Sprintf(":%02d", sec)
	}
	if nanos != 0 {
		base += "." + strings.TrimRight(fmt.Sprintf("%09d", nanos), "0")
	}
	if includeZone {
		_, off := t.Zone()
		base += formatOffsetString(off)
	}
	return base
}

func formatTemporalToString(temporal map[string]any) (string, bool) {
	typeName := strings.ToLower(strings.TrimSpace(fmt.Sprint(temporal["__temporal_type"])))
	switch typeName {
	case "date":
		y, m, d, ok := resolveDateFromTemporalMap(temporal)
		if !ok {
			return "", false
		}
		return fmt.Sprintf("%04d-%02d-%02d", y, m, d), true
	case "localtime":
		hour, _ := mapInt(temporal, "hour")
		minute, _ := mapInt(temporal, "minute")
		second, _ := mapInt(temporal, "second")
		nano := combineNanoseconds(temporal)
		return formatClockParts(hour, minute, second, nano), true
	case "time":
		hour, _ := mapInt(temporal, "hour")
		minute, _ := mapInt(temporal, "minute")
		second, _ := mapInt(temporal, "second")
		nano := combineNanoseconds(temporal)
		tzName := strings.TrimSpace(fmt.Sprint(temporal["timezone"]))
		if tzName == "" {
			tzName = "Z"
		}
		offsetRendered := tzName
		if offset, err := parseOffsetSeconds(tzName); err == nil {
			offsetRendered = formatOffsetString(offset)
		}
		return formatClockParts(hour, minute, second, nano) + offsetRendered, true
	case "localdatetime":
		y, m, d, ok := resolveDateFromTemporalMap(temporal)
		if !ok {
			return "", false
		}
		hour, _ := mapInt(temporal, "hour")
		minute, _ := mapInt(temporal, "minute")
		second, _ := mapInt(temporal, "second")
		nano := combineNanoseconds(temporal)
		return fmt.Sprintf("%04d-%02d-%02dT%s", y, m, d, formatClockParts(hour, minute, second, nano)), true
	case "datetime":
		y, m, d, ok := resolveDateFromTemporalMap(temporal)
		if !ok {
			return "", false
		}
		hour, _ := mapInt(temporal, "hour")
		minute, _ := mapInt(temporal, "minute")
		second, _ := mapInt(temporal, "second")
		nano := combineNanoseconds(temporal)
		tzName := strings.TrimSpace(fmt.Sprint(temporal["timezone"]))
		if tzName == "" {
			tzName = "Z"
		}
		clock := formatClockParts(hour, minute, second, nano)
		if offset, err := parseOffsetSeconds(tzName); err == nil {
			return fmt.Sprintf("%04d-%02d-%02dT%s%s", y, m, d, clock, formatOffsetString(offset)), true
		}
		if loc, err := time.LoadLocation(tzName); err == nil {
			t := time.Date(y, time.Month(m), d, hour, minute, second, nano, loc)
			_, offset := t.Zone()
			return fmt.Sprintf("%04d-%02d-%02dT%s%s[%s]", y, m, d, clock, formatOffsetString(offset), tzName), true
		}
		return fmt.Sprintf("%04d-%02d-%02dT%s%s", y, m, d, clock, tzName), true
	case "duration":
		if exact, ok := temporal["__duration_exact"].(bool); ok && exact {
			if sec, secOK := mapWholeInt64(temporal, "seconds"); secOK {
				if nanos, nanoOK := mapWholeInt64(temporal, "nanoseconds"); nanoOK {
					return formatDurationFromExactSecondNanos(sec, int(nanos)), true
				}
			}
		}
		return formatDurationComponents(durationComponentsFromMap(temporal)), true
	default:
		return "", false
	}
}

func formatDurationFromExactSecondNanos(seconds int64, nanos int) string {
	if nanos < 0 {
		nanos = 0
	}
	if nanos >= 1_000_000_000 {
		seconds += int64(nanos / 1_000_000_000)
		nanos = nanos % 1_000_000_000
	}

	negative := seconds < 0
	absSeconds := seconds
	absNanos := nanos
	if negative {
		absSeconds = -seconds
		if absNanos > 0 {
			absSeconds--
			absNanos = 1_000_000_000 - absNanos
		}
	}

	hours := int(absSeconds / 3600)
	remainingSeconds := absSeconds - int64(hours*3600)
	minutes := int(remainingSeconds / 60)
	secInt := int(remainingSeconds - int64(minutes*60))

	b := strings.Builder{}
	b.WriteString("PT")
	if hours != 0 {
		if negative {
			b.WriteString(fmt.Sprintf("-%dH", hours))
		} else {
			b.WriteString(fmt.Sprintf("%dH", hours))
		}
	}
	if minutes != 0 {
		if negative {
			b.WriteString(fmt.Sprintf("-%dM", minutes))
		} else {
			b.WriteString(fmt.Sprintf("%dM", minutes))
		}
	}
	if secInt != 0 || absNanos != 0 || (hours == 0 && minutes == 0) {
		if absNanos == 0 {
			if negative {
				b.WriteString(fmt.Sprintf("-%dS", secInt))
			} else {
				b.WriteString(fmt.Sprintf("%dS", secInt))
			}
		} else {
			sign := ""
			if negative {
				sign = "-"
			}
			frac := strings.TrimRight(fmt.Sprintf("%09d", absNanos), "0")
			b.WriteString(fmt.Sprintf("%s%d.%sS", sign, secInt, frac))
		}
	}
	return b.String()
}

func formatClockParts(hour, minute, second, nano int) string {
	base := fmt.Sprintf("%02d:%02d", hour, minute)
	if second != 0 || nano != 0 {
		base += fmt.Sprintf(":%02d", second)
	}
	if nano != 0 {
		base += "." + strings.TrimRight(fmt.Sprintf("%09d", nano), "0")
	}
	return base
}

func parseOffsetSeconds(raw string) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "Z" || raw == "z" {
		return 0, nil
	}
	if len(raw) != 6 && len(raw) != 9 {
		return 0, fmt.Errorf("invalid offset")
	}
	if raw[0] != '+' && raw[0] != '-' {
		return 0, fmt.Errorf("invalid offset")
	}
	if raw[3] != ':' {
		return 0, fmt.Errorf("invalid offset")
	}
	hours, err := strconv.Atoi(raw[1:3])
	if err != nil {
		return 0, err
	}
	minutes, err := strconv.Atoi(raw[4:6])
	if err != nil {
		return 0, err
	}
	seconds := 0
	if len(raw) == 9 {
		if raw[6] != ':' {
			return 0, fmt.Errorf("invalid offset")
		}
		seconds, err = strconv.Atoi(raw[7:9])
		if err != nil {
			return 0, err
		}
	}
	if hours > 18 || minutes > 59 || seconds > 59 {
		return 0, fmt.Errorf("invalid offset")
	}
	total := hours*3600 + minutes*60 + seconds
	if raw[0] == '-' {
		total = -total
	}
	return total, nil
}

func formatOffsetString(totalSeconds int) string {
	if totalSeconds == 0 {
		return "Z"
	}
	sign := "+"
	if totalSeconds < 0 {
		sign = "-"
		totalSeconds = -totalSeconds
	}
	hours := totalSeconds / 3600
	minutes := (totalSeconds % 3600) / 60
	seconds := totalSeconds % 60
	if seconds == 0 {
		return fmt.Sprintf("%s%02d:%02d", sign, hours, minutes)
	}
	return fmt.Sprintf("%s%02d:%02d:%02d", sign, hours, minutes, seconds)
}

func unquoteCypherString(raw string) (string, error) {
	if len(raw) < 2 {
		return "", fmt.Errorf("invalid string literal")
	}
	quote := raw[0]
	if raw[len(raw)-1] != quote {
		return "", fmt.Errorf("mismatched string literal quotes")
	}
	inner := raw[1 : len(raw)-1]
	switch quote {
	case '\'':
		var b strings.Builder
		b.Grow(len(inner))
		for i := 0; i < len(inner); i++ {
			ch := inner[i]
			if ch == '\'' && i+1 < len(inner) && inner[i+1] == '\'' {
				b.WriteByte('\'')
				i++
				continue
			}
			if ch != '\\' {
				b.WriteByte(ch)
				continue
			}
			if i+1 >= len(inner) {
				return "", fmt.Errorf("invalid string escape")
			}
			next := inner[i+1]
			i++
			switch next {
			case 'b':
				b.WriteByte('\b')
			case 'f':
				b.WriteByte('\f')
			case 'n':
				b.WriteByte('\n')
			case 'r':
				b.WriteByte('\r')
			case 't':
				b.WriteByte('\t')
			case '\\':
				if i+1 < len(inner) && inner[i+1] == '\'' {
					b.WriteByte('\'')
					i++
					break
				}
				b.WriteByte('\\')
			case '\'':
				b.WriteByte('\'')
			case '"':
				b.WriteByte('"')
			case 'u':
				if i+4 >= len(inner) {
					return "", fmt.Errorf("invalid unicode escape")
				}
				codePoint, err := strconv.ParseUint(inner[i+1:i+5], 16, 16)
				if err != nil {
					return "", err
				}
				b.WriteRune(rune(codePoint))
				i += 4
			default:
				return "", fmt.Errorf("unsupported string escape %q", next)
			}
		}
		return b.String(), nil
	case '"':
		unquoted, err := strconv.Unquote(raw)
		if err != nil {
			return "", err
		}
		return unquoted, nil
	default:
		return "", fmt.Errorf("unsupported quote character")
	}
}

func normalizeVertexProperties(props map[string]any) graph.PropertyMap {
	out := graph.PropertyMap{}
	for k, v := range props {
		if v == nil {
			continue
		}
		out[k] = valueToBytes(v)
	}
	return out
}

func normalizeEdgeProperties(props map[string]any) graph.PropertyMap {
	out := graph.PropertyMap{}
	for k, v := range props {
		if v == nil {
			continue
		}
		out[k] = valueToBytes(v)
	}
	return out
}

func stringFromProperty(props map[string]any, key string) string {
	v, ok := props[key]
	if !ok {
		return ""
	}
	switch typed := v.(type) {
	case string:
		return typed
	case []byte:
		return string(typed)
	case fmt.Stringer:
		return typed.String()
	case int:
		return strconv.Itoa(typed)
	case int64:
		return strconv.FormatInt(typed, 10)
	case bool:
		if typed {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprint(v)
	}
}

func valueToBytes(v any) []byte {
	switch typed := v.(type) {
	case nil:
		return []byte("null")
	case []byte:
		return append([]byte(nil), typed...)
	case string:
		return []byte(typed)
	case int:
		return []byte(strconv.Itoa(typed))
	case int64:
		return []byte(strconv.FormatInt(typed, 10))
	case bool:
		if typed {
			return []byte("true")
		}
		return []byte("false")
	default:
		return []byte(fmt.Sprint(v))
	}
}

func valueToPropertyBytes(v any) ([]byte, error) {
	if !isSupportedPropertyValue(v) {
		return nil, graph.NewError(graph.ErrKindInvalidInput, "InvalidPropertyType", nil)
	}
	return valueToBytes(v), nil
}

func isSupportedPropertyValue(v any) bool {
	switch typed := v.(type) {
	case nil, string, bool,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64,
		json.Number:
		return true
	case map[string]any:
		_, temporal := typed["__temporal_type"]
		_, spatial := typed["__spatial_type"]
		return temporal || spatial
	case *graph.Vertex, *graph.Edge, cypherPathValue, multiHopCypherPath:
		return false
	case []any:
		for _, item := range typed {
			if !isSupportedPropertyValue(item) {
				return false
			}
		}
		return true
	default:
		rv := reflect.ValueOf(v)
		if rv.IsValid() && (rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array) {
			for i := 0; i < rv.Len(); i++ {
				if !isSupportedPropertyValue(rv.Index(i).Interface()) {
					return false
				}
			}
			return true
		}
		return false
	}
}

func splitLabels(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ":")
	labels := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		labels = append(labels, part)
	}
	return labels
}

func stripLeadingClauseKeyword(raw, keyword string) string {
	raw = strings.TrimSpace(raw)
	return strings.TrimSpace(strings.TrimPrefix(raw, keyword))
}

func stripCypherLineComments(raw string) string {
	if raw == "" {
		return raw
	}
	var b strings.Builder
	b.Grow(len(raw))
	inSingle := false
	inDouble := false

	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		if inSingle {
			b.WriteByte(ch)
			if ch == '\'' {
				if i+1 < len(raw) && raw[i+1] == '\'' {
					b.WriteByte(raw[i+1])
					i++
					continue
				}
				inSingle = false
			}
			continue
		}
		if inDouble {
			b.WriteByte(ch)
			if ch == '\\' {
				if i+1 < len(raw) {
					b.WriteByte(raw[i+1])
					i++
				}
				continue
			}
			if ch == '"' {
				inDouble = false
			}
			continue
		}

		if ch == '\'' {
			inSingle = true
			b.WriteByte(ch)
			continue
		}
		if ch == '"' {
			inDouble = true
			b.WriteByte(ch)
			continue
		}
		if ch == '/' && i+1 < len(raw) && raw[i+1] == '/' {
			for i < len(raw) && raw[i] != '\n' && raw[i] != '\r' {
				i++
			}
			if i < len(raw) {
				b.WriteByte(raw[i])
			}
			continue
		}
		b.WriteByte(ch)
	}

	return b.String()
}

func normalizeClauseBody(raw string) string {
	if raw == "" {
		return ""
	}

	var b strings.Builder
	b.Grow(len(raw))
	inSingle := false
	inDouble := false

	for i := 0; i < len(raw); i++ {
		r := raw[i]
		if inSingle {
			b.WriteByte(r)
			if r == '\'' {
				if i+1 < len(raw) && raw[i+1] == '\'' {
					b.WriteByte(raw[i+1])
					i++
					continue
				}
				inSingle = false
			}
			continue
		}

		if inDouble {
			b.WriteByte(r)
			if r == '\\' {
				if i+1 < len(raw) {
					b.WriteByte(raw[i+1])
					i++
				}
				continue
			}
			if r == '"' {
				inDouble = false
			}
			continue
		}

		if unicode.IsSpace(rune(r)) {
			continue
		}

		b.WriteByte(r)
		if r == '\'' {
			inSingle = true
			continue
		}
		if r == '"' {
			inDouble = true
		}
	}

	return b.String()
}

func cloneRow(in Row) Row {
	out := make(Row, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneVertex(v *graph.Vertex) *graph.Vertex {
	if v == nil {
		return nil
	}
	out := &graph.Vertex{
		Tenant: v.Tenant,
		ID:     v.ID,
		Labels: append([]string(nil), v.Labels...),
	}
	if v.Properties != nil {
		out.Properties = make(graph.PropertyMap, len(v.Properties))
		for k, val := range v.Properties {
			out.Properties[k] = append([]byte(nil), val...)
		}
	}
	return out
}

func cloneEdge(e *graph.Edge) *graph.Edge {
	if e == nil {
		return nil
	}
	out := &graph.Edge{
		Tenant: e.Tenant,
		ID:     e.ID,
		Type:   e.Type,
		SrcID:  e.SrcID,
		DstID:  e.DstID,
	}
	if e.Properties != nil {
		out.Properties = make(graph.PropertyMap, len(e.Properties))
		for k, val := range e.Properties {
			out.Properties[k] = append([]byte(nil), val...)
		}
	}
	return out
}

func ensureProperties(v *graph.Vertex) {
	if v.Properties == nil {
		v.Properties = graph.PropertyMap{}
	}
}

func ensurePropertiesEdge(e *graph.Edge) {
	if e.Properties == nil {
		e.Properties = graph.PropertyMap{}
	}
}

func syntheticEdgeID(tenant, srcID, edgeType, dstID string) string {
	return strings.Join([]string{tenant, srcID, edgeType, dstID}, "|")
}
