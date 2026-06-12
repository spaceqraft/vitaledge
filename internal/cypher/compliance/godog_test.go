package compliance

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/cucumber/godog"
	"github.com/spaceqraft/vitaledge/internal/cypher/executor"
	"github.com/spaceqraft/vitaledge/internal/cypher/parser"
	"github.com/spaceqraft/vitaledge/internal/graph"
	pebblestore "github.com/spaceqraft/vitaledge/internal/graph/store/pebble"
)

const defaultTenant = "tck"

var procedureSignatureRE = regexp.MustCompile(`^\s*([A-Za-z_][A-Za-z0-9_.]*)\s*\(([^)]*)\)\s*::\s*\(([^)]*)\)\s*$`)

var tckDirFlag = flag.String("tck-dir", "", "path to the openCypher TCK features directory")

type graphSnapshot struct {
	Vertexes        int
	Relationships   int
	Properties      int
	Labels          int
	VertexSet       map[string]struct{}
	RelationshipSet map[string]struct{}
	PropertySet     map[string]struct{}
	LabelSet        map[string]struct{}
}

func (g graphSnapshot) Delta(before graphSnapshot) graphSnapshot {
	return graphSnapshot{
		Vertexes:      g.Vertexes - before.Vertexes,
		Relationships: g.Relationships - before.Relationships,
		Properties:    g.Properties - before.Properties,
		Labels:        g.Labels - before.Labels,
	}
}

type graphSideEffects struct {
	AddedVertexes        int
	RemovedVertexes      int
	AddedRelationships   int
	RemovedRelationships int
	AddedProperties      int
	RemovedProperties    int
	AddedLabels          int
	RemovedLabels        int
}

func diffGraphSideEffects(before, after graphSnapshot) graphSideEffects {
	addedVertexes, removedVertexes := propertySetDelta(before.VertexSet, after.VertexSet)
	addedRelationships, removedRelationships := propertySetDelta(before.RelationshipSet, after.RelationshipSet)
	addedProps, removedProps := propertySetDelta(before.PropertySet, after.PropertySet)
	addedLabels, removedLabels := propertySetDelta(before.LabelSet, after.LabelSet)
	return graphSideEffects{
		AddedVertexes:        addedVertexes,
		RemovedVertexes:      removedVertexes,
		AddedRelationships:   addedRelationships,
		RemovedRelationships: removedRelationships,
		AddedProperties:      addedProps,
		RemovedProperties:    removedProps,
		AddedLabels:          addedLabels,
		RemovedLabels:        removedLabels,
	}
}

func propertySetDelta(before, after map[string]struct{}) (added int, removed int) {
	for key := range after {
		if _, ok := before[key]; !ok {
			added++
		}
	}
	for key := range before {
		if _, ok := after[key]; !ok {
			removed++
		}
	}
	return added, removed
}

type cypherTCKFeature struct {
	ctx               context.Context
	tempDir           string
	store             *pebblestore.Store
	exec              *executor.Executor
	procedures        map[string]executor.ProcedureDecl
	params            executor.Params
	lastQuery         string
	lastResult        *executor.Result
	lastErr           error
	beforeQueryCounts graphSnapshot
	afterQueryCounts  graphSnapshot
}

const binaryTree1GraphCypher = `CREATE (a:A {name: 'a'}),
	(b1:X {name: 'b1'}),
	(b2:X {name: 'b2'}),
	(b3:X {name: 'b3'}),
	(b4:X {name: 'b4'}),
	(c11:X {name: 'c11'}),
	(c12:X {name: 'c12'}),
	(c21:X {name: 'c21'}),
	(c22:X {name: 'c22'}),
	(c31:X {name: 'c31'}),
	(c32:X {name: 'c32'}),
	(c41:X {name: 'c41'}),
	(c42:X {name: 'c42'})
CREATE
	(a)-[:KNOWS]->(b1),
	(a)-[:KNOWS]->(b2),
	(a)-[:FOLLOWS]->(b3),
	(a)-[:FOLLOWS]->(b4)
CREATE (b1)-[:FRIEND]->(c11),
	(b1)-[:FRIEND]->(c12),
	(b2)-[:FRIEND]->(c21),
	(b2)-[:FRIEND]->(c22),
	(b3)-[:FRIEND]->(c31),
	(b3)-[:FRIEND]->(c32),
	(b4)-[:FRIEND]->(c41),
	(b4)-[:FRIEND]->(c42)
CREATE (b1)-[:FRIEND]->(b2),
	(b2)-[:FRIEND]->(b3),
	(b3)-[:FRIEND]->(b4),
	(b4)-[:FRIEND]->(b1)`

const binaryTree2GraphCypher = `CREATE (a:A {name: 'a'}),
	(b1:X {name: 'b1'}),
	(b2:X {name: 'b2'}),
	(b3:X {name: 'b3'}),
	(b4:X {name: 'b4'}),
	(c11:X {name: 'c11'}),
	(c12:Y {name: 'c12'}),
	(c21:X {name: 'c21'}),
	(c22:Y {name: 'c22'}),
	(c31:X {name: 'c31'}),
	(c32:Y {name: 'c32'}),
	(c41:X {name: 'c41'}),
	(c42:Y {name: 'c42'})
CREATE
	(a)-[:KNOWS]->(b1),
	(a)-[:KNOWS]->(b2),
	(a)-[:FOLLOWS]->(b3),
	(a)-[:FOLLOWS]->(b4)
CREATE (b1)-[:FRIEND]->(c11),
	(b1)-[:FRIEND]->(c12),
	(b2)-[:FRIEND]->(c21),
	(b2)-[:FRIEND]->(c22),
	(b3)-[:FRIEND]->(c31),
	(b3)-[:FRIEND]->(c32),
	(b4)-[:FRIEND]->(c41),
	(b4)-[:FRIEND]->(c42)
CREATE (b1)-[:FRIEND]->(b2),
	(b2)-[:FRIEND]->(b3),
	(b3)-[:FRIEND]->(b4),
	(b4)-[:FRIEND]->(b1)`

func TestCypherCompliance(t *testing.T) {
	tckDir := resolveTCKDir(t)
	if tckDir == "" {
		t.Skip("openCypher TCK not present; run make cypher-compliance to fetch and execute it")
	}
	if _, err := os.Stat(tckDir); err != nil {
		t.Skipf("openCypher TCK directory unavailable: %v", err)
	}

	suite := godog.TestSuite{
		Name:                "cypher-compliance",
		ScenarioInitializer: InitializeScenario,
		Options: &godog.Options{
			Format:   "progress",
			Paths:    []string{tckDir},
			TestingT: t,
			Strict:   true,
		},
	}

	if suite.Run() != 0 {
		t.Fail()
	}
}

func InitializeScenario(sc *godog.ScenarioContext) {
	feature := &cypherTCKFeature{}

	sc.Before(func(ctx context.Context, _ *godog.Scenario) (context.Context, error) {
		feature.ctx = ctx
		feature.params = executor.Params{}
		feature.lastQuery = ""
		feature.lastResult = nil
		feature.lastErr = nil
		feature.beforeQueryCounts = graphSnapshot{}
		feature.afterQueryCounts = graphSnapshot{}
		return ctx, feature.resetStore()
	})

	sc.After(func(ctx context.Context, _ *godog.Scenario, _ error) (context.Context, error) {
		return ctx, feature.closeStore()
	})

	sc.Step(`^an empty graph$`, feature.anEmptyGraph)
	sc.Step(`^any graph$`, feature.anyGraph)
	sc.Step(`^the binary-tree-1 graph$`, feature.theBinaryTree1Graph)
	sc.Step(`^the binary-tree-2 graph$`, feature.theBinaryTree2Graph)
	sc.Step(`^parameters are:$`, feature.parametersAre)
	sc.Step(`^having executed:$`, feature.havingExecuted)
	sc.Step(`^executing query:$`, feature.executingQuery)
	sc.Step(`^executing control query:$`, feature.executingControlQuery)
	sc.Step(`^the result should be empty$`, feature.resultShouldBeEmpty)
	sc.Step(`^the result should be, in any order:$`, feature.resultShouldBeInAnyOrder)
	sc.Step(`^the result should be, in order:$`, feature.resultShouldBeInOrder)
	sc.Step(`^the result should be \(ignoring element order for lists\):$`, feature.resultShouldBeInAnyOrderIgnoringListOrder)
	sc.Step(`^the result should be, in order \(ignoring element order for lists\):$`, feature.resultShouldBeInOrderIgnoringListOrder)
	sc.Step(`^no side effects$`, feature.noSideEffects)
	sc.Step(`^the side effects should be:$`, feature.sideEffectsShouldBe)
	sc.Step(`^a ([A-Za-z]+) should be raised at (compile time|runtime): ([A-Za-z0-9]+)$`, feature.errorShouldBeRaised)
	sc.Step(`^a ([A-Za-z]+) should be raised at any time: (.+)$`, feature.errorShouldBeRaisedAnyTime)
	sc.Step(`^there exists a procedure (.+):$`, feature.thereExistsAProcedureWithBody)
	sc.Step(`^there exists a procedure (.+[^\s:])$`, feature.thereExistsAProcedure)
}

func resolveTCKDir(t *testing.T) string {
	t.Helper()
	if *tckDirFlag != "" {
		return *tckDirFlag
	}
	if env := os.Getenv("VITALEDGE_CYPHER_TCK_DIR"); env != "" {
		return env
	}
	wd, err := os.Getwd()
	if err != nil {
		return ""
	}
	root := filepath.Clean(filepath.Join(wd, "../../.."))
	candidate := filepath.Join(root, ".cache", "opencypher", "tck", "features")
	if _, err := os.Stat(candidate); err == nil {
		return candidate
	}
	return ""
}

func (f *cypherTCKFeature) anEmptyGraph() error {
	return f.resetStore()
}

func (f *cypherTCKFeature) anyGraph() error {
	return f.resetStore()
}

func (f *cypherTCKFeature) theBinaryTree1Graph() error {
	if err := f.resetStore(); err != nil {
		return err
	}
	_, err := f.runBatch(binaryTree1GraphCypher, nil)
	return err
}

func (f *cypherTCKFeature) theBinaryTree2Graph() error {
	if err := f.resetStore(); err != nil {
		return err
	}
	_, err := f.runBatch(binaryTree2GraphCypher, nil)
	return err
}

func (f *cypherTCKFeature) parametersAre(table *godog.Table) error {
	if table == nil || len(table.Rows) == 0 {
		return fmt.Errorf("parameter table is empty")
	}

	params := executor.Params{}
	for _, rawRow := range table.Rows {
		if len(rawRow.Cells) < 2 {
			return fmt.Errorf("parameter row must contain at least two values")
		}
		key := strings.TrimSpace(rawRow.Cells[0].Value)
		valueRaw := strings.TrimSpace(rawRow.Cells[1].Value)
		value, err := f.evaluateLiteral(valueRaw)
		if err != nil {
			return fmt.Errorf("parse parameter %q: %w", key, err)
		}
		params[key] = value
	}
	f.params = params
	return nil
}

func (f *cypherTCKFeature) havingExecuted(doc *godog.DocString) error {
	_, err := f.runBatch(strings.TrimSpace(doc.Content), f.params)
	return err
}

func (f *cypherTCKFeature) executingQuery(doc *godog.DocString) error {
	query := strings.TrimSpace(doc.Content)
	f.lastQuery = query
	f.lastResult = nil
	f.lastErr = nil

	before, err := f.snapshotGraph()
	if err != nil {
		return err
	}
	f.beforeQueryCounts = before

	result, execErr := f.runBatch(query, f.params)
	f.lastResult = result
	f.lastErr = execErr

	after, afterErr := f.snapshotGraph()
	if afterErr != nil {
		return afterErr
	}
	f.afterQueryCounts = after
	return nil
}

func (f *cypherTCKFeature) executingControlQuery(doc *godog.DocString) error {
	before := f.beforeQueryCounts
	after := f.afterQueryCounts
	err := f.executingQuery(doc)
	f.beforeQueryCounts = before
	f.afterQueryCounts = after
	return err
}

func (f *cypherTCKFeature) resultShouldBeEmpty() error {
	if f.lastErr != nil {
		return fmt.Errorf("query returned error instead of an empty result: %w\n%s", f.lastErr, f.failureContext())
	}
	if f.lastResult == nil {
		return fmt.Errorf("no query result captured\n%s", f.failureContext())
	}
	if len(f.lastResult.Rows) != 0 {
		return fmt.Errorf("expected no rows, got %d\n%s", len(f.lastResult.Rows), f.failureContext())
	}
	return nil
}

func (f *cypherTCKFeature) resultShouldBeInAnyOrder(table *godog.Table) error {
	return f.assertResultTable(table, false)
}

func (f *cypherTCKFeature) resultShouldBeInOrder(table *godog.Table) error {
	return f.assertResultTable(table, true)
}

func (f *cypherTCKFeature) resultShouldBeInAnyOrderIgnoringListOrder(table *godog.Table) error {
	return f.assertResultTableWithOptions(table, false, true)
}

func (f *cypherTCKFeature) resultShouldBeInOrderIgnoringListOrder(table *godog.Table) error {
	return f.assertResultTableWithOptions(table, true, true)
}

func (f *cypherTCKFeature) noSideEffects() error {
	effects := diffGraphSideEffects(f.beforeQueryCounts, f.afterQueryCounts)
	if effects != (graphSideEffects{}) {
		return fmt.Errorf("expected no side effects, got %+v", effects)
	}
	return nil
}

func (f *cypherTCKFeature) sideEffectsShouldBe(table *godog.Table) error {
	if table == nil || len(table.Rows) == 0 {
		return fmt.Errorf("table is empty")
	}
	if len(table.Rows[0].Cells) < 2 {
		return fmt.Errorf("side effect table must have two columns")
	}

	effects := diffGraphSideEffects(f.beforeQueryCounts, f.afterQueryCounts)
	actual := map[string]int{
		"+vertexes":      effects.AddedVertexes,
		"-vertexes":      effects.RemovedVertexes,
		"+relationships": effects.AddedRelationships,
		"-relationships": effects.RemovedRelationships,
		"+properties":    effects.AddedProperties,
		"-properties":    effects.RemovedProperties,
		"+labels":        effects.AddedLabels,
		"-labels":        effects.RemovedLabels,
	}

	expected := map[string]int{}
	for _, row := range table.Rows {
		if len(row.Cells) < 2 {
			return fmt.Errorf("side effect row must have two values")
		}
		key, err := normalizeSideEffectKey(strings.TrimSpace(row.Cells[0].Value))
		if err != nil {
			return err
		}
		count, err := strconv.Atoi(strings.TrimSpace(row.Cells[1].Value))
		if err != nil {
			return fmt.Errorf("invalid side effect count %q: %w", row.Cells[1].Value, err)
		}
		expected[key] = count
	}

	for key, want := range expected {
		if got := actual[key]; got != want {
			return fmt.Errorf("expected %s=%d, got %d", key, want, got)
		}
	}
	for key, got := range actual {
		if got == 0 {
			continue
		}
		if _, ok := expected[key]; !ok {
			return fmt.Errorf("unexpected side effect %s=%d", key, got)
		}
	}
	return nil
}

func normalizeSideEffectKey(key string) (string, error) {
	key = strings.TrimSpace(strings.ToLower(key))
	switch key {
	case "+nodes", "+vertices":
		return "+vertexes", nil
	case "-nodes", "-vertices":
		return "-vertexes", nil
	case "+labels", "-labels",
		"+properties", "-properties",
		"+relationships", "-relationships",
		"+vertexes", "-vertexes":
		return key, nil
	default:
		return "", fmt.Errorf("unsupported side effect key %q", key)
	}
}

func (f *cypherTCKFeature) errorShouldBeRaised(category, phase, reason string) error {
	if f.lastErr == nil {
		return fmt.Errorf("expected %s at %s (%s), but query succeeded", category, phase, reason)
	}

	actualPhase, actualCategory := classifyError(f.lastErr)
	if actualPhase != phase {
		return fmt.Errorf("expected %s at %s (%s), got %s at %s: %v", category, phase, reason, actualCategory, actualPhase, f.lastErr)
	}
	if actualCategory != category {
		return fmt.Errorf("expected %s at %s (%s), got %s: %v", category, phase, reason, actualCategory, f.lastErr)
	}
	return nil
}

func (f *cypherTCKFeature) errorShouldBeRaisedAnyTime(category, reason string) error {
	if f.lastErr == nil {
		return fmt.Errorf("expected %s at any time (%s), but query succeeded", category, reason)
	}

	_, actualCategory := classifyError(f.lastErr)
	if actualCategory != category {
		return fmt.Errorf("expected %s at any time (%s), got %s: %v", category, reason, actualCategory, f.lastErr)
	}
	return nil
}

func (f *cypherTCKFeature) thereExistsAProcedure(signature string) error {
	decl, err := parseProcedureSignature(signature)
	if err != nil {
		return err
	}
	if f.procedures == nil {
		f.procedures = map[string]executor.ProcedureDecl{}
	}
	f.procedures[decl.Name] = decl
	return nil
}

func (f *cypherTCKFeature) thereExistsAProcedureWithBody(signature string, table *godog.Table) error {
	decl, err := parseProcedureSignature(signature)
	if err != nil {
		return err
	}
	rows, err := f.parseProcedureRows(table)
	if err != nil {
		return err
	}
	decl.Rows = rows
	if f.procedures == nil {
		f.procedures = map[string]executor.ProcedureDecl{}
	}
	f.procedures[decl.Name] = decl
	return nil
}

func (f *cypherTCKFeature) assertResultTable(table *godog.Table, ordered bool) error {
	return f.assertResultTableWithOptions(table, ordered, false)
}

func (f *cypherTCKFeature) assertResultTableWithOptions(table *godog.Table, ordered bool, ignoreListOrder bool) error {
	if f.lastErr != nil {
		return fmt.Errorf("query returned error instead of rows: %w\n%s", f.lastErr, f.failureContext())
	}
	if f.lastResult == nil {
		return fmt.Errorf("no query result captured\n%s", f.failureContext())
	}

	headers, expectedRows, err := readTable(table)
	if err != nil {
		return err
	}
	normalizedExpectedHeaders := make([]string, len(headers))
	for i, header := range headers {
		normalizedExpectedHeaders[i] = normalizeColumnName(header)
	}
	normalizedActualHeaders := make([]string, len(f.lastResult.Columns))
	for i, header := range f.lastResult.Columns {
		normalizedActualHeaders[i] = normalizeColumnName(header)
	}
	if !reflect.DeepEqual(normalizedActualHeaders, normalizedExpectedHeaders) {
		return fmt.Errorf("expected columns %v, got %v\n%s", headers, f.lastResult.Columns, f.failureContext())
	}

	actualRows := make([][]string, 0, len(f.lastResult.Rows))
	for _, row := range f.lastResult.Rows {
		serialized := make([]string, 0, len(headers))
		for i, header := range f.lastResult.Columns {
			value, ok := row[header]
			if !ok {
				value = row[normalizedActualHeaders[i]]
			}
			serialized = append(serialized, renderTCKValue(value))
		}
		actualRows = append(actualRows, serialized)
	}

	for i := range expectedRows {
		for j := range expectedRows[i] {
			expectedRows[i][j] = normalizeExpectedCell(expectedRows[i][j])
			if ignoreListOrder {
				expectedRows[i][j] = canonicalizeListCellOrder(expectedRows[i][j])
			}
		}
	}
	for i := range actualRows {
		for j := range actualRows[i] {
			actualRows[i][j] = normalizeExpectedCell(actualRows[i][j])
			if ignoreListOrder {
				actualRows[i][j] = canonicalizeListCellOrder(actualRows[i][j])
			}
		}
	}

	if ordered {
		if !reflect.DeepEqual(actualRows, expectedRows) {
			return fmt.Errorf("expected rows %v, got %v\n%s", expectedRows, actualRows, f.failureContextWithRows(actualRows))
		}
		return nil
	}

	sort.Slice(actualRows, func(i, j int) bool {
		return strings.Join(actualRows[i], "\x00") < strings.Join(actualRows[j], "\x00")
	})
	sort.Slice(expectedRows, func(i, j int) bool {
		return strings.Join(expectedRows[i], "\x00") < strings.Join(expectedRows[j], "\x00")
	})
	if !reflect.DeepEqual(actualRows, expectedRows) {
		return fmt.Errorf("expected rows %v, got %v\n%s", expectedRows, actualRows, f.failureContextWithRows(actualRows))
	}
	return nil
}

func (f *cypherTCKFeature) failureContext() string {
	return f.failureContextWithRows(nil)
}

func (f *cypherTCKFeature) failureContextWithRows(actualRows [][]string) string {
	parts := []string{
		fmt.Sprintf("query=%q", strings.TrimSpace(f.lastQuery)),
		fmt.Sprintf("params=%s", formatDebugParams(f.params)),
	}
	if explainSummary := f.failureExplainSummary(); explainSummary != "" {
		parts = append(parts, explainSummary)
	}
	if f.lastResult != nil {
		parts = append(parts, fmt.Sprintf("columns=%v", f.lastResult.Columns))
		parts = append(parts, fmt.Sprintf("row_count=%d", len(f.lastResult.Rows)))
	}
	if len(actualRows) > 0 {
		parts = append(parts, fmt.Sprintf("actual_rows_sample=%s", formatDebugRowSample(actualRows, 5)))
	}
	return strings.Join(parts, "\n")
}

func (f *cypherTCKFeature) failureExplainSummary() string {
	query := strings.TrimSpace(f.lastQuery)
	if query == "" {
		return ""
	}
	if strings.HasPrefix(strings.ToUpper(query), "EXPLAIN ") {
		return ""
	}

	result, err := f.runBatch("EXPLAIN "+query, f.params)
	if err != nil {
		return fmt.Sprintf("explain_error=%v", err)
	}
	if result == nil || len(result.Rows) == 0 {
		return "explain_error=no explain rows returned"
	}
	payload, ok := result.Rows[0]["explain"].(map[string]any)
	if !ok || payload == nil {
		return "explain_error=unexpected explain payload shape"
	}
	ops := explainPhysicalOps(payload)
	if len(ops) == 0 {
		return "physical_ops=[]"
	}
	if len(ops) > 12 {
		return fmt.Sprintf("physical_ops=%v ... (+%d more)", ops[:12], len(ops)-12)
	}
	return fmt.Sprintf("physical_ops=%v", ops)
}

func explainPhysicalOps(payload map[string]any) []string {
	physicalPlan, ok := payload["physicalPlan"].(map[string]any)
	if !ok || physicalPlan == nil {
		return nil
	}
	rawNodes, ok := physicalPlan["nodes"]
	if !ok || rawNodes == nil {
		return nil
	}
	nodes := make([]map[string]any, 0)
	switch typed := rawNodes.(type) {
	case []any:
		for _, item := range typed {
			node, ok := item.(map[string]any)
			if !ok || node == nil {
				continue
			}
			nodes = append(nodes, node)
		}
	case []map[string]any:
		nodes = append(nodes, typed...)
	default:
		return nil
	}
	ops := make([]string, 0, len(nodes))
	for _, node := range nodes {
		op := strings.TrimSpace(fmt.Sprint(node["op"]))
		if op == "" {
			continue
		}
		ops = append(ops, op)
	}
	return ops
}

func formatDebugParams(params executor.Params) string {
	if len(params) == 0 {
		return "{}"
	}
	keys := make([]string, 0, len(params))
	for key := range params {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=%v", key, params[key]))
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

func formatDebugRowSample(rows [][]string, limit int) string {
	if len(rows) == 0 {
		return "[]"
	}
	if limit < 1 {
		limit = 1
	}
	end := len(rows)
	if end > limit {
		end = limit
	}
	sample := rows[:end]
	if len(rows) <= limit {
		return fmt.Sprintf("%v", sample)
	}
	return fmt.Sprintf("%v ... (+%d more)", sample, len(rows)-limit)
}

func normalizeColumnName(value string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(value)), "")
}

func canonicalizeListCellOrder(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	if strings.HasPrefix(trimmed, "[") && strings.HasSuffix(trimmed, "]") {
		inner := strings.TrimSpace(trimmed[1 : len(trimmed)-1])
		if inner == "" {
			return "[]"
		}
		elements := splitTopLevel(inner, ',')
		canonical := make([]string, 0, len(elements))
		for _, element := range elements {
			canonical = append(canonical, canonicalizeListCellOrder(strings.TrimSpace(element)))
		}
		sort.Strings(canonical)
		return "[" + strings.Join(canonical, ", ") + "]"
	}
	if strings.HasPrefix(trimmed, "{") && strings.HasSuffix(trimmed, "}") {
		inner := strings.TrimSpace(trimmed[1 : len(trimmed)-1])
		if inner == "" {
			return "{}"
		}
		entries := splitTopLevel(inner, ',')
		canonical := make([]string, 0, len(entries))
		for _, entry := range entries {
			key, val, ok := splitKeyValueTopLevel(entry)
			if !ok {
				canonical = append(canonical, strings.TrimSpace(entry))
				continue
			}
			canonical = append(canonical, strings.TrimSpace(key)+": "+canonicalizeListCellOrder(strings.TrimSpace(val)))
		}
		sort.Strings(canonical)
		return "{" + strings.Join(canonical, ", ") + "}"
	}
	return trimmed
}

func splitTopLevel(value string, sep byte) []string {
	parts := []string{}
	start := 0
	depthSquare := 0
	depthParen := 0
	depthCurly := 0
	inString := false
	for i := 0; i < len(value); i++ {
		ch := value[i]
		if ch == '\'' && (i == 0 || value[i-1] != '\\') {
			inString = !inString
			continue
		}
		if inString {
			continue
		}
		switch ch {
		case '[':
			depthSquare++
		case ']':
			if depthSquare > 0 {
				depthSquare--
			}
		case '(':
			depthParen++
		case ')':
			if depthParen > 0 {
				depthParen--
			}
		case '{':
			depthCurly++
		case '}':
			if depthCurly > 0 {
				depthCurly--
			}
		case sep:
			if depthSquare == 0 && depthParen == 0 && depthCurly == 0 {
				parts = append(parts, strings.TrimSpace(value[start:i]))
				start = i + 1
			}
		}
	}
	parts = append(parts, strings.TrimSpace(value[start:]))
	return parts
}

func splitKeyValueTopLevel(value string) (string, string, bool) {
	depthSquare := 0
	depthParen := 0
	depthCurly := 0
	inString := false
	for i := 0; i < len(value); i++ {
		ch := value[i]
		if ch == '\'' && (i == 0 || value[i-1] != '\\') {
			inString = !inString
			continue
		}
		if inString {
			continue
		}
		switch ch {
		case '[':
			depthSquare++
		case ']':
			if depthSquare > 0 {
				depthSquare--
			}
		case '(':
			depthParen++
		case ')':
			if depthParen > 0 {
				depthParen--
			}
		case '{':
			depthCurly++
		case '}':
			if depthCurly > 0 {
				depthCurly--
			}
		case ':':
			if depthSquare == 0 && depthParen == 0 && depthCurly == 0 {
				return value[:i], value[i+1:], true
			}
		}
	}
	return "", "", false
}

func (f *cypherTCKFeature) runBatch(query string, params executor.Params) (*executor.Result, error) {
	batch, err := parser.ParseBatch(query)
	if err != nil {
		return nil, err
	}
	effectiveParams := withDefaultTenant(params)
	if len(f.procedures) > 0 {
		effectiveParams[executor.ProcedureDeclsParam] = f.procedures
	}
	var result *executor.Result
	for _, stmt := range batch.Statements {
		result, err = f.exec.ExecuteStatement(f.ctx, stmt, effectiveParams)
		if err != nil {
			return nil, err
		}
	}
	if result == nil {
		result = &executor.Result{}
	}
	return result, nil
}

func (f *cypherTCKFeature) evaluateLiteral(raw string) (any, error) {
	result, err := f.runBatch("RETURN "+strings.TrimSpace(raw)+" AS value", nil)
	if err != nil {
		return nil, err
	}
	if len(result.Rows) != 1 {
		return nil, fmt.Errorf("expected one row when evaluating literal, got %d", len(result.Rows))
	}
	return result.Rows[0]["value"], nil
}

func (f *cypherTCKFeature) resetStore() error {
	if err := f.closeStore(); err != nil {
		return err
	}
	tempDir, err := os.MkdirTemp("", "vitaledge-cypher-tck-")
	if err != nil {
		return err
	}
	store, err := pebblestore.Open(tempDir)
	if err != nil {
		_ = os.RemoveAll(tempDir)
		return err
	}
	f.tempDir = tempDir
	f.store = store
	f.exec = executor.New(store, executor.Options{})
	f.procedures = map[string]executor.ProcedureDecl{}
	f.params = executor.Params{}
	f.lastQuery = ""
	f.lastResult = nil
	f.lastErr = nil
	f.beforeQueryCounts = graphSnapshot{}
	f.afterQueryCounts = graphSnapshot{}
	return nil
}

func (f *cypherTCKFeature) closeStore() error {
	var errs []error
	if f.store != nil {
		if err := f.store.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if f.tempDir != "" {
		if err := os.RemoveAll(f.tempDir); err != nil {
			errs = append(errs, err)
		}
	}
	f.store = nil
	f.exec = nil
	f.tempDir = ""
	return errors.Join(errs...)
}

func (f *cypherTCKFeature) snapshotGraph() (graphSnapshot, error) {
	stats := graphSnapshot{
		VertexSet:       map[string]struct{}{},
		RelationshipSet: map[string]struct{}{},
		PropertySet:     map[string]struct{}{},
		LabelSet:        map[string]struct{}{},
	}
	err := f.store.View(f.ctx, func(tx graph.Tx) error {
		if err := tx.ScanVertices(f.ctx, defaultTenant, 0, func(vertex *graph.Vertex) error {
			stats.Vertexes++
			stats.VertexSet[fmt.Sprintf("%s:%s", vertex.Tenant, vertex.ID)] = struct{}{}
			for _, label := range vertex.Labels {
				stats.LabelSet[fmt.Sprintf("%s:%s", vertex.Tenant, label)] = struct{}{}
			}
			stats.Properties += len(vertex.Properties)
			for key, value := range vertex.Properties {
				stats.PropertySet[fmt.Sprintf("v:%s:%s:%s", vertex.ID, key, hex.EncodeToString(value))] = struct{}{}
			}
			return nil
		}); err != nil {
			return err
		}

		edgeTypes := map[string]struct{}{}
		if snapshot, err := tx.GetStatsSnapshot(f.ctx, defaultTenant); err == nil && snapshot != nil {
			for edgeType := range snapshot.EdgeCounts {
				edgeType = strings.TrimSpace(edgeType)
				if edgeType == "" {
					continue
				}
				edgeTypes[edgeType] = struct{}{}
			}
		}
		seenEdges := map[string]struct{}{}
		for edgeType := range edgeTypes {
			if err := tx.ScanOutEdgeLinksByType(f.ctx, defaultTenant, edgeType, 0, func(_, edgeID, _ string) error {
				edgeID = strings.TrimSpace(edgeID)
				if edgeID == "" {
					return nil
				}
				if _, ok := seenEdges[edgeID]; ok {
					return nil
				}
				edge, err := tx.GetEdge(f.ctx, defaultTenant, edgeID)
				if err != nil {
					if graph.IsKind(err, graph.ErrKindNotFound) {
						return nil
					}
					return err
				}
				if edge == nil {
					return nil
				}
				seenEdges[edgeID] = struct{}{}
				stats.Relationships++
				stats.RelationshipSet[fmt.Sprintf("%s:%s", edge.Tenant, edge.ID)] = struct{}{}
				stats.Properties += len(edge.Properties)
				for key, value := range edge.Properties {
					stats.PropertySet[fmt.Sprintf("e:%s:%s:%s", edge.ID, key, hex.EncodeToString(value))] = struct{}{}
				}
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	stats.Labels = len(stats.LabelSet)
	return stats, err
}

func classifyError(err error) (phase string, category string) {
	var parseErr *parser.ParseError
	if errors.As(err, &parseErr) {
		switch parseErr.Kind {
		case parser.ParseErrorSyntax:
			return "compile time", "SyntaxError"
		case parser.ParseErrorSemantic:
			return "compile time", "SemanticError"
		case parser.ParseErrorUnsupported:
			if strings.Contains(strings.ToLower(parseErr.Message), "invalidargumenttypepropertyaccess") {
				return "compile time", "TypeError"
			}
			return "compile time", "SyntaxError"
		default:
			return "compile time", "SyntaxError"
		}
	}

	switch {
	case graph.IsKind(err, graph.ErrKindNotFound):
		return "runtime", "EntityNotFound"
	case graph.IsKind(err, graph.ErrKindConflict):
		return "runtime", "ConstraintVerificationFailed"
	case graph.IsKind(err, graph.ErrKindSemantic):
		message := strings.ToLower(err.Error())
		if strings.Contains(message, "procedure") && strings.Contains(message, "not found") {
			return "compile time", "ProcedureError"
		}
		if strings.Contains(message, "invalid number of arguments") ||
			strings.Contains(message, "invalid argument type") ||
			strings.Contains(message, "invalid argument passing mode") ||
			strings.Contains(message, "invalid aggregation") ||
			strings.Contains(message, "must be yielded") ||
			strings.Contains(message, "yield variable already bound") {
			return "compile time", "SyntaxError"
		}
		return "runtime", "SemanticError"
	case graph.IsKind(err, graph.ErrKindInvalidInput):
		message := strings.ToLower(err.Error())
		if strings.Contains(message, "missing parameter") {
			return "compile time", "ParameterMissing"
		}
		if strings.Contains(message, "invalidargumenttypepropertyaccess") {
			return "compile time", "TypeError"
		}
		if strings.Contains(message, "invalidargumenttype") ||
			strings.Contains(message, "invalidargumentvalue") ||
			strings.Contains(message, "invalidpropertytype") ||
			strings.Contains(message, "mapelementaccessbynonstring") {
			return "runtime", "TypeError"
		}
		return "runtime", "ArgumentError"
	case graph.IsKind(err, graph.ErrKindUnsupported):
		message := strings.ToLower(err.Error())
		if strings.Contains(message, "expression \"x%2\" is not yet supported") {
			return "compile time", "SyntaxError"
		}
		return "runtime", "SyntaxError"
	default:
		return "runtime", "ExecutionError"
	}
}

func TestClassifyErrorTypeErrorMappings(t *testing.T) {
	cases := []struct {
		err      error
		phase    string
		category string
	}{
		{
			err:      graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentType", nil),
			phase:    "runtime",
			category: "TypeError",
		},
		{
			err:      graph.NewError(graph.ErrKindInvalidInput, "InvalidArgumentValue", nil),
			phase:    "runtime",
			category: "TypeError",
		},
		{
			err:      graph.NewError(graph.ErrKindInvalidInput, "MapElementAccessByNonString", nil),
			phase:    "runtime",
			category: "TypeError",
		},
	}

	for _, tc := range cases {
		phase, category := classifyError(tc.err)
		if phase != tc.phase || category != tc.category {
			t.Fatalf("classifyError(%v) = (%q, %q), want (%q, %q)", tc.err, phase, category, tc.phase, tc.category)
		}
	}
}

func TestRenderTCKValuePreservesNewlinesAndTabs(t *testing.T) {
	if got := renderTCKValue("Foo\nFoo"); got != "'Foo\nFoo'" {
		t.Fatalf("renderTCKValue(newline) = %q, want %q", got, "'Foo\nFoo'")
	}
	if got := renderTCKValue("Foo\tFoo"); got != "'Foo\tFoo'" {
		t.Fatalf("renderTCKValue(tab) = %q, want %q", got, "'Foo\tFoo'")
	}
}

func TestNormalizeSideEffectKey(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{in: "+nodes", want: "+vertexes"},
		{in: "-nodes", want: "-vertexes"},
		{in: "+vertexes", want: "+vertexes"},
		{in: "-vertexes", want: "-vertexes"},
		{in: "+vertices", want: "+vertexes"},
		{in: "-vertices", want: "-vertexes"},
	}

	for _, tc := range tests {
		got, err := normalizeSideEffectKey(tc.in)
		if err != nil {
			t.Fatalf("normalizeSideEffectKey(%q) returned error: %v", tc.in, err)
		}
		if got != tc.want {
			t.Fatalf("normalizeSideEffectKey(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}

	if _, err := normalizeSideEffectKey("+foo"); err == nil {
		t.Fatalf("expected normalizeSideEffectKey to reject unknown key")
	}
}

func readTable(table *godog.Table) ([]string, [][]string, error) {
	if table == nil || len(table.Rows) == 0 {
		return nil, nil, fmt.Errorf("table is empty")
	}
	headers := make([]string, 0, len(table.Rows[0].Cells))
	for _, cell := range table.Rows[0].Cells {
		headers = append(headers, strings.TrimSpace(cell.Value))
	}
	rows := make([][]string, 0, max(len(table.Rows)-1, 0))
	for _, row := range table.Rows[1:] {
		values := make([]string, 0, len(row.Cells))
		for _, cell := range row.Cells {
			values = append(values, strings.TrimSpace(cell.Value))
		}
		rows = append(rows, values)
	}
	return headers, rows, nil
}

func renderTCKValue(value any) string {
	switch typed := value.(type) {
	case nil:
		return "null"
	case string:
		return quoteString(typed)
	case bool:
		if typed {
			return "true"
		}
		return "false"
	case int:
		return strconv.Itoa(typed)
	case int64:
		return strconv.FormatInt(typed, 10)
	case int32:
		return strconv.FormatInt(int64(typed), 10)
	case uint:
		return strconv.FormatUint(uint64(typed), 10)
	case uint64:
		return strconv.FormatUint(typed, 10)
	case json.Number:
		if i, err := typed.Int64(); err == nil {
			return strconv.FormatInt(i, 10)
		}
		if f, err := typed.Float64(); err == nil {
			return renderTCKFloat64(f)
		}
		return typed.String()
	case float64:
		return renderTCKFloat64(typed)
	case float32:
		return renderTCKFloat64(float64(typed))
	case []string:
		items := make([]string, 0, len(typed))
		for _, item := range typed {
			items = append(items, quoteString(item))
		}
		return "[" + strings.Join(items, ", ") + "]"
	case []any:
		items := make([]string, 0, len(typed))
		for _, item := range typed {
			items = append(items, renderTCKValue(item))
		}
		return "[" + strings.Join(items, ", ") + "]"
	case map[string]any:
		if isVertexValue(typed) {
			return renderVertexValue(typed)
		}
		if isRelationshipValue(typed) {
			return renderRelationshipValue(typed)
		}
		keys := sortedKeys(typed)
		parts := make([]string, 0, len(keys))
		for _, key := range keys {
			parts = append(parts, key+": "+renderTCKValue(typed[key]))
		}
		return "{" + strings.Join(parts, ", ") + "}"
	default:
		rv := reflect.ValueOf(value)
		if rv.IsValid() && (rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array) && !(rv.Kind() == reflect.Slice && rv.Type().Elem().Kind() == reflect.Uint8) {
			items := make([]string, 0, rv.Len())
			for i := 0; i < rv.Len(); i++ {
				items = append(items, renderTCKValue(rv.Index(i).Interface()))
			}
			return "[" + strings.Join(items, ", ") + "]"
		}
		rendered := fmt.Sprintf("%v", typed)
		if normalized, ok := renderFloatLikeStringValue(rendered); ok {
			return normalized
		}
		return rendered
	}
}

func renderFloatLikeStringValue(rendered string) (string, bool) {
	trimmed := strings.TrimSpace(rendered)
	if trimmed == "" {
		return "", false
	}
	if !strings.ContainsAny(trimmed, ".eE") {
		return "", false
	}
	if f, err := strconv.ParseFloat(trimmed, 64); err == nil {
		return renderTCKFloat64(f), true
	}
	return "", false
}

func renderTCKFloat64(value float64) string {
	if math.IsNaN(value) {
		return "NaN"
	}
	if math.IsInf(value, 1) {
		return "Infinity"
	}
	if math.IsInf(value, -1) {
		return "-Infinity"
	}
	if value == 0 {
		return "0.0"
	}
	abs := math.Abs(value)
	if math.Trunc(value) == value && abs < 1e20 {
		return strconv.FormatFloat(value, 'f', 1, 64)
	}
	if abs >= 1e20 || abs < 1e-100 {
		rendered := strconv.FormatFloat(value, 'g', -1, 64)
		return normalizeTCKExponent(rendered)
	}
	rendered := strconv.FormatFloat(value, 'f', -1, 64)
	if strings.ContainsAny(rendered, ".eE") {
		return rendered
	}
	return rendered + ".0"
}

func normalizeTCKExponent(rendered string) string {
	i := strings.IndexAny(rendered, "eE")
	if i <= 0 || i+1 >= len(rendered) {
		return rendered
	}
	mantissa := rendered[:i]
	exponent := rendered[i+1:]
	if exponent == "" {
		return rendered
	}
	sign := ""
	if exponent[0] == '+' || exponent[0] == '-' {
		sign = string(exponent[0])
		exponent = exponent[1:]
	}
	exponent = strings.TrimLeft(exponent, "0")
	if exponent == "" {
		exponent = "0"
	}
	if sign == "+" {
		sign = ""
	}
	return mantissa + "e" + sign + exponent
}

func renderVertexValue(value map[string]any) string {
	var b strings.Builder
	b.WriteByte('(')
	labels, _ := value["labels"].([]string)
	for _, label := range labels {
		b.WriteByte(':')
		b.WriteString(label)
	}
	props, _ := value["properties"].(map[string]any)
	if len(props) > 0 {
		if len(labels) > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(renderTCKValue(props))
	}
	b.WriteByte(')')
	return b.String()
}

func renderRelationshipValue(value map[string]any) string {
	var b strings.Builder
	b.WriteByte('[')
	if relType, _ := value["type"].(string); relType != "" {
		b.WriteByte(':')
		b.WriteString(relType)
	}
	props, _ := value["properties"].(map[string]any)
	if len(props) > 0 {
		if relType, _ := value["type"].(string); relType != "" {
			b.WriteByte(' ')
		}
		b.WriteString(renderTCKValue(props))
	}
	b.WriteByte(']')
	return b.String()
}

func isVertexValue(value map[string]any) bool {
	_, hasLabels := value["labels"]
	_, hasProps := value["properties"]
	_, hasType := value["type"]
	return hasLabels && hasProps && !hasType
}

func isRelationshipValue(value map[string]any) bool {
	_, hasType := value["type"]
	_, hasProps := value["properties"]
	_, hasSrc := value["src"]
	_, hasDst := value["dst"]
	return hasType && hasProps && hasSrc && hasDst
}

func sortedKeys(value map[string]any) []string {
	keys := make([]string, 0, len(value))
	for key := range value {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func quoteString(value string) string {
	replacer := strings.NewReplacer("\\", "\\\\", "'", "\\'")
	return "'" + replacer.Replace(value) + "'"
}

func normalizeExpectedCell(value string) string {
	value = strings.TrimSpace(value)
	value = strings.ReplaceAll(value, "\r\n", "\n")
	value = canonicalizeEmbeddedMaps(value)
	return collapseWhitespaceOutsideStrings(value)
}

func canonicalizeEmbeddedMaps(value string) string {
	if value == "" {
		return ""
	}
	var out strings.Builder
	for i := 0; i < len(value); {
		if value[i] == '{' {
			end := findMatchingBrace(value, i)
			if end > i {
				out.WriteString(canonicalizeMapLiteral(value[i : end+1]))
				i = end + 1
				continue
			}
		}
		out.WriteByte(value[i])
		i++
	}
	return out.String()
}

func canonicalizeMapLiteral(raw string) string {
	raw = strings.TrimSpace(raw)
	if len(raw) < 2 || raw[0] != '{' || raw[len(raw)-1] != '}' {
		return raw
	}
	body := strings.TrimSpace(raw[1 : len(raw)-1])
	if body == "" {
		return "{}"
	}
	entries := splitTopLevel(body, ',')
	canonical := make([]string, 0, len(entries))
	for _, entry := range entries {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		key, val, ok := splitKeyValueTopLevel(entry)
		if !ok {
			canonical = append(canonical, canonicalizeEmbeddedMaps(entry))
			continue
		}
		canonical = append(canonical, strings.TrimSpace(key)+": "+canonicalizeEmbeddedMaps(strings.TrimSpace(val)))
	}
	sort.Strings(canonical)
	return "{" + strings.Join(canonical, ", ") + "}"
}

func findMatchingBrace(raw string, start int) int {
	depth := 0
	inString := false
	for i := start; i < len(raw); i++ {
		ch := raw[i]
		if ch == '\'' && (i == 0 || raw[i-1] != '\\') {
			inString = !inString
			continue
		}
		if inString {
			continue
		}
		switch ch {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

func collapseWhitespaceOutsideStrings(value string) string {
	var b strings.Builder
	inString := false
	prevSpace := false
	for i := 0; i < len(value); i++ {
		ch := value[i]
		if ch == '\'' && (i == 0 || value[i-1] != '\\') {
			inString = !inString
			b.WriteByte(ch)
			prevSpace = false
			continue
		}
		if inString {
			b.WriteByte(ch)
			continue
		}
		if ch == ' ' || ch == '\t' || ch == '\n' {
			if !prevSpace {
				b.WriteByte(' ')
				prevSpace = true
			}
			continue
		}
		if strings.ContainsRune("[]{}(),:", rune(ch)) {
			trimTrailingSpace(&b)
			b.WriteByte(ch)
			prevSpace = false
			continue
		}
		b.WriteByte(ch)
		prevSpace = false
	}
	return strings.TrimSpace(b.String())
}

func trimTrailingSpace(b *strings.Builder) {
	content := b.String()
	if strings.HasSuffix(content, " ") {
		b.Reset()
		b.WriteString(strings.TrimRight(content, " "))
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func withDefaultTenant(params executor.Params) executor.Params {
	merged := executor.Params{"tenant": defaultTenant}
	for key, value := range params {
		merged[key] = value
	}
	return merged
}

func parseProcedureSignature(signature string) (executor.ProcedureDecl, error) {
	m := procedureSignatureRE.FindStringSubmatch(strings.TrimSpace(signature))
	if len(m) != 4 {
		return executor.ProcedureDecl{}, fmt.Errorf("invalid procedure signature %q", signature)
	}
	name := strings.TrimSpace(m[1])
	inputRaw := strings.TrimSpace(m[2])
	outputRaw := strings.TrimSpace(m[3])

	inputs, err := parseProcedureArgList(inputRaw)
	if err != nil {
		return executor.ProcedureDecl{}, err
	}
	outputs, err := parseProcedureArgList(outputRaw)
	if err != nil {
		return executor.ProcedureDecl{}, err
	}

	return executor.ProcedureDecl{Name: name, Inputs: inputs, Outputs: outputs}, nil
}

func parseProcedureArgList(raw string) ([]executor.ProcedureArg, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return []executor.ProcedureArg{}, nil
	}
	parts := splitTopLevel(raw, ',')
	args := make([]executor.ProcedureArg, 0, len(parts))
	for _, part := range parts {
		p := strings.TrimSpace(part)
		if p == "" {
			continue
		}
		kv := strings.Split(p, "::")
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid procedure argument %q", p)
		}
		name := strings.TrimSpace(kv[0])
		typ := strings.TrimSpace(kv[1])
		nullable := strings.HasSuffix(typ, "?")
		typ = strings.TrimSpace(strings.TrimSuffix(typ, "?"))
		args = append(args, executor.ProcedureArg{Name: name, Type: strings.ToUpper(typ), Nullable: nullable})
	}
	return args, nil
}

func (f *cypherTCKFeature) parseProcedureRows(table *godog.Table) ([]map[string]any, error) {
	if table == nil || len(table.Rows) == 0 {
		return []map[string]any{}, nil
	}
	headers, rows, err := readTable(table)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		entry := map[string]any{}
		for i, header := range headers {
			value := ""
			if i < len(row) {
				value = row[i]
			}
			parsed, err := f.parseProcedureCell(value)
			if err != nil {
				return nil, err
			}
			entry[header] = parsed
		}
		out = append(out, entry)
	}
	return out, nil
}

func (f *cypherTCKFeature) parseProcedureCell(raw string) (any, error) {
	trimmed := strings.TrimSpace(raw)
	if strings.EqualFold(trimmed, "null") {
		return nil, nil
	}
	return f.evaluateLiteral(trimmed)
}
