package executor

import (
	"context"
	"go/ast"
	goparser "go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/paegun/vitaledge/internal/cypher/parser"
)

// Query Pipeline guardrail checklist (QP-0 baseline):
// 1) Freeze supported query-shape behavior before clause-structure migration.
// 2) Keep EXPLAIN behavior stable while pipeline internals evolve.
// 3) During migrated slices, avoid adding new raw-text semantic recovery paths.
func TestQueryPipelineBaselineSupportedShapes(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	defer func() { _ = store.Close() }()
	seedGraph(t, ctx, store)

	exec := New(store, Options{})

	t.Run("order-skip-limit-shape", func(t *testing.T) {
		stmt, err := parser.ParseStatement("MATCH (src)-[:MEMBER_OF]->(dst) WHERE id(src) = $srcID RETURN id(dst) AS dstID ORDER BY dstID DESC SKIP 1 LIMIT 1")
		if err != nil {
			t.Fatalf("parse failed: %v", err)
		}
		res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme", "srcID": "u1"})
		if err != nil {
			t.Fatalf("execute failed: %v", err)
		}
		if len(res.Rows) != 1 || res.Rows[0]["dstID"] != "g1" {
			t.Fatalf("unexpected ORDER/SKIP/LIMIT rows: %#v", res.Rows)
		}
	})

	t.Run("distinct-projection-shape", func(t *testing.T) {
		stmt, err := parser.ParseStatement("MATCH (src)-[:MEMBER_OF]->(dst) WHERE id(src) = $srcID RETURN DISTINCT id(dst) AS dstID ORDER BY dstID ASC")
		if err != nil {
			t.Fatalf("parse failed: %v", err)
		}
		res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme", "srcID": "u1"})
		if err != nil {
			t.Fatalf("execute failed: %v", err)
		}
		got := []any{res.Rows[0]["dstID"], res.Rows[1]["dstID"]}
		if !reflect.DeepEqual(got, []any{"g1", "g2"}) {
			t.Fatalf("unexpected DISTINCT projection rows: %#v", res.Rows)
		}
	})

	t.Run("return-star-projection-shape", func(t *testing.T) {
		stmt, err := parser.ParseStatement("MATCH (src { id: $srcID })-[:MEMBER_OF]->(dst) RETURN * ORDER BY dst.id ASC LIMIT 1")
		if err != nil {
			t.Fatalf("parse failed: %v", err)
		}
		res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme", "srcID": "u1"})
		if err != nil {
			t.Fatalf("execute failed: %v", err)
		}
		if len(res.Rows) != 1 {
			t.Fatalf("expected one RETURN * row, got %#v", res.Rows)
		}
		if _, ok := res.Rows[0]["src"]; !ok {
			t.Fatalf("expected RETURN * row to include src binding, got %#v", res.Rows[0])
		}
		if _, ok := res.Rows[0]["dst"]; !ok {
			t.Fatalf("expected RETURN * row to include dst binding, got %#v", res.Rows[0])
		}
	})

	t.Run("optional-match-shape", func(t *testing.T) {
		stmt, err := parser.ParseStatement("OPTIONAL MATCH (src { id: $srcID })-[:MEMBER_OF]->(dst) RETURN dst.id AS dstID")
		if err != nil {
			t.Fatalf("parse failed: %v", err)
		}
		res, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme", "srcID": "u2"})
		if err != nil {
			t.Fatalf("execute failed: %v", err)
		}
		if len(res.Rows) != 1 || res.Rows[0]["dstID"] != nil {
			t.Fatalf("unexpected OPTIONAL MATCH rows: %#v", res.Rows)
		}
	})

	t.Run("merge-sequencing-shape", func(t *testing.T) {
		stmt, err := parser.ParseStatement("MERGE (u:User {id: $id}) ON CREATE SET u.role = 'new' ON MATCH SET u.role = 'existing' RETURN u.role AS role")
		if err != nil {
			t.Fatalf("parse failed: %v", err)
		}

		first, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme", "id": "u3"})
		if err != nil {
			t.Fatalf("first execute failed: %v", err)
		}
		if len(first.Rows) != 1 || first.Rows[0]["role"] != "new" {
			t.Fatalf("unexpected ON CREATE role result: %#v", first.Rows)
		}

		second, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme", "id": "u3"})
		if err != nil {
			t.Fatalf("second execute failed: %v", err)
		}
		if len(second.Rows) != 1 || second.Rows[0]["role"] != "existing" {
			t.Fatalf("unexpected ON MATCH role result: %#v", second.Rows)
		}
	})

	t.Run("merge-map-action-sequencing-shape", func(t *testing.T) {
		stmt, err := parser.ParseStatement("MERGE (u:User {id: $id}) ON CREATE SET u += {role:'new', age: 1} ON MATCH SET u += {role:'existing'} RETURN u.role AS role")
		if err != nil {
			t.Fatalf("parse failed: %v", err)
		}

		first, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme", "id": "u4"})
		if err != nil {
			t.Fatalf("first execute failed: %v", err)
		}
		if len(first.Rows) != 1 || first.Rows[0]["role"] != "new" {
			t.Fatalf("unexpected ON CREATE map action result: %#v", first.Rows)
		}

		second, err := exec.ExecuteStatement(ctx, stmt, Params{"tenant": "acme", "id": "u4"})
		if err != nil {
			t.Fatalf("second execute failed: %v", err)
		}
		if len(second.Rows) != 1 || second.Rows[0]["role"] != "existing" {
			t.Fatalf("unexpected ON MATCH map action result: %#v", second.Rows)
		}
	})
}

func TestExplainTestsParseExplainQueries(t *testing.T) {
	testFiles, err := filepath.Glob("*_test.go")
	if err != nil {
		t.Fatalf("glob test files failed: %v", err)
	}
	if len(testFiles) == 0 {
		t.Fatalf("expected test files in executor package")
	}

	for _, file := range testFiles {
		if file == "query_pipeline_guardrails_test.go" {
			continue
		}
		srcBytes, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s failed: %v", file, err)
		}

		fset := token.NewFileSet()
		parsedFile, err := goparser.ParseFile(fset, file, srcBytes, goparser.ParseComments)
		if err != nil {
			t.Fatalf("parse %s failed: %v", file, err)
		}

		for _, decl := range parsedFile.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || !isGoTestFunc(fn) || fn.Body == nil {
				continue
			}
			parsesExplain := functionParsesExplain(fn.Body)
			if strings.Contains(fn.Name.Name, "PipelinePayload") {
				if !parsesExplain {
					t.Fatalf("PipelinePayload test must parse EXPLAIN query: %s in %s", fn.Name.Name, file)
				}
			}
		}
	}
}

func TestPipelineExplainContractAssertionsUseSharedHelpers(t *testing.T) {
	testFiles, err := filepath.Glob("*_test.go")
	if err != nil {
		t.Fatalf("glob test files failed: %v", err)
	}
	if len(testFiles) == 0 {
		t.Fatalf("expected test files in executor package")
	}

	skippedFiles := map[string]bool{
		"query_pipeline_guardrails_test.go": true,
		"explain_pipeline_contract_test.go": true,
	}

	forbiddenOmittedKeys := []string{"influencers", "indexDecisions", "runtimeStats", "warnings"}

	for _, file := range testFiles {
		if skippedFiles[file] {
			continue
		}
		srcBytes, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s failed: %v", file, err)
		}

		fset := token.NewFileSet()
		parsedFile, err := goparser.ParseFile(fset, file, srcBytes, goparser.ParseComments)
		if err != nil {
			t.Fatalf("parse %s failed: %v", file, err)
		}

		for _, decl := range parsedFile.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || !isGoTestFunc(fn) || fn.Body == nil {
				continue
			}
			if !strings.Contains(fn.Name.Name, "PipelinePayload") {
				continue
			}
			if !functionParsesExplain(fn.Body) {
				continue
			}

			if !functionCallsHelper(fn.Body, "assertPipelineExplainPayloadEnvelope") {
				t.Fatalf("PipelinePayload EXPLAIN test must call assertPipelineExplainPayloadEnvelope: %s in %s", fn.Name.Name, file)
			}
			if !functionCallsHelper(fn.Body, "assertExplainPayloadOmitsKeys") {
				t.Fatalf("PipelinePayload EXPLAIN test must call assertExplainPayloadOmitsKeys: %s in %s", fn.Name.Name, file)
			}
			missingKeys := helperCallMissingStringArgs(fn.Body, "assertExplainPayloadOmitsKeys", forbiddenOmittedKeys)
			if len(missingKeys) > 0 {
				t.Fatalf("PipelinePayload EXPLAIN test must include required omission keys %v in assertExplainPayloadOmitsKeys: %s in %s", missingKeys, fn.Name.Name, file)
			}

			if functionHasManualPipelineVersionCheck(fn.Body) {
				t.Fatalf("manual pipeline version assertion detected in %s: %s; use assertPipelineExplainPayloadEnvelope", file, fn.Name.Name)
			}
			if functionHasManualPipelineRouteCheck(fn.Body) {
				t.Fatalf("manual pipeline route assertion detected in %s: %s; use assertPipelineExplainPayloadEnvelope", file, fn.Name.Name)
			}
			for _, key := range forbiddenOmittedKeys {
				if functionHasManualPayloadOmissionCheck(fn.Body, key) {
					t.Fatalf("manual pipeline omission assertion for %s detected in %s: %s; use assertExplainPayloadOmitsKeys", key, file, fn.Name.Name)
				}
			}
		}
	}
}

func functionHasManualPipelineVersionCheck(body *ast.BlockStmt) bool {
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		if found {
			return false
		}
		ifStmt, ok := n.(*ast.IfStmt)
		if !ok {
			return true
		}
		if ifStmtHasStringCompareOnField(ifStmt, "explainPayload", "version", "v2-pipeline") {
			found = true
			return false
		}
		return true
	})
	return found
}

func functionHasManualPipelineRouteCheck(body *ast.BlockStmt) bool {
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		if found {
			return false
		}
		ifStmt, ok := n.(*ast.IfStmt)
		if !ok {
			return true
		}
		if ifStmtHasStringCompareOnField(ifStmt, "metadata", "explainRoute", "pipeline_payload") || ifStmtHasStringCompareOnField(ifStmt, "metadata", "explainRouteReason", "pipeline_payload_opt_in") {
			found = true
			return false
		}
		return true
	})
	return found
}

func functionHasManualPayloadOmissionCheck(body *ast.BlockStmt, key string) bool {
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		if found {
			return false
		}
		ifStmt, ok := n.(*ast.IfStmt)
		if !ok || ifStmt.Init == nil {
			return true
		}
		assign, ok := ifStmt.Init.(*ast.AssignStmt)
		if !ok || len(assign.Rhs) != 1 {
			return true
		}
		indexExpr, ok := assign.Rhs[0].(*ast.IndexExpr)
		if !ok {
			return true
		}
		ident, ok := indexExpr.X.(*ast.Ident)
		if !ok || ident.Name != "explainPayload" {
			return true
		}
		lit, ok := indexExpr.Index.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return true
		}
		value, err := strconv.Unquote(lit.Value)
		if err != nil {
			value = lit.Value
		}
		if value == key {
			found = true
			return false
		}
		return true
	})
	return found
}

func functionCallsHelper(body *ast.BlockStmt, helperName string) bool {
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		if found {
			return false
		}
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		ident, ok := call.Fun.(*ast.Ident)
		if ok && ident.Name == helperName {
			found = true
			return false
		}
		return true
	})
	return found
}

func helperCallMissingStringArgs(body *ast.BlockStmt, helperName string, required []string) []string {
	seen := map[string]bool{}
	ast.Inspect(body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		ident, ok := call.Fun.(*ast.Ident)
		if !ok || ident.Name != helperName {
			return true
		}
		for _, arg := range call.Args {
			lit, ok := arg.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				continue
			}
			value, err := strconv.Unquote(lit.Value)
			if err != nil {
				value = lit.Value
			}
			seen[value] = true
		}
		return true
	})

	missing := make([]string, 0, len(required))
	for _, key := range required {
		if !seen[key] {
			missing = append(missing, key)
		}
	}
	return missing
}

func ifStmtHasStringCompareOnField(ifStmt *ast.IfStmt, objectName, fieldName, expectedLiteral string) bool {
	if ifStmt == nil || ifStmt.Init == nil {
		return false
	}
	assign, ok := ifStmt.Init.(*ast.AssignStmt)
	if !ok || len(assign.Lhs) < 1 || len(assign.Rhs) < 1 {
		return false
	}
	ident, ok := assign.Lhs[0].(*ast.Ident)
	if !ok {
		return false
	}
	boundName := ident.Name
	typeAssert, ok := assign.Rhs[0].(*ast.TypeAssertExpr)
	if !ok {
		return false
	}
	indexExpr, ok := typeAssert.X.(*ast.IndexExpr)
	if !ok {
		return false
	}
	obj, ok := indexExpr.X.(*ast.Ident)
	if !ok || obj.Name != objectName {
		return false
	}
	fieldLit, ok := indexExpr.Index.(*ast.BasicLit)
	if !ok || fieldLit.Kind != token.STRING {
		return false
	}
	fieldValue, err := strconv.Unquote(fieldLit.Value)
	if err != nil {
		fieldValue = fieldLit.Value
	}
	if fieldValue != fieldName {
		return false
	}

	b, ok := ifStmt.Cond.(*ast.BinaryExpr)
	if !ok || (b.Op != token.NEQ && b.Op != token.EQL) {
		return false
	}
	leftIdent, leftIsIdent := b.X.(*ast.Ident)
	rightIdent, rightIsIdent := b.Y.(*ast.Ident)
	leftLit, leftIsLit := stringLiteralValue(b.X)
	rightLit, rightIsLit := stringLiteralValue(b.Y)

	if leftIsIdent && leftIdent.Name == boundName && rightIsLit && rightLit == expectedLiteral {
		return true
	}
	if rightIsIdent && rightIdent.Name == boundName && leftIsLit && leftLit == expectedLiteral {
		return true
	}
	return false
}

func stringLiteralValue(expr ast.Expr) (string, bool) {
	lit, ok := expr.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return "", false
	}
	value, err := strconv.Unquote(lit.Value)
	if err != nil {
		return lit.Value, true
	}
	return value, true
}

func functionParsesExplain(body *ast.BlockStmt) bool {
	explainExprByIdent := map[string]bool{}
	found := false
	hasParseStatementCall := false
	hasExplainLiteral := false
	ast.Inspect(body, func(n ast.Node) bool {
		if found {
			return false
		}
		switch node := n.(type) {
		case *ast.AssignStmt:
			for i := 0; i < len(node.Lhs) && i < len(node.Rhs); i++ {
				ident, ok := node.Lhs[i].(*ast.Ident)
				if !ok {
					continue
				}
				explainExprByIdent[ident.Name] = exprContainsExplainLiteral(node.Rhs[i], explainExprByIdent)
			}
		case *ast.ValueSpec:
			for i := 0; i < len(node.Names) && i < len(node.Values); i++ {
				explainExprByIdent[node.Names[i].Name] = exprContainsExplainLiteral(node.Values[i], explainExprByIdent)
			}
		case *ast.BasicLit:
			if node.Kind == token.STRING {
				value, err := strconv.Unquote(node.Value)
				if err != nil {
					value = node.Value
				}
				if strings.Contains(strings.ToUpper(value), "EXPLAIN") {
					hasExplainLiteral = true
				}
			}
		}

		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel == nil || sel.Sel.Name != "ParseStatement" || len(call.Args) == 0 {
			return true
		}
		hasParseStatementCall = true
		if exprContainsExplainLiteral(call.Args[0], explainExprByIdent) {
			found = true
			return false
		}
		return true
	})
	return found || (hasParseStatementCall && hasExplainLiteral)
}

func exprContainsExplainLiteral(expr ast.Expr, explainExprByIdent map[string]bool) bool {
	switch e := expr.(type) {
	case *ast.BasicLit:
		if e.Kind != token.STRING {
			return false
		}
		value, err := strconv.Unquote(e.Value)
		if err != nil {
			value = e.Value
		}
		return strings.Contains(strings.ToUpper(value), "EXPLAIN")
	case *ast.BinaryExpr:
		return exprContainsExplainLiteral(e.X, explainExprByIdent) || exprContainsExplainLiteral(e.Y, explainExprByIdent)
	case *ast.CallExpr:
		for _, arg := range e.Args {
			if exprContainsExplainLiteral(arg, explainExprByIdent) {
				return true
			}
		}
		return false
	case *ast.ParenExpr:
		return exprContainsExplainLiteral(e.X, explainExprByIdent)
	case *ast.Ident:
		return explainExprByIdent[e.Name]
	default:
		return false
	}
}

func isGoTestFunc(fn *ast.FuncDecl) bool {
	if fn.Recv != nil || fn.Name == nil || !strings.HasPrefix(fn.Name.Name, "Test") {
		return false
	}
	if fn.Type == nil || fn.Type.Params == nil || len(fn.Type.Params.List) != 1 {
		return false
	}
	paramType := fn.Type.Params.List[0].Type
	star, ok := paramType.(*ast.StarExpr)
	if !ok {
		return false
	}
	sel, ok := star.X.(*ast.SelectorExpr)
	if !ok || sel.Sel == nil || sel.Sel.Name != "T" {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	return ok && pkg.Name == "testing"
}

func TestExplainGuardrailHelperDetection(t *testing.T) {
	testCases := []struct {
		name              string
		snippet           string
		wantParsesExplain bool
	}{
		{
			name: "inline explain and explicit true route",
			snippet: `
func TestInlineExplain(t *testing.T) {
	stmt, err := parser.ParseStatement("EXPLAIN MATCH (n) RETURN n")
	_ = stmt
	_ = err
	exec := New(nil, Options{})
	_ = exec
}
`,
			wantParsesExplain: true,
		},
		{
			name: "identifier explain expression and explicit false route",
			snippet: `
func TestIdentifierExplain(t *testing.T) {
	query := "MATCH (n) RETURN n"
	explainQuery := "EXPLAIN " + query
	stmt, err := parser.ParseStatement(explainQuery)
	_ = stmt
	_ = err
	exec := New(nil, Options{})
	_ = exec
}
`,
			wantParsesExplain: true,
		},
		{
			name: "no explain parse",
			snippet: `
func TestNoExplain(t *testing.T) {
	stmt, err := parser.ParseStatement("MATCH (n) RETURN n")
	_ = stmt
	_ = err
	exec := New(nil, Options{})
	_ = exec
}
`,
			wantParsesExplain: false,
		},
		{
			name: "explain parse without explicit route",
			snippet: `
func TestExplainNoRoute(t *testing.T) {
	query := "EXPLAIN MATCH (n) RETURN n"
	stmt, err := parser.ParseStatement(query)
	_ = stmt
	_ = err
	exec := New(nil, Options{})
	_ = exec
}
`,
			wantParsesExplain: true,
		},
		{
			name: "explain parse with non-literal route value",
			snippet: `
func TestExplainNonLiteralRouteValue(t *testing.T) {
	enabled := true
	query := "EXPLAIN MATCH (n) RETURN n"
	stmt, err := parser.ParseStatement(query)
	_ = stmt
	_ = err
	exec := New(nil, Options{})
	_ = exec
}
`,
			wantParsesExplain: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			src := "package executor\nimport (\"testing\"; \"github.com/paegun/vitaledge/internal/cypher/parser\")\n" + tc.snippet
			fset := token.NewFileSet()
			parsedFile, err := goparser.ParseFile(fset, "snippet_test.go", src, goparser.ParseComments)
			if err != nil {
				t.Fatalf("parse snippet failed: %v", err)
			}
			var fn *ast.FuncDecl
			for _, decl := range parsedFile.Decls {
				candidate, ok := decl.(*ast.FuncDecl)
				if ok && candidate.Body != nil {
					fn = candidate
					break
				}
			}
			if fn == nil {
				t.Fatalf("expected function declaration with body")
			}

			if got := functionParsesExplain(fn.Body); got != tc.wantParsesExplain {
				t.Fatalf("functionParsesExplain=%v want %v", got, tc.wantParsesExplain)
			}
		})
	}
}

func TestPipelineContractGuardrailHelperDetection(t *testing.T) {
	testCases := []struct {
		name                string
		snippet             string
		wantVersionCheck    bool
		wantRouteCheck      bool
		wantInfluencersOmit bool
		wantWarningsOmit    bool
		wantEnvelopeHelper  bool
		wantOmitHelper      bool
		wantMissingOmitKeys []string
	}{
		{
			name: "manual pipeline version and route checks are detected",
			snippet: `
func TestPipelinePayloadManualRouteVersion(t *testing.T) {
	if version, _ := explainPayload["version"].(string); version != "v2-pipeline" {
		t.Fatalf("bad version")
	}
	if route, _ := metadata["explainRoute"].(string); route != "pipeline_payload" {
		t.Fatalf("bad route")
	}
	if reason, _ := metadata["explainRouteReason"].(string); reason != "pipeline_payload_opt_in" {
		t.Fatalf("bad reason")
	}
}
`,
			wantVersionCheck:    true,
			wantRouteCheck:      true,
			wantInfluencersOmit: false,
			wantWarningsOmit:    false,
			wantEnvelopeHelper:  false,
			wantOmitHelper:      false,
			wantMissingOmitKeys: []string{"influencers", "indexDecisions", "runtimeStats", "warnings"},
		},
		{
			name: "manual omission checks are detected",
			snippet: `
func TestPipelinePayloadManualOmissions(t *testing.T) {
	if _, exists := explainPayload["influencers"]; exists {
		t.Fatalf("should omit influencers")
	}
	if _, exists := explainPayload["warnings"]; exists {
		t.Fatalf("should omit warnings")
	}
}
`,
			wantVersionCheck:    false,
			wantRouteCheck:      false,
			wantInfluencersOmit: true,
			wantWarningsOmit:    true,
			wantEnvelopeHelper:  false,
			wantOmitHelper:      false,
			wantMissingOmitKeys: []string{"influencers", "indexDecisions", "runtimeStats", "warnings"},
		},
		{
			name: "shared helper usage does not trigger manual checks",
			snippet: `
func TestPipelinePayloadUsesHelpers(t *testing.T) {
	assertPipelineExplainPayloadEnvelope(t, explainPayload)
	assertExplainPayloadOmitsKeys(t, explainPayload, "influencers", "indexDecisions", "runtimeStats", "warnings")
}
`,
			wantVersionCheck:    false,
			wantRouteCheck:      false,
			wantInfluencersOmit: false,
			wantWarningsOmit:    false,
			wantEnvelopeHelper:  true,
			wantOmitHelper:      true,
			wantMissingOmitKeys: []string{},
		},
		{
			name: "route checks do not trigger pipeline route detection",
			snippet: `
func TestRouteAssertions(t *testing.T) {
	if route, _ := metadata["explainRoute"].(string); route != "payload" {
		t.Fatalf("expected payload route")
	}
}
`,
			wantVersionCheck:    false,
			wantRouteCheck:      false,
			wantInfluencersOmit: false,
			wantWarningsOmit:    false,
			wantEnvelopeHelper:  false,
			wantOmitHelper:      false,
			wantMissingOmitKeys: []string{"influencers", "indexDecisions", "runtimeStats", "warnings"},
		},
		{
			name: "unrelated literals do not trigger version or route detection",
			snippet: `
func TestUnrelatedLiterals(t *testing.T) {
	value := "v2-pipeline"
	if value == "v2-pipeline" {
		t.Log("unrelated literal")
	}
	route := "pipeline_payload"
	if route == "pipeline_payload" {
		t.Log("unrelated route")
	}
}
`,
			wantVersionCheck:    false,
			wantRouteCheck:      false,
			wantInfluencersOmit: false,
			wantWarningsOmit:    false,
			wantEnvelopeHelper:  false,
			wantOmitHelper:      false,
			wantMissingOmitKeys: []string{"influencers", "indexDecisions", "runtimeStats", "warnings"},
		},
		{
			name: "omit helper with partial keys reports missing",
			snippet: `
func TestPipelinePayloadUsesPartialOmitKeys(t *testing.T) {
	assertExplainPayloadOmitsKeys(t, explainPayload, "influencers", "warnings")
}
`,
			wantVersionCheck:    false,
			wantRouteCheck:      false,
			wantInfluencersOmit: false,
			wantWarningsOmit:    false,
			wantEnvelopeHelper:  false,
			wantOmitHelper:      true,
			wantMissingOmitKeys: []string{"indexDecisions", "runtimeStats"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			src := "package executor\nimport \"testing\"\n" + tc.snippet
			fset := token.NewFileSet()
			parsedFile, err := goparser.ParseFile(fset, "snippet_test.go", src, goparser.ParseComments)
			if err != nil {
				t.Fatalf("parse snippet failed: %v", err)
			}
			var fn *ast.FuncDecl
			for _, decl := range parsedFile.Decls {
				candidate, ok := decl.(*ast.FuncDecl)
				if ok && candidate.Body != nil {
					fn = candidate
					break
				}
			}
			if fn == nil {
				t.Fatalf("expected function declaration with body")
			}

			if got := functionHasManualPipelineVersionCheck(fn.Body); got != tc.wantVersionCheck {
				t.Fatalf("functionHasManualPipelineVersionCheck=%v want %v", got, tc.wantVersionCheck)
			}
			if got := functionHasManualPipelineRouteCheck(fn.Body); got != tc.wantRouteCheck {
				t.Fatalf("functionHasManualPipelineRouteCheck=%v want %v", got, tc.wantRouteCheck)
			}
			if got := functionHasManualPayloadOmissionCheck(fn.Body, "influencers"); got != tc.wantInfluencersOmit {
				t.Fatalf("functionHasManualPayloadOmissionCheck(influencers)=%v want %v", got, tc.wantInfluencersOmit)
			}
			if got := functionHasManualPayloadOmissionCheck(fn.Body, "warnings"); got != tc.wantWarningsOmit {
				t.Fatalf("functionHasManualPayloadOmissionCheck(warnings)=%v want %v", got, tc.wantWarningsOmit)
			}
			if got := functionCallsHelper(fn.Body, "assertPipelineExplainPayloadEnvelope"); got != tc.wantEnvelopeHelper {
				t.Fatalf("functionCallsHelper(assertPipelineExplainPayloadEnvelope)=%v want %v", got, tc.wantEnvelopeHelper)
			}
			if got := functionCallsHelper(fn.Body, "assertExplainPayloadOmitsKeys"); got != tc.wantOmitHelper {
				t.Fatalf("functionCallsHelper(assertExplainPayloadOmitsKeys)=%v want %v", got, tc.wantOmitHelper)
			}
			requiredKeys := []string{"influencers", "indexDecisions", "runtimeStats", "warnings"}
			if got := helperCallMissingStringArgs(fn.Body, "assertExplainPayloadOmitsKeys", requiredKeys); !reflect.DeepEqual(got, tc.wantMissingOmitKeys) {
				t.Fatalf("helperCallMissingStringArgs=%#v want %#v", got, tc.wantMissingOmitKeys)
			}
		})
	}
}
