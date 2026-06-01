package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	v1 "github.com/paegun/vitaledge/api/proto/vitaledge/v1"
	"github.com/paegun/vitaledge/internal/cypher"
	"github.com/paegun/vitaledge/internal/cypher/parser"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultGRPCTarget     = "127.0.0.1:7443"
	defaultTenant         = "default"
	defaultMaxColumnWidth = 80
)

type cliConfig struct {
	grpcTarget     string
	tenant         string
	timeout        time.Duration
	execute        string
	maxColumnWidth int
	loadMode       string
	loadOps        int
	loadSeed       int64
	loadPrefix     string
	loadReadLimit  int
	loadReadMinHop int
	loadReadMaxHop int
	loadReportEach int
	purge          string
	purgeBatchSize int
}

type cliState struct {
	variables map[string]any
}

type loadGenState struct {
	createdIDs []string
}

func main() {
	cfg := loadCLIConfig()
	if err := run(cfg, os.Stdin, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func loadCLIConfig() cliConfig {
	var cfg cliConfig
	flag.StringVar(&cfg.grpcTarget, "grpc-target", defaultGRPCTarget, "gRPC address for QueryService")
	flag.StringVar(&cfg.tenant, "tenant", defaultTenant, "tenant for query execution")
	flag.DurationVar(&cfg.timeout, "timeout", 5*time.Second, "request timeout")
	flag.StringVar(&cfg.execute, "execute", "", "optional one-shot statement (non-interactive)")
	flag.IntVar(&cfg.maxColumnWidth, "max-column-width", defaultMaxColumnWidth, "maximum width for rendered table columns")
	flag.StringVar(&cfg.loadMode, "load-mode", "", "optional load generator mode: write|noop-write|read")
	flag.IntVar(&cfg.loadOps, "load-ops", 1000, "number of load operations to execute")
	flag.Int64Var(&cfg.loadSeed, "load-seed", 1, "seed for deterministic load generation")
	flag.StringVar(&cfg.loadPrefix, "load-prefix", "soak", "prefix for generated identifiers in load mode")
	flag.IntVar(&cfg.loadReadLimit, "load-read-limit", 25, "LIMIT value for read load queries")
	flag.IntVar(&cfg.loadReadMinHop, "load-read-min-hop", 1, "minimum hop count for read load queries")
	flag.IntVar(&cfg.loadReadMaxHop, "load-read-max-hop", 3, "maximum hop count for read load queries")
	flag.IntVar(&cfg.loadReportEach, "load-report-each", 100, "emit progress every N load operations")
	flag.StringVar(&cfg.purge, "purge", "", "optional one-shot purge: label expression (e.g. Movie|Genre|User or * for all)")
	flag.IntVar(&cfg.purgeBatchSize, "purge-batch-size", 1000, "vertices deleted per batch during purge")
	flag.Parse()
	if cfg.maxColumnWidth < 3 {
		cfg.maxColumnWidth = 3
	}
	if cfg.loadOps < 1 {
		cfg.loadOps = 1
	}
	if cfg.loadReadLimit < 1 {
		cfg.loadReadLimit = 1
	}
	if cfg.loadReadMinHop < 1 {
		cfg.loadReadMinHop = 1
	}
	if cfg.loadReadMaxHop < cfg.loadReadMinHop {
		cfg.loadReadMaxHop = cfg.loadReadMinHop
	}
	if cfg.loadReportEach < 1 {
		cfg.loadReportEach = 1
	}
	return cfg
}

func run(cfg cliConfig, in io.Reader, out io.Writer, stderr io.Writer) error {
	conn, err := grpc.NewClient(cfg.grpcTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("grpc dial failed: %w", err)
	}
	defer func() { _ = conn.Close() }()

	client := v1.NewQueryServiceClient(conn)
	state := cliState{variables: map[string]any{}}

	if strings.TrimSpace(cfg.loadMode) != "" {
		if strings.TrimSpace(cfg.execute) != "" {
			return errors.New("--load-mode cannot be combined with --execute")
		}
		return runLoadLoop(client, cfg, out, stderr)
	}

	if strings.TrimSpace(cfg.purge) != "" {
		labelExpr, _, err := parsePurgeArgs(":purge " + strings.TrimSpace(cfg.purge))
		if err != nil {
			return fmt.Errorf("--purge: %w", err)
		}
		return runPurge(context.Background(), client, cfg, labelExpr, cfg.purgeBatchSize, out, stderr)
	}

	if strings.TrimSpace(cfg.execute) != "" {
		return runStatement(context.Background(), client, cfg, state, strings.TrimSpace(cfg.execute), out, stderr)
	}

	fmt.Fprintln(out, "(v:Vital)ﮩ٨ـﮩﮩ٨ـ[e:Edge]ﮩ٨ـﮩﮩ٨ـ()")
	fmt.Fprintln(out, "Commands: SET name=<scalar>, SET (list), UNSET name, :quit")
	return runInteractiveLoop(client, cfg, &state, in, out, stderr)
}

func runInteractiveLoop(client v1.QueryServiceClient, cfg cliConfig, state *cliState, in io.Reader, out io.Writer, stderr io.Writer) error {
	scanner := bufio.NewScanner(in)
	buf := make([]byte, 0, 1024)
	scanner.Buffer(buf, 1024*1024)

	var statementBuilder strings.Builder

	for {
		if statementBuilder.Len() == 0 {
			fmt.Fprint(out, "vitaledge> ")
		} else {
			fmt.Fprint(out, "       -> ")
		}

		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				return err
			}
			fmt.Fprintln(out)
			return nil
		}
		line := scanner.Text()
		if statementBuilder.Len() == 0 {
			normalized := strings.TrimSpace(line)
			if strings.HasPrefix(strings.ToLower(normalized), ":purge") {
				labelExpr, batchOverride, err := parsePurgeArgs(normalized)
				if err != nil {
					fmt.Fprintf(stderr, "purge error: %v\n", err)
				} else {
					batchSize := cfg.purgeBatchSize
					if batchOverride > 0 {
						batchSize = batchOverride
					}
					if err := runPurge(context.Background(), client, cfg, labelExpr, batchSize, out, stderr); err != nil {
						fmt.Fprintf(stderr, "purge error: %v\n", err)
					}
				}
				continue
			}
			handled, err := handleCLICommand(normalized, state, out)
			if err != nil {
				if errors.Is(err, io.EOF) {
					fmt.Fprintln(out, "bye")
					return nil
				}
				fmt.Fprintf(stderr, "%v\n", err)
				continue
			}
			if handled {
				continue
			}
		}

		statementBuilder.WriteString(line)
		statementBuilder.WriteString("\n")
		candidate := statementBuilder.String()

		ready, parseErr := statementReady(candidate, state.variables)
		if !ready {
			continue
		}

		if parseErr != nil {
			fmt.Fprintf(stderr, "parse error: %v\n", parseErr)
			statementBuilder.Reset()
			continue
		}

		query := strings.TrimSpace(candidate)
		statementBuilder.Reset()
		if err := runStatement(context.Background(), client, cfg, *state, query, out, stderr); err != nil {
			fmt.Fprintf(stderr, "%v\n", err)
		}
	}
}

func runStatement(parent context.Context, client v1.QueryServiceClient, cfg cliConfig, state cliState, query string, out io.Writer, stderr io.Writer) error {
	ready, _ := statementReady(query, state.variables)
	if !ready {
		return errors.New("statement is incomplete; check for unclosed quotes, comments, or delimiters")
	}

	boundQuery, err := bindVariables(query, state.variables)
	if err != nil {
		return err
	}

	if _, err := cypher.ParseBatch(boundQuery); err != nil {
		return fmt.Errorf("parse error: %w", err)
	}

	ctx, cancel := context.WithTimeout(parent, cfg.timeout)
	defer cancel()

	if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(boundQuery)), "EXPLAIN ") {
		return runExplain(ctx, client, cfg.tenant, boundQuery, out)
	}
	return runExecute(ctx, client, cfg.tenant, boundQuery, cfg.maxColumnWidth, out, stderr)
}

func runLoadLoop(client v1.QueryServiceClient, cfg cliConfig, out io.Writer, stderr io.Writer) error {
	mode := strings.ToLower(strings.TrimSpace(cfg.loadMode))
	switch mode {
	case "write", "noop-write", "read":
		// valid modes
	default:
		return fmt.Errorf("unsupported --load-mode %q (expected write|noop-write|read)", cfg.loadMode)
	}
	if mode == "write" && cfg.loadOps%2 != 0 {
		return errors.New("--load-mode=write requires an even --load-ops so CREATE/DELETE counts match")
	}

	rng := rand.New(rand.NewSource(cfg.loadSeed))
	state := &loadGenState{}
	started := time.Now()
	successCount := 0
	failureCount := 0

	fmt.Fprintf(out, "load-start mode=%s ops=%d seed=%d tenant=%s\n", mode, cfg.loadOps, cfg.loadSeed, cfg.tenant)

	for op := 1; op <= cfg.loadOps; op++ {
		query, opKind := nextLoadQuery(cfg, mode, op-1, rng, state)
		opCtx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
		resp, err := executeCypher(opCtx, client, cfg.tenant, query)
		cancel()
		if err != nil {
			failureCount++
			fmt.Fprintf(stderr, "load-op=%d kind=%s error=%v\n", op, opKind, err)
		} else {
			successCount++
			if len(resp.GetWarnings()) > 0 {
				for _, warning := range resp.GetWarnings() {
					fmt.Fprintf(stderr, "load-op=%d warning[%s]=%s\n", op, warning.GetCode(), warning.GetMessage())
				}
			}
		}
		if op%cfg.loadReportEach == 0 || op == cfg.loadOps {
			elapsed := time.Since(started)
			rate := float64(op) / elapsed.Seconds()
			fmt.Fprintf(out, "load-progress completed=%d/%d success=%d failure=%d rate=%.2f ops/sec\n", op, cfg.loadOps, successCount, failureCount, rate)
		}
	}

	elapsed := time.Since(started)
	rate := float64(cfg.loadOps) / elapsed.Seconds()
	fmt.Fprintf(out, "load-done mode=%s ops=%d success=%d failure=%d elapsed=%s rate=%.2f ops/sec\n", mode, cfg.loadOps, successCount, failureCount, elapsed.Truncate(time.Millisecond), rate)
	if failureCount > 0 {
		return fmt.Errorf("load completed with %d failed operations", failureCount)
	}
	return nil
}

func nextLoadQuery(cfg cliConfig, mode string, opIndex int, rng *rand.Rand, state *loadGenState) (string, string) {
	if state == nil {
		state = &loadGenState{}
	}
	switch mode {
	case "write":
		if opIndex%2 == 0 {
			id := fmt.Sprintf("%s-write-%d", cfg.loadPrefix, opIndex/2)
			state.createdIDs = append(state.createdIDs, id)
			return fmt.Sprintf("CREATE (:SoakVertex {id: %s, mode: 'write'})", quoteCypherString(id)), "create"
		}
		id := ""
		if len(state.createdIDs) > 0 {
			id = state.createdIDs[0]
			state.createdIDs = state.createdIDs[1:]
		}
		return fmt.Sprintf("MATCH (n:SoakVertex {id: %s}) DETACH DELETE n", quoteCypherString(id)), "delete"
	case "noop-write":
		id := fmt.Sprintf("%s-noop", cfg.loadPrefix)
		return fmt.Sprintf("CREATE (:SoakNoopVertex {id: %s, mode: 'noop-write'})", quoteCypherString(id)), "create"
	case "read":
		hop := cfg.loadReadMinHop
		if cfg.loadReadMaxHop > cfg.loadReadMinHop {
			hop += rng.Intn(cfg.loadReadMaxHop - cfg.loadReadMinHop + 1)
		}
		query := fmt.Sprintf("MATCH p=(a)-[*%d]-(b) RETURN p LIMIT %d", hop, cfg.loadReadLimit)
		return query, "read"
	default:
		return "RETURN 1", "read"
	}
}

func runExecute(ctx context.Context, client v1.QueryServiceClient, tenant string, query string, maxColumnWidth int, out io.Writer, stderr io.Writer) error {
	resp, err := executeCypher(ctx, client, tenant, query)
	if err != nil {
		return err
	}

	renderTable(out, resp.GetColumns(), resp.GetRows(), maxColumnWidth)
	if len(resp.GetWarnings()) > 0 {
		for _, warning := range resp.GetWarnings() {
			fmt.Fprintf(stderr, "warning [%s]: %s\n", warning.GetCode(), warning.GetMessage())
		}
	}
	stats := resp.GetStats()
	if stats != nil {
		fmt.Fprintf(out, "stats: rows=%d durationMs=%d\n", stats.GetRowsReturned(), stats.GetDurationMs())
	}
	return nil
}

func executeCypher(ctx context.Context, client v1.QueryServiceClient, tenant string, query string) (*v1.QueryResponse, error) {
	resp, err := client.Execute(ctx, &v1.QueryRequest{
		Tenant: tenant,
		Input:  &v1.QueryInput{Kind: &v1.QueryInput_Cypher{Cypher: query}},
		Options: &v1.RequestOptions{
			IncludeStats:    true,
			IncludeWarnings: true,
		},
		Client: &v1.ClientContext{SdkLanguage: "cli", SdkVersion: "mvp", ProtocolVersion: "v1"},
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func runExplain(ctx context.Context, client v1.QueryServiceClient, tenant string, query string, out io.Writer) error {
	resp, err := client.Explain(ctx, &v1.QueryRequest{
		Tenant: tenant,
		Input:  &v1.QueryInput{Kind: &v1.QueryInput_Cypher{Cypher: query}},
		Options: &v1.RequestOptions{
			IncludeStats:    true,
			IncludeWarnings: true,
		},
		Client: &v1.ClientContext{SdkLanguage: "cli", SdkVersion: "mvp", ProtocolVersion: "v1"},
	})
	if err != nil {
		return err
	}

	if len(resp.GetExplainJson()) > 0 {
		var decoded map[string]any
		if err := json.Unmarshal(resp.GetExplainJson(), &decoded); err == nil {
			fmt.Fprintln(out, "--- EXPLAIN JSON ---")
			if pretty, err := json.MarshalIndent(decoded, "", "  "); err == nil {
				fmt.Fprintln(out, string(pretty))
			} else {
				fmt.Fprintln(out, string(resp.GetExplainJson()))
			}
			fmt.Fprintln(out, "--- EXPLAIN NARRATIVE ---")
			renderExplainNarrative(out, decoded)
		} else {
			fmt.Fprintln(out, string(resp.GetExplainJson()))
		}
	}
	if stats := resp.GetStats(); stats != nil {
		fmt.Fprintf(out, "stats: rows=%d durationMs=%d\n", stats.GetRowsReturned(), stats.GetDurationMs())
	}
	return nil
}

func renderExplainNarrative(out io.Writer, explain map[string]any) {
	if explain == nil {
		return
	}
	fmt.Fprintln(out, "EXPLAIN")

	if query, ok := explain["query"].(map[string]any); ok {
		if text := asString(query["text"]); text != "" {
			fmt.Fprintf(out, "Query: %s\n", text)
		}
	}

	logicalPlan, _ := explain["logicalPlan"].(map[string]any)
	vertexes := asMapSlice(logicalPlan["vertexes"])
	if len(vertexes) > 0 {
		fmt.Fprintln(out, "Execution path:")
		for idx, vertex := range explainPlanPath(vertexes, asString(logicalPlan["rootVertexId"])) {
			fmt.Fprintf(out, "%d. %s", idx+1, asString(vertex["op"]))
			details := make([]string, 0, 3)
			if accessPath := asString(vertex["accessPath"]); accessPath != "" {
				details = append(details, "accessPath="+accessPath)
			}
			if impl := asString(vertex["implementation"]); impl != "" {
				details = append(details, "implementation="+impl)
			}
			if prefilters := explainNarrativePrefilterSummary(vertex); prefilters != "" {
				details = append(details, prefilters)
			}
			if predicate := asString(vertex["predicate"]); predicate != "" {
				details = append(details, "predicate="+predicate)
			}
			if len(details) > 0 {
				fmt.Fprintf(out, " (%s)", strings.Join(details, ", "))
			}
			fmt.Fprintln(out)
		}
	}

	fastPaths := asMapSlice(explain["executionStrategies"])
	if len(fastPaths) > 0 {
		fmt.Fprintln(out, "Fast paths:")
		for _, path := range fastPaths {
			name := asString(path["name"])
			if name == "" {
				name = asString(path["implementation"])
			}
			line := fmt.Sprintf("- %s", name)
			if impl := asString(path["implementation"]); impl != "" {
				line += " implementation=" + impl
			}
			if pair := asString(path["clausePair"]); pair != "" {
				line += " clausePair=" + pair
			}
			if status := asString(path["status"]); status != "" {
				line += " status=" + status
			}
			if value, ok := path["wherePrefilterCoverage"].(bool); ok {
				line += fmt.Sprintf(" wherePrefilterCoverage=%t", value)
			}
			fmt.Fprintln(out, line)
		}
	}

	indexDecisions := asMapSlice(explain["indexDecisions"])
	if len(indexDecisions) > 0 {
		fmt.Fprintln(out, "Index decisions:")
		for _, d := range indexDecisions {
			schema := asString(d["schema"])
			property := asString(d["property"])
			entityClass := asString(d["entityClass"])
			selected, _ := d["selected"].(bool)
			reason := asString(d["reason"])
			tuningImpact := asString(d["tuningImpact"])
			recommendation := asString(d["recommendation"])
			accessPath := asString(d["accessPath"])
			scanPop := d["scanPopulation"]
			selectedStr := "NO"
			if selected {
				selectedStr = "YES"
			}
			line := fmt.Sprintf("  %s.%s (%s) selected=%s reason=%s", schema, property, entityClass, selectedStr, reason)
			if accessPath != "" {
				line += " accessPath=" + accessPath
			}
			if !selected && tuningImpact != "" {
				line += fmt.Sprintf(" tuningImpact=%s recommendation=%s", tuningImpact, recommendation)
			}
			if scanPop != nil {
				line += fmt.Sprintf(" scanPopulation=%v", scanPop)
			}
			fmt.Fprintln(out, line)
		}
	}

	if costEstimate, ok := explain["costEstimate"].(map[string]any); ok && len(costEstimate) > 0 {
		fmt.Fprintln(out, "Cost estimate:")
		for k, v := range costEstimate {
			fmt.Fprintf(out, "  %s: %v\n", k, v)
		}
	}

	warnings := asMapSlice(explain["warnings"])
	if len(warnings) > 0 {
		fmt.Fprintln(out, "Warnings:")
		for _, warning := range warnings {
			code := asString(warning["code"])
			message := asString(warning["message"])
			if code == "" {
				fmt.Fprintf(out, "- %s\n", message)
				continue
			}
			fmt.Fprintf(out, "- [%s] %s\n", code, message)
		}
	}
}

func explainNarrativePrefilterSummary(vertex map[string]any) string {
	if vertex == nil {
		return ""
	}
	parts := make([]string, 0, 3)
	if numeric, _ := vertex["numericPrefilter"].(bool); numeric {
		parts = append(parts, "numeric")
	}
	if antiJoin, _ := vertex["antiJoinPrefilter"].(bool); antiJoin {
		parts = append(parts, "anti-join")
	}
	if covered, _ := vertex["wherePrefilterCoverage"].(bool); covered {
		parts = append(parts, "residual-where-covered")
	}
	if len(parts) == 0 {
		return ""
	}
	return "prefilters=" + strings.Join(parts, "+")
}

func explainPlanPath(vertexes []map[string]any, rootVertexID string) []map[string]any {
	if len(vertexes) == 0 {
		return nil
	}
	byID := make(map[string]map[string]any, len(vertexes))
	for _, vertex := range vertexes {
		id := asString(vertex["id"])
		if id != "" {
			byID[id] = vertex
		}
	}
	if rootVertexID == "" {
		rootVertexID = asString(vertexes[len(vertexes)-1]["id"])
	}
	path := make([]map[string]any, 0, len(vertexes))
	currentID := rootVertexID
	visited := map[string]struct{}{}
	for currentID != "" {
		if _, seen := visited[currentID]; seen {
			break
		}
		visited[currentID] = struct{}{}
		vertex, ok := byID[currentID]
		if !ok {
			break
		}
		path = append(path, vertex)
		children := asStringSlice(vertex["children"])
		if len(children) == 0 {
			break
		}
		currentID = children[0]
	}
	for i, j := 0, len(path)-1; i < j; i, j = i+1, j-1 {
		path[i], path[j] = path[j], path[i]
	}
	if len(path) > 0 {
		return path
	}
	return vertexes
}

func asMapSlice(value any) []map[string]any {
	switch v := value.(type) {
	case []map[string]any:
		return v
	case []any:
		out := make([]map[string]any, 0, len(v))
		for _, item := range v {
			if m, ok := item.(map[string]any); ok {
				out = append(out, m)
			}
		}
		return out
	default:
		return nil
	}
}

func asStringSlice(value any) []string {
	switch v := value.(type) {
	case []string:
		return v
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			if str := asString(item); str != "" {
				out = append(out, str)
			}
		}
		return out
	default:
		return nil
	}
}

func asString(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case json.Number:
		return v.String()
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	default:
		return ""
	}
}

func handleCLICommand(line string, state *cliState, out io.Writer) (bool, error) {
	normalized := strings.TrimSpace(line)
	if normalized == "" {
		return true, nil
	}
	if strings.EqualFold(normalized, ":quit") || strings.EqualFold(normalized, ":exit") {
		return false, io.EOF
	}
	if strings.EqualFold(normalized, "SET") || strings.EqualFold(normalized, "LIST") || strings.EqualFold(normalized, "VARS") {
		renderVariables(out, state.variables)
		return true, nil
	}
	if strings.HasPrefix(strings.ToUpper(normalized), "UNSET ") {
		name := strings.TrimSpace(normalized[len("UNSET "):])
		if !isValidVarName(name) {
			return true, fmt.Errorf("invalid variable name: %q", name)
		}
		delete(state.variables, name)
		fmt.Fprintf(out, "unset $%s\n", name)
		return true, nil
	}
	if strings.HasPrefix(strings.ToUpper(normalized), "SET ") {
		name, valueRaw, ok := strings.Cut(strings.TrimSpace(normalized[len("SET "):]), "=")
		if !ok {
			return true, errors.New("SET syntax: SET name=<scalar value>")
		}
		name = strings.TrimSpace(name)
		valueRaw = strings.TrimSpace(valueRaw)
		if !isValidVarName(name) {
			return true, fmt.Errorf("invalid variable name: %q", name)
		}
		value, err := parseVariableScalar(valueRaw)
		if err != nil {
			return true, err
		}
		state.variables[name] = value
		fmt.Fprintf(out, "set $%s = %s\n", name, formatCypherLiteral(value))
		return true, nil
	}
	return false, nil
}

func renderVariables(out io.Writer, variables map[string]any) {
	if len(variables) == 0 {
		fmt.Fprintln(out, "(no variables)")
		return
	}
	names := make([]string, 0, len(variables))
	for name := range variables {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		fmt.Fprintf(out, "$%s = %s\n", name, formatCypherLiteral(variables[name]))
	}
}

func parseVariableScalar(raw string) (any, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, errors.New("variable value cannot be empty")
	}

	lower := strings.ToLower(trimmed)
	switch lower {
	case "null":
		return nil, nil
	case "true":
		return true, nil
	case "false":
		return false, nil
	}

	if i, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
		return i, nil
	}
	if f, err := strconv.ParseFloat(trimmed, 64); err == nil {
		return f, nil
	}

	if len(trimmed) >= 2 {
		first := trimmed[0]
		last := trimmed[len(trimmed)-1]
		if first == '\'' && last == '\'' {
			inner := trimmed[1 : len(trimmed)-1]
			inner = strings.ReplaceAll(inner, `\\`, `\`)
			inner = strings.ReplaceAll(inner, `\'`, `'`)
			return inner, nil
		}
		if first == '"' && last == '"' {
			unquoted, err := strconv.Unquote(trimmed)
			if err != nil {
				return nil, fmt.Errorf("invalid quoted scalar %q: %w", raw, err)
			}
			return unquoted, nil
		}
	}

	decoder := json.NewDecoder(strings.NewReader(trimmed))
	decoder.UseNumber()
	var parsed any
	if err := decoder.Decode(&parsed); err != nil {
		return nil, fmt.Errorf("invalid scalar value %q", raw)
	}
	switch v := parsed.(type) {
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return i, nil
		}
		if f, err := v.Float64(); err == nil {
			return f, nil
		}
		return nil, fmt.Errorf("invalid numeric scalar %q", raw)
	case string, bool, nil:
		return v, nil
	default:
		return nil, fmt.Errorf("SET only supports scalar values (string/number/bool/null)")
	}
}

func isValidVarName(name string) bool {
	if name == "" {
		return false
	}
	for i, r := range name {
		if i == 0 {
			if !unicode.IsLetter(r) && r != '_' {
				return false
			}
			continue
		}
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' {
			return false
		}
	}
	return true
}

func statementReady(raw string, variables map[string]any) (bool, error) {
	if strings.TrimSpace(raw) == "" {
		return false, nil
	}
	if !lexicallyComplete(raw) {
		return false, nil
	}
	if hasContinuationSuffix(raw) {
		return false, nil
	}
	if matchLikelyNeedsContinuation(raw) {
		return false, nil
	}
	probeQuery, err := bindVariablesForReadiness(raw, variables)
	if err != nil {
		return true, err
	}
	_, err = cypher.ParseBatch(probeQuery)
	if err == nil {
		return true, nil
	}
	if likelyIncompleteParseError(err) {
		return false, nil
	}
	return true, err
}

func hasContinuationSuffix(raw string) bool {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return false
	}
	return strings.HasSuffix(trimmed, ",")
}

func matchLikelyNeedsContinuation(raw string) bool {
	words := extractUpperWords(raw)
	if !words["MATCH"] {
		return false
	}
	if words["RETURN"] || words["WITH"] || words["DELETE"] || words["DETACH"] || words["SET"] || words["REMOVE"] || words["MERGE"] || words["CREATE"] || words["CALL"] || words["UNWIND"] {
		return false
	}
	return true
}

func extractUpperWords(raw string) map[string]bool {
	var builder strings.Builder
	builder.Grow(len(raw))
	for _, r := range raw {
		if unicode.IsLetter(r) {
			builder.WriteRune(unicode.ToUpper(r))
			continue
		}
		builder.WriteByte(' ')
	}
	words := map[string]bool{}
	for _, word := range strings.Fields(builder.String()) {
		words[word] = true
	}
	return words
}

func likelyIncompleteParseError(err error) bool {
	var parseErr *parser.ParseError
	if errors.As(err, &parseErr) {
		msg := strings.ToLower(parseErr.Message)
		if strings.Contains(msg, "<eof>") || strings.Contains(msg, "unterminated") {
			return true
		}
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "<eof>") || strings.Contains(msg, "unterminated")
}

func lexicallyComplete(query string) bool {
	var (
		inSingle       bool
		inDouble       bool
		inBacktick     bool
		inLineComment  bool
		inBlockComment bool
		escaped        bool
		parenDepth     int
		bracketDepth   int
		braceDepth     int
	)

	for i := 0; i < len(query); i++ {
		ch := query[i]
		if inLineComment {
			if ch == '\n' {
				inLineComment = false
			}
			continue
		}
		if inBlockComment {
			if ch == '*' && i+1 < len(query) && query[i+1] == '/' {
				inBlockComment = false
				i++
			}
			continue
		}
		if inSingle {
			if escaped {
				escaped = false
				continue
			}
			if ch == '\\' {
				escaped = true
				continue
			}
			if ch == '\'' {
				inSingle = false
			}
			continue
		}
		if inDouble {
			if escaped {
				escaped = false
				continue
			}
			if ch == '\\' {
				escaped = true
				continue
			}
			if ch == '"' {
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

		if ch == '/' && i+1 < len(query) {
			next := query[i+1]
			if next == '/' {
				inLineComment = true
				i++
				continue
			}
			if next == '*' {
				inBlockComment = true
				i++
				continue
			}
		}

		switch ch {
		case '\'':
			inSingle = true
		case '"':
			inDouble = true
		case '`':
			inBacktick = true
		case '(':
			parenDepth++
		case ')':
			parenDepth--
		case '[':
			bracketDepth++
		case ']':
			bracketDepth--
		case '{':
			braceDepth++
		case '}':
			braceDepth--
		}
	}

	return !inSingle && !inDouble && !inBacktick && !inBlockComment && parenDepth == 0 && bracketDepth == 0 && braceDepth == 0
}

func bindVariables(query string, variables map[string]any) (string, error) {
	return bindVariablesWithPolicy(query, variables, false)
}

func bindVariablesForReadiness(query string, variables map[string]any) (string, error) {
	return bindVariablesWithPolicy(query, variables, true)
}

func bindVariablesWithPolicy(query string, variables map[string]any, fillMissingWithNull bool) (string, error) {
	if len(variables) == 0 && !fillMissingWithNull {
		return query, nil
	}

	var out strings.Builder
	out.Grow(len(query) + 32)

	var (
		inSingle       bool
		inDouble       bool
		inBacktick     bool
		inLineComment  bool
		inBlockComment bool
		escaped        bool
	)

	missing := map[string]struct{}{}

	for i := 0; i < len(query); i++ {
		ch := query[i]
		if inLineComment {
			out.WriteByte(ch)
			if ch == '\n' {
				inLineComment = false
			}
			continue
		}
		if inBlockComment {
			out.WriteByte(ch)
			if ch == '*' && i+1 < len(query) && query[i+1] == '/' {
				out.WriteByte('/')
				inBlockComment = false
				i++
			}
			continue
		}
		if inSingle {
			out.WriteByte(ch)
			if escaped {
				escaped = false
				continue
			}
			if ch == '\\' {
				escaped = true
				continue
			}
			if ch == '\'' {
				inSingle = false
			}
			continue
		}
		if inDouble {
			out.WriteByte(ch)
			if escaped {
				escaped = false
				continue
			}
			if ch == '\\' {
				escaped = true
				continue
			}
			if ch == '"' {
				inDouble = false
			}
			continue
		}
		if inBacktick {
			out.WriteByte(ch)
			if ch == '`' {
				inBacktick = false
			}
			continue
		}

		if ch == '/' && i+1 < len(query) {
			next := query[i+1]
			if next == '/' {
				out.WriteString("//")
				inLineComment = true
				i++
				continue
			}
			if next == '*' {
				out.WriteString("/*")
				inBlockComment = true
				i++
				continue
			}
		}

		switch ch {
		case '\'':
			inSingle = true
			out.WriteByte(ch)
			continue
		case '"':
			inDouble = true
			out.WriteByte(ch)
			continue
		case '`':
			inBacktick = true
			out.WriteByte(ch)
			continue
		case '$':
			start := i + 1
			end := start
			for end < len(query) {
				r := rune(query[end])
				if end == start {
					if !unicode.IsLetter(r) && r != '_' {
						break
					}
				} else if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' {
					break
				}
				end++
			}
			if end == start {
				out.WriteByte(ch)
				continue
			}
			name := query[start:end]
			value, ok := variables[name]
			if !ok {
				if fillMissingWithNull {
					out.WriteString("null")
					i = end - 1
					continue
				}
				missing[name] = struct{}{}
				out.WriteString("$")
				out.WriteString(name)
				i = end - 1
				continue
			}
			out.WriteString(formatCypherLiteral(value))
			i = end - 1
			continue
		}

		out.WriteByte(ch)
	}

	if len(missing) > 0 {
		names := make([]string, 0, len(missing))
		for name := range missing {
			names = append(names, name)
		}
		sort.Strings(names)
		return "", fmt.Errorf("missing variable values for: %s", strings.Join(names, ", "))
	}

	return out.String(), nil
}

func formatCypherLiteral(value any) string {
	switch v := value.(type) {
	case nil:
		return "null"
	case bool:
		if v {
			return "true"
		}
		return "false"
	case json.Number:
		return v.String()
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", v)
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", v)
	case string:
		return quoteCypherString(v)
	case []any:
		parts := make([]string, 0, len(v))
		for _, item := range v {
			parts = append(parts, formatCypherLiteral(item))
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case map[string]any:
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		parts := make([]string, 0, len(keys))
		for _, key := range keys {
			parts = append(parts, key+": "+formatCypherLiteral(v[key]))
		}
		return "{" + strings.Join(parts, ", ") + "}"
	default:
		return quoteCypherString(fmt.Sprintf("%v", value))
	}
}

func quoteCypherString(raw string) string {
	replacer := strings.NewReplacer(`\\`, `\\\\`, `'`, `\\'`)
	return "'" + replacer.Replace(raw) + "'"
}

func renderTable(out io.Writer, columns []string, rows []*v1.Row, maxColumnWidth int) {
	if len(columns) == 0 {
		fmt.Fprintf(out, "(rows=%d)\n", len(rows))
		return
	}
	if maxColumnWidth < 3 {
		maxColumnWidth = 3
	}

	widths := make([]int, len(columns))
	for i, column := range columns {
		widths[i] = min(max(3, len(column)), maxColumnWidth)
	}

	grid := make([][]string, 0, len(rows))
	for _, row := range rows {
		cells := make([]string, len(columns))
		for i, column := range columns {
			value := "NULL"
			if row != nil {
				if protoVal, ok := row.GetValues()[column]; ok {
					value = formatProtoValue(protoVal)
				}
			}
			cells[i] = truncateCell(value, maxColumnWidth)
			if len(cells[i]) > widths[i] {
				widths[i] = len(cells[i])
			}
		}
		grid = append(grid, cells)
	}

	for i, column := range columns {
		fmt.Fprintf(out, "%-*s", widths[i], truncateCell(column, widths[i]))
		if i < len(columns)-1 {
			fmt.Fprint(out, " | ")
		}
	}
	fmt.Fprintln(out)

	for i, width := range widths {
		fmt.Fprint(out, strings.Repeat("-", width))
		if i < len(widths)-1 {
			fmt.Fprint(out, "-+-")
		}
	}
	fmt.Fprintln(out)

	for _, row := range grid {
		for i, cell := range row {
			fmt.Fprintf(out, "%-*s", widths[i], cell)
			if i < len(row)-1 {
				fmt.Fprint(out, " | ")
			}
		}
		fmt.Fprintln(out)
	}

	fmt.Fprintf(out, "(%d rows)\n", len(rows))
}

func formatProtoValue(value *v1.Value) string {
	if value == nil || value.GetKind() == nil {
		return "NULL"
	}
	switch kind := value.GetKind().(type) {
	case *v1.Value_NullValue:
		return "NULL"
	case *v1.Value_BoolValue:
		if kind.BoolValue {
			return "true"
		}
		return "false"
	case *v1.Value_IntValue:
		return strconv.FormatInt(kind.IntValue, 10)
	case *v1.Value_DoubleValue:
		return strconv.FormatFloat(kind.DoubleValue, 'f', -1, 64)
	case *v1.Value_StringValue:
		return kind.StringValue
	case *v1.Value_BytesValue:
		if utf8.Valid(kind.BytesValue) {
			return string(kind.BytesValue)
		}
		return fmt.Sprintf("0x%x", kind.BytesValue)
	case *v1.Value_ListValue:
		parts := make([]string, 0, len(kind.ListValue.GetValues()))
		for _, item := range kind.ListValue.GetValues() {
			parts = append(parts, formatProtoValue(item))
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case *v1.Value_MapValue:
		if vertex, ok := formatProtoVertex(kind.MapValue.GetValues()); ok {
			return vertex
		}
		if edge, ok := formatProtoEdge(kind.MapValue.GetValues()); ok {
			return edge
		}
		if path, ok := formatProtoPath(kind.MapValue.GetValues()); ok {
			return path
		}
		keys := make([]string, 0, len(kind.MapValue.GetValues()))
		for key := range kind.MapValue.GetValues() {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		parts := make([]string, 0, len(keys))
		for _, key := range keys {
			parts = append(parts, key+": "+formatProtoValue(kind.MapValue.GetValues()[key]))
		}
		return "{" + strings.Join(parts, ", ") + "}"
	default:
		return "NULL"
	}
}

func formatProtoPath(values map[string]*v1.Value) (string, bool) {
	if values == nil {
		return "", false
	}
	pathFlag, hasPath := values["__path__"]
	if !hasPath {
		return "", false
	}
	if bv, ok := pathFlag.GetKind().(*v1.Value_BoolValue); !ok || !bv.BoolValue {
		return "", false
	}

	vertexesVal := values["vertexes"]
	edgesVal := values["edges"]
	dirsVal := values["directions"]

	var vertexStrs []string
	if vertexesVal != nil {
		if lv, ok := vertexesVal.GetKind().(*v1.Value_ListValue); ok {
			for _, item := range lv.ListValue.GetValues() {
				if mv, ok := item.GetKind().(*v1.Value_MapValue); ok {
					if s, ok := formatProtoVertex(mv.MapValue.GetValues()); ok {
						vertexStrs = append(vertexStrs, s)
					} else {
						vertexStrs = append(vertexStrs, "()")
					}
				} else {
					vertexStrs = append(vertexStrs, "()")
				}
			}
		}
	}

	var edgeStrs []string
	if edgesVal != nil {
		if lv, ok := edgesVal.GetKind().(*v1.Value_ListValue); ok {
			for _, item := range lv.ListValue.GetValues() {
				if mv, ok := item.GetKind().(*v1.Value_MapValue); ok {
					if s, ok := formatProtoEdge(mv.MapValue.GetValues()); ok {
						edgeStrs = append(edgeStrs, s)
					} else {
						edgeStrs = append(edgeStrs, "[]")
					}
				} else {
					edgeStrs = append(edgeStrs, "[]")
				}
			}
		}
	}

	var dirs []string
	if dirsVal != nil {
		dirs = protoValueToStringList(dirsVal)
	}

	if len(vertexStrs) == 0 {
		return "", false
	}

	var b strings.Builder
	b.WriteString(vertexStrs[0])
	for i, edgeStr := range edgeStrs {
		dir := "forward"
		if i < len(dirs) {
			dir = dirs[i]
		}
		nextVertex := "()"
		if i+1 < len(vertexStrs) {
			nextVertex = vertexStrs[i+1]
		}
		switch dir {
		case "reverse":
			b.WriteString("<-")
			b.WriteString(edgeStr)
			b.WriteString("-")
		case "undirected":
			b.WriteString("-")
			b.WriteString(edgeStr)
			b.WriteString("-")
		default: // forward
			b.WriteString("-")
			b.WriteString(edgeStr)
			b.WriteString("->")
		}
		b.WriteString(nextVertex)
	}

	return b.String(), true
}

func formatProtoVertex(values map[string]*v1.Value) (string, bool) {
	if values == nil {
		return "", false
	}
	propsValue, hasProps := values["properties"]
	_, hasLabels := values["labels"]
	if !hasProps || !hasLabels {
		return "", false
	}

	properties, ok := protoValueToMap(propsValue)
	if !ok {
		return "", false
	}
	labels := protoValueToStringList(values["labels"])
	id := protoValueToString(values["id"])

	var b strings.Builder
	b.WriteByte('(')
	if id != "" && !strings.HasPrefix(id, "auto-") {
		b.WriteString(toCypherIdentifier(id))
	}
	if len(labels) > 0 && strings.TrimSpace(labels[0]) != "" {
		b.WriteByte(':')
		b.WriteString(toCypherIdentifier(labels[0]))
	}
	if len(properties) > 0 {
		encoded, err := json.Marshal(properties)
		if err == nil {
			b.WriteByte(' ')
			b.Write(encoded)
		}
	}
	b.WriteByte(')')

	return b.String(), true
}

func formatProtoEdge(values map[string]*v1.Value) (string, bool) {
	if values == nil {
		return "", false
	}
	propsValue, hasProps := values["properties"]
	typeValue, hasType := values["type"]
	if !hasProps || !hasType {
		return "", false
	}

	properties, ok := protoValueToMap(propsValue)
	if !ok {
		return "", false
	}
	edgeType := protoValueToString(typeValue)
	id := protoValueToString(values["id"])

	var b strings.Builder
	b.WriteByte('[')
	if id != "" && !strings.Contains(id, "|") {
		b.WriteString(toCypherIdentifier(id))
	}
	if strings.TrimSpace(edgeType) != "" {
		b.WriteByte(':')
		b.WriteString(toCypherIdentifier(edgeType))
	}
	if len(properties) > 0 {
		encoded, err := json.Marshal(properties)
		if err == nil {
			b.WriteByte(' ')
			b.Write(encoded)
		}
	}
	b.WriteByte(']')

	return b.String(), true
}

func toCypherIdentifier(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	for i, r := range trimmed {
		if i == 0 {
			if !unicode.IsLetter(r) && r != '_' {
				return "`" + strings.ReplaceAll(trimmed, "`", "``") + "`"
			}
			continue
		}
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' {
			return "`" + strings.ReplaceAll(trimmed, "`", "``") + "`"
		}
	}
	return trimmed
}

func protoValueToString(value *v1.Value) string {
	if value == nil || value.GetKind() == nil {
		return ""
	}
	switch kind := value.GetKind().(type) {
	case *v1.Value_StringValue:
		return kind.StringValue
	case *v1.Value_BytesValue:
		if utf8.Valid(kind.BytesValue) {
			return string(kind.BytesValue)
		}
		return ""
	default:
		return ""
	}
}

func protoValueToStringList(value *v1.Value) []string {
	if value == nil || value.GetKind() == nil {
		return nil
	}
	switch kind := value.GetKind().(type) {
	case *v1.Value_ListValue:
		items := make([]string, 0, len(kind.ListValue.GetValues()))
		for _, item := range kind.ListValue.GetValues() {
			if str := protoValueToString(item); str != "" {
				items = append(items, str)
			}
		}
		return items
	case *v1.Value_StringValue:
		if strings.TrimSpace(kind.StringValue) == "" {
			return nil
		}
		return []string{kind.StringValue}
	default:
		return nil
	}
}

func protoValueToMap(value *v1.Value) (map[string]any, bool) {
	if value == nil || value.GetKind() == nil {
		return nil, false
	}
	kind, ok := value.GetKind().(*v1.Value_MapValue)
	if !ok || kind.MapValue == nil {
		return nil, false
	}
	out := make(map[string]any, len(kind.MapValue.GetValues()))
	for key, item := range kind.MapValue.GetValues() {
		out[key] = protoValueToAny(item)
	}
	return out, true
}

func protoValueToAny(value *v1.Value) any {
	if value == nil || value.GetKind() == nil {
		return nil
	}
	switch kind := value.GetKind().(type) {
	case *v1.Value_NullValue:
		return nil
	case *v1.Value_BoolValue:
		return kind.BoolValue
	case *v1.Value_IntValue:
		return kind.IntValue
	case *v1.Value_DoubleValue:
		return kind.DoubleValue
	case *v1.Value_StringValue:
		return kind.StringValue
	case *v1.Value_BytesValue:
		if utf8.Valid(kind.BytesValue) {
			return string(kind.BytesValue)
		}
		return fmt.Sprintf("0x%x", kind.BytesValue)
	case *v1.Value_ListValue:
		items := make([]any, 0, len(kind.ListValue.GetValues()))
		for _, item := range kind.ListValue.GetValues() {
			items = append(items, protoValueToAny(item))
		}
		return items
	case *v1.Value_MapValue:
		out := make(map[string]any, len(kind.MapValue.GetValues()))
		for key, item := range kind.MapValue.GetValues() {
			out[key] = protoValueToAny(item)
		}
		return out
	default:
		return nil
	}
}

func truncateCell(value string, width int) string {
	runes := []rune(value)
	if len(runes) <= width {
		return value
	}
	if width <= 3 {
		return string(runes[:width])
	}
	return string(runes[:width-3]) + "..."
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// parsePurgeArgs parses a ":purge <labelExpr> [<batchSize>]" command line.
// labelExpr may be a pipe-separated list of Cypher labels (e.g. "Movie|Genre|User")
// or "*" to match all vertices.  An optional trailing integer overrides the batch size.
// Returns (labelExpr, batchSizeOverride, error).  batchSizeOverride is 0 when absent.
func parsePurgeArgs(line string) (string, int, error) {
	rest := strings.TrimSpace(line)
	// Strip the ":purge" prefix (case-insensitive).
	if len(rest) < 6 {
		return "", 0, errors.New("usage: :purge <labels> [<batchSize>]  (e.g. :purge Movie|Genre|User)")
	}
	rest = strings.TrimSpace(rest[6:]) // drop ":purge"
	if rest == "" {
		return "", 0, errors.New("usage: :purge <labels> [<batchSize>]  (e.g. :purge Movie|Genre|User)")
	}

	// Last token may be an optional integer batch size override.
	batchOverride := 0
	tokens := strings.Fields(rest)
	if len(tokens) >= 2 {
		if n, err := strconv.Atoi(tokens[len(tokens)-1]); err == nil && n > 0 {
			batchOverride = n
			rest = strings.TrimSpace(strings.Join(tokens[:len(tokens)-1], " "))
		}
	}

	labelExpr := strings.TrimSpace(rest)
	if labelExpr == "" {
		return "", 0, errors.New("purge label expression must not be empty")
	}
	if err := validatePurgeLabelExpr(labelExpr); err != nil {
		return "", 0, err
	}
	return labelExpr, batchOverride, nil
}

// validatePurgeLabelExpr ensures the label expression is either "*" (all vertices)
// or a pipe-separated list of valid Cypher identifiers.
func validatePurgeLabelExpr(expr string) error {
	if expr == "*" {
		return nil
	}
	parts := strings.Split(expr, "|")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return fmt.Errorf("empty label in expression %q", expr)
		}
		if !isValidCypherIdentifier(part) {
			return fmt.Errorf("invalid label %q: must be a valid Cypher identifier", part)
		}
	}
	return nil
}

// isValidCypherIdentifier reports whether s is a valid unquoted Cypher identifier.
func isValidCypherIdentifier(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		if i == 0 {
			if !unicode.IsLetter(r) && r != '_' {
				return false
			}
		} else {
			if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' {
				return false
			}
		}
	}
	return true
}

// runPurge deletes all vertices matching labelExpr in batches, reporting progress
// after each batch.  It uses countQuery round-trips to determine when all vertices
// have been removed, so it always terminates correctly regardless of whether
// concurrent writers are active.
func runPurge(parent context.Context, client v1.QueryServiceClient, cfg cliConfig, labelExpr string, batchSize int, out io.Writer, stderr io.Writer) error {
	if batchSize <= 0 {
		batchSize = 1000
	}

	var matchClause string
	if labelExpr == "*" {
		matchClause = "MATCH (n)"
	} else {
		matchClause = "MATCH (n:" + labelExpr + ")"
	}
	countQuery := matchClause + " RETURN count(n) AS remaining"
	deleteQuery := matchClause + fmt.Sprintf(" WITH n LIMIT %d DETACH DELETE n", batchSize)

	countVertexes := func() (int64, error) {
		ctx, cancel := context.WithTimeout(parent, cfg.timeout)
		defer cancel()
		resp, err := executeCypher(ctx, client, cfg.tenant, countQuery)
		if err != nil {
			return 0, fmt.Errorf("count query failed: %w", err)
		}
		if len(resp.GetRows()) == 0 {
			return 0, nil
		}
		row := resp.GetRows()[0]
		if val, ok := row.GetValues()["remaining"]; ok {
			if iv, ok2 := val.Kind.(*v1.Value_IntValue); ok2 {
				return iv.IntValue, nil
			}
		}
		return 0, nil
	}

	initial, err := countVertexes()
	if err != nil {
		return err
	}
	if initial == 0 {
		fmt.Fprintf(out, "purge-done labels=%s total_deleted=0 batches=0\n", labelExpr)
		return nil
	}

	fmt.Fprintf(out, "purge-start labels=%s total=%d batch_size=%d\n", labelExpr, initial, batchSize)

	started := time.Now()
	totalDeleted := int64(0)
	batch := 0

	for {
		ctx, cancel := context.WithTimeout(parent, cfg.timeout)
		_, err := executeCypher(ctx, client, cfg.tenant, deleteQuery)
		cancel()
		if err != nil {
			return fmt.Errorf("purge batch %d failed: %w", batch+1, err)
		}
		batch++

		remaining, err := countVertexes()
		if err != nil {
			return fmt.Errorf("purge count after batch %d failed: %w", batch, err)
		}
		totalDeleted = initial - remaining
		elapsed := time.Since(started)
		fmt.Fprintf(out, "purge-progress batch=%d deleted=%d remaining=%d elapsed=%s\n",
			batch, totalDeleted, remaining, elapsed.Truncate(time.Millisecond))

		if remaining == 0 {
			break
		}
	}

	elapsed := time.Since(started)
	fmt.Fprintf(out, "purge-done labels=%s total_deleted=%d batches=%d elapsed=%s\n",
		labelExpr, totalDeleted, batch, elapsed.Truncate(time.Millisecond))
	return nil
}
