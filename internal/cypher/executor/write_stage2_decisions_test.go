package executor

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/spaceqraft/vitaledge/internal/cypher/ast"
	"github.com/spaceqraft/vitaledge/internal/cypher/physical"
	"github.com/spaceqraft/vitaledge/internal/graph"
)

func stage2TestPattern() directedRelationshipPattern {
	return directedRelationshipPattern{
		Left:     vertexPattern{Var: "u"},
		EdgeVar:  "r",
		EdgeType: "RATED",
		Right:    vertexPattern{Var: "m"},
	}
}

func stage2TestHints(edgeType string, edgeCount int, sourceCount int, avgOutDegree float64) physical.StatsHints {
	return physical.StatsHints{
		EdgeTypeCounts:     map[string]int{edgeType: edgeCount},
		EdgeSourceCounts:   map[string]int{edgeType: sourceCount},
		EdgeAvgOutDegree:   map[string]float64{edgeType: avgOutDegree},
		AntiProbeHitRateBy: map[string]float64{},
	}
}

func TestStage2AdaptiveProbeLimitTightensWithPersistedAvgDegree(t *testing.T) {
	hints := stage2TestHints("RATED", 1000, 500, 2.0)
	limit, tightened := stage2AdaptiveProbeLimit(stage2IndexPushdownProbeCandidateLimit, 100, hints, "RATED", nil)
	if !tightened {
		t.Fatalf("expected adaptive probe limit to tighten")
	}
	if limit != 200 {
		t.Fatalf("expected tightened probe limit 200, got %d", limit)
	}
}

func TestStage2SourceProbePolicyInputAllowsWideLowCoverage(t *testing.T) {
	hints := stage2TestHints("RATED", 1000, 100, 2.0)
	shared := stage2BuildSourceProbePolicyInput(stage2BuildSourceProbeStrategyDecisionContext(12, hints, "RATED", nil))
	if !shared.useSharedMode {
		t.Fatalf("expected shared source mode for low coverage + low degree")
	}
	wide := stage2BuildSourceProbePolicyInput(stage2BuildSourceProbeStrategyDecisionContext(95, hints, "RATED", nil))
	if wide.useSharedMode {
		t.Fatalf("expected shared source mode disabled for high coverage set")
	}
}

func TestStage2SourceCoverageHelpers(t *testing.T) {
	if !stage2HasSourcePeers(1) {
		t.Fatalf("expected positive source peer count to be valid")
	}
	if stage2HasSourcePeers(0) {
		t.Fatalf("expected zero source peer count to be invalid")
	}

	if !stage2WithinSourceScopedProbeMaxSources(stage2IndexPushdownSourceScopedProbeMaxSources) {
		t.Fatalf("expected scoped probe max threshold to be included")
	}
	if stage2WithinSourceScopedProbeMaxSources(stage2IndexPushdownSourceScopedProbeMaxSources + 1) {
		t.Fatalf("expected counts above scoped probe max threshold to be excluded")
	}

	if coverage, ok := stage2ResolveObservedSourceCoverage(10, 100); !ok || coverage != 0.1 {
		t.Fatalf("expected observed source coverage 0.1, got coverage=%v ok=%v", coverage, ok)
	}
	if _, ok := stage2ResolveObservedSourceCoverage(0, 100); ok {
		t.Fatalf("expected zero observed count to disable observed source coverage")
	}
	if _, ok := stage2ResolveObservedSourceCoverage(10, 0); ok {
		t.Fatalf("expected zero source count to disable observed source coverage")
	}
}

func TestStage2ResolveSourceProbeCoverageAndDegree(t *testing.T) {
	hints := stage2TestHints("RATED", 1000, 100, 2.5)
	coverage, avgOutDegree, ok := stage2ResolveSourceProbeCoverageAndDegree(10, hints, "RATED", nil)
	if !ok {
		t.Fatalf("expected source probe coverage+degree helper to resolve with matching hints")
	}
	if coverage != 0.1 {
		t.Fatalf("expected observed source coverage 0.1, got %v", coverage)
	}
	if avgOutDegree != 2.5 {
		t.Fatalf("expected avg out-degree 2.5, got %v", avgOutDegree)
	}

	if _, _, ok := stage2ResolveSourceProbeCoverageAndDegree(10, hints, "UNKNOWN", nil); ok {
		t.Fatalf("expected helper to fail when hint degree selectivity is unavailable")
	}
	if _, _, ok := stage2ResolveSourceProbeCoverageAndDegree(0, hints, "RATED", nil); ok {
		t.Fatalf("expected helper to fail when observed source count is non-positive")
	}
}

func TestStage2SourceProbeStrategyDecisionHelpers(t *testing.T) {
	hints := stage2TestHints("RATED", 1000, 100, 2.0)
	decisionCtx := stage2BuildSourceProbeStrategyDecisionContext(12, hints, "RATED", nil)
	if decisionCtx.sourcePeerCount != 12 || decisionCtx.edgeType != "RATED" {
		t.Fatalf("expected source-probe decision builder to preserve source count and edge type")
	}

	coverage, avgOutDegree, ok := stage2ResolveSourceProbeCoverageAndDegreeForDecision(decisionCtx)
	if !ok || coverage <= 0 || avgOutDegree <= 0 {
		t.Fatalf("expected source-probe decision coverage resolver helper to resolve valid coverage and degree")
	}

	if !stage2ResolveSharedSourceProbeDecision(decisionCtx) {
		t.Fatalf("expected source-probe shared resolver helper to enable shared mode for low coverage + low degree")
	}
	if stage2ResolvePerPeerSourceProbeDecision(decisionCtx, true) {
		t.Fatalf("expected source-probe per-peer resolver helper to disable when shared mode already selected")
	}

	wideHighDegreeHints := stage2TestHints("RATED", 1000, 100, 10.0)
	wideCtx := stage2BuildSourceProbeStrategyDecisionContext(20, wideHighDegreeHints, "RATED", nil)
	if !stage2ResolvePerPeerSourceProbeDecision(wideCtx, false) {
		t.Fatalf("expected source-probe per-peer resolver helper to enable for wide/high-degree source set")
	}

	skipHints := stage2TestHints("RATED", 2000, 100, 20.0)
	skipCtx := stage2BuildSourceProbeStrategyDecisionContext(95, skipHints, "RATED", nil)
	if !stage2ResolveWideNonRangePushdownSkipDecision(skipCtx, false) {
		t.Fatalf("expected source-probe wide non-range skip resolver helper to enable skip for high coverage + high degree")
	}
	if stage2ResolveWideNonRangePushdownSkipDecision(skipCtx, true) {
		t.Fatalf("expected numeric range shape to disable wide non-range skip resolver helper")
	}
}

func TestStage2BuildSourceProbePolicyInputPrecomputesModes(t *testing.T) {
	sharedCtx := stage2BuildSourceProbeStrategyDecisionContext(8, stage2TestHints("RATED", 1000, 100, 2.0), "RATED", nil)
	sharedInput := stage2BuildSourceProbePolicyInput(sharedCtx)
	if !sharedInput.useSharedMode || sharedInput.usePerPeerMode {
		t.Fatalf("expected scoped source set to precompute shared mode, got %#v", sharedInput)
	}
	if sharedInput.coverageResolved {
		t.Fatalf("expected scoped source set to avoid coverage resolution work, got %#v", sharedInput)
	}

	wideCtx := stage2BuildSourceProbeStrategyDecisionContext(95, stage2TestHints("RATED", 2000, 100, 20.0), "RATED", nil)
	wideInput := stage2BuildSourceProbePolicyInput(wideCtx)
	if wideInput.useSharedMode || !wideInput.usePerPeerMode {
		t.Fatalf("expected wide high-degree source set to precompute per-peer mode, got %#v", wideInput)
	}
	if !wideInput.coverageResolved || !wideInput.skipWideNonRange {
		t.Fatalf("expected wide high-degree source set to precompute coverage-driven non-range skip, got %#v", wideInput)
	}

	unknownCtx := stage2BuildSourceProbeStrategyDecisionContext(95, stage2TestHints("OTHER", 1000, 100, 2.0), "RATED", nil)
	unknownInput := stage2BuildSourceProbePolicyInput(unknownCtx)
	if unknownInput.coverageResolved {
		t.Fatalf("expected unknown edge-type hints to skip coverage resolution, got %#v", unknownInput)
	}
	if unknownInput.useSharedMode || !unknownInput.usePerPeerMode || unknownInput.skipWideNonRange {
		t.Fatalf("expected unknown edge-type hints to default to per-peer without wide non-range skip, got %#v", unknownInput)
	}
}

func TestStage2ResolveIndexPushdownCandidateLoad(t *testing.T) {
	indexedEdgesBySource := map[string][]*graph.Edge{
		"u1": {
			{SrcID: "u1", DstID: "m1", Type: "RATED"},
			{SrcID: "u1", DstID: "m2", Type: "RATED"},
		},
		"u2": {
			{SrcID: "u2", DstID: "m3", Type: "RATED"},
		},
	}

	if got := stage2CountIndexedCandidates(indexedEdgesBySource); got != 3 {
		t.Fatalf("expected indexed candidate count 3, got %d", got)
	}
	if avg := stage2ResolveAverageCandidatesPerIndexedSource(3, 2); avg != 1.5 {
		t.Fatalf("expected average-per-indexed-source 1.5, got %v", avg)
	}
	if avg := stage2ResolveAverageCandidatesPerIndexedSource(3, 0); avg != 0 {
		t.Fatalf("expected zero indexed-source count to return average 0, got %v", avg)
	}

	totalCandidates, averagePerSource := stage2ResolveIndexPushdownCandidateLoad(indexedEdgesBySource)
	if totalCandidates != 3 {
		t.Fatalf("expected resolved candidate load total 3, got %d", totalCandidates)
	}
	if averagePerSource != 1.5 {
		t.Fatalf("expected resolved candidate load average 1.5, got %v", averagePerSource)
	}
}

func TestStage2ShouldRejectIndexPushdownForHintCoverageFromIndexedSources(t *testing.T) {
	if !stage2ShouldRejectIndexPushdownForHintCoverageFromIndexedSources(95, 100, 13.0, 10.0, stage2IndexPushdownMaxIndexedCandidates/2+1) {
		t.Fatalf("expected helper to reject for high observed source coverage and candidate overload")
	}
	if stage2ShouldRejectIndexPushdownForHintCoverageFromIndexedSources(0, 100, 13.0, 10.0, stage2IndexPushdownMaxIndexedCandidates/2+1) {
		t.Fatalf("expected helper to skip rejection when observed indexed source count is not positive")
	}
	if stage2ShouldRejectIndexPushdownForHintCoverageFromIndexedSources(80, 100, 13.0, 10.0, stage2IndexPushdownMaxIndexedCandidates/2+1) {
		t.Fatalf("expected helper to skip rejection when observed source coverage is below threshold")
	}
}

func TestStage2ShouldRejectIndexPushdownForHintPolicies(t *testing.T) {
	hints := stage2TestHints("RATED", 1000, 100, 10.0)
	if !stage2ShouldRejectIndexPushdownForHintPolicies(95, 13.0, stage2IndexPushdownMaxIndexedCandidates/2+1, hints, "RATED", nil) {
		t.Fatalf("expected hint-policy helper to reject for high coverage and overloaded candidates")
	}
	if stage2ShouldRejectIndexPushdownForHintPolicies(10, 5.0, 10, hints, "RATED", nil) {
		t.Fatalf("expected hint-policy helper to allow selective low-load candidates")
	}
	if stage2ShouldRejectIndexPushdownForHintPolicies(95, 13.0, stage2IndexPushdownMaxIndexedCandidates/2+1, hints, "UNKNOWN", nil) {
		t.Fatalf("expected hint-policy helper to skip rejection when hint selectivity is unavailable")
	}
}

func TestStage2ShouldRejectIndexPushdownForHardCaps(t *testing.T) {
	if stage2ShouldRejectIndexPushdownForHardCaps(stage2IndexPushdownMaxIndexedCandidates, float64(stage2IndexPushdownMaxAverageEdgesPerSource)) {
		t.Fatalf("expected hard-cap helper to allow candidates at both caps")
	}
	if !stage2ShouldRejectIndexPushdownForHardCaps(stage2IndexPushdownMaxIndexedCandidates+1, 1.0) {
		t.Fatalf("expected hard-cap helper to reject candidate counts above cap")
	}
	if !stage2ShouldRejectIndexPushdownForHardCaps(1, float64(stage2IndexPushdownMaxAverageEdgesPerSource)+0.01) {
		t.Fatalf("expected hard-cap helper to reject average-per-source above cap")
	}
}

func TestStage2ResolveWideNonRangePushdownSkipDecisionHighCoverageHighDegree(t *testing.T) {
	hints := stage2TestHints("RATED", 2000, 100, 20.0)
	decisionCtx := stage2BuildSourceProbeStrategyDecisionContext(95, hints, "RATED", nil)
	if ok := stage2ResolveWideNonRangePushdownSkipDecision(decisionCtx, false); !ok {
		t.Fatalf("expected wide non-range pushdown skip for high coverage + high degree")
	}
	if ok := stage2ResolveWideNonRangePushdownSkipDecision(decisionCtx, true); ok {
		t.Fatalf("expected numeric range shape to bypass wide non-range skip")
	}
}

func TestShouldApplyStage2IndexPushdownUsesHintCoverageGuard(t *testing.T) {
	hints := stage2TestHints("RATED", 1000, 100, 10.0)
	indexedEdgesBySource := map[string][]*graph.Edge{}
	for i := 0; i < 95; i++ {
		sourceID := "u" + string(rune('a'+(i%26))) + string(rune('A'+((i/26)%26)))
		edges := make([]*graph.Edge, 0, 20)
		for j := 0; j < 20; j++ {
			edges = append(edges, &graph.Edge{SrcID: sourceID, DstID: "m", Type: "RATED"})
		}
		indexedEdgesBySource[sourceID] = edges
	}
	policy := buildStage2HintPolicy(hints, stage2TestPattern(), "", Params{}, 95)
	if ok := policy.shouldApplyPushdown(indexedEdgesBySource); ok {
		t.Fatalf("expected pushdown to be rejected by high coverage/high candidate guard")
	}
}

func TestShouldApplyStage2IndexPushdownAcceptsSelectiveCandidates(t *testing.T) {
	hints := stage2TestHints("RATED", 1000, 1000, 1.0)
	indexedEdgesBySource := map[string][]*graph.Edge{
		"u1": {{SrcID: "u1", DstID: "m1", Type: "RATED"}},
		"u2": {{SrcID: "u2", DstID: "m2", Type: "RATED"}},
	}
	policy := buildStage2HintPolicy(hints, stage2TestPattern(), "", Params{}, 2)
	if ok := policy.shouldApplyPushdown(indexedEdgesBySource); !ok {
		t.Fatalf("expected selective candidates to keep pushdown enabled")
	}
}

func TestStage2IndexPushdownEligibilityDecisionHelpers(t *testing.T) {
	hints := stage2TestHints("RATED", 1000, 100, 10.0)
	indexedEdgesBySource := map[string][]*graph.Edge{
		"u1": {
			{SrcID: "u1", DstID: "m1", Type: "RATED"},
			{SrcID: "u1", DstID: "m2", Type: "RATED"},
		},
		"u2": {
			{SrcID: "u2", DstID: "m3", Type: "RATED"},
		},
	}

	hintPolicy := stage2BuildIndexPushdownHintPolicyInput(hints, "RATED", nil)
	decisionCtx := stage2BuildIndexPushdownEligibilityDecisionContextFromHintPolicy(indexedEdgesBySource, hintPolicy)
	if decisionCtx.indexedSourceCount != 2 || !decisionCtx.hintPolicy.hintSelectivityResolved {
		t.Fatalf("expected index-pushdown decision builder to preserve indexed source count and resolved hint policy")
	}

	decisionCtx = stage2ResolveIndexPushdownCandidateLoadForDecision(decisionCtx)
	if decisionCtx.totalCandidates != 3 || decisionCtx.averagePerSource != 1.5 {
		t.Fatalf("expected decision load resolver helper to set total=3 avg=1.5, got total=%d avg=%v", decisionCtx.totalCandidates, decisionCtx.averagePerSource)
	}
	if stage2ShouldApplyIndexPushdownWithoutCandidates(decisionCtx) {
		t.Fatalf("expected no-candidate helper to reject positive candidate loads")
	}
	if stage2ShouldRejectIndexPushdownForHintPoliciesDecision(decisionCtx) {
		t.Fatalf("expected hint-policy decision helper to allow selective load")
	}
	if stage2ShouldRejectIndexPushdownForHardCapsDecision(decisionCtx) {
		t.Fatalf("expected hard-cap decision helper to allow load below caps")
	}

	highCoverage := map[string][]*graph.Edge{}
	for i := 0; i < 95; i++ {
		sourceID := "u" + string(rune('a'+(i%26))) + string(rune('A'+((i/26)%26)))
		edges := make([]*graph.Edge, 0, 20)
		for j := 0; j < 20; j++ {
			edges = append(edges, &graph.Edge{SrcID: sourceID, DstID: "m", Type: "RATED"})
		}
		highCoverage[sourceID] = edges
	}
	rejectCtx := stage2BuildIndexPushdownEligibilityDecisionContextFromHintPolicy(highCoverage, hintPolicy)
	rejectCtx = stage2ResolveIndexPushdownCandidateLoadForDecision(rejectCtx)
	if !stage2ShouldRejectIndexPushdownForHintPoliciesDecision(rejectCtx) {
		t.Fatalf("expected hint-policy decision helper to reject high-coverage overloaded load")
	}
	if stage2ResolveIndexPushdownEligibility(rejectCtx) {
		t.Fatalf("expected eligibility resolver helper to reject high-coverage overloaded load")
	}
}

func TestStage2BuildIndexPushdownHintPolicyInput(t *testing.T) {
	hints := stage2TestHints("RATED", 1000, 100, 10.0)
	resolved := stage2BuildIndexPushdownHintPolicyInput(hints, "RATED", nil)
	if !resolved.hintSelectivityResolved || resolved.sourceCount != 100 || resolved.avgOutDegree != 10.0 {
		t.Fatalf("expected resolved hint policy input with sourceCount=100 avgOutDegree=10.0, got %#v", resolved)
	}

	unresolved := stage2BuildIndexPushdownHintPolicyInput(hints, "UNKNOWN", nil)
	if unresolved.hintSelectivityResolved || unresolved.sourceCount != 0 || unresolved.avgOutDegree != 0 {
		t.Fatalf("expected unresolved hint policy input for unknown edge type, got %#v", unresolved)
	}
}

func TestStage2ShouldRejectIndexPushdownForHintPolicyInput(t *testing.T) {
	resolved := stage2IndexPushdownHintPolicyInput{hintSelectivityResolved: true, sourceCount: 100, avgOutDegree: 10.0}
	if !stage2ShouldRejectIndexPushdownForHintPolicyInput(95, 13.0, stage2IndexPushdownMaxIndexedCandidates/2+1, resolved) {
		t.Fatalf("expected resolved hint policy input helper to reject high coverage overloaded candidates")
	}
	if stage2ShouldRejectIndexPushdownForHintPolicyInput(10, 5.0, 10, resolved) {
		t.Fatalf("expected resolved hint policy input helper to allow selective low-load candidates")
	}

	unresolved := stage2IndexPushdownHintPolicyInput{}
	if stage2ShouldRejectIndexPushdownForHintPolicyInput(95, 13.0, stage2IndexPushdownMaxIndexedCandidates/2+1, unresolved) {
		t.Fatalf("expected unresolved hint policy input helper to skip rejection")
	}
}

func TestStage2MaxPushdownCandidatesDefaultsAndTightens(t *testing.T) {
	defaultHints := stage2TestHints("RATED", 1000, 1000, 1.0)
	if got := stage2MaxPushdownCandidates(false, defaultHints, "RATED", nil); got != stage2IndexPushdownMaxIndexedCandidates+1 {
		t.Fatalf("expected default max pushdown candidates %d, got %d", stage2IndexPushdownMaxIndexedCandidates+1, got)
	}

	highDegreeHints := stage2TestHints("RATED", 20000, 200, 100.0)
	tightened := stage2MaxPushdownCandidates(true, highDegreeHints, "RATED", nil)
	if tightened >= stage2IndexPushdownMaxIndexedCandidatesOneSidedRange+1 {
		t.Fatalf("expected high-degree hints to tighten one-sided cap, got %d", tightened)
	}
	if tightened != stage2IndexPushdownMaxIndexedCandidates/2+1 {
		t.Fatalf("expected tightened max pushdown candidates %d, got %d", stage2IndexPushdownMaxIndexedCandidates/2+1, tightened)
	}
}

func TestStage2ShouldApplyTightenedMaxPushdownCandidates(t *testing.T) {
	if stage2ShouldApplyTightenedMaxPushdownCandidates(0, 10) {
		t.Fatalf("expected non-positive tightened cap to be ignored")
	}
	if stage2ShouldApplyTightenedMaxPushdownCandidates(10, 10) {
		t.Fatalf("expected equal tightened cap to be ignored")
	}
	if !stage2ShouldApplyTightenedMaxPushdownCandidates(5, 10) {
		t.Fatalf("expected smaller positive tightened cap to apply")
	}
}

func TestStage2MaxPushdownCandidateCapHelpers(t *testing.T) {
	if got := stage2ResolveBaseMaxPushdownCandidates(); got != stage2IndexPushdownMaxIndexedCandidates+1 {
		t.Fatalf("expected base max pushdown candidates %d, got %d", stage2IndexPushdownMaxIndexedCandidates+1, got)
	}
	if got := stage2ResolveOneSidedRangeRelaxedMaxPushdownCandidates(); got != stage2IndexPushdownMaxIndexedCandidatesOneSidedRange+1 {
		t.Fatalf("expected one-sided relaxed max pushdown candidates %d, got %d", stage2IndexPushdownMaxIndexedCandidatesOneSidedRange+1, got)
	}

	if !stage2ShouldApplyOneSidedRangeRelaxedMaxPushdownCandidates(true, 20, 10) {
		t.Fatalf("expected one-sided relaxation to apply when eligible and relaxed max is larger")
	}
	if stage2ShouldApplyOneSidedRangeRelaxedMaxPushdownCandidates(false, 20, 10) {
		t.Fatalf("expected one-sided relaxation to be disabled when not eligible")
	}
	if stage2ShouldApplyOneSidedRangeRelaxedMaxPushdownCandidates(true, 10, 20) {
		t.Fatalf("expected one-sided relaxation to be disabled when relaxed max is not larger")
	}

	if got := stage2ResolveHighDegreeThresholdForHintTightening(); got != float64(stage2IndexPushdownMaxAverageEdgesPerSource)*2 {
		t.Fatalf("expected high-degree threshold %v, got %v", float64(stage2IndexPushdownMaxAverageEdgesPerSource)*2, got)
	}
	if got := stage2ResolveHintTightenedMaxPushdownCandidates(); got != stage2IndexPushdownMaxIndexedCandidates/2+1 {
		t.Fatalf("expected hint-tightened max pushdown candidates %d, got %d", stage2IndexPushdownMaxIndexedCandidates/2+1, got)
	}

	if !stage2ShouldApplyHintTightenedMaxPushdownCandidates(100, 50, 10, 20) {
		t.Fatalf("expected hint-tightening helper to apply when avg-out-degree exceeds threshold and tightened cap is stricter")
	}
	if stage2ShouldApplyHintTightenedMaxPushdownCandidates(40, 50, 10, 20) {
		t.Fatalf("expected hint-tightening helper to skip when avg-out-degree is below threshold")
	}
	if stage2ShouldApplyHintTightenedMaxPushdownCandidates(100, 50, 25, 20) {
		t.Fatalf("expected hint-tightening helper to skip when tightened cap is not stricter")
	}
}

func TestStage2MaxPushdownDecisionHelpers(t *testing.T) {
	defaultHints := stage2TestHints("RATED", 1000, 1000, 1.0)
	decisionCtx := stage2BuildMaxPushdownDecisionContext(false, defaultHints, "RATED", nil)
	if decisionCtx.edgeType != "RATED" || decisionCtx.oneSidedNumericRangeEligible {
		t.Fatalf("expected max-pushdown decision builder to preserve edge-type and one-sided eligibility")
	}
	if decisionCtx.maxPushdownCandidates != stage2IndexPushdownMaxIndexedCandidates+1 {
		t.Fatalf("expected max-pushdown decision builder to seed base cap")
	}

	relaxedCtx := stage2BuildMaxPushdownDecisionContext(true, defaultHints, "RATED", nil)
	relaxedCtx = stage2ApplyOneSidedRangeRelaxedMaxPushdownCandidates(relaxedCtx)
	if relaxedCtx.maxPushdownCandidates != stage2IndexPushdownMaxIndexedCandidatesOneSidedRange+1 {
		t.Fatalf("expected one-sided relaxation helper to raise cap")
	}

	avgOutDegree, ok := stage2ResolveHintAvgOutDegreeForMaxPushdown(decisionCtx)
	if !ok || avgOutDegree <= 0 {
		t.Fatalf("expected hint-degree resolver helper to resolve avg out degree")
	}

	highDegreeHints := stage2TestHints("RATED", 20000, 200, 100.0)
	tightenedCtx := stage2BuildMaxPushdownDecisionContext(true, highDegreeHints, "RATED", nil)
	tightenedCtx = stage2ApplyOneSidedRangeRelaxedMaxPushdownCandidates(tightenedCtx)
	tightenedCtx = stage2ApplyHintTightenedMaxPushdownCandidates(tightenedCtx, 100.0)
	if tightenedCtx.maxPushdownCandidates != stage2IndexPushdownMaxIndexedCandidates/2+1 {
		t.Fatalf("expected hint-tightening helper to apply stricter cap")
	}

	if got := stage2ResolveMaxPushdownDecision(stage2BuildMaxPushdownDecisionContext(true, highDegreeHints, "RATED", nil)); got != stage2IndexPushdownMaxIndexedCandidates/2+1 {
		t.Fatalf("expected max-pushdown decision resolver helper to produce tightened cap, got %d", got)
	}
}

func TestStage2ShouldPreferSharedSourceProbeForCoverageAndDegree(t *testing.T) {
	if !stage2ShouldPreferSharedSourceProbeForCoverageAndDegree(0.20, 2.0) {
		t.Fatalf("expected low coverage + low degree to prefer shared-source probe")
	}
	if stage2ShouldPreferSharedSourceProbeForCoverageAndDegree(0.50, 2.0) {
		t.Fatalf("expected high coverage to avoid shared-source preference")
	}
	if stage2ShouldPreferSharedSourceProbeForCoverageAndDegree(0.20, float64(stage2IndexPushdownAdaptiveProbeEdgesPerSource)+1) {
		t.Fatalf("expected high degree to avoid shared-source preference")
	}
}

func TestStage2ShouldSkipWideNonRangeForCoverageAndDegree(t *testing.T) {
	if !stage2ShouldSkipWideNonRangeForCoverageAndDegree(0.95, float64(stage2IndexPushdownMaxAverageEdgesPerSource)+1) {
		t.Fatalf("expected high coverage + high degree to skip wide non-range pushdown")
	}
	if stage2ShouldSkipWideNonRangeForCoverageAndDegree(0.80, float64(stage2IndexPushdownMaxAverageEdgesPerSource)+1) {
		t.Fatalf("expected lower coverage to avoid wide non-range skip")
	}
	if stage2ShouldSkipWideNonRangeForCoverageAndDegree(0.95, float64(stage2IndexPushdownMaxAverageEdgesPerSource)) {
		t.Fatalf("expected non-high degree to avoid wide non-range skip")
	}
}

func TestStage2ShouldRejectIndexPushdownForHintCoverage(t *testing.T) {
	if !stage2ShouldRejectIndexPushdownForHintCoverage(0.95, 13.0, 10.0, stage2IndexPushdownMaxIndexedCandidates/2+1) {
		t.Fatalf("expected high coverage + high per-source load + high candidate count to reject pushdown")
	}
	if stage2ShouldRejectIndexPushdownForHintCoverage(0.80, 13.0, 10.0, stage2IndexPushdownMaxIndexedCandidates/2+1) {
		t.Fatalf("expected lower coverage to avoid rejection")
	}
	if stage2ShouldRejectIndexPushdownForHintCoverage(0.95, 12.0, 10.0, stage2IndexPushdownMaxIndexedCandidates/2+1) {
		t.Fatalf("expected insufficient per-source load to avoid rejection")
	}
	if stage2ShouldRejectIndexPushdownForHintCoverage(0.95, 13.0, 10.0, stage2IndexPushdownMaxIndexedCandidates/2) {
		t.Fatalf("expected insufficient candidate count to avoid rejection")
	}
}

func TestStage2ShouldRejectIndexPushdownForAvgOutDegreeOverload(t *testing.T) {
	if !stage2ShouldRejectIndexPushdownForAvgOutDegreeOverload(21.0, 10.0) {
		t.Fatalf("expected avg-out-degree overload to reject pushdown")
	}
	if stage2ShouldRejectIndexPushdownForAvgOutDegreeOverload(15.0, 10.0) {
		t.Fatalf("expected non-overloaded average-per-source to avoid rejection")
	}
	if stage2ShouldRejectIndexPushdownForAvgOutDegreeOverload(21.0, 0.0) {
		t.Fatalf("expected non-positive avg-out-degree to avoid rejection")
	}
}

func TestStage2ShouldRejectIndexPushdownForCandidateCap(t *testing.T) {
	if stage2ShouldRejectIndexPushdownForCandidateCap(stage2IndexPushdownMaxIndexedCandidates) {
		t.Fatalf("expected candidate count at cap to avoid rejection")
	}
	if !stage2ShouldRejectIndexPushdownForCandidateCap(stage2IndexPushdownMaxIndexedCandidates + 1) {
		t.Fatalf("expected candidate count above cap to reject pushdown")
	}
}

func TestStage2ShouldRejectIndexPushdownForAveragePerSourceCap(t *testing.T) {
	if stage2ShouldRejectIndexPushdownForAveragePerSourceCap(float64(stage2IndexPushdownMaxAverageEdgesPerSource)) {
		t.Fatalf("expected average-per-source at cap to avoid rejection")
	}
	if !stage2ShouldRejectIndexPushdownForAveragePerSourceCap(float64(stage2IndexPushdownMaxAverageEdgesPerSource) + 0.01) {
		t.Fatalf("expected average-per-source above cap to reject pushdown")
	}
}

func TestStage2ShouldApplyIndexPushdownForNoCandidates(t *testing.T) {
	if !stage2ShouldApplyIndexPushdownForNoCandidates(0) {
		t.Fatalf("expected zero candidates to keep pushdown eligible")
	}
	if !stage2ShouldApplyIndexPushdownForNoCandidates(-1) {
		t.Fatalf("expected negative candidate count to keep pushdown eligible")
	}
	if stage2ShouldApplyIndexPushdownForNoCandidates(1) {
		t.Fatalf("expected positive candidate count to continue eligibility checks")
	}
}

func TestStage2ShouldApplyIndexPushdownForNoIndexedSources(t *testing.T) {
	if !stage2ShouldApplyIndexPushdownForNoIndexedSources(map[string][]*graph.Edge{}) {
		t.Fatalf("expected empty indexed source map to keep pushdown eligible")
	}
	if stage2ShouldApplyIndexPushdownForNoIndexedSources(map[string][]*graph.Edge{"u1": {}}) {
		t.Fatalf("expected non-empty indexed source map to continue eligibility checks")
	}
}

func TestStage2HasHintDegreeSelectivityTypes(t *testing.T) {
	if stage2HasHintDegreeSelectivityTypes(nil) {
		t.Fatalf("expected nil type list to be treated as empty")
	}
	if stage2HasHintDegreeSelectivityTypes([]string{}) {
		t.Fatalf("expected empty type list to be treated as empty")
	}
	if !stage2HasHintDegreeSelectivityTypes([]string{"RATED"}) {
		t.Fatalf("expected non-empty type list to be accepted")
	}
}

func TestStage2HasHintDegreeSelectivityCounts(t *testing.T) {
	if stage2HasHintDegreeSelectivityCounts(0, 1) {
		t.Fatalf("expected zero source count to be rejected")
	}
	if stage2HasHintDegreeSelectivityCounts(1, 0) {
		t.Fatalf("expected zero edge count to be rejected")
	}
	if !stage2HasHintDegreeSelectivityCounts(1, 1) {
		t.Fatalf("expected positive source and edge counts to be accepted")
	}
}

func TestStage2CollectHintDegreeSelectivityTypes(t *testing.T) {
	types := stage2CollectHintDegreeSelectivityTypes(" RATED ", []string{"", " FRIEND ", ""})
	if len(types) != 2 {
		t.Fatalf("expected two collected types, got %d", len(types))
	}
	if types[0] != "RATED" {
		t.Fatalf("expected normalized primary edge type, got %q", types[0])
	}
	if types[1] != "FRIEND" {
		t.Fatalf("expected normalized any-of edge type, got %q", types[1])
	}
}

func TestStage2ShouldSkipSeenHintDegreeSelectivityType(t *testing.T) {
	seen := map[string]struct{}{"RATED": {}}
	if !stage2ShouldSkipSeenHintDegreeSelectivityType(seen, "RATED") {
		t.Fatalf("expected previously seen type to be skipped")
	}
	if stage2ShouldSkipSeenHintDegreeSelectivityType(seen, "FRIEND") {
		t.Fatalf("expected unseen type to be processed")
	}
}

func TestStage2HintDegreeSelectivityAggregationHelpers(t *testing.T) {
	if normalized := stage2NormalizeHintDegreeSelectivityType(" rated "); normalized != "RATED" {
		t.Fatalf("expected normalized type RATED, got %q", normalized)
	}

	seen := map[string]struct{}{}
	if normalized, include := stage2ResolveHintDegreeSelectivityTypeForAggregation(seen, " rated "); !include || normalized != "RATED" {
		t.Fatalf("expected first normalized type to be included, got normalized=%q include=%v", normalized, include)
	}
	if normalized, include := stage2ResolveHintDegreeSelectivityTypeForAggregation(seen, "RATED"); include || normalized != "" {
		t.Fatalf("expected duplicate type to be excluded, got normalized=%q include=%v", normalized, include)
	}
	if normalized, include := stage2ResolveHintDegreeSelectivityTypeForAggregation(seen, "   "); include || normalized != "" {
		t.Fatalf("expected empty normalized type to be excluded, got normalized=%q include=%v", normalized, include)
	}

	hints := physical.StatsHints{
		EdgeTypeCounts:   map[string]int{"RATED": 40},
		EdgeSourceCounts: map[string]int{"RATED": 10},
	}
	sources, edges := stage2AccumulateHintDegreeSelectivityCounts(5, 20, hints, "RATED")
	if sources != 15 || edges != 60 {
		t.Fatalf("expected accumulated counts sources=15 edges=60, got sources=%d edges=%d", sources, edges)
	}

	if avg, ok := stage2ResolveAggregatedHintDegreeSelectivityAverage(10, 50); !ok || avg != 5.0 {
		t.Fatalf("expected aggregated average 5.0, got avg=%v ok=%v", avg, ok)
	}
	if _, ok := stage2ResolveAggregatedHintDegreeSelectivityAverage(0, 50); ok {
		t.Fatalf("expected aggregated average resolution to fail with non-positive source counts")
	}

	if avg, ok := stage2ResolveHintDegreeSelectivityAverage(stage2TestHints("RATED", 1000, 100, 2.5), "RATED", nil, 100, 1000); !ok || avg != 2.5 {
		t.Fatalf("expected direct average override 2.5, got avg=%v ok=%v", avg, ok)
	}
	if avg, ok := stage2ResolveHintDegreeSelectivityAverage(stage2TestHints("RATED", 1000, 100, 0), "RATED", []string{"FRIEND"}, 10, 50); !ok || avg != 5.0 {
		t.Fatalf("expected aggregated fallback average 5.0, got avg=%v ok=%v", avg, ok)
	}
}

func TestStage2HintDegreeSelectivityDecisionHelpers(t *testing.T) {
	hints := physical.StatsHints{
		EdgeTypeCounts: map[string]int{
			"RATED":  80,
			"FRIEND": 20,
		},
		EdgeSourceCounts: map[string]int{
			"RATED":  20,
			"FRIEND": 10,
		},
		EdgeAvgOutDegree: map[string]float64{},
	}
	decisionCtx := stage2BuildHintDegreeSelectivityDecisionContext(hints, "rated", []string{"RATED", " friend "})
	if decisionCtx.edgeType != "rated" || len(decisionCtx.types) != 3 {
		t.Fatalf("expected hint-degree decision builder to preserve edge type and collect candidate types")
	}

	if normalized, include := stage2ResolveIncludedHintDegreeSelectivityType(decisionCtx, "rated"); !include || normalized != "RATED" {
		t.Fatalf("expected include-type helper to normalize and include first type")
	}
	if normalized, include := stage2ResolveIncludedHintDegreeSelectivityType(decisionCtx, "RATED"); include || normalized != "" {
		t.Fatalf("expected include-type helper to exclude duplicate normalized type")
	}

	accCtx := stage2AccumulateHintDegreeSelectivityForDecision(decisionCtx, "RATED")
	if accCtx.totalSourceCount != 20 || accCtx.totalEdgeCount != 80 {
		t.Fatalf("expected accumulate-for-decision helper to update totals to sources=20 edges=80")
	}

	if avg, ok := stage2ResolveHintDegreeSelectivityAverageForDecision(accCtx); !ok || avg <= 0 {
		t.Fatalf("expected average-for-decision helper to resolve positive average")
	}

	sourceCount, avgOutDegree, ok := stage2ResolveHintDegreeSelectivityDecision(stage2BuildHintDegreeSelectivityDecisionContext(hints, "rated", []string{"RATED", " friend "}))
	if !ok {
		t.Fatalf("expected hint-degree decision resolver helper to succeed")
	}
	if sourceCount != 30 {
		t.Fatalf("expected decision resolver helper to aggregate distinct source count 30, got %d", sourceCount)
	}
	if avgOutDegree != (100.0 / 30.0) {
		t.Fatalf("expected decision resolver helper average %v, got %v", 100.0/30.0, avgOutDegree)
	}
}

func TestStage2HintDegreeSelectivityAggregatesDistinctTypes(t *testing.T) {
	hints := physical.StatsHints{
		EdgeTypeCounts: map[string]int{
			"RATED":  80,
			"FRIEND": 20,
		},
		EdgeSourceCounts: map[string]int{
			"RATED":  20,
			"FRIEND": 10,
		},
		EdgeAvgOutDegree: map[string]float64{},
	}

	sourceCount, avgOutDegree, ok := stage2HintDegreeSelectivity(hints, "rated", []string{"RATED", " friend "})
	if !ok {
		t.Fatalf("expected selectivity resolution to succeed with aggregated distinct types")
	}
	if sourceCount != 30 {
		t.Fatalf("expected aggregated distinct source count 30, got %d", sourceCount)
	}
	if avgOutDegree != (100.0 / 30.0) {
		t.Fatalf("expected aggregated average %v, got %v", 100.0/30.0, avgOutDegree)
	}
}

func TestStage2ShouldUseDirectHintDegreeAverage(t *testing.T) {
	if !stage2ShouldUseDirectHintDegreeAverage(1.5, nil) {
		t.Fatalf("expected positive direct average with no any-of override")
	}
	if stage2ShouldUseDirectHintDegreeAverage(0, nil) {
		t.Fatalf("expected non-positive direct average to be rejected")
	}
	if stage2ShouldUseDirectHintDegreeAverage(1.5, []string{"RATED"}) {
		t.Fatalf("expected any-of edge types to disable direct average")
	}
}

func TestStage2ResolveDirectHintDegreeSelectivityAverage(t *testing.T) {
	hints := stage2TestHints("RATED", 1000, 100, 2.5)
	if avg, ok := stage2ResolveDirectHintDegreeSelectivityAverage(hints, " rated ", nil); !ok || avg != 2.5 {
		t.Fatalf("expected direct average lookup to resolve normalized edge type")
	}
	if _, ok := stage2ResolveDirectHintDegreeSelectivityAverage(hints, "rated", []string{"FRIEND"}); ok {
		t.Fatalf("expected any-of edge types to disable direct average resolution")
	}
	if _, ok := stage2ResolveDirectHintDegreeSelectivityAverage(hints, "unknown", nil); ok {
		t.Fatalf("expected missing edge type average to fail resolution")
	}
}

func TestStage2SourceProbePolicyInputPerPeerDecision(t *testing.T) {
	hints := stage2TestHints("RATED", 1000, 100, 10.0)
	sharedCtx := stage2BuildSourceProbeStrategyDecisionContext(20, hints, "RATED", nil)
	if ok := stage2ResolvePerPeerSourceProbeDecision(sharedCtx, true); ok {
		t.Fatalf("expected per-peer probe disabled when shared source filter is available")
	}
	withinScopedCtx := stage2BuildSourceProbeStrategyDecisionContext(8, hints, "RATED", nil)
	if ok := stage2ResolvePerPeerSourceProbeDecision(withinScopedCtx, false); ok {
		t.Fatalf("expected per-peer probe disabled at or below scoped source threshold")
	}
	if ok := stage2ResolvePerPeerSourceProbeDecision(sharedCtx, false); !ok {
		t.Fatalf("expected per-peer probe enabled for wide/high-degree source set")
	}
}

func TestBuildStage2HintPolicyDerivesSharedAndPerPeerModes(t *testing.T) {
	sharedHints := stage2TestHints("RATED", 1000, 100, 2.0)
	shared := buildStage2HintPolicy(sharedHints, stage2TestPattern(), "", Params{}, 12)
	if !shared.useSharedSourceProbeFilter {
		t.Fatalf("expected shared source probe filter in low coverage mode")
	}
	if shared.usePerPeerSourceProbe {
		t.Fatalf("expected per-peer source probe disabled when shared source filter is enabled")
	}

	perPeerHints := stage2TestHints("RATED", 1000, 100, 10.0)
	perPeer := buildStage2HintPolicy(perPeerHints, stage2TestPattern(), "", Params{}, 20)
	if perPeer.useSharedSourceProbeFilter {
		t.Fatalf("expected shared source probe filter disabled in high coverage mode")
	}
	if !perPeer.usePerPeerSourceProbe {
		t.Fatalf("expected per-peer source probe in wide/high-degree mode")
	}
}

func TestStage2BuildPlannerPolicyInputCapturesQueryShapeDecisions(t *testing.T) {
	pattern := stage2TestPattern()
	plannerInput := stage2BuildPlannerPolicyInput(pattern, "r.rating >= 3", Params{})
	if plannerInput.edgeType != "RATED" {
		t.Fatalf("expected planner policy input to preserve edge type, got %#v", plannerInput)
	}
	if !plannerInput.predicateShapeDecisive || !plannerInput.predicateShapeEligible {
		t.Fatalf("expected planner policy input to capture decisive eligible predicate shape, got %#v", plannerInput)
	}
	if !plannerInput.oneSidedNumericRangeEligible {
		t.Fatalf("expected planner policy input to capture one-sided numeric range shape, got %#v", plannerInput)
	}
}

func TestBuildStage2HintPolicyFromPlannerInputHonorsPreclassifiedSignals(t *testing.T) {
	hints := stage2TestHints("RATED", 1000, 100, 2.0)
	plannerInput := stage2PlannerPolicyInput{
		pattern:                      stage2TestPattern(),
		whereRaw:                     "",
		edgeType:                     "RATED",
		edgeTypeAnyOf:                nil,
		predicateShapeEligible:       true,
		predicateShapeDecisive:       true,
		oneSidedNumericRangeEligible: true,
	}

	policy := buildStage2HintPolicyFromPlannerInput(hints, plannerInput, 12)
	if !policy.predicateShapeDecisive || !policy.predicateShapeEligible {
		t.Fatalf("expected policy to honor planner-classified predicate shape, got %#v", policy)
	}
	if !policy.oneSidedNumericRangeEligible {
		t.Fatalf("expected policy to honor planner-classified one-sided range eligibility, got %#v", policy)
	}
	if !policy.useSharedSourceProbeFilter {
		t.Fatalf("expected planner-backed policy to continue deriving shared source probe mode")
	}
	if policy.usePerPeerSourceProbe {
		t.Fatalf("expected planner-backed policy to keep per-peer source probe disabled when shared mode is selected")
	}
	if policy.skipWideNonRangeWhenNoRange {
		t.Fatalf("expected planner-backed policy to keep wide non-range skip disabled for low-coverage low-degree source set")
	}
	limit, tightened := policy.indexProbeLimit, policy.probeLimitAdaptiveTightened
	if limit <= 0 {
		t.Fatalf("expected planner-backed policy to derive positive initial probe limit")
	}
	if !tightened {
		t.Fatalf("expected planner-backed policy to retain adaptive tightening for low-degree source set")
	}
}

func TestBuildStage2HintPolicyFromPlannerInputPrecomputesWideSkipSignal(t *testing.T) {
	hints := stage2TestHints("RATED", 2000, 100, 20.0)
	plannerInput := stage2PlannerPolicyInput{
		pattern:                      stage2TestPattern(),
		whereRaw:                     "",
		edgeType:                     "RATED",
		edgeTypeAnyOf:                nil,
		predicateShapeEligible:       true,
		predicateShapeDecisive:       true,
		oneSidedNumericRangeEligible: false,
	}

	policy := buildStage2HintPolicyFromPlannerInput(hints, plannerInput, 95)
	if !policy.skipWideNonRangeWhenNoRange {
		t.Fatalf("expected planner-backed policy to precompute wide non-range skip for high-coverage high-degree source set")
	}
	if !policy.shouldSkipWideNonRangePushdown(false) {
		t.Fatalf("expected precomputed wide non-range skip signal to be consumed when no numeric range shape exists")
	}
	if policy.shouldSkipWideNonRangePushdown(true) {
		t.Fatalf("expected numeric range shape to bypass precomputed wide non-range skip signal")
	}
}

func TestStage2BuildOperatorPolicySignals(t *testing.T) {
	plannerInput := stage2PlannerPolicyInput{
		pattern:                      stage2TestPattern(),
		whereRaw:                     "r.rating >= 3",
		edgeType:                     "RATED",
		edgeTypeAnyOf:                nil,
		predicateShapeEligible:       true,
		predicateShapeDecisive:       true,
		oneSidedNumericRangeEligible: true,
	}
	hints := stage2TestHints("RATED", 2000, 100, 20.0)
	signals := stage2BuildOperatorPolicySignals(plannerInput, hints, 95)

	if !signals.predicateShapeEligible || !signals.predicateShapeDecisive || !signals.oneSidedNumericRangeEligible {
		t.Fatalf("expected operator policy signals to preserve planner predicate-shape flags, got %#v", signals)
	}
	if !signals.usePerPeerSourceProbe || signals.useSharedSourceProbeFilter {
		t.Fatalf("expected wide high-degree source set to precompute per-peer mode in operator signals, got %#v", signals)
	}
	if !signals.skipWideNonRangeWhenNoRange {
		t.Fatalf("expected wide high-degree source set to precompute wide non-range skip signal, got %#v", signals)
	}
	if signals.indexProbeLimit <= 0 {
		t.Fatalf("expected operator policy signals to precompute a positive index probe limit, got %#v", signals)
	}
}

func TestStage2HintPolicyUsesPrecomputedProbeLimitSignals(t *testing.T) {
	plannerInput := stage2PlannerPolicyInput{
		pattern:                      stage2TestPattern(),
		whereRaw:                     "",
		edgeType:                     "RATED",
		edgeTypeAnyOf:                nil,
		predicateShapeEligible:       true,
		predicateShapeDecisive:       true,
		oneSidedNumericRangeEligible: true,
	}
	hints := stage2TestHints("RATED", 2000, 100, 20.0)
	policy := buildStage2HintPolicyFromPlannerInput(hints, plannerInput, 95)

	limit, tightened := policy.indexProbeLimit, policy.probeLimitAdaptiveTightened
	if limit != policy.indexProbeLimit {
		t.Fatalf("expected probe limit to use precomputed signal limit %d, got %d", policy.indexProbeLimit, limit)
	}
	if tightened != policy.probeLimitAdaptiveTightened {
		t.Fatalf("expected probe limit tightened flag to match precomputed signal (%v), got %v", policy.probeLimitAdaptiveTightened, tightened)
	}
}

func TestStage2HintPolicyInitialProbeLimitRespectsCapsAndAdaptiveTightening(t *testing.T) {
	hints := stage2TestHints("RATED", 20000, 200, 100.0)
	policy := buildStage2HintPolicy(hints, stage2TestPattern(), "", Params{}, 100)

	limit, tightened := policy.indexProbeLimit, policy.probeLimitAdaptiveTightened
	if tightened {
		t.Fatalf("expected no adaptive tightening once max pushdown cap already bound the limit")
	}
	if limit > policy.maxPushdownCandidates {
		t.Fatalf("expected probe limit <= max pushdown candidates (%d), got %d", policy.maxPushdownCandidates, limit)
	}
	if limit != policy.maxPushdownCandidates {
		t.Fatalf("expected probe limit to equal max pushdown candidates %d, got %d", policy.maxPushdownCandidates, limit)
	}
}

func TestStage2HintPolicyDelegatesDecisionHelpers(t *testing.T) {
	hints := stage2TestHints("RATED", 2000, 100, 20.0)
	policy := buildStage2HintPolicy(hints, stage2TestPattern(), "", Params{}, 95)
	if !policy.shouldSkipWideNonRangePushdown(false) {
		t.Fatalf("expected policy wide non-range skip decision to be true")
	}

	selectivePolicy := buildStage2HintPolicy(stage2TestHints("RATED", 1000, 1000, 1.0), stage2TestPattern(), "", Params{}, 2)
	indexedEdgesBySource := map[string][]*graph.Edge{
		"u1": {{SrcID: "u1", DstID: "m1", Type: "RATED"}},
		"u2": {{SrcID: "u2", DstID: "m2", Type: "RATED"}},
	}
	if !selectivePolicy.shouldApplyPushdown(indexedEdgesBySource) {
		t.Fatalf("expected policy pushdown decision to preserve selective workload")
	}
}

func TestStage2HintPolicyPredicateShapeMethods(t *testing.T) {
	hints := stage2TestHints("RATED", 1000, 100, 2.0)
	pattern := stage2TestPattern()

	decisivePolicy := buildStage2HintPolicy(hints, pattern, "r.rating >= 3", Params{}, 10)
	if !decisivePolicy.predicateShapeDecisive || !decisivePolicy.predicateShapeEligible {
		t.Fatalf("expected decisive+eligible predicate shape policy")
	}
	decisiveInput := decisivePolicy.buildRowPushdownInput(Row{}, Params{})
	if !decisiveInput.assessment.predicateShapeEligible {
		t.Fatalf("expected decisive predicate-shape eligibility")
	}
	if !decisiveInput.assessment.hasOneSidedNumeric {
		t.Fatalf("expected one-sided numeric range detection")
	}
	if !decisiveInput.assessment.hasNumericRangeShape {
		t.Fatalf("expected numeric range shape helper to detect range constraints")
	}

	nonDecisivePolicy := buildStage2HintPolicy(hints, pattern, "r.rating >= $min", Params{}, 10)
	if nonDecisivePolicy.predicateShapeDecisive {
		t.Fatalf("expected non-decisive predicate-shape policy for row-dependent parameter")
	}
	nonDecisiveMissing := nonDecisivePolicy.buildRowPushdownInput(Row{}, Params{})
	if nonDecisiveMissing.assessment.predicateShapeEligible {
		t.Fatalf("expected predicate-shape helper to be false when required parameter is missing")
	}
	if nonDecisiveMissing.assessment.hasNumericRangeShape {
		t.Fatalf("expected numeric range shape helper to be false when parameter is missing")
	}
	nonDecisiveBound := nonDecisivePolicy.buildRowPushdownInput(Row{}, Params{"min": 3.0})
	if !nonDecisiveBound.assessment.predicateShapeEligible {
		t.Fatalf("expected predicate-shape helper to evaluate true when parameter is bound")
	}
	if !nonDecisiveBound.assessment.hasNumericRangeShape {
		t.Fatalf("expected numeric range shape helper to evaluate true when parameter is bound")
	}
}

func TestStage2PredicateShapeConstraintHelpers(t *testing.T) {
	contradictory := edgeNumericRangeConstraint{lowerSet: true, lower: 5, lowerInclusive: true, upperSet: true, upper: 3, upperInclusive: true}
	if !stage2ConstraintEnablesIndexPushdownPredicateShape(contradictory) {
		t.Fatalf("expected contradictory constraint to enable predicate-shape pushdown")
	}

	twoSided := edgeNumericRangeConstraint{lowerSet: true, lower: 3, lowerInclusive: true, upperSet: true, upper: 5, upperInclusive: true}
	if !stage2ConstraintEnablesIndexPushdownPredicateShape(twoSided) {
		t.Fatalf("expected bounded numeric constraint to enable predicate-shape pushdown")
	}

	empty := edgeNumericRangeConstraint{}
	if stage2ConstraintEnablesIndexPushdownPredicateShape(empty) {
		t.Fatalf("expected empty constraint to avoid enabling predicate-shape pushdown")
	}

	constraints := map[string]edgeNumericRangeConstraint{"rating": twoSided}
	if !stage2HasConstraintEnablingIndexPushdownPredicateShape(constraints, true) {
		t.Fatalf("expected helper to detect enabling constraint when hasConstraints=true")
	}
	if stage2HasConstraintEnablingIndexPushdownPredicateShape(constraints, false) {
		t.Fatalf("expected helper to ignore constraints when hasConstraints=false")
	}

	oneSidedLower := edgeNumericRangeConstraint{lowerSet: true, lower: 4, lowerInclusive: true}
	if !stage2ConstraintHasOneSidedNumericRange(oneSidedLower) {
		t.Fatalf("expected lower-only constraint to be one-sided")
	}
	if stage2ConstraintHasOneSidedNumericRange(twoSided) {
		t.Fatalf("expected two-sided constraint to be non one-sided")
	}
	if stage2ConstraintHasOneSidedNumericRange(contradictory) {
		t.Fatalf("expected contradictory constraint to be excluded from one-sided detection")
	}

	oneSidedConstraints := map[string]edgeNumericRangeConstraint{"rating": oneSidedLower}
	if !stage2HasOneSidedNumericRangeConstraint(oneSidedConstraints, true) {
		t.Fatalf("expected helper to detect one-sided constraint when hasConstraints=true")
	}
	if stage2HasOneSidedNumericRangeConstraint(oneSidedConstraints, false) {
		t.Fatalf("expected helper to ignore one-sided constraints when hasConstraints=false")
	}
}

func TestStage2CanResolvePredicateShapeByEdgePropertyEquality(t *testing.T) {
	if !stage2CanResolvePredicateShapeByEdgePropertyEquality("rating: $min", Params{"min": 3.0}) {
		t.Fatalf("expected edge-property equality helper to resolve with bound parameter")
	}
	if stage2CanResolvePredicateShapeByEdgePropertyEquality("rating: $min", Params{}) {
		t.Fatalf("expected edge-property equality helper to reject missing parameter")
	}
	if stage2CanResolvePredicateShapeByEdgePropertyEquality("   ", Params{"min": 3.0}) {
		t.Fatalf("expected edge-property equality helper to reject blank edge property expression")
	}
}

func TestStage2HintPolicyRowAssessmentHelpers(t *testing.T) {
	constraints := map[string]edgeNumericRangeConstraint{
		"rating": {lowerSet: true, lower: 3.0, lowerInclusive: true},
	}
	if !stage2HasHintPolicyNumericRangeShape(constraints, true) {
		t.Fatalf("expected numeric-range-shape helper to detect enabling constraint")
	}
	if stage2HasHintPolicyNumericRangeShape(constraints, false) {
		t.Fatalf("expected numeric-range-shape helper to ignore constraints when hasConstraints=false")
	}

	assessment := stage2BuildInitialRowPushdownAssessment(true)
	if !assessment.predicateShapeEligible {
		t.Fatalf("expected initial assessment builder to preserve predicateShapeEligible=true")
	}
	if stage2ShouldReturnRowPushdownAssessmentEarly(assessment) {
		t.Fatalf("expected early-return helper to keep eligible assessments")
	}
	if !stage2ShouldReturnRowPushdownAssessmentEarly(stage2BuildInitialRowPushdownAssessment(false)) {
		t.Fatalf("expected early-return helper to short-circuit ineligible assessments")
	}
}

func TestStage2CompleteRowPushdownAssessment(t *testing.T) {
	hints := stage2TestHints("RATED", 2000, 100, 20.0)
	pattern := stage2TestPattern()
	policy := buildStage2HintPolicy(hints, pattern, "r.rating >= $min", Params{}, 95)

	base := stage2BuildInitialRowPushdownAssessment(true)
	constraints, hasConstraints := stage2ResolveHintPolicyRowNumericConstraints(policy.whereRaw, policy.pattern.EdgeVar, Row{}, Params{"min": 3.0})
	completed := stage2CompleteRowPushdownAssessmentFromResolvedConstraints(policy, base, constraints, hasConstraints)
	if !completed.hasOneSidedNumeric {
		t.Fatalf("expected completed assessment to detect one-sided numeric range")
	}
	if !completed.hasNumericRangeShape {
		t.Fatalf("expected completed assessment to detect numeric range shape")
	}
	if completed.skipWideNonRange {
		t.Fatalf("expected numeric range shape to bypass wide non-range skip")
	}
}

func TestStage2HintPolicyAssessRowForPushdown(t *testing.T) {
	hints := stage2TestHints("RATED", 2000, 100, 20.0)
	pattern := stage2TestPattern()

	missingParamPolicy := buildStage2HintPolicy(hints, pattern, "r.rating >= $min", Params{}, 95)
	missingParamAssessment := missingParamPolicy.buildRowPushdownInput(Row{}, Params{}).assessment
	if missingParamAssessment.predicateShapeEligible {
		t.Fatalf("expected missing parameter row assessment to be predicate-ineligible")
	}
	if missingParamAssessment.hasOneSidedNumeric {
		t.Fatalf("expected one-sided numeric marker to be false when predicate is ineligible")
	}
	if missingParamAssessment.hasNumericRangeShape {
		t.Fatalf("expected numeric range shape marker to be false when predicate is ineligible")
	}
	if missingParamAssessment.skipWideNonRange {
		t.Fatalf("expected wide non-range skip marker to be false when predicate is ineligible")
	}

	eligibleAssessment := missingParamPolicy.buildRowPushdownInput(Row{}, Params{"min": 3.0}).assessment
	if !eligibleAssessment.predicateShapeEligible {
		t.Fatalf("expected predicate-eligible row assessment when parameter is bound")
	}
	if !eligibleAssessment.hasOneSidedNumeric {
		t.Fatalf("expected one-sided numeric marker when range predicate is bound")
	}
	if !eligibleAssessment.hasNumericRangeShape {
		t.Fatalf("expected numeric range shape marker when range predicate is bound")
	}
	if eligibleAssessment.skipWideNonRange {
		t.Fatalf("expected numeric range shape to bypass wide non-range skip")
	}

}

func TestStage2HintPolicyBuildRowPushdownInput(t *testing.T) {
	hints := stage2TestHints("RATED", 2000, 100, 20.0)
	pattern := stage2TestPattern()
	policy := buildStage2HintPolicy(hints, pattern, "r.rating >= $min", Params{}, 95)

	missingInput := policy.buildRowPushdownInput(Row{}, Params{})
	if missingInput.assessment.predicateShapeEligible {
		t.Fatalf("expected row pushdown input to be predicate-ineligible when required parameter is missing")
	}
	if missingInput.cacheable || missingInput.cacheKey != "" {
		t.Fatalf("expected row pushdown input to skip cache key generation for ineligible predicate")
	}

	eligibleInput := policy.buildRowPushdownInput(Row{}, Params{"min": 3.0})
	if !eligibleInput.assessment.predicateShapeEligible {
		t.Fatalf("expected row pushdown input to become predicate-eligible when range parameter is bound")
	}
	if !eligibleInput.assessment.hasOneSidedNumeric || !eligibleInput.assessment.hasNumericRangeShape {
		t.Fatalf("expected row pushdown input to preserve one-sided/numeric-range flags for bound range predicate")
	}
	if eligibleInput.assessment.skipWideNonRange {
		t.Fatalf("expected row pushdown input numeric-range shape to bypass wide non-range skip")
	}
	if !eligibleInput.cacheable || eligibleInput.cacheKey == "" {
		t.Fatalf("expected row pushdown input to precompute cache key for eligible/cacheable shape")
	}
}

func TestStage2ResolveEarlyStopSettings(t *testing.T) {
	enabled, keep := stage2ResolveEarlyStopSettings(true, fastPeerCandidateTopKSpec{descending: true, skip: 2, limit: 5})
	if !enabled {
		t.Fatalf("expected early-stop settings helper to enable for descending top-k with positive limit")
	}
	if keep != 7 {
		t.Fatalf("expected early-stop keep=7, got %d", keep)
	}

	enabled, keep = stage2ResolveEarlyStopSettings(true, fastPeerCandidateTopKSpec{descending: true, skip: -5, limit: 3})
	if !enabled {
		t.Fatalf("expected early-stop settings helper to remain enabled when limit fallback is positive")
	}
	if keep != 3 {
		t.Fatalf("expected early-stop keep fallback to limit=3, got %d", keep)
	}

	enabled, keep = stage2ResolveEarlyStopSettings(true, fastPeerCandidateTopKSpec{descending: false, skip: 0, limit: 3})
	if enabled {
		t.Fatalf("expected non-descending top-k to disable early-stop settings")
	}
	if keep != 3 {
		t.Fatalf("expected keep to preserve limit even when disabled, got %d", keep)
	}

	enabled, keep = stage2ResolveEarlyStopSettings(true, fastPeerCandidateTopKSpec{descending: true, skip: 0, limit: 0})
	if enabled {
		t.Fatalf("expected non-positive limit to disable early-stop settings")
	}
	if keep != 0 {
		t.Fatalf("expected keep=0 for non-positive limit, got %d", keep)
	}
}

func TestStage2ResolveWorkItemSimilarity(t *testing.T) {
	numeric, numericOK, resolved := stage2ResolveWorkItemSimilarity("1.5", Row{}, Params{})
	if !resolved {
		t.Fatalf("expected similarity helper to resolve constant expression")
	}
	if !numericOK || numeric != 1.5 {
		t.Fatalf("expected similarity helper numeric=1.5 numericOK=true, got numeric=%v numericOK=%v", numeric, numericOK)
	}

	_, _, resolved = stage2ResolveWorkItemSimilarity("NOT_A_VALID_EXPR(", Row{}, Params{})
	if resolved {
		t.Fatalf("expected similarity helper to report unresolved for invalid expression")
	}
}

func TestStage2BuildPeerWorkItems(t *testing.T) {
	e := &Executor{}
	params := Params{}
	collectState := newStage2CollectOrchestrationState(true)
	inputs := []stage2PeerInput{{row: Row{}, peer: &graph.Vertex{ID: "u1"}}}
	matchSpec := anchoredMatchSpec{Where: ""}
	pattern := directedRelationshipPattern{EdgeVar: "r", Right: vertexPattern{Var: "m"}}
	projection := fastPeerCandidateReturnProjection{sumSimilarityExpr: "2.0"}

	collectPeerEdges := func(row Row, peer *graph.Vertex) ([]*graph.Edge, bool, error) {
		return []*graph.Edge{{ID: "e1", SrcID: "u1", DstID: "m1", Type: "RATED"}}, true, nil
	}

	workItems, resolved, err := e.stage2BuildPeerWorkItems(context.Background(), nil, "tenant", inputs, matchSpec, pattern, projection, collectPeerEdges, &collectState, params)
	if err != nil {
		t.Fatalf("expected work-item builder helper success, got error: %v", err)
	}
	if !resolved {
		t.Fatalf("expected work-item builder helper to resolve work items")
	}
	if len(workItems) != 1 {
		t.Fatalf("expected one work item, got %d", len(workItems))
	}
	item := workItems[0]
	if !item.similarityNumericOK || item.similarityNumeric != 2.0 {
		t.Fatalf("expected similarity payload numeric=2.0 numericOK=true, got numeric=%v numericOK=%v", item.similarityNumeric, item.similarityNumericOK)
	}
	if !item.indexedEdges || len(item.edges) != 1 {
		t.Fatalf("expected one indexed edge in built work item")
	}
	if item.remainingPotential != 2.0 {
		t.Fatalf("expected remaining potential 2.0, got %v", item.remainingPotential)
	}
	if collectState.totalRemainingPotential != 2.0 {
		t.Fatalf("expected collect-state total remaining potential 2.0, got %v", collectState.totalRemainingPotential)
	}
	if item.row == nil || item.peer == nil {
		t.Fatalf("expected built work item to preserve input row and peer")
	}

	unresolvedProjection := fastPeerCandidateReturnProjection{sumSimilarityExpr: "NOT_A_VALID_EXPR("}
	workItems, resolved, err = e.stage2BuildPeerWorkItems(context.Background(), nil, "tenant", inputs, matchSpec, pattern, unresolvedProjection, collectPeerEdges, &collectState, Params{})
	if err != nil {
		t.Fatalf("expected unresolved-similarity path to return nil error, got: %v", err)
	}
	if resolved {
		t.Fatalf("expected unresolved-similarity path to mark work items unresolved")
	}
	if len(workItems) != 0 {
		t.Fatalf("expected unresolved-similarity path to return no work items")
	}
}

func TestStage2HintPolicyLookupContext(t *testing.T) {
	sharedHints := stage2TestHints("RATED", 1000, 100, 2.0)
	sharedPolicy := buildStage2HintPolicy(sharedHints, stage2TestPattern(), "", Params{}, 12)
	sharedFilter := map[string]struct{}{"u1": {}, "u2": {}}

	sharedLookup := sharedPolicy.lookupContext("base", "u1", sharedFilter)
	if sharedLookup.perPeerScoped {
		t.Fatalf("expected shared lookup mode to avoid per-peer scoping")
	}
	if sharedLookup.lookupCacheKey != "base" {
		t.Fatalf("expected shared lookup cache key base, got %q", sharedLookup.lookupCacheKey)
	}
	if len(sharedLookup.probeSourceFilter) != len(sharedFilter) {
		t.Fatalf("expected shared probe filter to be reused")
	}

	perPeerHints := stage2TestHints("RATED", 1000, 100, 10.0)
	perPeerPolicy := buildStage2HintPolicy(perPeerHints, stage2TestPattern(), "", Params{}, 20)
	perPeerLookup := perPeerPolicy.lookupContext("base", "u9", sharedFilter)
	if !perPeerLookup.perPeerScoped {
		t.Fatalf("expected per-peer lookup mode for wide/high-degree policy")
	}
	if perPeerLookup.lookupCacheKey != "base|src=u9" {
		t.Fatalf("expected per-peer lookup cache key base|src=u9, got %q", perPeerLookup.lookupCacheKey)
	}
	if len(perPeerLookup.probeSourceFilter) != 1 {
		t.Fatalf("expected per-peer probe filter size 1, got %d", len(perPeerLookup.probeSourceFilter))
	}
	if _, ok := perPeerLookup.probeSourceFilter["u9"]; !ok {
		t.Fatalf("expected per-peer probe filter to include u9")
	}

	missingPeerLookup := perPeerPolicy.lookupContext("base", "", sharedFilter)
	if missingPeerLookup.perPeerScoped {
		t.Fatalf("expected empty peer id to disable per-peer lookup scoping")
	}
	if missingPeerLookup.lookupCacheKey != "base" {
		t.Fatalf("expected empty peer fallback cache key base, got %q", missingPeerLookup.lookupCacheKey)
	}
}

func TestStage2LookupContextHelpers(t *testing.T) {
	sharedFilter := map[string]struct{}{"u1": {}, "u2": {}}
	defaultLookup := stage2BuildDefaultIndexLookupContext("base", sharedFilter)
	if defaultLookup.lookupCacheKey != "base" {
		t.Fatalf("expected default lookup cache key base, got %q", defaultLookup.lookupCacheKey)
	}
	if len(defaultLookup.probeSourceFilter) != 2 {
		t.Fatalf("expected default lookup probe filter size 2, got %d", len(defaultLookup.probeSourceFilter))
	}
	if defaultLookup.perPeerScoped {
		t.Fatalf("expected default lookup context to be shared-scoped")
	}

	if !stage2ShouldUsePerPeerLookupContext(true) {
		t.Fatalf("expected per-peer lookup gate to pass true")
	}
	if stage2ShouldUsePerPeerLookupContext(false) {
		t.Fatalf("expected per-peer lookup gate to reject false")
	}

	if got := stage2ResolvePerPeerLookupPeerID("  u9  "); got != "u9" {
		t.Fatalf("expected peer ID normalization to trim whitespace, got %q", got)
	}
	if !stage2CanScopePerPeerLookup("u9") {
		t.Fatalf("expected non-empty peer ID to be scopeable")
	}
	if stage2CanScopePerPeerLookup("") {
		t.Fatalf("expected empty peer ID to be non-scopeable")
	}

	perPeer := stage2BuildPerPeerLookupContext("base", "u9")
	if perPeer.lookupCacheKey != "base|src=u9" {
		t.Fatalf("expected per-peer lookup cache key base|src=u9, got %q", perPeer.lookupCacheKey)
	}
	if !perPeer.perPeerScoped {
		t.Fatalf("expected per-peer lookup context to be scoped")
	}
	if len(perPeer.probeSourceFilter) != 1 {
		t.Fatalf("expected per-peer probe filter size 1, got %d", len(perPeer.probeSourceFilter))
	}
	if _, ok := perPeer.probeSourceFilter["u9"]; !ok {
		t.Fatalf("expected per-peer probe filter to include u9")
	}
}

func TestStage2FirstHitBookkeepingEmitsOnce(t *testing.T) {
	e := &Executor{}
	params := Params{}
	b := stage2FirstHitBookkeeping{}

	b.notePredicateShapeSkipped(e, params)
	b.notePredicateShapeSkipped(e, params)
	b.noteWideNonRangeSkipped(e, params)
	b.noteWideNonRangeSkipped(e, params)
	b.notePerPeerSourceProbeScoped(e, params)
	b.notePerPeerSourceProbeScoped(e, params)
	b.noteIndexPushdownApplied(e, params)
	b.noteIndexPushdownApplied(e, params)

	state := ensureRuntimeCounterState(params)
	if got := state.counters["fast_path.stage2.index_pushdown_skipped_predicate_shape"]; got != 1 {
		t.Fatalf("expected predicate-shape skip counter 1, got %d", got)
	}
	if got := state.counters["fast_path.stage2.index_pushdown_skipped_wide_non_range"]; got != 1 {
		t.Fatalf("expected wide non-range skip counter 1, got %d", got)
	}
	if got := state.counters["fast_path.stage2.index_probe_source_scoped_per_peer"]; got != 1 {
		t.Fatalf("expected per-peer scoped probe counter 1, got %d", got)
	}
	if got := state.counters["fast_path.stage2.index_pushdown_applied"]; got != 1 {
		t.Fatalf("expected index-pushdown-applied counter 1, got %d", got)
	}
}

func TestStage2IndexLookupPushdownPredicates(t *testing.T) {
	if !stage2ShouldObserveIndexProbeCapExceeded(true) {
		t.Fatalf("expected probe-cap-exceeded predicate to pass true values")
	}
	if stage2ShouldObserveIndexProbeCapExceeded(false) {
		t.Fatalf("expected probe-cap-exceeded predicate to reject false values")
	}

	if !stage2ShouldEvaluateIndexedLookupCandidates(true) {
		t.Fatalf("expected indexed-candidates predicate to pass indexed=true")
	}
	if stage2ShouldEvaluateIndexedLookupCandidates(false) {
		t.Fatalf("expected indexed-candidates predicate to reject indexed=false")
	}

	if !stage2ShouldCacheIndexLookupEdges(true) {
		t.Fatalf("expected cache-index-edges predicate to pass applyPushdown=true")
	}
	if stage2ShouldCacheIndexLookupEdges(false) {
		t.Fatalf("expected cache-index-edges predicate to reject applyPushdown=false")
	}
}

func TestStage2CollectPeerEdgesIndexLookupPredicates(t *testing.T) {
	if !stage2ShouldAttemptCollectPeerEdgesIndexLookup(true) {
		t.Fatalf("expected collectPeerEdges index-lookup attempt predicate to pass cacheable=true")
	}
	if stage2ShouldAttemptCollectPeerEdgesIndexLookup(false) {
		t.Fatalf("expected collectPeerEdges index-lookup attempt predicate to reject cacheable=false")
	}

	if !stage2ShouldNotePerPeerSourceProbeScoped(stage2IndexLookupContext{perPeerScoped: true}) {
		t.Fatalf("expected per-peer source probe note predicate to pass perPeerScoped=true")
	}
	if stage2ShouldNotePerPeerSourceProbeScoped(stage2IndexLookupContext{perPeerScoped: false}) {
		t.Fatalf("expected per-peer source probe note predicate to reject perPeerScoped=false")
	}

	if !stage2ShouldUseCollectPeerEdgesIndexLookupDecision(stage2IndexLookupDecision{lookupByIndex: true}) {
		t.Fatalf("expected collectPeerEdges index decision predicate to pass lookupByIndex=true")
	}
	if stage2ShouldUseCollectPeerEdgesIndexLookupDecision(stage2IndexLookupDecision{lookupByIndex: false}) {
		t.Fatalf("expected collectPeerEdges index decision predicate to reject lookupByIndex=false")
	}
}

func TestStage2IndexLookupCacheKeyHelpers(t *testing.T) {
	if !stage2HasIndexLookupCacheCandidateTypes([]string{"RATED"}) {
		t.Fatalf("expected non-empty type list to be cache-key eligible")
	}
	if stage2HasIndexLookupCacheCandidateTypes(nil) {
		t.Fatalf("expected nil type list to be cache-key ineligible")
	}

	parts := stage2AppendIndexLookupTypeCachePart(nil, []string{"RATED", "LIKED"})
	if len(parts) != 1 || parts[0] != "types=RATED,LIKED" {
		t.Fatalf("expected type cache part to be appended, got %v", parts)
	}

	parts = stage2AppendIndexLookupEqualityCachePart(parts, "rating: $min", Params{"min": 3.0}, Row{})
	if len(parts) != 2 {
		t.Fatalf("expected equality cache part append when edge property equality is resolvable")
	}
	partsNoEq := stage2AppendIndexLookupEqualityCachePart([]string{"types=RATED"}, "", Params{}, Row{})
	if len(partsNoEq) != 1 {
		t.Fatalf("expected no equality cache part append when edge property equality is absent")
	}

	constraints := map[string]edgeNumericRangeConstraint{
		"b": {lowerSet: true, lower: 1.0, lowerInclusive: true},
		"a": {upperSet: true, upper: 9.0, upperInclusive: false},
	}
	keys := stage2ResolveIndexLookupConstraintKeys(constraints)
	if len(keys) != 2 || keys[0] != "a" || keys[1] != "b" {
		t.Fatalf("expected sorted constraint keys [a b], got %v", keys)
	}

	rangeParts := stage2AppendIndexLookupRangeCacheParts([]string{"types=RATED"}, constraints, true)
	if len(rangeParts) != 3 {
		t.Fatalf("expected two range cache parts appended, got %v", rangeParts)
	}
	rangePartsNoConstraints := stage2AppendIndexLookupRangeCacheParts([]string{"types=RATED"}, constraints, false)
	if len(rangePartsNoConstraints) != 1 {
		t.Fatalf("expected no range cache part append when hasConstraints=false")
	}

	if !stage2HasIndexLookupCacheKeyComponents([]string{"types=RATED", "eq:rating:01"}) {
		t.Fatalf("expected cache-key components predicate to pass when parts include type + selector")
	}
	if stage2HasIndexLookupCacheKeyComponents([]string{"types=RATED"}) {
		t.Fatalf("expected cache-key components predicate to reject type-only parts")
	}

	if key, ok := stage2BuildIndexLookupCacheKeyResult([]string{"types=RATED"}); ok || key != "" {
		t.Fatalf("expected type-only cache key parts to produce no cache key")
	}
	if key, ok := stage2BuildIndexLookupCacheKeyResult([]string{"types=RATED", "eq:rating:01"}); !ok || key == "" {
		t.Fatalf("expected selector-inclusive cache key parts to produce a key")
	}
}

func TestStage2IndexLookupCacheKey(t *testing.T) {
	patternTypeOnly := directedRelationshipPattern{EdgeType: "RATED", EdgeVar: "r"}
	if key, ok := stage2IndexLookupCacheKey(patternTypeOnly, "", Row{}, Params{}); ok || key != "" {
		t.Fatalf("expected type-only stage2 index lookup pattern to be non-cacheable")
	}

	patternWithEq := directedRelationshipPattern{EdgeType: "RATED", EdgeVar: "r", EdgeProps: "rating: $min"}
	if key, ok := stage2IndexLookupCacheKey(patternWithEq, "", Row{}, Params{"min": 4.0}); !ok || key == "" {
		t.Fatalf("expected edge-prop equality stage2 index lookup pattern to be cacheable")
	}
}

func TestStage2IndexLookupCacheKeyFromResolvedConstraints(t *testing.T) {
	pattern := directedRelationshipPattern{EdgeType: "RATED", EdgeVar: "r"}
	whereRaw := "r.rating >= $min"
	params := Params{"min": 4.0}

	constraints, hasConstraints := extractEdgeWhereNumericConstraints(whereRaw, pattern.EdgeVar, Row{}, params)
	resolvedKey, resolvedOK := stage2IndexLookupCacheKeyFromResolvedConstraints(pattern, Row{}, params, constraints, hasConstraints)
	wrappedKey, wrappedOK := stage2IndexLookupCacheKey(pattern, whereRaw, Row{}, params)
	if resolvedOK != wrappedOK || resolvedKey != wrappedKey {
		t.Fatalf("expected resolved-constraints cache-key helper to match wrapped helper, got resolved=(%q,%v) wrapped=(%q,%v)", resolvedKey, resolvedOK, wrappedKey, wrappedOK)
	}
}

func TestStage2BuildCollectPeerEdgesIndexedResult(t *testing.T) {
	original := []*graph.Edge{{ID: "e1"}, {ID: "e2"}}
	resolved, indexed := stage2BuildCollectPeerEdgesIndexedResult(original)
	if !indexed {
		t.Fatalf("expected collectPeerEdges indexed result helper to mark indexed=true")
	}
	if len(resolved) != 2 {
		t.Fatalf("expected collectPeerEdges indexed result length 2, got %d", len(resolved))
	}
	if resolved[0] != original[0] || resolved[1] != original[1] {
		t.Fatalf("expected collectPeerEdges indexed result to preserve edge order")
	}

	original[0] = &graph.Edge{ID: "mutated"}
	if resolved[0].ID != "e1" {
		t.Fatalf("expected collectPeerEdges indexed result to be slice-cloned")
	}
}

func TestStage2ResolveIndexedLookupCandidates(t *testing.T) {
	edgeU1A := &graph.Edge{SrcID: "u1", DstID: "m1", Type: "RATED"}
	edgeU1B := &graph.Edge{SrcID: "u1", DstID: "m2", Type: "RATED"}
	edgeU2 := &graph.Edge{SrcID: "u2", DstID: "m3", Type: "RATED"}
	edges := []*graph.Edge{nil, edgeU1A, edgeU1B, edgeU2, {SrcID: "", DstID: "mx", Type: "RATED"}}

	edgesBySource, indexedEdges, totalCandidates := stage2ResolveIndexedLookupCandidates(edges, "u1")
	if totalCandidates != 3 {
		t.Fatalf("expected 3 valid candidates, got %d", totalCandidates)
	}
	if len(edgesBySource) != 2 {
		t.Fatalf("expected candidates grouped for 2 sources, got %d", len(edgesBySource))
	}
	if len(indexedEdges) != 2 {
		t.Fatalf("expected selected indexed edges for peer u1 to have length 2, got %d", len(indexedEdges))
	}
	if indexedEdges[0] != edgeU1A || indexedEdges[1] != edgeU1B {
		t.Fatalf("expected selected indexed edges to preserve source grouping order for peer u1")
	}
}

func TestStage2IndexEdgesBySourceHelpers(t *testing.T) {
	if sourceID, ok := stage2ResolveIndexedCandidateSourceID(nil); ok || sourceID != "" {
		t.Fatalf("expected nil candidate edge to be excluded from source grouping")
	}
	if sourceID, ok := stage2ResolveIndexedCandidateSourceID(&graph.Edge{SrcID: "   "}); ok || sourceID != "" {
		t.Fatalf("expected blank source ID candidate edge to be excluded from source grouping")
	}
	edge := &graph.Edge{SrcID: " u1 ", DstID: "m1", Type: "RATED"}
	if sourceID, ok := stage2ResolveIndexedCandidateSourceID(edge); !ok || sourceID != "u1" {
		t.Fatalf("expected source ID resolver to trim and include non-empty source IDs")
	}

	grouped := map[string][]*graph.Edge{}
	stage2AppendIndexedEdgeForSource(grouped, "u1", edge)
	if len(grouped["u1"]) != 1 || grouped["u1"][0] != edge {
		t.Fatalf("expected append helper to add candidate edge under source key")
	}

	if got := stage2IncrementIndexedCandidateTotal(0); got != 1 {
		t.Fatalf("expected indexed candidate total increment from 0 to 1, got %d", got)
	}
}

func TestStage2IndexEdgesBySource(t *testing.T) {
	edgeU1A := &graph.Edge{SrcID: "u1", DstID: "m1", Type: "RATED"}
	edgeU1B := &graph.Edge{SrcID: "u1", DstID: "m2", Type: "RATED"}
	edgeU2 := &graph.Edge{SrcID: "u2", DstID: "m3", Type: "RATED"}
	edges := []*graph.Edge{nil, edgeU1A, {SrcID: "   "}, edgeU1B, edgeU2}

	edgesBySource, totalCandidates := stage2IndexEdgesBySource(edges)
	if totalCandidates != 3 {
		t.Fatalf("expected three valid indexed candidates, got %d", totalCandidates)
	}
	if len(edgesBySource) != 2 {
		t.Fatalf("expected two source groups, got %d", len(edgesBySource))
	}
	if len(edgesBySource["u1"]) != 2 || edgesBySource["u1"][0] != edgeU1A || edgesBySource["u1"][1] != edgeU1B {
		t.Fatalf("expected source group u1 to preserve insertion order of matching edges")
	}
	if len(edgesBySource["u2"]) != 1 || edgesBySource["u2"][0] != edgeU2 {
		t.Fatalf("expected source group u2 to contain one matching edge")
	}
}

func TestStage2BuildIndexLookupDecision(t *testing.T) {
	if decision := stage2BuildIndexLookupDecision(false, nil); decision.lookupByIndex {
		t.Fatalf("expected non-pushdown lookup decision to disable lookupByIndex")
	}

	indexedEdges := []*graph.Edge{{ID: "e1"}, {ID: "e2"}}
	decision := stage2BuildIndexLookupDecision(true, indexedEdges)
	if !decision.lookupByIndex {
		t.Fatalf("expected pushdown lookup decision to enable lookupByIndex")
	}
	if len(decision.indexedEdges) != 2 {
		t.Fatalf("expected indexedEdges payload length 2, got %d", len(decision.indexedEdges))
	}
}

func TestStage2IndexLookupResolutionContextHelpers(t *testing.T) {
	e := &Executor{}
	params := Params{}
	policy := buildStage2HintPolicy(stage2TestHints("RATED", 1000, 1000, 1.0), stage2TestPattern(), "", Params{}, 2)
	decisionCache := map[string]bool{"k1": true, "k2": false}
	indexedEdgeCache := map[string]map[string][]*graph.Edge{
		"k1": {
			"u1": {{ID: "e1"}},
		},
	}

	flowInput := stage2BuildIndexLookupFlowInput("acme", stage2TestPattern(), "", Row{}, policy, stage2IndexLookupContext{lookupCacheKey: "k1"}, "u1", 32)
	flowCtx := stage2BuildIndexLookupFlowContext(flowInput, decisionCache, indexedEdgeCache)
	resolutionCtx := flowCtx.resolution
	if resolutionCtx.lookup.lookupCacheKey != "k1" || resolutionCtx.peerID != "u1" {
		t.Fatalf("expected resolution-context builder to preserve lookup key and peer id")
	}
	if resolutionCtx.decisionCache == nil || resolutionCtx.indexedEdgeCache == nil {
		t.Fatalf("expected resolution-context builder to preserve cache maps")
	}

	decision, cached := e.stage2ResolveCachedIndexLookupDecision(flowCtx, params)
	if !cached || !decision.lookupByIndex || len(decision.indexedEdges) != 1 || decision.indexedEdges[0].ID != "e1" {
		t.Fatalf("expected cached-decision helper to resolve lookupByIndex=true with indexed edge payload")
	}

	flowCtxMiss := stage2BuildIndexLookupFlowContext(stage2BuildIndexLookupFlowInput("acme", stage2TestPattern(), "", Row{}, policy, stage2IndexLookupContext{lookupCacheKey: "k2"}, "u2", 32), decisionCache, indexedEdgeCache)
	decision, cached = e.stage2ResolveCachedIndexLookupDecision(flowCtxMiss, params)
	if !cached || decision.lookupByIndex {
		t.Fatalf("expected cached-decision helper to resolve lookupByIndex=false when cached decision is false")
	}

	flowCtxAbsent := stage2BuildIndexLookupFlowContext(stage2BuildIndexLookupFlowInput("acme", stage2TestPattern(), "", Row{}, policy, stage2IndexLookupContext{lookupCacheKey: "k3"}, "u3", 32), decisionCache, indexedEdgeCache)
	_, cached = e.stage2ResolveCachedIndexLookupDecision(flowCtxAbsent, params)
	if cached {
		t.Fatalf("expected cached-decision helper to report uncached key")
	}

	counters := ensureRuntimeCounterState(params).counters
	if got := counters["fast_path.stage2.index_lookup_cache_hits"]; got != 2 {
		t.Fatalf("expected index_lookup_cache_hits counter 2, got %d", got)
	}
}

func TestStage2ObserveIndexProbeCapDecision(t *testing.T) {
	e := &Executor{}
	params := Params{}

	e.stage2ObserveIndexProbeCapDecision(false, params)
	e.stage2ObserveIndexProbeCapDecision(true, params)

	counters := ensureRuntimeCounterState(params).counters
	if got := counters["fast_path.stage2.index_probe_cap_exceeded"]; got != 1 {
		t.Fatalf("expected index_probe_cap_exceeded counter 1, got %d", got)
	}
	if got := counters["fast_path.stage2.index_pushdown_skipped_probe_cap"]; got != 1 {
		t.Fatalf("expected index_pushdown_skipped_probe_cap counter 1, got %d", got)
	}
}

func TestStage2ResolveIndexLookupCandidateDecisionAndFinalize(t *testing.T) {
	e := &Executor{}
	params := Params{}
	hints := stage2TestHints("RATED", 1000, 1000, 1.0)
	policy := buildStage2HintPolicy(hints, stage2TestPattern(), "", Params{}, 2)
	decisionCache := map[string]bool{}
	indexedEdgeCache := map[string]map[string][]*graph.Edge{}
	flowCtx := stage2BuildIndexLookupFlowContext(stage2BuildIndexLookupFlowInput("acme", stage2TestPattern(), "", Row{}, policy, stage2IndexLookupContext{lookupCacheKey: "k1"}, "u1", 32), decisionCache, indexedEdgeCache)
	edges := []*graph.Edge{{SrcID: "u1", DstID: "m1", Type: "RATED"}, {SrcID: "u2", DstID: "m2", Type: "RATED"}}

	applyPushdown, indexedEdges := e.stage2ResolveIndexLookupCandidateDecision(edges, true, flowCtx, params)
	if !applyPushdown {
		t.Fatalf("expected candidate-decision helper to apply pushdown for selective indexed edges")
	}
	if len(indexedEdges) != 1 || indexedEdges[0].DstID != "m1" {
		t.Fatalf("expected candidate-decision helper to select peer-scoped indexed edge list")
	}
	if _, ok := indexedEdgeCache["k1"]; !ok {
		t.Fatalf("expected candidate-decision helper to cache edges by source when pushdown applies")
	}

	decision := stage2FinalizeIndexLookupDecision(flowCtx, applyPushdown, indexedEdges)
	if !decision.lookupByIndex || len(decision.indexedEdges) != 1 {
		t.Fatalf("expected finalize helper to return lookupByIndex=true with indexed edge payload")
	}
	if cached, ok := decisionCache["k1"]; !ok || !cached {
		t.Fatalf("expected finalize helper to persist applyPushdown=true in decision cache")
	}

	decisionCache2 := map[string]bool{}
	indexedEdgeCache2 := map[string]map[string][]*graph.Edge{}
	flowCtx2 := stage2BuildIndexLookupFlowContext(stage2BuildIndexLookupFlowInput("acme", stage2TestPattern(), "", Row{}, policy, stage2IndexLookupContext{lookupCacheKey: "k2"}, "u1", 32), decisionCache2, indexedEdgeCache2)
	applyPushdown, indexedEdges = e.stage2ResolveIndexLookupCandidateDecision(edges, false, flowCtx2, params)
	if applyPushdown || len(indexedEdges) != 0 {
		t.Fatalf("expected non-indexed candidate-decision helper path to keep applyPushdown=false")
	}
	decision = stage2FinalizeIndexLookupDecision(flowCtx2, applyPushdown, indexedEdges)
	if decision.lookupByIndex {
		t.Fatalf("expected finalize helper to keep lookupByIndex=false when applyPushdown is false")
	}
	if cached, ok := decisionCache2["k2"]; !ok || cached {
		t.Fatalf("expected finalize helper to persist applyPushdown=false in decision cache")
	}

	counters := ensureRuntimeCounterState(params).counters
	if got := counters["fast_path.stage2.index_candidates_total"]; got != 2 {
		t.Fatalf("expected index_candidates_total counter 2, got %d", got)
	}
}

func TestStage2CollectOrchestrationStateEarlyStopToggleAndPotential(t *testing.T) {
	state := newStage2CollectOrchestrationState(true)
	if !state.earlyStopEnabled {
		t.Fatalf("expected early stop to start enabled")
	}

	state.noteCollectedEdges(true)
	if !state.earlyStopEnabled {
		t.Fatalf("expected indexed edges to preserve early stop")
	}

	state.noteCollectedEdges(false)
	if state.earlyStopEnabled {
		t.Fatalf("expected non-indexed edges to disable early stop")
	}

	if got := state.remainingPotential(0.5, true, 4); got != 2.0 {
		t.Fatalf("expected remaining potential 2.0, got %v", got)
	}
	if got := state.remainingPotential(-1, true, 4); got != 0 {
		t.Fatalf("expected non-positive similarity to yield zero remaining potential, got %v", got)
	}
	if got := state.remainingPotential(0.5, false, 4); got != 0 {
		t.Fatalf("expected invalid similarity numeric flag to yield zero remaining potential, got %v", got)
	}
	if got := state.remainingPotential(0.5, true, 0); got != 0 {
		t.Fatalf("expected zero edge count to yield zero remaining potential, got %v", got)
	}
}

func TestStage2CollectOrchestrationStatePotentialAccounting(t *testing.T) {
	state := newStage2CollectOrchestrationState(true)
	state.addRemainingPotential(3.5)
	state.addRemainingPotential(0)
	state.addRemainingPotential(1.5)
	if state.totalRemainingPotential != 5.0 {
		t.Fatalf("expected accumulated remaining potential 5.0, got %v", state.totalRemainingPotential)
	}

	state.consumeRemainingPotential(2.0)
	if state.totalRemainingPotential != 3.0 {
		t.Fatalf("expected remaining potential 3.0 after consume, got %v", state.totalRemainingPotential)
	}

	state.consumeRemainingPotential(10.0)
	if state.totalRemainingPotential != 0 {
		t.Fatalf("expected remaining potential clamped at zero, got %v", state.totalRemainingPotential)
	}
}

func TestStage2CollectOrchestrationStateShouldSkipEdgeByFrontier(t *testing.T) {
	state := newStage2CollectOrchestrationState(true)
	e := &Executor{}
	params := Params{}

	if state.shouldSkipEdgeByFrontier(nil, "m1", e, params) {
		t.Fatalf("expected no frontier to avoid skipping edges")
	}

	frontier := map[string]struct{}{"m1": {}}
	if state.shouldSkipEdgeByFrontier(frontier, "m1", e, params) {
		t.Fatalf("expected frontier member edge to be kept")
	}
	if !state.shouldSkipEdgeByFrontier(frontier, "m2", e, params) {
		t.Fatalf("expected non-frontier edge to be skipped")
	}
	if !state.shouldSkipEdgeByFrontier(frontier, "m3", e, params) {
		t.Fatalf("expected each non-frontier edge to be skipped")
	}

	counters := ensureRuntimeCounterState(params).counters
	if got := counters["fast_path.stage2.early_stop_edges_skipped"]; got != 2 {
		t.Fatalf("expected early-stop skipped-edge counter 2, got %d", got)
	}
}

func TestStage2ReuseExistingCandidateAggregateIfEligible(t *testing.T) {
	e := &Executor{}
	params := Params{}
	aggs := map[string]*fastPeerCandidateAggregate{
		"m1": {},
	}
	projection := fastPeerCandidateReturnProjection{avgEdgeProperty: "rating"}
	edge := &graph.Edge{Properties: graph.PropertyMap{"rating": []byte("4.0")}}

	if reused := stage2ReuseExistingCandidateAggregateIfEligible(aggs, "m1", false, edge, projection, 0.7, true, e, params); reused {
		t.Fatalf("expected reuse helper to be disabled when skipWhereEval is false")
	}
	if aggs["m1"].edgeCount != 0 {
		t.Fatalf("expected edge count unchanged when reuse is disabled")
	}

	if reused := stage2ReuseExistingCandidateAggregateIfEligible(aggs, "m1", true, edge, projection, 0.7, true, e, params); !reused {
		t.Fatalf("expected reuse helper to update aggregate when skipWhereEval is true and group exists")
	}
	if aggs["m1"].edgeCount != 1 {
		t.Fatalf("expected edge count 1 after reuse update, got %d", aggs["m1"].edgeCount)
	}
	if aggs["m1"].avgCount != 1 || aggs["m1"].avgSum != 4.0 {
		t.Fatalf("expected avg fields to be updated from edge property, got count=%d sum=%v", aggs["m1"].avgCount, aggs["m1"].avgSum)
	}
	if aggs["m1"].similaritySum != 0.7 {
		t.Fatalf("expected similarity sum 0.7, got %v", aggs["m1"].similaritySum)
	}
	if got := ensureRuntimeCounterState(params).counters["fast_path.stage2.candidate_group_reuse_hits"]; got != 1 {
		t.Fatalf("expected candidate-group-reuse counter 1, got %d", got)
	}
}

func TestStage2ResolveReusableAggregate(t *testing.T) {
	aggs := map[string]*fastPeerCandidateAggregate{
		"m1": {},
	}

	if agg, reusable := stage2ResolveReusableAggregate(aggs, "m1", false); reusable || agg != nil {
		t.Fatalf("expected helper to reject reuse when skipWhereEval is false")
	}

	if agg, reusable := stage2ResolveReusableAggregate(aggs, "m2", true); reusable || agg != nil {
		t.Fatalf("expected helper to reject reuse when aggregate group does not exist")
	}

	if agg, reusable := stage2ResolveReusableAggregate(map[string]*fastPeerCandidateAggregate{"m1": nil}, "m1", true); reusable || agg != nil {
		t.Fatalf("expected helper to reject reuse when aggregate entry is nil")
	}

	if agg, reusable := stage2ResolveReusableAggregate(aggs, "m1", true); !reusable || agg == nil {
		t.Fatalf("expected helper to return reusable aggregate when conditions are met")
	}
}

func TestStage2ShouldApplyReusableAggregate(t *testing.T) {
	if !stage2ShouldApplyReusableAggregate(true) {
		t.Fatalf("expected reusable-aggregate apply gate to pass reusable=true")
	}
	if stage2ShouldApplyReusableAggregate(false) {
		t.Fatalf("expected reusable-aggregate apply gate to reject reusable=false")
	}
}

func TestStage2ApplyReusableAggregateAccumulation(t *testing.T) {
	agg := &fastPeerCandidateAggregate{}
	edge := &graph.Edge{Properties: graph.PropertyMap{"rating": []byte("4.0")}}
	projection := fastPeerCandidateReturnProjection{avgEdgeProperty: "rating"}

	stage2ApplyReusableAggregateAccumulation(agg, edge, projection, 0.75, true)
	if agg.edgeCount != 1 {
		t.Fatalf("expected reusable aggregate accumulation to increment edgeCount to 1, got %d", agg.edgeCount)
	}
	if agg.avgCount != 1 || agg.avgSum != 4.0 {
		t.Fatalf("expected reusable aggregate accumulation avg fields count=1 sum=4.0, got count=%d sum=%v", agg.avgCount, agg.avgSum)
	}
	if agg.similaritySum != 0.75 {
		t.Fatalf("expected reusable aggregate accumulation similaritySum 0.75, got %v", agg.similaritySum)
	}
}

func TestStage2ReusableAggregateDecisionHelpers(t *testing.T) {
	e := &Executor{}
	params := Params{}
	aggs := map[string]*fastPeerCandidateAggregate{"m1": {}}
	edge := &graph.Edge{Properties: graph.PropertyMap{"rating": []byte("4.0")}}
	projection := fastPeerCandidateReturnProjection{avgEdgeProperty: "rating"}
	decisionCtx := stage2BuildReusableAggregateDecisionContext(aggs, "m1", true, edge, projection, 0.7, true)

	if decisionCtx.groupID != "m1" || !decisionCtx.skipWhereEval {
		t.Fatalf("expected reusable-decision builder to preserve group and skipWhereEval")
	}
	if decisionCtx.aggs == nil || decisionCtx.edge != edge || decisionCtx.projection.avgEdgeProperty != "rating" {
		t.Fatalf("expected reusable-decision builder to preserve aggregate/edge/projection references")
	}

	if agg, reusable := stage2ResolveReusableAggregateForDecision(decisionCtx); !reusable || agg == nil {
		t.Fatalf("expected reusable-decision resolver helper to find reusable aggregate")
	}

	reused := e.stage2ResolveReusableAggregateDecision(decisionCtx, params)
	if !reused {
		t.Fatalf("expected reusable-decision helper to apply reusable aggregate path")
	}
	if got := ensureRuntimeCounterState(params).counters["fast_path.stage2.candidate_group_reuse_hits"]; got != 1 {
		t.Fatalf("expected candidate_group_reuse_hits counter 1, got %d", got)
	}
	if aggs["m1"].edgeCount != 1 || aggs["m1"].avgCount != 1 || aggs["m1"].avgSum != 4.0 {
		t.Fatalf("expected reusable-decision helper to accumulate edge/avg fields")
	}
	if aggs["m1"].similaritySum != 0.7 {
		t.Fatalf("expected reusable-decision helper to accumulate similarity sum 0.7")
	}

	paramsNoReuse := Params{}
	noReuseCtx := stage2BuildReusableAggregateDecisionContext(aggs, "m1", false, edge, projection, 0.9, true)
	if e.stage2ResolveReusableAggregateDecision(noReuseCtx, paramsNoReuse) {
		t.Fatalf("expected reusable-decision helper to reject when skipWhereEval is false")
	}
	if got := ensureRuntimeCounterState(paramsNoReuse).counters["fast_path.stage2.candidate_group_reuse_hits"]; got != 0 {
		t.Fatalf("expected no reuse-hit counter increment when reuse is rejected, got %d", got)
	}
}

func TestStage2BuildAndRegisterCandidateAggregate(t *testing.T) {
	projection := fastPeerCandidateReturnProjection{lateMaterializeNonAggregates: true}
	merged := Row{"m": "scope"}
	candidate := &graph.Vertex{ID: "m1"}

	agg, lateMaterialized := stage2BuildNewCandidateAggregate(projection, merged, candidate)
	if agg == nil {
		t.Fatalf("expected builder to create candidate aggregate")
	}
	if !lateMaterialized {
		t.Fatalf("expected late-materialized builder result for late-materialize projection")
	}
	if agg.sampleCandidate == nil || agg.sampleCandidate.ID != "m1" {
		t.Fatalf("expected built aggregate to capture sampleCandidate m1")
	}

	aggs := map[string]*fastPeerCandidateAggregate{}
	groupOrder := []string{}
	stage2RegisterCandidateAggregate(aggs, &groupOrder, "m1", agg)
	if aggs["m1"] != agg {
		t.Fatalf("expected register helper to store aggregate by groupID")
	}
	if len(groupOrder) != 1 || groupOrder[0] != "m1" {
		t.Fatalf("expected register helper to append groupOrder entry m1")
	}
}

func TestStage2EnsureCandidateAggregateDecisionHelpers(t *testing.T) {
	e := &Executor{}
	params := Params{}
	projection := fastPeerCandidateReturnProjection{lateMaterializeNonAggregates: false}
	merged := Row{"m": "value"}
	candidate := &graph.Vertex{ID: "m1"}

	aggs := map[string]*fastPeerCandidateAggregate{}
	groupOrder := []string{}
	decisionCtx := stage2BuildEnsureCandidateAggregateDecisionContext(aggs, &groupOrder, "m1", projection, merged, candidate)
	if decisionCtx.groupOrder != &groupOrder || decisionCtx.groupID != "m1" {
		t.Fatalf("expected ensure-decision builder to preserve group-order pointer and group id")
	}
	if decisionCtx.aggs == nil || decisionCtx.merged["m"] != "value" || decisionCtx.candidate != candidate {
		t.Fatalf("expected ensure-decision builder to preserve aggregate map, merged row, and candidate")
	}

	if agg, reuse := stage2ResolveExistingCandidateAggregateForEnsure(decisionCtx); reuse || agg != nil {
		t.Fatalf("expected ensure-existing resolver helper to reject missing aggregate")
	}

	created := e.stage2ResolveNewCandidateAggregateForEnsure(decisionCtx, params)
	if created == nil {
		t.Fatalf("expected ensure-new resolver helper to create aggregate")
	}
	if aggs["m1"] != created || len(groupOrder) != 1 || groupOrder[0] != "m1" {
		t.Fatalf("expected ensure-new resolver helper to register created aggregate")
	}
	if got := ensureRuntimeCounterState(params).counters["fast_path.stage2.candidate_groups_created"]; got != 1 {
		t.Fatalf("expected candidate_groups_created counter 1, got %d", got)
	}

	reuseCtx := stage2BuildEnsureCandidateAggregateDecisionContext(aggs, &groupOrder, "m1", projection, merged, candidate)
	if agg, reuse := stage2ResolveExistingCandidateAggregateForEnsure(reuseCtx); !reuse || agg != created {
		t.Fatalf("expected ensure-existing resolver helper to reuse existing aggregate")
	}
	if resolved := e.stage2ResolveEnsuredCandidateAggregate(reuseCtx, Params{}); resolved != created {
		t.Fatalf("expected ensure resolver helper to return existing aggregate on reuse")
	}
}

func TestStage2EnsureCandidateAggregateCreatesAndReuses(t *testing.T) {
	e := &Executor{}
	params := Params{}
	aggs := map[string]*fastPeerCandidateAggregate{}
	groupOrder := []string{}
	projection := fastPeerCandidateReturnProjection{lateMaterializeNonAggregates: false}
	merged := Row{"m": "value"}
	candidate := &graph.Vertex{ID: "m1"}

	agg := stage2EnsureCandidateAggregate(aggs, &groupOrder, "m1", projection, merged, candidate, e, params)
	if agg == nil {
		t.Fatalf("expected ensure helper to create aggregate")
	}
	if len(groupOrder) != 1 || groupOrder[0] != "m1" {
		t.Fatalf("expected groupOrder to include newly created group")
	}
	if got := ensureRuntimeCounterState(params).counters["fast_path.stage2.candidate_groups_created"]; got != 1 {
		t.Fatalf("expected candidate-groups-created counter 1, got %d", got)
	}
	if agg.sampleScope == nil {
		t.Fatalf("expected non-late-materialized aggregate to capture sample scope")
	}

	aggAgain := stage2EnsureCandidateAggregate(aggs, &groupOrder, "m1", projection, merged, candidate, e, params)
	if aggAgain != agg {
		t.Fatalf("expected ensure helper to reuse existing aggregate")
	}
	if len(groupOrder) != 1 {
		t.Fatalf("expected groupOrder to remain unchanged on reuse")
	}
	if got := ensureRuntimeCounterState(params).counters["fast_path.stage2.candidate_groups_created"]; got != 1 {
		t.Fatalf("expected candidate-groups-created counter to remain 1, got %d", got)
	}

	lateParams := Params{}
	lateGroupOrder := []string{}
	lateProjection := fastPeerCandidateReturnProjection{lateMaterializeNonAggregates: true}
	lateAgg := stage2EnsureCandidateAggregate(map[string]*fastPeerCandidateAggregate{}, &lateGroupOrder, "m2", lateProjection, merged, candidate, e, lateParams)
	if lateAgg.sampleCandidate == nil || lateAgg.sampleCandidate.ID != "m1" {
		t.Fatalf("expected late-materialized aggregate to capture sample candidate")
	}
	if got := ensureRuntimeCounterState(lateParams).counters["fast_path.stage2.late_materialization_candidates"]; got != 1 {
		t.Fatalf("expected late-materialization counter 1, got %d", got)
	}
}

func TestStage2ShouldReuseExistingCandidateAggregate(t *testing.T) {
	if stage2ShouldReuseExistingCandidateAggregate(false) {
		t.Fatalf("expected missing aggregate to skip reuse")
	}
	if !stage2ShouldReuseExistingCandidateAggregate(true) {
		t.Fatalf("expected existing aggregate to be reused")
	}
}

func TestStage2SeedCandidateAggregateSample(t *testing.T) {
	merged := Row{"m": "scope"}
	candidate := &graph.Vertex{ID: "m1"}

	aggDefault := &fastPeerCandidateAggregate{}
	if late := stage2SeedCandidateAggregateSample(aggDefault, fastPeerCandidateReturnProjection{}, merged, candidate); late {
		t.Fatalf("expected default projection sample seed to be non-late")
	}
	if aggDefault.sampleScope == nil {
		t.Fatalf("expected default projection sampleScope to be captured")
	}
	if aggDefault.sampleCandidate != nil {
		t.Fatalf("expected default projection to avoid sampleCandidate")
	}

	aggLate := &fastPeerCandidateAggregate{}
	lateProjection := fastPeerCandidateReturnProjection{lateMaterializeNonAggregates: true}
	if late := stage2SeedCandidateAggregateSample(aggLate, lateProjection, merged, candidate); !late {
		t.Fatalf("expected late-materialize projection sample seed to be marked late")
	}
	if aggLate.sampleCandidate == nil || aggLate.sampleCandidate.ID != "m1" {
		t.Fatalf("expected late-materialize projection to capture sampleCandidate")
	}
	if aggLate.sampleScope != nil {
		t.Fatalf("expected late-materialize projection to avoid sampleScope capture")
	}

	if late := stage2SeedCandidateAggregateSample(nil, lateProjection, merged, candidate); late {
		t.Fatalf("expected nil aggregate sample seed to return non-late")
	}
}

func TestStage2ShouldSeedSampleCandidate(t *testing.T) {
	if stage2ShouldSeedSampleCandidate(fastPeerCandidateReturnProjection{}) {
		t.Fatalf("expected default projection to avoid sample-candidate seeding")
	}
	if !stage2ShouldSeedSampleCandidate(fastPeerCandidateReturnProjection{lateMaterializeNonAggregates: true}) {
		t.Fatalf("expected late-materialize projection to enable sample-candidate seeding")
	}
}

func TestStage2LookupExistingCandidateAggregate(t *testing.T) {
	aggs := map[string]*fastPeerCandidateAggregate{
		"m1": {},
		"m2": nil,
	}

	if agg, exists := stage2LookupExistingCandidateAggregate(aggs, "m0"); exists || agg != nil {
		t.Fatalf("expected helper to report missing aggregate as non-existing")
	}
	if agg, exists := stage2LookupExistingCandidateAggregate(aggs, "m2"); exists || agg != nil {
		t.Fatalf("expected helper to report nil aggregate entry as non-existing")
	}
	if agg, exists := stage2LookupExistingCandidateAggregate(aggs, "m1"); !exists || agg == nil {
		t.Fatalf("expected helper to return existing non-nil aggregate")
	}
}

func TestStage2HasUsableExistingCandidateAggregate(t *testing.T) {
	if stage2HasUsableExistingCandidateAggregate(nil, true) {
		t.Fatalf("expected nil aggregate to be unusable even when exists=true")
	}
	if stage2HasUsableExistingCandidateAggregate(&fastPeerCandidateAggregate{}, false) {
		t.Fatalf("expected exists=false to be unusable even when aggregate is non-nil")
	}
	if !stage2HasUsableExistingCandidateAggregate(&fastPeerCandidateAggregate{}, true) {
		t.Fatalf("expected non-nil aggregate with exists=true to be usable")
	}
}

func TestStage2ObserveCandidateAggregateCreation(t *testing.T) {
	e := &Executor{}
	params := Params{}

	e.stage2ObserveCandidateAggregateCreation(false, params)
	e.stage2ObserveCandidateAggregateCreation(true, params)

	counters := ensureRuntimeCounterState(params).counters
	if got := counters["fast_path.stage2.candidate_groups_created"]; got != 2 {
		t.Fatalf("expected candidate_groups_created counter 2, got %d", got)
	}
	if got := counters["fast_path.stage2.late_materialization_candidates"]; got != 1 {
		t.Fatalf("expected late_materialization_candidates counter 1, got %d", got)
	}
}

func TestStage2ShouldObserveLateMaterializationCandidate(t *testing.T) {
	if stage2ShouldObserveLateMaterializationCandidate(false) {
		t.Fatalf("expected false late-materialization flag to skip observation")
	}
	if !stage2ShouldObserveLateMaterializationCandidate(true) {
		t.Fatalf("expected true late-materialization flag to enable observation")
	}
}

func TestStage2AccumulateSimilarityIfEligible(t *testing.T) {
	agg := &fastPeerCandidateAggregate{}
	if updated := stage2AccumulateSimilarityIfEligible(agg, 0.7, false); updated {
		t.Fatalf("expected similarity helper to skip when numeric flag is false")
	}
	if agg.similaritySum != 0 {
		t.Fatalf("expected similarity sum unchanged when numeric flag is false, got %v", agg.similaritySum)
	}

	if updated := stage2AccumulateSimilarityIfEligible(agg, 0.7, true); !updated {
		t.Fatalf("expected similarity helper to update when numeric flag is true")
	}
	if agg.similaritySum != 0.7 {
		t.Fatalf("expected similarity sum 0.7 after update, got %v", agg.similaritySum)
	}

	if updated := stage2AccumulateSimilarityIfEligible(nil, 0.7, true); updated {
		t.Fatalf("expected similarity helper to skip nil aggregate")
	}
}

func TestStage2CanAccumulateSimilarity(t *testing.T) {
	if stage2CanAccumulateSimilarity(nil, true) {
		t.Fatalf("expected nil aggregate to be ineligible for similarity accumulation")
	}
	if stage2CanAccumulateSimilarity(&fastPeerCandidateAggregate{}, false) {
		t.Fatalf("expected false similarity numeric flag to be ineligible")
	}
	if !stage2CanAccumulateSimilarity(&fastPeerCandidateAggregate{}, true) {
		t.Fatalf("expected non-nil aggregate with numeric flag to be eligible")
	}
}

func TestStage2CanAccumulateCandidateAggregate(t *testing.T) {
	if stage2CanAccumulateCandidateAggregate(nil) {
		t.Fatalf("expected nil aggregate to be ineligible for aggregate accumulation")
	}
	if !stage2CanAccumulateCandidateAggregate(&fastPeerCandidateAggregate{}) {
		t.Fatalf("expected non-nil aggregate to be eligible for aggregate accumulation")
	}
}

func TestStage2CanReuseExistingAggregate(t *testing.T) {
	if stage2CanReuseExistingAggregate(false) {
		t.Fatalf("expected skipWhereEval=false to disable aggregate reuse")
	}
	if !stage2CanReuseExistingAggregate(true) {
		t.Fatalf("expected skipWhereEval=true to enable aggregate reuse eligibility")
	}
}

func TestStage2AccumulateAverageIfEligible(t *testing.T) {
	agg := &fastPeerCandidateAggregate{}
	if updated := stage2AccumulateAverageIfEligible(agg, &graph.Edge{Properties: graph.PropertyMap{"rating": []byte("4.0")}}, "rating"); !updated {
		t.Fatalf("expected average helper to update when numeric edge property exists")
	}
	if agg.avgCount != 1 || agg.avgSum != 4.0 {
		t.Fatalf("expected average helper to update avg fields count=1 sum=4.0, got count=%d sum=%v", agg.avgCount, agg.avgSum)
	}

	if updated := stage2AccumulateAverageIfEligible(agg, &graph.Edge{Properties: graph.PropertyMap{"rating": []byte("nonnumeric")}}, "rating"); updated {
		t.Fatalf("expected average helper to skip non-numeric edge property")
	}
	if agg.avgCount != 1 || agg.avgSum != 4.0 {
		t.Fatalf("expected average fields unchanged after non-numeric property, got count=%d sum=%v", agg.avgCount, agg.avgSum)
	}

	if updated := stage2AccumulateAverageIfEligible(nil, &graph.Edge{Properties: graph.PropertyMap{"rating": []byte("4.0")}}, "rating"); updated {
		t.Fatalf("expected average helper to skip nil aggregate")
	}
}

func TestStage2ResolveAverageContribution(t *testing.T) {
	if rating, ok := stage2ResolveAverageContribution(&graph.Edge{Properties: graph.PropertyMap{"rating": []byte("4.0")}}, "rating"); !ok || rating != 4.0 {
		t.Fatalf("expected average contribution resolver to parse numeric property, got ok=%v rating=%v", ok, rating)
	}
	if _, ok := stage2ResolveAverageContribution(&graph.Edge{Properties: graph.PropertyMap{"rating": []byte("nonnumeric")}}, "rating"); ok {
		t.Fatalf("expected average contribution resolver to reject non-numeric property")
	}
	if _, ok := stage2ResolveAverageContribution(nil, "rating"); ok {
		t.Fatalf("expected average contribution resolver to reject nil edge")
	}
}

func TestStage2BuildCandidateMergedBase(t *testing.T) {
	pattern := directedRelationshipPattern{
		Left:    vertexPattern{Var: "u"},
		EdgeVar: "r",
	}
	base := Row{"existing": "value"}
	peer := &graph.Vertex{ID: "u1"}
	edge := &graph.Edge{ID: "e1"}

	merged := stage2BuildCandidateMergedBase(base, pattern, peer, edge)
	if merged == nil {
		t.Fatalf("expected merged base row")
	}
	if merged["existing"] != "value" {
		t.Fatalf("expected existing value to be preserved")
	}
	if merged["u"] != peer {
		t.Fatalf("expected left binding to be set in merged base row")
	}
	if merged["r"] != edge {
		t.Fatalf("expected edge binding to be set in merged base row")
	}
	if _, exists := base["u"]; exists {
		t.Fatalf("expected base row to remain unmodified (no left binding)")
	}
	if _, exists := base["r"]; exists {
		t.Fatalf("expected base row to remain unmodified (no edge binding)")
	}
}

func TestStage2ShouldEvaluateWhere(t *testing.T) {
	if stage2ShouldEvaluateWhere("r.rating >= 3", true) {
		t.Fatalf("expected skipWhereEval to disable where evaluation")
	}
	if stage2ShouldEvaluateWhere("   ", false) {
		t.Fatalf("expected blank where expression to disable where evaluation")
	}
	if !stage2ShouldEvaluateWhere("r.rating >= 3", false) {
		t.Fatalf("expected non-blank where expression with skipWhereEval=false to enable where evaluation")
	}
}

func TestStage2MatchesCandidateEdgeGate(t *testing.T) {
	params := Params{"minRating": 4.5}
	row := Row{}
	edge := &graph.Edge{Type: "RATED", Properties: graph.PropertyMap{"rating": []byte("4.5")}}

	if !stage2MatchesCandidateEdgeGate(edge, "RATED", nil, "rating: $minRating", params, row) {
		t.Fatalf("expected gate helper to pass when edge type and props both match")
	}
	if stage2MatchesCandidateEdgeGate(edge, "LIKED", nil, "rating: $minRating", params, row) {
		t.Fatalf("expected gate helper to fail on edge type mismatch")
	}
	if stage2MatchesCandidateEdgeGate(edge, "RATED", nil, "rating: 5.0", params, row) {
		t.Fatalf("expected gate helper to fail on edge props mismatch")
	}

	nilEdge := (*graph.Edge)(nil)
	if stage2MatchesCandidateEdgeGate(nilEdge, "RATED", nil, "", params, row) {
		t.Fatalf("expected gate helper to fail for nil edge")
	}
}

func TestStage2CandidatePrefilterDecisionHelpers(t *testing.T) {
	edge := &graph.Edge{DstID: "m1", Properties: graph.PropertyMap{"rating": []byte("4.0")}}
	constraints := map[string]edgeNumericRangeConstraint{
		"rating": {
			lower:          3.5,
			lowerSet:       true,
			lowerInclusive: true,
		},
	}
	excluded := map[string]struct{}{"m1": {}}

	decisionCtx := stage2BuildCandidatePrefilterDecisionContext(edge, constraints, true, excluded, true)
	if decisionCtx.edge != edge {
		t.Fatalf("expected candidate-prefilter decision builder to preserve edge reference")
	}
	if !decisionCtx.hasNumericConstraints || !decisionCtx.hasExcludedRightIDs {
		t.Fatalf("expected candidate-prefilter decision builder to preserve prefilter gates")
	}

	if !stage2ShouldEvaluateNumericPrefilter(true) || stage2ShouldEvaluateNumericPrefilter(false) {
		t.Fatalf("expected numeric-prefilter gate helper to mirror hasNumericConstraints")
	}

	hardConstraint := map[string]edgeNumericRangeConstraint{
		"rating": {
			lower:          5.0,
			lowerSet:       true,
			lowerInclusive: true,
		},
	}
	if !stage2ResolveNumericPrefilterDrop(edge, hardConstraint, true) {
		t.Fatalf("expected numeric-prefilter drop helper to reject out-of-range edge")
	}
	if stage2ResolveNumericPrefilterDrop(edge, constraints, true) {
		t.Fatalf("expected numeric-prefilter drop helper to keep in-range edge")
	}
	if stage2ResolveNumericPrefilterDrop(edge, hardConstraint, false) {
		t.Fatalf("expected numeric-prefilter drop helper to skip evaluation when disabled")
	}

	if !stage2ResolveAntijoinPrefilterDrop(edge, excluded, true) {
		t.Fatalf("expected anti-join prefilter helper to reject excluded destination")
	}
	if stage2ResolveAntijoinPrefilterDrop(edge, map[string]struct{}{"m2": {}}, true) {
		t.Fatalf("expected anti-join prefilter helper to keep non-excluded destination")
	}
	if stage2ResolveAntijoinPrefilterDrop(edge, excluded, false) {
		t.Fatalf("expected anti-join prefilter helper to skip evaluation when disabled")
	}
	if stage2ResolveAntijoinPrefilterDrop(nil, excluded, true) {
		t.Fatalf("expected anti-join prefilter helper to keep nil edge")
	}
}

func TestStage2ResolveCandidatePrefilterDrop(t *testing.T) {
	e := &Executor{}
	params := Params{}
	edge := &graph.Edge{DstID: "m1", Properties: graph.PropertyMap{"rating": []byte("4.0")}}

	numericDropCtx := stage2BuildCandidatePrefilterDecisionContext(
		edge,
		map[string]edgeNumericRangeConstraint{
			"rating": {
				lower:          5.0,
				lowerSet:       true,
				lowerInclusive: true,
			},
		},
		true,
		map[string]struct{}{"m1": {}},
		true,
	)
	if !e.stage2ResolveCandidatePrefilterDrop(numericDropCtx, params) {
		t.Fatalf("expected resolver helper to prioritize numeric-prefilter drop")
	}

	antijoinDropCtx := stage2BuildCandidatePrefilterDecisionContext(
		edge,
		map[string]edgeNumericRangeConstraint{
			"rating": {
				lower:          3.0,
				lowerSet:       true,
				lowerInclusive: true,
			},
		},
		true,
		map[string]struct{}{"m1": {}},
		true,
	)
	if !e.stage2ResolveCandidatePrefilterDrop(antijoinDropCtx, params) {
		t.Fatalf("expected resolver helper to drop on anti-join prefilter when numeric passes")
	}

	passCtx := stage2BuildCandidatePrefilterDecisionContext(
		edge,
		map[string]edgeNumericRangeConstraint{
			"rating": {
				lower:          3.0,
				lowerSet:       true,
				lowerInclusive: true,
			},
		},
		true,
		map[string]struct{}{"m2": {}},
		true,
	)
	if e.stage2ResolveCandidatePrefilterDrop(passCtx, params) {
		t.Fatalf("expected resolver helper to keep edge when both prefilters pass")
	}

	counters := ensureRuntimeCounterState(params).counters
	if got := counters["fast_path.stage2.numeric_prefilter_drops"]; got != 1 {
		t.Fatalf("expected numeric_prefilter_drops counter 1, got %d", got)
	}
	if got := counters["fast_path.stage2.antijoin_prefilter_drops"]; got != 1 {
		t.Fatalf("expected antijoin_prefilter_drops counter 1, got %d", got)
	}
}

func TestStage2ShouldDropCandidateEdgeByPrefilters(t *testing.T) {
	e := &Executor{}
	params := Params{}
	edge := &graph.Edge{DstID: "m1", Properties: graph.PropertyMap{"rating": []byte("4.0")}}

	constraints := map[string]edgeNumericRangeConstraint{
		"rating": {
			lower:          5.0,
			lowerSet:       true,
			lowerInclusive: true,
		},
	}
	if !e.stage2ShouldDropCandidateEdgeByPrefilters(edge, constraints, true, nil, false, params) {
		t.Fatalf("expected numeric prefilter helper to drop edge below lower bound")
	}

	nonBlockingConstraint := map[string]edgeNumericRangeConstraint{
		"rating": {
			lower:          3.0,
			lowerSet:       true,
			lowerInclusive: true,
		},
	}
	blockedRightIDs := map[string]struct{}{"m1": {}}
	if !e.stage2ShouldDropCandidateEdgeByPrefilters(edge, nonBlockingConstraint, true, blockedRightIDs, true, params) {
		t.Fatalf("expected anti-join prefilter helper to drop excluded destination edge")
	}

	if e.stage2ShouldDropCandidateEdgeByPrefilters(edge, nonBlockingConstraint, true, map[string]struct{}{"m2": {}}, true, params) {
		t.Fatalf("expected edge to pass prefilters when numeric and anti-join checks pass")
	}

	counters := ensureRuntimeCounterState(params).counters
	if got := counters["fast_path.stage2.numeric_prefilter_drops"]; got != 1 {
		t.Fatalf("expected numeric prefilter drop counter 1, got %d", got)
	}
	if got := counters["fast_path.stage2.antijoin_prefilter_drops"]; got != 1 {
		t.Fatalf("expected anti-join prefilter drop counter 1, got %d", got)
	}
}

func TestStage2CandidateGroupVisitDecisionHelpers(t *testing.T) {
	e := &Executor{}
	params := Params{}
	state := newStage2CollectOrchestrationState(true)
	frontier := map[string]struct{}{"m1": {}}
	edge := &graph.Edge{DstID: "m1"}

	decisionCtx := stage2BuildCandidateGroupVisitDecisionContext(edge, &state, frontier)
	if decisionCtx.edge != edge || decisionCtx.collectState != &state {
		t.Fatalf("expected group-visit decision builder to preserve edge/state references")
	}
	if _, ok := decisionCtx.earlyStopFrontier["m1"]; !ok {
		t.Fatalf("expected group-visit decision builder to preserve frontier contents")
	}

	if groupID, ok := stage2ResolveCandidateGroupIDForVisit(nil); ok || groupID != "" {
		t.Fatalf("expected group-id resolver helper to reject nil edge")
	}
	if groupID, ok := stage2ResolveCandidateGroupIDForVisit(&graph.Edge{DstID: "   "}); ok || groupID != "" {
		t.Fatalf("expected group-id resolver helper to reject blank destination")
	}
	if groupID, ok := stage2ResolveCandidateGroupIDForVisit(edge); !ok || groupID != "m1" {
		t.Fatalf("expected group-id resolver helper to resolve destination m1")
	}

	if e.stage2ShouldSkipCandidateGroupVisit(decisionCtx, "m1", params) {
		t.Fatalf("expected group-visit skip helper to keep frontier member visitable")
	}
	if !e.stage2ShouldSkipCandidateGroupVisit(decisionCtx, "m2", params) {
		t.Fatalf("expected group-visit skip helper to skip non-frontier group")
	}

	e.stage2ObserveCandidateGroupVisit(params)
	if got := ensureRuntimeCounterState(params).counters["fast_path.stage2.edges_visited"]; got != 1 {
		t.Fatalf("expected edges_visited counter 1 after observe helper, got %d", got)
	}
}

func TestStage2ResolveCandidateGroupVisit(t *testing.T) {
	e := &Executor{}
	params := Params{}
	state := newStage2CollectOrchestrationState(true)

	blankCtx := stage2BuildCandidateGroupVisitDecisionContext(&graph.Edge{DstID: " "}, &state, nil)
	if groupID, shouldVisit := e.stage2ResolveCandidateGroupVisit(blankCtx, params); shouldVisit || groupID != "" {
		t.Fatalf("expected group-visit resolver helper to reject blank destination")
	}

	skippedCtx := stage2BuildCandidateGroupVisitDecisionContext(&graph.Edge{DstID: "m2"}, &state, map[string]struct{}{"m1": {}})
	if groupID, shouldVisit := e.stage2ResolveCandidateGroupVisit(skippedCtx, params); shouldVisit || groupID != "" {
		t.Fatalf("expected group-visit resolver helper to skip non-frontier destination")
	}

	visitableCtx := stage2BuildCandidateGroupVisitDecisionContext(&graph.Edge{DstID: "m1"}, &state, map[string]struct{}{"m1": {}})
	groupID, shouldVisit := e.stage2ResolveCandidateGroupVisit(visitableCtx, params)
	if !shouldVisit || groupID != "m1" {
		t.Fatalf("expected group-visit resolver helper to allow frontier destination m1")
	}

	counters := ensureRuntimeCounterState(params).counters
	if got := counters["fast_path.stage2.early_stop_edges_skipped"]; got != 1 {
		t.Fatalf("expected early_stop_edges_skipped counter 1, got %d", got)
	}
	if got := counters["fast_path.stage2.edges_visited"]; got != 1 {
		t.Fatalf("expected edges_visited counter 1, got %d", got)
	}
}

func TestStage2ProcessableCandidateGroupDecisionHelpers(t *testing.T) {
	e := &Executor{}
	params := Params{}
	state := newStage2CollectOrchestrationState(true)
	edge := &graph.Edge{Type: "RATED", DstID: "m1", Properties: graph.PropertyMap{"rating": []byte("4.0")}}
	pattern := directedRelationshipPattern{EdgeType: "RATED", EdgeProps: "rating: 4.0"}
	row := Row{}
	numericConstraints := map[string]edgeNumericRangeConstraint{
		"rating": {
			lower:          3.0,
			lowerSet:       true,
			lowerInclusive: true,
		},
	}
	decisionCtx := stage2BuildProcessableCandidateGroupDecisionContext(edge, pattern, row, numericConstraints, true, nil, false, &state, map[string]struct{}{"m1": {}})

	if decisionCtx.edge != edge || decisionCtx.pattern.EdgeType != "RATED" {
		t.Fatalf("expected processable-group decision builder to preserve edge/pattern")
	}
	if !decisionCtx.hasNumericConstraints || decisionCtx.collectState != &state {
		t.Fatalf("expected processable-group decision builder to preserve prefilter and state fields")
	}

	if !stage2ResolveEdgeGateForProcessableCandidateGroup(decisionCtx, params) {
		t.Fatalf("expected edge-gate helper to accept matching candidate edge")
	}
	if e.stage2ResolvePrefilterGateForProcessableCandidateGroup(decisionCtx, params) {
		t.Fatalf("expected prefilter-gate helper to keep in-range edge")
	}

	groupID, process := e.stage2ResolveVisitGateForProcessableCandidateGroup(decisionCtx, params)
	if !process || groupID != "m1" {
		t.Fatalf("expected visit-gate helper to accept frontier destination m1")
	}

	if got := ensureRuntimeCounterState(params).counters["fast_path.stage2.edges_visited"]; got != 1 {
		t.Fatalf("expected edges_visited counter 1, got %d", got)
	}
}

func TestStage2ResolveProcessableCandidateGroup(t *testing.T) {
	e := &Executor{}
	params := Params{}
	state := newStage2CollectOrchestrationState(true)
	pattern := directedRelationshipPattern{EdgeType: "RATED"}
	row := Row{}

	wrongTypeCtx := stage2BuildProcessableCandidateGroupDecisionContext(&graph.Edge{Type: "LIKED", DstID: "m1"}, pattern, row, nil, false, nil, false, &state, map[string]struct{}{"m1": {}})
	if groupID, process := e.stage2ResolveProcessableCandidateGroup(wrongTypeCtx, params); process || groupID != "" {
		t.Fatalf("expected processable-group resolver helper to reject non-matching edge type")
	}

	blockedCtx := stage2BuildProcessableCandidateGroupDecisionContext(&graph.Edge{Type: "RATED", DstID: "m1", Properties: graph.PropertyMap{"rating": []byte("2.0")}}, pattern, row, map[string]edgeNumericRangeConstraint{"rating": {lower: 3.0, lowerSet: true, lowerInclusive: true}}, true, nil, false, &state, map[string]struct{}{"m1": {}})
	if groupID, process := e.stage2ResolveProcessableCandidateGroup(blockedCtx, params); process || groupID != "" {
		t.Fatalf("expected processable-group resolver helper to reject prefilter-dropped edge")
	}

	visitableCtx := stage2BuildProcessableCandidateGroupDecisionContext(&graph.Edge{Type: "RATED", DstID: "m2"}, pattern, row, nil, false, nil, false, &state, map[string]struct{}{"m2": {}})
	groupID, process := e.stage2ResolveProcessableCandidateGroup(visitableCtx, params)
	if !process || groupID != "m2" {
		t.Fatalf("expected processable-group resolver helper to allow frontier destination m2")
	}

	counters := ensureRuntimeCounterState(params).counters
	if got := counters["fast_path.stage2.numeric_prefilter_drops"]; got != 1 {
		t.Fatalf("expected numeric_prefilter_drops counter 1, got %d", got)
	}
	if got := counters["fast_path.stage2.edges_visited"]; got != 1 {
		t.Fatalf("expected edges_visited counter 1, got %d", got)
	}
}

func TestStage2ResolveCandidateGroupVisitGate(t *testing.T) {
	e := &Executor{}
	params := Params{}
	state := newStage2CollectOrchestrationState(true)

	if groupID, shouldVisit := e.stage2ResolveCandidateGroupVisitGate(&graph.Edge{DstID: "   "}, &state, nil, params); shouldVisit || groupID != "" {
		t.Fatalf("expected blank destination edge to be rejected")
	}

	frontier := map[string]struct{}{"m1": {}}
	if groupID, shouldVisit := e.stage2ResolveCandidateGroupVisitGate(&graph.Edge{DstID: "m2"}, &state, frontier, params); shouldVisit || groupID != "" {
		t.Fatalf("expected non-frontier destination edge to be skipped")
	}

	groupID, shouldVisit := e.stage2ResolveCandidateGroupVisitGate(&graph.Edge{DstID: "m1"}, &state, frontier, params)
	if !shouldVisit || groupID != "m1" {
		t.Fatalf("expected frontier destination edge to be visitable, got groupID=%q shouldVisit=%v", groupID, shouldVisit)
	}

	counters := ensureRuntimeCounterState(params).counters
	if got := counters["fast_path.stage2.early_stop_edges_skipped"]; got != 1 {
		t.Fatalf("expected early_stop_edges_skipped counter 1, got %d", got)
	}
	if got := counters["fast_path.stage2.edges_visited"]; got != 1 {
		t.Fatalf("expected edges_visited counter 1, got %d", got)
	}
}

func TestStage2ResolveProcessableCandidateGroupID(t *testing.T) {
	e := &Executor{}
	params := Params{}
	state := newStage2CollectOrchestrationState(true)
	pattern := directedRelationshipPattern{EdgeType: "RATED"}
	row := Row{}

	if groupID, process := e.stage2ResolveProcessableCandidateGroupID(&graph.Edge{Type: "LIKED", DstID: "m1"}, pattern, row, nil, false, nil, false, &state, nil, params); process || groupID != "" {
		t.Fatalf("expected non-matching edge type to be rejected")
	}

	blocked := map[string]struct{}{"m1": {}}
	if groupID, process := e.stage2ResolveProcessableCandidateGroupID(&graph.Edge{Type: "RATED", DstID: "m1"}, pattern, row, nil, false, blocked, true, &state, nil, params); process || groupID != "" {
		t.Fatalf("expected prefilter-blocked edge to be rejected")
	}

	frontier := map[string]struct{}{"m2": {}}
	if groupID, process := e.stage2ResolveProcessableCandidateGroupID(&graph.Edge{Type: "RATED", DstID: "m1"}, pattern, row, nil, false, nil, false, &state, frontier, params); process || groupID != "" {
		t.Fatalf("expected non-frontier edge to be rejected by visit gate")
	}

	if groupID, process := e.stage2ResolveProcessableCandidateGroupID(&graph.Edge{Type: "RATED", DstID: "m2"}, pattern, row, nil, false, nil, false, &state, frontier, params); !process || groupID != "m2" {
		t.Fatalf("expected frontier edge to be processable with groupID m2")
	}

	counters := ensureRuntimeCounterState(params).counters
	if got := counters["fast_path.stage2.antijoin_prefilter_drops"]; got != 1 {
		t.Fatalf("expected antijoin prefilter drop counter 1, got %d", got)
	}
	if got := counters["fast_path.stage2.early_stop_edges_skipped"]; got != 1 {
		t.Fatalf("expected early-stop skipped-edge counter 1, got %d", got)
	}
	if got := counters["fast_path.stage2.edges_visited"]; got != 1 {
		t.Fatalf("expected edges_visited counter 1, got %d", got)
	}
}

func TestStage2CandidateEdgeEvalContextHelpers(t *testing.T) {
	e := &Executor{}
	params := Params{}
	state := newStage2CollectOrchestrationState(true)
	frontier := map[string]struct{}{"m1": {}}
	projection := fastPeerCandidateReturnProjection{avgEdgeProperty: "rating"}
	evalCtx := stage2BuildCandidateEdgeEvalContext(
		directedRelationshipPattern{EdgeType: "RATED"},
		"",
		Row{},
		&graph.Vertex{ID: "u1"},
		nil,
		false,
		nil,
		false,
		&state,
		frontier,
		true,
		nil,
		projection,
		0.5,
		true,
	)

	if evalCtx.pattern.EdgeType != "RATED" || evalCtx.row == nil || evalCtx.peer == nil {
		t.Fatalf("expected eval context builder to preserve core pattern/row/peer fields")
	}
	if !evalCtx.skipWhereEval || !evalCtx.similarityNumericOK || evalCtx.similarityNumeric != 0.5 {
		t.Fatalf("expected eval context builder to preserve skip/similarity fields")
	}

	groupID, process := e.stage2ResolveProcessableCandidateGroupForEvalContext(&graph.Edge{Type: "RATED", DstID: "m1"}, evalCtx, params)
	if !process || groupID != "m1" {
		t.Fatalf("expected eval-context group resolver to return processable groupID m1")
	}

	aggs := map[string]*fastPeerCandidateAggregate{"m1": {}}
	groupOrder := []string{}
	err := e.stage2ApplyCandidateAggregationDecisionForEvalContext(context.Background(), nil, &graph.Edge{Type: "RATED", DstID: "m1", Properties: graph.PropertyMap{"rating": []byte("4.0")}}, aggs, &groupOrder, "m1", evalCtx, params)
	if err != nil {
		t.Fatalf("expected eval-context aggregation applier to succeed, got: %v", err)
	}
	if aggs["m1"].edgeCount != 1 {
		t.Fatalf("expected eval-context aggregation applier to increment edgeCount to 1, got %d", aggs["m1"].edgeCount)
	}
	if aggs["m1"].avgCount != 1 || aggs["m1"].avgSum != 4.0 {
		t.Fatalf("expected eval-context aggregation applier to update avg fields count=1 sum=4.0, got count=%d sum=%v", aggs["m1"].avgCount, aggs["m1"].avgSum)
	}
	if aggs["m1"].similaritySum != 0.5 {
		t.Fatalf("expected eval-context aggregation applier to update similarity sum 0.5, got %v", aggs["m1"].similaritySum)
	}

	counters := ensureRuntimeCounterState(params).counters
	if got := counters["fast_path.stage2.edges_visited"]; got != 1 {
		t.Fatalf("expected edges_visited counter 1, got %d", got)
	}
	if got := counters["fast_path.stage2.candidate_group_reuse_hits"]; got != 1 {
		t.Fatalf("expected candidate_group_reuse_hits counter 1, got %d", got)
	}
}

func TestStage2ProcessCandidateEdgeAggregation(t *testing.T) {
	e := &Executor{}
	params := Params{}
	state := newStage2CollectOrchestrationState(true)
	pattern := directedRelationshipPattern{EdgeType: "RATED"}
	row := Row{}
	peer := &graph.Vertex{ID: "u1"}
	edge := &graph.Edge{Type: "RATED", DstID: "m1", Properties: graph.PropertyMap{"rating": []byte("4.0")}}

	aggs := map[string]*fastPeerCandidateAggregate{"m1": {}}
	groupOrder := []string{}
	projection := fastPeerCandidateReturnProjection{avgEdgeProperty: "rating"}

	if err := e.stage2ProcessCandidateEdgeAggregation(context.Background(), nil, edge, aggs, &groupOrder, pattern, "", row, peer, nil, false, nil, false, &state, nil, true, nil, projection, 0.8, true, params); err != nil {
		t.Fatalf("expected candidate-edge aggregation helper to succeed, got %v", err)
	}
	if aggs["m1"].edgeCount != 1 {
		t.Fatalf("expected reuse-path aggregation to increment edgeCount to 1, got %d", aggs["m1"].edgeCount)
	}
	if aggs["m1"].avgCount != 1 || aggs["m1"].avgSum != 4.0 {
		t.Fatalf("expected reuse-path aggregation to update avg fields count=1 sum=4.0, got count=%d sum=%v", aggs["m1"].avgCount, aggs["m1"].avgSum)
	}
	if aggs["m1"].similaritySum != 0.8 {
		t.Fatalf("expected reuse-path aggregation to update similarity sum 0.8, got %v", aggs["m1"].similaritySum)
	}

	counters := ensureRuntimeCounterState(params).counters
	if got := counters["fast_path.stage2.edges_visited"]; got != 1 {
		t.Fatalf("expected edges_visited counter 1, got %d", got)
	}
	if got := counters["fast_path.stage2.candidate_group_reuse_hits"]; got != 1 {
		t.Fatalf("expected candidate_group_reuse_hits counter 1, got %d", got)
	}
}

func TestStage2ShouldProcessIndexedEdgePath(t *testing.T) {
	if !stage2ShouldProcessIndexedEdgePath(true) {
		t.Fatalf("expected indexed-edge path predicate to be true when indexedEdges=true")
	}
	if stage2ShouldProcessIndexedEdgePath(false) {
		t.Fatalf("expected indexed-edge path predicate to be false when indexedEdges=false")
	}
}

func TestStage2ShouldSkipIndexedCandidateEdge(t *testing.T) {
	if !stage2ShouldSkipIndexedCandidateEdge(nil) {
		t.Fatalf("expected nil indexed candidate edge to be skipped")
	}
	if stage2ShouldSkipIndexedCandidateEdge(&graph.Edge{ID: "e1"}) {
		t.Fatalf("expected non-nil indexed candidate edge to be processed")
	}
}

func TestStage2IndexedEdgeIterationHelpers(t *testing.T) {
	e := &Executor{}
	params := Params{}
	processed := 0
	edges := []*graph.Edge{nil, {ID: "e1"}}
	iterationCtx := stage2BuildIndexedEdgeIterationContext(edges, params, func(edge *graph.Edge) error {
		processed++
		return nil
	})

	if len(iterationCtx.edges) != 2 {
		t.Fatalf("expected indexed-iteration builder to preserve edge slice")
	}

	if err := e.stage2ProcessIndexedCandidateEdgeIteration(iterationCtx, nil); err != nil {
		t.Fatalf("expected indexed-iteration helper to skip nil edge without error, got %v", err)
	}
	if processed != 0 {
		t.Fatalf("expected indexed-iteration helper to avoid processing nil edge")
	}

	if err := e.stage2ProcessIndexedCandidateEdgeIteration(iterationCtx, &graph.Edge{ID: "e1"}); err != nil {
		t.Fatalf("expected indexed-iteration helper to process non-nil edge, got %v", err)
	}
	if processed != 1 {
		t.Fatalf("expected indexed-iteration helper to process exactly one non-nil edge, got %d", processed)
	}
	if got := ensureRuntimeCounterState(params).counters["fast_path.stage2.index_edges_considered"]; got != 1 {
		t.Fatalf("expected index_edges_considered counter 1, got %d", got)
	}

	noProcessCtx := stage2BuildIndexedEdgeIterationContext(edges, params, nil)
	if err := e.stage2ProcessIndexedCandidateEdgeIteration(noProcessCtx, &graph.Edge{ID: "e2"}); err != nil {
		t.Fatalf("expected indexed-iteration helper with nil callback to no-op, got %v", err)
	}
}

func TestStage2ProcessIndexedEdgePath(t *testing.T) {
	e := &Executor{}
	params := Params{}
	processed := 0
	edges := []*graph.Edge{nil, {ID: "e1"}, {ID: "e2"}}
	if err := e.stage2ProcessIndexedEdgePath(edges, params, func(edge *graph.Edge) error {
		processed++
		return nil
	}); err != nil {
		t.Fatalf("expected indexed-edge path processing to succeed, got %v", err)
	}
	if processed != 2 {
		t.Fatalf("expected indexed-edge path to process two non-nil edges, got %d", processed)
	}
	if got := ensureRuntimeCounterState(params).counters["fast_path.stage2.index_edges_considered"]; got != 2 {
		t.Fatalf("expected index_edges_considered counter 2, got %d", got)
	}
}

func TestStage2WorkItemEdgePathDecisionHelpers(t *testing.T) {
	e := &Executor{}
	params := Params{}
	processed := 0
	edges := []*graph.Edge{{ID: "e1"}}
	decisionCtx := stage2BuildWorkItemEdgePathDecisionContext(context.Background(), nil, "acme", "u1", directedRelationshipPattern{EdgeType: "RATED"}, edges, true, params, func(edge *graph.Edge) error {
		processed++
		return nil
	})

	if decisionCtx.tenant != "acme" || decisionCtx.peerID != "u1" {
		t.Fatalf("expected edge-path decision builder to preserve tenant/peer inputs")
	}
	if decisionCtx.pattern.EdgeType != "RATED" || len(decisionCtx.edges) != 1 {
		t.Fatalf("expected edge-path decision builder to preserve pattern/edges")
	}
	if !stage2ResolveWorkItemEdgePathStrategy(decisionCtx) {
		t.Fatalf("expected edge-path strategy helper to select indexed path")
	}

	if err := e.stage2ProcessResolvedWorkItemEdgePath(decisionCtx, true); err != nil {
		t.Fatalf("expected resolved edge-path helper to execute indexed path, got %v", err)
	}
	if processed != 1 {
		t.Fatalf("expected resolved edge-path helper to process one indexed edge, got %d", processed)
	}
	if got := ensureRuntimeCounterState(params).counters["fast_path.stage2.index_edges_considered"]; got != 1 {
		t.Fatalf("expected index_edges_considered counter 1, got %d", got)
	}
}

func TestStage2ProcessWorkItemEdgePathIndexedBranch(t *testing.T) {
	e := &Executor{}
	params := Params{}
	processed := 0
	edges := []*graph.Edge{{ID: "e1"}}
	if err := e.stage2ProcessWorkItemEdgePath(context.Background(), nil, "", "", directedRelationshipPattern{}, edges, true, params, func(edge *graph.Edge) error {
		processed++
		return nil
	}); err != nil {
		t.Fatalf("expected indexed branch of work-item edge-path processor to succeed, got %v", err)
	}
	if processed != 1 {
		t.Fatalf("expected indexed branch to process one edge, got %d", processed)
	}
}

func TestStage2ProcessSinglePeerWorkItem(t *testing.T) {
	e := &Executor{}
	params := Params{}
	collectState := newStage2CollectOrchestrationState(false)
	aggs := map[string]*fastPeerCandidateAggregate{
		"m1": {},
	}
	groupOrder := []string{}
	item := stage2PeerWorkItem{
		row:                 Row{},
		peer:                &graph.Vertex{ID: "u1"},
		skipWhereEval:       true,
		similarityNumeric:   0.75,
		similarityNumericOK: true,
		edges:               []*graph.Edge{{ID: "e1", Type: "RATED", DstID: "m1", Properties: graph.PropertyMap{"rating": []byte("4.0")}}},
		indexedEdges:        true,
		remainingPotential:  0,
	}
	pattern := directedRelationshipPattern{EdgeType: "RATED"}
	projection := fastPeerCandidateReturnProjection{avgEdgeProperty: "rating"}

	nextFrontier, err := e.stage2ProcessSinglePeerWorkItem(context.Background(), nil, "acme", item, aggs, &groupOrder, pattern, "", projection, &collectState, map[string]struct{}{}, 1, params)
	if err != nil {
		t.Fatalf("expected single work-item processor helper to succeed, got: %v", err)
	}
	if len(nextFrontier) != 0 {
		t.Fatalf("expected early-stop frontier to remain empty when early stop is disabled")
	}
	if aggs["m1"].edgeCount != 1 {
		t.Fatalf("expected reused aggregate edgeCount 1, got %d", aggs["m1"].edgeCount)
	}
	if aggs["m1"].avgCount != 1 || aggs["m1"].avgSum != 4.0 {
		t.Fatalf("expected reused aggregate avg fields count=1 sum=4.0, got count=%d sum=%v", aggs["m1"].avgCount, aggs["m1"].avgSum)
	}
	if aggs["m1"].similaritySum != 0.75 {
		t.Fatalf("expected reused aggregate similarity sum 0.75, got %v", aggs["m1"].similaritySum)
	}

	counters := ensureRuntimeCounterState(params).counters
	if got := counters["fast_path.stage2.index_edges_considered"]; got != 1 {
		t.Fatalf("expected index_edges_considered counter 1, got %d", got)
	}
	if got := counters["fast_path.stage2.edges_visited"]; got != 1 {
		t.Fatalf("expected edges_visited counter 1, got %d", got)
	}
	if got := counters["fast_path.stage2.candidate_group_reuse_hits"]; got != 1 {
		t.Fatalf("expected candidate_group_reuse_hits counter 1, got %d", got)
	}
}

func TestStage2ProcessPeerWorkItems(t *testing.T) {
	e := &Executor{}
	params := Params{}
	collectState := newStage2CollectOrchestrationState(false)
	aggs := map[string]*fastPeerCandidateAggregate{
		"m1": {},
		"m2": {},
	}
	groupOrder := []string{}
	workItems := []stage2PeerWorkItem{
		{
			row:                 Row{},
			peer:                &graph.Vertex{ID: "u1"},
			skipWhereEval:       true,
			similarityNumeric:   0.5,
			similarityNumericOK: true,
			edges:               []*graph.Edge{{ID: "e1", Type: "RATED", DstID: "m1", Properties: graph.PropertyMap{"rating": []byte("3.0")}}},
			indexedEdges:        true,
			remainingPotential:  0,
		},
		{
			row:                 Row{},
			peer:                &graph.Vertex{ID: "u2"},
			skipWhereEval:       true,
			similarityNumeric:   1.25,
			similarityNumericOK: true,
			edges:               []*graph.Edge{{ID: "e2", Type: "RATED", DstID: "m2", Properties: graph.PropertyMap{"rating": []byte("5.0")}}},
			indexedEdges:        true,
			remainingPotential:  0,
		},
	}
	pattern := directedRelationshipPattern{EdgeType: "RATED"}
	projection := fastPeerCandidateReturnProjection{avgEdgeProperty: "rating"}

	nextFrontier, err := e.stage2ProcessPeerWorkItems(context.Background(), nil, "acme", workItems, aggs, &groupOrder, pattern, "", projection, &collectState, map[string]struct{}{}, 1, params)
	if err != nil {
		t.Fatalf("expected work-item loop helper to succeed, got: %v", err)
	}
	if len(nextFrontier) != 0 {
		t.Fatalf("expected early-stop frontier to remain empty when early stop is disabled")
	}
	if aggs["m1"].edgeCount != 1 || aggs["m2"].edgeCount != 1 {
		t.Fatalf("expected each work item to update its aggregate edgeCount once")
	}
	if aggs["m1"].similaritySum != 0.5 || aggs["m2"].similaritySum != 1.25 {
		t.Fatalf("expected work-item loop to preserve per-item similarity accumulation")
	}

	counters := ensureRuntimeCounterState(params).counters
	if got := counters["fast_path.stage2.index_edges_considered"]; got != 2 {
		t.Fatalf("expected index_edges_considered counter 2, got %d", got)
	}
	if got := counters["fast_path.stage2.candidate_group_reuse_hits"]; got != 2 {
		t.Fatalf("expected candidate_group_reuse_hits counter 2, got %d", got)
	}
}

func TestStage2BuildWorkItemProcessingContext(t *testing.T) {
	item := stage2PeerWorkItem{peer: &graph.Vertex{ID: "u1"}, remainingPotential: 1.25}
	aggs := map[string]*fastPeerCandidateAggregate{"m1": {}}
	groupOrder := []string{"m1"}
	pattern := directedRelationshipPattern{EdgeType: "RATED"}
	projection := fastPeerCandidateReturnProjection{avgEdgeProperty: "rating"}
	state := newStage2CollectOrchestrationState(true)
	frontier := map[string]struct{}{"m1": {}}

	processingCtx := stage2BuildWorkItemProcessingContext(item, aggs, &groupOrder, pattern, "", projection, &state, frontier, 3, nil)
	if processingCtx.item.peer == nil || processingCtx.item.peer.ID != "u1" {
		t.Fatalf("expected processing-context builder to preserve work-item peer")
	}
	if processingCtx.aggs == nil || processingCtx.groupOrder != &groupOrder {
		t.Fatalf("expected processing-context builder to preserve aggregate/group-order references")
	}
	if processingCtx.pattern.EdgeType != "RATED" || processingCtx.projection.avgEdgeProperty != "rating" {
		t.Fatalf("expected processing-context builder to preserve pattern/projection")
	}
	if processingCtx.collectState == nil || processingCtx.earlyStopKeep != 3 {
		t.Fatalf("expected processing-context builder to preserve collect-state and keep values")
	}
	if _, ok := processingCtx.earlyStopFrontier["m1"]; !ok {
		t.Fatalf("expected processing-context builder to preserve early-stop frontier")
	}
}

func TestStage2AdvanceWorkItemFrontier(t *testing.T) {
	e := &Executor{}
	params := Params{}
	aggs := map[string]*fastPeerCandidateAggregate{
		"m1": {edgeCount: 2, similaritySum: 10},
		"m2": {edgeCount: 1, similaritySum: 2},
	}
	groupOrder := []string{"m1", "m2"}
	state := newStage2CollectOrchestrationState(true)
	state.totalRemainingPotential = 3
	item := stage2PeerWorkItem{remainingPotential: 1}
	processingCtx := stage2BuildWorkItemProcessingContext(item, aggs, &groupOrder, directedRelationshipPattern{}, "", fastPeerCandidateReturnProjection{}, &state, map[string]struct{}{}, 1, nil)

	nextFrontier := e.stage2AdvanceWorkItemFrontier(processingCtx, params)
	if state.totalRemainingPotential != 2 {
		t.Fatalf("expected frontier-advance helper to consume remaining potential to 2, got %v", state.totalRemainingPotential)
	}
	if len(nextFrontier) != 1 {
		t.Fatalf("expected frontier-advance helper to activate one frontier group, got %d", len(nextFrontier))
	}
	if _, ok := nextFrontier["m1"]; !ok {
		t.Fatalf("expected frontier-advance helper to include top group m1")
	}
}

func TestStage2BuildPeerWorkItemLoopContext(t *testing.T) {
	ctx := context.Background()
	aggs := map[string]*fastPeerCandidateAggregate{"m1": {}}
	groupOrder := []string{"m1"}
	pattern := directedRelationshipPattern{EdgeType: "RATED"}
	projection := fastPeerCandidateReturnProjection{avgEdgeProperty: "rating"}
	state := newStage2CollectOrchestrationState(true)
	params := Params{"tenant": "acme"}

	loopCtx := stage2BuildPeerWorkItemLoopContext(ctx, nil, "acme", aggs, &groupOrder, pattern, "", projection, &state, 4, params)
	if loopCtx.ctx == nil || loopCtx.tenant != "acme" {
		t.Fatalf("expected loop-context builder to preserve ctx and tenant")
	}
	if loopCtx.aggs == nil || loopCtx.groupOrder != &groupOrder {
		t.Fatalf("expected loop-context builder to preserve aggregate/group-order references")
	}
	if loopCtx.pattern.EdgeType != "RATED" || loopCtx.projection.avgEdgeProperty != "rating" {
		t.Fatalf("expected loop-context builder to preserve pattern/projection")
	}
	if loopCtx.collectState == nil || loopCtx.earlyStopKeep != 4 {
		t.Fatalf("expected loop-context builder to preserve collect-state and keep values")
	}
	if loopCtx.params["tenant"] != "acme" {
		t.Fatalf("expected loop-context builder to preserve params map")
	}
}

func TestStage2ProcessPeerWorkItemLoopIterationAndFrontierUpdate(t *testing.T) {
	e := &Executor{}
	params := Params{}
	collectState := newStage2CollectOrchestrationState(false)
	aggs := map[string]*fastPeerCandidateAggregate{"m1": {}}
	groupOrder := []string{}
	pattern := directedRelationshipPattern{EdgeType: "RATED"}
	projection := fastPeerCandidateReturnProjection{avgEdgeProperty: "rating"}
	item := stage2PeerWorkItem{
		row:                 Row{},
		peer:                &graph.Vertex{ID: "u1"},
		skipWhereEval:       true,
		similarityNumeric:   0.7,
		similarityNumericOK: true,
		edges:               []*graph.Edge{{ID: "e1", Type: "RATED", DstID: "m1", Properties: graph.PropertyMap{"rating": []byte("4.0")}}},
		indexedEdges:        true,
		remainingPotential:  0,
	}
	loopCtx := stage2BuildPeerWorkItemLoopContext(context.Background(), nil, "acme", aggs, &groupOrder, pattern, "", projection, &collectState, 1, params)

	nextFrontier, err := e.stage2ProcessPeerWorkItemLoopIteration(loopCtx, item, map[string]struct{}{})
	if err != nil {
		t.Fatalf("expected loop-iteration helper to succeed, got: %v", err)
	}
	if len(nextFrontier) != 0 {
		t.Fatalf("expected loop-iteration helper to keep empty frontier when early stop is disabled")
	}
	if aggs["m1"].edgeCount != 1 || aggs["m1"].avgCount != 1 || aggs["m1"].avgSum != 4.0 {
		t.Fatalf("expected loop-iteration helper to update aggregate edge/avg fields")
	}
	if aggs["m1"].similaritySum != 0.7 {
		t.Fatalf("expected loop-iteration helper to update aggregate similarity sum 0.7")
	}

	current := map[string]struct{}{"old": {}}
	updated := stage2UpdatePeerWorkItemLoopFrontier(current, nextFrontier)
	if len(updated) != 0 {
		t.Fatalf("expected frontier-update helper to adopt next frontier")
	}

	counters := ensureRuntimeCounterState(params).counters
	if got := counters["fast_path.stage2.index_edges_considered"]; got != 1 {
		t.Fatalf("expected index_edges_considered counter 1, got %d", got)
	}
	if got := counters["fast_path.stage2.candidate_group_reuse_hits"]; got != 1 {
		t.Fatalf("expected candidate_group_reuse_hits counter 1, got %d", got)
	}
}

func TestStage2BuildAggregationDecisionContext(t *testing.T) {
	aggs := map[string]*fastPeerCandidateAggregate{}
	groupOrder := []string{}
	pattern := directedRelationshipPattern{EdgeType: "RATED"}
	row := Row{"seed": "v"}
	peer := &graph.Vertex{ID: "u1"}
	edge := &graph.Edge{ID: "e1", DstID: "m1"}
	projection := fastPeerCandidateReturnProjection{avgEdgeProperty: "rating"}

	decisionCtx := stage2BuildAggregationDecisionContext(aggs, &groupOrder, "m1", pattern, "", row, peer, edge, true, nil, projection, 0.9, true)
	if decisionCtx.groupOrder != &groupOrder {
		t.Fatalf("expected aggregation-decision context builder to preserve group-order pointer")
	}
	decisionCtx.aggs["probe"] = &fastPeerCandidateAggregate{}
	if _, ok := aggs["probe"]; !ok {
		t.Fatalf("expected aggregation-decision context builder to preserve aggregate map reference")
	}
	if decisionCtx.groupID != "m1" || decisionCtx.pattern.EdgeType != "RATED" {
		t.Fatalf("expected aggregation-decision context builder to preserve group/pattern")
	}
	if decisionCtx.row["seed"] != "v" || decisionCtx.peer != peer || decisionCtx.edge != edge {
		t.Fatalf("expected aggregation-decision context builder to preserve row/peer/edge")
	}
	if !decisionCtx.skipWhereEval || !decisionCtx.similarityNumericOK || decisionCtx.similarityNumeric != 0.9 {
		t.Fatalf("expected aggregation-decision context builder to preserve skip/similarity fields")
	}
}

func TestStage2ApplyResolvedAggregationDecision(t *testing.T) {
	e := &Executor{}
	params := Params{}
	aggs := map[string]*fastPeerCandidateAggregate{}
	groupOrder := []string{}
	decisionCtx := stage2BuildAggregationDecisionContext(
		aggs,
		&groupOrder,
		"m1",
		directedRelationshipPattern{},
		"",
		Row{},
		nil,
		&graph.Edge{Properties: graph.PropertyMap{"rating": []byte("4.0")}},
		true,
		nil,
		fastPeerCandidateReturnProjection{avgEdgeProperty: "rating"},
		0.6,
		true,
	)

	handled := e.stage2ApplyResolvedAggregationDecision(decisionCtx, true, Row{"m": "scope"}, &graph.Vertex{ID: "m1"}, params)
	if !handled {
		t.Fatalf("expected resolved aggregation-decision helper to report handled=true")
	}
	if len(groupOrder) != 1 || groupOrder[0] != "m1" {
		t.Fatalf("expected resolved aggregation-decision helper to create group m1")
	}
	if aggs["m1"].edgeCount != 1 || aggs["m1"].avgCount != 1 || aggs["m1"].avgSum != 4.0 {
		t.Fatalf("expected resolved aggregation-decision helper to accumulate edge/avg fields")
	}
	if aggs["m1"].similaritySum != 0.6 {
		t.Fatalf("expected resolved aggregation-decision helper to accumulate similarity sum 0.6")
	}
}

func TestStage2ApplyCandidateAggregationDecisionReuseFastPath(t *testing.T) {
	e := &Executor{}
	params := Params{}
	aggs := map[string]*fastPeerCandidateAggregate{
		"m1": {},
	}
	groupOrder := []string{}
	projection := fastPeerCandidateReturnProjection{avgEdgeProperty: "rating"}
	edge := &graph.Edge{DstID: "m1", Properties: graph.PropertyMap{"rating": []byte("4.0")}}

	handled, err := e.stage2ApplyCandidateAggregationDecision(context.Background(), nil, aggs, &groupOrder, "m1", directedRelationshipPattern{}, "", Row{}, nil, edge, true, nil, projection, 0.8, true, params)
	if err != nil {
		t.Fatalf("expected reuse fast-path helper to avoid scope-eval and return nil error, got: %v", err)
	}
	if !handled {
		t.Fatalf("expected helper to report handled on reuse fast-path")
	}
	if aggs["m1"].edgeCount != 1 {
		t.Fatalf("expected reused aggregate edge count 1, got %d", aggs["m1"].edgeCount)
	}
	if aggs["m1"].avgCount != 1 || aggs["m1"].avgSum != 4.0 {
		t.Fatalf("expected reused aggregate avg fields count=1 sum=4.0, got count=%d sum=%v", aggs["m1"].avgCount, aggs["m1"].avgSum)
	}
	if aggs["m1"].similaritySum != 0.8 {
		t.Fatalf("expected reused aggregate similarity sum 0.8, got %v", aggs["m1"].similaritySum)
	}
	if got := ensureRuntimeCounterState(params).counters["fast_path.stage2.candidate_group_reuse_hits"]; got != 1 {
		t.Fatalf("expected candidate_group_reuse_hits counter 1, got %d", got)
	}
}

func TestStage2ShouldHandleByReuseAggregation(t *testing.T) {
	e := &Executor{}
	params := Params{}
	projection := fastPeerCandidateReturnProjection{avgEdgeProperty: "rating"}
	edge := &graph.Edge{DstID: "m1", Properties: graph.PropertyMap{"rating": []byte("4.0")}}

	aggs := map[string]*fastPeerCandidateAggregate{
		"m1": {},
	}
	if !e.stage2ShouldHandleByReuseAggregation(aggs, "m1", true, edge, projection, 0.5, true, params) {
		t.Fatalf("expected helper to handle candidate via reuse path")
	}
	if aggs["m1"].edgeCount != 1 {
		t.Fatalf("expected reused aggregate edgeCount 1, got %d", aggs["m1"].edgeCount)
	}
	if got := ensureRuntimeCounterState(params).counters["fast_path.stage2.candidate_group_reuse_hits"]; got != 1 {
		t.Fatalf("expected candidate_group_reuse_hits counter 1, got %d", got)
	}

	paramsMiss := Params{}
	aggsMiss := map[string]*fastPeerCandidateAggregate{}
	if e.stage2ShouldHandleByReuseAggregation(aggsMiss, "m2", true, edge, projection, 0.5, true, paramsMiss) {
		t.Fatalf("expected helper to return false when aggregate does not exist")
	}
	if got := ensureRuntimeCounterState(paramsMiss).counters["fast_path.stage2.candidate_group_reuse_hits"]; got != 0 {
		t.Fatalf("expected no reuse counter increment on missing aggregate, got %d", got)
	}

	paramsSkip := Params{}
	aggsSkip := map[string]*fastPeerCandidateAggregate{
		"m1": {},
	}
	if e.stage2ShouldHandleByReuseAggregation(aggsSkip, "m1", false, edge, projection, 0.5, true, paramsSkip) {
		t.Fatalf("expected helper to return false when skipWhereEval is disabled")
	}
	if got := ensureRuntimeCounterState(paramsSkip).counters["fast_path.stage2.candidate_group_reuse_hits"]; got != 0 {
		t.Fatalf("expected no reuse counter increment when skipWhereEval is false, got %d", got)
	}
}

func TestStage2ApplyResolvedCandidateAggregationIfMatched(t *testing.T) {
	e := &Executor{}
	params := Params{}
	aggs := map[string]*fastPeerCandidateAggregate{}
	groupOrder := []string{}
	projection := fastPeerCandidateReturnProjection{avgEdgeProperty: "rating"}
	edge := &graph.Edge{Properties: graph.PropertyMap{"rating": []byte("4.0")}}

	if handled := stage2ApplyResolvedCandidateAggregationIfMatched(aggs, &groupOrder, "m1", false, Row{}, nil, edge, projection, 0.6, true, e, params); !handled {
		t.Fatalf("expected helper to report handled when candidate does not match")
	}
	if len(groupOrder) != 0 {
		t.Fatalf("expected no aggregate creation when matched=false")
	}

	merged := Row{"m": "value"}
	candidate := &graph.Vertex{ID: "m1"}
	if handled := stage2ApplyResolvedCandidateAggregationIfMatched(aggs, &groupOrder, "m1", true, merged, candidate, edge, projection, 0.6, true, e, params); !handled {
		t.Fatalf("expected helper to report handled when candidate matches")
	}
	if len(groupOrder) != 1 || groupOrder[0] != "m1" {
		t.Fatalf("expected matched candidate to create aggregate group m1")
	}
	agg := aggs["m1"]
	if agg == nil {
		t.Fatalf("expected aggregate for matched candidate")
	}
	if agg.edgeCount != 1 {
		t.Fatalf("expected aggregate edgeCount 1, got %d", agg.edgeCount)
	}
	if agg.avgCount != 1 || agg.avgSum != 4.0 {
		t.Fatalf("expected aggregate avg fields count=1 sum=4.0, got count=%d sum=%v", agg.avgCount, agg.avgSum)
	}
	if agg.similaritySum != 0.6 {
		t.Fatalf("expected aggregate similarity sum 0.6, got %v", agg.similaritySum)
	}
	if got := ensureRuntimeCounterState(params).counters["fast_path.stage2.candidate_groups_created"]; got != 1 {
		t.Fatalf("expected candidate_groups_created counter 1, got %d", got)
	}
}

func TestStage2BindRightCandidateIfNamed(t *testing.T) {
	merged := Row{"existing": "value"}
	candidate := &graph.Vertex{ID: "m1"}

	stage2BindRightCandidateIfNamed(merged, "m", candidate)
	if merged["m"] != candidate {
		t.Fatalf("expected named right variable binding to be set")
	}

	stage2BindRightCandidateIfNamed(merged, "   ", &graph.Vertex{ID: "m2"})
	if got := merged["m"]; got != candidate {
		t.Fatalf("expected blank right variable binding to be ignored")
	}

	stage2BindRightCandidateIfNamed(nil, "m", candidate)
}

func TestStage2CandidateWhereGateDecisionHelpers(t *testing.T) {
	e := &Executor{}
	params := Params{}
	merged := Row{"r": 5}
	decisionCtx := stage2BuildCandidateWhereGateDecisionContext("r >= 3", merged, false)

	if decisionCtx.whereRaw != "r >= 3" || decisionCtx.merged["r"] != 5 {
		t.Fatalf("expected where-gate decision builder to preserve where/merged inputs")
	}
	if stage2ShouldBypassCandidateWhereGate(decisionCtx) {
		t.Fatalf("expected where-gate bypass helper to require evaluation for non-empty where")
	}

	bypassCtx := stage2BuildCandidateWhereGateDecisionContext("", merged, false)
	if !stage2ShouldBypassCandidateWhereGate(bypassCtx) {
		t.Fatalf("expected where-gate bypass helper to bypass blank where")
	}

	matched := e.stage2ResolveCandidateWhereGate(decisionCtx, false, params)
	if matched {
		t.Fatalf("expected where-gate resolver helper to reject unmatched candidate")
	}
	if got := ensureRuntimeCounterState(params).counters["fast_path.stage2.where_eval_drops"]; got != 1 {
		t.Fatalf("expected where_eval_drops counter 1, got %d", got)
	}

	paramsMatched := Params{}
	if !e.stage2ResolveCandidateWhereGate(decisionCtx, true, paramsMatched) {
		t.Fatalf("expected where-gate resolver helper to keep matched candidate")
	}
	if got := ensureRuntimeCounterState(paramsMatched).counters["fast_path.stage2.where_eval_drops"]; got != 0 {
		t.Fatalf("expected where_eval_drops counter 0 for matched candidate, got %d", got)
	}
}

func TestStage2EvaluateCandidateWhereGate(t *testing.T) {
	e := &Executor{}
	decisionCtx := stage2BuildCandidateWhereGateDecisionContext("true", Row{}, false)

	ok, err := e.stage2EvaluateCandidateWhereGate(context.Background(), nil, decisionCtx, Params{})
	if err != nil {
		t.Fatalf("expected where-gate evaluator helper to succeed, got %v", err)
	}
	if !ok {
		t.Fatalf("expected where-gate evaluator helper to return ok=true for true expression")
	}
}

func TestStage2ApplyCandidateWhereGate(t *testing.T) {
	e := &Executor{}

	paramsSkip := Params{}
	matched, err := e.stage2ApplyCandidateWhereGate(context.Background(), nil, "r.rating >= 3", Row{"r": 5}, true, paramsSkip)
	if err != nil {
		t.Fatalf("expected skipWhereEval path to avoid errors, got %v", err)
	}
	if !matched {
		t.Fatalf("expected skipWhereEval path to keep candidate matched")
	}

	paramsTrue := Params{}
	matched, err = e.stage2ApplyCandidateWhereGate(context.Background(), nil, "true", Row{}, false, paramsTrue)
	if err != nil {
		t.Fatalf("expected true where expression to evaluate without error, got %v", err)
	}
	if !matched {
		t.Fatalf("expected true where expression to keep candidate matched")
	}

	paramsFalse := Params{}
	matched, err = e.stage2ApplyCandidateWhereGate(context.Background(), nil, "false", Row{}, false, paramsFalse)
	if err != nil {
		t.Fatalf("expected false where expression to evaluate without error, got %v", err)
	}
	if matched {
		t.Fatalf("expected false where expression to reject candidate")
	}
	if got := ensureRuntimeCounterState(paramsFalse).counters["fast_path.stage2.where_eval_drops"]; got != 1 {
		t.Fatalf("expected where_eval_drops counter 1, got %d", got)
	}
}

func TestStage2NormalizeRightCandidateMatch(t *testing.T) {
	candidate := &graph.Vertex{ID: "m1"}

	normalized, matched, err := stage2NormalizeRightCandidateMatch(candidate, true, nil)
	if err != nil {
		t.Fatalf("expected matched normalization to return nil error, got %v", err)
	}
	if !matched {
		t.Fatalf("expected matched normalization to keep matched=true")
	}
	if normalized != candidate {
		t.Fatalf("expected matched normalization to preserve candidate pointer")
	}

	normalized, matched, err = stage2NormalizeRightCandidateMatch(candidate, false, nil)
	if err != nil {
		t.Fatalf("expected unmatched normalization to return nil error, got %v", err)
	}
	if matched {
		t.Fatalf("expected unmatched normalization to set matched=false")
	}
	if normalized != nil {
		t.Fatalf("expected unmatched normalization to clear candidate")
	}

	resolveErr := errors.New("resolve failed")
	normalized, matched, err = stage2NormalizeRightCandidateMatch(candidate, true, resolveErr)
	if !errors.Is(err, resolveErr) {
		t.Fatalf("expected resolve error passthrough, got %v", err)
	}
	if matched {
		t.Fatalf("expected resolve error normalization to set matched=false")
	}
	if normalized != nil {
		t.Fatalf("expected resolve error normalization to clear candidate")
	}
}

func TestStage2NormalizeCandidateWhereGateResult(t *testing.T) {
	matched, err := stage2NormalizeCandidateWhereGateResult(true, nil)
	if err != nil {
		t.Fatalf("expected matched where result to keep nil error, got %v", err)
	}
	if !matched {
		t.Fatalf("expected matched where result to remain matched")
	}

	matched, err = stage2NormalizeCandidateWhereGateResult(false, nil)
	if err != nil {
		t.Fatalf("expected unmatched where result to keep nil error, got %v", err)
	}
	if matched {
		t.Fatalf("expected unmatched where result to stay unmatched")
	}

	whereErr := errors.New("where failed")
	matched, err = stage2NormalizeCandidateWhereGateResult(true, whereErr)
	if !errors.Is(err, whereErr) {
		t.Fatalf("expected where error passthrough, got %v", err)
	}
	if matched {
		t.Fatalf("expected where error normalization to return matched=false")
	}
}

func TestStage2BuildCandidateScopeEvalContext(t *testing.T) {
	pattern := directedRelationshipPattern{Right: vertexPattern{Var: "m"}, EdgeType: "RATED"}
	row := Row{"seed": "v"}
	peer := &graph.Vertex{ID: "u1"}
	edge := &graph.Edge{ID: "e1", DstID: "m1"}

	evalCtx := stage2BuildCandidateScopeEvalContext(pattern, "m.rating > 3", row, peer, edge, true)
	if evalCtx.pattern.EdgeType != "RATED" {
		t.Fatalf("expected eval-context builder to preserve pattern")
	}
	if evalCtx.whereRaw != "m.rating > 3" {
		t.Fatalf("expected eval-context builder to preserve whereRaw")
	}
	if evalCtx.row["seed"] != "v" || evalCtx.peer != peer || evalCtx.edge != edge {
		t.Fatalf("expected eval-context builder to preserve row/peer/edge")
	}
	if !evalCtx.skipWhereEval {
		t.Fatalf("expected eval-context builder to preserve skipWhereEval=true")
	}
}

func TestStage2BuildCandidateScopeMergedRow(t *testing.T) {
	mergedBase := Row{"seed": "v"}
	candidate := &graph.Vertex{ID: "m1"}
	merged := stage2BuildCandidateScopeMergedRow(mergedBase, "m", candidate)

	if merged["seed"] != "v" {
		t.Fatalf("expected merged-row helper to preserve base scope entries")
	}
	if merged["m"] != candidate {
		t.Fatalf("expected merged-row helper to bind right candidate variable")
	}
	if _, ok := mergedBase["m"]; ok {
		t.Fatalf("expected merged-row helper to avoid mutating merged base row")
	}
}

func TestStage2ResolveCandidateScopeWhereMatch(t *testing.T) {
	e := &Executor{}

	evalCtx := stage2BuildCandidateScopeEvalContext(directedRelationshipPattern{}, "true", Row{}, nil, nil, false)
	matched, err := e.stage2ResolveCandidateScopeWhereMatch(context.Background(), nil, evalCtx, Row{}, Params{})
	if err != nil {
		t.Fatalf("expected where-match helper to succeed for true expression, got: %v", err)
	}
	if !matched {
		t.Fatalf("expected where-match helper to return matched=true for true expression")
	}

	paramsFalse := Params{}
	evalCtx = stage2BuildCandidateScopeEvalContext(directedRelationshipPattern{}, "false", Row{}, nil, nil, false)
	matched, err = e.stage2ResolveCandidateScopeWhereMatch(context.Background(), nil, evalCtx, Row{}, paramsFalse)
	if err != nil {
		t.Fatalf("expected where-match helper to succeed for false expression, got: %v", err)
	}
	if matched {
		t.Fatalf("expected where-match helper to return matched=false for false expression")
	}
	if got := ensureRuntimeCounterState(paramsFalse).counters["fast_path.stage2.where_eval_drops"]; got != 1 {
		t.Fatalf("expected where_eval_drops counter 1 on false expression, got %d", got)
	}

	evalCtx = stage2BuildCandidateScopeEvalContext(directedRelationshipPattern{}, "m.rating > 3", Row{}, nil, nil, true)
	matched, err = e.stage2ResolveCandidateScopeWhereMatch(context.Background(), nil, evalCtx, Row{}, Params{})
	if err != nil {
		t.Fatalf("expected where-match helper to bypass where eval when skipWhereEval=true, got: %v", err)
	}
	if !matched {
		t.Fatalf("expected where-match helper to keep matched=true on skipWhereEval path")
	}
}

func TestStage2CandidateScopeResolutionAbortHelpers(t *testing.T) {
	abort, abortErr := stage2ShouldAbortCandidateScopeResolution(true, nil)
	if abort || abortErr != nil {
		t.Fatalf("expected abort helper to continue when matched=true and err=nil")
	}

	abort, abortErr = stage2ShouldAbortCandidateScopeResolution(false, nil)
	if !abort || abortErr != nil {
		t.Fatalf("expected abort helper to stop with nil error when matched=false")
	}
	merged, candidate, matched, err := stage2BuildCandidateScopeAbortResult(abortErr)
	if err != nil || matched || merged != nil || candidate != nil {
		t.Fatalf("expected abort-result helper to shape nil result tuple on unmatched path")
	}

	resolveErr := errors.New("hydrate failed")
	abort, abortErr = stage2ShouldAbortCandidateScopeResolution(false, resolveErr)
	if !abort || !errors.Is(abortErr, resolveErr) {
		t.Fatalf("expected abort helper to forward resolution error")
	}
	merged, candidate, matched, err = stage2BuildCandidateScopeAbortResult(abortErr)
	if !errors.Is(err, resolveErr) || matched || merged != nil || candidate != nil {
		t.Fatalf("expected abort-result helper to preserve abort error")
	}
}

func TestStage2CandidateScopeHydrationAndMergedMatchHelpers(t *testing.T) {
	e := &Executor{}
	evalCtx := stage2BuildCandidateScopeEvalContext(
		directedRelationshipPattern{Right: vertexPattern{Var: "m", PropertiesRaw: "id: 'm1'"}},
		"true",
		Row{},
		nil,
		&graph.Edge{DstID: "m1"},
		false,
	)
	mergedBase := Row{}
	hydrationPolicy := &deferredHydrationPolicy{}
	hydrationCtx := stage2BuildCandidateScopeHydrationDecisionContext(evalCtx, mergedBase, hydrationPolicy)
	if hydrationCtx.evalCtx.pattern.Right.Var != "m" || hydrationCtx.hydrationPolicy != hydrationPolicy {
		t.Fatalf("expected hydration decision builder to preserve eval context, merged base, and policy")
	}
	hydrationCtx.mergedBase["probe"] = "v"
	if mergedBase["probe"] != "v" {
		t.Fatalf("expected hydration decision builder to preserve merged-base map reference")
	}

	hydrationPolicy = newDeferredHydrationPolicy(e, context.Background(), nil, "acme", Params{})
	hydrationCtx = stage2BuildCandidateScopeHydrationDecisionContext(evalCtx, mergedBase, hydrationPolicy)
	resolvedCandidate, matched, err := e.stage2ResolveCandidateScopeHydratedMatch(hydrationCtx)
	if err != nil {
		t.Fatalf("expected hydrated-match helper to resolve candidate without error, got %v", err)
	}
	if matched || resolvedCandidate != nil {
		t.Fatalf("expected hydrated-match helper to return unmatched result when tx is nil")
	}

	candidate := &graph.Vertex{ID: "m1"}

	merged, whereMatched, err := e.stage2ResolveCandidateScopeMergedMatch(context.Background(), nil, evalCtx, mergedBase, candidate, Params{})
	if err != nil {
		t.Fatalf("expected merged-match helper to evaluate where without error, got %v", err)
	}
	if !whereMatched {
		t.Fatalf("expected merged-match helper to preserve matched=true for true where")
	}
	if merged["m"] != candidate {
		t.Fatalf("expected merged-match helper to bind right variable candidate")
	}
}

func TestStage2CollectOrchestrationStateTryActivateEarlyStopFrontier(t *testing.T) {
	state := newStage2CollectOrchestrationState(true)
	e := &Executor{}
	params := Params{}
	frontier := map[string]struct{}{"m1": {}}

	active := map[string]struct{}{}
	active = state.tryActivateEarlyStopFrontier(10, 5, active, frontier, e, params)
	if len(active) != 1 {
		t.Fatalf("expected frontier activation when boundary exceeds outside score")
	}
	if _, ok := active["m1"]; !ok {
		t.Fatalf("expected activated frontier to include m1")
	}

	active = state.tryActivateEarlyStopFrontier(20, 1, active, map[string]struct{}{"m2": {}}, e, params)
	if len(active) != 1 {
		t.Fatalf("expected existing frontier to remain unchanged after first activation")
	}
	if _, ok := active["m1"]; !ok {
		t.Fatalf("expected original frontier to remain active")
	}

	inactive := map[string]struct{}{}
	inactive = state.tryActivateEarlyStopFrontier(4, 5, inactive, frontier, e, params)
	if len(inactive) != 0 {
		t.Fatalf("expected no activation when boundary does not exceed outside score")
	}

	if got := ensureRuntimeCounterState(params).counters["fast_path.stage2.early_stop_triggers"]; got != 1 {
		t.Fatalf("expected early-stop trigger counter 1, got %d", got)
	}
}

func TestStage2CollectOrchestrationStateMaxOutsideScore(t *testing.T) {
	state := newStage2CollectOrchestrationState(true)
	state.totalRemainingPotential = 3.0

	if got := state.maxOutsideScore(math.Inf(-1)); got != 3.0 {
		t.Fatalf("expected maxOutsideScore to equal remaining potential when maxNonFrontierScore is -Inf, got %v", got)
	}
	if got := state.maxOutsideScore(5.0); got != 8.0 {
		t.Fatalf("expected maxOutsideScore to include non-frontier upper bound, got %v", got)
	}
	if got := state.maxOutsideScore(-2.0); got != 3.0 {
		t.Fatalf("expected maxOutsideScore to remain remaining potential when non-frontier bound is smaller, got %v", got)
	}

	var nilState *stage2CollectOrchestrationState
	if got := nilState.maxOutsideScore(10.0); got != 0 {
		t.Fatalf("expected nil state maxOutsideScore to be 0, got %v", got)
	}
}

func TestStage2CollectOrchestrationStateResolveEarlyStopFrontier(t *testing.T) {
	state := newStage2CollectOrchestrationState(true)
	e := &Executor{}
	params := Params{}

	aggs := map[string]*fastPeerCandidateAggregate{
		"m1": {edgeCount: 2, similaritySum: 10},
		"m2": {edgeCount: 1, similaritySum: 1},
	}
	groupOrder := []string{"m1", "m2"}

	frontier := state.resolveEarlyStopFrontier(aggs, groupOrder, 1, map[string]struct{}{}, e, params)
	if len(frontier) != 1 {
		t.Fatalf("expected one active frontier group after activation")
	}
	if _, ok := frontier["m1"]; !ok {
		t.Fatalf("expected top-scoring group m1 in frontier")
	}

	notReady := state.resolveEarlyStopFrontier(aggs, groupOrder, 3, map[string]struct{}{}, e, params)
	if len(notReady) != 0 {
		t.Fatalf("expected no frontier activation when boundary is not ready")
	}

	if got := ensureRuntimeCounterState(params).counters["fast_path.stage2.early_stop_triggers"]; got != 1 {
		t.Fatalf("expected early-stop trigger counter 1, got %d", got)
	}
}

func TestStage2CollectOrchestrationStateAdvanceEarlyStopAfterWorkItem(t *testing.T) {
	e := &Executor{}
	params := Params{}
	aggs := map[string]*fastPeerCandidateAggregate{
		"m1": {edgeCount: 2, similaritySum: 10},
		"m2": {edgeCount: 1, similaritySum: 1},
	}
	groupOrder := []string{"m1", "m2"}

	state := newStage2CollectOrchestrationState(true)
	state.totalRemainingPotential = 5
	frontier := state.advanceEarlyStopAfterWorkItem(2, aggs, groupOrder, 1, map[string]struct{}{}, e, params)
	if state.totalRemainingPotential != 3 {
		t.Fatalf("expected remaining potential 3 after advance, got %v", state.totalRemainingPotential)
	}
	if len(frontier) != 1 {
		t.Fatalf("expected active frontier after advance")
	}
	if _, ok := frontier["m1"]; !ok {
		t.Fatalf("expected top-scoring group m1 in frontier")
	}
	if got := ensureRuntimeCounterState(params).counters["fast_path.stage2.early_stop_checks"]; got != 1 {
		t.Fatalf("expected early-stop checks counter 1, got %d", got)
	}

	disabled := newStage2CollectOrchestrationState(false)
	disabled.totalRemainingPotential = 9
	paramsDisabled := Params{}
	initialFrontier := map[string]struct{}{"m9": {}}
	nextFrontier := disabled.advanceEarlyStopAfterWorkItem(4, aggs, groupOrder, 1, initialFrontier, e, paramsDisabled)
	if disabled.totalRemainingPotential != 9 {
		t.Fatalf("expected disabled early-stop state to preserve remaining potential")
	}
	if len(nextFrontier) != 1 {
		t.Fatalf("expected disabled early-stop state to preserve frontier")
	}
	if _, ok := nextFrontier["m9"]; !ok {
		t.Fatalf("expected existing frontier m9 to remain unchanged")
	}
	if got := ensureRuntimeCounterState(paramsDisabled).counters["fast_path.stage2.early_stop_checks"]; got != 0 {
		t.Fatalf("expected no early-stop checks counter when disabled, got %d", got)
	}
}

func TestFinalizeStage2OutputRowsNonTopK(t *testing.T) {
	e := &Executor{}
	params := Params{}
	aggs := map[string]*fastPeerCandidateAggregate{
		"m1": {edgeCount: 2, avgSum: 8, avgCount: 2, similaritySum: 3.5},
		"m2": {edgeCount: 1, avgSum: 2, avgCount: 1, similaritySum: 1.0},
	}
	groupOrder := []string{"m1", "m2"}
	projection := fastPeerCandidateReturnProjection{
		countKey:         "count(r)",
		avgKey:           "avg(r.rating)",
		sumSimilarityKey: "sum(sim)",
	}

	rows, err := e.finalizeStage2OutputRows(aggs, groupOrder, false, projection, fastPeerCandidateTopKSpec{}, projectionClauseSpec{}, params)
	if err != nil {
		t.Fatalf("expected non-top-k finalization to succeed, got error: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected two rows from non-top-k finalization, got %d", len(rows))
	}
	if got := ensureRuntimeCounterState(params).counters["fast_path.stage2.candidate_groups_total"]; got != 2 {
		t.Fatalf("expected candidate-groups-total counter 2, got %d", got)
	}
	if got := ensureRuntimeCounterState(params).counters["fast_path.stage2.topk_pushdown_applied"]; got != 0 {
		t.Fatalf("expected no top-k counter for non-top-k finalization, got %d", got)
	}
}

func TestFinalizeStage2OutputRowsTopK(t *testing.T) {
	e := &Executor{}
	params := Params{}
	aggs := map[string]*fastPeerCandidateAggregate{
		"m1": {edgeCount: 2, avgSum: 8, avgCount: 2, similaritySum: 10},
		"m2": {edgeCount: 1, avgSum: 2, avgCount: 1, similaritySum: 1},
	}
	groupOrder := []string{"m1", "m2"}
	projection := fastPeerCandidateReturnProjection{
		countKey:         "count(r)",
		avgKey:           "avg(r.rating)",
		sumSimilarityKey: "sum(sim)",
	}
	topK := fastPeerCandidateTopKSpec{descending: true, skip: 0, limit: 1}

	rows, err := e.finalizeStage2OutputRows(aggs, groupOrder, true, projection, topK, projectionClauseSpec{}, params)
	if err != nil {
		t.Fatalf("expected top-k finalization to succeed, got error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected one row from top-k finalization, got %d", len(rows))
	}
	if got := ensureRuntimeCounterState(params).counters["fast_path.stage2.candidate_groups_total"]; got != 2 {
		t.Fatalf("expected candidate-groups-total counter 2, got %d", got)
	}
	if got := ensureRuntimeCounterState(params).counters["fast_path.stage2.topk_pushdown_applied"]; got != 1 {
		t.Fatalf("expected top-k counter 1 for top-k finalization, got %d", got)
	}
}

func TestStage2ShouldFinalizeWithTopK(t *testing.T) {
	if stage2ShouldFinalizeWithTopK(false) {
		t.Fatalf("expected non-top-k finalization path when useTopK=false")
	}
	if !stage2ShouldFinalizeWithTopK(true) {
		t.Fatalf("expected top-k finalization path when useTopK=true")
	}
}

func TestStage2ShouldIncludeAggregateInFinalRows(t *testing.T) {
	if stage2ShouldIncludeAggregateInFinalRows(nil) {
		t.Fatalf("expected nil aggregate to be excluded from final rows")
	}
	if stage2ShouldIncludeAggregateInFinalRows(&fastPeerCandidateAggregate{edgeCount: 0}) {
		t.Fatalf("expected zero-edge aggregate to be excluded from final rows")
	}
	if !stage2ShouldIncludeAggregateInFinalRows(&fastPeerCandidateAggregate{edgeCount: 1}) {
		t.Fatalf("expected positive-edge aggregate to be included in final rows")
	}
}

func TestStage2FinalizeOutputRowsDecisionHelpers(t *testing.T) {
	e := &Executor{}
	params := Params{}
	aggs := map[string]*fastPeerCandidateAggregate{
		"m1": {edgeCount: 2, avgSum: 8, avgCount: 2, similaritySum: 10},
		"m2": {edgeCount: 1, avgSum: 2, avgCount: 1, similaritySum: 1},
	}
	groupOrder := []string{"m1", "m2"}
	projection := fastPeerCandidateReturnProjection{
		countKey:         "count(r)",
		avgKey:           "avg(r.rating)",
		sumSimilarityKey: "sum(sim)",
	}

	topKCtx := stage2BuildFinalizeOutputRowsDecisionContext(
		aggs,
		groupOrder,
		true,
		projection,
		fastPeerCandidateTopKSpec{descending: true, skip: 0, limit: 1},
		projectionClauseSpec{},
		params,
	)
	if !topKCtx.useTopK {
		t.Fatalf("expected decision context to preserve top-k flag")
	}

	topKRows, err := e.stage2ResolveTopKFinalOutputRows(topKCtx)
	if err != nil {
		t.Fatalf("expected top-k resolver helper to succeed, got error: %v", err)
	}
	if len(topKRows) != 1 {
		t.Fatalf("expected top-k resolver helper to return one row, got %d", len(topKRows))
	}
	if got := ensureRuntimeCounterState(params).counters["fast_path.stage2.topk_pushdown_applied"]; got != 1 {
		t.Fatalf("expected top-k counter 1 from resolver helper, got %d", got)
	}

	nonTopKParams := Params{}
	nonTopKCtx := stage2BuildFinalizeOutputRowsDecisionContext(
		aggs,
		groupOrder,
		false,
		projection,
		fastPeerCandidateTopKSpec{},
		projectionClauseSpec{},
		nonTopKParams,
	)
	nonTopKRows, err := e.stage2ResolveNonTopKFinalOutputRows(nonTopKCtx)
	if err != nil {
		t.Fatalf("expected non-top-k resolver helper to succeed, got error: %v", err)
	}
	if len(nonTopKRows) != 2 {
		t.Fatalf("expected non-top-k resolver helper to return two rows, got %d", len(nonTopKRows))
	}

	resolvedRows, err := e.stage2ResolveFinalOutputRows(nonTopKCtx)
	if err != nil {
		t.Fatalf("expected final output rows resolver helper to succeed, got error: %v", err)
	}
	if len(resolvedRows) != 2 {
		t.Fatalf("expected final output rows resolver helper to return two rows, got %d", len(resolvedRows))
	}
	if got := ensureRuntimeCounterState(nonTopKParams).counters["fast_path.stage2.candidate_groups_total"]; got != 2 {
		t.Fatalf("expected candidate-groups-total counter 2 from resolver helper, got %d", got)
	}
}

func TestStage2BuildFastPeerCandidateResultScope(t *testing.T) {
	agg := &fastPeerCandidateAggregate{sampleScope: Row{"m": "scope"}, sampleCandidate: &graph.Vertex{ID: "m1"}}

	defaultScope := stage2BuildFastPeerCandidateResultScope(agg, fastPeerCandidateReturnProjection{})
	if defaultScope["m"] != "scope" {
		t.Fatalf("expected default result scope to reuse aggregate sample scope")
	}

	lateProjection := fastPeerCandidateReturnProjection{lateMaterializeNonAggregates: true, rightVar: "m"}
	lateScope := stage2BuildFastPeerCandidateResultScope(agg, lateProjection)
	if _, ok := lateScope["m"]; !ok {
		t.Fatalf("expected late-materialized result scope to bind right variable")
	}
	if lateScope["m"].(*graph.Vertex).ID != "m1" {
		t.Fatalf("expected late-materialized right variable binding to use sample candidate")
	}
}

func TestStage2ResolveFastPeerCandidateProjectionKey(t *testing.T) {
	if got := stage2ResolveFastPeerCandidateProjectionKey(projectionSpec{Expression: "m.id"}); got != "m.id" {
		t.Fatalf("expected expression key when alias is empty, got %q", got)
	}
	if got := stage2ResolveFastPeerCandidateProjectionKey(projectionSpec{Expression: "m.id", Alias: "movie_id"}); got != "movie_id" {
		t.Fatalf("expected alias key when alias is present, got %q", got)
	}
}

func TestStage2ResolveFastPeerCandidateAverageValue(t *testing.T) {
	if got := stage2ResolveFastPeerCandidateAverageValue(&fastPeerCandidateAggregate{avgCount: 0, avgSum: 10}); got != nil {
		t.Fatalf("expected nil average value when avgCount is zero")
	}
	if got := stage2ResolveFastPeerCandidateAverageValue(&fastPeerCandidateAggregate{avgCount: 2, avgSum: 9.0}); got.(float64) != 4.5 {
		t.Fatalf("expected computed average value 4.5, got %v", got)
	}
}

func TestStage2FastPeerCandidateResultRowDecisionHelpers(t *testing.T) {
	agg := &fastPeerCandidateAggregate{
		edgeCount:       2,
		avgSum:          8,
		avgCount:        2,
		similaritySum:   6.5,
		sampleScope:     Row{"m": &graph.Vertex{ID: "m1"}},
		sampleCandidate: &graph.Vertex{ID: "m1"},
	}
	projection := fastPeerCandidateReturnProjection{
		nonAggregates:    []projectionSpec{{Expression: "m.id", Alias: "movie_id"}},
		countKey:         "count(r)",
		avgKey:           "avg(r.rating)",
		sumSimilarityKey: "sum(sim)",
	}

	decisionCtx := stage2BuildFastPeerCandidateResultRowDecisionContext(agg, projection, Params{})
	if decisionCtx.result == nil {
		t.Fatalf("expected result row decision context to initialize result map")
	}
	if decisionCtx.scope["m"].(*graph.Vertex).ID != "m1" {
		t.Fatalf("expected result row decision context to bind projection scope")
	}

	key, value, err := stage2ResolveFastPeerCandidateNonAggregateValue(decisionCtx, projection.nonAggregates[0])
	if err != nil {
		t.Fatalf("expected non-aggregate value helper success, got error: %v", err)
	}
	if key != "movie_id" || value.(string) != "m1" {
		t.Fatalf("expected non-aggregate helper output {movie_id:m1}, got {%s:%v}", key, value)
	}

	resolvedCtx, err := stage2ResolveFastPeerCandidateResultRowNonAggregates(decisionCtx)
	if err != nil {
		t.Fatalf("expected non-aggregate row resolver success, got error: %v", err)
	}
	if resolvedCtx.result["movie_id"].(string) != "m1" {
		t.Fatalf("expected non-aggregate row resolver to populate movie_id")
	}

	resolvedCtx = stage2ResolveFastPeerCandidateResultRowAggregates(resolvedCtx)
	if resolvedCtx.result[projection.countKey].(int) != 2 {
		t.Fatalf("expected aggregate resolver to set edge count")
	}
	if resolvedCtx.result[projection.avgKey].(float64) != 4.0 {
		t.Fatalf("expected aggregate resolver to set average rating 4.0")
	}
	if resolvedCtx.result[projection.sumSimilarityKey].(float64) != 6.5 {
		t.Fatalf("expected aggregate resolver to set similarity sum 6.5")
	}

	result, err := stage2ResolveFastPeerCandidateResultRowDecision(decisionCtx)
	if err != nil {
		t.Fatalf("expected result row decision resolver success, got error: %v", err)
	}
	if result["movie_id"].(string) != "m1" {
		t.Fatalf("expected result row decision resolver to preserve non-aggregate value")
	}

	_, err = stage2ResolveFastPeerCandidateResultRowDecision(stage2BuildFastPeerCandidateResultRowDecisionContext(agg, fastPeerCandidateReturnProjection{
		nonAggregates:    []projectionSpec{{Expression: "m."}},
		countKey:         "count(r)",
		avgKey:           "avg(r.rating)",
		sumSimilarityKey: "sum(sim)",
	}, Params{}))
	if err == nil {
		t.Fatalf("expected result row decision resolver to return expression evaluation error")
	}
}

func TestStage2HasSingleTopKOrderBy(t *testing.T) {
	if !stage2HasSingleTopKOrderBy(projectionClauseSpec{OrderBy: []projectionOrderBySpec{{Expression: "sum(sim)"}}}) {
		t.Fatalf("expected single ORDER BY clause to be accepted")
	}
	if stage2HasSingleTopKOrderBy(projectionClauseSpec{OrderBy: []projectionOrderBySpec{}}) {
		t.Fatalf("expected zero ORDER BY clauses to be rejected")
	}
	if stage2HasSingleTopKOrderBy(projectionClauseSpec{OrderBy: []projectionOrderBySpec{{Expression: "a"}, {Expression: "b"}}}) {
		t.Fatalf("expected multiple ORDER BY clauses to be rejected")
	}
}

func TestStage2HasTopKLimitRaw(t *testing.T) {
	if stage2HasTopKLimitRaw(projectionClauseSpec{LimitRaw: ""}) {
		t.Fatalf("expected empty limit raw to be rejected")
	}
	if stage2HasTopKLimitRaw(projectionClauseSpec{LimitRaw: "   "}) {
		t.Fatalf("expected whitespace-only limit raw to be rejected")
	}
	if !stage2HasTopKLimitRaw(projectionClauseSpec{LimitRaw: "10"}) {
		t.Fatalf("expected non-empty limit raw to be accepted")
	}
}

func TestStage2MatchesTopKOrderExpression(t *testing.T) {
	projection := fastPeerCandidateReturnProjection{sumSimilarityKey: "sum(sim)", sumSimilarityExpr: "sum(similarity)"}
	if !stage2MatchesTopKOrderExpression("sum(sim)", projection) {
		t.Fatalf("expected sumSimilarityKey expression match")
	}
	if !stage2MatchesTopKOrderExpression("SUM(SIMILARITY)", projection) {
		t.Fatalf("expected case-insensitive sumSimilarityExpr match")
	}
	if stage2MatchesTopKOrderExpression("count(r)", projection) {
		t.Fatalf("expected non-similarity order expression to be rejected")
	}
}

func TestStage2TopKSpecParsingHelpers(t *testing.T) {
	retSpec := projectionClauseSpec{
		OrderBy:  []projectionOrderBySpec{{Expression: "  sum(sim)  ", Descending: true}},
		SkipRaw:  "2",
		LimitRaw: "5",
	}
	if orderExpr := stage2ResolveTopKOrderExpression(retSpec); orderExpr != "sum(sim)" {
		t.Fatalf("expected trimmed top-k order expression sum(sim), got %q", orderExpr)
	}

	skip, limit, err := stage2ResolveTopKPagination(retSpec, Params{})
	if err != nil {
		t.Fatalf("expected top-k pagination evaluation success, got error: %v", err)
	}
	if skip != 2 || limit != 5 {
		t.Fatalf("expected top-k pagination skip=2 limit=5, got skip=%d limit=%d", skip, limit)
	}

	if normalized := stage2ResolveTopKSpecLimit(0); normalized != 0 {
		t.Fatalf("expected non-positive top-k limit normalization to 0, got %d", normalized)
	}
	if normalized := stage2ResolveTopKSpecLimit(7); normalized != 7 {
		t.Fatalf("expected positive top-k limit normalization to preserve value, got %d", normalized)
	}

	spec := stage2BuildFastPeerCandidateTopKSpec(retSpec, skip, limit)
	if !spec.descending || spec.skip != 2 || spec.limit != 5 {
		t.Fatalf("expected top-k spec {descending:true skip:2 limit:5}, got %+v", spec)
	}

	zeroLimitSpec := stage2BuildFastPeerCandidateTopKSpec(retSpec, 1, 0)
	if zeroLimitSpec.limit != 0 {
		t.Fatalf("expected top-k spec builder to normalize non-positive limit to 0, got %d", zeroLimitSpec.limit)
	}
}

func TestFastPeerCandidateTopKSpecFromProjection(t *testing.T) {
	projection := fastPeerCandidateReturnProjection{sumSimilarityKey: "sum(sim)", sumSimilarityExpr: "sum(similarity)"}

	retSpec := projectionClauseSpec{
		OrderBy:  []projectionOrderBySpec{{Expression: "sum(sim)", Descending: true}},
		SkipRaw:  "1",
		LimitRaw: "3",
	}
	spec, ok, err := fastPeerCandidateTopKSpecFromProjection(retSpec, projection, Params{})
	if err != nil {
		t.Fatalf("expected top-k spec parser success, got error: %v", err)
	}
	if !ok {
		t.Fatalf("expected top-k spec parser to accept compatible projection spec")
	}
	if !spec.descending || spec.skip != 1 || spec.limit != 3 {
		t.Fatalf("expected parsed spec {descending:true skip:1 limit:3}, got %+v", spec)
	}

	retSpec.LimitRaw = "0"
	spec, ok, err = fastPeerCandidateTopKSpecFromProjection(retSpec, projection, Params{})
	if err != nil {
		t.Fatalf("expected top-k spec parser success for zero limit, got error: %v", err)
	}
	if !ok {
		t.Fatalf("expected top-k spec parser to remain accepted for zero limit")
	}
	if spec.limit != 0 {
		t.Fatalf("expected parsed spec limit to normalize to 0, got %d", spec.limit)
	}

	retSpec.OrderBy[0].Expression = "count(r)"
	if _, ok, err := fastPeerCandidateTopKSpecFromProjection(retSpec, projection, Params{}); err != nil || ok {
		t.Fatalf("expected top-k spec parser to reject non-similarity order expression")
	}
}

func TestStage2TopKSpecFromProjectionDecisionHelpers(t *testing.T) {
	projection := fastPeerCandidateReturnProjection{sumSimilarityKey: "sum(sim)", sumSimilarityExpr: "sum(similarity)"}
	retSpec := projectionClauseSpec{
		OrderBy:  []projectionOrderBySpec{{Expression: "sum(sim)", Descending: true}},
		SkipRaw:  "1",
		LimitRaw: "3",
	}

	decisionCtx := stage2BuildTopKSpecFromProjectionDecisionContext(retSpec, projection, Params{})
	if !stage2IsTopKSpecFromProjectionEligible(decisionCtx) {
		t.Fatalf("expected top-k spec decision eligibility helper to accept valid spec")
	}
	if !stage2MatchesTopKSpecFromProjectionOrderExpression(decisionCtx) {
		t.Fatalf("expected top-k spec decision order-expression helper to accept similarity order")
	}

	spec, err := stage2ResolveTopKSpecFromProjectionBuild(decisionCtx)
	if err != nil {
		t.Fatalf("expected top-k spec decision build helper success, got error: %v", err)
	}
	if !spec.descending || spec.skip != 1 || spec.limit != 3 {
		t.Fatalf("expected top-k spec decision build helper to return {descending:true skip:1 limit:3}, got %+v", spec)
	}

	resolved, ok, err := stage2ResolveTopKSpecFromProjectionDecision(decisionCtx)
	if err != nil {
		t.Fatalf("expected top-k spec decision resolver success, got error: %v", err)
	}
	if !ok {
		t.Fatalf("expected top-k spec decision resolver to accept valid spec")
	}
	if resolved != spec {
		t.Fatalf("expected resolver spec to match build helper spec")
	}

	ineligibleCtx := stage2BuildTopKSpecFromProjectionDecisionContext(projectionClauseSpec{OrderBy: []projectionOrderBySpec{{Expression: "sum(sim)"}}, LimitRaw: ""}, projection, Params{})
	if stage2IsTopKSpecFromProjectionEligible(ineligibleCtx) {
		t.Fatalf("expected eligibility helper to reject missing limit")
	}

	mismatchCtx := stage2BuildTopKSpecFromProjectionDecisionContext(projectionClauseSpec{OrderBy: []projectionOrderBySpec{{Expression: "count(r)"}}, LimitRaw: "3"}, projection, Params{})
	if stage2MatchesTopKSpecFromProjectionOrderExpression(mismatchCtx) {
		t.Fatalf("expected order-expression helper to reject non-similarity order")
	}
	if _, ok, err := stage2ResolveTopKSpecFromProjectionDecision(mismatchCtx); err != nil || ok {
		t.Fatalf("expected decision resolver to reject non-similarity order expression")
	}
}

func TestStage2ShouldReturnEmptyTopKRows(t *testing.T) {
	if !stage2ShouldReturnEmptyTopKRows(0) {
		t.Fatalf("expected zero limit to return empty top-k rows")
	}
	if !stage2ShouldReturnEmptyTopKRows(-1) {
		t.Fatalf("expected negative limit to return empty top-k rows")
	}
	if stage2ShouldReturnEmptyTopKRows(1) {
		t.Fatalf("expected positive limit to continue top-k row planning")
	}
}

func TestStage2ResolveTopKKeepSize(t *testing.T) {
	if got := stage2ResolveTopKKeepSize(2, 3); got != 5 {
		t.Fatalf("expected keep size 5, got %d", got)
	}
	if got := stage2ResolveTopKKeepSize(-5, 3); got != 3 {
		t.Fatalf("expected keep size fallback to limit 3, got %d", got)
	}
}

func TestStage2ShouldIncludeAggregateInTopKRows(t *testing.T) {
	if stage2ShouldIncludeAggregateInTopKRows(nil) {
		t.Fatalf("expected nil aggregate to be excluded")
	}
	if stage2ShouldIncludeAggregateInTopKRows(&fastPeerCandidateAggregate{edgeCount: 0}) {
		t.Fatalf("expected zero-edge aggregate to be excluded")
	}
	if !stage2ShouldIncludeAggregateInTopKRows(&fastPeerCandidateAggregate{edgeCount: 1}) {
		t.Fatalf("expected positive-edge aggregate to be included")
	}
}

func TestStage2TopKWindowPredicates(t *testing.T) {
	if !stage2ShouldReturnEmptyTopKRowsAfterSkip(3, 3) {
		t.Fatalf("expected skip at ranked length to return empty window")
	}
	if stage2ShouldReturnEmptyTopKRowsAfterSkip(2, 3) {
		t.Fatalf("expected skip below ranked length to keep window")
	}
	if got := stage2ResolveTopKWindowEnd(1, 2, 10); got != 3 {
		t.Fatalf("expected capped top-k window end 3, got %d", got)
	}
	if got := stage2ResolveTopKWindowEnd(1, 20, 10); got != 10 {
		t.Fatalf("expected ranked-length top-k window end 10, got %d", got)
	}
}

func TestStage2ShouldPushTopKCandidate(t *testing.T) {
	if !stage2ShouldPushTopKCandidate(0, 3) {
		t.Fatalf("expected top-k candidate push while heap has capacity")
	}
	if stage2ShouldPushTopKCandidate(3, 3) {
		t.Fatalf("expected top-k candidate push disabled at heap capacity")
	}
}

func TestStage2ShouldReplaceTopKRoot(t *testing.T) {
	better := fastPeerCandidateRankedRow{score: 10, inputIndex: 1}
	worse := fastPeerCandidateRankedRow{score: 5, inputIndex: 2}
	if !stage2ShouldReplaceTopKRoot(better, worse, true) {
		t.Fatalf("expected better descending candidate to replace root")
	}
	if stage2ShouldReplaceTopKRoot(worse, better, true) {
		t.Fatalf("expected worse descending candidate to avoid replacing root")
	}
}

func TestStage2SortTopKRankedRows(t *testing.T) {
	ranked := []fastPeerCandidateRankedRow{
		{score: 2.0, inputIndex: 1},
		{score: 9.0, inputIndex: 2},
		{score: 9.0, inputIndex: 0},
	}

	stage2SortTopKRankedRows(ranked, true)
	if ranked[0].inputIndex != 0 || ranked[1].inputIndex != 2 || ranked[2].inputIndex != 1 {
		t.Fatalf("expected descending sort by score with input-index tie-break")
	}

	stage2SortTopKRankedRows(ranked, false)
	if ranked[0].inputIndex != 1 || ranked[2].inputIndex != 2 {
		t.Fatalf("expected ascending sort by score")
	}
}

func TestStage2ResolveTopKRankedWindow(t *testing.T) {
	ranked := []fastPeerCandidateRankedRow{
		{inputIndex: 0},
		{inputIndex: 1},
		{inputIndex: 2},
	}

	window := stage2ResolveTopKRankedWindow(ranked, 1, 1)
	if len(window) != 1 || window[0].inputIndex != 1 {
		t.Fatalf("expected skip/limit window to return middle ranked candidate")
	}

	empty := stage2ResolveTopKRankedWindow(ranked, 3, 1)
	if len(empty) != 0 {
		t.Fatalf("expected window to be empty when skip reaches ranked length")
	}
}

func TestStage2BuildTopKOutputRows(t *testing.T) {
	projection := fastPeerCandidateReturnProjection{
		countKey:         "count(r)",
		avgKey:           "avg(r.rating)",
		sumSimilarityKey: "sum(sim)",
	}
	window := []fastPeerCandidateRankedRow{
		{agg: &fastPeerCandidateAggregate{edgeCount: 2, avgSum: 8, avgCount: 2, similaritySum: 5}},
		{agg: &fastPeerCandidateAggregate{edgeCount: 1, avgSum: 2, avgCount: 1, similaritySum: 1}},
	}

	out, err := stage2BuildTopKOutputRows(window, projection, Params{})
	if err != nil {
		t.Fatalf("expected top-k output row builder to succeed, got %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected two output rows, got %d", len(out))
	}
	if out[0][projection.countKey].(int) != 2 || out[1][projection.countKey].(int) != 1 {
		t.Fatalf("expected output rows to preserve ranked window order")
	}
}

func TestStage2TopKRowsDecisionHelpers(t *testing.T) {
	projection := fastPeerCandidateReturnProjection{
		countKey:         "count(r)",
		avgKey:           "avg(r.rating)",
		sumSimilarityKey: "sum(sim)",
	}
	aggs := map[string]*fastPeerCandidateAggregate{
		"m1": {edgeCount: 2, avgSum: 8, avgCount: 2, similaritySum: 10},
		"m2": {edgeCount: 1, avgSum: 2, avgCount: 1, similaritySum: 1},
		"m3": {edgeCount: 0, avgSum: 5, avgCount: 1, similaritySum: 20},
	}
	groupOrder := []string{"m1", "m2", "m3"}
	spec := fastPeerCandidateTopKSpec{descending: true, skip: 0, limit: 1}

	decisionCtx := stage2BuildTopKRowsDecisionContext(aggs, groupOrder, projection, spec, Params{})
	if decisionCtx.keep != 1 {
		t.Fatalf("expected top-k decision keep size 1, got %d", decisionCtx.keep)
	}
	if decisionCtx.top == nil {
		t.Fatalf("expected top-k decision context to initialize heap")
	}

	top := stage2AccumulateTopKCandidatesForDecision(decisionCtx)
	if top.Len() != 1 {
		t.Fatalf("expected top-k accumulation to retain one candidate, got %d", top.Len())
	}
	if top.rows[0].agg != aggs["m1"] {
		t.Fatalf("expected top-k accumulation to keep highest-scoring eligible aggregate")
	}

	rows, err := stage2ResolveTopKRowsDecision(decisionCtx)
	if err != nil {
		t.Fatalf("expected top-k decision resolver to succeed, got %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected top-k decision resolver to return one row, got %d", len(rows))
	}
	if got := rows[0][projection.countKey].(int); got != 2 {
		t.Fatalf("expected resolved top-k row to come from m1 aggregate, got count %d", got)
	}

	emptyRows, err := stage2ResolveTopKRowsDecision(stage2BuildTopKRowsDecisionContext(aggs, groupOrder, projection, fastPeerCandidateTopKSpec{descending: true, skip: 0, limit: 0}, Params{}))
	if err != nil {
		t.Fatalf("expected zero-limit top-k decision to succeed, got %v", err)
	}
	if len(emptyRows) != 0 {
		t.Fatalf("expected zero-limit top-k decision to return no rows")
	}

	heapState := &fastPeerCandidateTopKHeap{descending: true, rows: make([]fastPeerCandidateRankedRow, 0, 1)}
	stage2MaybePushOrReplaceTopKCandidateForDecision(heapState, stage2BuildTopKCandidateForDecision(aggs["m2"], 1), 1, true)
	if heapState.Len() != 1 {
		t.Fatalf("expected helper to push first top-k candidate")
	}
	stage2MaybePushOrReplaceTopKCandidateForDecision(heapState, stage2BuildTopKCandidateForDecision(aggs["m1"], 0), 1, true)
	if heapState.rows[0].agg != aggs["m1"] {
		t.Fatalf("expected helper to replace root with better candidate")
	}
}

func TestStage2CompareTopKScore(t *testing.T) {
	if got := stage2CompareTopKScore(9, 7, true); got != -1 {
		t.Fatalf("expected descending compare to prefer higher score, got %d", got)
	}
	if got := stage2CompareTopKScore(7, 9, true); got != 1 {
		t.Fatalf("expected descending compare to rank lower score later, got %d", got)
	}
	if got := stage2CompareTopKScore(7, 9, false); got != -1 {
		t.Fatalf("expected ascending compare to prefer lower score, got %d", got)
	}
	if got := stage2CompareTopKScore(8, 8, false); got != 0 {
		t.Fatalf("expected equal scores to tie, got %d", got)
	}
}

func TestStage2CompareTopKInputIndex(t *testing.T) {
	if got := stage2CompareTopKInputIndex(1, 2); got != -1 {
		t.Fatalf("expected lower input index to rank first, got %d", got)
	}
	if got := stage2CompareTopKInputIndex(3, 2); got != 1 {
		t.Fatalf("expected higher input index to rank later, got %d", got)
	}
	if got := stage2CompareTopKInputIndex(4, 4); got != 0 {
		t.Fatalf("expected equal input index to tie, got %d", got)
	}
}

func TestStage2FrontierBoundaryAggregateAndHeapPredicates(t *testing.T) {
	if !stage2ShouldSkipFrontierBoundaryAggregate(nil) {
		t.Fatalf("expected nil aggregate to be skipped in frontier boundary")
	}
	if !stage2ShouldSkipFrontierBoundaryAggregate(&fastPeerCandidateAggregate{edgeCount: 0}) {
		t.Fatalf("expected zero-edge aggregate to be skipped in frontier boundary")
	}
	if stage2ShouldSkipFrontierBoundaryAggregate(&fastPeerCandidateAggregate{edgeCount: 1}) {
		t.Fatalf("expected positive-edge aggregate to be included in frontier boundary")
	}

	if !stage2ShouldPushFrontierBoundaryCandidate(0, 2) {
		t.Fatalf("expected push while heap has capacity")
	}
	if stage2ShouldPushFrontierBoundaryCandidate(2, 2) {
		t.Fatalf("expected push disabled at heap capacity")
	}

	if !stage2IsFrontierBoundaryReady(2, 2) {
		t.Fatalf("expected frontier boundary readiness when heap is full")
	}
	if stage2IsFrontierBoundaryReady(1, 2) {
		t.Fatalf("expected frontier boundary non-readiness when heap is underfilled")
	}

	better := fastPeerCandidateRankedRow{score: 10, inputIndex: 1}
	worse := fastPeerCandidateRankedRow{score: 5, inputIndex: 2}
	if !stage2ShouldReplaceFrontierBoundaryRoot(better, worse, true) {
		t.Fatalf("expected better descending candidate to replace frontier root")
	}
	if stage2ShouldReplaceFrontierBoundaryRoot(worse, better, true) {
		t.Fatalf("expected worse descending candidate to avoid replacing frontier root")
	}
}

func TestStage2BuildFrontierBoundaryIndexesAndGroups(t *testing.T) {
	ranked := []fastPeerCandidateRankedRow{
		{inputIndex: 2},
		{inputIndex: 0},
		{inputIndex: -1},
		{inputIndex: 7},
	}
	groupOrder := []string{"m0", "m1", "m2"}
	indexes, groups := stage2BuildFrontierBoundaryIndexesAndGroups(ranked, groupOrder)

	if _, ok := indexes[2]; !ok {
		t.Fatalf("expected frontier indexes to include inputIndex 2")
	}
	if _, ok := indexes[0]; !ok {
		t.Fatalf("expected frontier indexes to include inputIndex 0")
	}
	if _, ok := indexes[-1]; !ok {
		t.Fatalf("expected frontier indexes to include raw ranked index values")
	}

	if _, ok := groups["m2"]; !ok {
		t.Fatalf("expected frontier groups to include valid groupOrder member m2")
	}
	if _, ok := groups["m0"]; !ok {
		t.Fatalf("expected frontier groups to include valid groupOrder member m0")
	}
	if _, ok := groups["m1"]; ok {
		t.Fatalf("expected non-frontier group m1 to be absent")
	}
}

func TestStage2ResolveFrontierBoundaryMaxNonFrontierScore(t *testing.T) {
	aggs := map[string]*fastPeerCandidateAggregate{
		"m0": {edgeCount: 1, similaritySum: 10},
		"m1": {edgeCount: 2, similaritySum: 7},
		"m2": {edgeCount: 1, similaritySum: 3},
		"m3": nil,
	}
	groupOrder := []string{"m0", "m1", "m2", "m3"}

	frontier := map[int]struct{}{0: {}, 2: {}}
	if got := stage2ResolveFrontierBoundaryMaxNonFrontierScore(aggs, groupOrder, frontier); got != 7 {
		t.Fatalf("expected max non-frontier score 7, got %v", got)
	}

	allFrontier := map[int]struct{}{0: {}, 1: {}, 2: {}, 3: {}}
	if got := stage2ResolveFrontierBoundaryMaxNonFrontierScore(aggs, groupOrder, allFrontier); !math.IsInf(got, -1) {
		t.Fatalf("expected no non-frontier candidates to keep -Inf score, got %v", got)
	}
}

func TestStage2BuildFrontierBoundaryRankedCandidate(t *testing.T) {
	agg := &fastPeerCandidateAggregate{similaritySum: 4.5}
	candidate := stage2BuildFrontierBoundaryRankedCandidate(agg, 3)
	if candidate.agg != agg {
		t.Fatalf("expected frontier candidate to keep aggregate pointer")
	}
	if candidate.score != 4.5 || candidate.inputIndex != 3 {
		t.Fatalf("expected frontier candidate score/index to be preserved")
	}
}

func TestStage2IsFastTargetSharedPeerTopKWithSpecEligible(t *testing.T) {
	if !stage2IsFastTargetSharedPeerTopKWithSpecEligible(projectionClauseSpec{OrderBy: []projectionOrderBySpec{{Expression: "sim"}}, LimitRaw: "10"}) {
		t.Fatalf("expected simple with-spec shape to be eligible")
	}
	if stage2IsFastTargetSharedPeerTopKWithSpecEligible(projectionClauseSpec{Distinct: true, OrderBy: []projectionOrderBySpec{{Expression: "sim"}}, LimitRaw: "10"}) {
		t.Fatalf("expected DISTINCT with-spec to be rejected")
	}
	if stage2IsFastTargetSharedPeerTopKWithSpecEligible(projectionClauseSpec{OrderBy: []projectionOrderBySpec{{Expression: "sim"}}, LimitRaw: "", WhereRaw: "x > 1"}) {
		t.Fatalf("expected WHERE/empty-limit with-spec to be rejected")
	}
}

func TestStage1FastTargetSharedPeerTopKResultAndColumnsHelpers(t *testing.T) {
	if !stage1ShouldReturnEmptyFastTargetSharedPeerTopKResult(projectionClauseSpec{}, map[string]*fastTargetSharedPeerAggregate{}) {
		t.Fatalf("expected empty-result helper to return true with no WHERE and no aggregates")
	}
	if stage1ShouldReturnEmptyFastTargetSharedPeerTopKResult(projectionClauseSpec{WhereRaw: "sim > 0"}, map[string]*fastTargetSharedPeerAggregate{}) {
		t.Fatalf("expected WHERE presence to disable empty-result helper")
	}
	if stage1ShouldReturnEmptyFastTargetSharedPeerTopKResult(projectionClauseSpec{}, map[string]*fastTargetSharedPeerAggregate{"m|u": {}}) {
		t.Fatalf("expected non-empty aggregates to disable empty-result helper")
	}

	projection := fastTargetSharedPeerProjection{targetKey: "target", peerKey: "peer", sharedCountKey: "shared", avgDiffKey: "avg"}
	aggColumns := stage1BuildFastTargetSharedPeerAggregateColumns(projection)
	if len(aggColumns) != 4 || aggColumns[0] != "target" || aggColumns[1] != "peer" || aggColumns[2] != "shared" || aggColumns[3] != "avg" {
		t.Fatalf("expected aggregate columns [target peer shared avg], got %v", aggColumns)
	}

	topKProjection := fastTargetSharedPeerTopKProjection{targetKey: "t", peerKey: "p", similarityKey: "sim"}
	topKColumns := stage1BuildFastTargetSharedPeerTopKColumns(topKProjection, []string{"fallback"})
	if len(topKColumns) != 3 || topKColumns[0] != "t" || topKColumns[1] != "p" || topKColumns[2] != "sim" {
		t.Fatalf("expected top-k columns [t p sim], got %v", topKColumns)
	}

	emptyTopKColumns := stage1BuildFastTargetSharedPeerTopKColumns(fastTargetSharedPeerTopKProjection{}, []string{"fallback"})
	if len(emptyTopKColumns) != 3 || emptyTopKColumns[0] != "" || emptyTopKColumns[1] != "" || emptyTopKColumns[2] != "" {
		t.Fatalf("expected zero-value projection to preserve explicit three-column shape, got %v", emptyTopKColumns)
	}
}

func TestStage1FastTargetSharedPeerAggregateCollectionPreflightHelpers(t *testing.T) {
	if !stage1CanCollectFastTargetSharedPeerAggregates([]Row{{}}, struct{ graph.Tx }{}) {
		t.Fatalf("expected preflight helper to accept one empty seed row with non-nil tx")
	}
	if stage1CanCollectFastTargetSharedPeerAggregates([]Row{}, struct{ graph.Tx }{}) {
		t.Fatalf("expected preflight helper to reject missing seed row")
	}
	if stage1CanCollectFastTargetSharedPeerAggregates([]Row{{"x": 1}}, struct{ graph.Tx }{}) {
		t.Fatalf("expected preflight helper to reject non-empty seed row")
	}
	if stage1CanCollectFastTargetSharedPeerAggregates([]Row{{}}, nil) {
		t.Fatalf("expected preflight helper to reject nil tx")
	}

	if !stage1IsFastTargetSharedPeerWithSpecEligible(projectionClauseSpec{}) {
		t.Fatalf("expected base with-spec helper eligibility to allow non-distinct, no order, no skip/limit")
	}
	if stage1IsFastTargetSharedPeerWithSpecEligible(projectionClauseSpec{Distinct: true}) {
		t.Fatalf("expected DISTINCT with-spec to be ineligible")
	}
	if stage1IsFastTargetSharedPeerWithSpecEligible(projectionClauseSpec{OrderBy: []projectionOrderBySpec{{Expression: "x"}}}) {
		t.Fatalf("expected ORDER BY with-spec to be ineligible")
	}
	if stage1IsFastTargetSharedPeerWithSpecEligible(projectionClauseSpec{SkipRaw: "1"}) {
		t.Fatalf("expected SKIP with-spec to be ineligible")
	}
	if stage1IsFastTargetSharedPeerWithSpecEligible(projectionClauseSpec{LimitRaw: "1"}) {
		t.Fatalf("expected LIMIT with-spec to be ineligible")
	}

	if _, _, ok := stage1ResolveFastTargetSharedPeerMatchAndChain(ast.Clause{}); ok {
		t.Fatalf("expected match+chain resolver to reject empty clause")
	}
	if _, _, ok := stage1ResolveFastTargetSharedPeerWithProjection(ast.Clause{}, twoHopDirectedChainPattern{}); ok {
		t.Fatalf("expected with+projection resolver to reject empty clause")
	}
}

func TestStage1SharedPeerWhereAndScanTypeHelpers(t *testing.T) {
	if stage1HasMatchWhereClause("   ") {
		t.Fatalf("expected whitespace-only match WHERE to be treated as absent")
	}
	if !stage1HasMatchWhereClause("r.rating > 3") {
		t.Fatalf("expected non-empty match WHERE to be detected")
	}

	chain := twoHopDirectedChainPattern{FirstEdgeType: "RATED", SecondEdgeType: "SIMILAR"}
	if got := stage1ResolveFirstHopScanType(chain); got != "RATED" {
		t.Fatalf("expected first-hop scan type RATED, got %q", got)
	}
	if got := stage1ResolveSecondHopScanType(chain); got != "SIMILAR" {
		t.Fatalf("expected second-hop scan type SIMILAR, got %q", got)
	}

	chain.FirstEdgeAnyOf = []string{"RATED", "LIKED"}
	chain.SecondEdgeAnyOf = []string{"SIMILAR", "NEAR"}
	if got := stage1ResolveFirstHopScanType(chain); got != "" {
		t.Fatalf("expected first-hop scan type to be empty for ANY-OF edges, got %q", got)
	}
	if got := stage1ResolveSecondHopScanType(chain); got != "" {
		t.Fatalf("expected second-hop scan type to be empty for ANY-OF edges, got %q", got)
	}

	e := &Executor{}
	params := Params{}
	keep, err := e.stage1EvaluateSharedPeerMatchWhere(context.Background(), nil, params, "", stage1WhereShortcutPlan{}, nil, nil, false, Row{})
	if err != nil || !keep {
		t.Fatalf("expected empty-match-WHERE helper path to keep row without errors")
	}

	target := &graph.Vertex{ID: "v1"}
	peer := &graph.Vertex{ID: "v1"}
	keep, err = e.stage1EvaluateSharedPeerMatchWhere(context.Background(), nil, params, "x", stage1WhereShortcutPlan{enabled: true, requirePeerNotTarget: true}, target, peer, false, Row{})
	if err != nil {
		t.Fatalf("expected shortcut-drop helper path to avoid errors, got %v", err)
	}
	if keep {
		t.Fatalf("expected shortcut-drop helper path to drop row")
	}
	if got := ensureRuntimeCounterState(params).counters["fast_path.stage1.where_eval_drops"]; got <= 0 {
		t.Fatalf("expected where_eval_drops counter to increment on shortcut-drop path")
	}

	params = Params{}
	keep, err = e.stage1EvaluateSharedPeerMatchWhere(context.Background(), nil, params, "x", stage1WhereShortcutPlan{enabled: true, requireSecondEdgeCover: true}, &graph.Vertex{ID: "v1"}, &graph.Vertex{ID: "v2"}, true, Row{})
	if err != nil {
		t.Fatalf("expected shortcut-bypass helper path to avoid errors, got %v", err)
	}
	if !keep {
		t.Fatalf("expected shortcut-bypass helper path to keep row")
	}
	if got := ensureRuntimeCounterState(params).counters["fast_path.stage1.where_eval_shortcuts"]; got <= 0 {
		t.Fatalf("expected where_eval_shortcuts counter to increment on shortcut-bypass path")
	}
}

func TestStage1FastTargetSharedPeerWithPostCollectionHelpers(t *testing.T) {
	if stage1ShouldIncludeFastTargetSharedPeerWithRow(nil) {
		t.Fatalf("expected nil aggregate to be excluded from with-row materialization")
	}
	if stage1ShouldIncludeFastTargetSharedPeerWithRow(&fastTargetSharedPeerAggregate{shared: 0}) {
		t.Fatalf("expected zero-shared aggregate to be excluded from with-row materialization")
	}
	if !stage1ShouldIncludeFastTargetSharedPeerWithRow(&fastTargetSharedPeerAggregate{shared: 1}) {
		t.Fatalf("expected positive-shared aggregate to be included from with-row materialization")
	}

	agg := &fastTargetSharedPeerAggregate{target: &graph.Vertex{ID: "m1"}, peer: &graph.Vertex{ID: "u1"}, shared: 2, sumAbsDiff: 3}
	projection := fastTargetSharedPeerProjection{targetKey: "target", peerKey: "peer", sharedCountKey: "shared", avgDiffKey: "avg"}
	row := stage1BuildFastTargetSharedPeerWithRow(agg, projection)
	if row["target"].(*graph.Vertex).ID != "m1" || row["peer"].(*graph.Vertex).ID != "u1" {
		t.Fatalf("expected with-row builder to include target/peer bindings")
	}
	if row["shared"].(int) != 2 || row["avg"].(float64) != 1.5 {
		t.Fatalf("expected with-row builder to include shared count 2 and avg 1.5")
	}

	e := &Executor{}
	params := Params{}
	rows := []Row{{"target": "m1", "peer": "u1"}}
	filtered, err := e.stage1ApplyFastTargetSharedPeerWithFilter(context.Background(), nil, rows, projectionClauseSpec{}, params)
	if err != nil {
		t.Fatalf("expected empty with-filter helper path to succeed, got error: %v", err)
	}
	if len(filtered) != 1 || filtered[0]["target"] != "m1" {
		t.Fatalf("expected empty with-filter helper path to preserve rows")
	}

	columns := stage1BuildFastTargetSharedPeerWithColumns(projection, []string{"fallback"})
	if len(columns) != 4 || columns[0] != "target" || columns[1] != "peer" || columns[2] != "shared" || columns[3] != "avg" {
		t.Fatalf("expected with-columns [target peer shared avg], got %v", columns)
	}
	zeroColumns := stage1BuildFastTargetSharedPeerWithColumns(fastTargetSharedPeerProjection{}, []string{"fallback"})
	if len(zeroColumns) != 4 {
		t.Fatalf("expected zero-value projection to preserve explicit four-column shape, got %v", zeroColumns)
	}
}

func TestStage1FastTwoHopDistinctWritePreflightHelpers(t *testing.T) {
	if !stage1CanTryFastTwoHopDistinctWrite([]Row{{}}, struct{ graph.Tx }{}) {
		t.Fatalf("expected two-hop distinct write preflight to accept one empty seed row with non-nil tx")
	}
	if stage1CanTryFastTwoHopDistinctWrite([]Row{}, struct{ graph.Tx }{}) {
		t.Fatalf("expected two-hop distinct write preflight to reject missing seed row")
	}
	if stage1CanTryFastTwoHopDistinctWrite([]Row{{"x": 1}}, struct{ graph.Tx }{}) {
		t.Fatalf("expected two-hop distinct write preflight to reject non-empty seed row")
	}
	if stage1CanTryFastTwoHopDistinctWrite([]Row{{}}, nil) {
		t.Fatalf("expected two-hop distinct write preflight to reject nil tx")
	}

	eligibleChain := twoHopDirectedChainPattern{
		Left:          vertexPattern{Var: "u"},
		Mid:           vertexPattern{Var: "x"},
		Right:         vertexPattern{Var: "v"},
		FirstEdgeType: "RATED",
	}
	if !stage1IsFastTwoHopDistinctWriteChainEligible(eligibleChain) {
		t.Fatalf("expected eligible two-hop chain to pass chain-eligibility helper")
	}
	ineligibleChain := eligibleChain
	ineligibleChain.FirstEdgeType = ""
	if stage1IsFastTwoHopDistinctWriteChainEligible(ineligibleChain) {
		t.Fatalf("expected missing first-edge type to fail chain-eligibility helper")
	}

	if !stage1IsFastTwoHopDistinctWriteWithSpecEligible(projectionClauseSpec{Distinct: true}) {
		t.Fatalf("expected DISTINCT-only with-spec to be eligible for two-hop distinct write")
	}
	if stage1IsFastTwoHopDistinctWriteWithSpecEligible(projectionClauseSpec{Distinct: false}) {
		t.Fatalf("expected non-DISTINCT with-spec to be ineligible")
	}
	if stage1IsFastTwoHopDistinctWriteWithSpecEligible(projectionClauseSpec{Distinct: true, WhereRaw: "x > 1"}) {
		t.Fatalf("expected with-spec WHERE to be ineligible")
	}

	writeClause := ast.Clause{MergePattern: "(a)-[:RATED]->(b)", Raw: "MERGE (x)-[:LIKED]->(y)", Kind: ast.ClauseKindMerge}
	if raw := stage1ResolveFastTwoHopDistinctWritePatternRaw(writeClause); raw == "" {
		t.Fatalf("expected write-pattern raw resolver to produce non-empty merge pattern")
	}
	writeClause.MergePattern = ""
	if raw := stage1ResolveFastTwoHopDistinctWritePatternRaw(writeClause); raw == "" {
		t.Fatalf("expected write-pattern raw resolver to fallback to clause raw")
	}

	if !stage1CanUseFastTwoHopDistinctMergeSemantics(ast.Clause{}, false) {
		t.Fatalf("expected non-merge semantics to bypass merge-clause on-create/on-match checks")
	}
	if !stage1CanUseFastTwoHopDistinctMergeSemantics(ast.Clause{}, true) {
		t.Fatalf("expected merge semantics to allow empty on-create/on-match sections")
	}
	if stage1CanUseFastTwoHopDistinctMergeSemantics(ast.Clause{MergeOnCreate: "SET r.createdAt = 1"}, true) {
		t.Fatalf("expected merge semantics helper to reject MERGE ON CREATE in fast path")
	}

	items := []projectionSpec{{Expression: "u", Alias: "left"}, {Expression: "v", Alias: "right"}}
	if !stage1HasFastTwoHopDistinctWriteProjectionBindings(items, eligibleChain, "left", "right") {
		t.Fatalf("expected projection-binding helper to accept exact left/right var bindings")
	}
	if stage1HasFastTwoHopDistinctWriteProjectionBindings(items, eligibleChain, "left", "other") {
		t.Fatalf("expected projection-binding helper to reject mismatched right binding")
	}

	if _, _, ok := stage1ResolveFastTwoHopDistinctWriteMatchAndChain(ast.Clause{}); ok {
		t.Fatalf("expected match+chain resolver to reject empty clause")
	}
	if _, _, ok := stage1ResolveFastTwoHopDistinctWriteWithItems(ast.Clause{}); ok {
		t.Fatalf("expected with+items resolver to reject empty clause")
	}
}

func TestStage1TwoHopDistinctWriteAntiJoinPolicyHelpers(t *testing.T) {
	shortcut := twoHopAntiJoinShortcutPlan{requireNoDirectEdge: true, directEdgeType: "RATED"}

	if directType, ok := stage1ResolveAntiJoinZeroTypeShortcutEligibility(true, false, shortcut); !ok || directType != "RATED" {
		t.Fatalf("expected zero-type shortcut eligibility helper to resolve direct type RATED")
	}
	if _, ok := stage1ResolveAntiJoinZeroTypeShortcutEligibility(false, false, shortcut); ok {
		t.Fatalf("expected zero-type shortcut eligibility helper to reject when no-direct checks are disabled")
	}
	if _, ok := stage1ResolveAntiJoinZeroTypeShortcutEligibility(true, true, shortcut); ok {
		t.Fatalf("expected zero-type shortcut eligibility helper to reject in merge semantics mode")
	}
	if _, ok := stage1ResolveAntiJoinZeroTypeShortcutEligibility(true, false, twoHopAntiJoinShortcutPlan{requireNoDirectEdge: true, directEdgeType: "", directEdgeAnyOf: []string{"RATED"}}); ok {
		t.Fatalf("expected zero-type shortcut eligibility helper to reject any-of type signatures")
	}

	chain := twoHopDirectedChainPattern{FirstEdgeType: "RATED"}
	probeEdgeType, canUseTypedProbe, canPrebuild, canReusePrefetchedFirstHop, usePrefetchedSets := stage1ResolveAntiJoinEndpointPrefetchPolicy(true, shortcut, chain)
	if !canUseTypedProbe || probeEdgeType != "RATED" {
		t.Fatalf("expected endpoint-prefetch policy helper to enable typed probe for single direct type")
	}
	if canPrebuild {
		t.Fatalf("expected typed probe policy to skip neighbor prebuild path")
	}
	if !canReusePrefetchedFirstHop || !usePrefetchedSets {
		t.Fatalf("expected matching first-edge/direct-edge type to enable prefetched first-hop reuse")
	}

	anyOfShortcut := twoHopAntiJoinShortcutPlan{requireNoDirectEdge: true, directEdgeType: "", directEdgeAnyOf: []string{"RATED", "LIKED"}}
	_, canUseTypedProbe, canPrebuild, canReusePrefetchedFirstHop, usePrefetchedSets = stage1ResolveAntiJoinEndpointPrefetchPolicy(true, anyOfShortcut, chain)
	if canUseTypedProbe {
		t.Fatalf("expected any-of direct-edge type to disable typed endpoint probe")
	}
	if !canPrebuild {
		t.Fatalf("expected any-of direct-edge type to enable neighbor prebuild path")
	}
	if canReusePrefetchedFirstHop || usePrefetchedSets {
		t.Fatalf("expected non-matching any-of direct-edge type to disable prefetched first-hop reuse")
	}

	leftCandidateSet := stage1CollectLeftCandidateSet([]string{"u1", "  ", "u2", "u1"})
	if len(leftCandidateSet) != 2 {
		t.Fatalf("expected left-candidate-set helper to collect two unique non-empty IDs, got %d", len(leftCandidateSet))
	}
	if _, ok := leftCandidateSet["u1"]; !ok {
		t.Fatalf("expected left-candidate-set helper to include u1")
	}
	if _, ok := leftCandidateSet["u2"]; !ok {
		t.Fatalf("expected left-candidate-set helper to include u2")
	}
}

func TestStage2IsFastTargetSharedPeerTopKProjectionItemEligible(t *testing.T) {
	if !stage2IsFastTargetSharedPeerTopKProjectionItemEligible(projectionSpec{Expression: "u"}) {
		t.Fatalf("expected plain projection item to be eligible")
	}
	if stage2IsFastTargetSharedPeerTopKProjectionItemEligible(projectionSpec{Expression: "*"}) {
		t.Fatalf("expected wildcard projection item to be rejected")
	}
	if stage2IsFastTargetSharedPeerTopKProjectionItemEligible(projectionSpec{Expression: "u", AggFunc: "sum"}) {
		t.Fatalf("expected aggregate projection item to be rejected")
	}
}

func TestStage2FastTargetSharedPeerTopKProjectionPredicates(t *testing.T) {
	projection := fastTargetSharedPeerTopKProjection{targetKey: "u", peerKey: "p", similarityKey: "sim", similarityExpr: "u.score"}
	if !stage2HasFastTargetSharedPeerTopKProjectionBindings(projection) {
		t.Fatalf("expected fully bound top-k projection to be accepted")
	}
	if stage2HasFastTargetSharedPeerTopKProjectionBindings(fastTargetSharedPeerTopKProjection{targetKey: "u"}) {
		t.Fatalf("expected incomplete top-k projection bindings to be rejected")
	}
	if !stage2MatchesFastTargetSharedPeerTopKOrderExpression("SIM", projection) {
		t.Fatalf("expected case-insensitive order-expression key match")
	}
	if !stage2MatchesFastTargetSharedPeerTopKOrderExpression("u.score", projection) {
		t.Fatalf("expected similarity expression match")
	}
	if stage2MatchesFastTargetSharedPeerTopKOrderExpression("count(*)", projection) {
		t.Fatalf("expected non-similarity order expression to be rejected")
	}
}

func TestStage2ResolveFastTargetSharedPeerTopKProjectionItemKey(t *testing.T) {
	if got := stage2ResolveFastTargetSharedPeerTopKProjectionItemKey(projectionSpec{Expression: "u"}); got != "u" {
		t.Fatalf("expected expression key when alias is empty, got %q", got)
	}
	if got := stage2ResolveFastTargetSharedPeerTopKProjectionItemKey(projectionSpec{Expression: "u", Alias: "target"}); got != "target" {
		t.Fatalf("expected alias key when alias is present, got %q", got)
	}
}

func TestStage2ResolveFastTargetSharedPeerTopKProjectionItemExpression(t *testing.T) {
	if got := stage2ResolveFastTargetSharedPeerTopKProjectionItemExpression(projectionSpec{Expression: "  sum(sim)  "}); got != "sum(sim)" {
		t.Fatalf("expected projection item expression to be trimmed, got %q", got)
	}
}

func TestStage2FastTargetSharedPeerTopKParsingHelpers(t *testing.T) {
	if !stage2HasFastTargetSharedPeerTopKProjectionItems([]projectionSpec{{Expression: "a"}, {Expression: "b"}, {Expression: "c"}}) {
		t.Fatalf("expected exactly three projection items to be accepted")
	}
	if stage2HasFastTargetSharedPeerTopKProjectionItems([]projectionSpec{{Expression: "a"}, {Expression: "b"}}) {
		t.Fatalf("expected non-three projection item count to be rejected")
	}

	withSpec := projectionClauseSpec{
		OrderBy:  []projectionOrderBySpec{{Expression: "  sim  ", Descending: true}},
		SkipRaw:  "2",
		LimitRaw: "5",
	}
	if orderExpr := stage2ResolveFastTargetSharedPeerTopKOrderExpression(withSpec); orderExpr != "sim" {
		t.Fatalf("expected trimmed shared-peer top-k order expression sim, got %q", orderExpr)
	}

	skip, limit, err := stage2ResolveFastTargetSharedPeerTopKPagination(withSpec, Params{})
	if err != nil {
		t.Fatalf("expected shared-peer top-k pagination evaluation success, got error: %v", err)
	}
	if skip != 2 || limit != 5 {
		t.Fatalf("expected shared-peer top-k pagination skip=2 limit=5, got skip=%d limit=%d", skip, limit)
	}

	spec := stage2BuildFastTargetSharedPeerTopKSpec(withSpec, skip, limit)
	if !spec.descending || spec.skip != 2 || spec.limit != 5 {
		t.Fatalf("expected shared-peer top-k spec {descending:true skip:2 limit:5}, got %+v", spec)
	}
}

func TestStage2ApplyFastTargetSharedPeerTopKProjectionItemBinding(t *testing.T) {
	prior := fastTargetSharedPeerProjection{targetKey: "target", targetExpr: "m", peerKey: "peer", peerExpr: "u"}

	projection := fastTargetSharedPeerTopKProjection{}
	itemTarget := projectionSpec{Expression: "m", Alias: "t"}
	updated, applied := stage2ApplyFastTargetSharedPeerTopKProjectionItemBinding(projection, prior, itemTarget)
	if !applied {
		t.Fatalf("expected target projection item binding to apply")
	}
	if updated.targetExpr != "m" || updated.targetKey != "t" {
		t.Fatalf("expected target binding to set expr/key, got expr=%q key=%q", updated.targetExpr, updated.targetKey)
	}

	itemPeer := projectionSpec{Expression: "u", Alias: "p"}
	updated, applied = stage2ApplyFastTargetSharedPeerTopKProjectionItemBinding(updated, prior, itemPeer)
	if !applied {
		t.Fatalf("expected peer projection item binding to apply")
	}
	if updated.peerExpr != "u" || updated.peerKey != "p" {
		t.Fatalf("expected peer binding to set expr/key, got expr=%q key=%q", updated.peerExpr, updated.peerKey)
	}

	itemSimilarity := projectionSpec{Expression: " sim ", Alias: "score"}
	updated, applied = stage2ApplyFastTargetSharedPeerTopKProjectionItemBinding(updated, prior, itemSimilarity)
	if !applied {
		t.Fatalf("expected first similarity projection item binding to apply")
	}
	if updated.similarityExpr != "sim" || updated.similarityKey != "score" {
		t.Fatalf("expected similarity binding to set trimmed expr/key, got expr=%q key=%q", updated.similarityExpr, updated.similarityKey)
	}

	duplicateSimilarity := projectionSpec{Expression: "sim2", Alias: "score2"}
	_, applied = stage2ApplyFastTargetSharedPeerTopKProjectionItemBinding(updated, prior, duplicateSimilarity)
	if applied {
		t.Fatalf("expected duplicate similarity projection item binding to be rejected")
	}
}

func TestStage2FastTargetSharedPeerTopKProjectionItemBindingDecisionHelpers(t *testing.T) {
	prior := fastTargetSharedPeerProjection{targetKey: "target", targetExpr: "m", peerKey: "peer", peerExpr: "u"}

	targetCtx := stage2BuildFastTargetSharedPeerTopKProjectionItemBindingDecisionContext(fastTargetSharedPeerTopKProjection{}, prior, projectionSpec{Expression: "m", Alias: "t"})
	if targetCtx.key != "t" || targetCtx.expr != "m" {
		t.Fatalf("expected binding decision context to resolve key/expr t/m, got key=%q expr=%q", targetCtx.key, targetCtx.expr)
	}

	targetCtx = stage2ResolveFastTargetSharedPeerTopKProjectionItemTargetBindingDecision(targetCtx)
	if !targetCtx.handled || !targetCtx.applied {
		t.Fatalf("expected target binding decision helper to handle/apply target binding")
	}
	if targetCtx.updated.targetKey != "t" || targetCtx.updated.targetExpr != "m" {
		t.Fatalf("expected target binding decision helper to set target key/expr, got %+v", targetCtx.updated)
	}

	peerCtx := stage2BuildFastTargetSharedPeerTopKProjectionItemBindingDecisionContext(fastTargetSharedPeerTopKProjection{}, prior, projectionSpec{Expression: "u", Alias: "p"})
	peerCtx = stage2ResolveFastTargetSharedPeerTopKProjectionItemTargetBindingDecision(peerCtx)
	peerCtx = stage2ResolveFastTargetSharedPeerTopKProjectionItemPeerBindingDecision(peerCtx)
	if !peerCtx.handled || !peerCtx.applied {
		t.Fatalf("expected peer binding decision helper to handle/apply peer binding")
	}
	if peerCtx.updated.peerKey != "p" || peerCtx.updated.peerExpr != "u" {
		t.Fatalf("expected peer binding decision helper to set peer key/expr, got %+v", peerCtx.updated)
	}

	similarityCtx := stage2BuildFastTargetSharedPeerTopKProjectionItemBindingDecisionContext(fastTargetSharedPeerTopKProjection{}, prior, projectionSpec{Expression: "sim", Alias: "score"})
	similarityCtx = stage2ResolveFastTargetSharedPeerTopKProjectionItemTargetBindingDecision(similarityCtx)
	similarityCtx = stage2ResolveFastTargetSharedPeerTopKProjectionItemPeerBindingDecision(similarityCtx)
	similarityCtx = stage2ResolveFastTargetSharedPeerTopKProjectionItemSimilarityBindingDecision(similarityCtx)
	if !similarityCtx.handled || !similarityCtx.applied {
		t.Fatalf("expected similarity binding decision helper to handle/apply similarity binding")
	}
	if similarityCtx.updated.similarityKey != "score" || similarityCtx.updated.similarityExpr != "sim" {
		t.Fatalf("expected similarity binding decision helper to set similarity key/expr, got %+v", similarityCtx.updated)
	}

	duplicateSimilarityCtx := stage2BuildFastTargetSharedPeerTopKProjectionItemBindingDecisionContext(
		fastTargetSharedPeerTopKProjection{similarityExpr: "sim", similarityKey: "score"},
		prior,
		projectionSpec{Expression: "sim2", Alias: "score2"},
	)
	duplicateSimilarityCtx = stage2ResolveFastTargetSharedPeerTopKProjectionItemTargetBindingDecision(duplicateSimilarityCtx)
	duplicateSimilarityCtx = stage2ResolveFastTargetSharedPeerTopKProjectionItemPeerBindingDecision(duplicateSimilarityCtx)
	duplicateSimilarityCtx = stage2ResolveFastTargetSharedPeerTopKProjectionItemSimilarityBindingDecision(duplicateSimilarityCtx)
	if duplicateSimilarityCtx.applied {
		t.Fatalf("expected similarity binding decision helper to reject duplicate similarity binding")
	}

	updated, applied := stage2ResolveFastTargetSharedPeerTopKProjectionItemBindingDecision(stage2BuildFastTargetSharedPeerTopKProjectionItemBindingDecisionContext(fastTargetSharedPeerTopKProjection{}, prior, projectionSpec{Expression: "m", Alias: "t"}))
	if !applied || updated.targetKey != "t" {
		t.Fatalf("expected unified projection-item binding decision helper to apply target binding, got applied=%v updated=%+v", applied, updated)
	}
}

func TestStage2ResolveFastTargetSharedPeerTopKProjection(t *testing.T) {
	prior := fastTargetSharedPeerProjection{targetKey: "target", targetExpr: "m", peerKey: "peer", peerExpr: "u"}
	items := []projectionSpec{
		{Expression: "m", Alias: "t"},
		{Expression: "u", Alias: "p"},
		{Expression: "sim", Alias: "score"},
	}
	projection, ok := stage2ResolveFastTargetSharedPeerTopKProjection(items, prior)
	if !ok {
		t.Fatalf("expected projection resolver to accept target/peer/similarity bindings")
	}
	if projection.targetKey != "t" || projection.peerKey != "p" || projection.similarityKey != "score" {
		t.Fatalf("expected resolved projection keys t/p/score, got %+v", projection)
	}

	invalidItems := []projectionSpec{
		{Expression: "m", Alias: "t"},
		{Expression: "u", Alias: "p"},
		{Expression: "sim", Alias: "score"},
		{Expression: "sim2", Alias: "score2"},
	}
	if _, ok := stage2ResolveFastTargetSharedPeerTopKProjection(invalidItems, prior); ok {
		t.Fatalf("expected projection resolver to reject duplicate similarity bindings")
	}
}

func TestStage2FastTargetSharedPeerTopKProjectionDecisionHelpers(t *testing.T) {
	prior := fastTargetSharedPeerProjection{targetKey: "target", targetExpr: "m", peerKey: "peer", peerExpr: "u"}
	items := []projectionSpec{
		{Expression: "m", Alias: "t"},
		{Expression: "u", Alias: "p"},
		{Expression: "sim", Alias: "score"},
	}

	decisionCtx := stage2BuildFastTargetSharedPeerTopKProjectionDecisionContext(items, prior)
	if len(decisionCtx.items) != 3 {
		t.Fatalf("expected projection decision context to preserve 3 projection items, got %d", len(decisionCtx.items))
	}

	decisionCtx, ok := stage2ResolveFastTargetSharedPeerTopKProjectionItemsDecision(decisionCtx)
	if !ok {
		t.Fatalf("expected projection-items decision helper to accept valid projection items")
	}
	if decisionCtx.projection.targetKey != "t" || decisionCtx.projection.peerKey != "p" || decisionCtx.projection.similarityKey != "score" {
		t.Fatalf("expected projection-items decision helper to bind t/p/score keys, got %+v", decisionCtx.projection)
	}

	decisionCtx = stage2ResolveFastTargetSharedPeerTopKProjectionBindingsDecision(decisionCtx)
	if !decisionCtx.hasBindings {
		t.Fatalf("expected projection-bindings decision helper to mark bindings as complete")
	}

	projection, ok := stage2ResolveFastTargetSharedPeerTopKProjectionDecision(stage2BuildFastTargetSharedPeerTopKProjectionDecisionContext(items, prior))
	if !ok {
		t.Fatalf("expected projection decision resolver to accept valid projection set")
	}
	if projection.targetKey != "t" || projection.peerKey != "p" || projection.similarityKey != "score" {
		t.Fatalf("expected projection decision resolver to preserve bound keys, got %+v", projection)
	}

	invalidItems := []projectionSpec{
		{Expression: "m", Alias: "t"},
		{Expression: "u", Alias: "p"},
		{Expression: "sim", Alias: "score"},
		{Expression: "sim2", Alias: "score2"},
	}
	if _, ok := stage2ResolveFastTargetSharedPeerTopKProjectionDecision(stage2BuildFastTargetSharedPeerTopKProjectionDecisionContext(invalidItems, prior)); ok {
		t.Fatalf("expected projection decision resolver to reject duplicate similarity bindings")
	}
}

func TestStage2ResolveFastTargetSharedPeerTopKSpecFromWith(t *testing.T) {
	withSpec := projectionClauseSpec{
		OrderBy:  []projectionOrderBySpec{{Expression: "score", Descending: true}},
		SkipRaw:  "1",
		LimitRaw: "4",
	}
	projection := fastTargetSharedPeerTopKProjection{similarityKey: "score", similarityExpr: "sim"}

	spec, ok, err := stage2ResolveFastTargetSharedPeerTopKSpecFromWith(withSpec, projection, Params{})
	if err != nil {
		t.Fatalf("expected shared-peer top-k spec resolver success, got error: %v", err)
	}
	if !ok {
		t.Fatalf("expected shared-peer top-k spec resolver to accept matching order expression")
	}
	if !spec.descending || spec.skip != 1 || spec.limit != 4 {
		t.Fatalf("expected resolved spec {descending:true skip:1 limit:4}, got %+v", spec)
	}

	withSpec.OrderBy[0].Expression = "count(*)"
	if _, ok, err := stage2ResolveFastTargetSharedPeerTopKSpecFromWith(withSpec, projection, Params{}); err != nil || ok {
		t.Fatalf("expected shared-peer top-k spec resolver to reject non-similarity order expression")
	}
}

func TestStage2FastTargetSharedPeerTopKSpecDecisionHelpers(t *testing.T) {
	withSpec := projectionClauseSpec{
		OrderBy:  []projectionOrderBySpec{{Expression: "score", Descending: true}},
		SkipRaw:  "2",
		LimitRaw: "5",
	}
	projection := fastTargetSharedPeerTopKProjection{similarityKey: "score", similarityExpr: "sim"}

	decisionCtx := stage2BuildFastTargetSharedPeerTopKSpecDecisionContext(withSpec, projection, Params{})
	decisionCtx = stage2ResolveFastTargetSharedPeerTopKOrderMatchDecision(decisionCtx)
	if !decisionCtx.orderMatch {
		t.Fatalf("expected order-match decision helper to accept similarity order expression")
	}
	if decisionCtx.orderExpr != "score" {
		t.Fatalf("expected order-match decision helper to capture order expression score, got %q", decisionCtx.orderExpr)
	}

	resolvedDecisionCtx, err := stage2ResolveFastTargetSharedPeerTopKPaginationDecision(decisionCtx)
	if err != nil {
		t.Fatalf("expected pagination decision helper success, got error: %v", err)
	}
	if !resolvedDecisionCtx.paginationOK || resolvedDecisionCtx.skip != 2 || resolvedDecisionCtx.limit != 5 {
		t.Fatalf("expected pagination decision helper to resolve {skip:2 limit:5}, got %+v", resolvedDecisionCtx)
	}

	spec := stage2ResolveFastTargetSharedPeerTopKSpecDecision(resolvedDecisionCtx)
	if !spec.descending || spec.skip != 2 || spec.limit != 5 {
		t.Fatalf("expected spec decision helper to return {descending:true skip:2 limit:5}, got %+v", spec)
	}

	mismatchDecisionCtx := stage2BuildFastTargetSharedPeerTopKSpecDecisionContext(
		projectionClauseSpec{OrderBy: []projectionOrderBySpec{{Expression: "count(*)", Descending: true}}, SkipRaw: "0", LimitRaw: "1"},
		projection,
		Params{},
	)
	mismatchDecisionCtx = stage2ResolveFastTargetSharedPeerTopKOrderMatchDecision(mismatchDecisionCtx)
	if mismatchDecisionCtx.orderMatch {
		t.Fatalf("expected order-match decision helper to reject non-similarity order expression")
	}
}

func TestStage2FastTargetSharedPeerTopKSpecResolveDecisionHelpers(t *testing.T) {
	withSpec := projectionClauseSpec{
		OrderBy:  []projectionOrderBySpec{{Expression: "score", Descending: true}},
		SkipRaw:  "2",
		LimitRaw: "5",
	}
	projection := fastTargetSharedPeerTopKProjection{similarityKey: "score", similarityExpr: "sim"}

	resolveCtx := stage2BuildFastTargetSharedPeerTopKSpecResolveDecisionContext(withSpec, projection, Params{})
	if resolveCtx.decisionCtx.withSpec.OrderBy[0].Expression != "score" {
		t.Fatalf("expected spec-resolve context builder to preserve order expression score")
	}

	resolveCtx = stage2ResolveFastTargetSharedPeerTopKSpecOrderDecision(resolveCtx)
	if !resolveCtx.decisionCtx.orderMatch {
		t.Fatalf("expected spec-order decision helper to accept similarity order expression")
	}

	resolveCtx, err := stage2ResolveFastTargetSharedPeerTopKSpecPaginationDecision(resolveCtx)
	if err != nil {
		t.Fatalf("expected spec-pagination decision helper success, got error: %v", err)
	}
	if !resolveCtx.decisionCtx.paginationOK || resolveCtx.decisionCtx.skip != 2 || resolveCtx.decisionCtx.limit != 5 {
		t.Fatalf("expected spec-pagination decision helper to resolve skip=2 limit=5, got %+v", resolveCtx.decisionCtx)
	}

	resolveCtx = stage2ResolveFastTargetSharedPeerTopKSpecFinalizeDecision(resolveCtx)
	if !resolveCtx.hasSpec {
		t.Fatalf("expected spec-finalize decision helper to produce top-k spec")
	}
	if !resolveCtx.spec.descending || resolveCtx.spec.skip != 2 || resolveCtx.spec.limit != 5 {
		t.Fatalf("expected spec-finalize decision helper spec {descending:true skip:2 limit:5}, got %+v", resolveCtx.spec)
	}

	spec, ok, err := stage2ResolveFastTargetSharedPeerTopKSpecResolveDecision(stage2BuildFastTargetSharedPeerTopKSpecResolveDecisionContext(withSpec, projection, Params{}))
	if err != nil {
		t.Fatalf("expected spec-resolve decision helper success, got error: %v", err)
	}
	if !ok {
		t.Fatalf("expected spec-resolve decision helper to accept valid spec inputs")
	}
	if !spec.descending || spec.skip != 2 || spec.limit != 5 {
		t.Fatalf("expected spec-resolve decision helper spec {descending:true skip:2 limit:5}, got %+v", spec)
	}

	mismatchSpec, ok, err := stage2ResolveFastTargetSharedPeerTopKSpecResolveDecision(stage2BuildFastTargetSharedPeerTopKSpecResolveDecisionContext(
		projectionClauseSpec{OrderBy: []projectionOrderBySpec{{Expression: "count(*)", Descending: true}}, SkipRaw: "0", LimitRaw: "1"},
		projection,
		Params{},
	))
	if err != nil {
		t.Fatalf("expected spec-resolve decision helper mismatch path without error, got: %v", err)
	}
	if ok {
		t.Fatalf("expected spec-resolve decision helper to reject non-similarity order expression")
	}
	if mismatchSpec != (fastTargetSharedPeerTopKSpec{}) {
		t.Fatalf("expected mismatch path to return zero-value spec, got %+v", mismatchSpec)
	}
}

func TestStage2FastTargetSharedPeerTopKWithClauseDecisionHelpers(t *testing.T) {
	withSpec := projectionClauseSpec{
		ProjectionRaw: "m AS t, u AS p, sim AS score",
		OrderBy:       []projectionOrderBySpec{{Expression: "score", Descending: true}},
		SkipRaw:       "1",
		LimitRaw:      "4",
	}
	prior := fastTargetSharedPeerProjection{targetKey: "target", targetExpr: "m", peerKey: "peer", peerExpr: "u"}

	decisionCtx := stage2BuildFastTargetSharedPeerTopKWithClauseDecisionContext(withSpec, prior, Params{})
	resolvedItemsCtx, ok := stage2ResolveFastTargetSharedPeerTopKWithClauseItemsDecision(decisionCtx)
	if !ok || !resolvedItemsCtx.hasItems || len(resolvedItemsCtx.items) != 3 {
		t.Fatalf("expected items decision helper to resolve three projection items")
	}

	resolvedProjectionCtx, ok := stage2ResolveFastTargetSharedPeerTopKWithClauseProjectionDecision(resolvedItemsCtx)
	if !ok || !resolvedProjectionCtx.hasProjection {
		t.Fatalf("expected projection decision helper to resolve projection bindings")
	}
	if resolvedProjectionCtx.projection.targetKey != "t" || resolvedProjectionCtx.projection.peerKey != "p" || resolvedProjectionCtx.projection.similarityKey != "score" {
		t.Fatalf("expected projection decision helper keys t/p/score, got %+v", resolvedProjectionCtx.projection)
	}

	resolvedSpecCtx, ok, err := stage2ResolveFastTargetSharedPeerTopKWithClauseSpecDecision(resolvedProjectionCtx)
	if err != nil {
		t.Fatalf("expected spec decision helper success, got error: %v", err)
	}
	if !ok || !resolvedSpecCtx.hasSpec {
		t.Fatalf("expected spec decision helper to resolve top-k spec")
	}
	if !resolvedSpecCtx.spec.descending || resolvedSpecCtx.spec.skip != 1 || resolvedSpecCtx.spec.limit != 4 {
		t.Fatalf("expected resolved spec {descending:true skip:1 limit:4}, got %+v", resolvedSpecCtx.spec)
	}

	projection, spec, ok, err := stage2ResolveFastTargetSharedPeerTopKWithClauseDecision(decisionCtx)
	if err != nil {
		t.Fatalf("expected with-clause decision resolver success, got error: %v", err)
	}
	if !ok {
		t.Fatalf("expected with-clause decision resolver to accept valid with spec")
	}
	if projection.targetKey != "t" || projection.peerKey != "p" || projection.similarityKey != "score" {
		t.Fatalf("expected with-clause resolver projection keys t/p/score, got %+v", projection)
	}
	if !spec.descending || spec.skip != 1 || spec.limit != 4 {
		t.Fatalf("expected with-clause resolver spec {descending:true skip:1 limit:4}, got %+v", spec)
	}

	invalidDecisionCtx := stage2BuildFastTargetSharedPeerTopKWithClauseDecisionContext(
		projectionClauseSpec{ProjectionRaw: "m AS t, u AS p", OrderBy: []projectionOrderBySpec{{Expression: "score", Descending: true}}, SkipRaw: "0", LimitRaw: "1"},
		prior,
		Params{},
	)
	if _, _, ok, err := stage2ResolveFastTargetSharedPeerTopKWithClauseDecision(invalidDecisionCtx); err != nil || ok {
		t.Fatalf("expected with-clause decision resolver to reject invalid projection item count")
	}
}

func TestStage2FastTargetSharedPeerTopKWithClauseResolveDecisionHelpers(t *testing.T) {
	withSpec := projectionClauseSpec{
		ProjectionRaw: "m AS t, u AS p, sim AS score",
		OrderBy:       []projectionOrderBySpec{{Expression: "score", Descending: true}},
		SkipRaw:       "1",
		LimitRaw:      "4",
	}
	prior := fastTargetSharedPeerProjection{targetKey: "target", targetExpr: "m", peerKey: "peer", peerExpr: "u"}

	decisionCtx := stage2BuildFastTargetSharedPeerTopKWithClauseDecisionContext(withSpec, prior, Params{})
	resolveCtx := stage2BuildFastTargetSharedPeerTopKWithClauseResolveDecisionContext(decisionCtx)

	resolveCtx = stage2ResolveFastTargetSharedPeerTopKWithClauseItemsResolveDecision(resolveCtx)
	if !resolveCtx.ok || !resolveCtx.decisionCtx.hasItems {
		t.Fatalf("expected with-clause items resolve decision helper to resolve projection items")
	}

	resolveCtx = stage2ResolveFastTargetSharedPeerTopKWithClauseProjectionResolveDecision(resolveCtx)
	if !resolveCtx.ok || !resolveCtx.decisionCtx.hasProjection {
		t.Fatalf("expected with-clause projection resolve decision helper to resolve projection bindings")
	}

	resolveCtx, err := stage2ResolveFastTargetSharedPeerTopKWithClauseSpecResolveDecision(resolveCtx)
	if err != nil {
		t.Fatalf("expected with-clause spec resolve decision helper success, got error: %v", err)
	}
	if !resolveCtx.ok || !resolveCtx.decisionCtx.hasSpec {
		t.Fatalf("expected with-clause spec resolve decision helper to resolve top-k spec")
	}

	resolveCtx = stage2ResolveFastTargetSharedPeerTopKWithClauseFinalizeResolveDecision(resolveCtx)
	if resolveCtx.projection.targetKey != "t" || resolveCtx.projection.peerKey != "p" || resolveCtx.projection.similarityKey != "score" {
		t.Fatalf("expected with-clause finalize resolve decision helper projection keys t/p/score, got %+v", resolveCtx.projection)
	}
	if !resolveCtx.spec.descending || resolveCtx.spec.skip != 1 || resolveCtx.spec.limit != 4 {
		t.Fatalf("expected with-clause finalize resolve decision helper spec {descending:true skip:1 limit:4}, got %+v", resolveCtx.spec)
	}

	projection, spec, ok, err := stage2ResolveFastTargetSharedPeerTopKWithClauseResolveDecision(stage2BuildFastTargetSharedPeerTopKWithClauseResolveDecisionContext(decisionCtx))
	if err != nil {
		t.Fatalf("expected with-clause resolve decision helper success, got error: %v", err)
	}
	if !ok {
		t.Fatalf("expected with-clause resolve decision helper to accept valid with-clause decision context")
	}
	if projection.targetKey != "t" || projection.peerKey != "p" || projection.similarityKey != "score" {
		t.Fatalf("expected with-clause resolve decision helper projection keys t/p/score, got %+v", projection)
	}
	if !spec.descending || spec.skip != 1 || spec.limit != 4 {
		t.Fatalf("expected with-clause resolve decision helper spec {descending:true skip:1 limit:4}, got %+v", spec)
	}

	invalidDecisionCtx := stage2BuildFastTargetSharedPeerTopKWithClauseDecisionContext(
		projectionClauseSpec{ProjectionRaw: "m AS t, u AS p", OrderBy: []projectionOrderBySpec{{Expression: "score", Descending: true}}, SkipRaw: "0", LimitRaw: "1"},
		prior,
		Params{},
	)
	if _, _, ok, err := stage2ResolveFastTargetSharedPeerTopKWithClauseResolveDecision(stage2BuildFastTargetSharedPeerTopKWithClauseResolveDecisionContext(invalidDecisionCtx)); err != nil || ok {
		t.Fatalf("expected with-clause resolve decision helper to reject invalid projection item count")
	}
}

func TestStage2FastTargetSharedPeerTopKWithClauseParseDecisionHelpers(t *testing.T) {
	clause := ast.Clause{
		Kind: ast.ClauseKindWith,
		Projection: &ast.ReturnClause{
			Items: []ast.ProjectionItem{
				{Expression: ast.Expression{Raw: "m"}, Alias: "t"},
				{Expression: ast.Expression{Raw: "u"}, Alias: "p"},
				{Expression: ast.Expression{Raw: "sim"}, Alias: "score"},
			},
			OrderBy: []ast.SortItem{{Expression: ast.Expression{Raw: "score"}, Direction: ast.SortDirectionDesc}},
			Skip:    &ast.Expression{Raw: "1"},
			Limit:   &ast.Expression{Raw: "4"},
		},
	}
	prior := fastTargetSharedPeerProjection{targetKey: "target", targetExpr: "m", peerKey: "peer", peerExpr: "u"}

	parseDecisionCtx := stage2BuildFastTargetSharedPeerTopKWithClauseParseDecisionContext(clause, prior, Params{})
	parseDecisionCtx = stage2ResolveFastTargetSharedPeerTopKWithClauseSpecParseDecision(parseDecisionCtx)
	if !parseDecisionCtx.hasWithSpec {
		t.Fatalf("expected with-clause parse spec decision helper to resolve with spec")
	}
	if parseDecisionCtx.withSpec.ProjectionRaw != "m AS t, u AS p, sim AS score" {
		t.Fatalf("expected with-clause parse spec decision helper to preserve projection raw, got %q", parseDecisionCtx.withSpec.ProjectionRaw)
	}

	parseDecisionCtx = stage2ResolveFastTargetSharedPeerTopKWithClauseEligibilityDecision(parseDecisionCtx)
	if !parseDecisionCtx.eligible {
		t.Fatalf("expected with-clause eligibility decision helper to accept valid with spec")
	}

	projection, spec, ok, err := stage2ResolveFastTargetSharedPeerTopKWithClauseParseDecision(stage2BuildFastTargetSharedPeerTopKWithClauseParseDecisionContext(clause, prior, Params{}))
	if err != nil {
		t.Fatalf("expected with-clause parse decision resolver success, got error: %v", err)
	}
	if !ok {
		t.Fatalf("expected with-clause parse decision resolver to accept valid with clause")
	}
	if projection.targetKey != "t" || projection.peerKey != "p" || projection.similarityKey != "score" {
		t.Fatalf("expected with-clause parse decision resolver projection keys t/p/score, got %+v", projection)
	}
	if !spec.descending || spec.skip != 1 || spec.limit != 4 {
		t.Fatalf("expected with-clause parse decision resolver spec {descending:true skip:1 limit:4}, got %+v", spec)
	}

	invalidParseDecisionCtx := stage2BuildFastTargetSharedPeerTopKWithClauseParseDecisionContext(ast.Clause{Kind: ast.ClauseKindWith}, prior, Params{})
	invalidParseDecisionCtx = stage2ResolveFastTargetSharedPeerTopKWithClauseSpecParseDecision(invalidParseDecisionCtx)
	if invalidParseDecisionCtx.hasWithSpec {
		t.Fatalf("expected with-clause parse spec decision helper to reject missing projection metadata")
	}
	invalidParseDecisionCtx = stage2ResolveFastTargetSharedPeerTopKWithClauseEligibilityDecision(invalidParseDecisionCtx)
	if invalidParseDecisionCtx.eligible {
		t.Fatalf("expected with-clause eligibility decision helper to reject missing with spec")
	}
	if _, _, ok, err := stage2ResolveFastTargetSharedPeerTopKWithClauseParseDecision(stage2BuildFastTargetSharedPeerTopKWithClauseParseDecisionContext(ast.Clause{Kind: ast.ClauseKindWith}, prior, Params{})); err != nil || ok {
		t.Fatalf("expected with-clause parse decision resolver to reject missing projection metadata")
	}
}

func TestStage2ShouldReturnEmptyFastTargetSharedPeerTopKRows(t *testing.T) {
	if !stage2ShouldReturnEmptyFastTargetSharedPeerTopKRows(0) {
		t.Fatalf("expected zero limit to return empty shared-peer top-k rows")
	}
	if !stage2ShouldReturnEmptyFastTargetSharedPeerTopKRows(-1) {
		t.Fatalf("expected negative limit to return empty shared-peer top-k rows")
	}
	if stage2ShouldReturnEmptyFastTargetSharedPeerTopKRows(1) {
		t.Fatalf("expected positive limit to continue shared-peer top-k planning")
	}
}

func TestStage2ResolveFastTargetSharedPeerTopKKeepSize(t *testing.T) {
	if got := stage2ResolveFastTargetSharedPeerTopKKeepSize(2, 3); got != 5 {
		t.Fatalf("expected keep size 5, got %d", got)
	}
	if got := stage2ResolveFastTargetSharedPeerTopKKeepSize(-5, 3); got != 3 {
		t.Fatalf("expected keep size fallback to limit 3, got %d", got)
	}
}

func TestStage2ShouldIncludeFastTargetSharedPeerAggregate(t *testing.T) {
	if stage2ShouldIncludeFastTargetSharedPeerAggregate(nil) {
		t.Fatalf("expected nil aggregate to be excluded")
	}
	if stage2ShouldIncludeFastTargetSharedPeerAggregate(&fastTargetSharedPeerAggregate{shared: 0}) {
		t.Fatalf("expected non-shared aggregate to be excluded")
	}
	if !stage2ShouldIncludeFastTargetSharedPeerAggregate(&fastTargetSharedPeerAggregate{shared: 1}) {
		t.Fatalf("expected shared aggregate to be included")
	}
}

func TestStage2FastTargetSharedPeerTopKWindowPredicates(t *testing.T) {
	if !stage2ShouldReturnEmptyFastTargetSharedPeerTopKRowsAfterSkip(3, 3) {
		t.Fatalf("expected skip at ranked length to return empty shared-peer top-k window")
	}
	if stage2ShouldReturnEmptyFastTargetSharedPeerTopKRowsAfterSkip(2, 3) {
		t.Fatalf("expected skip below ranked length to keep shared-peer top-k window")
	}
	if got := stage2ResolveFastTargetSharedPeerTopKWindowEnd(1, 2, 10); got != 3 {
		t.Fatalf("expected capped shared-peer top-k window end 3, got %d", got)
	}
	if got := stage2ResolveFastTargetSharedPeerTopKWindowEnd(1, 20, 10); got != 10 {
		t.Fatalf("expected ranked-length shared-peer top-k window end 10, got %d", got)
	}
}

func TestStage2CompareSharedPeerTopKScore(t *testing.T) {
	if got := stage2CompareSharedPeerTopKScore(9, 7, true); got != -1 {
		t.Fatalf("expected descending compare to prefer higher score, got %d", got)
	}
	if got := stage2CompareSharedPeerTopKScore(7, 9, true); got != 1 {
		t.Fatalf("expected descending compare to rank lower score later, got %d", got)
	}
	if got := stage2CompareSharedPeerTopKScore(7, 9, false); got != -1 {
		t.Fatalf("expected ascending compare to prefer lower score, got %d", got)
	}
	if got := stage2CompareSharedPeerTopKScore(8, 8, false); got != 0 {
		t.Fatalf("expected equal scores to tie, got %d", got)
	}
}

func TestStage2CompareSharedPeerTopKInputIndex(t *testing.T) {
	if got := stage2CompareSharedPeerTopKInputIndex(1, 2); got != -1 {
		t.Fatalf("expected lower input index to rank first, got %d", got)
	}
	if got := stage2CompareSharedPeerTopKInputIndex(3, 2); got != 1 {
		t.Fatalf("expected higher input index to rank later, got %d", got)
	}
	if got := stage2CompareSharedPeerTopKInputIndex(4, 4); got != 0 {
		t.Fatalf("expected equal input index to tie, got %d", got)
	}
}

func TestStage2FastTargetSharedPeerVertexID(t *testing.T) {
	if got := stage2FastTargetSharedPeerVertexID(nil); got != "" {
		t.Fatalf("expected nil vertex to normalize to empty ID, got %q", got)
	}
	if got := stage2FastTargetSharedPeerVertexID(&graph.Vertex{ID: " m1 "}); got != "m1" {
		t.Fatalf("expected vertex ID to be trimmed, got %q", got)
	}
}

func TestStage2FastTargetSharedPeerAggregateLess(t *testing.T) {
	left := &fastTargetSharedPeerAggregate{target: &graph.Vertex{ID: "m1"}, peer: &graph.Vertex{ID: "u1"}}
	right := &fastTargetSharedPeerAggregate{target: &graph.Vertex{ID: "m2"}, peer: &graph.Vertex{ID: "u0"}}
	if !stage2FastTargetSharedPeerAggregateLess(left, right) {
		t.Fatalf("expected lower target ID to sort first")
	}

	tieLeft := &fastTargetSharedPeerAggregate{target: &graph.Vertex{ID: "m1"}, peer: &graph.Vertex{ID: "u1"}}
	tieRight := &fastTargetSharedPeerAggregate{target: &graph.Vertex{ID: "m1"}, peer: &graph.Vertex{ID: "u2"}}
	if !stage2FastTargetSharedPeerAggregateLess(tieLeft, tieRight) {
		t.Fatalf("expected lower peer ID to break target-ID ties")
	}
}

func TestStage2FastTargetSharedPeerAggregateOrderingHelpers(t *testing.T) {
	if stage2ShouldIncludeFastTargetSharedPeerAggregateForOrdering(nil) {
		t.Fatalf("expected nil aggregate to be excluded from ordering collection")
	}
	if !stage2ShouldIncludeFastTargetSharedPeerAggregateForOrdering(&fastTargetSharedPeerAggregate{}) {
		t.Fatalf("expected non-nil aggregate to be included in ordering collection")
	}

	a := &fastTargetSharedPeerAggregate{target: &graph.Vertex{ID: "m2"}, peer: &graph.Vertex{ID: "u2"}}
	b := &fastTargetSharedPeerAggregate{target: &graph.Vertex{ID: "m1"}, peer: &graph.Vertex{ID: "u3"}}
	c := &fastTargetSharedPeerAggregate{target: &graph.Vertex{ID: "m1"}, peer: &graph.Vertex{ID: "u1"}}

	collected := stage2CollectFastTargetSharedPeerAggregates(map[string]*fastTargetSharedPeerAggregate{
		"a": a,
		"b": nil,
		"c": b,
		"d": c,
	})
	if len(collected) != 3 {
		t.Fatalf("expected collection helper to keep three non-nil aggregates, got %d", len(collected))
	}

	stage2SortFastTargetSharedPeerAggregates(collected)
	if collected[0] != c || collected[1] != b || collected[2] != a {
		t.Fatalf("expected sorted ordering by target ID then peer ID")
	}

	sorted := sortedFastTargetSharedPeerAggregates(map[string]*fastTargetSharedPeerAggregate{
		"a": a,
		"b": nil,
		"c": b,
		"d": c,
	})
	if len(sorted) != 3 {
		t.Fatalf("expected sorted resolver to preserve non-nil aggregate count, got %d", len(sorted))
	}
	if sorted[0] != c || sorted[1] != b || sorted[2] != a {
		t.Fatalf("expected sorted resolver to apply deterministic ordering")
	}
}

func TestStage2ShouldEvaluateFastTargetSharedPeerWhere(t *testing.T) {
	if stage2ShouldEvaluateFastTargetSharedPeerWhere(projectionClauseSpec{WhereRaw: ""}) {
		t.Fatalf("expected empty WHERE to skip shared-peer where evaluation")
	}
	if stage2ShouldEvaluateFastTargetSharedPeerWhere(projectionClauseSpec{WhereRaw: "   "}) {
		t.Fatalf("expected whitespace WHERE to skip shared-peer where evaluation")
	}
	if !stage2ShouldEvaluateFastTargetSharedPeerWhere(projectionClauseSpec{WhereRaw: "sim > 0"}) {
		t.Fatalf("expected non-empty WHERE to enable shared-peer where evaluation")
	}
}

func TestStage2ResolveFastTargetSharedPeerTopKScore(t *testing.T) {
	if score, ok := stage2ResolveFastTargetSharedPeerTopKScore(2.5); !ok || score != 2.5 {
		t.Fatalf("expected numeric similarity value to resolve score")
	}
	if _, ok := stage2ResolveFastTargetSharedPeerTopKScore("not-numeric"); ok {
		t.Fatalf("expected non-numeric similarity value to be rejected")
	}
}

func TestStage2BuildFastTargetSharedPeerTopKTrimmedRow(t *testing.T) {
	projection := fastTargetSharedPeerProjection{targetKey: "t", peerKey: "p"}
	topKProjection := fastTargetSharedPeerTopKProjection{targetKey: "target", peerKey: "peer", similarityKey: "sim"}
	row := Row{"t": "m1", "p": "u1", "ignore": "x"}

	trimmed := stage2BuildFastTargetSharedPeerTopKTrimmedRow(row, projection, topKProjection, 3.14)
	if len(trimmed) != 3 {
		t.Fatalf("expected trimmed row to keep only top-k projection keys, got %d keys", len(trimmed))
	}
	if trimmed["target"] != "m1" || trimmed["peer"] != "u1" {
		t.Fatalf("expected trimmed row to map target/peer values")
	}
	if trimmed["sim"].(float64) != 3.14 {
		t.Fatalf("expected trimmed row to keep similarity value")
	}
}

func TestStage2ResolveFastTargetSharedPeerTopKCandidateRow(t *testing.T) {
	agg := &fastTargetSharedPeerAggregate{target: &graph.Vertex{ID: "m1"}, peer: &graph.Vertex{ID: "u1"}, shared: 3, sumAbsDiff: 6}
	projection := fastTargetSharedPeerProjection{targetKey: "target", peerKey: "peer", sharedCountKey: "shared", avgDiffKey: "avg"}
	topKProjection := fastTargetSharedPeerTopKProjection{targetKey: "outTarget", peerKey: "outPeer", similarityKey: "sim", similarityExpr: "shared"}

	trimmed, score, include, err := stage2ResolveFastTargetSharedPeerTopKCandidateRow(agg, projection, projectionClauseSpec{}, topKProjection, context.Background(), nil, Params{}, nil)
	if err != nil {
		t.Fatalf("expected candidate-row helper success, got error: %v", err)
	}
	if !include {
		t.Fatalf("expected numeric similarity expression to include candidate row")
	}
	if score != 3 {
		t.Fatalf("expected numeric score 3 from shared count, got %v", score)
	}
	if trimmed["outTarget"].(*graph.Vertex).ID != "m1" || trimmed["outPeer"].(*graph.Vertex).ID != "u1" {
		t.Fatalf("expected trimmed row to keep mapped target/peer bindings")
	}
	if trimmed["sim"].(int) != 3 {
		t.Fatalf("expected trimmed row similarity payload to preserve evaluated value 3")
	}

	topKProjection.similarityExpr = "target"
	if _, _, include, err := stage2ResolveFastTargetSharedPeerTopKCandidateRow(agg, projection, projectionClauseSpec{}, topKProjection, context.Background(), nil, Params{}, nil); err != nil || include {
		t.Fatalf("expected non-numeric similarity expression to exclude candidate row")
	}
}

func TestStage2FastTargetSharedPeerTopKCandidateRowDecisionHelpers(t *testing.T) {
	agg := &fastTargetSharedPeerAggregate{target: &graph.Vertex{ID: "m1"}, peer: &graph.Vertex{ID: "u1"}, shared: 3, sumAbsDiff: 6}
	projection := fastTargetSharedPeerProjection{targetKey: "target", peerKey: "peer", sharedCountKey: "shared", avgDiffKey: "avg"}
	topKProjection := fastTargetSharedPeerTopKProjection{targetKey: "outTarget", peerKey: "outPeer", similarityKey: "sim", similarityExpr: "shared"}

	decisionCtx := stage2BuildFastTargetSharedPeerTopKCandidateRowDecisionContext(agg, projection, projectionClauseSpec{}, topKProjection, context.Background(), nil, Params{}, nil)
	if decisionCtx.row == nil {
		t.Fatalf("expected candidate-row decision context to seed projection row")
	}
	if decisionCtx.row["shared"].(int) != 3 {
		t.Fatalf("expected candidate-row decision context seed to include shared count")
	}

	include, err := stage2ResolveFastTargetSharedPeerTopKCandidateWhereDecision(decisionCtx)
	if err != nil {
		t.Fatalf("expected where-decision helper success without where clause, got %v", err)
	}
	if !include {
		t.Fatalf("expected where-decision helper to include row when where clause is empty")
	}

	resolvedCtx, include, err := stage2ResolveFastTargetSharedPeerTopKCandidateSimilarityDecision(decisionCtx)
	if err != nil {
		t.Fatalf("expected similarity-decision helper success, got %v", err)
	}
	if !include {
		t.Fatalf("expected similarity-decision helper to include numeric similarity expression")
	}
	if resolvedCtx.score != 3 {
		t.Fatalf("expected similarity-decision helper score 3, got %v", resolvedCtx.score)
	}
	if resolvedCtx.trimmed["outTarget"].(*graph.Vertex).ID != "m1" {
		t.Fatalf("expected similarity-decision helper trimmed row to preserve mapped target")
	}

	trimmed, score, include, err := stage2ResolveFastTargetSharedPeerTopKCandidateRowDecision(decisionCtx)
	if err != nil {
		t.Fatalf("expected candidate-row decision resolver success, got %v", err)
	}
	if !include || score != 3 {
		t.Fatalf("expected candidate-row decision resolver include=true score=3, got include=%v score=%v", include, score)
	}
	if trimmed["sim"].(int) != 3 {
		t.Fatalf("expected candidate-row decision resolver to preserve similarity payload 3")
	}

	nonNumericCtx := stage2BuildFastTargetSharedPeerTopKCandidateRowDecisionContext(agg, projection, projectionClauseSpec{}, fastTargetSharedPeerTopKProjection{targetKey: "outTarget", peerKey: "outPeer", similarityKey: "sim", similarityExpr: "target"}, context.Background(), nil, Params{}, nil)
	if _, include, err := stage2ResolveFastTargetSharedPeerTopKCandidateSimilarityDecision(nonNumericCtx); err != nil || include {
		t.Fatalf("expected similarity-decision helper to exclude non-numeric similarity expression")
	}
}

func TestStage2ResolveFastTargetSharedPeerTopKCandidate(t *testing.T) {
	agg := &fastTargetSharedPeerAggregate{target: &graph.Vertex{ID: "m1"}, peer: &graph.Vertex{ID: "u1"}, shared: 2, sumAbsDiff: 1}
	projection := fastTargetSharedPeerProjection{targetKey: "target", peerKey: "peer", sharedCountKey: "shared", avgDiffKey: "avg"}
	topKProjection := fastTargetSharedPeerTopKProjection{targetKey: "outTarget", peerKey: "outPeer", similarityKey: "sim", similarityExpr: "shared"}

	candidate, nextInputIndex, include, err := stage2ResolveFastTargetSharedPeerTopKCandidate(agg, projection, projectionClauseSpec{}, topKProjection, 7, context.Background(), nil, Params{}, nil)
	if err != nil {
		t.Fatalf("expected candidate helper success, got error: %v", err)
	}
	if !include {
		t.Fatalf("expected numeric similarity expression to include candidate")
	}
	if nextInputIndex != 8 {
		t.Fatalf("expected input index increment to 8, got %d", nextInputIndex)
	}
	if candidate.score != 2 || candidate.inputIndex != 7 {
		t.Fatalf("expected candidate score/index to be preserved, got score=%v index=%d", candidate.score, candidate.inputIndex)
	}

	topKProjection.similarityExpr = "target"
	_, nextInputIndex, include, err = stage2ResolveFastTargetSharedPeerTopKCandidate(agg, projection, projectionClauseSpec{}, topKProjection, 7, context.Background(), nil, Params{}, nil)
	if err != nil {
		t.Fatalf("expected non-numeric candidate helper path to avoid errors, got: %v", err)
	}
	if include {
		t.Fatalf("expected non-numeric similarity expression to exclude candidate")
	}
	if nextInputIndex != 7 {
		t.Fatalf("expected excluded candidate to keep input index unchanged, got %d", nextInputIndex)
	}
}

func TestStage2FastTargetSharedPeerTopKCandidateDecisionHelpers(t *testing.T) {
	agg := &fastTargetSharedPeerAggregate{target: &graph.Vertex{ID: "m1"}, peer: &graph.Vertex{ID: "u1"}, shared: 2, sumAbsDiff: 1}
	projection := fastTargetSharedPeerProjection{targetKey: "target", peerKey: "peer", sharedCountKey: "shared", avgDiffKey: "avg"}
	topKProjection := fastTargetSharedPeerTopKProjection{targetKey: "outTarget", peerKey: "outPeer", similarityKey: "sim", similarityExpr: "shared"}

	decisionCtx := stage2BuildFastTargetSharedPeerTopKCandidateDecisionContext(agg, projection, projectionClauseSpec{}, topKProjection, 7, context.Background(), nil, Params{}, nil)
	if decisionCtx.inputIndex != 7 {
		t.Fatalf("expected decision context to preserve input index 7, got %d", decisionCtx.inputIndex)
	}

	resolvedCtx, include, err := stage2ResolveFastTargetSharedPeerTopKCandidateRowForDecision(decisionCtx)
	if err != nil {
		t.Fatalf("expected candidate row-for-decision helper success, got error: %v", err)
	}
	if !include {
		t.Fatalf("expected candidate row-for-decision helper to include numeric candidate")
	}
	if resolvedCtx.score != 2 {
		t.Fatalf("expected candidate row-for-decision helper score 2, got %v", resolvedCtx.score)
	}

	resolvedCtx = stage2ResolveFastTargetSharedPeerRankedCandidateForDecision(resolvedCtx)
	if resolvedCtx.candidate.score != 2 || resolvedCtx.candidate.inputIndex != 7 {
		t.Fatalf("expected ranked-candidate decision helper to preserve score/index, got score=%v index=%d", resolvedCtx.candidate.score, resolvedCtx.candidate.inputIndex)
	}
	if resolvedCtx.nextInputIndex != 8 {
		t.Fatalf("expected ranked-candidate decision helper next index 8, got %d", resolvedCtx.nextInputIndex)
	}

	candidate, nextInputIndex, include, err := stage2ResolveFastTargetSharedPeerTopKCandidateDecision(decisionCtx)
	if err != nil {
		t.Fatalf("expected candidate decision resolver success, got error: %v", err)
	}
	if !include || candidate.score != 2 || nextInputIndex != 8 {
		t.Fatalf("expected candidate decision resolver include=true score=2 next=8, got include=%v score=%v next=%d", include, candidate.score, nextInputIndex)
	}

	nonNumericCtx := stage2BuildFastTargetSharedPeerTopKCandidateDecisionContext(agg, projection, projectionClauseSpec{}, fastTargetSharedPeerTopKProjection{targetKey: "outTarget", peerKey: "outPeer", similarityKey: "sim", similarityExpr: "target"}, 7, context.Background(), nil, Params{}, nil)
	_, nextInputIndex, include, err = stage2ResolveFastTargetSharedPeerTopKCandidateDecision(nonNumericCtx)
	if err != nil {
		t.Fatalf("expected candidate decision resolver non-numeric path without error, got: %v", err)
	}
	if include {
		t.Fatalf("expected candidate decision resolver to exclude non-numeric candidate")
	}
	if nextInputIndex != 7 {
		t.Fatalf("expected excluded candidate to preserve input index 7, got %d", nextInputIndex)
	}

	resolveCtx := stage2BuildFastTargetSharedPeerTopKCandidateResolveDecisionContext(decisionCtx)
	resolveCtx, err = stage2ResolveFastTargetSharedPeerTopKCandidateRowResolveDecision(resolveCtx)
	if err != nil {
		t.Fatalf("expected candidate row-resolve decision helper success, got error: %v", err)
	}
	if !resolveCtx.include {
		t.Fatalf("expected candidate row-resolve decision helper to mark include=true for numeric candidate")
	}

	resolveCtx = stage2ResolveFastTargetSharedPeerTopKRankedCandidateResolveDecision(resolveCtx)
	if resolveCtx.candidate.score != 2 || resolveCtx.nextInputIndex != 8 {
		t.Fatalf("expected ranked-candidate resolve helper to produce score=2 next=8, got score=%v next=%d", resolveCtx.candidate.score, resolveCtx.nextInputIndex)
	}

	resolvedCandidate, resolvedNext, resolvedInclude := stage2ResolveFastTargetSharedPeerTopKCandidateFinalizeResolveDecision(resolveCtx)
	if !resolvedInclude || resolvedCandidate.score != 2 || resolvedNext != 8 {
		t.Fatalf("expected candidate finalize-resolve helper include=true score=2 next=8, got include=%v score=%v next=%d", resolvedInclude, resolvedCandidate.score, resolvedNext)
	}

	candidateFlowCtx := stage2BuildFastTargetSharedPeerTopKCandidateResolveFlowDecisionContext(stage2BuildFastTargetSharedPeerTopKCandidateResolveDecisionContext(decisionCtx))
	candidateFlowCtx = stage2ResolveFastTargetSharedPeerTopKCandidateRowFlowDecision(candidateFlowCtx)
	if candidateFlowCtx.hasError {
		t.Fatalf("expected candidate row flow helper success, got error: %v", candidateFlowCtx.err)
	}
	candidateFlowCtx = stage2ResolveFastTargetSharedPeerTopKCandidateRankedFlowDecision(candidateFlowCtx)
	flowCandidate, flowNext, flowInclude, err := stage2ResolveFastTargetSharedPeerTopKCandidateResolveFlowResultDecision(candidateFlowCtx)
	if err != nil {
		t.Fatalf("expected candidate resolve flow result helper success, got error: %v", err)
	}
	if !flowInclude || flowCandidate.score != 2 || flowNext != 8 {
		t.Fatalf("expected candidate resolve flow result helper include=true score=2 next=8, got include=%v score=%v next=%d", flowInclude, flowCandidate.score, flowNext)
	}

	flowCandidate, flowNext, flowInclude, err = stage2ResolveFastTargetSharedPeerTopKCandidateResolveFlowDecision(stage2BuildFastTargetSharedPeerTopKCandidateResolveFlowDecisionContext(stage2BuildFastTargetSharedPeerTopKCandidateResolveDecisionContext(decisionCtx)))
	if err != nil {
		t.Fatalf("expected unified candidate resolve flow helper success, got error: %v", err)
	}
	if !flowInclude || flowCandidate.score != 2 || flowNext != 8 {
		t.Fatalf("expected unified candidate resolve flow helper include=true score=2 next=8, got include=%v score=%v next=%d", flowInclude, flowCandidate.score, flowNext)
	}

	resolvedCandidate, resolvedNext, resolvedInclude, err = stage2ResolveFastTargetSharedPeerTopKCandidateResolveDecision(stage2BuildFastTargetSharedPeerTopKCandidateResolveDecisionContext(decisionCtx))
	if err != nil {
		t.Fatalf("expected unified candidate-resolve decision helper success, got error: %v", err)
	}
	if !resolvedInclude || resolvedCandidate.score != 2 || resolvedNext != 8 {
		t.Fatalf("expected unified candidate-resolve decision helper include=true score=2 next=8, got include=%v score=%v next=%d", resolvedInclude, resolvedCandidate.score, resolvedNext)
	}

	nonNumericResolveCtx := stage2BuildFastTargetSharedPeerTopKCandidateResolveDecisionContext(nonNumericCtx)
	nonNumericResolveCtx, err = stage2ResolveFastTargetSharedPeerTopKCandidateRowResolveDecision(nonNumericResolveCtx)
	if err != nil {
		t.Fatalf("expected candidate row-resolve decision helper non-numeric path without error, got: %v", err)
	}
	if nonNumericResolveCtx.include {
		t.Fatalf("expected candidate row-resolve decision helper to mark include=false for non-numeric candidate")
	}
	nonNumericResolveCtx = stage2ResolveFastTargetSharedPeerTopKRankedCandidateResolveDecision(nonNumericResolveCtx)
	_, nonNumericNext, nonNumericInclude := stage2ResolveFastTargetSharedPeerTopKCandidateFinalizeResolveDecision(nonNumericResolveCtx)
	if nonNumericInclude {
		t.Fatalf("expected candidate finalize-resolve helper to keep include=false for non-numeric candidate")
	}
	if nonNumericNext != 7 {
		t.Fatalf("expected candidate finalize-resolve helper to preserve input index 7 on exclusion, got %d", nonNumericNext)
	}

	errDecisionCtx := stage2BuildFastTargetSharedPeerTopKCandidateDecisionContext(agg, projection, projectionClauseSpec{}, fastTargetSharedPeerTopKProjection{targetKey: "outTarget", peerKey: "outPeer", similarityKey: "sim", similarityExpr: "("}, 7, context.Background(), nil, Params{}, nil)
	errCandidateFlowCtx := stage2BuildFastTargetSharedPeerTopKCandidateResolveFlowDecisionContext(stage2BuildFastTargetSharedPeerTopKCandidateResolveDecisionContext(errDecisionCtx))
	errCandidateFlowCtx = stage2ResolveFastTargetSharedPeerTopKCandidateRowFlowDecision(errCandidateFlowCtx)
	if !errCandidateFlowCtx.hasError {
		t.Fatalf("expected candidate row flow helper to capture similarity-eval error")
	}
	errCandidateFlowCtx = stage2ResolveFastTargetSharedPeerTopKCandidateRankedFlowDecision(errCandidateFlowCtx)
	if _, next, include, err := stage2ResolveFastTargetSharedPeerTopKCandidateResolveFlowResultDecision(errCandidateFlowCtx); err == nil || include || next != 7 {
		t.Fatalf("expected candidate resolve flow result helper to return error with include=false and preserved index 7")
	}
	if _, _, _, err := stage2ResolveFastTargetSharedPeerTopKCandidateResolveFlowDecision(stage2BuildFastTargetSharedPeerTopKCandidateResolveFlowDecisionContext(stage2BuildFastTargetSharedPeerTopKCandidateResolveDecisionContext(errDecisionCtx))); err == nil {
		t.Fatalf("expected unified candidate resolve flow helper to return similarity-eval error")
	}
}

func TestStage2ShouldPushSharedPeerTopKCandidate(t *testing.T) {
	if !stage2ShouldPushSharedPeerTopKCandidate(0, 3) {
		t.Fatalf("expected candidate push while heap has remaining capacity")
	}
	if stage2ShouldPushSharedPeerTopKCandidate(3, 3) {
		t.Fatalf("expected candidate push disabled at heap capacity")
	}
}

func TestStage2ShouldReplaceSharedPeerTopKRoot(t *testing.T) {
	better := fastTargetSharedPeerRankedRow{score: 10, inputIndex: 1}
	worse := fastTargetSharedPeerRankedRow{score: 5, inputIndex: 2}
	if !stage2ShouldReplaceSharedPeerTopKRoot(better, worse, true) {
		t.Fatalf("expected better descending candidate to replace root")
	}
	if stage2ShouldReplaceSharedPeerTopKRoot(worse, better, true) {
		t.Fatalf("expected worse descending candidate to avoid replacing root")
	}
}

func TestStage2ApplySharedPeerTopKCandidate(t *testing.T) {
	top := &fastTargetSharedPeerTopKHeap{descending: true, rows: make([]fastTargetSharedPeerRankedRow, 0, 2)}
	stage2ApplySharedPeerTopKCandidate(top, fastTargetSharedPeerRankedRow{row: Row{"id": "a"}, score: 5, inputIndex: 0}, 2)
	stage2ApplySharedPeerTopKCandidate(top, fastTargetSharedPeerRankedRow{row: Row{"id": "b"}, score: 7, inputIndex: 1}, 2)
	if top.Len() != 2 {
		t.Fatalf("expected heap size 2 after two pushes, got %d", top.Len())
	}

	stage2ApplySharedPeerTopKCandidate(top, fastTargetSharedPeerRankedRow{row: Row{"id": "c"}, score: 9, inputIndex: 2}, 2)
	hasC := false
	for _, ranked := range top.rows {
		if ranked.row["id"] == "c" {
			hasC = true
			break
		}
	}
	if !hasC {
		t.Fatalf("expected better candidate to replace heap root when at capacity")
	}

	previousRows := append([]fastTargetSharedPeerRankedRow(nil), top.rows...)
	stage2ApplySharedPeerTopKCandidate(top, fastTargetSharedPeerRankedRow{row: Row{"id": "z"}, score: 1, inputIndex: 3}, 2)
	if top.Len() != 2 {
		t.Fatalf("expected heap size to remain 2 after non-improving candidate")
	}
	matched := 0
	for _, before := range previousRows {
		for _, after := range top.rows {
			if before.row["id"] == after.row["id"] && before.score == after.score {
				matched++
				break
			}
		}
	}
	if matched != len(previousRows) {
		t.Fatalf("expected non-improving candidate to preserve heap contents")
	}
}

func TestStage2SharedPeerTopKCandidateApplyDecisionHelpers(t *testing.T) {
	top := &fastTargetSharedPeerTopKHeap{descending: true, rows: make([]fastTargetSharedPeerRankedRow, 0, 2)}
	candidateA := fastTargetSharedPeerRankedRow{row: Row{"id": "a"}, score: 5, inputIndex: 0}
	decisionCtx := stage2BuildSharedPeerTopKCandidateApplyDecisionContext(top, candidateA, 2)
	if decisionCtx.keep != 2 {
		t.Fatalf("expected candidate-apply decision context keep size 2, got %d", decisionCtx.keep)
	}

	decisionCtx = stage2ResolveSharedPeerTopKCandidatePushDecision(decisionCtx)
	if !decisionCtx.shouldPush {
		t.Fatalf("expected push decision helper to enable push when heap has capacity")
	}
	decisionCtx = stage2ResolveSharedPeerTopKCandidateReplaceDecision(decisionCtx)
	if decisionCtx.shouldReplace {
		t.Fatalf("expected replace decision helper to stay false when push path is active")
	}
	decisionCtx = stage2ResolveSharedPeerTopKCandidateApplyExecutionDecision(decisionCtx)
	if !decisionCtx.pushApplied {
		t.Fatalf("expected apply execution helper to record push-applied path")
	}
	stage2ResolveSharedPeerTopKCandidateApplyFinalizeDecision(decisionCtx)
	if top.Len() != 1 {
		t.Fatalf("expected apply finalize helper to preserve single pushed candidate, got %d", top.Len())
	}

	stage2ResolveSharedPeerTopKCandidateApplyDecision(stage2BuildSharedPeerTopKCandidateApplyDecisionContext(top, fastTargetSharedPeerRankedRow{row: Row{"id": "b"}, score: 7, inputIndex: 1}, 2))
	if top.Len() != 2 {
		t.Fatalf("expected unified apply decision helper to fill heap to capacity, got %d", top.Len())
	}

	replaceCtx := stage2BuildSharedPeerTopKCandidateApplyDecisionContext(top, fastTargetSharedPeerRankedRow{row: Row{"id": "c"}, score: 9, inputIndex: 2}, 2)
	replaceCtx = stage2ResolveSharedPeerTopKCandidatePushDecision(replaceCtx)
	replaceCtx = stage2ResolveSharedPeerTopKCandidateReplaceDecision(replaceCtx)
	if replaceCtx.shouldPush {
		t.Fatalf("expected push decision helper to disable push when heap is full")
	}
	if !replaceCtx.shouldReplace {
		t.Fatalf("expected replace decision helper to enable replacement for better candidate")
	}
	replaceCtx = stage2ResolveSharedPeerTopKCandidateApplyExecutionDecision(replaceCtx)
	if replaceCtx.pushApplied {
		t.Fatalf("expected apply execution helper to keep push-applied=false for replacement path")
	}
	stage2ResolveSharedPeerTopKCandidateApplyFinalizeDecision(replaceCtx)

	hasC := false
	for _, ranked := range top.rows {
		if ranked.row["id"] == "c" {
			hasC = true
			break
		}
	}
	if !hasC {
		t.Fatalf("expected replace apply helper to place replacement candidate in heap")
	}
}

func TestStage2FastTargetSharedPeerTopKRowsDecisionHelpers(t *testing.T) {
	aggs := map[string]*fastTargetSharedPeerAggregate{
		"a": {target: &graph.Vertex{ID: "m1"}, peer: &graph.Vertex{ID: "u1"}, shared: 2, sumAbsDiff: 1},
		"b": {target: &graph.Vertex{ID: "m2"}, peer: &graph.Vertex{ID: "u2"}, shared: 1, sumAbsDiff: 1},
		"c": {target: &graph.Vertex{ID: "m3"}, peer: &graph.Vertex{ID: "u3"}, shared: 0, sumAbsDiff: 0},
	}
	projection := fastTargetSharedPeerProjection{targetKey: "target", peerKey: "peer", sharedCountKey: "shared", avgDiffKey: "avg"}
	withSpec := projectionClauseSpec{}
	topKProjection := fastTargetSharedPeerTopKProjection{targetKey: "outTarget", peerKey: "outPeer", similarityKey: "sim", similarityExpr: "shared"}
	spec := fastTargetSharedPeerTopKSpec{descending: true, skip: 0, limit: 1}

	decisionCtx := stage2BuildFastTargetSharedPeerTopKRowsDecisionContext(aggs, projection, withSpec, topKProjection, spec, context.Background(), nil, Params{"k": "v"}, nil)
	if decisionCtx.keep != 1 {
		t.Fatalf("expected keep size 1, got %d", decisionCtx.keep)
	}
	if decisionCtx.top == nil {
		t.Fatalf("expected decision context heap initialization")
	}
	if _, ok := decisionCtx.params[projectionEvalCtxParam]; !ok {
		t.Fatalf("expected decision context to inject projection evaluation runtime params")
	}

	resolvedCtx, err := stage2ResolveFastTargetSharedPeerTopKRowsCandidates(decisionCtx)
	if err != nil {
		t.Fatalf("expected candidate accumulation helper success, got error: %v", err)
	}
	if resolvedCtx.top.Len() != 1 {
		t.Fatalf("expected candidate accumulation helper to keep one ranked row, got %d", resolvedCtx.top.Len())
	}
	if resolvedCtx.inputIndex != 2 {
		t.Fatalf("expected candidate accumulation helper to advance input index to 2, got %d", resolvedCtx.inputIndex)
	}

	rows, err := stage2ResolveFastTargetSharedPeerTopKRowsDecision(decisionCtx)
	if err != nil {
		t.Fatalf("expected top-k rows decision resolver success, got error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected top-k rows decision resolver to return one row, got %d", len(rows))
	}
	if rows[0]["outTarget"].(*graph.Vertex).ID != "m1" {
		t.Fatalf("expected top-k rows decision resolver to keep highest shared candidate")
	}

	emptyRows, err := stage2ResolveFastTargetSharedPeerTopKRowsDecision(stage2BuildFastTargetSharedPeerTopKRowsDecisionContext(aggs, projection, withSpec, topKProjection, fastTargetSharedPeerTopKSpec{descending: true, skip: 0, limit: 0}, context.Background(), nil, Params{}, nil))
	if err != nil {
		t.Fatalf("expected zero-limit top-k rows decision to succeed, got error: %v", err)
	}
	if len(emptyRows) != 0 {
		t.Fatalf("expected zero-limit top-k rows decision to return no rows")
	}
}

func TestStage2FastTargetSharedPeerTopKAggregateDecisionHelpers(t *testing.T) {
	aggs := map[string]*fastTargetSharedPeerAggregate{
		"a": {target: &graph.Vertex{ID: "m1"}, peer: &graph.Vertex{ID: "u1"}, shared: 2, sumAbsDiff: 1},
		"b": {target: &graph.Vertex{ID: "m2"}, peer: &graph.Vertex{ID: "u2"}, shared: 0, sumAbsDiff: 0},
	}
	projection := fastTargetSharedPeerProjection{targetKey: "target", peerKey: "peer", sharedCountKey: "shared", avgDiffKey: "avg"}
	withSpec := projectionClauseSpec{}
	topKProjection := fastTargetSharedPeerTopKProjection{targetKey: "outTarget", peerKey: "outPeer", similarityKey: "sim", similarityExpr: "shared"}
	spec := fastTargetSharedPeerTopKSpec{descending: true, skip: 0, limit: 1}

	rowsDecisionCtx := stage2BuildFastTargetSharedPeerTopKRowsDecisionContext(aggs, projection, withSpec, topKProjection, spec, context.Background(), nil, Params{}, nil)
	aggDecisionCtx := stage2BuildFastTargetSharedPeerTopKAggregateDecisionContext(rowsDecisionCtx, aggs["a"])
	if aggDecisionCtx.agg != aggs["a"] {
		t.Fatalf("expected aggregate decision context to preserve aggregate pointer")
	}

	aggDecisionCtx, err := stage2ResolveFastTargetSharedPeerTopKAggregateCandidateDecision(aggDecisionCtx)
	if err != nil {
		t.Fatalf("expected aggregate-candidate decision helper success, got error: %v", err)
	}
	if !aggDecisionCtx.include || aggDecisionCtx.nextInputIndex != 1 {
		t.Fatalf("expected aggregate-candidate decision helper include=true next=1, got include=%v next=%d", aggDecisionCtx.include, aggDecisionCtx.nextInputIndex)
	}
	if aggDecisionCtx.candidate.score != 2 {
		t.Fatalf("expected aggregate-candidate decision helper score 2, got %v", aggDecisionCtx.candidate.score)
	}

	appliedCtx := stage2ApplyFastTargetSharedPeerTopKAggregateCandidateDecision(aggDecisionCtx)
	if appliedCtx.inputIndex != 1 {
		t.Fatalf("expected apply aggregate decision helper to advance input index to 1, got %d", appliedCtx.inputIndex)
	}
	if appliedCtx.top.Len() != 1 {
		t.Fatalf("expected apply aggregate decision helper to push one candidate, got heap len %d", appliedCtx.top.Len())
	}

	resolvedCtx, err := stage2ResolveFastTargetSharedPeerTopKAggregateDecision(rowsDecisionCtx, aggs["a"])
	if err != nil {
		t.Fatalf("expected aggregate decision resolver success, got error: %v", err)
	}
	if resolvedCtx.inputIndex != 1 || resolvedCtx.top.Len() != 1 {
		t.Fatalf("expected aggregate decision resolver to apply include candidate, got input=%d heap=%d", resolvedCtx.inputIndex, resolvedCtx.top.Len())
	}

	excludedCtx, err := stage2ResolveFastTargetSharedPeerTopKAggregateDecision(resolvedCtx, aggs["b"])
	if err != nil {
		t.Fatalf("expected aggregate decision resolver exclusion path success, got error: %v", err)
	}
	if excludedCtx.inputIndex != 1 {
		t.Fatalf("expected excluded aggregate to preserve input index 1, got %d", excludedCtx.inputIndex)
	}
	if excludedCtx.top.Len() != 1 {
		t.Fatalf("expected excluded aggregate to preserve heap length 1, got %d", excludedCtx.top.Len())
	}

	resolveCtx := stage2BuildFastTargetSharedPeerTopKAggregateCandidateResolveDecisionContext(stage2BuildFastTargetSharedPeerTopKAggregateDecisionContext(rowsDecisionCtx, aggs["a"]))
	resolveCtx = stage2ResolveFastTargetSharedPeerTopKAggregateEligibilityDecision(resolveCtx)
	if !resolveCtx.eligible {
		t.Fatalf("expected aggregate eligibility helper to mark shared aggregate as eligible")
	}
	resolveCtx, err = stage2ResolveFastTargetSharedPeerTopKAggregateCandidateValuesDecision(resolveCtx)
	if err != nil {
		t.Fatalf("expected aggregate candidate-values helper success, got error: %v", err)
	}
	if !resolveCtx.include || resolveCtx.nextInputIndex != 1 {
		t.Fatalf("expected aggregate candidate-values helper include=true next=1, got include=%v next=%d", resolveCtx.include, resolveCtx.nextInputIndex)
	}
	finalizedDecisionCtx := stage2ResolveFastTargetSharedPeerTopKAggregateCandidateFinalizeDecision(resolveCtx)
	if !finalizedDecisionCtx.include || finalizedDecisionCtx.nextInputIndex != 1 {
		t.Fatalf("expected aggregate candidate-finalize helper to preserve include=true next=1, got include=%v next=%d", finalizedDecisionCtx.include, finalizedDecisionCtx.nextInputIndex)
	}

	excludedResolveCtx := stage2BuildFastTargetSharedPeerTopKAggregateCandidateResolveDecisionContext(stage2BuildFastTargetSharedPeerTopKAggregateDecisionContext(rowsDecisionCtx, aggs["b"]))
	excludedResolveCtx = stage2ResolveFastTargetSharedPeerTopKAggregateEligibilityDecision(excludedResolveCtx)
	if excludedResolveCtx.eligible {
		t.Fatalf("expected aggregate eligibility helper to mark zero-shared aggregate as ineligible")
	}
	excludedResolveCtx, err = stage2ResolveFastTargetSharedPeerTopKAggregateCandidateValuesDecision(excludedResolveCtx)
	if err != nil {
		t.Fatalf("expected aggregate candidate-values helper exclusion path success, got error: %v", err)
	}
	if excludedResolveCtx.include {
		t.Fatalf("expected aggregate candidate-values helper to keep excluded aggregate include=false")
	}
	if stage2ResolveFastTargetSharedPeerTopKAggregateCandidateFinalizeDecision(excludedResolveCtx).include {
		t.Fatalf("expected aggregate candidate-finalize helper to keep excluded aggregate include=false")
	}

	resolvedAggregateDecisionCtx, err := stage2ResolveFastTargetSharedPeerTopKAggregateCandidateResolveDecision(resolveCtx)
	if err != nil {
		t.Fatalf("expected aggregate candidate-resolve helper success, got error: %v", err)
	}
	if !resolvedAggregateDecisionCtx.include || resolvedAggregateDecisionCtx.nextInputIndex != 1 {
		t.Fatalf("expected aggregate candidate-resolve helper include=true next=1, got include=%v next=%d", resolvedAggregateDecisionCtx.include, resolvedAggregateDecisionCtx.nextInputIndex)
	}

	aggCandidateFlowCtx := stage2BuildFastTargetSharedPeerTopKAggregateCandidateResolveFlowDecisionContext(resolveCtx)
	aggCandidateFlowCtx = stage2ResolveFastTargetSharedPeerTopKAggregateCandidateEligibilityFlowDecision(aggCandidateFlowCtx)
	aggCandidateFlowCtx = stage2ResolveFastTargetSharedPeerTopKAggregateCandidateValuesFlowDecision(aggCandidateFlowCtx)
	if aggCandidateFlowCtx.hasError {
		t.Fatalf("expected aggregate-candidate values flow helper success, got error: %v", aggCandidateFlowCtx.err)
	}
	aggCandidateFlowCtx = stage2ResolveFastTargetSharedPeerTopKAggregateCandidateFinalizeFlowDecision(aggCandidateFlowCtx)
	if !aggCandidateFlowCtx.hasResolvedDecision {
		t.Fatalf("expected aggregate-candidate finalize flow helper to produce resolved decision")
	}
	flowResolvedAggDecisionCtx, err := stage2ResolveFastTargetSharedPeerTopKAggregateCandidateResolveFlowResultDecision(aggCandidateFlowCtx)
	if err != nil {
		t.Fatalf("expected aggregate-candidate resolve flow result helper success, got error: %v", err)
	}
	if !flowResolvedAggDecisionCtx.include || flowResolvedAggDecisionCtx.nextInputIndex != 1 {
		t.Fatalf("expected aggregate-candidate resolve flow result helper include=true next=1, got include=%v next=%d", flowResolvedAggDecisionCtx.include, flowResolvedAggDecisionCtx.nextInputIndex)
	}
	flowResolvedAggDecisionCtx, err = stage2ResolveFastTargetSharedPeerTopKAggregateCandidateResolveFlowDecision(stage2BuildFastTargetSharedPeerTopKAggregateCandidateResolveFlowDecisionContext(resolveCtx))
	if err != nil {
		t.Fatalf("expected unified aggregate-candidate resolve flow helper success, got error: %v", err)
	}
	if !flowResolvedAggDecisionCtx.include || flowResolvedAggDecisionCtx.nextInputIndex != 1 {
		t.Fatalf("expected unified aggregate-candidate resolve flow helper include=true next=1, got include=%v next=%d", flowResolvedAggDecisionCtx.include, flowResolvedAggDecisionCtx.nextInputIndex)
	}

	applyDecisionCtx := stage2BuildFastTargetSharedPeerTopKAggregateApplyDecisionContext(aggDecisionCtx)
	applyDecisionCtx = stage2ResolveFastTargetSharedPeerTopKAggregateApplyGateDecision(applyDecisionCtx)
	if !applyDecisionCtx.shouldApply {
		t.Fatalf("expected aggregate apply-gate helper to enable apply for included candidate")
	}
	if !stage2ResolveFastTargetSharedPeerTopKAggregateApplyRowsContextGateDecision(applyDecisionCtx) {
		t.Fatalf("expected aggregate apply rows-context gate helper to return true for included candidate")
	}
	applyDecisionCtx = stage2ResolveFastTargetSharedPeerTopKAggregateApplyRowsContextDecision(applyDecisionCtx)
	if !applyDecisionCtx.rowsCtxPrepared || applyDecisionCtx.updatedRowsCtx.inputIndex != 1 {
		t.Fatalf("expected aggregate apply-rows-context helper to prepare rows context with next input index 1")
	}
	rowsContextResultCtx := stage2ResolveFastTargetSharedPeerTopKAggregateApplyRowsContextResultDecision(stage2BuildFastTargetSharedPeerTopKAggregateApplyDecisionContext(aggDecisionCtx), true)
	if !rowsContextResultCtx.rowsCtxPrepared || rowsContextResultCtx.updatedRowsCtx.inputIndex != 1 {
		t.Fatalf("expected aggregate apply rows-context result helper to prepare rows context when gate is true")
	}
	if !stage2ResolveFastTargetSharedPeerTopKAggregateApplyCandidateGateDecision(applyDecisionCtx) {
		t.Fatalf("expected aggregate apply candidate gate helper to return true when rows context is prepared")
	}
	applyDecisionCtx = stage2ApplyFastTargetSharedPeerTopKAggregateApplyCandidateDecision(applyDecisionCtx)
	if !applyDecisionCtx.candidateApplied || applyDecisionCtx.updatedRowsCtx.top.Len() != 1 {
		t.Fatalf("expected aggregate apply-candidate helper to apply candidate into heap")
	}
	candidateResultCtx := stage2ResolveFastTargetSharedPeerTopKAggregateApplyCandidateResultDecision(rowsContextResultCtx, true)
	if !candidateResultCtx.candidateApplied || candidateResultCtx.updatedRowsCtx.top.Len() != 1 {
		t.Fatalf("expected aggregate apply candidate result helper to apply candidate when gate is true")
	}
	if !stage2ResolveFastTargetSharedPeerTopKAggregateApplyFinalizeGateDecision(applyDecisionCtx) {
		t.Fatalf("expected aggregate apply finalize gate helper to return true for included candidate")
	}
	finalRowsCtx := stage2ResolveFastTargetSharedPeerTopKAggregateApplyFinalizeDecision(applyDecisionCtx)
	if finalRowsCtx.inputIndex != 1 || finalRowsCtx.top.Len() != 1 {
		t.Fatalf("expected aggregate apply-finalize helper to return updated rows context, got input=%d heap=%d", finalRowsCtx.inputIndex, finalRowsCtx.top.Len())
	}
	finalRowsResultCtx := stage2ResolveFastTargetSharedPeerTopKAggregateApplyFinalizeResultDecision(applyDecisionCtx, true)
	if finalRowsResultCtx.inputIndex != 1 || finalRowsResultCtx.top.Len() != 1 {
		t.Fatalf("expected aggregate apply finalize result helper to return updated rows context when gate is true")
	}

	resolvedRowsCtx := stage2ResolveFastTargetSharedPeerTopKAggregateApplyDecision(stage2BuildFastTargetSharedPeerTopKAggregateApplyDecisionContext(aggDecisionCtx))
	if resolvedRowsCtx.inputIndex != 1 || resolvedRowsCtx.top.Len() != 1 {
		t.Fatalf("expected unified aggregate-apply decision helper to apply candidate, got input=%d heap=%d", resolvedRowsCtx.inputIndex, resolvedRowsCtx.top.Len())
	}

	excludedApplyDecisionCtx := stage2BuildFastTargetSharedPeerTopKAggregateApplyDecisionContext(stage2BuildFastTargetSharedPeerTopKAggregateDecisionContext(rowsDecisionCtx, aggs["b"]))
	excludedApplyDecisionCtx = stage2ResolveFastTargetSharedPeerTopKAggregateApplyGateDecision(excludedApplyDecisionCtx)
	if excludedApplyDecisionCtx.shouldApply {
		t.Fatalf("expected aggregate apply-gate helper to disable apply for excluded candidate")
	}
	if stage2ResolveFastTargetSharedPeerTopKAggregateApplyRowsContextGateDecision(excludedApplyDecisionCtx) {
		t.Fatalf("expected aggregate apply rows-context gate helper to return false for excluded candidate")
	}
	excludedApplyDecisionCtx = stage2ResolveFastTargetSharedPeerTopKAggregateApplyRowsContextDecision(excludedApplyDecisionCtx)
	if excludedApplyDecisionCtx.rowsCtxPrepared {
		t.Fatalf("expected aggregate apply-rows-context helper to skip preparation for excluded candidate")
	}
	excludedRowsContextResultCtx := stage2ResolveFastTargetSharedPeerTopKAggregateApplyRowsContextResultDecision(excludedApplyDecisionCtx, false)
	if excludedRowsContextResultCtx.rowsCtxPrepared {
		t.Fatalf("expected aggregate apply rows-context result helper to keep rowsCtxPrepared=false when gate is false")
	}
	if stage2ResolveFastTargetSharedPeerTopKAggregateApplyCandidateGateDecision(excludedApplyDecisionCtx) {
		t.Fatalf("expected aggregate apply candidate gate helper to return false when rows context is not prepared")
	}
	excludedApplyDecisionCtx = stage2ApplyFastTargetSharedPeerTopKAggregateApplyCandidateDecision(excludedApplyDecisionCtx)
	if excludedApplyDecisionCtx.candidateApplied {
		t.Fatalf("expected aggregate apply-candidate helper to skip apply for excluded candidate")
	}
	excludedCandidateResultCtx := stage2ResolveFastTargetSharedPeerTopKAggregateApplyCandidateResultDecision(excludedRowsContextResultCtx, false)
	if excludedCandidateResultCtx.candidateApplied {
		t.Fatalf("expected aggregate apply candidate result helper to keep candidateApplied=false when gate is false")
	}
	if stage2ResolveFastTargetSharedPeerTopKAggregateApplyFinalizeGateDecision(excludedApplyDecisionCtx) {
		t.Fatalf("expected aggregate apply finalize gate helper to return false for excluded candidate")
	}
	excludedRowsCtx := stage2ResolveFastTargetSharedPeerTopKAggregateApplyFinalizeDecision(excludedApplyDecisionCtx)
	if excludedRowsCtx.inputIndex != rowsDecisionCtx.inputIndex {
		t.Fatalf("expected aggregate apply-finalize helper exclusion path to preserve input index %d, got %d", rowsDecisionCtx.inputIndex, excludedRowsCtx.inputIndex)
	}
	excludedRowsResultCtx := stage2ResolveFastTargetSharedPeerTopKAggregateApplyFinalizeResultDecision(excludedApplyDecisionCtx, false)
	if excludedRowsResultCtx.inputIndex != rowsDecisionCtx.inputIndex {
		t.Fatalf("expected aggregate apply finalize result helper to preserve input index for excluded candidate")
	}

	flowCtx := stage2BuildFastTargetSharedPeerTopKAggregateResolveFlowDecisionContext(rowsDecisionCtx, aggs["a"])
	flowCtx = stage2ResolveFastTargetSharedPeerTopKAggregateCandidateFlowDecision(flowCtx)
	if flowCtx.hasError {
		t.Fatalf("expected aggregate-resolve candidate flow helper success, got error: %v", flowCtx.err)
	}
	flowCtx = stage2ResolveFastTargetSharedPeerTopKAggregateApplyFlowDecision(flowCtx)
	if !flowCtx.hasResolvedRowsCtx || flowCtx.resolvedRowsCtx.inputIndex != 1 {
		t.Fatalf("expected aggregate-resolve apply flow helper to produce updated rows ctx with input index 1")
	}
	flowResultRowsCtx, err := stage2ResolveFastTargetSharedPeerTopKAggregateResolveFlowResultDecision(flowCtx)
	if err != nil {
		t.Fatalf("expected aggregate-resolve result flow helper success, got error: %v", err)
	}
	if flowResultRowsCtx.inputIndex != 1 {
		t.Fatalf("expected aggregate-resolve result flow helper to return resolved rows ctx input index 1, got %d", flowResultRowsCtx.inputIndex)
	}

	flowResolvedRowsCtx, err := stage2ResolveFastTargetSharedPeerTopKAggregateResolveFlowDecision(stage2BuildFastTargetSharedPeerTopKAggregateResolveFlowDecisionContext(rowsDecisionCtx, aggs["a"]))
	if err != nil {
		t.Fatalf("expected unified aggregate-resolve flow helper success, got error: %v", err)
	}
	if flowResolvedRowsCtx.inputIndex != 1 || flowResolvedRowsCtx.top.Len() != 1 {
		t.Fatalf("expected unified aggregate-resolve flow helper to apply candidate, got input=%d heap=%d", flowResolvedRowsCtx.inputIndex, flowResolvedRowsCtx.top.Len())
	}

	errRowsDecisionCtx := stage2BuildFastTargetSharedPeerTopKRowsDecisionContext(
		aggs,
		projection,
		withSpec,
		fastTargetSharedPeerTopKProjection{targetKey: "outTarget", peerKey: "outPeer", similarityKey: "sim", similarityExpr: "("},
		spec,
		context.Background(),
		nil,
		Params{},
		nil,
	)
	errFlowCtx := stage2BuildFastTargetSharedPeerTopKAggregateResolveFlowDecisionContext(errRowsDecisionCtx, aggs["a"])
	errFlowCtx = stage2ResolveFastTargetSharedPeerTopKAggregateCandidateFlowDecision(errFlowCtx)
	if !errFlowCtx.hasError {
		t.Fatalf("expected aggregate-resolve candidate flow helper to capture candidate error")
	}
	errFlowCtx = stage2ResolveFastTargetSharedPeerTopKAggregateApplyFlowDecision(errFlowCtx)
	if errFlowCtx.hasResolvedRowsCtx {
		t.Fatalf("expected aggregate-resolve apply flow helper to skip applying when flow has error")
	}
	if _, err := stage2ResolveFastTargetSharedPeerTopKAggregateResolveFlowResultDecision(errFlowCtx); err == nil {
		t.Fatalf("expected aggregate-resolve result flow helper to return candidate error")
	}
	if _, err := stage2ResolveFastTargetSharedPeerTopKAggregateResolveFlowDecision(stage2BuildFastTargetSharedPeerTopKAggregateResolveFlowDecisionContext(errRowsDecisionCtx, aggs["a"])); err == nil {
		t.Fatalf("expected unified aggregate-resolve flow helper to return candidate error")
	}

	// Additional checks for exclusion path
	excludedAggCandidateFlowCtx := stage2BuildFastTargetSharedPeerTopKAggregateCandidateResolveFlowDecisionContext(excludedResolveCtx)
	excludedAggCandidateFlowCtx = stage2ResolveFastTargetSharedPeerTopKAggregateCandidateEligibilityFlowDecision(excludedAggCandidateFlowCtx)
	if excludedAggCandidateFlowCtx.hasError {
		t.Fatalf("expected aggregate-candidate eligibility flow helper success, got error: %v", excludedAggCandidateFlowCtx.err)
	}

	excludedAggCandidateFlowCtx = stage2ResolveFastTargetSharedPeerTopKAggregateCandidateValuesFlowDecision(excludedAggCandidateFlowCtx)
	if excludedAggCandidateFlowCtx.hasError {
		t.Fatalf("expected aggregate-candidate values flow helper success, got error: %v", excludedAggCandidateFlowCtx.err)
	}

	excludedAggCandidateFlowCtx = stage2ResolveFastTargetSharedPeerTopKAggregateCandidateFinalizeFlowDecision(excludedAggCandidateFlowCtx)
	excludedFlowResolvedAggDecisionCtx, err := stage2ResolveFastTargetSharedPeerTopKAggregateCandidateResolveFlowResultDecision(excludedAggCandidateFlowCtx)
	if err != nil {
		t.Fatalf("expected aggregate-candidate resolve flow result exclusion path success, got error: %v", err)
	}
	if excludedFlowResolvedAggDecisionCtx.include {
		t.Fatalf("expected aggregate-candidate resolve flow result helper to preserve include=false for ineligible aggregate")
	}

	// Additional checks for error handling
	errAggResolveCtx := stage2BuildFastTargetSharedPeerTopKAggregateCandidateResolveDecisionContext(stage2BuildFastTargetSharedPeerTopKAggregateDecisionContext(errRowsDecisionCtx, aggs["a"]))
	errAggCandidateFlowCtx := stage2BuildFastTargetSharedPeerTopKAggregateCandidateResolveFlowDecisionContext(errAggResolveCtx)
	errAggCandidateFlowCtx = stage2ResolveFastTargetSharedPeerTopKAggregateCandidateEligibilityFlowDecision(errAggCandidateFlowCtx)
	errAggCandidateFlowCtx = stage2ResolveFastTargetSharedPeerTopKAggregateCandidateValuesFlowDecision(errAggCandidateFlowCtx)
	if !errAggCandidateFlowCtx.hasError {
		t.Fatalf("expected aggregate-candidate values flow helper to capture candidate error")
	}

	errAggCandidateFlowCtx = stage2ResolveFastTargetSharedPeerTopKAggregateCandidateFinalizeFlowDecision(errAggCandidateFlowCtx)
	if _, err := stage2ResolveFastTargetSharedPeerTopKAggregateCandidateResolveFlowResultDecision(errAggCandidateFlowCtx); err == nil {
		t.Fatalf("expected aggregate-candidate resolve flow result helper to return candidate error")
	}

	if _, err := stage2ResolveFastTargetSharedPeerTopKAggregateCandidateResolveFlowDecision(stage2BuildFastTargetSharedPeerTopKAggregateCandidateResolveFlowDecisionContext(errAggResolveCtx)); err == nil {
		t.Fatalf("expected unified aggregate-candidate resolve flow helper to return candidate error")
	}
}

func TestStage2FastTargetSharedPeerTopKRowsCandidatesResolveDecisionHelpers(t *testing.T) {
	aggs := map[string]*fastTargetSharedPeerAggregate{
		"a": {target: &graph.Vertex{ID: "m2"}, peer: &graph.Vertex{ID: "u2"}, shared: 1, sumAbsDiff: 1},
		"b": {target: &graph.Vertex{ID: "m1"}, peer: &graph.Vertex{ID: "u1"}, shared: 2, sumAbsDiff: 1},
		"c": {target: &graph.Vertex{ID: "m3"}, peer: &graph.Vertex{ID: "u3"}, shared: 0, sumAbsDiff: 0},
	}
	projection := fastTargetSharedPeerProjection{targetKey: "target", peerKey: "peer", sharedCountKey: "shared", avgDiffKey: "avg"}
	withSpec := projectionClauseSpec{}
	topKProjection := fastTargetSharedPeerTopKProjection{targetKey: "outTarget", peerKey: "outPeer", similarityKey: "sim", similarityExpr: "shared"}
	spec := fastTargetSharedPeerTopKSpec{descending: true, skip: 0, limit: 2}

	rowsDecisionCtx := stage2BuildFastTargetSharedPeerTopKRowsDecisionContext(aggs, projection, withSpec, topKProjection, spec, context.Background(), nil, Params{}, nil)
	resolveCtx := stage2BuildFastTargetSharedPeerTopKRowsCandidatesResolveDecisionContext(rowsDecisionCtx)
	if len(resolveCtx.sortedAggs) != 3 {
		t.Fatalf("expected rows-candidates resolve context to collect three aggregates, got %d", len(resolveCtx.sortedAggs))
	}

	iterCtx, err := stage2ResolveFastTargetSharedPeerTopKRowsCandidateIterationDecision(resolveCtx, resolveCtx.sortedAggs[0])
	if err != nil {
		t.Fatalf("expected rows-candidates iteration helper success, got error: %v", err)
	}
	if iterCtx.rowsDecisionCtx.inputIndex != 1 {
		t.Fatalf("expected rows-candidates iteration helper to advance input index to 1, got %d", iterCtx.rowsDecisionCtx.inputIndex)
	}

	iterFlowCtx := stage2BuildFastTargetSharedPeerTopKRowsCandidateIterationResolveFlowDecisionContext(resolveCtx, resolveCtx.sortedAggs[0])
	iterFlowCtx = stage2ResolveFastTargetSharedPeerTopKRowsCandidateIterationAggregateFlowDecision(iterFlowCtx)
	if iterFlowCtx.hasError {
		t.Fatalf("expected rows-candidate-iteration aggregate flow helper success, got error: %v", iterFlowCtx.err)
	}
	iterAggSuccessCtx := stage2ResolveFastTargetSharedPeerTopKRowsCandidateIterationAggregateFlowSuccessResultDecision(stage2BuildFastTargetSharedPeerTopKRowsCandidateIterationResolveFlowDecisionContext(resolveCtx, resolveCtx.sortedAggs[0]), iterFlowCtx.resolvedRowsDecisionCtx)
	if !iterAggSuccessCtx.hasResolvedRowsDecisionCtx || iterAggSuccessCtx.resolvedRowsDecisionCtx.inputIndex != 1 {
		t.Fatalf("expected rows-candidate-iteration aggregate flow success result helper to set resolved rows ctx")
	}
	if !iterFlowCtx.hasResolvedRowsDecisionCtx || iterFlowCtx.resolvedRowsDecisionCtx.inputIndex != 1 {
		t.Fatalf("expected rows-candidate-iteration aggregate flow helper to resolve rows ctx with input index 1")
	}
	if !stage2ResolveFastTargetSharedPeerTopKRowsCandidateIterationApplyGateDecision(iterFlowCtx) {
		t.Fatalf("expected rows-candidate-iteration apply gate helper to return true when resolved rows ctx is available")
	}
	iterApplyResultCtx := stage2ResolveFastTargetSharedPeerTopKRowsCandidateIterationApplyResultDecision(iterFlowCtx, true)
	if iterApplyResultCtx.resolveCtx.rowsDecisionCtx.inputIndex != 1 {
		t.Fatalf("expected rows-candidate-iteration apply result helper to apply resolved rows ctx when gate is true")
	}
	iterFlowCtx = stage2ResolveFastTargetSharedPeerTopKRowsCandidateIterationApplyFlowDecision(iterFlowCtx)
	if iterFlowCtx.resolveCtx.rowsDecisionCtx.inputIndex != 1 {
		t.Fatalf("expected rows-candidate-iteration apply flow helper to update rows ctx input index to 1, got %d", iterFlowCtx.resolveCtx.rowsDecisionCtx.inputIndex)
	}
	if stage2ResolveFastTargetSharedPeerTopKRowsCandidateIterationResolveFlowResultGateDecision(iterFlowCtx) {
		t.Fatalf("expected rows-candidate-iteration resolve flow result gate helper to return false on success path")
	}
	iterFlowResultResultCtx, err := stage2ResolveFastTargetSharedPeerTopKRowsCandidateIterationResolveFlowResultResultDecision(iterFlowCtx, false)
	if err != nil {
		t.Fatalf("expected rows-candidate-iteration resolve flow result result helper success, got error: %v", err)
	}
	if iterFlowSuccessResultCtx, err := stage2ResolveFastTargetSharedPeerTopKRowsCandidateIterationResolveFlowSuccessResultDecision(iterFlowCtx); err != nil || iterFlowSuccessResultCtx.rowsDecisionCtx.inputIndex != 1 {
		t.Fatalf("expected rows-candidate-iteration resolve flow success result helper to preserve rows ctx")
	}
	if iterFlowResultResultCtx.rowsDecisionCtx.inputIndex != 1 {
		t.Fatalf("expected rows-candidate-iteration resolve flow result result helper to preserve resolved rows ctx on success")
	}
	iterFlowResultCtx, err := stage2ResolveFastTargetSharedPeerTopKRowsCandidateIterationResolveFlowResultDecision(iterFlowCtx)
	if err != nil {
		t.Fatalf("expected rows-candidate-iteration result flow helper success, got error: %v", err)
	}
	if iterFlowResultCtx.rowsDecisionCtx.inputIndex != 1 {
		t.Fatalf("expected rows-candidate-iteration result flow helper to return rows ctx input index 1, got %d", iterFlowResultCtx.rowsDecisionCtx.inputIndex)
	}

	iterFlowResolveCtx, err := stage2ResolveFastTargetSharedPeerTopKRowsCandidateIterationResolveFlowDecision(stage2BuildFastTargetSharedPeerTopKRowsCandidateIterationResolveFlowDecisionContext(resolveCtx, resolveCtx.sortedAggs[0]))
	if err != nil {
		t.Fatalf("expected unified rows-candidate-iteration resolve flow helper success, got error: %v", err)
	}
	if iterFlowResolveCtx.rowsDecisionCtx.inputIndex != 1 {
		t.Fatalf("expected unified rows-candidate-iteration resolve flow helper to update input index to 1, got %d", iterFlowResolveCtx.rowsDecisionCtx.inputIndex)
	}

	resolvedRowsDecisionCtx, err := stage2ResolveFastTargetSharedPeerTopKRowsCandidatesResolveDecision(stage2BuildFastTargetSharedPeerTopKRowsCandidatesResolveDecisionContext(rowsDecisionCtx))
	if err != nil {
		t.Fatalf("expected rows-candidates resolve helper success, got error: %v", err)
	}
	if resolvedRowsDecisionCtx.inputIndex != 2 {
		t.Fatalf("expected rows-candidates resolve helper to process two included aggregates, got input index %d", resolvedRowsDecisionCtx.inputIndex)
	}
	if resolvedRowsDecisionCtx.top.Len() != 2 {
		t.Fatalf("expected rows-candidates resolve helper to keep two ranked candidates, got heap len %d", resolvedRowsDecisionCtx.top.Len())
	}

	flowCtx := stage2BuildFastTargetSharedPeerTopKRowsCandidatesResolveFlowDecisionContext(resolveCtx)
	if !stage2ResolveFastTargetSharedPeerTopKRowsCandidatesIterationGateDecision(flowCtx) {
		t.Fatalf("expected rows-candidates iteration gate helper to return true when flow has no error")
	}
	gateBlockedFlowCtx := flowCtx
	gateBlockedFlowCtx.hasError = true
	if stage2ResolveFastTargetSharedPeerTopKRowsCandidatesIterationGateDecision(gateBlockedFlowCtx) {
		t.Fatalf("expected rows-candidates iteration gate helper to return false when flow already has error")
	}
	flowCtx = stage2ResolveFastTargetSharedPeerTopKRowsCandidatesIterationFlowDecision(flowCtx, resolveCtx.sortedAggs[0])
	if flowCtx.hasError {
		t.Fatalf("expected rows-candidates iteration flow helper success, got error: %v", flowCtx.err)
	}
	if flowCtx.resolveCtx.rowsDecisionCtx.inputIndex != 1 {
		t.Fatalf("expected rows-candidates iteration flow helper to advance input index to 1, got %d", flowCtx.resolveCtx.rowsDecisionCtx.inputIndex)
	}
	flowResultCtx := stage2ResolveFastTargetSharedPeerTopKRowsCandidatesIterationResultDecision(stage2BuildFastTargetSharedPeerTopKRowsCandidatesResolveFlowDecisionContext(resolveCtx), iterCtx, nil)
	if flowResultCtx.hasError || flowResultCtx.resolveCtx.rowsDecisionCtx.inputIndex != 1 {
		t.Fatalf("expected rows-candidates iteration result helper to apply updated context when no error")
	}
	flowSuccessResultCtx := stage2ResolveFastTargetSharedPeerTopKRowsCandidatesIterationSuccessResultDecision(stage2BuildFastTargetSharedPeerTopKRowsCandidatesResolveFlowDecisionContext(resolveCtx), iterCtx)
	if flowSuccessResultCtx.hasError || flowSuccessResultCtx.resolveCtx.rowsDecisionCtx.inputIndex != 1 {
		t.Fatalf("expected rows-candidates iteration success result helper to apply updated context")
	}

	flowResolvedRowsDecisionCtx, err := stage2ResolveFastTargetSharedPeerTopKRowsCandidatesResolveFlowDecision(stage2BuildFastTargetSharedPeerTopKRowsCandidatesResolveFlowDecisionContext(resolveCtx))
	if err != nil {
		t.Fatalf("expected unified rows-candidates resolve flow helper success, got error: %v", err)
	}
	if flowResolvedRowsDecisionCtx.inputIndex != 2 || flowResolvedRowsDecisionCtx.top.Len() != 2 {
		t.Fatalf("expected unified rows-candidates resolve flow helper to resolve two candidates, got input=%d heap=%d", flowResolvedRowsDecisionCtx.inputIndex, flowResolvedRowsDecisionCtx.top.Len())
	}
	if stage2ResolveFastTargetSharedPeerTopKRowsCandidatesResolveFlowResultGateDecision(flowCtx) {
		t.Fatalf("expected rows-candidates resolve flow result gate helper to return false on success path")
	}
	flowResultResultRowsDecisionCtx, err := stage2ResolveFastTargetSharedPeerTopKRowsCandidatesResolveFlowResultResultDecision(flowCtx, false)
	if err != nil {
		t.Fatalf("expected rows-candidates resolve flow result result helper success, got error: %v", err)
	}
	if flowSuccessRowsDecisionCtx, err := stage2ResolveFastTargetSharedPeerTopKRowsCandidatesResolveFlowSuccessResultDecision(flowCtx); err != nil || flowSuccessRowsDecisionCtx.inputIndex != 1 {
		t.Fatalf("expected rows-candidates resolve flow success result helper to preserve rows ctx")
	}
	if flowResultResultRowsDecisionCtx.inputIndex != 1 {
		t.Fatalf("expected rows-candidates resolve flow result result helper to preserve updated rows ctx input index 1 after single iteration, got %d", flowResultResultRowsDecisionCtx.inputIndex)
	}
	flowResultRowsDecisionCtx, err := stage2ResolveFastTargetSharedPeerTopKRowsCandidatesResolveFlowResultDecision(flowCtx)
	if err != nil {
		t.Fatalf("expected rows-candidates result flow helper success, got error: %v", err)
	}
	if flowResultRowsDecisionCtx.inputIndex != 1 {
		t.Fatalf("expected rows-candidates result flow helper to return updated rows ctx input index 1 after single iteration, got %d", flowResultRowsDecisionCtx.inputIndex)
	}

	errRowsDecisionCtx := stage2BuildFastTargetSharedPeerTopKRowsDecisionContext(
		aggs,
		projection,
		withSpec,
		fastTargetSharedPeerTopKProjection{targetKey: "outTarget", peerKey: "outPeer", similarityKey: "sim", similarityExpr: "("},
		spec,
		context.Background(),
		nil,
		Params{},
		nil,
	)
	errResolveCtx := stage2BuildFastTargetSharedPeerTopKRowsCandidatesResolveDecisionContext(errRowsDecisionCtx)
	errIterFlowCtx := stage2BuildFastTargetSharedPeerTopKRowsCandidateIterationResolveFlowDecisionContext(errResolveCtx, errResolveCtx.sortedAggs[0])
	errIterFlowCtx = stage2ResolveFastTargetSharedPeerTopKRowsCandidateIterationAggregateFlowDecision(errIterFlowCtx)
	if !errIterFlowCtx.hasError {
		t.Fatalf("expected rows-candidate-iteration aggregate flow helper to capture aggregate-decision error")
	}
	errIterAggResultCtx := stage2ResolveFastTargetSharedPeerTopKRowsCandidateIterationAggregateFlowErrorResultDecision(stage2BuildFastTargetSharedPeerTopKRowsCandidateIterationResolveFlowDecisionContext(errResolveCtx, errResolveCtx.sortedAggs[0]), errors.New("rows-candidate-iteration-aggregate-error"))
	if !errIterAggResultCtx.hasError || errIterAggResultCtx.err == nil {
		t.Fatalf("expected rows-candidate-iteration aggregate flow error result helper to capture error")
	}
	if stage2ResolveFastTargetSharedPeerTopKRowsCandidateIterationApplyGateDecision(errIterFlowCtx) {
		t.Fatalf("expected rows-candidate-iteration apply gate helper to return false when flow has error")
	}
	errApplyResultCtx := stage2ResolveFastTargetSharedPeerTopKRowsCandidateIterationApplyResultDecision(errIterFlowCtx, false)
	if errApplyResultCtx.resolveCtx.rowsDecisionCtx.inputIndex != errResolveCtx.rowsDecisionCtx.inputIndex {
		t.Fatalf("expected rows-candidate-iteration apply result helper to preserve rows ctx when gate is false")
	}
	errIterFlowCtx = stage2ResolveFastTargetSharedPeerTopKRowsCandidateIterationApplyFlowDecision(errIterFlowCtx)
	if errIterFlowCtx.resolveCtx.rowsDecisionCtx.inputIndex != errResolveCtx.rowsDecisionCtx.inputIndex {
		t.Fatalf("expected rows-candidate-iteration apply flow helper to preserve rows ctx on error")
	}
	if !stage2ResolveFastTargetSharedPeerTopKRowsCandidateIterationResolveFlowResultGateDecision(errIterFlowCtx) {
		t.Fatalf("expected rows-candidate-iteration resolve flow result gate helper to return true on error path")
	}
	if _, err := stage2ResolveFastTargetSharedPeerTopKRowsCandidateIterationResolveFlowResultResultDecision(errIterFlowCtx, true); err == nil {
		t.Fatalf("expected rows-candidate-iteration resolve flow result result helper to return aggregate-decision error when gate is true")
	}
	if _, err := stage2ResolveFastTargetSharedPeerTopKRowsCandidateIterationResolveFlowErrorResultDecision(errIterFlowCtx); err == nil {
		t.Fatalf("expected rows-candidate-iteration resolve flow error result helper to return aggregate-decision error")
	}
	if _, err := stage2ResolveFastTargetSharedPeerTopKRowsCandidateIterationResolveFlowResultDecision(errIterFlowCtx); err == nil {
		t.Fatalf("expected rows-candidate-iteration result flow helper to return aggregate-decision error")
	}
	if _, err := stage2ResolveFastTargetSharedPeerTopKRowsCandidateIterationResolveFlowDecision(stage2BuildFastTargetSharedPeerTopKRowsCandidateIterationResolveFlowDecisionContext(errResolveCtx, errResolveCtx.sortedAggs[0])); err == nil {
		t.Fatalf("expected unified rows-candidate-iteration resolve flow helper to return aggregate-decision error")
	}
	errFlowCtx := stage2BuildFastTargetSharedPeerTopKRowsCandidatesResolveFlowDecisionContext(errResolveCtx)
	errResultFlowCtx := stage2ResolveFastTargetSharedPeerTopKRowsCandidatesIterationResultDecision(errFlowCtx, errResolveCtx, errors.New("rows-candidates-iteration-error"))
	if !errResultFlowCtx.hasError || errResultFlowCtx.err == nil {
		t.Fatalf("expected rows-candidates iteration result helper to capture iteration error")
	}
	errIterationResultCtx := stage2ResolveFastTargetSharedPeerTopKRowsCandidatesIterationErrorResultDecision(errFlowCtx, errors.New("rows-candidates-iteration-error-helper"))
	if !errIterationResultCtx.hasError || errIterationResultCtx.err == nil {
		t.Fatalf("expected rows-candidates iteration error result helper to capture iteration error")
	}
	errFlowCtx = stage2ResolveFastTargetSharedPeerTopKRowsCandidatesIterationFlowDecision(errFlowCtx, errResolveCtx.sortedAggs[0])
	if !errFlowCtx.hasError {
		t.Fatalf("expected rows-candidates iteration flow helper to capture candidate error")
	}
	if !stage2ResolveFastTargetSharedPeerTopKRowsCandidatesResolveFlowResultGateDecision(errFlowCtx) {
		t.Fatalf("expected rows-candidates resolve flow result gate helper to return true on error path")
	}
	if _, err := stage2ResolveFastTargetSharedPeerTopKRowsCandidatesResolveFlowResultResultDecision(errFlowCtx, true); err == nil {
		t.Fatalf("expected rows-candidates resolve flow result result helper to return candidate error when gate is true")
	}
	if _, err := stage2ResolveFastTargetSharedPeerTopKRowsCandidatesResolveFlowErrorResultDecision(errFlowCtx); err == nil {
		t.Fatalf("expected rows-candidates resolve flow error result helper to return candidate error")
	}
	if _, err := stage2ResolveFastTargetSharedPeerTopKRowsCandidatesResolveFlowErrorResultDecision(errFlowCtx); err == nil {
		t.Fatalf("expected rows-candidates resolve flow error result helper to return candidate error")
	}
	if _, err := stage2ResolveFastTargetSharedPeerTopKRowsCandidatesResolveFlowResultDecision(errFlowCtx); err == nil {
		t.Fatalf("expected rows-candidates result flow helper to return candidate error")
	}
	if _, err := stage2ResolveFastTargetSharedPeerTopKRowsCandidatesResolveFlowDecision(stage2BuildFastTargetSharedPeerTopKRowsCandidatesResolveFlowDecisionContext(errResolveCtx)); err == nil {
		t.Fatalf("expected unified rows-candidates resolve flow helper to return candidate error")
	}
}

func TestStage2FastTargetSharedPeerTopKRowsResolveDecisionHelpers(t *testing.T) {
	aggs := map[string]*fastTargetSharedPeerAggregate{
		"a": {target: &graph.Vertex{ID: "m1"}, peer: &graph.Vertex{ID: "u1"}, shared: 2, sumAbsDiff: 1},
		"b": {target: &graph.Vertex{ID: "m2"}, peer: &graph.Vertex{ID: "u2"}, shared: 1, sumAbsDiff: 1},
	}
	projection := fastTargetSharedPeerProjection{targetKey: "target", peerKey: "peer", sharedCountKey: "shared", avgDiffKey: "avg"}
	withSpec := projectionClauseSpec{}
	topKProjection := fastTargetSharedPeerTopKProjection{targetKey: "outTarget", peerKey: "outPeer", similarityKey: "sim", similarityExpr: "shared"}

	nonEmptyRowsDecisionCtx := stage2BuildFastTargetSharedPeerTopKRowsDecisionContext(aggs, projection, withSpec, topKProjection, fastTargetSharedPeerTopKSpec{descending: true, skip: 0, limit: 1}, context.Background(), nil, Params{}, nil)
	resolveCtx := stage2BuildFastTargetSharedPeerTopKRowsResolveDecisionContext(nonEmptyRowsDecisionCtx)
	if resolveCtx.rowsDecisionCtx.keep != 1 {
		t.Fatalf("expected rows-resolve decision context to preserve keep size 1, got %d", resolveCtx.rowsDecisionCtx.keep)
	}
	if stage2ResolveFastTargetSharedPeerTopKRowsEmptyLimitGateDecision(resolveCtx) {
		t.Fatalf("expected empty-limit gate helper to return false for non-empty limit")
	}
	resultCtx := stage2ResolveFastTargetSharedPeerTopKRowsEmptyLimitResultDecision(resolveCtx, false)
	if resultCtx.returnEmpty {
		t.Fatalf("expected empty-limit result helper to preserve returnEmpty=false when gate is false")
	}
	preserveEmptyLimitCtx := stage2ResolveFastTargetSharedPeerTopKRowsEmptyLimitPreserveResultDecision(resolveCtx)
	if preserveEmptyLimitCtx.returnEmpty {
		t.Fatalf("expected empty-limit preserve result helper to keep returnEmpty=false")
	}

	resolveCtx = stage2ResolveFastTargetSharedPeerTopKRowsEmptyLimitDecision(resolveCtx)
	if resolveCtx.returnEmpty {
		t.Fatalf("expected non-empty limit decision to keep returnEmpty=false")
	}

	resolveCtx, err := stage2ResolveFastTargetSharedPeerTopKRowsCandidateDecision(resolveCtx)
	if err != nil {
		t.Fatalf("expected rows-candidate decision helper success, got error: %v", err)
	}
	if resolveCtx.rowsDecisionCtx.inputIndex != 2 {
		t.Fatalf("expected rows-candidate decision helper to advance input index to 2, got %d", resolveCtx.rowsDecisionCtx.inputIndex)
	}

	candidateFlowCtx := stage2BuildFastTargetSharedPeerTopKRowsCandidateResolveFlowDecisionContext(stage2BuildFastTargetSharedPeerTopKRowsResolveDecisionContext(nonEmptyRowsDecisionCtx))
	if !stage2ResolveFastTargetSharedPeerTopKRowsCandidateRowsFlowGateDecision(candidateFlowCtx) {
		t.Fatalf("expected rows-candidate rows flow gate helper to return true when returnEmpty=false")
	}
	candidateFlowCtx = stage2ResolveFastTargetSharedPeerTopKRowsCandidateRowsFlowDecision(candidateFlowCtx)
	if candidateFlowCtx.hasError {
		t.Fatalf("expected rows-candidate rows flow helper success, got error: %v", candidateFlowCtx.err)
	}
	if !candidateFlowCtx.hasResolvedRowsDecisionCtx || candidateFlowCtx.resolvedRowsDecisionCtx.inputIndex != 2 {
		t.Fatalf("expected rows-candidate rows flow helper to resolve rows ctx with input index 2")
	}
	rowsFlowResultCtx := stage2ResolveFastTargetSharedPeerTopKRowsCandidateRowsFlowResultDecision(stage2BuildFastTargetSharedPeerTopKRowsCandidateResolveFlowDecisionContext(stage2BuildFastTargetSharedPeerTopKRowsResolveDecisionContext(nonEmptyRowsDecisionCtx)), candidateFlowCtx.resolvedRowsDecisionCtx, nil)
	if rowsFlowResultCtx.hasError || !rowsFlowResultCtx.hasResolvedRowsDecisionCtx {
		t.Fatalf("expected rows-candidate rows flow result helper to set resolved rows context on success")
	}
	rowsFlowSuccessCtx := stage2ResolveFastTargetSharedPeerTopKRowsCandidateRowsFlowSuccessResultDecision(stage2BuildFastTargetSharedPeerTopKRowsCandidateResolveFlowDecisionContext(stage2BuildFastTargetSharedPeerTopKRowsResolveDecisionContext(nonEmptyRowsDecisionCtx)), candidateFlowCtx.resolvedRowsDecisionCtx)
	if rowsFlowSuccessCtx.hasError || !rowsFlowSuccessCtx.hasResolvedRowsDecisionCtx {
		t.Fatalf("expected rows-candidate rows flow success result helper to set resolved rows context")
	}
	if !stage2ResolveFastTargetSharedPeerTopKRowsCandidateApplyFlowGateDecision(candidateFlowCtx) {
		t.Fatalf("expected rows-candidate apply flow gate helper to return true on success path")
	}
	candidateApplyResultCtx := stage2ResolveFastTargetSharedPeerTopKRowsCandidateApplyFlowResultDecision(candidateFlowCtx, true)
	if candidateApplyResultCtx.resolveCtx.rowsDecisionCtx.inputIndex != 2 {
		t.Fatalf("expected rows-candidate apply flow result helper to apply resolved rows ctx input index 2, got %d", candidateApplyResultCtx.resolveCtx.rowsDecisionCtx.inputIndex)
	}
	candidateFlowCtx = stage2ResolveFastTargetSharedPeerTopKRowsCandidateApplyFlowDecision(candidateFlowCtx)
	if candidateFlowCtx.resolveCtx.rowsDecisionCtx.inputIndex != 2 {
		t.Fatalf("expected rows-candidate apply flow helper to update rows ctx input index to 2, got %d", candidateFlowCtx.resolveCtx.rowsDecisionCtx.inputIndex)
	}
	candidateFlowResultCtx, err := stage2ResolveFastTargetSharedPeerTopKRowsCandidateResolveFlowResultDecision(candidateFlowCtx)
	if err != nil {
		t.Fatalf("expected rows-candidate result flow helper success, got error: %v", err)
	}
	if candidateFlowResultCtx.rowsDecisionCtx.inputIndex != 2 {
		t.Fatalf("expected rows-candidate result flow helper to return updated rows ctx input index 2, got %d", candidateFlowResultCtx.rowsDecisionCtx.inputIndex)
	}

	flowResolvedCtx, err := stage2ResolveFastTargetSharedPeerTopKRowsCandidateResolveFlowDecision(stage2BuildFastTargetSharedPeerTopKRowsCandidateResolveFlowDecisionContext(stage2BuildFastTargetSharedPeerTopKRowsResolveDecisionContext(nonEmptyRowsDecisionCtx)))
	if err != nil {
		t.Fatalf("expected unified rows-candidate resolve flow helper success, got error: %v", err)
	}
	if flowResolvedCtx.rowsDecisionCtx.inputIndex != 2 {
		t.Fatalf("expected unified rows-candidate resolve flow helper to update input index to 2, got %d", flowResolvedCtx.rowsDecisionCtx.inputIndex)
	}
	if stage2ResolveFastTargetSharedPeerTopKRowsCandidateResolveFlowResultGateDecision(candidateFlowCtx) {
		t.Fatalf("expected rows-candidate resolve flow result gate helper to return false on success path")
	}
	candidateFlowResultResultCtx, err := stage2ResolveFastTargetSharedPeerTopKRowsCandidateResolveFlowResultResultDecision(candidateFlowCtx, false)
	if err != nil {
		t.Fatalf("expected rows-candidate resolve flow result result helper success, got error: %v", err)
	}
	if candidateFlowResultResultCtx.rowsDecisionCtx.inputIndex != 2 {
		t.Fatalf("expected rows-candidate resolve flow result result helper to preserve updated rows ctx input index 2, got %d", candidateFlowResultResultCtx.rowsDecisionCtx.inputIndex)
	}
	if successResultCtx, err := stage2ResolveFastTargetSharedPeerTopKRowsCandidateResolveFlowSuccessResultDecision(candidateFlowCtx); err != nil || successResultCtx.rowsDecisionCtx.inputIndex != 2 {
		t.Fatalf("expected rows-candidate resolve-flow success result helper to return updated rows ctx without error")
	}

	resolveCtx = stage2ResolveFastTargetSharedPeerTopKRowsFinalizeDecision(resolveCtx)
	if len(resolveCtx.rows) != 1 {
		t.Fatalf("expected rows-finalize decision helper to produce one row, got %d", len(resolveCtx.rows))
	}
	if resolveCtx.rows[0]["outTarget"].(*graph.Vertex).ID != "m1" {
		t.Fatalf("expected rows-finalize decision helper to keep highest shared candidate")
	}
	if !stage2ResolveFastTargetSharedPeerTopKRowsFinalizeGateDecision(resolveCtx) {
		t.Fatalf("expected finalize gate helper to return true when returnEmpty=false")
	}
	finalizeResultCtx := stage2ResolveFastTargetSharedPeerTopKRowsFinalizeResultDecision(resolveCtx, true)
	if len(finalizeResultCtx.rows) != 1 {
		t.Fatalf("expected finalize result helper to preserve finalized rows when gate is true")
	}
	finalizeApplyResultCtx := stage2ResolveFastTargetSharedPeerTopKRowsFinalizeApplyResultDecision(resolveCtx)
	if len(finalizeApplyResultCtx.rows) != 1 {
		t.Fatalf("expected finalize apply result helper to produce finalized rows")
	}

	rows, err := stage2ResolveFastTargetSharedPeerTopKRowsResolveDecision(stage2BuildFastTargetSharedPeerTopKRowsResolveDecisionContext(nonEmptyRowsDecisionCtx))
	if err != nil {
		t.Fatalf("expected rows resolve decision helper success, got error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected rows resolve decision helper to return one row, got %d", len(rows))
	}

	emptyRowsDecisionCtx := stage2BuildFastTargetSharedPeerTopKRowsDecisionContext(aggs, projection, withSpec, topKProjection, fastTargetSharedPeerTopKSpec{descending: true, skip: 0, limit: 0}, context.Background(), nil, Params{}, nil)
	emptyResolveCtx := stage2ResolveFastTargetSharedPeerTopKRowsEmptyLimitDecision(stage2BuildFastTargetSharedPeerTopKRowsResolveDecisionContext(emptyRowsDecisionCtx))
	if !emptyResolveCtx.returnEmpty {
		t.Fatalf("expected empty-limit decision helper to set returnEmpty=true")
	}
	if len(emptyResolveCtx.rows) != 0 {
		t.Fatalf("expected empty-limit decision helper to initialize empty rows")
	}
	if !stage2ResolveFastTargetSharedPeerTopKRowsEmptyLimitGateDecision(stage2BuildFastTargetSharedPeerTopKRowsResolveDecisionContext(emptyRowsDecisionCtx)) {
		t.Fatalf("expected empty-limit gate helper to return true for empty limit")
	}
	emptyResultCtx := stage2ResolveFastTargetSharedPeerTopKRowsEmptyLimitResultDecision(stage2BuildFastTargetSharedPeerTopKRowsResolveDecisionContext(emptyRowsDecisionCtx), true)
	if !emptyResultCtx.returnEmpty || len(emptyResultCtx.rows) != 0 {
		t.Fatalf("expected empty-limit result helper to set returnEmpty=true and empty rows when gate is true")
	}
	applyEmptyLimitCtx := stage2ResolveFastTargetSharedPeerTopKRowsEmptyLimitApplyResultDecision(stage2BuildFastTargetSharedPeerTopKRowsResolveDecisionContext(emptyRowsDecisionCtx))
	if !applyEmptyLimitCtx.returnEmpty || len(applyEmptyLimitCtx.rows) != 0 {
		t.Fatalf("expected empty-limit apply result helper to set returnEmpty=true and empty rows")
	}

	emptyCandidateFlowCtx := stage2BuildFastTargetSharedPeerTopKRowsCandidateResolveFlowDecisionContext(emptyResolveCtx)
	if stage2ResolveFastTargetSharedPeerTopKRowsCandidateRowsFlowGateDecision(emptyCandidateFlowCtx) {
		t.Fatalf("expected rows-candidate rows flow gate helper to return false when returnEmpty=true")
	}
	emptyCandidateFlowCtx = stage2ResolveFastTargetSharedPeerTopKRowsCandidateRowsFlowDecision(emptyCandidateFlowCtx)
	if emptyCandidateFlowCtx.hasResolvedRowsDecisionCtx {
		t.Fatalf("expected rows-candidate rows flow helper to skip candidate resolution when returnEmpty=true")
	}
	if stage2ResolveFastTargetSharedPeerTopKRowsCandidateApplyFlowGateDecision(emptyCandidateFlowCtx) {
		t.Fatalf("expected rows-candidate apply flow gate helper to return false when resolved rows ctx is missing")
	}
	emptyApplyResultCtx := stage2ResolveFastTargetSharedPeerTopKRowsCandidateApplyFlowResultDecision(emptyCandidateFlowCtx, false)
	if emptyApplyResultCtx.resolveCtx.rowsDecisionCtx.inputIndex != emptyRowsDecisionCtx.inputIndex {
		t.Fatalf("expected rows-candidate apply flow result helper to preserve rows ctx when gate is false")
	}
	emptyCandidateFlowCtx = stage2ResolveFastTargetSharedPeerTopKRowsCandidateApplyFlowDecision(emptyCandidateFlowCtx)
	if emptyCandidateFlowCtx.resolveCtx.rowsDecisionCtx.inputIndex != emptyRowsDecisionCtx.inputIndex {
		t.Fatalf("expected rows-candidate apply flow helper to preserve rows ctx when returnEmpty=true")
	}

	emptyRows, err := stage2ResolveFastTargetSharedPeerTopKRowsResolveDecision(stage2BuildFastTargetSharedPeerTopKRowsResolveDecisionContext(emptyRowsDecisionCtx))
	if err != nil {
		t.Fatalf("expected empty-limit resolve decision helper success, got error: %v", err)
	}
	if len(emptyRows) != 0 {
		t.Fatalf("expected empty-limit resolve decision helper to return no rows")
	}

	flowCtx := stage2BuildFastTargetSharedPeerTopKRowsResolveFlowDecisionContext(stage2BuildFastTargetSharedPeerTopKRowsResolveDecisionContext(nonEmptyRowsDecisionCtx))
	flowCtx = stage2ResolveFastTargetSharedPeerTopKRowsEmptyLimitFlowDecision(flowCtx)
	if flowCtx.resolveCtx.returnEmpty {
		t.Fatalf("expected rows-resolve empty-limit flow helper to keep returnEmpty=false for non-empty limit")
	}
	if !stage2ResolveFastTargetSharedPeerTopKRowsCandidateFlowGateDecision(flowCtx) {
		t.Fatalf("expected rows-resolve candidate flow gate helper to return true when no prior error is present")
	}
	blockedCandidateFlowCtx := flowCtx
	blockedCandidateFlowCtx.hasError = true
	if stage2ResolveFastTargetSharedPeerTopKRowsCandidateFlowGateDecision(blockedCandidateFlowCtx) {
		t.Fatalf("expected rows-resolve candidate flow gate helper to return false when prior error is present")
	}
	resolvedCandidateFlowCtx, err := stage2ResolveFastTargetSharedPeerTopKRowsCandidateDecision(flowCtx.resolveCtx)
	if err != nil {
		t.Fatalf("expected rows-candidate decision helper success for candidate-flow result helper test, got error: %v", err)
	}
	rowsCandidateFlowResultCtx := stage2ResolveFastTargetSharedPeerTopKRowsCandidateFlowResultDecision(flowCtx, resolvedCandidateFlowCtx, nil)
	if rowsCandidateFlowResultCtx.hasError || rowsCandidateFlowResultCtx.resolveCtx.rowsDecisionCtx.inputIndex != 2 {
		t.Fatalf("expected rows-resolve candidate flow result helper to apply resolved context when no error")
	}
	successCandidateFlowResultCtx := stage2ResolveFastTargetSharedPeerTopKRowsCandidateFlowSuccessResultDecision(flowCtx, resolvedCandidateFlowCtx)
	if successCandidateFlowResultCtx.hasError || successCandidateFlowResultCtx.resolveCtx.rowsDecisionCtx.inputIndex != 2 {
		t.Fatalf("expected rows-resolve candidate flow success result helper to apply resolved context")
	}
	flowCtx = stage2ResolveFastTargetSharedPeerTopKRowsCandidateFlowDecision(flowCtx)
	if flowCtx.hasError {
		t.Fatalf("expected rows-resolve candidate flow helper success, got error: %v", flowCtx.err)
	}
	if !stage2ResolveFastTargetSharedPeerTopKRowsFinalizeFlowGateDecision(flowCtx) {
		t.Fatalf("expected rows-resolve finalize flow gate helper to return true when no error is present")
	}
	blockedFinalizeFlowCtx := flowCtx
	blockedFinalizeFlowCtx.hasError = true
	if stage2ResolveFastTargetSharedPeerTopKRowsFinalizeFlowGateDecision(blockedFinalizeFlowCtx) {
		t.Fatalf("expected rows-resolve finalize flow gate helper to return false when error is present")
	}
	finalizeFlowResultCtx := stage2ResolveFastTargetSharedPeerTopKRowsFinalizeFlowResultDecision(flowCtx, true)
	if len(finalizeFlowResultCtx.resolveCtx.rows) != 1 {
		t.Fatalf("expected rows-resolve finalize flow result helper to finalize rows when gate is true")
	}
	finalizeFlowApplyCtx := stage2ResolveFastTargetSharedPeerTopKRowsFinalizeFlowApplyResultDecision(flowCtx)
	if len(finalizeFlowApplyCtx.resolveCtx.rows) != 1 {
		t.Fatalf("expected rows-resolve finalize flow apply result helper to finalize rows")
	}
	flowCtx = stage2ResolveFastTargetSharedPeerTopKRowsFinalizeFlowDecision(flowCtx)
	if len(flowCtx.resolveCtx.rows) != 1 {
		t.Fatalf("expected rows-resolve finalize flow helper to produce one row, got %d", len(flowCtx.resolveCtx.rows))
	}

	flowRows, err := stage2ResolveFastTargetSharedPeerTopKRowsResolveFlowDecision(stage2BuildFastTargetSharedPeerTopKRowsResolveFlowDecisionContext(stage2BuildFastTargetSharedPeerTopKRowsResolveDecisionContext(nonEmptyRowsDecisionCtx)))
	if err != nil {
		t.Fatalf("expected unified rows-resolve flow helper success, got error: %v", err)
	}
	if len(flowRows) != 1 {
		t.Fatalf("expected unified rows-resolve flow helper to return one row, got %d", len(flowRows))
	}
	if stage2ResolveFastTargetSharedPeerTopKRowsResolveFlowResultGateDecision(flowCtx) {
		t.Fatalf("expected rows-resolve flow result gate helper to return false on success path")
	}
	flowResultResultRows, err := stage2ResolveFastTargetSharedPeerTopKRowsResolveFlowResultResultDecision(flowCtx, false)
	if err != nil {
		t.Fatalf("expected rows-resolve flow result result helper success, got error: %v", err)
	}
	if len(flowResultResultRows) != 1 {
		t.Fatalf("expected rows-resolve flow result result helper to return one row on success path, got %d", len(flowResultResultRows))
	}
	flowSuccessRows, err := stage2ResolveFastTargetSharedPeerTopKRowsResolveFlowSuccessResultDecision(flowCtx)
	if err != nil || len(flowSuccessRows) != 1 {
		t.Fatalf("expected rows-resolve flow success result helper to return one row without error")
	}
	flowResultRows, err := stage2ResolveFastTargetSharedPeerTopKRowsResolveFlowResultDecision(flowCtx)
	if err != nil {
		t.Fatalf("expected rows-resolve result flow helper success, got error: %v", err)
	}
	if len(flowResultRows) != 1 {
		t.Fatalf("expected rows-resolve result flow helper to return one row, got %d", len(flowResultRows))
	}

	errRowsDecisionCtx := stage2BuildFastTargetSharedPeerTopKRowsDecisionContext(
		aggs,
		projection,
		withSpec,
		fastTargetSharedPeerTopKProjection{targetKey: "outTarget", peerKey: "outPeer", similarityKey: "sim", similarityExpr: "("},
		fastTargetSharedPeerTopKSpec{descending: true, skip: 0, limit: 1},
		context.Background(),
		nil,
		Params{},
		nil,
	)
	errFlowCtx := stage2BuildFastTargetSharedPeerTopKRowsResolveFlowDecisionContext(stage2BuildFastTargetSharedPeerTopKRowsResolveDecisionContext(errRowsDecisionCtx))
	errCandidateFlowCtx := stage2BuildFastTargetSharedPeerTopKRowsCandidateResolveFlowDecisionContext(stage2BuildFastTargetSharedPeerTopKRowsResolveDecisionContext(errRowsDecisionCtx))
	errCandidateFlowCtx = stage2ResolveFastTargetSharedPeerTopKRowsCandidateRowsFlowDecision(errCandidateFlowCtx)
	if !errCandidateFlowCtx.hasError {
		t.Fatalf("expected rows-candidate rows flow helper to capture rows-candidates error")
	}
	errRowsFlowResultCtx := stage2ResolveFastTargetSharedPeerTopKRowsCandidateRowsFlowResultDecision(stage2BuildFastTargetSharedPeerTopKRowsCandidateResolveFlowDecisionContext(stage2BuildFastTargetSharedPeerTopKRowsResolveDecisionContext(errRowsDecisionCtx)), stage2FastTargetSharedPeerTopKRowsDecisionContext{}, errors.New("rows-candidate-rows-flow-error"))
	if !errRowsFlowResultCtx.hasError || errRowsFlowResultCtx.err == nil {
		t.Fatalf("expected rows-candidate rows flow result helper to capture error")
	}
	errRowsFlowErrorCtx := stage2ResolveFastTargetSharedPeerTopKRowsCandidateRowsFlowErrorResultDecision(stage2BuildFastTargetSharedPeerTopKRowsCandidateResolveFlowDecisionContext(stage2BuildFastTargetSharedPeerTopKRowsResolveDecisionContext(errRowsDecisionCtx)), errors.New("rows-candidate-rows-flow-error-helper"))
	if !errRowsFlowErrorCtx.hasError || errRowsFlowErrorCtx.err == nil {
		t.Fatalf("expected rows-candidate rows flow error result helper to capture error")
	}
	if stage2ResolveFastTargetSharedPeerTopKRowsCandidateApplyFlowGateDecision(errCandidateFlowCtx) {
		t.Fatalf("expected rows-candidate apply flow gate helper to return false when prior error is present")
	}
	errApplyResultCtx := stage2ResolveFastTargetSharedPeerTopKRowsCandidateApplyFlowResultDecision(errCandidateFlowCtx, false)
	if errApplyResultCtx.resolveCtx.rowsDecisionCtx.inputIndex != errRowsDecisionCtx.inputIndex {
		t.Fatalf("expected rows-candidate apply flow result helper to preserve rows ctx when gate is false due to error")
	}
	errCandidateFlowCtx = stage2ResolveFastTargetSharedPeerTopKRowsCandidateApplyFlowDecision(errCandidateFlowCtx)
	if errCandidateFlowCtx.resolveCtx.rowsDecisionCtx.inputIndex != errRowsDecisionCtx.inputIndex {
		t.Fatalf("expected rows-candidate apply flow helper to preserve rows ctx when error is present")
	}
	if !stage2ResolveFastTargetSharedPeerTopKRowsCandidateResolveFlowResultGateDecision(errCandidateFlowCtx) {
		t.Fatalf("expected rows-candidate resolve flow result gate helper to return true on error path")
	}
	if _, err := stage2ResolveFastTargetSharedPeerTopKRowsCandidateResolveFlowResultResultDecision(errCandidateFlowCtx, true); err == nil {
		t.Fatalf("expected rows-candidate resolve flow result result helper to return rows-candidates error when gate is true")
	}
	if _, err := stage2ResolveFastTargetSharedPeerTopKRowsCandidateResolveFlowErrorResultDecision(errCandidateFlowCtx); err == nil {
		t.Fatalf("expected rows-candidate resolve-flow error result helper to return rows-candidates error")
	}
	if _, err := stage2ResolveFastTargetSharedPeerTopKRowsCandidateResolveFlowResultDecision(errCandidateFlowCtx); err == nil {
		t.Fatalf("expected rows-candidate result flow helper to return rows-candidates error")
	}
	if _, err := stage2ResolveFastTargetSharedPeerTopKRowsCandidateResolveFlowDecision(stage2BuildFastTargetSharedPeerTopKRowsCandidateResolveFlowDecisionContext(stage2BuildFastTargetSharedPeerTopKRowsResolveDecisionContext(errRowsDecisionCtx))); err == nil {
		t.Fatalf("expected unified rows-candidate resolve flow helper to return rows-candidates error")
	}
	errFlowCtx = stage2ResolveFastTargetSharedPeerTopKRowsEmptyLimitFlowDecision(errFlowCtx)
	errCandidateFlowResultCtx := stage2ResolveFastTargetSharedPeerTopKRowsCandidateFlowResultDecision(errFlowCtx, errFlowCtx.resolveCtx, errors.New("rows-resolve-candidate-flow-error"))
	if !errCandidateFlowResultCtx.hasError || errCandidateFlowResultCtx.err == nil {
		t.Fatalf("expected rows-resolve candidate flow result helper to capture candidate-flow error")
	}
	errCandidateFlowErrorResultCtx := stage2ResolveFastTargetSharedPeerTopKRowsCandidateFlowErrorResultDecision(errFlowCtx, errors.New("rows-resolve-candidate-flow-error-helper"))
	if !errCandidateFlowErrorResultCtx.hasError || errCandidateFlowErrorResultCtx.err == nil {
		t.Fatalf("expected rows-resolve candidate flow error result helper to capture candidate-flow error")
	}
	errFlowCtx = stage2ResolveFastTargetSharedPeerTopKRowsCandidateFlowDecision(errFlowCtx)
	if !errFlowCtx.hasError {
		t.Fatalf("expected rows-resolve candidate flow helper to capture candidate resolution error")
	}
	if stage2ResolveFastTargetSharedPeerTopKRowsFinalizeFlowGateDecision(errFlowCtx) {
		t.Fatalf("expected rows-resolve finalize flow gate helper to return false when candidate flow has error")
	}
	errFinalizeFlowResultCtx := stage2ResolveFastTargetSharedPeerTopKRowsFinalizeFlowResultDecision(errFlowCtx, false)
	if len(errFinalizeFlowResultCtx.resolveCtx.rows) != 0 {
		t.Fatalf("expected rows-resolve finalize flow result helper to preserve rows when gate is false")
	}
	errFinalizeFlowPreserveCtx := stage2ResolveFastTargetSharedPeerTopKRowsFinalizeFlowPreserveResultDecision(errFlowCtx)
	if len(errFinalizeFlowPreserveCtx.resolveCtx.rows) != 0 {
		t.Fatalf("expected rows-resolve finalize flow preserve result helper to preserve rows")
	}
	errFlowCtx = stage2ResolveFastTargetSharedPeerTopKRowsFinalizeFlowDecision(errFlowCtx)
	if len(errFlowCtx.resolveCtx.rows) != 0 {
		t.Fatalf("expected rows-resolve finalize flow helper to skip finalization when error is present")
	}
	if len(stage2ResolveFastTargetSharedPeerTopKRowsFinalizePreserveResultDecision(stage2FastTargetSharedPeerTopKRowsResolveDecisionContext{returnEmpty: true, rows: []Row{{"id": "preserve"}}}).rows) != 1 {
		t.Fatalf("expected finalize preserve result helper to preserve rows")
	}
	if stage2ResolveFastTargetSharedPeerTopKRowsFinalizeGateDecision(stage2FastTargetSharedPeerTopKRowsResolveDecisionContext{returnEmpty: true}) {
		t.Fatalf("expected finalize gate helper to return false when returnEmpty=true")
	}
	if stage2ResolveFastTargetSharedPeerTopKRowsFinalizeResultDecision(stage2FastTargetSharedPeerTopKRowsResolveDecisionContext{returnEmpty: true, rows: []Row{{"id": "preserve"}}}, false).rows[0]["id"] != "preserve" {
		t.Fatalf("expected finalize result helper to preserve rows when gate is false")
	}
	if !stage2ResolveFastTargetSharedPeerTopKRowsResolveFlowResultGateDecision(errFlowCtx) {
		t.Fatalf("expected rows-resolve flow result gate helper to return true on error path")
	}
	if _, err := stage2ResolveFastTargetSharedPeerTopKRowsResolveFlowResultResultDecision(errFlowCtx, true); err == nil {
		t.Fatalf("expected rows-resolve flow result result helper to return candidate resolution error when gate is true")
	}
	if _, err := stage2ResolveFastTargetSharedPeerTopKRowsResolveFlowErrorResultDecision(errFlowCtx); err == nil {
		t.Fatalf("expected rows-resolve flow error result helper to return candidate resolution error")
	}
	if _, err := stage2ResolveFastTargetSharedPeerTopKRowsResolveFlowResultDecision(errFlowCtx); err == nil {
		t.Fatalf("expected rows-resolve result flow helper to return candidate resolution error")
	}
	if _, err := stage2ResolveFastTargetSharedPeerTopKRowsResolveFlowDecision(stage2BuildFastTargetSharedPeerTopKRowsResolveFlowDecisionContext(stage2BuildFastTargetSharedPeerTopKRowsResolveDecisionContext(errRowsDecisionCtx))); err == nil {
		t.Fatalf("expected unified rows-resolve flow helper to return error for invalid similarity expression")
	}
}

func TestStage2ResolveFastTargetSharedPeerAverageDiff(t *testing.T) {
	agg := &fastTargetSharedPeerAggregate{shared: 4, sumAbsDiff: 10}
	if got := stage2ResolveFastTargetSharedPeerAverageDiff(agg); got != 2.5 {
		t.Fatalf("expected average diff 2.5, got %v", got)
	}
}

func TestStage2BuildFastTargetSharedPeerTopKRowSeed(t *testing.T) {
	agg := &fastTargetSharedPeerAggregate{target: &graph.Vertex{ID: "m1"}, peer: &graph.Vertex{ID: "u1"}, shared: 2, sumAbsDiff: 3}
	projection := fastTargetSharedPeerProjection{targetKey: "target", peerKey: "peer", sharedCountKey: "shared", avgDiffKey: "avgdiff"}
	row := stage2BuildFastTargetSharedPeerTopKRowSeed(agg, projection)
	if row["target"].(*graph.Vertex).ID != "m1" || row["peer"].(*graph.Vertex).ID != "u1" {
		t.Fatalf("expected row seed to include target/peer bindings")
	}
	if row["shared"].(int) != 2 {
		t.Fatalf("expected row seed to include shared count")
	}
	if row["avgdiff"].(float64) != 1.5 {
		t.Fatalf("expected row seed to include average diff 1.5")
	}
}

func TestStage2BuildFastTargetSharedPeerRankedCandidate(t *testing.T) {
	trimmed := Row{"target": "m1", "peer": "u1", "sim": 2.0}
	candidate := stage2BuildFastTargetSharedPeerRankedCandidate(trimmed, 2.0, 7)
	if candidate.row["target"] != "m1" || candidate.row["peer"] != "u1" {
		t.Fatalf("expected candidate row payload to be preserved")
	}
	if candidate.score != 2.0 || candidate.inputIndex != 7 {
		t.Fatalf("expected candidate score/index to be preserved")
	}
}

func TestStage2SortFastTargetSharedPeerRankedRows(t *testing.T) {
	ranked := []fastTargetSharedPeerRankedRow{
		{row: Row{"id": "a"}, score: 2.0, inputIndex: 1},
		{row: Row{"id": "b"}, score: 9.0, inputIndex: 2},
		{row: Row{"id": "c"}, score: 9.0, inputIndex: 0},
	}

	stage2SortFastTargetSharedPeerRankedRows(ranked, true)
	if ranked[0].row["id"] != "c" || ranked[1].row["id"] != "b" || ranked[2].row["id"] != "a" {
		t.Fatalf("expected descending sort by score with input-index tie-break")
	}

	stage2SortFastTargetSharedPeerRankedRows(ranked, false)
	if ranked[0].row["id"] != "a" || ranked[2].row["id"] != "b" {
		t.Fatalf("expected ascending sort by score")
	}
}

func TestStage2ResolveFastTargetSharedPeerTopKRankedWindow(t *testing.T) {
	ranked := []fastTargetSharedPeerRankedRow{
		{row: Row{"id": "a"}},
		{row: Row{"id": "b"}},
		{row: Row{"id": "c"}},
	}

	window := stage2ResolveFastTargetSharedPeerTopKRankedWindow(ranked, 1, 1)
	if len(window) != 1 || window[0].row["id"] != "b" {
		t.Fatalf("expected skip/limit window to return middle ranked row")
	}

	empty := stage2ResolveFastTargetSharedPeerTopKRankedWindow(ranked, 3, 1)
	if len(empty) != 0 {
		t.Fatalf("expected window to be empty when skip reaches ranked length")
	}
}

func TestStage2BuildFastTargetSharedPeerTopKOutputRows(t *testing.T) {
	window := []fastTargetSharedPeerRankedRow{
		{row: Row{"id": "a"}},
		{row: Row{"id": "b"}},
	}
	out := stage2BuildFastTargetSharedPeerTopKOutputRows(window)
	if len(out) != 2 {
		t.Fatalf("expected two output rows, got %d", len(out))
	}
	if out[0]["id"] != "a" || out[1]["id"] != "b" {
		t.Fatalf("expected output rows to preserve ranked window order")
	}
}

func TestStage2FinalizeFastTargetSharedPeerTopKRows(t *testing.T) {
	top := &fastTargetSharedPeerTopKHeap{descending: true, rows: []fastTargetSharedPeerRankedRow{
		{row: Row{"id": "a"}, score: 2.0, inputIndex: 1},
		{row: Row{"id": "b"}, score: 9.0, inputIndex: 2},
		{row: Row{"id": "c"}, score: 9.0, inputIndex: 0},
	}}
	spec := fastTargetSharedPeerTopKSpec{descending: true, skip: 1, limit: 1}

	decisionCtx := stage2BuildFastTargetSharedPeerTopKFinalizeDecisionContext(top, spec)
	if decisionCtx.top != top {
		t.Fatalf("expected finalize decision context to preserve heap pointer")
	}

	decisionCtx = stage2ResolveFastTargetSharedPeerTopKFinalizeRankedDecision(decisionCtx)
	if len(decisionCtx.ranked) != 3 {
		t.Fatalf("expected finalize ranked-decision helper to preserve ranked length 3, got %d", len(decisionCtx.ranked))
	}
	if decisionCtx.ranked[0].row["id"] != "c" {
		t.Fatalf("expected finalize ranked-decision helper to sort ranked rows, got first id %v", decisionCtx.ranked[0].row["id"])
	}

	decisionCtx = stage2ResolveFastTargetSharedPeerTopKFinalizeWindowDecision(decisionCtx)
	if len(decisionCtx.window) != 1 || decisionCtx.window[0].row["id"] != "b" {
		t.Fatalf("expected finalize window-decision helper to resolve single-row window [b], got %v", decisionCtx.window)
	}

	decisionCtx = stage2ResolveFastTargetSharedPeerTopKFinalizeRowsDecision(decisionCtx)
	if len(decisionCtx.rows) != 1 || decisionCtx.rows[0]["id"] != "b" {
		t.Fatalf("expected finalize rows-decision helper to materialize [b], got %v", decisionCtx.rows)
	}

	resolvedRows := stage2ResolveFastTargetSharedPeerTopKFinalizeDecision(stage2BuildFastTargetSharedPeerTopKFinalizeDecisionContext(top, spec))
	if len(resolvedRows) != 1 || resolvedRows[0]["id"] != "b" {
		t.Fatalf("expected unified finalize decision helper to materialize [b], got %v", resolvedRows)
	}

	out := stage2FinalizeFastTargetSharedPeerTopKRows(top, spec)
	if len(out) != 1 {
		t.Fatalf("expected exactly one finalized top-k row, got %d", len(out))
	}
	if out[0]["id"] != "b" {
		t.Fatalf("expected finalized top-k row to respect sort+window semantics, got %v", out[0]["id"])
	}
}

func TestStage2ProjectionTailDecisionHelpers(t *testing.T) {
	e := &Executor{}
	params := Params{}
	rows := []Row{{"count(r)": 2, "avg(r.rating)": 4.0, "extra": "drop"}}
	projection := fastPeerCandidateReturnProjection{orderedOutputKeys: []string{"count(r)", "avg(r.rating)"}}
	priorColumns := []string{"fallback"}

	decisionCtx := stage2BuildProjectionTailDecisionContext(rows, projection, priorColumns, params)
	columns := stage2ResolveProjectionTailColumns(decisionCtx)
	if len(columns) != 2 || columns[0] != "count(r)" || columns[1] != "avg(r.rating)" {
		t.Fatalf("expected decision helper columns from projection order, got %v", columns)
	}

	trimmed := stage2ResolveProjectionTailTrimmedRows(decisionCtx, columns)
	if len(trimmed) != 1 {
		t.Fatalf("expected one trimmed row from projection-tail helper, got %d", len(trimmed))
	}
	if _, ok := trimmed[0]["extra"]; ok {
		t.Fatalf("expected projection-tail trim helper to drop non-selected key")
	}

	resolvedRows, resolvedColumns := e.stage2ResolveProjectionTailDecision(decisionCtx)
	if len(resolvedColumns) != 2 || resolvedColumns[0] != "count(r)" || resolvedColumns[1] != "avg(r.rating)" {
		t.Fatalf("expected projection-tail resolver to preserve projection order, got %v", resolvedColumns)
	}
	if len(resolvedRows) != 1 {
		t.Fatalf("expected projection-tail resolver to return one row, got %d", len(resolvedRows))
	}
	if got := ensureRuntimeCounterState(params).counters["fast_path.stage2.rows_output"]; got != 1 {
		t.Fatalf("expected rows_output counter 1 from resolver helper, got %d", got)
	}

	fallbackParams := Params{}
	fallbackCtx := stage2BuildProjectionTailDecisionContext([]Row{{"a": 1, "b": 2, "c": 3}}, fastPeerCandidateReturnProjection{}, []string{"b", "a"}, fallbackParams)
	fallbackColumns := stage2ResolveProjectionTailColumns(fallbackCtx)
	if len(fallbackColumns) != 2 || fallbackColumns[0] != "b" || fallbackColumns[1] != "a" {
		t.Fatalf("expected projection-tail helper fallback columns order, got %v", fallbackColumns)
	}
}

func TestFinalizeStage2ProjectionTailUsesProjectionColumns(t *testing.T) {
	e := &Executor{}
	params := Params{}
	rows := []Row{{"count(r)": 2, "avg(r.rating)": 4.0, "extra": "drop"}}
	projection := fastPeerCandidateReturnProjection{orderedOutputKeys: []string{"count(r)", "avg(r.rating)"}}

	trimmed, columns := e.finalizeStage2ProjectionTail(rows, projection, []string{"fallback"}, params)
	if len(columns) != 2 || columns[0] != "count(r)" || columns[1] != "avg(r.rating)" {
		t.Fatalf("expected projection ordered columns to be used, got %v", columns)
	}
	if len(trimmed) != 1 {
		t.Fatalf("expected one trimmed row, got %d", len(trimmed))
	}
	if _, ok := trimmed[0]["extra"]; ok {
		t.Fatalf("expected non-projection key to be trimmed")
	}
	if got := ensureRuntimeCounterState(params).counters["fast_path.stage2.rows_output"]; got != 1 {
		t.Fatalf("expected rows_output counter 1, got %d", got)
	}
}

func TestFinalizeStage2ProjectionTailFallsBackToPriorColumns(t *testing.T) {
	e := &Executor{}
	params := Params{}
	rows := []Row{{"a": 1, "b": 2, "c": 3}}
	projection := fastPeerCandidateReturnProjection{}
	priorColumns := []string{"b", "a"}

	trimmed, columns := e.finalizeStage2ProjectionTail(rows, projection, priorColumns, params)
	if len(columns) != 2 || columns[0] != "b" || columns[1] != "a" {
		t.Fatalf("expected prior columns fallback order, got %v", columns)
	}
	if len(trimmed) != 1 {
		t.Fatalf("expected one trimmed row, got %d", len(trimmed))
	}
	if _, ok := trimmed[0]["c"]; ok {
		t.Fatalf("expected fallback trim to drop non-selected columns")
	}
	if got := ensureRuntimeCounterState(params).counters["fast_path.stage2.rows_output"]; got != 1 {
		t.Fatalf("expected rows_output counter 1, got %d", got)
	}
}

func TestStage2AdjacencyScanType(t *testing.T) {
	patternSingle := directedRelationshipPattern{EdgeType: "RATED"}
	if got := stage2AdjacencyScanType(patternSingle); got != "RATED" {
		t.Fatalf("expected single-type adjacency scan to use edge type, got %q", got)
	}

	patternAnyOf := directedRelationshipPattern{EdgeType: "RATED", EdgeAnyOf: []string{"RATED", "LIKED"}}
	if got := stage2AdjacencyScanType(patternAnyOf); got != "" {
		t.Fatalf("expected any-of adjacency scan to clear scan type, got %q", got)
	}
}
