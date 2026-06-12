package pipeline_test

import (
	"reflect"
	"testing"

	"github.com/spaceqraft/vitaledge/internal/cypher/logical"
	"github.com/spaceqraft/vitaledge/internal/cypher/parser"
	"github.com/spaceqraft/vitaledge/internal/cypher/physical"
	"github.com/spaceqraft/vitaledge/internal/cypher/semantic"
)

func TestSemanticToLogicalToPhysicalHandoffMatchQuery(t *testing.T) {
	stmt, err := parser.ParseStatement("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b.name AS name ORDER BY name ASC LIMIT 5")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	semanticModel, err := semantic.Build(stmt)
	if err != nil {
		t.Fatalf("semantic build failed: %v", err)
	}
	logicalPlan := logical.Build(semanticModel)
	physicalPlan := physical.Build(logicalPlan)

	if semanticModel.StatementKind == "" {
		t.Fatalf("semantic model missing statement kind")
	}
	if logicalPlan.RootNodeID == "" || physicalPlan.RootNodeID == "" {
		t.Fatalf("expected rooted plans, logical=%#v physical=%#v", logicalPlan, physicalPlan)
	}

	lops := make([]string, 0, len(logicalPlan.Nodes))
	for _, node := range logicalPlan.Nodes {
		lops = append(lops, node.Op)
	}
	pops := make([]string, 0, len(physicalPlan.Nodes))
	for _, node := range physicalPlan.Nodes {
		pops = append(pops, node.Op)
	}

	if !reflect.DeepEqual(lops, []string{"MATCH", "PROJECT", "SORT", "PAGINATION"}) {
		t.Fatalf("unexpected logical ops: %v", lops)
	}
	if !reflect.DeepEqual(pops, []string{"PHY_EXPAND_MATCH", "PHY_PROJECT", "PHY_SORT", "PHY_PAGINATION"}) {
		t.Fatalf("unexpected physical ops: %v", pops)
	}
}
