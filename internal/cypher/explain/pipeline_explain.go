package explain

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/spaceqraft/vitaledge/internal/cypher/logical"
	"github.com/spaceqraft/vitaledge/internal/cypher/physical"
)

type explainNode struct {
	id       string
	op       string
	children []string
	attrs    map[string]any
}

// RenderPipeline renders a deterministic explain string for the logical and
// physical plans used by the graph-native pipeline.
func RenderPipeline(logicalPlan logical.Plan, physicalPlan physical.Plan) string {
	logicalNodes := make([]explainNode, 0, len(logicalPlan.Nodes))
	for _, node := range logicalPlan.Nodes {
		logicalNodes = append(logicalNodes, explainNode{
			id:       node.ID,
			op:       node.Op,
			children: append([]string(nil), node.Children...),
			attrs:    node.Attrs,
		})
	}

	physicalNodes := make([]explainNode, 0, len(physicalPlan.Nodes))
	for _, node := range physicalPlan.Nodes {
		physicalNodes = append(physicalNodes, explainNode{
			id:       node.ID,
			op:       node.Op,
			children: append([]string(nil), node.Children...),
			attrs:    node.Attrs,
		})
	}

	var b strings.Builder
	b.WriteString(renderSection("LOGICAL", logicalPlan.RootNodeID, logicalNodes))
	b.WriteString("\n")
	b.WriteString(renderSection("PHYSICAL", physicalPlan.RootNodeID, physicalNodes))
	return b.String()
}

func renderSection(name, root string, nodes []explainNode) string {
	var b strings.Builder
	b.WriteString(name)
	b.WriteString(" root=")
	b.WriteString(root)
	b.WriteString("\n")
	if len(nodes) == 0 {
		b.WriteString("- (none)")
		return b.String()
	}
	for i, node := range nodes {
		if i > 0 {
			b.WriteString("\n")
		}
		b.WriteString("- ")
		b.WriteString(node.id)
		if strings.TrimSpace(node.op) != "" {
			b.WriteString(" ")
			b.WriteString(node.op)
		}
		b.WriteString(" children=")
		b.WriteString(formatValue(node.children))
		b.WriteString(" attrs=")
		b.WriteString(formatAttrs(node.attrs))
	}
	return b.String()
}

func formatAttrs(attrs map[string]any) string {
	if len(attrs) == 0 {
		return "{}"
	}
	return formatValue(attrs)
}

func formatValue(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf("%v", value)
	}
	return string(encoded)
}
