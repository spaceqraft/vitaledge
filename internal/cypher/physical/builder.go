package physical

import (
	"fmt"

	"github.com/paegun/vitaledge/internal/cypher/logical"
)

// Build constructs a deterministic physical plan from a logical plan.
func Build(plan logical.Plan) Plan {
	out := Plan{RootNodeID: "", Nodes: []Node{}}
	idMap := map[string]string{}

	for idx, ln := range plan.Nodes {
		pid := fmt.Sprintf("p%d", idx+1)
		idMap[ln.ID] = pid

		children := make([]string, 0, len(ln.Children))
		for _, child := range ln.Children {
			if mapped, ok := idMap[child]; ok {
				children = append(children, mapped)
			}
		}

		op, attrs := lowerLogicalNode(ln.Op, ln.Attrs)
		out.Nodes = append(out.Nodes, Node{ID: pid, Op: op, Children: children, Attrs: attrs})
	}

	if mappedRoot, ok := idMap[plan.RootNodeID]; ok {
		out.RootNodeID = mappedRoot
	}
	if out.RootNodeID == "" && len(out.Nodes) > 0 {
		out.RootNodeID = out.Nodes[len(out.Nodes)-1].ID
	}

	return out
}

func lowerLogicalNode(logicalOp string, logicalAttrs map[string]any) (string, map[string]any) {
	attrs := map[string]any{}
	for k, v := range logicalAttrs {
		attrs[k] = v
	}

	switch logicalOp {
	case "MATCH":
		attrs["accessPath"] = "adjacency_expand"
		return "PHY_EXPAND_MATCH", attrs
	case "OPTIONAL_MATCH":
		attrs["accessPath"] = "adjacency_expand_optional"
		return "PHY_EXPAND_OPTIONAL", attrs
	case "WRITE":
		attrs["strategy"] = "typed_write_operator"
		return "PHY_WRITE", attrs
	case "PROJECT":
		attrs["strategy"] = "projection_eval"
		return "PHY_PROJECT", attrs
	case "SORT":
		attrs["strategy"] = "in_memory_sort"
		return "PHY_SORT", attrs
	case "PAGINATION":
		attrs["strategy"] = "offset_limit"
		return "PHY_PAGINATION", attrs
	default:
		attrs["strategy"] = "passthrough"
		return "PHY_" + logicalOp, attrs
	}
}
