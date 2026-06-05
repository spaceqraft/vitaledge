package pipeline

import (
	"fmt"

	"github.com/paegun/vitaledge/internal/cypher/ast"
)

// ErrUnsupportedStatement reports that a statement kind is not yet supported
// by the pipeline stage being exercised.
type ErrUnsupportedStatement struct {
	Kind ast.StatementKind
}

func (e ErrUnsupportedStatement) Error() string {
	return fmt.Sprintf("pipeline: unsupported statement kind %q", e.Kind)
}
