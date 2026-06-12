package tcp

import (
	"github.com/spaceqraft/vitaledge/internal/cypher"
)

func parseCommand(data []byte) (cypher.Command, error) {
	return cypher.ParseCommand(string(data))
}
