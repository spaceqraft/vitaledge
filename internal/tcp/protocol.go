package tcp

import (
	"github.com/paegun/vitaledge/internal/cypher"
)

func parseCommand(data []byte) (cypher.Command, error) {
	return cypher.ParseCommand(string(data))
}
