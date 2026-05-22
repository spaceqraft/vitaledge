package cypher

type Command interface {
	Query() string
}

func ParseCommand(query string) (Command, error) {
	// For now, we will just return a simple command that echoes the query back
	return &SimpleCommand{query: query}, nil
}

type SimpleCommand struct {
	query string
}

func (c *SimpleCommand) Query() string {
	return c.query
}
