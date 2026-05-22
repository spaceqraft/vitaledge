package graph

// PropertyMap stores opaque property values by key.
type PropertyMap map[string][]byte

// Vertex is the canonical property graph vertex contract.
type Vertex struct {
	Tenant     string
	ID         string
	Labels     []string
	Properties PropertyMap
}

// Edge is the canonical property graph edge contract.
type Edge struct {
	Tenant     string
	ID         string
	Type       string
	SrcID      string
	DstID      string
	Properties PropertyMap
}

// PropertyIndexEntry represents one secondary-index write.
type PropertyIndexEntry struct {
	Tenant      string
	Schema      string
	Property    string
	Value       []byte
	EntityID    string
	EntityClass string // vertex|edge
}
