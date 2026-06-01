package keyspace

import "fmt"

const (
	vertexPrefix   = "v"
	edgePrefix     = "e"
	outPrefix      = "a/out"
	inPrefix       = "a/in"
	indexPrefix    = "i"
	indexNumPrefix = "in"
	statsPrefix    = "s"
	metaPrefix     = "m"
)

func VertexKey(tenant, vertexID string) []byte {
	return []byte(fmt.Sprintf("%s/%s/%s", vertexPrefix, tenant, vertexID))
}

func VertexPrefix(tenant string) []byte {
	return []byte(fmt.Sprintf("%s/%s/", vertexPrefix, tenant))
}

func EdgeKey(tenant, edgeID string) []byte {
	return []byte(fmt.Sprintf("%s/%s/%s", edgePrefix, tenant, edgeID))
}

func EdgePrefix(tenant string) []byte {
	return []byte(fmt.Sprintf("%s/%s/", edgePrefix, tenant))
}

func OutAdjacencyKey(tenant, srcID, edgeType, edgeID string) []byte {
	return []byte(fmt.Sprintf("%s/%s/%s/%s/%s", outPrefix, tenant, srcID, edgeType, edgeID))
}

func OutAdjacencyPrefix(tenant, srcID, edgeType string) []byte {
	if edgeType == "" {
		return []byte(fmt.Sprintf("%s/%s/%s/", outPrefix, tenant, srcID))
	}
	return []byte(fmt.Sprintf("%s/%s/%s/%s/", outPrefix, tenant, srcID, edgeType))
}

func InAdjacencyKey(tenant, dstID, edgeType, edgeID string) []byte {
	return []byte(fmt.Sprintf("%s/%s/%s/%s/%s", inPrefix, tenant, dstID, edgeType, edgeID))
}

func InAdjacencyPrefix(tenant, dstID, edgeType string) []byte {
	if edgeType == "" {
		return []byte(fmt.Sprintf("%s/%s/%s/", inPrefix, tenant, dstID))
	}
	return []byte(fmt.Sprintf("%s/%s/%s/%s/", inPrefix, tenant, dstID, edgeType))
}

func PropertyIndexKey(tenant, schema, property string, encodedValue []byte, entityID string) []byte {
	return []byte(fmt.Sprintf("%s/%s/%s/%s/%x/%s", indexPrefix, tenant, schema, property, encodedValue, entityID))
}

func PropertyIndexPrefix(tenant, schema, property string) []byte {
	return []byte(fmt.Sprintf("%s/%s/%s/%s/", indexPrefix, tenant, schema, property))
}

func PropertyIndexValuePrefix(tenant, schema, property string, encodedValue []byte) []byte {
	return []byte(fmt.Sprintf("%s/%s/%s/%s/%x/", indexPrefix, tenant, schema, property, encodedValue))
}

func PropertyIndexNumericPrefix(tenant, schema, property string) []byte {
	return []byte(fmt.Sprintf("%s/%s/%s/%s/", indexNumPrefix, tenant, schema, property))
}

func PropertyIndexNumericKey(tenant, schema, property string, orderedValue []byte, entityID string) []byte {
	return []byte(fmt.Sprintf("%s/%s/%s/%s/%x/%s", indexNumPrefix, tenant, schema, property, orderedValue, entityID))
}

func PropertyIndexNumericValuePrefix(tenant, schema, property string, orderedValue []byte) []byte {
	return []byte(fmt.Sprintf("%s/%s/%s/%s/%x/", indexNumPrefix, tenant, schema, property, orderedValue))
}

func PropertyIndexNumericValueUpperBound(tenant, schema, property string, orderedValue []byte) []byte {
	return []byte(fmt.Sprintf("%s/%s/%s/%s/%x/\xff", indexNumPrefix, tenant, schema, property, orderedValue))
}

func StatsVertexTotalKey(tenant string) []byte {
	return []byte(fmt.Sprintf("%s/%s/vertex_total", statsPrefix, tenant))
}

func StatsEdgeTotalKey(tenant string) []byte {
	return []byte(fmt.Sprintf("%s/%s/edge_total", statsPrefix, tenant))
}

func StatsVertexLabelCountKey(tenant, label string) []byte {
	return []byte(fmt.Sprintf("%s/%s/label/%s", statsPrefix, tenant, label))
}

func StatsVertexLabelPrefix(tenant string) []byte {
	return []byte(fmt.Sprintf("%s/%s/label/", statsPrefix, tenant))
}

func StatsEdgeTypeCountKey(tenant, edgeType string) []byte {
	return []byte(fmt.Sprintf("%s/%s/edge_type/%s", statsPrefix, tenant, edgeType))
}

func StatsEdgeTypePrefix(tenant string) []byte {
	return []byte(fmt.Sprintf("%s/%s/edge_type/", statsPrefix, tenant))
}

func SchemaVersionKey() []byte {
	return []byte(fmt.Sprintf("%s/schema_version", metaPrefix))
}
