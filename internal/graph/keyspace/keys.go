package keyspace

import (
	"encoding/hex"
	"strings"
)

const (
	vertexPrefix           = "v"
	edgePrefix             = "e"
	vertexLabelPrefix      = "vl"
	labelVertexPrefix      = "lv"
	edgeTypePrefix         = "et"
	typeEdgePrefix         = "te"
	vertexPropertyPrefix   = "vp"
	propertyVertexPrefix   = "pv"
	edgePropertyPrefix     = "ep"
	propertyEdgePrefix     = "pe"
	outPrefix              = "rf"
	outEndpointPrefix      = "od"
	outEndpointCountPrefix = "odc"
	undEndpointCountPrefix = "udc"
	inPrefix               = "rt"
	indexPrefix            = "pi"
	indexNumPrefix         = "pn"
	statsPrefix            = "s"
	metaPrefix             = "m"
)

func VertexKey(tenant, vertexID string) []byte {
	return buildKey(vertexPrefix, tenant, vertexID)
}

func VertexPrefix(tenant string) []byte {
	return buildPrefix(vertexPrefix, tenant)
}

func VertexLabelMembershipKey(tenant, label, vertexID string) []byte {
	return LabelVertexKey(tenant, label, vertexID)
}

func VertexLabelKey(tenant, vertexID, label string) []byte {
	return buildKey(vertexLabelPrefix, tenant, vertexID, label)
}

func LabelVertexKey(tenant, label, vertexID string) []byte {
	return buildKey(labelVertexPrefix, tenant, label, vertexID)
}

func VertexLabelPrefix(tenant, vertexID string) []byte {
	return buildPrefix(vertexLabelPrefix, tenant, vertexID)
}

func LabelVertexPrefix(tenant, label string) []byte {
	return buildPrefix(labelVertexPrefix, tenant, label)
}

func EdgeKey(tenant, edgeID string) []byte {
	return buildKey(edgePrefix, tenant, edgeID)
}

func EdgePrefix(tenant string) []byte {
	return buildPrefix(edgePrefix, tenant)
}

func EdgeTypeKey(tenant, edgeID, edgeType string) []byte {
	return buildKey(edgeTypePrefix, tenant, edgeID, edgeType)
}

func EdgeTypePrefix(tenant, edgeID string) []byte {
	return buildPrefix(edgeTypePrefix, tenant, edgeID)
}

func TypeEdgeKey(tenant, edgeType, edgeID string) []byte {
	return buildKey(typeEdgePrefix, tenant, edgeType, edgeID)
}

func TypeEdgePrefix(tenant, edgeType string) []byte {
	return buildPrefix(typeEdgePrefix, tenant, edgeType)
}

func OutAdjacencyKey(tenant, srcID, edgeType, edgeID string) []byte {
	return buildKey(outPrefix, tenant, srcID, edgeType, edgeID)
}

func OutAdjacencyPrefix(tenant, srcID, edgeType string) []byte {
	if edgeType == "" {
		return buildPrefix(outPrefix, tenant, srcID)
	}
	return buildPrefix(outPrefix, tenant, srcID, edgeType)
}

func OutAdjacencyTenantPrefix(tenant string) []byte {
	return buildPrefix(outPrefix, tenant)
}

func OutEndpointKey(tenant, srcID, edgeType, dstID, edgeID string) []byte {
	return buildKey(outEndpointPrefix, tenant, srcID, edgeType, dstID, edgeID)
}

func OutEndpointPrefix(tenant, srcID, edgeType, dstID string) []byte {
	return buildPrefix(outEndpointPrefix, tenant, srcID, edgeType, dstID)
}

func OutEndpointPairCountKey(tenant, srcID, edgeType, dstID string) []byte {
	return buildKey(outEndpointCountPrefix, tenant, srcID, edgeType, dstID)
}

func UndirectedEndpointPairCountKey(tenant, leftID, edgeType, rightID string) []byte {
	return buildKey(undEndpointCountPrefix, tenant, leftID, edgeType, rightID)
}

func InAdjacencyKey(tenant, dstID, edgeType, edgeID string) []byte {
	return buildKey(inPrefix, tenant, dstID, edgeType, edgeID)
}

func InAdjacencyPrefix(tenant, dstID, edgeType string) []byte {
	if edgeType == "" {
		return buildPrefix(inPrefix, tenant, dstID)
	}
	return buildPrefix(inPrefix, tenant, dstID, edgeType)
}

func InAdjacencyTenantPrefix(tenant string) []byte {
	return buildPrefix(inPrefix, tenant)
}

func VertexPropertyKey(tenant, vertexID, schema, property string, encodedValue []byte) []byte {
	return buildKey(vertexPropertyPrefix, tenant, vertexID, schema, property, hex.EncodeToString(encodedValue))
}

func VertexPropertyPrefix(tenant, vertexID, schema, property string) []byte {
	return buildPrefix(vertexPropertyPrefix, tenant, vertexID, schema, property)
}

func PropertyVertexKey(tenant, schema, property string, encodedValue []byte, vertexID string) []byte {
	return buildKey(propertyVertexPrefix, tenant, schema, property, hex.EncodeToString(encodedValue), vertexID)
}

func PropertyVertexPrefix(tenant, schema, property string) []byte {
	return buildPrefix(propertyVertexPrefix, tenant, schema, property)
}

func EdgePropertyKey(tenant, edgeID, schema, property string, encodedValue []byte) []byte {
	return buildKey(edgePropertyPrefix, tenant, edgeID, schema, property, hex.EncodeToString(encodedValue))
}

func EdgePropertyPrefix(tenant, edgeID, schema, property string) []byte {
	return buildPrefix(edgePropertyPrefix, tenant, edgeID, schema, property)
}

func EdgePropertyEntityPrefix(tenant, edgeID string) []byte {
	return buildPrefix(edgePropertyPrefix, tenant, edgeID)
}

func PropertyEdgeKey(tenant, schema, property string, encodedValue []byte, edgeID string) []byte {
	return buildKey(propertyEdgePrefix, tenant, schema, property, hex.EncodeToString(encodedValue), edgeID)
}

func PropertyEdgePrefix(tenant, schema, property string) []byte {
	return buildPrefix(propertyEdgePrefix, tenant, schema, property)
}

func PropertyIndexKey(tenant, schema, property, typeSegment string, encodedValue []byte, entityID string) []byte {
	return buildKey(indexPrefix, tenant, schema, property, typeSegment, hex.EncodeToString(encodedValue), entityID)
}

func PropertyIndexPrefix(tenant, schema, property string) []byte {
	return buildPrefix(indexPrefix, tenant, schema, property)
}

func PropertyIndexValuePrefix(tenant, schema, property, typeSegment string, encodedValue []byte) []byte {
	return buildPrefix(indexPrefix, tenant, schema, property, typeSegment, hex.EncodeToString(encodedValue))
}

func PropertyIndexNumericPrefix(tenant, schema, property string) []byte {
	return buildPrefix(indexNumPrefix, tenant, schema, property, "numeric")
}

func PropertyIndexBooleanPrefix(tenant, schema, property string) []byte {
	return buildPrefix(indexPrefix+"b", tenant, schema, property, "boolean")
}

func PropertyIndexBooleanKey(tenant, schema, property string, orderedValue []byte, entityID string) []byte {
	return buildKey(indexPrefix+"b", tenant, schema, property, "boolean", hex.EncodeToString(orderedValue), entityID)
}

func PropertyIndexBooleanValuePrefix(tenant, schema, property string, orderedValue []byte) []byte {
	return buildPrefix(indexPrefix+"b", tenant, schema, property, "boolean", hex.EncodeToString(orderedValue))
}

func PropertyIndexBooleanValueUpperBound(tenant, schema, property string, orderedValue []byte) []byte {
	return append(PropertyIndexBooleanValuePrefix(tenant, schema, property, orderedValue), 0xFF)
}

func PropertyIndexDateTimePrefix(tenant, schema, property string) []byte {
	return buildPrefix(indexPrefix+"t", tenant, schema, property, "datetime")
}

func PropertyIndexDateTimeKey(tenant, schema, property string, orderedValue []byte, entityID string) []byte {
	return buildKey(indexPrefix+"t", tenant, schema, property, "datetime", hex.EncodeToString(orderedValue), entityID)
}

func PropertyIndexDateTimeValuePrefix(tenant, schema, property string, orderedValue []byte) []byte {
	return buildPrefix(indexPrefix+"t", tenant, schema, property, "datetime", hex.EncodeToString(orderedValue))
}

func PropertyIndexDateTimeValueUpperBound(tenant, schema, property string, orderedValue []byte) []byte {
	return append(PropertyIndexDateTimeValuePrefix(tenant, schema, property, orderedValue), 0xFF)
}

func PropertyIndexNumericKey(tenant, schema, property string, orderedValue []byte, entityID string) []byte {
	return buildKey(indexNumPrefix, tenant, schema, property, "numeric", hex.EncodeToString(orderedValue), entityID)
}

func PropertyIndexNumericValuePrefix(tenant, schema, property string, orderedValue []byte) []byte {
	return buildPrefix(indexNumPrefix, tenant, schema, property, "numeric", hex.EncodeToString(orderedValue))
}

func PropertyIndexNumericValueUpperBound(tenant, schema, property string, orderedValue []byte) []byte {
	return append(PropertyIndexNumericValuePrefix(tenant, schema, property, orderedValue), 0xFF)
}

func StatsVertexTotalKey(tenant string) []byte {
	return buildKey(statsPrefix, tenant, "vertex_total")
}

func StatsEdgeTotalKey(tenant string) []byte {
	return buildKey(statsPrefix, tenant, "edge_total")
}

func StatsVertexLabelCountKey(tenant, label string) []byte {
	return buildKey(statsPrefix, tenant, "label", label)
}

func StatsVertexLabelPrefix(tenant string) []byte {
	return buildPrefix(statsPrefix, tenant, "label")
}

func StatsEdgeTypeCountKey(tenant, edgeType string) []byte {
	return buildKey(statsPrefix, tenant, "edge_type", edgeType)
}

func StatsEdgeTypePrefix(tenant string) []byte {
	return buildPrefix(statsPrefix, tenant, "edge_type")
}

func StatsEdgeTypeSourceCountKey(tenant, edgeType string) []byte {
	return buildKey(statsPrefix, tenant, "edge_type_sources", edgeType)
}

func StatsEdgeTypeSourceCountPrefix(tenant string) []byte {
	return buildPrefix(statsPrefix, tenant, "edge_type_sources")
}

func StatsEdgeTypeSourceDegreeKey(tenant, edgeType, srcID string) []byte {
	return buildKey(statsPrefix, tenant, "edge_type_src_degree", edgeType, srcID)
}

func StatsEdgeTypeSourceDegreePrefix(tenant string) []byte {
	return buildPrefix(statsPrefix, tenant, "edge_type_src_degree")
}

func StatsEdgeTypeSourceDegreeTypePrefix(tenant, edgeType string) []byte {
	return buildPrefix(statsPrefix, tenant, "edge_type_src_degree", edgeType)
}

func StatsEpochKey(tenant string) []byte {
	return buildKey(statsPrefix, tenant, "epoch")
}

func StatsSampleSizeKey(tenant string) []byte {
	return buildKey(statsPrefix, tenant, "sample_size")
}

func StatsLastRefreshKey(tenant string) []byte {
	return buildKey(statsPrefix, tenant, "last_refresh_ts")
}

func StatsVertexPropertyDistinctCountKey(tenant, schema, property string) []byte {
	return buildKey(statsPrefix, tenant, "vertex_property_ndv", schema, property)
}

func StatsVertexPropertyDistinctCountByKindKey(tenant, schema, property, kind string) []byte {
	return buildKey(statsPrefix, tenant, "vertex_property_ndv_kind", schema, property, kind)
}

func StatsVertexPropertyDistinctCountPrefix(tenant string) []byte {
	return buildPrefix(statsPrefix, tenant, "vertex_property_ndv")
}

func StatsVertexPropertyDistinctCountByKindPrefix(tenant string) []byte {
	return buildPrefix(statsPrefix, tenant, "vertex_property_ndv_kind")
}

func StatsVertexPropertyEntryCountKey(tenant, schema, property string) []byte {
	return buildKey(statsPrefix, tenant, "vertex_property_entries", schema, property)
}

func StatsVertexPropertyEntryCountByKindKey(tenant, schema, property, kind string) []byte {
	return buildKey(statsPrefix, tenant, "vertex_property_entries_kind", schema, property, kind)
}

func StatsVertexPropertyEntryCountPrefix(tenant string) []byte {
	return buildPrefix(statsPrefix, tenant, "vertex_property_entries")
}

func StatsVertexPropertyEntryCountByKindPrefix(tenant string) []byte {
	return buildPrefix(statsPrefix, tenant, "vertex_property_entries_kind")
}

func StatsVertexPropertyEpochKey(tenant, schema, property string) []byte {
	return buildKey(statsPrefix, tenant, "vertex_property_epoch", schema, property)
}

func StatsVertexPropertyEpochPrefix(tenant string) []byte {
	return buildPrefix(statsPrefix, tenant, "vertex_property_epoch")
}

func StatsVertexPropertySampleSizeKey(tenant, schema, property string) []byte {
	return buildKey(statsPrefix, tenant, "vertex_property_sample_size", schema, property)
}

func StatsVertexPropertySampleSizePrefix(tenant string) []byte {
	return buildPrefix(statsPrefix, tenant, "vertex_property_sample_size")
}

func StatsVertexPropertyLastRefreshKey(tenant, schema, property string) []byte {
	return buildKey(statsPrefix, tenant, "vertex_property_last_refresh_ts", schema, property)
}

func StatsVertexPropertyLastRefreshPrefix(tenant string) []byte {
	return buildPrefix(statsPrefix, tenant, "vertex_property_last_refresh_ts")
}

func StatsEdgePropertyDistinctCountKey(tenant, schema, property string) []byte {
	return buildKey(statsPrefix, tenant, "edge_property_ndv", schema, property)
}

func StatsEdgePropertyDistinctCountByKindKey(tenant, schema, property, kind string) []byte {
	return buildKey(statsPrefix, tenant, "edge_property_ndv_kind", schema, property, kind)
}

func StatsEdgePropertyDistinctCountPrefix(tenant string) []byte {
	return buildPrefix(statsPrefix, tenant, "edge_property_ndv")
}

func StatsEdgePropertyDistinctCountByKindPrefix(tenant string) []byte {
	return buildPrefix(statsPrefix, tenant, "edge_property_ndv_kind")
}

func StatsEdgePropertyEntryCountKey(tenant, schema, property string) []byte {
	return buildKey(statsPrefix, tenant, "edge_property_entries", schema, property)
}

func StatsEdgePropertyEntryCountByKindKey(tenant, schema, property, kind string) []byte {
	return buildKey(statsPrefix, tenant, "edge_property_entries_kind", schema, property, kind)
}

func StatsEdgePropertyEntryCountPrefix(tenant string) []byte {
	return buildPrefix(statsPrefix, tenant, "edge_property_entries")
}

func StatsEdgePropertyEntryCountByKindPrefix(tenant string) []byte {
	return buildPrefix(statsPrefix, tenant, "edge_property_entries_kind")
}

func StatsEdgePropertyEpochKey(tenant, schema, property string) []byte {
	return buildKey(statsPrefix, tenant, "edge_property_epoch", schema, property)
}

func StatsEdgePropertyEpochPrefix(tenant string) []byte {
	return buildPrefix(statsPrefix, tenant, "edge_property_epoch")
}

func StatsEdgePropertySampleSizeKey(tenant, schema, property string) []byte {
	return buildKey(statsPrefix, tenant, "edge_property_sample_size", schema, property)
}

func StatsEdgePropertySampleSizePrefix(tenant string) []byte {
	return buildPrefix(statsPrefix, tenant, "edge_property_sample_size")
}

func StatsEdgePropertyLastRefreshKey(tenant, schema, property string) []byte {
	return buildKey(statsPrefix, tenant, "edge_property_last_refresh_ts", schema, property)
}

func StatsEdgePropertyLastRefreshPrefix(tenant string) []byte {
	return buildPrefix(statsPrefix, tenant, "edge_property_last_refresh_ts")
}

func StatsVertexPropertyHistogramKey(tenant, schema, property, kind string, bucket int) []byte {
	return buildKey(statsPrefix, tenant, "vertex_property_hist", schema, property, kind, encodeStatsBucket(bucket))
}

func StatsVertexPropertyHistogramPrefix(tenant string) []byte {
	return buildPrefix(statsPrefix, tenant, "vertex_property_hist")
}

func StatsEdgePropertyHistogramKey(tenant, schema, property, kind string, bucket int) []byte {
	return buildKey(statsPrefix, tenant, "edge_property_hist", schema, property, kind, encodeStatsBucket(bucket))
}

func StatsEdgePropertyHistogramPrefix(tenant string) []byte {
	return buildPrefix(statsPrefix, tenant, "edge_property_hist")
}

func SchemaVersionKey() []byte {
	return buildKey(metaPrefix, "schema_version")
}

func buildKey(parts ...string) []byte {
	return []byte(strings.Join(parts, "/"))
}

func buildPrefix(parts ...string) []byte {
	return []byte(strings.Join(parts, "/") + "/")
}

func encodeStatsBucket(bucket int) string {
	if bucket < 0 {
		bucket = 0
	}
	return strings.ToUpper(hex.EncodeToString([]byte{
		byte((bucket >> 24) & 0xFF),
		byte((bucket >> 16) & 0xFF),
		byte((bucket >> 8) & 0xFF),
		byte(bucket & 0xFF),
	}))
}
