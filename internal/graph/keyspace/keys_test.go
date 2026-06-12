package keyspace

import "testing"

func TestVertexAndEdgeKeys(t *testing.T) {
	if got := string(VertexKey("t1", "v1")); got != "v/t1/v1" {
		t.Fatalf("unexpected vertex key: %s", got)
	}
	if got := string(VertexLabelMembershipKey("t1", "Person", "v1")); got != "lv/t1/Person/v1" {
		t.Fatalf("unexpected vertex label membership key: %s", got)
	}
	if got := string(VertexLabelKey("t1", "v1", "Person")); got != "vl/t1/v1/Person" {
		t.Fatalf("unexpected vertex label key: %s", got)
	}
	if got := string(LabelVertexKey("t1", "Person", "v1")); got != "lv/t1/Person/v1" {
		t.Fatalf("unexpected label vertex key: %s", got)
	}
	if got := string(VertexLabelPrefix("t1", "v1")); got != "vl/t1/v1/" {
		t.Fatalf("unexpected vertex label prefix: %s", got)
	}
	if got := string(LabelVertexPrefix("t1", "Person")); got != "lv/t1/Person/" {
		t.Fatalf("unexpected label vertex prefix: %s", got)
	}
	if got := string(EdgeKey("t1", "e1")); got != "e/t1/e1" {
		t.Fatalf("unexpected edge key: %s", got)
	}
	if got := string(EdgeTypeKey("t1", "e1", "KNOWS")); got != "et/t1/e1/KNOWS" {
		t.Fatalf("unexpected edge type key: %s", got)
	}
	if got := string(TypeEdgeKey("t1", "KNOWS", "e1")); got != "te/t1/KNOWS/e1" {
		t.Fatalf("unexpected type edge key: %s", got)
	}
	if got := string(TypeEdgePrefix("t1", "KNOWS")); got != "te/t1/KNOWS/" {
		t.Fatalf("unexpected type edge prefix: %s", got)
	}
}

func TestAdjacencyPrefixes(t *testing.T) {
	if got := string(OutAdjacencyPrefix("t1", "src", "")); got != "rf/t1/src/" {
		t.Fatalf("unexpected out adjacency prefix: %s", got)
	}
	if got := string(InAdjacencyPrefix("t1", "dst", "LIKES")); got != "rt/t1/dst/LIKES/" {
		t.Fatalf("unexpected in adjacency prefix: %s", got)
	}
	if got := string(OutEndpointPrefix("t1", "src", "KNOWS", "dst")); got != "od/t1/src/KNOWS/dst/" {
		t.Fatalf("unexpected out endpoint prefix: %s", got)
	}
	if got := string(OutEndpointKey("t1", "src", "KNOWS", "dst", "e1")); got != "od/t1/src/KNOWS/dst/e1" {
		t.Fatalf("unexpected out endpoint key: %s", got)
	}
	if got := string(OutEndpointPairCountKey("t1", "src", "KNOWS", "dst")); got != "odc/t1/src/KNOWS/dst" {
		t.Fatalf("unexpected out endpoint pair count key: %s", got)
	}
	if got := string(UndirectedEndpointPairCountKey("t1", "left", "KNOWS", "right")); got != "udc/t1/left/KNOWS/right" {
		t.Fatalf("unexpected undirected endpoint pair count key: %s", got)
	}
}

func TestPropertyIndexKey(t *testing.T) {
	key := string(PropertyIndexKey("t1", "Person", "email", "string", []byte("a@b"), "v1"))
	if key != "pi/t1/Person/email/string/614062/v1" {
		t.Fatalf("unexpected property index key: %s", key)
	}
}

func TestPropertyIndexNumericKeys(t *testing.T) {
	if got := string(PropertyIndexNumericPrefix("t1", "RATED", "rating")); got != "pn/t1/RATED/rating/numeric/" {
		t.Fatalf("unexpected numeric property index prefix: %s", got)
	}
	if got := string(PropertyIndexNumericKey("t1", "RATED", "rating", []byte{0x01, 0xff}, "e1")); got != "pn/t1/RATED/rating/numeric/01ff/e1" {
		t.Fatalf("unexpected numeric property index key: %s", got)
	}
	if got := string(PropertyIndexNumericValuePrefix("t1", "RATED", "rating", []byte{0x01, 0xff})); got != "pn/t1/RATED/rating/numeric/01ff/" {
		t.Fatalf("unexpected numeric property index value prefix: %s", got)
	}
}

func TestPropertyEntityReverseKeys(t *testing.T) {
	if got := string(VertexPropertyKey("t1", "v1", "Person", "email", []byte("a@b"))); got != "vp/t1/v1/Person/email/614062" {
		t.Fatalf("unexpected vertex property key: %s", got)
	}
	if got := string(VertexPropertyPrefix("t1", "v1", "Person", "email")); got != "vp/t1/v1/Person/email/" {
		t.Fatalf("unexpected vertex property prefix: %s", got)
	}
	if got := string(PropertyVertexKey("t1", "Person", "email", []byte("a@b"), "v1")); got != "pv/t1/Person/email/614062/v1" {
		t.Fatalf("unexpected property vertex key: %s", got)
	}
	if got := string(PropertyVertexPrefix("t1", "Person", "email")); got != "pv/t1/Person/email/" {
		t.Fatalf("unexpected property vertex prefix: %s", got)
	}
	if got := string(EdgePropertyKey("t1", "e1", "RATED", "rating", []byte("4.5"))); got != "ep/t1/e1/RATED/rating/342e35" {
		t.Fatalf("unexpected edge property key: %s", got)
	}
	if got := string(EdgePropertyPrefix("t1", "e1", "RATED", "rating")); got != "ep/t1/e1/RATED/rating/" {
		t.Fatalf("unexpected edge property prefix: %s", got)
	}
	if got := string(PropertyEdgeKey("t1", "RATED", "rating", []byte("4.5"), "e1")); got != "pe/t1/RATED/rating/342e35/e1" {
		t.Fatalf("unexpected property edge key: %s", got)
	}
	if got := string(PropertyEdgePrefix("t1", "RATED", "rating")); got != "pe/t1/RATED/rating/" {
		t.Fatalf("unexpected property edge prefix: %s", got)
	}
}

func TestStatsKeys(t *testing.T) {
	if got := string(StatsVertexTotalKey("t1")); got != "s/t1/vertex_total" {
		t.Fatalf("unexpected vertex total stats key: %s", got)
	}
	if got := string(StatsEdgeTotalKey("t1")); got != "s/t1/edge_total" {
		t.Fatalf("unexpected edge total stats key: %s", got)
	}
	if got := string(StatsEpochKey("t1")); got != "s/t1/epoch" {
		t.Fatalf("unexpected stats epoch key: %s", got)
	}
	if got := string(StatsSampleSizeKey("t1")); got != "s/t1/sample_size" {
		t.Fatalf("unexpected stats sample size key: %s", got)
	}
	if got := string(StatsLastRefreshKey("t1")); got != "s/t1/last_refresh_ts" {
		t.Fatalf("unexpected stats last refresh key: %s", got)
	}
	if got := string(StatsVertexLabelCountKey("t1", "Movie")); got != "s/t1/label/Movie" {
		t.Fatalf("unexpected label stats key: %s", got)
	}
	if got := string(StatsEdgeTypeCountKey("t1", "RATED")); got != "s/t1/edge_type/RATED" {
		t.Fatalf("unexpected edge type stats key: %s", got)
	}
	if got := string(StatsVertexPropertyDistinctCountKey("t1", "User", "email")); got != "s/t1/vertex_property_ndv/User/email" {
		t.Fatalf("unexpected vertex property ndv key: %s", got)
	}
	if got := string(StatsVertexPropertyDistinctCountByKindKey("t1", "User", "email", "boolean")); got != "s/t1/vertex_property_ndv_kind/User/email/boolean" {
		t.Fatalf("unexpected vertex property ndv-by-kind key: %s", got)
	}
	if got := string(StatsVertexPropertyEntryCountKey("t1", "User", "email")); got != "s/t1/vertex_property_entries/User/email" {
		t.Fatalf("unexpected vertex property entry count key: %s", got)
	}
	if got := string(StatsVertexPropertyEntryCountByKindKey("t1", "User", "email", "categorical")); got != "s/t1/vertex_property_entries_kind/User/email/categorical" {
		t.Fatalf("unexpected vertex property entry-by-kind key: %s", got)
	}
	if got := string(StatsVertexPropertyEpochKey("t1", "User", "email")); got != "s/t1/vertex_property_epoch/User/email" {
		t.Fatalf("unexpected vertex property epoch key: %s", got)
	}
	if got := string(StatsVertexPropertySampleSizeKey("t1", "User", "email")); got != "s/t1/vertex_property_sample_size/User/email" {
		t.Fatalf("unexpected vertex property sample-size key: %s", got)
	}
	if got := string(StatsVertexPropertyLastRefreshKey("t1", "User", "email")); got != "s/t1/vertex_property_last_refresh_ts/User/email" {
		t.Fatalf("unexpected vertex property refresh key: %s", got)
	}
	if got := string(StatsEdgePropertyDistinctCountKey("t1", "RATED", "score")); got != "s/t1/edge_property_ndv/RATED/score" {
		t.Fatalf("unexpected edge property ndv key: %s", got)
	}
	if got := string(StatsEdgePropertyDistinctCountByKindKey("t1", "RATED", "score", "numeric")); got != "s/t1/edge_property_ndv_kind/RATED/score/numeric" {
		t.Fatalf("unexpected edge property ndv-by-kind key: %s", got)
	}
	if got := string(StatsEdgePropertyEntryCountKey("t1", "RATED", "score")); got != "s/t1/edge_property_entries/RATED/score" {
		t.Fatalf("unexpected edge property entry count key: %s", got)
	}
	if got := string(StatsEdgePropertyEntryCountByKindKey("t1", "RATED", "score", "datetime")); got != "s/t1/edge_property_entries_kind/RATED/score/datetime" {
		t.Fatalf("unexpected edge property entry-by-kind key: %s", got)
	}
	if got := string(StatsEdgePropertyEpochKey("t1", "RATED", "score")); got != "s/t1/edge_property_epoch/RATED/score" {
		t.Fatalf("unexpected edge property epoch key: %s", got)
	}
	if got := string(StatsEdgePropertySampleSizeKey("t1", "RATED", "score")); got != "s/t1/edge_property_sample_size/RATED/score" {
		t.Fatalf("unexpected edge property sample-size key: %s", got)
	}
	if got := string(StatsEdgePropertyLastRefreshKey("t1", "RATED", "score")); got != "s/t1/edge_property_last_refresh_ts/RATED/score" {
		t.Fatalf("unexpected edge property refresh key: %s", got)
	}
	if got := string(StatsVertexPropertyHistogramKey("t1", "User", "age", "numeric", 7)); got != "s/t1/vertex_property_hist/User/age/numeric/00000007" {
		t.Fatalf("unexpected vertex property histogram key: %s", got)
	}
	if got := string(StatsEdgePropertyHistogramKey("t1", "RATED", "createdAt", "datetime", 12)); got != "s/t1/edge_property_hist/RATED/createdAt/datetime/0000000C" {
		t.Fatalf("unexpected edge property histogram key: %s", got)
	}
	if got := string(StatsVertexLabelPrefix("t1")); got != "s/t1/label/" {
		t.Fatalf("unexpected label stats prefix: %s", got)
	}
	if got := string(StatsEdgeTypePrefix("t1")); got != "s/t1/edge_type/" {
		t.Fatalf("unexpected edge type stats prefix: %s", got)
	}
	if got := string(StatsVertexPropertyDistinctCountPrefix("t1")); got != "s/t1/vertex_property_ndv/" {
		t.Fatalf("unexpected vertex property ndv prefix: %s", got)
	}
	if got := string(StatsVertexPropertyDistinctCountByKindPrefix("t1")); got != "s/t1/vertex_property_ndv_kind/" {
		t.Fatalf("unexpected vertex property ndv-by-kind prefix: %s", got)
	}
	if got := string(StatsVertexPropertyEntryCountByKindPrefix("t1")); got != "s/t1/vertex_property_entries_kind/" {
		t.Fatalf("unexpected vertex property entries-by-kind prefix: %s", got)
	}
	if got := string(StatsVertexPropertyEpochPrefix("t1")); got != "s/t1/vertex_property_epoch/" {
		t.Fatalf("unexpected vertex property epoch prefix: %s", got)
	}
	if got := string(StatsVertexPropertySampleSizePrefix("t1")); got != "s/t1/vertex_property_sample_size/" {
		t.Fatalf("unexpected vertex property sample-size prefix: %s", got)
	}
	if got := string(StatsVertexPropertyLastRefreshPrefix("t1")); got != "s/t1/vertex_property_last_refresh_ts/" {
		t.Fatalf("unexpected vertex property refresh prefix: %s", got)
	}
	if got := string(StatsEdgePropertyEntryCountPrefix("t1")); got != "s/t1/edge_property_entries/" {
		t.Fatalf("unexpected edge property entry count prefix: %s", got)
	}
	if got := string(StatsEdgePropertyDistinctCountByKindPrefix("t1")); got != "s/t1/edge_property_ndv_kind/" {
		t.Fatalf("unexpected edge property ndv-by-kind prefix: %s", got)
	}
	if got := string(StatsEdgePropertyEntryCountByKindPrefix("t1")); got != "s/t1/edge_property_entries_kind/" {
		t.Fatalf("unexpected edge property entries-by-kind prefix: %s", got)
	}
	if got := string(StatsEdgePropertyEpochPrefix("t1")); got != "s/t1/edge_property_epoch/" {
		t.Fatalf("unexpected edge property epoch prefix: %s", got)
	}
	if got := string(StatsEdgePropertySampleSizePrefix("t1")); got != "s/t1/edge_property_sample_size/" {
		t.Fatalf("unexpected edge property sample-size prefix: %s", got)
	}
	if got := string(StatsEdgePropertyLastRefreshPrefix("t1")); got != "s/t1/edge_property_last_refresh_ts/" {
		t.Fatalf("unexpected edge property refresh prefix: %s", got)
	}
	if got := string(StatsVertexPropertyHistogramPrefix("t1")); got != "s/t1/vertex_property_hist/" {
		t.Fatalf("unexpected vertex property histogram prefix: %s", got)
	}
	if got := string(StatsEdgePropertyHistogramPrefix("t1")); got != "s/t1/edge_property_hist/" {
		t.Fatalf("unexpected edge property histogram prefix: %s", got)
	}
	if got := string(SchemaVersionKey()); got != "m/schema_version" {
		t.Fatalf("unexpected schema version key: %s", got)
	}
}
