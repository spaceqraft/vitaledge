package keyspace

import "testing"

func TestVertexAndEdgeKeys(t *testing.T) {
	if got := string(VertexKey("t1", "v1")); got != "v/t1/v1" {
		t.Fatalf("unexpected vertex key: %s", got)
	}
	if got := string(VertexLabelMembershipKey("t1", "Person", "v1")); got != "vl/t1/Person/v1" {
		t.Fatalf("unexpected vertex label membership key: %s", got)
	}
	if got := string(EdgeKey("t1", "e1")); got != "e/t1/e1" {
		t.Fatalf("unexpected edge key: %s", got)
	}
}

func TestAdjacencyPrefixes(t *testing.T) {
	if got := string(OutAdjacencyPrefix("t1", "src", "")); got != "a/out/t1/src/" {
		t.Fatalf("unexpected out adjacency prefix: %s", got)
	}
	if got := string(InAdjacencyPrefix("t1", "dst", "LIKES")); got != "a/in/t1/dst/LIKES/" {
		t.Fatalf("unexpected in adjacency prefix: %s", got)
	}
	if got := string(OutEndpointPrefix("t1", "src", "KNOWS", "dst")); got != "a/out_ep/t1/src/KNOWS/dst/" {
		t.Fatalf("unexpected out endpoint prefix: %s", got)
	}
	if got := string(OutEndpointKey("t1", "src", "KNOWS", "dst", "e1")); got != "a/out_ep/t1/src/KNOWS/dst/e1" {
		t.Fatalf("unexpected out endpoint key: %s", got)
	}
	if got := string(OutEndpointPairCountKey("t1", "src", "KNOWS", "dst")); got != "a/out_epc/t1/src/KNOWS/dst" {
		t.Fatalf("unexpected out endpoint pair count key: %s", got)
	}
	if got := string(UndirectedEndpointPairCountKey("t1", "left", "KNOWS", "right")); got != "a/und_epc/t1/left/KNOWS/right" {
		t.Fatalf("unexpected undirected endpoint pair count key: %s", got)
	}
}

func TestPropertyIndexKey(t *testing.T) {
	key := string(PropertyIndexKey("t1", "Person", "email", []byte("a@b"), "v1"))
	if key != "i/t1/Person/email/614062/v1" {
		t.Fatalf("unexpected property index key: %s", key)
	}
}

func TestPropertyIndexNumericKeys(t *testing.T) {
	if got := string(PropertyIndexNumericPrefix("t1", "RATED", "rating")); got != "in/t1/RATED/rating/" {
		t.Fatalf("unexpected numeric property index prefix: %s", got)
	}
	if got := string(PropertyIndexNumericKey("t1", "RATED", "rating", []byte{0x01, 0xff}, "e1")); got != "in/t1/RATED/rating/01ff/e1" {
		t.Fatalf("unexpected numeric property index key: %s", got)
	}
	if got := string(PropertyIndexNumericValuePrefix("t1", "RATED", "rating", []byte{0x01, 0xff})); got != "in/t1/RATED/rating/01ff/" {
		t.Fatalf("unexpected numeric property index value prefix: %s", got)
	}
}

func TestStatsKeys(t *testing.T) {
	if got := string(StatsVertexTotalKey("t1")); got != "s/t1/vertex_total" {
		t.Fatalf("unexpected vertex total stats key: %s", got)
	}
	if got := string(StatsEdgeTotalKey("t1")); got != "s/t1/edge_total" {
		t.Fatalf("unexpected edge total stats key: %s", got)
	}
	if got := string(StatsVertexLabelCountKey("t1", "Movie")); got != "s/t1/label/Movie" {
		t.Fatalf("unexpected label stats key: %s", got)
	}
	if got := string(StatsEdgeTypeCountKey("t1", "RATED")); got != "s/t1/edge_type/RATED" {
		t.Fatalf("unexpected edge type stats key: %s", got)
	}
	if got := string(StatsVertexLabelPrefix("t1")); got != "s/t1/label/" {
		t.Fatalf("unexpected label stats prefix: %s", got)
	}
	if got := string(StatsEdgeTypePrefix("t1")); got != "s/t1/edge_type/" {
		t.Fatalf("unexpected edge type stats prefix: %s", got)
	}
	if got := string(SchemaVersionKey()); got != "m/schema_version" {
		t.Fatalf("unexpected schema version key: %s", got)
	}
}
