package keyspace

import "testing"

func TestVertexAndEdgeKeys(t *testing.T) {
	if got := string(VertexKey("t1", "v1")); got != "v/t1/v1" {
		t.Fatalf("unexpected vertex key: %s", got)
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
}

func TestPropertyIndexKey(t *testing.T) {
	key := string(PropertyIndexKey("t1", "Person", "email", []byte("a@b"), "v1"))
	if key != "i/t1/Person/email/614062/v1" {
		t.Fatalf("unexpected property index key: %s", key)
	}
}
