package indexschema

import "testing"

func TestCatalogPropertyIndexMembership(t *testing.T) {
	catalog := NewCatalog()
	if catalog.HasPropertyIndex("acme", "User", "email") {
		t.Fatalf("expected empty catalog to miss")
	}

	catalog.AddPropertyIndex("acme", "User", "email")
	if !catalog.HasPropertyIndex("acme", "User", "email") {
		t.Fatalf("expected catalog to contain index")
	}
	if catalog.HasPropertyIndex("acme", "User", "name") {
		t.Fatalf("expected unrelated index to miss")
	}
}

func TestCatalogEdgePropertyIndexMembership(t *testing.T) {
	catalog := NewCatalog()
	if catalog.HasEdgePropertyIndex("acme", "RATED", "rating") {
		t.Fatalf("expected empty catalog to miss edge index")
	}

	if !catalog.AddEdgePropertyIndex("acme", "RATED", "rating") {
		t.Fatalf("expected edge index to be added")
	}
	if !catalog.HasEdgePropertyIndex("acme", "RATED", "rating") {
		t.Fatalf("expected catalog to contain edge index")
	}
	if catalog.HasEdgePropertyIndex("acme", "RATED", "timestamp") {
		t.Fatalf("expected unrelated edge index to miss")
	}

	entries := catalog.EdgePropertyIndexes()
	if len(entries) != 1 {
		t.Fatalf("expected one edge property index, got %d", len(entries))
	}
	if entries[0].Tenant != "acme" || entries[0].EdgeType != "RATED" || entries[0].Property != "rating" {
		t.Fatalf("unexpected edge property index entry: %#v", entries[0])
	}
}
