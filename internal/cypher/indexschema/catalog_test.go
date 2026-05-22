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
