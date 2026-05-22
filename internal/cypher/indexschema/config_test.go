package indexschema

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestCatalogFromConfig(t *testing.T) {
	catalog, err := CatalogFromConfig(Config{
		PropertyIndexes: []PropertyIndexConfig{
			{Tenant: "acme", Schema: "User", Property: "email"},
			{Tenant: "acme", Schema: "Device", Property: "serial"},
		},
	})
	if err != nil {
		t.Fatalf("CatalogFromConfig failed: %v", err)
	}

	if !catalog.HasPropertyIndex("acme", "User", "email") {
		t.Fatalf("expected User.email index")
	}
	if !catalog.HasPropertyIndex("acme", "Device", "serial") {
		t.Fatalf("expected Device.serial index")
	}
}

func TestCatalogFromConfigRejectsInvalidEntry(t *testing.T) {
	_, err := CatalogFromConfig(Config{
		PropertyIndexes: []PropertyIndexConfig{{Tenant: "acme", Schema: "User", Property: ""}},
	})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig, got %v", err)
	}
}

func TestParseConfigRejectsUnknownFields(t *testing.T) {
	_, err := ParseConfig([]byte(`{"property_indexes":[{"tenant":"acme","schema":"User","property":"email","unknown":true}]}`))
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig, got %v", err)
	}
}

func TestLoadCatalogFromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "indexes.json")
	if err := os.WriteFile(path, []byte(`
{
  "property_indexes": [
    { "tenant": "acme", "schema": "User", "property": "email" }
  ]
}`), 0o644); err != nil {
		t.Fatalf("write file failed: %v", err)
	}

	catalog, err := LoadCatalogFromFile(path)
	if err != nil {
		t.Fatalf("LoadCatalogFromFile failed: %v", err)
	}
	if !catalog.HasPropertyIndex("acme", "User", "email") {
		t.Fatalf("expected loaded index")
	}
}
