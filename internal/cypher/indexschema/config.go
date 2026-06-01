package indexschema

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

var ErrInvalidConfig = errors.New("invalid index schema config")

type Config struct {
	PropertyIndexes     []PropertyIndexConfig     `json:"property_indexes"`
	EdgePropertyIndexes []EdgePropertyIndexConfig `json:"edge_property_indexes"`
}

type PropertyIndexConfig struct {
	Tenant   string `json:"tenant"`
	Schema   string `json:"schema"`
	Property string `json:"property"`
}

type EdgePropertyIndexConfig struct {
	Tenant   string `json:"tenant"`
	EdgeType string `json:"edge_type"`
	Property string `json:"property"`
}

func LoadConfigFromFile(path string) (Config, error) {
	if strings.TrimSpace(path) == "" {
		return Config{}, fmt.Errorf("%w: path is required", ErrInvalidConfig)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	return ParseConfig(raw)
}

func ParseConfig(raw []byte) (Config, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return Config{}, fmt.Errorf("%w: empty config", ErrInvalidConfig)
	}

	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()

	var cfg Config
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("%w: decode: %v", ErrInvalidConfig, err)
	}

	if err := ensureEOF(dec); err != nil {
		return Config{}, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}

	if err := validateConfig(cfg); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func LoadCatalogFromFile(path string) (*Catalog, error) {
	cfg, err := LoadConfigFromFile(path)
	if err != nil {
		return nil, err
	}
	return CatalogFromConfig(cfg)
}

func CatalogFromConfig(cfg Config) (*Catalog, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}

	catalog := NewCatalog()
	for _, idx := range cfg.PropertyIndexes {
		catalog.AddPropertyIndex(strings.TrimSpace(idx.Tenant), strings.TrimSpace(idx.Schema), strings.TrimSpace(idx.Property))
	}
	for _, idx := range cfg.EdgePropertyIndexes {
		catalog.AddEdgePropertyIndex(strings.TrimSpace(idx.Tenant), strings.TrimSpace(idx.EdgeType), strings.TrimSpace(idx.Property))
	}
	return catalog, nil
}

func validateConfig(cfg Config) error {
	for i, idx := range cfg.PropertyIndexes {
		if strings.TrimSpace(idx.Tenant) == "" || strings.TrimSpace(idx.Schema) == "" || strings.TrimSpace(idx.Property) == "" {
			return fmt.Errorf("%w: property_indexes[%d] requires tenant, schema, and property", ErrInvalidConfig, i)
		}
	}
	for i, idx := range cfg.EdgePropertyIndexes {
		if strings.TrimSpace(idx.Tenant) == "" || strings.TrimSpace(idx.EdgeType) == "" || strings.TrimSpace(idx.Property) == "" {
			return fmt.Errorf("%w: edge_property_indexes[%d] requires tenant, edge_type, and property", ErrInvalidConfig, i)
		}
	}
	return nil
}

func ensureEOF(dec *json.Decoder) error {
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("unexpected trailing JSON value")
		}
		return err
	}
	return nil
}
