package indexschema

import (
	"sort"
	"sync"
)

type PropertyIndex struct {
	Tenant   string
	Schema   string
	Property string
}

type EdgePropertyIndex struct {
	Tenant   string
	EdgeType string
	Property string
}

type Catalog struct {
	mu                  sync.RWMutex
	propertyIndexes     map[PropertyIndex]struct{}
	edgePropertyIndexes map[EdgePropertyIndex]struct{}
}

func NewCatalog() *Catalog {
	return &Catalog{
		propertyIndexes:     map[PropertyIndex]struct{}{},
		edgePropertyIndexes: map[EdgePropertyIndex]struct{}{},
	}
}

func (c *Catalog) AddPropertyIndex(tenant, schema, property string) bool {
	if c == nil || tenant == "" || schema == "" || property == "" {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.propertyIndexes == nil {
		c.propertyIndexes = map[PropertyIndex]struct{}{}
	}
	key := PropertyIndex{Tenant: tenant, Schema: schema, Property: property}
	if _, exists := c.propertyIndexes[key]; exists {
		return false
	}
	c.propertyIndexes[key] = struct{}{}
	return true
}

func (c *Catalog) HasPropertyIndex(tenant, schema, property string) bool {
	if c == nil {
		return false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.propertyIndexes[PropertyIndex{Tenant: tenant, Schema: schema, Property: property}]
	return ok
}

func (c *Catalog) RemovePropertyIndex(tenant, schema, property string) bool {
	if c == nil || tenant == "" || schema == "" || property == "" {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	key := PropertyIndex{Tenant: tenant, Schema: schema, Property: property}
	if _, exists := c.propertyIndexes[key]; !exists {
		return false
	}
	delete(c.propertyIndexes, key)
	return true
}

func (c *Catalog) AddEdgePropertyIndex(tenant, edgeType, property string) bool {
	if c == nil || tenant == "" || edgeType == "" || property == "" {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.edgePropertyIndexes == nil {
		c.edgePropertyIndexes = map[EdgePropertyIndex]struct{}{}
	}
	key := EdgePropertyIndex{Tenant: tenant, EdgeType: edgeType, Property: property}
	if _, exists := c.edgePropertyIndexes[key]; exists {
		return false
	}
	c.edgePropertyIndexes[key] = struct{}{}
	return true
}

func (c *Catalog) HasEdgePropertyIndex(tenant, edgeType, property string) bool {
	if c == nil {
		return false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.edgePropertyIndexes[EdgePropertyIndex{Tenant: tenant, EdgeType: edgeType, Property: property}]
	return ok
}

func (c *Catalog) RemoveEdgePropertyIndex(tenant, edgeType, property string) bool {
	if c == nil || tenant == "" || edgeType == "" || property == "" {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	key := EdgePropertyIndex{Tenant: tenant, EdgeType: edgeType, Property: property}
	if _, exists := c.edgePropertyIndexes[key]; !exists {
		return false
	}
	delete(c.edgePropertyIndexes, key)
	return true
}

func (c *Catalog) PropertyIndexes() []PropertyIndex {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]PropertyIndex, 0, len(c.propertyIndexes))
	for idx := range c.propertyIndexes {
		out = append(out, idx)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Tenant != out[j].Tenant {
			return out[i].Tenant < out[j].Tenant
		}
		if out[i].Schema != out[j].Schema {
			return out[i].Schema < out[j].Schema
		}
		return out[i].Property < out[j].Property
	})
	return out
}

func (c *Catalog) EdgePropertyIndexes() []EdgePropertyIndex {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]EdgePropertyIndex, 0, len(c.edgePropertyIndexes))
	for idx := range c.edgePropertyIndexes {
		out = append(out, idx)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Tenant != out[j].Tenant {
			return out[i].Tenant < out[j].Tenant
		}
		if out[i].EdgeType != out[j].EdgeType {
			return out[i].EdgeType < out[j].EdgeType
		}
		return out[i].Property < out[j].Property
	})
	return out
}
