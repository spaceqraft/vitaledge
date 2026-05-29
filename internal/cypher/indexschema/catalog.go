package indexschema

import "sync"

type PropertyIndex struct {
	Tenant   string
	Schema   string
	Property string
}

type Catalog struct {
	mu              sync.RWMutex
	propertyIndexes map[PropertyIndex]struct{}
}

func NewCatalog() *Catalog {
	return &Catalog{propertyIndexes: map[PropertyIndex]struct{}{}}
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
