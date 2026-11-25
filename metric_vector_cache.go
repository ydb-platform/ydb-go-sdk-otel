package ydb

import (
	"strings"
	"sync"
)

// Scopes sharing a meter share its vector registrations. The first registration
// fixes the name and unit; legacy label defaults only affect binding.
type vectorCache[V any] struct {
	mu      sync.Mutex
	entries map[string]*vectorRegistration[V]
}

type vectorRegistration[V any] struct {
	name    string
	unit    string
	bind    func([]string) V
	vectors map[string]V
}

type vectorSpec struct {
	name       string
	unit       string
	labelNames []string
}

func (c *vectorCache[V]) get(spec vectorSpec, create func(name, unit string) func([]string) V) V {
	entry := c.registration(spec)
	schema := strings.Join(spec.labelNames, "\xff")

	c.mu.Lock()
	bind := entry.bind
	c.mu.Unlock()
	if bind == nil {
		// Meter implementations may block or reenter the adapter.
		bind = create(entry.name, entry.unit)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if entry.bind == nil {
		entry.bind = bind
	}
	if vector, ok := entry.vectors[schema]; ok {
		return vector
	}
	vector := entry.bind(spec.labelNames)
	entry.vectors[schema] = vector

	return vector
}

func (c *vectorCache[V]) registration(spec vectorSpec) *vectorRegistration[V] {
	// OTel instrument names are case-insensitive.
	name := strings.ToLower(spec.name)
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry, ok := c.entries[name]; ok {
		return entry
	}
	if c.entries == nil {
		c.entries = make(map[string]*vectorRegistration[V])
	}
	entry := &vectorRegistration[V]{name: spec.name, unit: spec.unit, vectors: make(map[string]V)}
	c.entries[name] = entry

	return entry
}
