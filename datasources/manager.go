/*
SPDX-License-Identifier: Apache-2.0

Copyright 2024 The Taxinomia Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package datasources

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/google/taxinomia/core/tables"
	"google.golang.org/protobuf/encoding/prototext"
)

// Manager handles loading and caching of data sources.
// Annotations are loaded eagerly; data is loaded lazily on demand.
type Manager struct {
	mu sync.RWMutex

	// Annotations indexed by annotations_id - loaded eagerly
	annotations map[string]*ColumnAnnotations

	// Source metadata indexed by name - loaded eagerly
	sources map[string]*DataSource

	// Entity type definitions indexed by name - loaded eagerly
	entityTypes map[string]*EntityTypeDefinition

	// Cached tables indexed by source name - populated lazily
	tables map[string]*tables.DataTable

	// Registered loaders indexed by source_type
	loaders map[string]DataSourceLoader

	// Base directory for resolving relative paths
	baseDir string
}

// NewManager creates a new data source manager.
func NewManager() *Manager {
	return &Manager{
		annotations: make(map[string]*ColumnAnnotations),
		sources:     make(map[string]*DataSource),
		entityTypes: make(map[string]*EntityTypeDefinition),
		tables:      make(map[string]*tables.DataTable),
		loaders:     make(map[string]DataSourceLoader),
	}
}

// RegisterLoader registers a data source loader for a specific source type.
// If a loader is already registered for this type, it will be replaced.
func (m *Manager) RegisterLoader(loader DataSourceLoader) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.loaders[loader.SourceType()] = loader
}

// LoadConfig loads a DataSourcesConfig from a file.
// Annotations are loaded eagerly; source metadata is registered for lazy loading.
func (m *Manager) LoadConfig(configPath string) error {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("failed to read config file: %w", err)
	}

	config := &DataSourcesConfig{}
	if err := prototext.Unmarshal(data, config); err != nil {
		return fmt.Errorf("failed to parse config file: %w", err)
	}

	m.baseDir = filepath.Dir(configPath)

	return m.LoadConfigFromProto(config)
}

// LoadConfigFromProto loads configuration from a proto message.
// Annotations and entity types are loaded eagerly; source metadata is registered for lazy loading.
func (m *Manager) LoadConfigFromProto(config *DataSourcesConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Load all annotations (eager)
	for _, ann := range config.GetAnnotations() {
		m.annotations[ann.GetAnnotationsId()] = ann
	}

	// Register source metadata (data loaded lazily)
	for _, source := range config.GetSources() {
		m.sources[source.GetName()] = source
	}

	// Load entity type definitions (eager)
	for _, et := range config.GetEntityTypes() {
		m.entityTypes[et.GetName()] = et
	}

	return nil
}

// SetBaseDir sets the base directory for resolving relative paths in config.
func (m *Manager) SetBaseDir(dir string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.baseDir = dir
}

// GetAnnotations returns the annotations for a given annotations_id.
// Returns nil if the annotations are not found.
func (m *Manager) GetAnnotations(annotationsID string) *ColumnAnnotations {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.annotations[annotationsID]
}

// GetAnnotationIDs returns all registered annotation IDs.
func (m *Manager) GetAnnotationIDs() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ids := make([]string, 0, len(m.annotations))
	for id := range m.annotations {
		ids = append(ids, id)
	}
	return ids
}

// GetSourceNames returns all registered source names.
func (m *Manager) GetSourceNames() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	names := make([]string, 0, len(m.sources))
	for name := range m.sources {
		names = append(names, name)
	}
	return names
}

// GetSource returns the source metadata for a given name.
// Returns nil if the source is not found.
func (m *Manager) GetSource(name string) *DataSource {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sources[name]
}

// LoadData loads data for a source by name.
// Returns cached data if already loaded; otherwise loads from the source.
//
// The loading process:
// 1. Loader discovers schema from the data source (column names and types)
// 2. Manager enriches schema with annotations (display names, entity types)
// 3. Loader creates table with the enriched schema
func (m *Manager) LoadData(sourceName string) (*tables.DataTable, error) {
	// Check cache first (with read lock)
	m.mu.RLock()
	if table, ok := m.tables[sourceName]; ok {
		m.mu.RUnlock()
		return table, nil
	}
	source, ok := m.sources[sourceName]
	if !ok {
		m.mu.RUnlock()
		return nil, fmt.Errorf("source %q not found", sourceName)
	}
	annotations := m.annotations[source.GetAnnotationsId()]
	loader, hasLoader := m.loaders[source.GetSourceType()]
	baseDir := m.baseDir
	m.mu.RUnlock()

	// Check if loader is registered
	if !hasLoader {
		return nil, fmt.Errorf("no loader registered for source type %q", source.GetSourceType())
	}

	// Prepare config with resolved paths
	config := m.resolveConfigPaths(source.GetConfig(), baseDir)

	// Step 1: Discover schema from the data source
	schema, err := loader.DiscoverSchema(config)
	if err != nil {
		return nil, fmt.Errorf("failed to discover schema for source %q: %w", sourceName, err)
	}

	// Step 2: Enrich schema with annotations
	enrichedColumns := EnrichSchema(schema, annotations)

	// Step 3: Load data with enriched schema
	table, err := loader.Load(config, enrichedColumns)
	if err != nil {
		return nil, fmt.Errorf("failed to load source %q: %w", sourceName, err)
	}

	// Cache the result
	m.mu.Lock()
	m.tables[sourceName] = table
	m.mu.Unlock()

	return table, nil
}

// resolveConfigPaths resolves relative file paths in config to absolute paths.
func (m *Manager) resolveConfigPaths(config map[string]string, baseDir string) map[string]string {
	if baseDir == "" {
		return config
	}

	resolved := make(map[string]string, len(config))
	pathKeys := map[string]bool{
		"file_path":      true,
		"proto_file":     true,
		"descriptor_set": true,
	}

	for k, v := range config {
		if pathKeys[k] && v != "" && !filepath.IsAbs(v) {
			resolved[k] = filepath.Join(baseDir, v)
		} else {
			resolved[k] = v
		}
	}
	return resolved
}

// InvalidateCache removes a source from the cache, forcing reload on next access.
func (m *Manager) InvalidateCache(sourceName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.tables, sourceName)
}

// InvalidateAllCaches removes all sources from the cache.
func (m *Manager) InvalidateAllCaches() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tables = make(map[string]*tables.DataTable)
}

// IsLoaded returns whether data for a source is currently cached.
func (m *Manager) IsLoaded(sourceName string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.tables[sourceName]
	return ok
}

// GetLoadedSources returns names of all currently loaded (cached) sources.
func (m *Manager) GetLoadedSources() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	names := make([]string, 0, len(m.tables))
	for name := range m.tables {
		names = append(names, name)
	}
	return names
}

// AddAnnotations adds annotations to the manager.
func (m *Manager) AddAnnotations(annotations *ColumnAnnotations) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.annotations[annotations.GetAnnotationsId()] = annotations
}

// AddSource adds a source to the manager.
func (m *Manager) AddSource(source *DataSource) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sources[source.GetName()] = source
}

// FindJoinableColumns returns all columns across sources that share the given entity type.
func (m *Manager) FindJoinableColumns(entityType string) []JoinableColumn {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []JoinableColumn

	for sourceName, source := range m.sources {
		annotations := m.annotations[source.GetAnnotationsId()]
		if annotations == nil {
			continue
		}
		for _, col := range annotations.GetColumns() {
			if col.GetEntityType() == entityType {
				result = append(result, JoinableColumn{
					SourceName: sourceName,
					ColumnName: col.GetName(),
					EntityType: entityType,
				})
			}
		}
	}

	return result
}

// JoinableColumn represents a column that can be joined with other columns.
type JoinableColumn struct {
	SourceName string
	ColumnName string
	EntityType string
}

// GetAllEntityTypes returns all unique entity types defined across all annotations.
func (m *Manager) GetAllEntityTypes() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entityTypes := make(map[string]bool)
	for _, ann := range m.annotations {
		for _, col := range ann.GetColumns() {
			if et := col.GetEntityType(); et != "" {
				entityTypes[et] = true
			}
		}
	}

	result := make([]string, 0, len(entityTypes))
	for et := range entityTypes {
		result = append(result, et)
	}
	return result
}

// GetEntityType returns the definition for an entity type.
// Returns nil if the entity type is not defined.
func (m *Manager) GetEntityType(name string) *EntityTypeDefinition {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.entityTypes[name]
}

// GetEntityTypeNames returns all defined entity type names.
func (m *Manager) GetEntityTypeNames() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	names := make([]string, 0, len(m.entityTypes))
	for name := range m.entityTypes {
		names = append(names, name)
	}
	return names
}

// AddEntityType adds an entity type definition to the manager.
func (m *Manager) AddEntityType(et *EntityTypeDefinition) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entityTypes[et.GetName()] = et
}

// ResolveURL resolves a URL template for a given entity type and value.
// Returns empty string if the entity type has no URL templates.
// If urlName is empty, uses the default URL (marked with is_default=true),
// or falls back to the first URL template.
func (m *Manager) ResolveURL(entityType, value, urlName string) string {
	m.mu.RLock()
	et := m.entityTypes[entityType]
	m.mu.RUnlock()

	if et == nil || len(et.GetUrls()) == 0 {
		return ""
	}

	// Find the URL template
	var template string
	if urlName == "" {
		// Use the default URL if specified, otherwise use first template
		template = getDefaultTemplate(et.GetUrls())
	} else {
		// Find by name
		for _, u := range et.GetUrls() {
			if u.GetName() == urlName {
				template = u.GetTemplate()
				break
			}
		}
	}

	if template == "" {
		return ""
	}

	// Replace placeholders
	return replacePlaceholders(template, value, entityType)
}

// ResolveDefaultURL resolves the default URL template for a given entity type and value.
// This is a convenience method that calls ResolveURL with an empty urlName.
func (m *Manager) ResolveDefaultURL(entityType, value string) string {
	return m.ResolveURL(entityType, value, "")
}

// ResolvedURL represents a resolved URL with its name.
type ResolvedURL struct {
	Name string // Display name for the URL
	URL  string // The resolved URL
}

// GetPrimaryKeyEntityType returns the primary key entity type for a source.
// Returns empty string if the source doesn't exist or has no primary key defined.
func (m *Manager) GetPrimaryKeyEntityType(sourceName string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if source, ok := m.sources[sourceName]; ok {
		return source.GetPrimaryKeyEntityType()
	}
	return ""
}

// GetEntityTypeDescription returns the description for an entity type.
// Returns empty string if the entity type doesn't exist or has no description.
func (m *Manager) GetEntityTypeDescription(entityType string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if et, ok := m.entityTypes[entityType]; ok {
		return et.GetDescription()
	}
	return ""
}

// GetAllURLs returns all resolved URLs for a given entity type and value.
// Returns an empty slice if the entity type has no URL templates.
func (m *Manager) GetAllURLs(entityType, value string) []ResolvedURL {
	m.mu.RLock()
	et := m.entityTypes[entityType]
	m.mu.RUnlock()

	if et == nil || len(et.GetUrls()) == 0 {
		return nil
	}

	result := make([]ResolvedURL, 0, len(et.GetUrls()))
	for _, u := range et.GetUrls() {
		resolvedURL := replacePlaceholders(u.GetTemplate(), value, entityType)
		result = append(result, ResolvedURL{
			Name: u.GetName(),
			URL:  resolvedURL,
		})
	}
	return result
}

// getDefaultTemplate returns the template marked as default, or the first template.
func getDefaultTemplate(urls []*URLTemplate) string {
	if len(urls) == 0 {
		return ""
	}
	// Look for URL marked as default
	for _, u := range urls {
		if u.GetIsDefault() {
			return u.GetTemplate()
		}
	}
	// Fall back to first URL
	return urls[0].GetTemplate()
}

// replacePlaceholders replaces {value} and {entity_type} placeholders in a template.
func replacePlaceholders(template, value, entityType string) string {
	result := template
	// Simple string replacement for now
	result = replaceAll(result, "{value}", value)
	result = replaceAll(result, "{entity_type}", entityType)
	return result
}

// replaceAll is a simple string replacement helper.
func replaceAll(s, old, new string) string {
	for {
		i := indexOf(s, old)
		if i < 0 {
			return s
		}
		s = s[:i] + new + s[i+len(old):]
	}
}

// indexOf returns the index of substr in s, or -1 if not found.
func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}
