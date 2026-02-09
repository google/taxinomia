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
	"strings"
	"sync"

	"github.com/google/taxinomia/core/columns"
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

	// Hierarchies indexed by name - loaded eagerly
	hierarchies map[string]*Hierarchy
	// Order of hierarchy names (preserves definition order)
	hierarchyOrder []string

	// Entity type to hierarchies mapping (which hierarchies include each entity type)
	entityTypeHierarchies map[string][]*Hierarchy

	// Cached tables indexed by source name - populated lazily
	tables map[string]*tables.DataTable

	// Programmatically registered tables (not from datasources config)
	// These are indexed by table name and used for hierarchy lookups
	registeredTables map[string]*tables.DataTable

	// Registered loaders indexed by source_type
	loaders map[string]DataSourceLoader

	// Base directory for resolving relative paths
	baseDir string
}

// NewManager creates a new data source manager.
func NewManager() *Manager {
	return &Manager{
		annotations:           make(map[string]*ColumnAnnotations),
		sources:               make(map[string]*DataSource),
		entityTypes:           make(map[string]*EntityTypeDefinition),
		hierarchies:           make(map[string]*Hierarchy),
		entityTypeHierarchies: make(map[string][]*Hierarchy),
		tables:                make(map[string]*tables.DataTable),
		registeredTables:      make(map[string]*tables.DataTable),
		loaders:               make(map[string]DataSourceLoader),
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

	// Load hierarchies and build entity type mapping (eager)
	// Preserve definition order in hierarchyOrder slice
	for _, h := range config.GetHierarchies() {
		m.hierarchies[h.GetName()] = h
		m.hierarchyOrder = append(m.hierarchyOrder, h.GetName())
		// Map each entity type in this hierarchy to the hierarchy
		for _, level := range h.GetLevels() {
			m.entityTypeHierarchies[level] = append(m.entityTypeHierarchies[level], h)
		}
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

// GetHierarchiesForEntityType returns all hierarchies that include the given entity type.
// Returns nil if the entity type is not part of any hierarchy.
func (m *Manager) GetHierarchiesForEntityType(entityType string) []*Hierarchy {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.entityTypeHierarchies[entityType]
}

// GetHierarchy returns a hierarchy by name.
// Returns nil if the hierarchy doesn't exist.
func (m *Manager) GetHierarchy(name string) *Hierarchy {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.hierarchies[name]
}

// GetAllHierarchies returns all registered hierarchies in definition order.
func (m *Manager) GetAllHierarchies() []*Hierarchy {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]*Hierarchy, 0, len(m.hierarchyOrder))
	for _, name := range m.hierarchyOrder {
		if h, ok := m.hierarchies[name]; ok {
			result = append(result, h)
		}
	}
	return result
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

// RegisterTable registers a programmatically created table for hierarchy joins.
// This is used for tables that are not loaded from datasources config (e.g., the Google demo tables).
// The table name should match the name used elsewhere (e.g., "google_machines").
func (m *Manager) RegisterTable(name string, table *tables.DataTable) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.registeredTables[name] = table
}

// HierarchyAncestorColumnPrefix is the prefix used for hierarchy ancestor column names
// when there's a naming collision with an existing column.
const HierarchyAncestorColumnPrefix = "§"

// AddHierarchyAncestorColumns adds joined columns for ancestor entity types
// in hierarchies that include the given entity type. This enables filtering/grouping
// by ancestor values even when the table doesn't have a direct column for them.
//
// Columns are only added for ancestor entity types that don't already have a
// column in the table. For example, if racks table already has a "zone" column
// with entity type "google.zone", no zone column will be added.
//
// For example, if a machines table has primary key entity type "google.machine" and
// is part of the "google" hierarchy, this will add columns like:
//   - region (joined through zones table)
//
// Column naming:
//   - Uses simple name derived from entity type (e.g., "google.region" -> "region")
//   - Only uses §-prefix if there's a naming collision with an existing column
//
// These are implemented as joined columns using the existing join infrastructure,
// chaining through intermediate tables as needed.
func (m *Manager) AddHierarchyAncestorColumns(table *tables.DataTable, primaryKeyEntityType string) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Find hierarchies that include this entity type
	hierarchies := m.entityTypeHierarchies[primaryKeyEntityType]
	if len(hierarchies) == 0 {
		return
	}

	// Build maps of entity type -> column and column name -> exists for this table
	entityTypeToColumn := make(map[string]columns.IDataColumn)
	existingColumnNames := make(map[string]bool)
	for _, colName := range table.GetColumnNames() {
		existingColumnNames[colName] = true
		col := table.GetColumn(colName)
		if col == nil {
			continue
		}
		et := col.ColumnDef().EntityType()
		if et != "" {
			entityTypeToColumn[et] = col
		}
	}

	// For each hierarchy, add columns for all ancestor entity types
	for _, h := range hierarchies {
		levels := h.GetLevels()

		// Find the position of this entity type in the hierarchy
		myLevel := -1
		for i, level := range levels {
			if level == primaryKeyEntityType {
				myLevel = i
				break
			}
		}

		if myLevel <= 0 {
			// Not found or already at the top - no ancestors
			continue
		}

		// Add columns for ancestors that aren't already in the table
		for targetLevel := myLevel - 1; targetLevel >= 0; targetLevel-- {
			ancestorEntityType := levels[targetLevel]

			// Skip if table already has a column with this entity type
			if _, exists := entityTypeToColumn[ancestorEntityType]; exists {
				continue
			}

			// Find the nearest ancestor that IS in the table
			// This will be our starting point for the join chain
			startLevel := -1
			for j := targetLevel + 1; j < myLevel; j++ {
				if _, exists := entityTypeToColumn[levels[j]]; exists {
					startLevel = j
					break
				}
			}

			if startLevel == -1 {
				// No intermediate ancestor found, use the immediate parent of target
				startLevel = targetLevel + 1
			}

			// Build a chain of joiners from startLevel down to targetLevel
			joinedColumn := m.buildHierarchyJoinedColumn(table, entityTypeToColumn, existingColumnNames, levels, startLevel, targetLevel)
			if joinedColumn != nil {
				table.AddColumn(joinedColumn)
				// Track the new column name to avoid collisions with subsequent columns
				existingColumnNames[joinedColumn.ColumnDef().Name()] = true
			}
		}
	}
}

// buildHierarchyJoinedColumn creates a joined column by chaining through the hierarchy.
// It starts from startLevel (which should have a column in the source table) and
// chains through tables until it reaches targetLevel.
func (m *Manager) buildHierarchyJoinedColumn(
	sourceTable *tables.DataTable,
	entityTypeToColumn map[string]columns.IDataColumn,
	existingColumnNames map[string]bool,
	levels []string,
	startLevel, targetLevel int,
) columns.IDataColumn {
	if startLevel <= targetLevel {
		return nil
	}

	ancestorEntityType := levels[targetLevel]

	// Create column name from entity type (e.g., "google.region" -> "region")
	// Use §-prefix only if there's a naming collision
	columnName := ancestorEntityType
	if lastDot := strings.LastIndex(ancestorEntityType, "."); lastDot >= 0 {
		columnName = ancestorEntityType[lastDot+1:]
	}
	if existingColumnNames[columnName] {
		// Collision - use the §-prefixed name
		columnName = HierarchyAncestorColumnPrefix + ancestorEntityType
	}

	// Create display name (e.g., "region" -> "Region")
	displayName := columnName
	if strings.HasPrefix(displayName, HierarchyAncestorColumnPrefix) {
		displayName = displayName[len(HierarchyAncestorColumnPrefix):]
	}
	if lastDot := strings.LastIndex(displayName, "."); lastDot >= 0 {
		displayName = displayName[lastDot+1:]
	}
	if len(displayName) > 0 {
		displayName = strings.ToUpper(displayName[:1]) + displayName[1:]
	}

	// Build joiners from startLevel down to targetLevel
	var joiners []columns.IJoiner
	currentLevel := startLevel

	for currentLevel > targetLevel {
		currentEntityType := levels[currentLevel]
		parentEntityType := levels[currentLevel-1]

		// Find the table for current level's entity type
		currentTable := m.findTableByEntityType(currentEntityType)
		if currentTable == nil {
			fmt.Printf("Warning: could not find table for entity type %s\n", currentEntityType)
			return nil
		}

		// Find the column in current table that links to parent entity type
		var parentColumn columns.IDataColumn
		for _, colName := range currentTable.GetColumnNames() {
			col := currentTable.GetColumn(colName)
			if col != nil && col.ColumnDef().EntityType() == parentEntityType {
				parentColumn = col
				break
			}
		}

		if parentColumn == nil {
			fmt.Printf("Warning: could not find column with entity type %s in table\n", parentEntityType)
			return nil
		}

		// For the first joiner, use the source table's column to join to currentTable's PK
		if len(joiners) == 0 {
			// Find source table's column with currentEntityType
			fromCol, exists := entityTypeToColumn[currentEntityType]
			if !exists {
				fmt.Printf("Warning: source table doesn't have column with entity type %s\n", currentEntityType)
				return nil
			}

			// Find currentTable's PK column (same entity type)
			var toCol columns.IDataColumn
			for _, colName := range currentTable.GetColumnNames() {
				col := currentTable.GetColumn(colName)
				if col != nil && col.ColumnDef().EntityType() == currentEntityType && col.IsKey() {
					toCol = col
					break
				}
			}

			if toCol == nil {
				fmt.Printf("Warning: could not find PK column with entity type %s\n", currentEntityType)
				return nil
			}

			// Create joiner from source to currentTable
			fromStrCol, ok1 := fromCol.(columns.IDataColumnT[string])
			toStrCol, ok2 := toCol.(columns.IDataColumnT[string])
			if !ok1 || !ok2 {
				fmt.Printf("Warning: columns are not string type for join\n")
				return nil
			}

			joiners = append(joiners, &columns.Joiner[string]{
				FromColumn: fromStrCol,
				ToColumn:   toStrCol,
			})
		}

		// If we're not at the target yet, add joiner to next level's table
		if currentLevel-1 > targetLevel {
			nextEntityType := parentEntityType
			nextTable := m.findTableByEntityType(nextEntityType)
			if nextTable == nil {
				fmt.Printf("Warning: could not find table for entity type %s\n", nextEntityType)
				return nil
			}

			// Find next table's PK column
			var nextPKCol columns.IDataColumn
			for _, colName := range nextTable.GetColumnNames() {
				col := nextTable.GetColumn(colName)
				if col != nil && col.ColumnDef().EntityType() == nextEntityType && col.IsKey() {
					nextPKCol = col
					break
				}
			}

			if nextPKCol == nil {
				fmt.Printf("Warning: could not find PK column with entity type %s\n", nextEntityType)
				return nil
			}

			// Create joiner from parentColumn to nextTable's PK
			fromStrCol, ok1 := parentColumn.(columns.IDataColumnT[string])
			toStrCol, ok2 := nextPKCol.(columns.IDataColumnT[string])
			if !ok1 || !ok2 {
				fmt.Printf("Warning: columns are not string type for join\n")
				return nil
			}

			joiners = append(joiners, &columns.Joiner[string]{
				FromColumn: fromStrCol,
				ToColumn:   toStrCol,
			})
		}

		currentLevel--
	}

	// Get the final target column (ancestor value from the last joined table)
	targetTable := m.findTableByEntityType(levels[targetLevel+1])
	if targetTable == nil {
		fmt.Printf("Warning: could not find table for entity type %s\n", levels[targetLevel+1])
		return nil
	}

	var targetColumn columns.IDataColumn
	for _, colName := range targetTable.GetColumnNames() {
		col := targetTable.GetColumn(colName)
		if col != nil && col.ColumnDef().EntityType() == ancestorEntityType {
			targetColumn = col
			break
		}
	}

	if targetColumn == nil {
		fmt.Printf("Warning: could not find column with entity type %s in target table\n", ancestorEntityType)
		return nil
	}

	// Create the joined column
	colDef := columns.NewColumnDef(
		columnName,
		displayName,
		ancestorEntityType,
	)

	var joiner columns.IJoiner
	if len(joiners) == 1 {
		joiner = joiners[0]
	} else if len(joiners) > 1 {
		joiner = columns.NewChainedJoiner(joiners...)
	} else {
		return nil
	}

	return targetColumn.CreateJoinedColumn(colDef, joiner)
}

// findTableByEntityType finds a registered table whose primary key has the given entity type.
func (m *Manager) findTableByEntityType(entityType string) *tables.DataTable {
	// Look through registered tables
	for _, table := range m.registeredTables {
		for _, colName := range table.GetColumnNames() {
			col := table.GetColumn(colName)
			if col != nil && col.ColumnDef().EntityType() == entityType && col.IsKey() {
				return table
			}
		}
	}

	// Also check loaded tables
	for _, table := range m.tables {
		for _, colName := range table.GetColumnNames() {
			col := table.GetColumn(colName)
			if col != nil && col.ColumnDef().EntityType() == entityType && col.IsKey() {
				return table
			}
		}
	}

	return nil
}
