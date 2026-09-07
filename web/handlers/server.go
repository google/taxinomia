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

package handlers

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/taxinomia/core/buildinfo"
	"github.com/google/taxinomia/core/columns"
	"github.com/google/taxinomia/core/engine"
	"github.com/google/taxinomia/core/expr"
	"github.com/google/taxinomia/core/hrclock"
	"github.com/google/taxinomia/core/models"
	"github.com/google/taxinomia/core/tables"
	"github.com/google/taxinomia/core/users"
	"github.com/google/taxinomia/web/navigation"
	"github.com/google/taxinomia/web/rendering"
	"github.com/google/taxinomia/web/urlquery"
	"github.com/google/taxinomia/web/viewmodel"
)

// Resource limits to prevent abuse
const (
	MaxComputedColumns = 10 // Maximum number of computed columns per request
	MaxFilters         = 20 // Maximum number of filter expressions per request
	MaxGroupingLevels  = 5  // Maximum number of grouping levels per request
)

// ProductConfig defines the configuration interface for a product.
// Products provide their own tables, landing page settings, and default columns.
type ProductConfig interface {
	GetName() string
	GetTitle() string
	GetSubtitle() string
	GetTables() []viewmodel.TableInfo
	GetDefaultColumns(tableName string) []string
}

// PrimaryKeyResolver is a function that returns the primary key entity type for a table.
type PrimaryKeyResolver func(tableName string) string

// EntityTypeDescriptionResolver is a function that returns the description for an entity type.
type EntityTypeDescriptionResolver func(entityType string) string

// Server represents the application server with all its dependencies
type Server struct {
	dataModel      *models.DataModel
	renderer       *rendering.TableRenderer
	tableViewCache map[string]*tables.TableView
	userStore      users.UserStore
	clock          hrclock.Clock // times the request phases of the perf breakdown (SetClock)

	// navigator provides the catalog-driven navigation defaults (SetCatalog).
	// The Set*Resolver callbacks below override it individually where set.
	navigator               *navigation.Navigator
	urlResolver             viewmodel.URLResolver             // Optional resolver for entity type URLs
	allURLsResolver         viewmodel.AllURLsResolver         // Optional resolver for all entity type URLs (for detail panel)
	primaryKeyResolver      PrimaryKeyResolver                // Optional resolver for table primary key entity types
	entityTypeDescResolver  EntityTypeDescriptionResolver     // Optional resolver for entity type descriptions
	hierarchyContextBuilder viewmodel.HierarchyContextBuilder // Optional builder for hierarchy contexts in detail panel
	relatedTablesResolver   viewmodel.RelatedTablesResolver   // Optional resolver for related tables in detail panel

	// Caches for computed columns
	exprCache         map[string]*expr.Expression  // expression string -> compiled expression
	computedColState  map[string]map[string]string // cacheKey -> columnName -> expression
	computedColErrors map[string]map[string]string // cacheKey -> columnName -> error message (empty if no error)
}

// NewServer creates a new server with the given data model
func NewServer(dataModel *models.DataModel) (*Server, error) {
	renderer, err := rendering.NewTableRenderer()
	if err != nil {
		return nil, fmt.Errorf("failed to create renderer: %w", err)
	}

	return &Server{
		dataModel:         dataModel,
		renderer:          renderer,
		tableViewCache:    make(map[string]*tables.TableView),
		clock:             hrclock.System(),
		exprCache:         make(map[string]*expr.Expression),
		computedColState:  make(map[string]map[string]string),
		computedColErrors: make(map[string]map[string]string),
	}, nil
}

// SetUserStore sets the user store for authentication
func (s *Server) SetUserStore(store users.UserStore) {
	s.userStore = store
}

// SetClock installs the clock that times the request phases shown in the
// perf breakdown. The default is hrclock.System(), the platform's finest
// monotonic counter; embedding servers can supply their own (a different
// counter, an injected clock for tests, one that also feeds their
// tracing). nil restores the default.
func (s *Server) SetClock(clock hrclock.Clock) {
	if clock == nil {
		clock = hrclock.System()
	}
	s.clock = clock
}

// SetCatalog installs the catalog-driven navigation defaults: entity URL
// resolution, primary key and description lookups, hierarchy contexts and
// related tables are all answered from the catalog by web/navigation. Any
// resolver installed through a Set*Resolver method overrides its
// catalog-driven default individually.
func (s *Server) SetCatalog(catalog *engine.Catalog) {
	if catalog == nil {
		s.navigator = nil
		return
	}
	s.navigator = navigation.NewNavigator(catalog)
}

// SetURLResolver sets the URL resolver for entity type links.
//
// Deprecated: prefer SetCatalog; the catalog-driven default replaces this
// callback, which remains as an override seam.
func (s *Server) SetURLResolver(resolver viewmodel.URLResolver) {
	s.urlResolver = resolver
}

// SetAllURLsResolver sets the resolver for all entity type URLs (used in detail panel).
//
// Deprecated: prefer SetCatalog; the catalog-driven default replaces this
// callback, which remains as an override seam.
func (s *Server) SetAllURLsResolver(resolver viewmodel.AllURLsResolver) {
	s.allURLsResolver = resolver
}

// SetPrimaryKeyResolver sets the resolver for table primary key entity types.
//
// Deprecated: prefer SetCatalog; the catalog-driven default replaces this
// callback, which remains as an override seam.
func (s *Server) SetPrimaryKeyResolver(resolver PrimaryKeyResolver) {
	s.primaryKeyResolver = resolver
}

// SetEntityTypeDescriptionResolver sets the resolver for entity type descriptions.
//
// Deprecated: prefer SetCatalog; the catalog-driven default replaces this
// callback, which remains as an override seam.
func (s *Server) SetEntityTypeDescriptionResolver(resolver EntityTypeDescriptionResolver) {
	s.entityTypeDescResolver = resolver
}

// SetHierarchyContextBuilder sets the builder for hierarchy contexts in the detail panel.
//
// Deprecated: prefer SetCatalog; the catalog-driven default replaces this
// callback, which remains as an override seam.
func (s *Server) SetHierarchyContextBuilder(builder viewmodel.HierarchyContextBuilder) {
	s.hierarchyContextBuilder = builder
}

// SetRelatedTablesResolver sets the resolver for finding related tables in the detail panel.
//
// Deprecated: prefer SetCatalog; the catalog-driven default replaces this
// callback, which remains as an override seam.
func (s *Server) SetRelatedTablesResolver(resolver viewmodel.RelatedTablesResolver) {
	s.relatedTablesResolver = resolver
}

// effectiveResolvers returns the resolver set for view-model building:
// explicitly installed resolvers first, the navigator's catalog-driven
// defaults where none is installed, nil where neither exists.
func (s *Server) effectiveResolvers() (viewmodel.URLResolver, viewmodel.AllURLsResolver, PrimaryKeyResolver, EntityTypeDescriptionResolver, viewmodel.HierarchyContextBuilder, viewmodel.RelatedTablesResolver) {
	urlRes := s.urlResolver
	allURLs := s.allURLsResolver
	pkRes := s.primaryKeyResolver
	descRes := s.entityTypeDescResolver
	hierarchies := s.hierarchyContextBuilder
	related := s.relatedTablesResolver
	if s.navigator != nil {
		if urlRes == nil {
			urlRes = s.navigator.ResolveDefaultURL
		}
		if allURLs == nil {
			allURLs = s.navigator.AllURLs
		}
		if pkRes == nil {
			pkRes = s.navigator.PrimaryKeyEntityType
		}
		if descRes == nil {
			descRes = s.navigator.EntityTypeDescription
		}
		if hierarchies == nil {
			hierarchies = s.navigator.HierarchyContexts
		}
		if related == nil {
			related = s.navigator.RelatedTables
		}
	}
	return urlRes, allURLs, pkRes, descRes, hierarchies, related
}

// makeCacheKey creates a cache key combining user and table name
// This ensures each user has their own TableView with their own computed columns
func (s *Server) makeCacheKey(userName, tableName string) string {
	if userName == "" {
		return tableName
	}
	return userName + ":" + tableName
}

// TableHandlerResult represents the result of handling a table request
type TableHandlerResult struct {
	Error      error
	StatusCode int
	Message    string
}

// ValidationResult holds validation errors for filters and computed columns
type ValidationResult struct {
	ComputedColumnErrors map[string]string // columnName -> error message
	FilterErrors         map[string]string // columnName -> error message
}

// NewValidationResult creates a new ValidationResult
func NewValidationResult() *ValidationResult {
	return &ValidationResult{
		ComputedColumnErrors: make(map[string]string),
		FilterErrors:         make(map[string]string),
	}
}

// HasErrors returns true if there are any validation errors
func (v *ValidationResult) HasErrors() bool {
	return len(v.ComputedColumnErrors) > 0 || len(v.FilterErrors) > 0
}

// validateFilters checks if filter columns exist in the table view
func (s *Server) validateFilters(tableView *tables.TableView, filters map[string]string) map[string]string {
	errors := make(map[string]string)
	for colName := range filters {
		if tableView.GetColumn(colName) == nil {
			errors[colName] = fmt.Sprintf("column '%s' does not exist", colName)
		}
	}
	return errors
}

// TimingCollector collects timing measurements for various operations.
// Phases are measured with an hrclock.Clock, so sub-millisecond phases
// show real values instead of 0.00ms.
type TimingCollector struct {
	entries []viewmodel.TimingEntry
	clock   hrclock.Clock
	start   hrclock.Stamp
}

// NewTimingCollector creates a timing collector on the platform's default
// high-resolution clock.
func NewTimingCollector() *TimingCollector {
	return NewTimingCollectorWithClock(hrclock.System())
}

// NewTimingCollectorWithClock creates a timing collector on the given
// clock (nil means hrclock.System()).
func NewTimingCollectorWithClock(clock hrclock.Clock) *TimingCollector {
	if clock == nil {
		clock = hrclock.System()
	}
	return &TimingCollector{clock: clock, start: clock.Now()}
}

// Now reads the collector's clock; pair with Since to time a phase.
func (tc *TimingCollector) Now() hrclock.Stamp { return tc.clock.Now() }

// Since returns the time elapsed since a stamp from Now.
func (tc *TimingCollector) Since(s hrclock.Stamp) time.Duration { return tc.clock.Since(s) }

// Record records a timing entry
func (tc *TimingCollector) Record(operation string, duration time.Duration) {
	tc.entries = append(tc.entries, viewmodel.TimingEntry{
		Operation:  operation,
		DurationMs: formatMs(duration),
	})
}

// RecordSub records a step within the phase recorded just before it; the
// perf tab lists it indented under that phase.
func (tc *TimingCollector) RecordSub(step string, duration time.Duration) {
	tc.entries = append(tc.entries, viewmodel.TimingEntry{
		Operation:  step,
		DurationMs: formatMs(duration),
		Sub:        true,
	})
}

// RecordEntry records a fully described entry (volume, settings).
func (tc *TimingCollector) RecordEntry(e viewmodel.TimingEntry) {
	tc.entries = append(tc.entries, e)
}

// entry builds a phase entry with its duration, volume and settings.
func entry(operation string, d time.Duration, rows int, settings ...viewmodel.SettingLink) viewmodel.TimingEntry {
	return viewmodel.TimingEntry{Operation: operation, DurationMs: formatMs(d), Settings: settings}.WithVolume(d, rows)
}

// stepEntry turns a grouping step into an indented entry, attaching the
// links that switch off the setting behind it.
func stepEntry(q *urlquery.Query, tv *tables.TableView, step tables.GroupingStep) viewmodel.TimingEntry {
	e := viewmodel.TimingEntry{Operation: step.Name, DurationMs: formatMs(step.Duration), Sub: true}.WithVolume(step.Duration, step.Rows)
	e.Settings = settingLinks(q, tv, step.Setting)
	return e
}

// settingLinks maps a grouping step's setting onto the query's toggles.
func settingLinks(q *urlquery.Query, tv *tables.TableView, s tables.StepSetting) []viewmodel.SettingLink {
	var links []viewmodel.SettingLink
	switch s.Kind {
	case "group":
		col := s.Columns[0]
		links = append(links, viewmodel.SettingLink{Text: "grouped by " + col, Title: "ungroup " + col, URL: viewmodel.BuildToggleGroupingURL(q, col), HasURL: true})
	case "aggsort":
		col := s.Columns[0]
		nq := q.Clone()
		delete(nq.GroupAggregateSorts, col)
		links = append(links, viewmodel.SettingLink{Text: "group sort on " + col, Title: "sort " + col + " by value instead", URL: nq.ToSafeURL(), HasURL: true})
	case "aggregate":
		for _, col := range s.Columns {
			enabled := q.GetEnabledAggregates(col, tv.GetColumnType(col))
			n := 0
			for _, agg := range enabled {
				if agg == urlquery.AggCount {
					continue
				}
				n++
				links = append(links, viewmodel.SettingLink{Text: urlquery.AggregateSymbol(agg) + " on " + col, Title: "disable " + string(agg) + " on " + col, URL: q.WithAggregateToggled(col, agg), HasURL: true})
			}
			if n == 0 {
				// State needed without a displayed aggregate: a group sort
				// ranks by it, or the column is computed/joined.
				links = append(links, viewmodel.SettingLink{Text: "state for " + col + " (group sort or virtual column)"})
			}
		}
	}
	return links
}

// GetEntries returns all timing entries
func (tc *TimingCollector) GetEntries() []viewmodel.TimingEntry {
	return tc.entries
}

// TotalMs returns total elapsed time in milliseconds as formatted string
func (tc *TimingCollector) TotalMs() string {
	return formatMs(tc.clock.Since(tc.start))
}

// formatMs renders a duration as milliseconds with two decimals, rounded.
func formatMs(d time.Duration) string {
	return fmt.Sprintf("%.2f", float64(d.Microseconds())/1000.0)
}

// HandleTableRequest processes a table request and writes the response
// Returns an error result if the request is invalid, nil on success
func (s *Server) HandleTableRequest(w io.Writer, requestURL *url.URL, product ProductConfig, setHeader func(key, value string)) *TableHandlerResult {
	return s.HandleTableRequestContext(context.Background(), w, requestURL, product, setHeader)
}

// HandleTableRequestContext is HandleTableRequest under a context, normally
// the http.Request's: query work runs on the shared executor pool and stops
// at chunk granularity when the context is cancelled — a superseded request
// (the user scrolled or refined the filter before the response arrived)
// releases the pool to its successor instead of competing with it
// (docs/scaling-to-1b-rows.md §8). A cancelled request returns status 499
// (client closed request) without writing to w.
func (s *Server) HandleTableRequestContext(ctx context.Context, w io.Writer, requestURL *url.URL, product ProductConfig, setHeader func(key, value string)) *TableHandlerResult {
	timing := NewTimingCollectorWithClock(s.clock)

	// Parse URL into Query
	parseStart := timing.Now()
	q := urlquery.NewQuery(requestURL)
	timing.Record("Parse Query", timing.Since(parseStart))

	exec, res := s.Execute(ctx, q, ExecOptions{
		User:           requestURL.Query().Get("user"), // cache is user-specific
		DefaultColumns: product.GetDefaultColumns(q.Table),
		Timing:         timing,
	})
	if res != nil {
		return res
	}
	viewModel := s.BuildViewModel(exec)

	// Set content type and render
	setHeader("Content-Type", "text/html; charset=utf-8")
	setHeader(versionHeader, viewModel.Build.Version())
	if err := s.renderer.Render(w, viewModel); err != nil {
		log.Printf("Template rendering error: %v", err)
		return &TableHandlerResult{Error: err}
	}
	return nil
}

// ExecOptions parameterizes Execute.
type ExecOptions struct {
	// User scopes the per-user table view cache (computed columns, filters
	// and groupings are per user). Empty means the anonymous shared view.
	User string
	// DefaultColumns are displayed when the query names none. Empty: the
	// table's first four columns.
	DefaultColumns []string
	// Timing receives the phase timings; nil creates a collector on the
	// server's clock. A caller that timed earlier phases (URL parsing)
	// passes its own so they appear in the breakdown.
	Timing *TimingCollector
}

// Execution is the outcome of Execute: the user's table view after joins,
// computed columns, filters and grouping have been applied for the query,
// plus validation errors and phase timings. Read the view directly for your
// own output, or hand the execution to BuildViewModel for the table page.
type Execution struct {
	Query      *urlquery.Query
	View       viewmodel.View
	TableView  *tables.TableView
	Validation *ValidationResult
	Timing     *TimingCollector
}

// Execute runs the request pipeline for q. It is the one supported way to
// run a query: it validates the resource limits, resolves the table,
// selects the display columns, fetches the user's cached table view (with
// encodings selected), updates joins and computed columns, applies
// filters, and groups with the aggregate needs, level-0 aggregate sort and
// viewport that keep grouping cheap. Those invariants live here so that no
// caller has to reassemble them — a pipeline rebuilt from the building
// blocks without them groups 20–250x slower on the same table. Failures
// come back as a TableHandlerResult (HTTP status + message); nil on
// success.
func (s *Server) Execute(ctx context.Context, q *urlquery.Query, opts ExecOptions) (*Execution, *TableHandlerResult) {
	timing := opts.Timing
	if timing == nil {
		timing = NewTimingCollectorWithClock(s.clock)
	}
	cacheKey := s.makeCacheKey(opts.User, q.Table)

	// Validate table parameter
	if q.Table == "" {
		return nil, &TableHandlerResult{StatusCode: 400, Message: "Table parameter is required"}
	}

	// Resource limits validation
	if len(q.ComputedColumns) > MaxComputedColumns {
		return nil, &TableHandlerResult{StatusCode: 400, Message: fmt.Sprintf("Too many computed columns (max %d)", MaxComputedColumns)}
	}
	if len(q.Filters) > MaxFilters {
		return nil, &TableHandlerResult{StatusCode: 400, Message: fmt.Sprintf("Too many filters (max %d)", MaxFilters)}
	}
	if len(q.GroupedColumns) > MaxGroupingLevels {
		return nil, &TableHandlerResult{StatusCode: 400, Message: fmt.Sprintf("Too many grouping levels (max %d)", MaxGroupingLevels)}
	}

	// Get the table from data model
	table := s.dataModel.GetTable(q.Table)
	if table == nil {
		return nil, &TableHandlerResult{StatusCode: 404, Message: fmt.Sprintf("Table '%s' not found", q.Table)}
	}

	// Default columns from the caller, or the first few columns if not defined
	defaultColumns := opts.DefaultColumns
	if len(defaultColumns) == 0 {
		// Use first 4 columns as default
		allCols := table.GetColumnNames()
		if len(allCols) > 4 {
			defaultColumns = allCols[:4]
		} else {
			defaultColumns = allCols
		}
	}

	// Use default columns if none specified
	if len(q.Columns) == 0 {
		q.Columns = defaultColumns
	}

	// Convert expanded paths to map for compatibility
	expandedPaths := make(map[string]bool)
	for _, path := range q.Expanded {
		expandedPaths[path] = true
	}

	// Define the view - which columns to display and in what order
	view := viewmodel.View{
		Columns:        q.Columns,
		Expanded:       expandedPaths,
		GroupedColumns: q.GroupedColumns,
	}

	// Get or create a cached TableView for this user+table combination. The
	// table's storage encodings are selected on first use (a no-op for the
	// loaders that already did it).
	cacheStart := timing.Now()
	table.EnsureEncodings()
	tableView := viewmodel.GetOrCreateTableView(cacheKey, table, s.tableViewCache)
	timing.Record("Get TableView", timing.Since(cacheStart))

	// Update joined columns to match the current request
	joinStart := timing.Now()
	viewmodel.ProcessJoinsAndUpdateColumns(tableView, &view, s.dataModel)
	var joinLinks []viewmodel.SettingLink
	for _, col := range view.Columns {
		if strings.Contains(col, ".") {
			joinLinks = append(joinLinks, viewmodel.SettingLink{Text: "joined column " + col, Title: "hide " + col, URL: q.WithoutColumn(col), HasURL: true})
		}
	}
	timing.RecordEntry(entry("Process Joins", timing.Since(joinStart), 0, joinLinks...))

	// Create validation result to collect errors
	validation := NewValidationResult()

	// Create computed columns from the query (with caching)
	computedStart := timing.Now()
	validation.ComputedColumnErrors = s.updateComputedColumns(tableView, q, cacheKey)
	var computedLinks []viewmodel.SettingLink
	for _, c := range q.ComputedColumns {
		nq := q.Clone()
		nq.ComputedColumns = nil
		for _, o := range q.ComputedColumns {
			if o.Name != c.Name {
				nq.ComputedColumns = append(nq.ComputedColumns, o)
			}
		}
		computedLinks = append(computedLinks, viewmodel.SettingLink{Text: "computed column " + c.Name, Title: "remove " + c.Name, URL: nq.ToSafeURL(), HasURL: true})
	}
	timing.RecordEntry(entry("Computed Columns", timing.Since(computedStart), 0, computedLinks...))

	// Validate filter columns exist before applying
	validation.FilterErrors = s.validateFilters(tableView, q.Filters)

	// Apply filters to the table view (even with errors, apply valid filters)
	filterStart := timing.Now()
	if err := tableView.ApplyFiltersContext(ctx, q.Filters); err != nil {
		return nil, &TableHandlerResult{StatusCode: 499, Message: "request cancelled"}
	}
	var filterLinks []viewmodel.SettingLink
	filterRows := 0
	if len(q.Filters) > 0 {
		if tableView.LastFiltersRecomputed() {
			filterRows = table.Length()
		} else {
			filterLinks = append(filterLinks, viewmodel.SettingLink{Text: "cached: filters unchanged"})
		}
		for col, value := range q.Filters {
			nq := q.Clone()
			delete(nq.Filters, col)
			filterLinks = append(filterLinks, viewmodel.SettingLink{Text: "filter " + col + " = " + value, Title: "clear the filter on " + col, URL: nq.ToSafeURL(), HasURL: true})
		}
		sort.Slice(filterLinks, func(i, j int) bool { return filterLinks[i].Text < filterLinks[j].Text })
	}
	timing.RecordEntry(entry("Apply Filters", timing.Since(filterStart), filterRows, filterLinks...))

	// Apply grouping if grouped columns are specified
	groupStart := timing.Now()
	if len(q.GroupedColumns) > 0 {
		// Group order per level: by value, ascending unless the column's
		// direction is flipped
		ascMap := make(map[string]bool)
		for _, col := range q.GroupedColumns {
			ascMap[col] = !q.Descending[col]
		}
		// Tell the grouping build which leaf columns actually need a full
		// aggregate state: those with any non-count aggregate enabled, and
		// any column an aggregate group sort ranks by. Count-only storage
		// columns then skip state building entirely (their count is the
		// group size), which is the difference between ~7ms and ~1.8s on a
		// 10M-row grouping with several visible string columns.
		aggNeeds := make(map[string]bool)
		groupedSet := make(map[string]bool, len(q.GroupedColumns))
		for _, col := range q.GroupedColumns {
			groupedSet[col] = true
		}
		for _, col := range view.Columns {
			if groupedSet[col] {
				continue
			}
			for _, agg := range q.GetEnabledAggregates(col, tableView.GetColumnType(col)) {
				if agg != urlquery.AggCount {
					aggNeeds[col] = true
					break
				}
			}
		}
		for _, gs := range q.GroupAggregateSorts {
			if gs != nil && gs.LeafColumn != "" {
				aggNeeds[gs.LeafColumn] = true
			}
		}
		tableView.SetAggregateNeeds(aggNeeds)
		// An aggregate sort on the level-0 column ranks inside the build:
		// level 0 is built and ranked in full, child subtrees only for the
		// displayed top-K — instead of suspending the trim and building
		// every subtree (10'000 entity subtrees to show 25).
		tableView.SetLevelZeroAggSort(q.GetGroupAggSort(q.GroupedColumns[0]))
		// Group with the viewport (display limit) and expansion state from the
		// URL. Without a gexp parameter the expansion is expand-all, which is
		// the historical eager build and byte-identical output; with one, only
		// the opened subtrees are computed.
		expansion := tables.GroupExpansion{ExpandAll: !q.HasExpandedGroups, Paths: q.ExpandedGroups}
		// With the in-build level-0 aggregate ranking, the real display
		// limit is passed again: the build ranks all groups and uses the
		// limit only to bound the child subtrees it constructs.
		tableView.SetClock(s.clock)
		if err := tableView.GroupTableWindowedContext(ctx, q.GroupedColumns, []string{}, make(map[string]tables.Compare), ascMap, q.Limit, expansion); err != nil {
			return nil, &TableHandlerResult{StatusCode: 499, Message: "request cancelled"}
		}

	} else {
		tableView.ClearGroupings()
	}
	timing.Record("Grouping", timing.Since(groupStart))
	// The grouping build's own steps (partition, level sorts, per-column
	// aggregates, release), listed under the phase with their volume and
	// the setting that caused each.
	if len(q.GroupedColumns) > 0 {
		for _, step := range tableView.LastGroupingSteps() {
			timing.RecordEntry(stepEntry(q, tableView, step))
		}
	}

	return &Execution{Query: q, View: view, TableView: tableView, Validation: validation, Timing: timing}, nil
}

// BuildViewModel turns an execution into the table page's view model:
// rows or grouped rows with entity URLs, hierarchy contexts, validation
// errors, phase timings, build version and info pane state. Render it with
// rendering.NewTableRenderer or your own template.
func (s *Server) BuildViewModel(exec *Execution) viewmodel.TableViewModel {
	q, timing := exec.Query, exec.Timing

	vmStart := timing.Now()
	title := strings.Title(q.Table)
	urlResolver, allURLsResolver, primaryKeyResolver, descResolver, hierarchyContextBuilder, relatedTablesResolver := s.effectiveResolvers()
	var primaryKeyEntityType string
	if primaryKeyResolver != nil {
		primaryKeyEntityType = primaryKeyResolver(q.Table)
	}
	var entityTypeDescResolver viewmodel.EntityTypeDescriptionResolver
	if descResolver != nil {
		entityTypeDescResolver = viewmodel.EntityTypeDescriptionResolver(descResolver)
	}
	viewModel := viewmodel.BuildViewModel(s.dataModel, q.Table, exec.TableView, exec.View, title, q, exec.Validation.ComputedColumnErrors, exec.Validation.FilterErrors, urlResolver, allURLsResolver, primaryKeyEntityType, entityTypeDescResolver, hierarchyContextBuilder, relatedTablesResolver)
	displayed := len(viewModel.Rows)
	if viewModel.IsGrouped {
		displayed = viewModel.DisplayedRows
	}
	timing.RecordEntry(entry("Build ViewModel", timing.Since(vmStart), displayed, viewmodel.SettingLink{Text: fmt.Sprintf("limit %d", q.Limit)}))

	// Set timing information
	viewModel.RenderTimeMs = timing.TotalMs()
	viewModel.TimingBreakdown = timing.GetEntries()
	viewModel.Perf = perfData(exec.TableView, q, displayed)

	// Set info pane state from Query (already parsed from URL)
	viewModel.ShowInfoPane = q.ShowInfoPane
	viewModel.InfoPaneTab = q.InfoPaneTab
	viewModel.Build = buildinfo.Get()

	// Set animation state (transient, for newly grouped columns)
	viewModel.AnimatedColumn = q.AnimatedColumn

	// Column types display state
	viewModel.ShowColumnTypes = q.ShowColumnTypes

	return viewModel
}

// perfData assembles the Performance tab's "Data" section: the sizes the
// request's costs are proportional to.
func perfData(tv *tables.TableView, q *urlquery.Query, displayed int) viewmodel.PerfData {
	total, filtered := tv.NumRows(), tv.GetFilteredRowCount()
	d := viewmodel.PerfData{
		TableRows:    viewmodel.FormatCount(total),
		Columns:      viewmodel.FormatCount(len(tv.GetBaseTable().GetColumnNames())),
		FilteredRows: viewmodel.FormatCount(filtered),
		Displayed:    viewmodel.FormatCount(displayed),
		VisibleCols:  viewmodel.FormatCount(len(q.Columns)),
	}
	if len(q.Filters) > 0 && total > 0 {
		d.Selectivity = fmt.Sprintf("%.1f%%", 100*float64(filtered)/float64(total))
	}
	for _, col := range q.GroupedColumns {
		d.Levels = append(d.Levels, viewmodel.PerfLevel{Column: col, Groups: viewmodel.FormatCount(tv.GetGroupCount(col))})
	}
	return d
}

// versionHeader carries the serving build's version (core/buildinfo) on
// every page response, so scripts and bug reports get it without parsing
// HTML.
const versionHeader = "X-Taxinomia-Version"

// HandleLandingRequest processes the landing page request
func (s *Server) HandleLandingRequest(w io.Writer, requestURL *url.URL, product ProductConfig, setHeader func(key, value string)) error {
	setHeader("Content-Type", "text/html; charset=utf-8")

	// Get user from URL parameter (for testing)
	userName := requestURL.Query().Get("user")

	// Create a copy of the landing view model to filter tables
	vm := viewmodel.LandingViewModel{
		Title:    product.GetTitle(),
		Subtitle: product.GetSubtitle(),
		Build:    buildinfo.Get(),
	}
	setHeader(versionHeader, vm.Build.Version())

	// If we have a user store and a user parameter, filter tables by domain
	if s.userStore != nil && userName != "" {
		user := s.userStore.GetUser(userName)
		if user != nil {
			vm.UserName = userName

			// Filter tables to only those matching user's domains
			for _, table := range product.GetTables() {
				if users.HasAnyDomain(user, table.Domains) {
					vm.Tables = append(vm.Tables, table)
				}
			}
		} else {
			// Unknown user - show no tables
			vm.UserName = userName + " (unknown)"
		}
	} else {
		// No user filtering - show all tables
		vm.Tables = product.GetTables()
	}

	if err := s.renderer.RenderLanding(w, vm); err != nil {
		log.Printf("Landing page rendering error: %v", err)
		return err
	}
	return nil
}

// updateComputedColumns manages computed columns with caching.
// Only recompiles expressions and recreates columns when something has changed.
// The cacheKey is user+table specific to ensure users have isolated computed columns.
// Returns a map of column names to error messages for any columns that failed to compile.
func (s *Server) updateComputedColumns(tableView *tables.TableView, q *urlquery.Query, cacheKey string) map[string]string {
	errors := make(map[string]string)

	// Get current state for this user+table combination
	currentState, exists := s.computedColState[cacheKey]
	if !exists {
		currentState = make(map[string]string)
		s.computedColState[cacheKey] = currentState
	}

	// Get cached errors for this user+table combination
	cachedErrors, errCacheExists := s.computedColErrors[cacheKey]
	if !errCacheExists {
		cachedErrors = make(map[string]string)
		s.computedColErrors[cacheKey] = cachedErrors
	}

	// Build map of requested columns
	requested := make(map[string]string)
	for _, comp := range q.ComputedColumns {
		requested[comp.Name] = comp.Expression
	}

	// Remove columns that are no longer requested
	for name := range currentState {
		if _, ok := requested[name]; !ok {
			tableView.RemoveComputedColumn(name)
			delete(currentState, name)
			delete(cachedErrors, name)
		}
	}

	// Add or update columns
	for _, comp := range q.ComputedColumns {
		existingExpr, exists := currentState[comp.Name]

		// Skip if column exists with same expression - but still report cached errors
		if exists && existingExpr == comp.Expression {
			if cachedErr, hasErr := cachedErrors[comp.Name]; hasErr && cachedErr != "" {
				errors[comp.Name] = cachedErr
			}
			continue
		}

		// Create the column and capture any errors
		if err := s.createComputedColumn(tableView, comp.Name, comp.Expression); err != nil {
			errMsg := err.Error()
			errors[comp.Name] = errMsg
			cachedErrors[comp.Name] = errMsg
		} else {
			// Clear any previous error for this column
			delete(cachedErrors, comp.Name)
		}
		currentState[comp.Name] = comp.Expression
	}

	return errors
}

// createComputedColumn creates a single computed column, using cached compiled expressions.
// Returns an error if the expression fails to compile or evaluate.
func (s *Server) createComputedColumn(tableView *tables.TableView, name, expression string) error {
	if expression == "" {
		tableView.AddComputedColumn(name, nil)
		return nil
	}

	// Check expression cache first
	compiled, ok := s.exprCache[expression]
	if !ok {
		var err error
		compiled, err = expr.Compile(expression)
		if err != nil {
			tableView.AddComputedColumn(name, nil)
			return fmt.Errorf("syntax error: %v", err)
		}
		s.exprCache[expression] = compiled
	}

	// Get a reference column to determine length
	var length int
	colNames := tableView.GetColumnNames()
	if len(colNames) > 0 {
		if col := tableView.GetColumn(colNames[0]); col != nil {
			length = col.Length()
		}
	}
	if length == 0 {
		tableView.AddComputedColumn(name, nil)
		return nil
	}

	// Create a column getter function that retrieves values from the table view
	getColumn := func(colName string, rowIndex uint32) (expr.Value, error) {
		col := tableView.GetColumn(colName)
		if col == nil {
			return expr.NilValue(), fmt.Errorf("column '%s' not found", colName)
		}

		// Handle datetime columns - return datetime type for type-aware operations
		switch dtCol := col.(type) {
		case *columns.DatetimeColumn:
			t, err := dtCol.GetValue(rowIndex)
			if err != nil {
				return expr.NilValue(), err
			}
			return expr.NewDatetime(t.UnixNano()), nil
		case *columns.JoinedDatetimeColumn:
			t, err := dtCol.GetValue(rowIndex)
			if err != nil {
				return expr.NilValue(), err
			}
			return expr.NewDatetime(t.UnixNano()), nil
		case *columns.ComputedDatetimeColumn:
			nanos, err := dtCol.GetValue(rowIndex)
			if err != nil {
				return expr.NilValue(), err
			}
			return expr.NewDatetime(nanos), nil
		case *columns.DurationColumn:
			nanos, err := dtCol.Nanoseconds(rowIndex)
			if err != nil {
				return expr.NilValue(), err
			}
			return expr.NewDuration(nanos), nil
		case *columns.JoinedDurationColumn:
			nanos, err := dtCol.Nanoseconds(rowIndex)
			if err != nil {
				return expr.NilValue(), err
			}
			return expr.NewDuration(nanos), nil
		case *columns.ComputedDurationColumn:
			nanos, err := dtCol.Nanoseconds(rowIndex)
			if err != nil {
				return expr.NilValue(), err
			}
			return expr.NewDuration(nanos), nil
		// Any other column with typed datetime access (the chunked datetime
		// column) — checked after the concrete cases above.
		case interface {
			GetValue(uint32) (time.Time, error)
		}:
			t, err := dtCol.GetValue(rowIndex)
			if err != nil {
				return expr.NilValue(), err
			}
			return expr.NewDatetime(t.UnixNano()), nil
		}

		strVal, err := col.GetString(rowIndex)
		if err != nil {
			return expr.NilValue(), err
		}
		// Try to parse as int first, then float
		if intVal, err := strconv.ParseInt(strVal, 10, 64); err == nil {
			return expr.NewInt(intVal), nil
		}
		if numVal, err := strconv.ParseFloat(strVal, 64); err == nil {
			return expr.NewFloat(numVal), nil
		}
		return expr.NewString(strVal), nil
	}

	// Bind the expression to the column getter
	bound := compiled.Bind(getColumn)

	// Create the computed column definition
	colDef := columns.NewColumnDef(name, name, "")

	// Evaluate once on row 0 to detect the return type
	sampleVal, err := bound.Eval(0)
	if err != nil {
		// Can't determine type - report the error
		tableView.AddComputedColumn(name, nil)
		return fmt.Errorf("evaluation error: %v", err)
	}

	// Create the appropriate column type based on the expression result
	if sampleVal.IsDuration() {
		// Duration value - create a duration column
		computedCol := columns.NewComputedDurationColumn(colDef, length, func(i uint32) (time.Duration, error) {
			val, err := bound.Eval(i)
			if err != nil {
				return 0, err
			}
			return val.AsDuration(), nil
		})
		tableView.AddComputedColumn(name, computedCol)
	} else if sampleVal.IsDatetime() {
		// Datetime value - create a datetime column
		computedCol := columns.NewComputedDatetimeColumn(colDef, length, func(i uint32) (int64, error) {
			val, err := bound.Eval(i)
			if err != nil {
				return 0, err
			}
			return val.AsInt(), nil
		})
		tableView.AddComputedColumn(name, computedCol)
	} else if sampleVal.IsInt() {
		// Integer value - create an int64 column
		computedCol := columns.NewComputedInt64Column(colDef, length, func(i uint32) (int64, error) {
			val, err := bound.Eval(i)
			if err != nil {
				return 0, err
			}
			return val.AsInt(), nil
		})
		tableView.AddComputedColumn(name, computedCol)
	} else if sampleVal.IsFloat() {
		// Float value - create a float64 column
		computedCol := columns.NewComputedFloat64Column(colDef, length, func(i uint32) (float64, error) {
			val, err := bound.Eval(i)
			if err != nil {
				return 0, err
			}
			return val.AsFloat(), nil
		})
		tableView.AddComputedColumn(name, computedCol)
	} else if sampleVal.IsBool() {
		// Boolean value - create a bool column
		computedCol := columns.NewComputedBoolColumn(colDef, length, func(i uint32) (bool, error) {
			val, err := bound.Eval(i)
			if err != nil {
				return false, err
			}
			return val.AsBool(), nil
		})
		tableView.AddComputedColumn(name, computedCol)
	} else {
		// String - use string column
		computedCol := columns.NewComputedStringColumn(colDef, length, func(i uint32) (string, error) {
			val, err := bound.Eval(i)
			if err != nil {
				return "", err
			}
			return val.AsString(), nil
		})
		tableView.AddComputedColumn(name, computedCol)
	}
	return nil
}
