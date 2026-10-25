# Taxinomia TODOs


## Pending
- [ ] Add the links to the single item pane view
- [ ] Filter on aggregates
- [ ] Cleanup the hiearchies code
- [ ] Many toggles result in the page being vertically reset 
- [ ] Grouped cells should move their content to be visible 
- [ ] Enable selection of grouped cells 
- [ ] Grouped view truncation indicator should says how many of the top columns groups are missing
- [ ] AI-powered query feature (see docs/ai-query-feature.md)

### Feature requests (waiting on a decision)
- [ ] User preferences: where they are stored (proposed: the user profile on the server); first uses: which entity links show as chips in table cells, and the colours of the entity page's link areas
- [ ] Data origin: per table, a source description (origin system, owner, URL, licence, refresh) plus what loading knows (file, load time, rows), in a "Source" info tab; per column, an optional origin falling back to the table's, joined and computed columns explaining themselves. Open: "on request" as one toggle under every header, or a popup per column?
- [ ] List of tables without a dedicated page. Open: a searchable switcher dropping from the table title (proposed), or a side panel?
- [ ] Conditional links (visible or disabled) on other column values, in cells and on the entity page: `visible_if` / `enabled_if` expressions. Open: evaluated on the entity's own row in its home table (proposed) or on the cell's row? The request about links "only visible" was cut off.

### Security follow-ups (review of 2026-10-24)
- [ ] Import limits: maximum columns and cells (an import of 400 KB can ask for 10^10 cells), a linear duplicate-name loop (a header of repeated names is quadratic), a cap on the number or bytes of imports kept
- [ ] Bound the per-user table view cache (an unauthenticated `user=` creates an entry each)
- [ ] Authorization hook: one check for tables, joins, entity pages and related entity lists; `?user=` is no access control (document it). Design together with the embedding handler below
- [ ] Security headers: frame-ancestors, nosniff, Cache-Control private on per-user pages; timeouts on the demo server
- [ ] Hardening: expression length and nesting limits; CSS.escape the `#focus=` hash; the reflected XSS in experimental/interpunctus

### Page weight (feedback of 2026-10-25)
- [ ] Minify CSS/JS (inline assets ~188 KB → ~109 KB): decide on the dependency, or minify at build time
- [ ] An embedding http.Handler with a per-request Load hook (gzip, Server-Timing, entity/help routes, static assets), with the authorization hook
- [ ] Trim template whitespace (about 1 KB gzipped)
