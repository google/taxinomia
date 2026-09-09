        // Table page script. Every UI action changes the URL, which is the
        // single source of truth; the server renders the page for a URL.
        // Navigations between table pages are done as fragment navigations
        // (navigate()): the new page is fetched, its sidebar and main content
        // are swapped in, and the URL is pushed — no full reload, no re-parse
        // of this script or the stylesheet, scroll and sidebar state kept.
        // Anything the page cannot swap falls back to a full navigation.
        //
        // All event handling is delegated from the document, so a swapped-in
        // page needs no per-element wiring; afterSwap() does the little
        // per-page work that remains.
        //
        // Self-test hook: opening a page with "#navtest=<url>" performs one
        // fragment navigation to <url> on load and sets data-navtest="swapped"
        // on <html> when done (used by the headless smoke test).

        // ---------- URL helpers ----------

        function currentUrl() {
            return new URL(window.location);
        }

        // Toggle sidebar visibility (page-local state, not in the URL)
        function toggleSidebar() {
            const sidebar = document.getElementById('sidebar');
            const mainContent = document.getElementById('main-content');
            const openBtn = document.getElementById('sidebar-open-btn');

            sidebar.classList.toggle('collapsed');
            mainContent.classList.toggle('expanded');
            openBtn.classList.toggle('visible');
        }

        // Open the info pane on the given tab ('url' or 'perf'; updates URL)
        function showInfoPane(tabName) {
            const url = currentUrl();
            url.searchParams.set('info', '1');
            if (tabName === 'perf') {
                url.searchParams.set('infotab', 'perf');
            } else {
                url.searchParams.delete('infotab');
            }
            navigate(url);
        }

        // Collapse the info pane to the status bar (updates URL)
        function hideInfoPane() {
            const url = currentUrl();
            url.searchParams.delete('info');
            navigate(url);
        }

        // Toggle column types row visibility (updates URL)
        function toggleColumnTypes() {
            const url = currentUrl();
            if (url.searchParams.get('types') === '1') {
                url.searchParams.delete('types');
            } else {
                url.searchParams.set('types', '1');
            }
            navigate(url);
        }

        // Switch info pane tab (updates URL)
        function switchInfoTab(tabName) {
            showInfoPane(tabName);
        }

        // Fill the URL tab: the current URL and its parameters. Only when the
        // pane is open; a collapsed pane's content is not displayed.
        function parseUrlParams() {
            const pane = document.getElementById('info-pane');
            const paramList = document.getElementById('param-list');
            if (!pane || !paramList || pane.classList.contains('collapsed')) return;
            const urlParams = new URLSearchParams(window.location.search);

            paramList.innerHTML = '';
            // URLSearchParams already yields decoded values; decoding again
            // would throw on a literal '%' (e.g. a filter for "100%").
            for (const [key, value] of urlParams) {
                const item = document.createElement('li');
                item.className = 'param-item';

                const nameSpan = document.createElement('span');
                nameSpan.className = 'param-name';
                nameSpan.textContent = key + ':';

                const valueSpan = document.createElement('span');
                valueSpan.className = 'param-value';

                if (key === 'columns') {
                    const columns = value.split(',');
                    const formattedColumns = columns.map(col => {
                        // Joined column (format: fromColumn.toTable.toColumn.selectedColumn)
                        if (col.includes('.') && col.split('.').length === 4) {
                            const parts = col.split('.');
                            return `${parts[0]} → ${parts[1]}.${parts[3]}`;
                        }
                        return col;
                    });
                    valueSpan.textContent = formattedColumns.join(', ');
                } else {
                    valueSpan.textContent = value;
                }

                item.appendChild(nameSpan);
                item.appendChild(valueSpan);
                paramList.appendChild(item);
            }

            const urlDisplay = document.getElementById('url-display');
            let readableHref = window.location.href;
            try {
                readableHref = decodeURIComponent(readableHref);
            } catch (e) {
                // keep the raw href
            }
            urlDisplay.textContent = readableHref;
        }

        // Apply filter by updating URL
        function applyFilter(columnName, filterValue) {
            const url = currentUrl();
            const paramKey = 'filter:' + columnName;

            if (filterValue && filterValue.trim() !== '') {
                url.searchParams.set(paramKey, filterValue.trim());
            } else {
                url.searchParams.delete(paramKey);
            }
            navigate(url);
        }

        // Restore scroll position from URL parameter (full loads only; a
        // fragment navigation keeps the scroll position by itself)
        function restoreScrollPosition() {
            const url = currentUrl();
            const scrollY = parseInt(url.searchParams.get('scrollY'), 10);
            if (url.searchParams.has('scrollY')) {
                if (scrollY > 0 && !fragmentState.swapped) {
                    window.scrollTo(0, scrollY);
                }
                url.searchParams.delete('scrollY');
                history.replaceState(history.state, '', url.toString());
            }
        }

        // ---------- Waiting indicator ----------

        // Shown the moment a navigation starts — full or fragment — with an
        // elapsed-time counter, so a slow query is visibly in progress
        // instead of leaving the old page frozen.
        const waiting = { timer: null, start: 0 };

        function showWaiting() {
            const el = document.getElementById('waiting');
            if (!el) return;
            waiting.start = performance.now();
            el.hidden = false;
            const secs = document.getElementById('waiting-secs');
            if (waiting.timer) clearInterval(waiting.timer);
            waiting.timer = setInterval(function() {
                if (secs) secs.textContent = ((performance.now() - waiting.start) / 1000).toFixed(1);
            }, 100);
        }

        function hideWaiting() {
            const el = document.getElementById('waiting');
            if (el) el.hidden = true;
            if (waiting.timer) {
                clearInterval(waiting.timer);
                waiting.timer = null;
            }
        }

        // ---------- Fragment navigation ----------

        const fragmentState = { swapped: false, inflight: null, navtest: false };

        // isFragmentTarget: same origin and the same route as this page — a
        // table page of this product. Anything else (landing page, entity
        // URLs elsewhere, other products) is a full navigation.
        function isFragmentTarget(url) {
            return url.origin === window.location.origin && url.pathname === window.location.pathname;
        }

        // navigate replaces window.location.href assignments: a fragment
        // navigation when the target is a table page, a full one otherwise or
        // when anything goes wrong (the error page then shows as before).
        function navigate(target, options) {
            const url = new URL(target, window.location);
            const push = !(options && options.push === false);
            if (!isFragmentTarget(url) || !window.fetch || !window.DOMParser) {
                showWaiting();
                window.location.href = url.toString();
                return;
            }
            showWaiting();
            const t0 = performance.now();
            const controller = window.AbortController ? new AbortController() : null;
            if (fragmentState.inflight) fragmentState.inflight.abort();
            fragmentState.inflight = controller;
            const req = fetch(url.toString(), {
                credentials: 'same-origin',
                headers: { 'Accept': 'text/html', 'X-Taxinomia-Fragment': '1' },
                signal: controller ? controller.signal : undefined
            });
            req.then(function(resp) {
                const ct = resp.headers.get('Content-Type') || '';
                if (!resp.ok || ct.indexOf('text/html') === -1) throw new Error('not a page: ' + resp.status);
                return resp.text();
            }).then(function(html) {
                const t1 = performance.now();
                const doc = new DOMParser().parseFromString(html, 'text/html');
                const newMain = doc.getElementById('main-content');
                const newSidebar = doc.getElementById('sidebar');
                if (!newMain || !newSidebar) throw new Error('not a table page');
                swapPage(doc, newMain, newSidebar);
                if (push) {
                    history.pushState({ taxinomia: true }, '', url.toString());
                } else {
                    history.replaceState({ taxinomia: true }, '', url.toString());
                }
                fragmentState.swapped = true;
                afterSwap();
                recordFragmentTiming(t0, t1);
            }).catch(function(err) {
                if (err && err.name === 'AbortError') return;
                window.location.href = url.toString();
            });
        }

        // swapPage replaces the sidebar (only if it changed) and the main
        // content; the outer elements keep their page-local state (sidebar
        // collapsed, main expanded).
        function swapPage(doc, newMain, newSidebar) {
            const sidebar = document.getElementById('sidebar');
            if (sidebar.innerHTML !== newSidebar.innerHTML) {
                sidebar.innerHTML = newSidebar.innerHTML;
            }
            document.getElementById('main-content').innerHTML = newMain.innerHTML;
            document.title = doc.title;
        }

        // afterSwap does the per-page work a fresh load does at
        // DOMContentLoaded; everything else is delegated and needs nothing.
        function afterSwap() {
            applyColumnWidths();
            updateHiddenRowsCount();
            parseUrlParams();
            restoreScrollPosition();
            focusNewComputedColumn();
            scheduleAnimCleanup();
            hideWaiting();
            if (fragmentState.navtest) {
                document.documentElement.setAttribute('data-navtest', 'swapped');
            }
        }

        // recordFragmentTiming fills the status bar's Total and the perf
        // tab's browser section for a fragment navigation: fetch (server +
        // transfer) and swap + layout, measured to the next frame.
        function recordFragmentTiming(t0, t1) {
            requestAnimationFrame(function() {
                const t2 = performance.now();
                const total = t2 - t0;
                const timingElement = document.getElementById('browser-timing');
                if (timingElement) timingElement.textContent = total.toFixed(0) + 'ms';
                const perfElement = document.getElementById('perf-browser-total');
                if (perfElement) perfElement.textContent = total.toFixed(0) + 'ms';
                const list = document.getElementById('browser-timing-list');
                const totalItem = document.getElementById('perf-browser-total-item');
                if (!list || !totalItem) return;
                const add = function(label, ms) {
                    const li = document.createElement('li');
                    li.className = 'perf-timing-item';
                    const op = document.createElement('span');
                    op.className = 'perf-operation';
                    op.textContent = label;
                    const dur = document.createElement('span');
                    dur.className = 'perf-duration';
                    dur.textContent = ms.toFixed(1) + 'ms';
                    li.appendChild(op);
                    li.appendChild(dur);
                    list.insertBefore(li, totalItem);
                };
                add('Fragment navigation: fetch (server + transfer)', t1 - t0);
                add('Fragment navigation: swap, layout and paint', t2 - t1);
                totalItem.querySelector('.perf-operation').textContent = 'Total (click to painted)';
            });
        }

        // ---------- Browser timing (full loads) ----------

        // Fills the status bar's "Total" and the Performance tab's "Browser
        // Timing" section from the browser's own Navigation, Resource and
        // Paint timing entries: where the time between the server finishing
        // and the page being usable went (transfer, HTML parse, scripts,
        // layout/paint), plus whether the stylesheet and script came from
        // the cache.
        function displayBrowserTiming() {
            const nav = performance.getEntriesByType('navigation')[0];
            let totalTime;
            if (nav) {
                totalTime = nav.loadEventEnd - nav.startTime;
            } else if (performance.timing) {
                totalTime = performance.timing.loadEventEnd - performance.timing.navigationStart;
            }
            const ms = function(v) { return (v > 0 ? v : 0).toFixed(1) + 'ms'; };
            const kb = function(b) { return b >= 1024 ? (b / 1024).toFixed(1) + ' KB' : b + ' B'; };
            const timeStr = (totalTime && totalTime > 0) ? totalTime.toFixed(0) + 'ms' : 'N/A';

            const timingElement = document.getElementById('browser-timing');
            if (timingElement) {
                timingElement.textContent = timeStr;
            }
            const perfElement = document.getElementById('perf-browser-total');
            if (perfElement) {
                perfElement.textContent = timeStr;
            }

            const list = document.getElementById('browser-timing-list');
            const totalItem = document.getElementById('perf-browser-total-item');
            if (!list || !nav) return;
            const addRow = function(label, value, detail, sub) {
                const li = document.createElement('li');
                li.className = 'perf-timing-item' + (sub ? ' sub' : '');
                const op = document.createElement('span');
                op.className = 'perf-operation';
                op.textContent = label;
                const vol = document.createElement('span');
                vol.className = 'perf-volume';
                vol.textContent = detail || '';
                const dur = document.createElement('span');
                dur.className = 'perf-duration';
                dur.textContent = value;
                li.appendChild(op);
                li.appendChild(vol);
                li.appendChild(dur);
                list.insertBefore(li, totalItem);
            };

            if (nav.connectEnd - nav.startTime > 1) {
                addRow('Connect (DNS, TCP, TLS)', ms(nav.connectEnd - nav.startTime));
            }
            addRow('Request to first byte (server + network)', ms(nav.responseStart - nav.requestStart));
            addRow('Download HTML', ms(nav.responseEnd - nav.responseStart),
                nav.encodedBodySize ? kb(nav.encodedBodySize) + ' on the wire → ' + kb(nav.decodedBodySize) : '');
            addRow('Parse HTML (to DOM interactive)', ms(nav.domInteractive - nav.responseEnd));
            addRow('Scripts and DOMContentLoaded handlers', ms(nav.domContentLoadedEventEnd - nav.domInteractive));
            addRow('Layout and paint (to load event)', ms(nav.loadEventEnd - nav.domContentLoadedEventEnd));
            performance.getEntriesByType('paint').forEach(function(p) {
                if (p.name === 'first-contentful-paint') {
                    addRow('First contentful paint', ms(p.startTime), 'from navigation start');
                }
            });
            performance.getEntriesByType('resource').forEach(function(r) {
                const file = r.name.split('/').pop().split('?')[0];
                if (file !== 'table.css' && file !== 'table.js') return;
                const fromCache = r.transferSize === 0;
                addRow(file + (fromCache ? ' (from cache)' : ' (fetched)'), ms(r.duration),
                    fromCache ? kb(r.decodedBodySize) : kb(r.transferSize) + ' on the wire → ' + kb(r.decodedBodySize), true);
            });
        }

        // ---------- Rows ----------

        // Calculate and display hidden rows count
        function updateHiddenRowsCount() {
            const table = document.getElementById('data-table');
            const hiddenRowsSpan = document.getElementById('hidden-rows-count');
            if (!table || !hiddenRowsSpan) return;

            const totalRows = parseInt(table.dataset.totalRows, 10) || 0;
            const displayedRows = parseInt(table.dataset.displayedRows, 10) || 0;
            const hiddenRows = totalRows - displayedRows;
            hiddenRowsSpan.textContent = hiddenRows.toLocaleString();
        }

        // ---------- Column widths ----------

        // Get current column widths from table headers
        function getCurrentColumnWidths() {
            const widths = {};
            const table = document.getElementById('data-table');
            if (!table) return widths;

            const headers = table.querySelectorAll('thead tr:first-child th');
            headers.forEach(function(th) {
                const colName = th.dataset.colName;
                // Only include if width was explicitly set (has inline style)
                if (colName && th.style.width) {
                    widths[colName] = parseInt(th.style.width, 10);
                }
            });
            return widths;
        }

        // Update URL with current column widths
        // If reload is true, navigates to the new URL; otherwise uses replaceState
        function updateUrlWithWidths(reload) {
            const url = currentUrl();
            const columnsParam = url.searchParams.get('columns');
            if (!columnsParam) return;

            const widths = getCurrentColumnWidths();
            const columns = columnsParam.split(',');

            // Rebuild columns param with widths
            const newColumns = columns.map(function(col) {
                // Strip existing width if present
                const colonIdx = col.lastIndexOf(':');
                let colName = col;
                if (colonIdx !== -1) {
                    const possibleWidth = col.substring(colonIdx + 1);
                    if (/^\d+$/.test(possibleWidth)) {
                        colName = col.substring(0, colonIdx);
                    }
                }
                if (widths[colName]) {
                    return colName + ':' + widths[colName];
                }
                return colName;
            });

            url.searchParams.set('columns', newColumns.join(','));

            if (reload) {
                navigate(url);
            } else {
                history.replaceState(history.state, '', url.toString());
            }
        }

        // Apply column widths from data attributes (set by backend)
        function applyColumnWidths() {
            const table = document.getElementById('data-table');
            if (!table) return;

            const headers = table.querySelectorAll('thead tr:first-child th');
            headers.forEach(function(th) {
                const width = parseInt(th.dataset.colWidth, 10);
                if (width > 0) {
                    th.style.width = width + 'px';
                }
            });
        }

        // Reset column widths to default (removes widths from URL)
        function resetColumnWidths() {
            const url = currentUrl();
            const columnsParam = url.searchParams.get('columns');
            if (!columnsParam) return;

            const columns = columnsParam.split(',').map(function(col) {
                const colonIdx = col.lastIndexOf(':');
                if (colonIdx !== -1) {
                    const possibleWidth = col.substring(colonIdx + 1);
                    if (/^\d+$/.test(possibleWidth)) {
                        return col.substring(0, colonIdx);
                    }
                }
                return col;
            });

            url.searchParams.set('columns', columns.join(','));
            navigate(url);
        }

        // ---------- Column resize (delegated) ----------

        const resize = { th: null, handle: null, startX: 0, startWidth: 0 };

        function onResizeMouseMove(e) {
            const diff = e.pageX - resize.startX;
            const newWidth = Math.max(50, resize.startWidth + diff); // Minimum 50px width
            resize.th.style.width = newWidth + 'px';
        }

        function onResizeMouseUp() {
            document.removeEventListener('mousemove', onResizeMouseMove);
            document.removeEventListener('mouseup', onResizeMouseUp);
            resize.handle.classList.remove('resizing');
            document.body.classList.remove('resizing');
            resize.th = null;
            resize.handle = null;
            // Update URL with new widths and navigate
            updateUrlWithWidths(true);
        }

        document.addEventListener('mousedown', function(e) {
            const handle = e.target.closest('.resize-handle');
            if (!handle) return;
            const th = handle.closest('th');
            if (!th) return;
            e.preventDefault();
            resize.th = th;
            resize.handle = handle;
            resize.startX = e.pageX;
            resize.startWidth = th.offsetWidth;
            handle.classList.add('resizing');
            document.body.classList.add('resizing');
            document.addEventListener('mousemove', onResizeMouseMove);
            document.addEventListener('mouseup', onResizeMouseUp);
        });

        // ---------- Column drag-and-drop (delegated) ----------

        let draggedHeader = null;

        // The server orders columns by zone (filtered, grouped, others); a
        // drag only means something within one zone. Grouped columns reorder
        // the grouping hierarchy; the other zones reorder the columns
        // parameter.
        function columnZone(th) {
            if (th.dataset.isGrouped) return 'grouped';
            if (th.dataset.isFiltered) return 'filtered';
            return 'other';
        }

        function headerCells() {
            const table = document.getElementById('data-table');
            return table ? table.querySelectorAll('thead tr:first-child th') : [];
        }

        function clearDragIndicators() {
            headerCells().forEach(function(header) {
                header.classList.remove('drag-over-left', 'drag-over-right');
            });
        }

        function dragTarget(e) {
            return e.target.closest ? e.target.closest('thead tr:first-child th[data-col-name]') : null;
        }

        document.addEventListener('dragstart', function(e) {
            const th = dragTarget(e);
            if (!th) return;
            draggedHeader = th;
            document.body.classList.add('column-dragging');
            e.dataTransfer.effectAllowed = 'move';
            e.dataTransfer.setData('text/plain', th.dataset.colName);
            // Let the drag image be captured before adding opacity
            setTimeout(function() {
                th.classList.add('dragging');
            }, 0);
        });

        document.addEventListener('dragend', function(e) {
            const th = dragTarget(e);
            if (th) th.classList.remove('dragging');
            document.body.classList.remove('column-dragging');
            draggedHeader = null;
            clearDragIndicators();
        });

        document.addEventListener('dragover', function(e) {
            const th = dragTarget(e);
            if (!th || !draggedHeader || draggedHeader === th) return;
            // Cross-zone drops are no-ops (the server snaps zones back);
            // without preventDefault the browser shows the no-drop cursor and
            // no indicators appear.
            if (columnZone(draggedHeader) !== columnZone(th)) return;
            e.preventDefault();
            e.dataTransfer.dropEffect = 'move';
            const rect = th.getBoundingClientRect();
            const isLeftHalf = e.clientX < rect.left + rect.width / 2;
            clearDragIndicators();
            th.classList.add(isLeftHalf ? 'drag-over-left' : 'drag-over-right');
        });

        document.addEventListener('dragleave', function(e) {
            const th = dragTarget(e);
            if (th) th.classList.remove('drag-over-left', 'drag-over-right');
        });

        document.addEventListener('drop', function(e) {
            const th = dragTarget(e);
            if (!th) return;
            e.preventDefault();
            if (!draggedHeader || draggedHeader === th) return;
            if (columnZone(draggedHeader) !== columnZone(th)) return;

            const rect = th.getBoundingClientRect();
            const dropOnLeft = e.clientX < rect.left + rect.width / 2;
            const url = currentUrl();

            // Grouped columns: their display order is the grouping hierarchy
            // (the grouped= parameter), not the columns= order — reorder the
            // hierarchy itself. Expansion paths (gexp) encode the old level
            // order, so drop them.
            if (columnZone(draggedHeader) === 'grouped') {
                const grouped = (url.searchParams.get('grouped') || '').split(',').filter(Boolean);
                const draggedName = draggedHeader.dataset.colName;
                const from = grouped.indexOf(draggedName);
                let to = grouped.indexOf(th.dataset.colName);
                if (from === -1 || to === -1) return;
                grouped.splice(from, 1);
                if (from < to) to--;
                grouped.splice(dropOnLeft ? to : to + 1, 0, draggedName);
                url.searchParams.set('grouped', grouped.join(','));
                url.searchParams.delete('gexp');
                navigate(url);
                return;
            }

            // Current column order from the URL, or built from the headers
            let columnsParam = url.searchParams.get('columns');
            if (!columnsParam) {
                const colNames = [];
                headerCells().forEach(function(header) {
                    const colName = header.dataset.colName;
                    const width = header.style.width ? parseInt(header.style.width, 10) : 0;
                    if (colName) {
                        colNames.push(width > 0 ? colName + ':' + width : colName);
                    }
                });
                columnsParam = colNames.join(',');
            }

            const columns = columnsParam.split(',');
            const draggedColName = draggedHeader.dataset.colName;
            const targetColName = th.dataset.colName;
            let draggedIdx = -1;
            let targetIdx = -1;
            columns.forEach(function(col, idx) {
                const colName = col.split(':')[0];
                if (colName === draggedColName) draggedIdx = idx;
                if (colName === targetColName) targetIdx = idx;
            });
            if (draggedIdx === -1 || targetIdx === -1) return;

            const draggedCol = columns.splice(draggedIdx, 1)[0];
            if (draggedIdx < targetIdx) {
                targetIdx--;
            }
            columns.splice(dropOnLeft ? targetIdx : targetIdx + 1, 0, draggedCol);
            url.searchParams.set('columns', columns.join(','));
            navigate(url);
        });

        // ---------- Filters, multiselect, computed columns (delegated) ----------

        document.addEventListener('focusin', function(e) {
            const t = e.target;
            if (t.matches && (t.matches('.filter-input') || t.matches('.th-name-input') || t.matches('.formula-cell .formula-input'))) {
                t.dataset.originalValue = t.value;
            }
        });

        document.addEventListener('focusout', function(e) {
            const t = e.target;
            if (!t.matches) return;
            if (t.matches('.filter-input')) {
                if (t.value !== t.dataset.originalValue) {
                    applyFilter(t.dataset.column, t.value);
                }
            } else if (t.matches('.th-name-input')) {
                const originalName = t.dataset.originalValue;
                const newName = t.value.trim();
                if (newName && newName !== originalName) {
                    renameComputedColumn(originalName, newName);
                } else {
                    t.value = originalName;
                }
            } else if (t.matches('.formula-cell .formula-input')) {
                const originalValue = t.dataset.originalValue;
                const newValue = t.value.trim();
                if (newValue !== originalValue && newValue !== '') {
                    updateComputedFormula(t.dataset.column, newValue);
                } else if (newValue === '') {
                    t.value = originalValue;
                }
            }
        });

        document.addEventListener('keydown', function(e) {
            const t = e.target;
            if (!t.matches) return;
            if (t.matches('.filter-input')) {
                if (e.key === 'Enter') {
                    e.preventDefault();
                    applyFilter(t.dataset.column, t.value);
                } else if (e.key === 'Escape') {
                    e.preventDefault();
                    t.value = '';
                    applyFilter(t.dataset.column, '');
                }
            } else if (t.matches('.th-name-input')) {
                const originalName = t.dataset.originalValue !== undefined ? t.dataset.originalValue : t.value;
                if (e.key === 'Enter') {
                    e.preventDefault();
                    const newName = t.value.trim();
                    if (newName !== originalName) {
                        renameComputedColumn(originalName, newName);
                    }
                    t.blur();
                } else if (e.key === 'Escape') {
                    t.value = originalName;
                    t.blur();
                }
            } else if (t.matches('.formula-cell .formula-input')) {
                const originalValue = t.dataset.originalValue !== undefined ? t.dataset.originalValue : t.value;
                if (e.key === 'Enter') {
                    e.preventDefault();
                    const newValue = t.value.trim();
                    if (newValue !== originalValue) {
                        updateComputedFormula(t.dataset.column, newValue);
                    }
                    t.blur();
                } else if (e.key === 'Escape') {
                    t.value = originalValue;
                    t.blur();
                }
            }
        });

        document.addEventListener('click', function(e) {
            const clear = e.target.closest('.filter-clear');
            if (clear) {
                const columnName = clear.dataset.column;
                const input = document.querySelector(`.filter-input[data-column="${columnName}"]`);
                if (input) input.value = '';
                applyFilter(columnName, '');
                return;
            }

            const toggle = e.target.closest('.multiselect-toggle');
            if (toggle) {
                const table = document.getElementById('data-table');
                if (!table) return;
                const columnName = toggle.dataset.column;
                if (toggle.classList.contains('active')) {
                    // Exiting multi-select mode - apply filter with selected values
                    const checkboxes = table.querySelectorAll(`.multiselect-checkbox[data-column="${columnName}"]:checked`);
                    const values = Array.from(checkboxes).map(cb => cb.dataset.value);
                    table.querySelectorAll(`td[data-column="${columnName}"]`).forEach(td => {
                        td.classList.remove('multiselect-active');
                    });
                    toggle.classList.remove('active');
                    if (values.length > 0) {
                        applyFilter(columnName, values.join('|'));
                    }
                } else {
                    toggle.classList.add('active');
                    table.querySelectorAll(`td[data-column="${columnName}"]`).forEach(td => {
                        td.classList.add('multiselect-active');
                    });
                    table.querySelectorAll(`.multiselect-checkbox[data-column="${columnName}"]`).forEach(cb => {
                        cb.checked = false;
                    });
                }
                return;
            }

            if (e.target.closest('#add-computed-btn')) {
                createNewComputedColumn();
                return;
            }

            const remove = e.target.closest('.remove-computed-btn');
            if (remove) {
                const columnName = remove.dataset.columnName;
                if (columnName) removeComputedColumn(columnName);
                return;
            }

            // Row selection (flat rows only): a click on a row that is not on
            // a control opens the detail panel; on the selected row, closes it.
            const row = e.target.closest('tbody tr[data-row-id]');
            if (row && !e.target.closest('a, button, input, .filter-link, .entity-link, .multiselect-checkbox')) {
                const rowId = row.dataset.rowId;
                if (rowId) {
                    const currentRowId = currentUrl().searchParams.get('row') || '';
                    if (currentRowId === rowId) {
                        deselectRow();
                    } else {
                        selectRow(rowId);
                    }
                }
                return;
            }

            // Plain links to table pages become fragment navigations.
            const a = e.target.closest('a[href]');
            if (!a) return;
            if (e.defaultPrevented || e.button !== 0 || e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) return;
            if (a.target && a.target !== '_self') return;
            if (a.hasAttribute('download')) return;
            const href = a.getAttribute('href');
            if (!href || href.startsWith('javascript:') || href.startsWith('#')) return;
            const url = new URL(a.href, window.location);
            if (!isFragmentTarget(url)) return;
            e.preventDefault();
            navigate(url);
        });

        // Back/forward: re-fetch the page for the URL the browser moved to.
        window.addEventListener('popstate', function() {
            if (fragmentState.swapped || (history.state && history.state.taxinomia)) {
                navigate(window.location.href, { push: false });
            }
        });

        // ---------- Per-page work ----------

        // Focus the formula input of a just-created computed column
        function focusNewComputedColumn() {
            if (!window.location.hash.startsWith('#focus=')) return;
            const columnName = window.location.hash.substring(7);
            const formulaInput = document.querySelector('.formula-input[data-column="' + columnName + '"]');
            if (formulaInput) {
                formulaInput.focus();
                formulaInput.select();
                // Clear the hash to avoid re-focusing on refresh
                history.replaceState(history.state, '', window.location.pathname + window.location.search);
            }
        }

        // Clean up the _anim parameter after the grouping animation plays, so a
        // refresh does not replay it
        function scheduleAnimCleanup() {
            if (!currentUrl().searchParams.has('_anim')) return;
            setTimeout(function() {
                const u = currentUrl();
                if (u.searchParams.has('_anim')) {
                    u.searchParams.delete('_anim');
                    history.replaceState(history.state, '', u.toString());
                }
            }, 1500);
        }

        function initPage() {
            applyColumnWidths();
            updateHiddenRowsCount();
            parseUrlParams();
            restoreScrollPosition();
            focusNewComputedColumn();
            scheduleAnimCleanup();
            // Self-test hook (see the file comment). Only a table page of this
            // route qualifies: the hash must never be able to send the
            // browser elsewhere.
            if (window.location.hash.startsWith('#navtest=')) {
                const target = new URL(decodeURIComponent(window.location.hash.substring(9)), window.location);
                history.replaceState(null, '', window.location.pathname + window.location.search);
                if (isFragmentTarget(target)) {
                    fragmentState.navtest = true;
                    navigate(target);
                }
            }
        }

        window.addEventListener('DOMContentLoaded', initPage);

        // Display browser timing after page is fully loaded
        window.addEventListener('load', function() {
            // Use setTimeout to ensure loadEventEnd is populated
            setTimeout(displayBrowserTiming, 0);
        });

        // A full navigation starts: show the indicator until the new page
        // replaces this one. pageshow (including back/forward cache restores)
        // hides it again.
        window.addEventListener('beforeunload', showWaiting);
        window.addEventListener('pageshow', hideWaiting);
