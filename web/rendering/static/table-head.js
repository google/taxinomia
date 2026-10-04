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
            document.body.classList.add('waiting-cursor');
            const secs = document.getElementById('waiting-secs');
            if (waiting.timer) clearInterval(waiting.timer);
            waiting.timer = setInterval(function() {
                if (secs) secs.textContent = ((performance.now() - waiting.start) / 1000).toFixed(1);
            }, 100);
        }

        function hideWaiting() {
            const el = document.getElementById('waiting');
            if (el) el.hidden = true;
            document.body.classList.remove('waiting-cursor');
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
        // The page's asset URLs (they carry the build revision); empty when
        // the assets are inlined.
        function assetVersion(doc) {
            const script = doc.querySelector('script[src]');
            const css = doc.querySelector('link[rel="stylesheet"]');
            return (script ? script.getAttribute('src') : '') + '|' + (css ? css.getAttribute('href') : '');
        }

        function isFragmentTarget(url) {
            return url.origin === window.location.origin && url.pathname === window.location.pathname;
        }

        // navigate replaces window.location.href assignments: a fragment
        // navigation when the target is a table page, a full one otherwise or
        // when anything goes wrong (the error page then shows as before).
        function navigate(target, options) {
            hideHelp();
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
                // A new build (another script or stylesheet version) needs
                // the full page: swapping would keep running this script.
                if (assetVersion(doc) !== assetVersion(document)) throw new Error('new build');
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
            showHelpNoteIfAny();
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
            if (e.target.closest('#help-toggle, #help-toggle-pane')) {
                toggleHelp();
                return;
            }
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

            if (e.target.closest('#add-computed-btn, .pane-add-hint')) {
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
            showHelpNoteIfAny();
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
            // "#help" opens the control labels on load (a link docs can give).
            if (window.location.hash === "#help") {
                setTimeout(showHelp, 0);
            } else if (window.location.hash.startsWith("#help-card=")) {
                // ... and "#help-card=<label>" also opens that label's card.
                const wanted = decodeURIComponent(window.location.hash.substring(11));
                setTimeout(function() {
                    showHelp();
                    const bubble = Array.from(document.querySelectorAll(".help-bubble")).find(b => b.textContent === wanted);
                    if (bubble) bubble.click();
                }, 0);
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

        // --- Help: what each control is, and why use it ---------------------
        // The "?" button in the status bar pins a 2-3 word label on the first
        // visible instance of each kind of control, coloured by area. Labels
        // follow the page's state (a flat table says "Group by", a grouped
        // one "Nest another level"); controls that are not on the page get
        // none, so this works on any table. Clicking a label opens a card:
        // one sentence on why to use the control and, where it applies, a
        // "Show me" that operates the real control on this table; a note
        // after the page changes names what happened and offers Undo (the
        // browser's back). Esc, a click elsewhere, scrolling or a navigation
        // closes the labels; a resize places them again.
        // The open card's label item, and where each label was placed (for
        // reopening the card when a resize places the labels again).
        let openHelpItem = null;
        let helpPlacements = new Map();

        function isGroupedPage() {
            return !!document.querySelector('.group-toggle-btn.grouped');
        }

        const HELP_AREAS = {
            view: 'View and sharing',
            sort: 'Sorting',
            group: 'Grouping and totals',
            filter: 'Filtering',
            columns: 'Columns and joins',
            computed: 'Computed columns',
        };

        // show: 'click' operates the control itself; a function does
        // something else; absent means the card only explains.
        const HELP_LABELS = [
            {sel: '.limit-btn', label: 'Fewer / more rows', area: 'view', show: 'click',
             why: 'The table lists a screenful of rows; these list fewer or more. The rest are still counted.'},
            {sel: '.type-toggle-btn', label: 'Column types', area: 'view', show: 'click',
             why: 'Shows how each column is stored (text, number, date), which decides how it sorts and totals.'},
            {sel: 'thead tr:first-child th[draggable] .th-content', label: 'Drag: sort priority', area: 'sort', inside: true, show: moveFirstColumnRight,
             why: 'Rows are always sorted by the columns, left to right. Drag a header left to make it sort first.'},
            {sel: 'thead .resize-handle', label: 'Drag to resize', area: 'columns',
             why: 'Drag the edge of a header to make the column wider or narrower. Widths are kept in the link.'},
            {sel: '.sort-toggle-btn', label: 'Flip sort', area: 'sort', show: 'click',
             why: 'Flips this column between ascending and descending, without changing its priority.'},
            {sel: '.group-toggle-btn:not(.grouped)', label: () => isGroupedPage() ? 'Nest another level' : 'Group by', area: 'group', show: 'click',
             why: () => isGroupedPage()
                ? 'Groups each group again by this column, one level deeper.'
                : 'Collapses rows with the same value into one group, with its count and totals.',
             how: () => isGroupedPage() ? null : [
                'Once grouped, the other columns get total buttons (# ◇ Σ μ σ ↓ ↑) for per-group totals.',
                'The grouped column gets a ⟳ button that sorts its groups by one of those totals.',
             ]},
            {sel: '.group-toggle-btn.grouped', label: 'Ungroup', area: 'group', show: 'click',
             why: 'Turns this column back into a plain column.'},
            {sel: '.agg-sort-toggle-btn', label: 'Sort groups by total', area: 'sort', show: 'click',
             why: 'Orders the groups by a number instead of by their value: the biggest (or smallest) groups first.',
             how: [
                'Each click steps to the next choice: rows per group, subgroups per group, then every total switched on in the other columns (Σ, μ, ...).',
                'The number the groups are sorted by is shown in bold (a total also in blue).',
                'The arrow next to ⟳ flips the order: biggest first or smallest first.',
                'Keep clicking until the groups are sorted by their value again.',
                'Only the totals switched on can be sorted by: turn one on first in its column.',
             ]},
            {sel: '.agg-toggle-btn', label: 'Per-group totals', area: 'group', show: 'click',
             why: 'Switches a total of this column on or off for every group. Totals show in each group\'s row and add up in the group above.',
             how: [
                '# rows  ◇ distinct values',
                'Σ sum  μ average  σ spread (standard deviation)',
                '↓ smallest  ↑ largest',
                '✓ ✗ % for yes/no columns: how many true, how many false, share true',
                'A total switched on can then sort the groups: the ⟳ button of the grouped column.',
             ]},
            {sel: '.stats-cell', label: () => isGroupedPage() ? 'Groups / filtered / total' : 'Filtered / total rows', area: 'filter', inside: true,
             why: () => isGroupedPage()
                ? 'How many groups there are, how many rows the filters keep, and how many rows the table has.'
                : 'How many rows the filters keep, and how many rows the table has.'},
            {sel: '.filter-input', label: 'text, "exact", a|b', area: 'filter', show: typeExampleFilter,
             why: 'Type a word to find it anywhere in the value, "quoted" for an exact match, or a|b for any of several values. Enter applies it.'},
            {sel: '.multiselect-toggle', label: 'Pick several values', area: 'filter', show: 'click',
             why: 'Tick several groups to keep only those, as one filter.'},
            {sel: 'tbody td.group-cell', label: 'Value [subgroups/rows]', area: 'group', inside: true,
             why: 'Each group shows its value and, in brackets, how many subgroups and rows it holds.'},
            {sel: 'tbody .filter-link', label: 'Drill into group', area: 'filter', show: 'click',
             why: 'Keeps only this group\'s rows and ungroups the column, to look inside the group.'},
            {sel: 'tbody .group-expand', label: 'List group rows', area: 'group', show: 'click',
             why: 'Lists this group\'s own rows right under it, without leaving the grouped view.'},
            {sel: 'tbody .entity-link', label: 'Open related', area: 'columns', show: 'click',
             why: 'A linked value leads to the matching rows in a related table.'},
            {sel: 'tbody tr[data-row-id] td:last-child', label: 'Click: row details', area: 'view', inside: true, show: 'click',
             why: 'Click a row to see all its values and where it sits in the hierarchies.'},
            {sel: '.formula-input', label: 'Edit formula', area: 'computed',
             why: 'This column is computed from the others. Edit its expression and press Enter.'},
            {sel: '#add-computed-btn', label: 'New computed column', area: 'computed', show: 'click',
             why: 'Adds a column computed from an expression over the others, for this view only.'},
            {sel: '#sidebar a.pane-disclosure', label: 'Join other tables', area: 'columns', show: 'click',
             why: 'Opens the tables this column links to, so you can add their columns here.'},
            {sel: '#sidebar .pane-main .pane-toggle', label: 'Show / hide column', area: 'columns', show: 'click',
             why: 'Adds the column to the table, or removes it.'},
            {sel: '.info-pane-toggle[data-help="url"]', label: 'Shareable view', area: 'view', show: 'click',
             why: 'Everything you see is in the link: copy it to share or bookmark exactly this view.'},
            {sel: '.info-pane-toggle[data-help="perf"]', label: 'Query cost', area: 'view', show: 'click',
             why: 'Shows how long each part of this view took, with links to switch costly parts off.'},
        ];

        const textOf = v => typeof v === 'function' ? v() : v;

        // "Show me" for the sort-priority drag: the first column moves one
        // place right (what dragging it would do).
        function moveFirstColumnRight() {
            const names = Array.from(headerCells()).map(th => th.dataset.colName).filter(Boolean);
            if (names.length < 2) return false;
            [names[0], names[1]] = [names[1], names[0]];
            const url = currentUrl();
            url.searchParams.set('columns', names.join(','));
            navigate(url);
            return true;
        }

        // "Show me" for the filter box: filter the first column on the start
        // of a value from the table itself.
        function typeExampleFilter(input) {
            const column = input.dataset.column;
            let sample = '';
            const groupCell = document.querySelector('tbody td.group-cell');
            if (groupCell) {
                sample = groupCell.textContent.trim().split(/\s+\[/)[0];
            } else {
                const cells = Array.from(input.closest('tr').children);
                const idx = cells.indexOf(input.closest('td'));
                const firstRow = document.querySelector('tbody tr');
                if (firstRow && firstRow.children[idx]) sample = firstRow.children[idx].textContent.trim();
            }
            if (!sample || sample === '[error]') return false;
            const word = sample.length > 4 ? sample.substring(0, Math.ceil(sample.length / 2)) : sample;
            input.value = word;
            input.classList.add('help-typed');
            setTimeout(() => applyFilter(column, word), 600); // let the typed text be seen
            return true;
        }

        function firstVisible(selector) {
            const vw = window.innerWidth, vh = window.innerHeight;
            for (const el of document.querySelectorAll(selector)) {
                const r = el.getBoundingClientRect();
                if (r.width > 0 && r.height > 0 && r.right > 0 && r.left < vw && r.bottom > 0 && r.top < vh) {
                    return el;
                }
            }
            return null;
        }

        function overlaps(a, b) {
            return a.left < b.right && b.left < a.right && a.top < b.bottom && b.top < a.bottom;
        }

        function showHelp() {
            hideHelp();
            const layer = document.createElement('div');
            layer.id = 'help-layer';
            layer.className = 'help-layer';
            document.body.appendChild(layer);
            const placed = [];
            const vw = window.innerWidth, vh = window.innerHeight;
            // Find every labelled control first, so a label avoids covering
            // the others (large ones, such as a whole header, excepted).
            const targets = [];
            for (const item of HELP_LABELS) {
                const el = firstVisible(item.sel);
                if (el) targets.push({el: el, item: item, r: el.getBoundingClientRect()});
            }
            for (const t of targets) {
                if (t.r.width * t.r.height < 4000) placed.push({left: t.r.left, right: t.r.right, top: t.r.top, bottom: t.r.bottom});
            }
            // Labels inside their (large) target go first; the others avoid them.
            targets.sort((a, b) => (b.item.inside ? 1 : 0) - (a.item.inside ? 1 : 0));
            const areasShown = new Set();
            for (const {el, item, r} of targets) {
                el.classList.add('help-target');
                areasShown.add(item.area);
                const bubble = document.createElement('button');
                bubble.type = 'button';
                bubble.className = 'help-bubble help-area-' + item.area;
                bubble.textContent = textOf(item.label);
                bubble.title = 'Why use it?';
                layer.appendChild(bubble);
                const w = bubble.offsetWidth, h = bubble.offsetHeight, gap = 7;
                const cx = r.left + r.width / 2, cy = r.top + r.height / 2;
                const clampX = x => Math.max(4, Math.min(vw - w - 4, x));
                // Candidate spots, nearest first: above, below, right, left;
                // the first that is on screen and free wins. Otherwise the
                // spot above, nudged away from the labels already placed.
                const spots = [
                    {left: clampX(cx - w / 2), top: r.top - h - gap, side: 'points-down'},
                    {left: clampX(cx - w / 2), top: r.bottom + gap, side: 'points-up'},
                    {left: r.right + gap, top: cy - h / 2, side: 'points-left'},
                    {left: r.left - w - gap, top: cy - h / 2, side: 'points-right'},
                ];
                const fits = s => s.left >= 4 && s.left + w <= vw - 4 && s.top >= 4 && s.top + h <= vh - 4;
                const free = s => !placed.some(p => overlaps(p, {left: s.left, right: s.left + w, top: s.top, bottom: s.top + h}));
                let spot = item.inside
                    ? {left: clampX(cx - w / 2), top: cy - h / 2, side: 'inside'}
                    : spots.find(s => fits(s) && free(s));
                if (!spot) {
                    spot = Object.assign({}, fits(spots[0]) ? spots[0] : spots[1]);
                    const step = spot.side === 'points-down' ? -(h + 3) : h + 3;
                    for (let tries = 0; tries < 6 && !free(spot); tries++) spot.top += step;
                    spot.top = Math.max(4, Math.min(vh - h - 4, spot.top));
                }
                bubble.style.left = spot.left + 'px';
                bubble.style.top = spot.top + 'px';
                bubble.classList.add(spot.side);
                bubble.style.setProperty('--arrow-x', Math.round(cx - spot.left) + 'px');
                placed.push({left: spot.left, right: spot.left + w, top: spot.top, bottom: spot.top + h});
                helpPlacements.set(item, {el: el, bubble: bubble});
                bubble.addEventListener('click', function(e) {
                    e.stopPropagation();
                    showHelpCard(item, el, bubble);
                });
            }
            // Legend: the areas present on this page, in the area colours.
            const legend = document.createElement('div');
            legend.className = 'help-legend';
            legend.addEventListener('click', e => e.stopPropagation());
            const intro = document.createElement('div');
            intro.className = 'help-legend-intro';
            intro.textContent = 'Click a label to see why you would use it.';
            legend.appendChild(intro);
            for (const area of Object.keys(HELP_AREAS)) {
                if (!areasShown.has(area)) continue;
                const row = document.createElement('div');
                const swatch = document.createElement('span');
                swatch.className = 'help-swatch help-area-' + area;
                row.appendChild(swatch);
                row.appendChild(document.createTextNode(HELP_AREAS[area]));
                legend.appendChild(row);
            }
            layer.appendChild(legend);
            layer.addEventListener('click', hideHelp);
        }

        // The card a label opens: why to use the control, and "Show me".
        function showHelpCard(item, el, bubble) {
            openHelpItem = item;
            const old = document.getElementById('help-card');
            if (old) old.remove();
            const layer = document.getElementById('help-layer');
            if (!layer) return;
            const card = document.createElement('div');
            card.id = 'help-card';
            card.className = 'help-card help-area-border-' + item.area;
            card.addEventListener('click', e => e.stopPropagation());
            const title = document.createElement('div');
            title.className = 'help-card-title';
            title.textContent = textOf(item.label);
            const why = document.createElement('div');
            why.className = 'help-card-why';
            why.textContent = textOf(item.why);
            card.appendChild(title);
            card.appendChild(why);
            const how = textOf(item.how);
            if (how && how.length) {
                const list = document.createElement('ul');
                list.className = 'help-card-how';
                for (const line of how) {
                    const li = document.createElement('li');
                    li.textContent = line;
                    list.appendChild(li);
                }
                card.appendChild(list);
            }
            const buttons = document.createElement('div');
            buttons.className = 'help-card-buttons';
            if (item.show) {
                const show = document.createElement('button');
                show.type = 'button';
                show.className = 'help-card-show';
                show.textContent = 'Show me';
                show.addEventListener('click', function() {
                    hideHelp();
                    rememberHelpNote(textOf(item.label));
                    const done = item.show === 'click' ? (el.click(), true) : item.show(el);
                    if (!done) forgetHelpNote();
                    // An action that does not change the page (opening a pane,
                    // a form) leaves no note behind for a later page.
                    const from = window.location.href;
                    setTimeout(function() {
                        const waiting = document.getElementById('waiting');
                        if (window.location.href === from && (!waiting || waiting.hidden)) forgetHelpNote();
                    }, 3000);
                });
                buttons.appendChild(show);
            }
            const close = document.createElement('button');
            close.type = 'button';
            close.className = 'help-card-close';
            close.textContent = 'Close';
            close.addEventListener('click', () => { card.remove(); openHelpItem = null; });
            buttons.appendChild(close);
            card.appendChild(buttons);
            layer.appendChild(card);
            // Beside the label, kept on screen.
            const b = bubble.getBoundingClientRect();
            const vw = window.innerWidth, vh = window.innerHeight;
            const w = card.offsetWidth, h = card.offsetHeight;
            let left = Math.max(8, Math.min(vw - w - 8, b.left));
            let top = b.bottom + 6;
            if (top + h > vh - 8) top = Math.max(8, b.top - h - 6);
            card.style.left = left + 'px';
            card.style.top = top + 'px';
        }

        // After "Show me" changes the page, a note names what happened and
        // offers Undo (the browser's back). Kept in sessionStorage so it
        // survives a full page load as well as a fragment navigation.
        function rememberHelpNote(label) {
            try { sessionStorage.setItem('taxinomia-help-note', JSON.stringify({label: label, from: window.location.href})); } catch (e) {}
        }
        function forgetHelpNote() {
            try { sessionStorage.removeItem('taxinomia-help-note'); } catch (e) {}
        }
        function showHelpNoteIfAny() {
            let note = null;
            try { note = JSON.parse(sessionStorage.getItem('taxinomia-help-note') || 'null'); } catch (e) {}
            if (!note) return;
            if (note.from === window.location.href) return; // the page has not changed (yet)
            forgetHelpNote();
            const old = document.getElementById('help-note');
            if (old) old.remove();
            const box = document.createElement('div');
            box.id = 'help-note';
            box.className = 'help-note';
            box.appendChild(document.createTextNode('That was "' + note.label + '". '));
            const undo = document.createElement('button');
            undo.type = 'button';
            undo.textContent = 'Undo';
            undo.addEventListener('click', () => { box.remove(); history.back(); });
            const again = document.createElement('button');
            again.type = 'button';
            again.textContent = 'Labels';
            again.addEventListener('click', () => { box.remove(); showHelp(); });
            box.appendChild(undo);
            box.appendChild(again);
            document.body.appendChild(box);
            setTimeout(() => box.remove(), 8000);
        }

        function hideHelp() {
            openHelpItem = null;
            helpPlacements = new Map();
            const layer = document.getElementById('help-layer');
            if (layer) layer.remove();
            document.querySelectorAll('.help-target').forEach(el => el.classList.remove('help-target'));
        }

        function toggleHelp() {
            if (document.getElementById('help-layer')) hideHelp(); else showHelp();
        }

        document.addEventListener('keydown', function(e) {
            if (e.key === 'Escape' && document.getElementById('help-layer')) hideHelp();
        });
        window.addEventListener('scroll', hideHelp, {passive: true});
        // A resize moves the controls: place the labels again rather than close.
        window.addEventListener('resize', function() {
            if (!document.getElementById('help-layer')) return;
            const item = openHelpItem;
            showHelp();
            const p = item && helpPlacements.get(item);
            if (p) showHelpCard(item, p.el, p.bubble);
        });
