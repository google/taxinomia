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
            restoreSyntaxPanel();
            showJourneyStep();
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

        // The header cell a drag event concerns: the header itself, or the
        // header above a cell of the controls row (the UNGROUP chip says
        // "drag to reorder", so the controls row is a drag handle too).
        function dragTarget(e) {
            // A drag can be reported on a text node, which has no closest().
            const el = e.target && e.target.nodeType === 3 ? e.target.parentElement : e.target;
            if (!el || !el.closest) return null;
            const th = el.closest('thead tr:first-child th[data-col-name]');
            if (th) return th;
            const cell = el.closest('thead tr.grouping-row td.grouping-cell');
            if (!cell) return null;
            const idx = Array.prototype.indexOf.call(cell.parentElement.children, cell);
            const header = headerCells()[idx];
            return header && header.dataset.colName ? header : null;
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
            const onLeft = dropOnLeftSide(draggedHeader, th, e.clientX);
            e.preventDefault();
            e.dataTransfer.dropEffect = 'move';
            clearDragIndicators();
            th.classList.add(onLeft ? 'drag-over-left' : 'drag-over-right');
        });

        // Which side of target a drop lands on. Over a neighbour, anywhere
        // on it moves the dragged column past it (so a swap needs no aim;
        // its near half would otherwise put the column back where it is);
        // further away, the half under the pointer decides.
        function dropOnLeftSide(dragged, target, clientX) {
            const cells = Array.from(headerCells());
            const from = cells.indexOf(dragged), to = cells.indexOf(target);
            if (to === from + 1) return false; // right neighbour: drop after it
            if (to === from - 1) return true;  // left neighbour: drop before it
            const rect = target.getBoundingClientRect();
            return clientX < rect.left + rect.width / 2;
        }

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

            const dropOnLeft = dropOnLeftSide(draggedHeader, th, e.clientX);
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
            restoreSyntaxPanel();
            showJourneyStep();
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
            } else if (window.location.hash.startsWith("#syntax=")) {
                // ... and "#syntax=<filtering|expressions>:<column>" opens the syntax panel.
                const [kind, column] = decodeURIComponent(window.location.hash.substring(8)).split(":");
                const sel = (kind === "expressions" ? ".formula-input" : kind === "grouping" ? ".having-input" : ".filter-input") + '[data-column="' + CSS.escape(column || "") + '"]';
                const field = document.querySelector(sel);
                if (field) setTimeout(() => { field.focus(); openSyntaxPanel(field); }, 0);
            } else if (window.location.hash.startsWith("#journey=")) {
                // ... and "#journey=<name>" starts that guided journey.
                const name = decodeURIComponent(window.location.hash.substring(9));
                history.replaceState(history.state, "", window.location.pathname + window.location.search);
                setTimeout(() => startJourney(name), 0);
            } else if (window.location.hash === "#feedback") {
                // ... and "#feedback" opens the feedback form (when enabled).
                const fb = document.querySelector(".feedback-open[data-feedback-url]");
                if (fb) setTimeout(() => openFeedback(fb.dataset.feedbackUrl), 0);
            } else if (window.location.hash === "#import") {
                // ... and "#import" opens the import dialog (when enabled).
                const im = document.querySelector(".import-open[data-import-url]");
                if (im) setTimeout(() => openImport(im.dataset.importUrl), 0);
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
            group: 'Grouping and aggregates',
            filter: 'Filtering',
            columns: 'Columns and joins',
            computed: 'Computed columns',
        };

        // One label and card per aggregate type, on the first button of that
        // type on the page: [type (data-agg), label, what it is per group].
        const AGGREGATE_HELP = [
            ['count', '# count', 'How many rows each group has with a value in this column.'],
            ['unique', '◇ distinct', 'How many different values this column has within each group.'],
            ['sum', 'Σ sum', 'The sum of this column within each group.'],
            ['avg', 'μ average', 'The average of this column within each group (for dates, the average date).'],
            ['stddev', 'σ spread', 'How widely the values spread around the average within each group (the standard deviation).'],
            ['min', '↓ minimum', 'The smallest value in each group: the lowest number, the earliest date, or the first text alphabetically.'],
            ['max', '↑ maximum', 'The largest value in each group: the highest number, the latest date, or the last text alphabetically.'],
            ['true', '✓ true count', 'How many rows in each group are true (yes/no columns).'],
            ['false', '✗ false count', 'How many rows in each group are false (yes/no columns).'],
            ['ratio', '% true share', 'The share of rows in each group that are true (yes/no columns).'],
            ['span', 'Δ time span', 'The time from the earliest to the latest date in each group.'],
        ].map(([type, label, why]) => ({
            sel: '.agg-toggle-btn[data-agg="' + type + '"]', label: label, area: 'group', show: 'click', why: why,
            how: [
                'Click the button to switch this aggregate on or off for every group. It shows in each group\'s row and adds up in the group above.',
                'Once on, it can sort the groups: the ⟳ button of the grouped column.',
                'Which aggregates a column offers depends on its type: numbers, text, dates, yes/no.',
            ],
        }));

        // show: 'click' operates the control itself; a function does
        // something else; absent means the card only explains.
        const HELP_LABELS = [
            {sel: '.limit-btn', label: 'Fewer / more rows', area: 'view', show: 'click',
             why: 'The table lists a screenful of rows; these list fewer or more. The rest are still counted.'},
            {sel: '.type-toggle-btn', label: 'Column types', area: 'view', show: 'click',
             why: 'Shows how each column is stored (text, number, date), which decides how it sorts and which aggregates it offers.'},
            {sel: '.pagination-info a[href="?help=syntax"]', label: 'Syntax help', area: 'view', show: 'click', doc: '',
             why: 'Opens the help page: the filter syntax, grouping and aggregates, and the expressions of computed columns.'},
            {sel: 'thead tr:first-child th[draggable] .th-content', label: 'Drag: sort priority', area: 'sort', inside: true, show: moveFirstColumnRight,
             why: 'Rows are always sorted by the columns, left to right. Drag a header left to make it sort first.'},
            {sel: 'thead .resize-handle', label: 'Drag to resize', area: 'columns',
             why: 'Drag the edge of a header to make the column wider or narrower. Widths are kept in the link.'},
            {sel: '.sort-toggle-btn', label: 'Flip sort', area: 'sort', show: 'click',
             why: 'Flips this column between ascending and descending, without changing its priority.'},
            {sel: '.group-toggle-btn:not(.grouped)', label: () => isGroupedPage() ? 'Nest another level' : 'Group by', area: 'group', show: 'click',
             why: () => isGroupedPage()
                ? 'Groups each group again by this column, one level deeper.'
                : 'Collapses rows with the same value into one group, with its count and aggregates.',
             how: () => isGroupedPage() ? null : [
                'Once grouped, the other columns get aggregate buttons (Σ sum, μ average and more, by column type).',
                'The grouped column gets a ⟳ button that sorts its groups by one of those aggregates.',
             ]},
            {sel: '.group-toggle-btn.grouped', label: 'Ungroup', area: 'group', show: 'click',
             why: 'Turns this column back into a plain column.'},
            {sel: '.agg-sort-toggle-btn', label: 'Sort groups by aggregate', area: 'sort', show: 'click',
             why: 'Orders the groups by a number instead of by their value: the biggest (or smallest) groups first.',
             how: [
                'Each click steps to the next choice: rows per group, subgroups per group, then every aggregate switched on in the other columns (Σ, μ, ...).',
                'The number the groups are sorted by is shown in bold (an aggregate also in blue).',
                'The arrow next to ⟳ flips the order: biggest first or smallest first.',
                'Keep clicking until the groups are sorted by their value again.',
                'Only the aggregates switched on can be sorted by: switch one on first in its column.',
             ]},
            ...AGGREGATE_HELP,
            {sel: '.stats-cell', label: () => isGroupedPage() ? 'Groups / filtered / total' : 'Filtered / total rows', area: 'filter', inside: true,
             why: () => isGroupedPage()
                ? 'How many groups there are, how many rows the filters keep, and how many rows the table has.'
                : 'How many rows the filters keep, and how many rows the table has.'},
            {sel: '.filter-input', label: 'text, "exact", a|b', area: 'filter', show: typeExampleFilter,
             why: 'Type a word to find it anywhere in the value, "quoted" for an exact match, or a|b for any of several values. Enter applies it.'},
            {sel: '.hierarchy-path-name', label: 'Group by hierarchy', area: 'group', show: 'click', doc: 'grouping',
             why: 'Groups by every level of a hierarchy at once, root first, and shows their columns. Click a level to group only down to it; click the current grouping again to ungroup.'},
            {sel: '.having-input', label: 'Keep groups where', area: 'group', show: '', doc: 'grouping',
             why: 'Keeps only the groups whose aggregates satisfy a condition, e.g. count() > 10 or sum(amount) > 1000. The rows of the other groups leave the view.'},
            {sel: '.multiselect-toggle', label: 'Pick several values', area: 'filter', show: 'click',
             why: 'Tick several groups to keep only those, as one filter.'},
            {sel: 'tbody td.group-cell', label: 'Value [subgroups/rows]', area: 'group', inside: true,
             why: 'Each group shows its value and, in brackets, how many subgroups and rows it holds.'},
            {sel: 'tbody .cell-link', label: 'Link to another site', area: 'view', show: '',
             why: 'Opens this value on another site, in a new tab: a link the data source declared for this kind of value. Hover it for the full name.'},
            {sel: 'tbody .filter-link', label: 'Drill into group', area: 'filter', show: 'click',
             why: 'Keeps only this group\'s rows. While levels are grouped below it, its levels stay grouped and show once; on the last level they are ungrouped to show the rows.'},
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

        // Cards about filtering, grouping and sorting, and computed columns link
        // to the matching part of the embedded help page (?help=syntax).
        function helpDocFor(sel) {
            if (/filter|multiselect|stats-cell/.test(sel)) return 'filtering';
            if (/group|agg-|sort|th-content/.test(sel)) return 'grouping';
            if (/formula|computed/.test(sel)) return 'expressions';
            return '';
        }
        HELP_LABELS.forEach(function(item) {
            if (item.doc === undefined) item.doc = helpDocFor(item.sel);
        });

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
            // Legend first (the areas present on this page): labels avoid it.
            const areasShown = new Set(targets.map(t => t.item.area));
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
            const journeysList = journeyLegendItems();
            if (journeysList) legend.appendChild(journeysList);
            layer.appendChild(legend);
            const lr = legend.getBoundingClientRect();
            placed.push({left: lr.left, right: lr.right, top: lr.top, bottom: lr.bottom});
            // Labels inside their (large) target go first; the others avoid them.
            targets.sort((a, b) => (b.item.inside ? 1 : 0) - (a.item.inside ? 1 : 0));
            for (const {el, item, r} of targets) {
                el.classList.add('help-target');
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
                    // Every side is taken: search outward, above and below in
                    // turn, for the nearest free spot on screen.
                    for (let k = 1; k <= 8 && !spot; k++) {
                        const up = Object.assign({}, spots[0], {top: spots[0].top - k * (h + 3)});
                        const down = Object.assign({}, spots[1], {top: spots[1].top + k * (h + 3)});
                        spot = [up, down].find(s => fits(s) && free(s));
                    }
                    if (!spot) spot = fits(spots[0]) ? spots[0] : spots[1];
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
            if (item.doc) {
                const doc = document.createElement('a');
                doc.className = 'help-card-doc';
                doc.href = '?help=syntax#' + item.doc;
                doc.textContent = 'Syntax and examples';
                card.appendChild(doc);
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

        // --- Feedback: report a bug or ask for a feature ---------------------
        // The "Feedback" button (shown when the application set a feedback
        // address, Server.SetFeedbackURL) opens a small form; Send posts a
        // JSON report {kind, text, page, build} to that address. What
        // happens to it is the application's back end.
        function openFeedback(url) {
            closeFeedback();
            const overlay = document.createElement('div');
            overlay.id = 'feedback-overlay';
            overlay.className = 'feedback-overlay';
            const box = document.createElement('div');
            box.className = 'feedback-box';
            box.setAttribute('role', 'dialog');
            box.setAttribute('aria-label', 'Feedback');
            box.innerHTML =
                '<div class="feedback-title">Report a bug or ask for a feature</div>' +
                '<div class="feedback-kinds">' +
                '<label><input type="radio" name="feedback-kind" value="bug" checked> Bug</label>' +
                '<label><input type="radio" name="feedback-kind" value="feature"> Feature request</label>' +
                '</div>' +
                '<textarea class="feedback-text" rows="7" maxlength="10000" placeholder="What happened, or what would you like?"></textarea>' +
                '<label class="feedback-page"><input type="checkbox" checked> Include the address of this page</label>' +
                '<div class="feedback-status" aria-live="polite"></div>' +
                '<div class="feedback-buttons">' +
                '<button type="button" class="feedback-cancel">Cancel</button>' +
                '<button type="button" class="feedback-send">Send</button>' +
                '</div>';
            overlay.appendChild(box);
            document.body.appendChild(overlay);
            const text = box.querySelector('.feedback-text');
            const status = box.querySelector('.feedback-status');
            const send = box.querySelector('.feedback-send');
            text.focus();
            overlay.addEventListener('click', e => { if (e.target === overlay) closeFeedback(); });
            box.querySelector('.feedback-cancel').addEventListener('click', closeFeedback);
            send.addEventListener('click', function() {
                const body = {
                    kind: box.querySelector('input[name="feedback-kind"]:checked').value,
                    text: text.value.trim(),
                    page: box.querySelector('.feedback-page input').checked ? window.location.href : '',
                    build: (document.querySelector('.build-version') || {textContent: ''}).textContent.trim(),
                };
                if (!body.text) {
                    status.textContent = 'Please write something first.';
                    status.className = 'feedback-status error';
                    text.focus();
                    return;
                }
                send.disabled = true;
                status.textContent = 'Sending...';
                status.className = 'feedback-status';
                fetch(url, {
                    method: 'POST',
                    credentials: 'same-origin',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(body),
                }).then(function(resp) {
                    if (resp.ok) {
                        status.textContent = 'Thank you, your report was sent.';
                        status.className = 'feedback-status ok';
                        setTimeout(closeFeedback, 1500);
                        return;
                    }
                    return resp.json().catch(() => ({})).then(function(j) {
                        throw new Error(j.error || ('error ' + resp.status));
                    });
                }).catch(function(err) {
                    send.disabled = false;
                    status.textContent = 'Could not send: ' + err.message;
                    status.className = 'feedback-status error';
                });
            });
        }

        function closeFeedback() {
            const overlay = document.getElementById('feedback-overlay');
            if (overlay) overlay.remove();
        }

        document.addEventListener('click', function(e) {
            const button = e.target.closest && e.target.closest('.feedback-open');
            if (button && button.dataset.feedbackUrl) {
                e.preventDefault();
                openFeedback(button.dataset.feedbackUrl);
            }
        });
        document.addEventListener('keydown', function(e) {
            if (e.key === 'Escape' && document.getElementById('feedback-overlay')) closeFeedback();
        });

        // --- Import: paste a table or load a CSV / TSV file ------------------
        // The "Import" button (shown when the application set an import
        // address, Server.SetImportURL) opens a dialog: paste cells copied
        // from a spreadsheet (tab-separated) or CSV text, or choose or drop a
        // file. Load posts it as a form and opens the new table.
        function openImport(url) {
            closeImport();
            const overlay = document.createElement('div');
            overlay.id = 'import-overlay';
            overlay.className = 'feedback-overlay';
            const box = document.createElement('div');
            box.className = 'feedback-box import-box';
            box.setAttribute('role', 'dialog');
            box.setAttribute('aria-label', 'Import a table');
            box.innerHTML =
                '<div class="feedback-title">Import a table</div>' +
                '<div class="import-hint">Paste cells copied from a spreadsheet, or CSV or TSV text, ' +
                'or choose or drop a file. The first line holds the column names.</div>' +
                '<textarea class="feedback-text import-text" rows="9" placeholder="Paste here, or drop a file"></textarea>' +
                '<div class="import-row">' +
                '<label>Or a file <input type="file" class="import-file" accept=".csv,.tsv,.tab,.txt,text/csv,text/tab-separated-values,text/plain"></label>' +
                '<label>Table name <input type="text" class="import-name" placeholder="optional"></label>' +
                '</div>' +
                '<div class="feedback-status" aria-live="polite"></div>' +
                '<div class="feedback-buttons">' +
                '<button type="button" class="feedback-cancel">Cancel</button>' +
                '<button type="button" class="feedback-send">Load</button>' +
                '</div>';
            overlay.appendChild(box);
            document.body.appendChild(overlay);
            const text = box.querySelector('.import-text');
            const fileInput = box.querySelector('.import-file');
            const nameInput = box.querySelector('.import-name');
            const status = box.querySelector('.feedback-status');
            const load = box.querySelector('.feedback-send');
            let droppedFile = null;
            text.focus();
            overlay.addEventListener('click', e => { if (e.target === overlay) closeImport(); });
            box.querySelector('.feedback-cancel').addEventListener('click', closeImport);
            // A pasted range keeps its tabs: Tab in the box types a tab
            // instead of leaving it.
            text.addEventListener('keydown', function(e) {
                if (e.key === 'Tab' && !e.shiftKey) {
                    e.preventDefault();
                    const s = text.selectionStart;
                    text.setRangeText('\t', s, text.selectionEnd, 'end');
                }
            });
            fileInput.addEventListener('change', function() {
                droppedFile = null;
                if (fileInput.files[0]) status.textContent = 'File: ' + fileInput.files[0].name;
            });
            box.addEventListener('dragover', function(e) {
                e.preventDefault();
                box.classList.add('dragging-file');
            });
            box.addEventListener('dragleave', function() { box.classList.remove('dragging-file'); });
            box.addEventListener('drop', function(e) {
                e.preventDefault();
                box.classList.remove('dragging-file');
                const f = e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files[0];
                if (f) {
                    droppedFile = f;
                    fileInput.value = '';
                    status.textContent = 'File: ' + f.name;
                    status.className = 'feedback-status';
                }
            });
            load.addEventListener('click', function() {
                const form = new FormData();
                const file = droppedFile || fileInput.files[0];
                if (file) form.append('file', file, file.name);
                else if (text.value.trim()) form.append('text', text.value);
                else {
                    status.textContent = 'Paste a table or choose a file first.';
                    status.className = 'feedback-status error';
                    text.focus();
                    return;
                }
                if (nameInput.value.trim()) form.append('name', nameInput.value.trim());
                load.disabled = true;
                status.textContent = 'Loading...';
                status.className = 'feedback-status';
                fetch(url, {
                    method: 'POST',
                    credentials: 'same-origin',
                    headers: {'Accept': 'application/json'},
                    body: form,
                }).then(function(resp) {
                    return resp.json().catch(() => ({})).then(function(j) {
                        if (!resp.ok) throw new Error(j.error || ('error ' + resp.status));
                        return j;
                    });
                }).then(function(j) {
                    // The answer is relative to the product: /<product>/table?...
                    const base = window.location.pathname.replace(/[^/]*$/, '');
                    window.location.href = base + j.url;
                }).catch(function(err) {
                    load.disabled = false;
                    status.textContent = 'Could not import: ' + err.message;
                    status.className = 'feedback-status error';
                });
            });
        }

        function closeImport() {
            const overlay = document.getElementById('import-overlay');
            if (overlay) overlay.remove();
        }

        document.addEventListener('click', function(e) {
            const button = e.target.closest && e.target.closest('.import-open');
            if (button && button.dataset.importUrl) {
                e.preventDefault();
                openImport(button.dataset.importUrl);
            }
        });
        document.addEventListener('keydown', function(e) {
            if (e.key === 'Escape' && document.getElementById('import-overlay')) closeImport();
        });

        // --- Syntax panel: the detailed syntax while typing -------------------
        // Focusing a filter box or a computed column's formula shows a small
        // "Syntax" button beside it. The button opens a wide panel under that
        // row with the matching part of the help page (fetched once from
        // ?help=syntax, so the text has one source): for a filter, a line
        // saying what the filter will do as you type; for a formula, the
        // table's columns as chips that insert their name. Typing goes on in
        // the field. The panel stays open across the page changes a filter
        // makes, until closed with its ×.
        const syntaxCache = {};
        let syntaxPanelInput = null; // the field the open panel belongs to
        const SYNTAX_KEY = 'taxinomia-syntax-panel';

        function syntaxKind(input) {
            if (input.matches('.filter-input')) return 'filtering';
            if (input.matches('.formula-input')) return 'expressions';
            if (input.matches('.having-input')) return 'grouping';
            return '';
        }

        function showSyntaxButton(input) {
            if (!syntaxKind(input) || input.parentElement.querySelector('.syntax-btn')) return;
            const btn = document.createElement('button');
            btn.type = 'button';
            btn.className = 'syntax-btn';
            btn.textContent = 'Syntax';
            btn.title = 'Show the syntax under this row';
            // Keep the focus in the field: leaving a filter box applies it.
            btn.addEventListener('mousedown', e => e.preventDefault());
            btn.addEventListener('click', () => openSyntaxPanel(input));
            input.insertAdjacentElement('afterend', btn);
        }

        function hideSyntaxButton(input) {
            const btn = input.parentElement && input.parentElement.querySelector('.syntax-btn');
            if (btn) btn.remove();
        }

        // What a filter box value will do (the rules of ApplyFilters).
        function describeFilter(v) {
            v = v.trim();
            if (!v) return 'Type a word, "an exact value", or several|values.';
            if (v.indexOf('|') !== -1) {
                const values = v.split('|').map(s => s.trim()).filter(Boolean);
                return 'Keeps rows whose value is exactly one of: ' + values.join(', ') + '.';
            }
            if (v.length >= 2 && v[0] === '"' && v[v.length - 1] === '"') {
                return 'Keeps rows whose value is exactly ' + v.slice(1, -1) + ' (upper and lower case count).';
            }
            if (v.toLowerCase() === '[error]') return 'Keeps the rows whose value could not be computed.';
            if (v.toLowerCase() === '[unmatched]') return 'Keeps the rows without a match in the joined table.';
            return 'Keeps rows whose value contains "' + v + '", in any case.';
        }

        function loadSyntaxSection(kind) {
            if (syntaxCache[kind]) return Promise.resolve(syntaxCache[kind]);
            const url = currentUrl();
            url.search = '?help=syntax';
            url.hash = '';
            return fetch(url.toString(), {credentials: 'same-origin'})
                .then(r => r.text())
                .then(function(html) {
                    const doc = new DOMParser().parseFromString(html, 'text/html');
                    const section = doc.getElementById(kind);
                    syntaxCache[kind] = section ? section.innerHTML : '';
                    return syntaxCache[kind];
                });
        }

        function closeSyntaxPanel(forget) {
            document.querySelectorAll('tr.syntax-panel-row').forEach(r => r.remove());
            syntaxPanelInput = null;
            if (forget) {
                try { sessionStorage.removeItem(SYNTAX_KEY); } catch (e) {}
            }
        }

        function openSyntaxPanel(input) {
            const kind = syntaxKind(input);
            if (!kind) return;
            closeSyntaxPanel(false);
            syntaxPanelInput = input;
            try { sessionStorage.setItem(SYNTAX_KEY, JSON.stringify({kind: kind, column: input.dataset.column, table: currentUrl().searchParams.get('table')})); } catch (e) {}
            const row = input.closest('tr');
            const cols = row.children.length;
            const tr = document.createElement('tr');
            tr.className = 'syntax-panel-row';
            const td = document.createElement('td');
            td.colSpan = cols;
            td.className = 'syntax-panel-cell';
            const panel = document.createElement('div');
            panel.className = 'syntax-panel';
            const head = document.createElement('div');
            head.className = 'syntax-panel-head';
            const title = document.createElement('strong');
            title.textContent = kind === 'filtering' ? 'Filter syntax' : kind === 'grouping' ? 'Group conditions' : 'Expression syntax';
            head.appendChild(title);
            const full = document.createElement('a');
            full.href = '?help=syntax#' + kind;
            full.textContent = 'Open the full help page';
            head.appendChild(full);
            const close = document.createElement('button');
            close.type = 'button';
            close.className = 'syntax-panel-close';
            close.title = 'Close';
            close.textContent = '×';
            close.addEventListener('mousedown', e => e.preventDefault());
            close.addEventListener('click', () => closeSyntaxPanel(true));
            head.appendChild(close);
            panel.appendChild(head);

            if (kind === 'filtering' || kind === 'grouping') {
                const readout = document.createElement('div');
                readout.className = 'syntax-readout';
                const describe = kind === 'grouping' ? describeGroupCondition : describeFilter;
                const update = () => { readout.textContent = describe(input.value); };
                update();
                input.addEventListener('input', update);
                panel.appendChild(readout);
            } else {
                const table = document.getElementById('data-table');
                const names = ((table && table.dataset.columns) || '').split(',')
                    .filter(n => /^[A-Za-z_][A-Za-z0-9_]*$/.test(n));
                if (names.length) {
                    const chips = document.createElement('div');
                    chips.className = 'syntax-chips';
                    const label = document.createElement('span');
                    label.textContent = 'Columns: ';
                    chips.appendChild(label);
                    for (const name of names) {
                        const chip = document.createElement('button');
                        chip.type = 'button';
                        chip.className = 'syntax-chip';
                        chip.textContent = name;
                        chip.title = 'Insert ' + name + ' at the cursor';
                        chip.addEventListener('mousedown', e => e.preventDefault());
                        chip.addEventListener('click', function() {
                            const start = input.selectionStart != null ? input.selectionStart : input.value.length;
                            const end = input.selectionEnd != null ? input.selectionEnd : start;
                            input.value = input.value.slice(0, start) + name + input.value.slice(end);
                            input.focus();
                            input.setSelectionRange(start + name.length, start + name.length);
                            input.dispatchEvent(new Event('input', {bubbles: true}));
                        });
                        chips.appendChild(chip);
                    }
                    panel.appendChild(chips);
                }
            }

            const body = document.createElement('div');
            body.className = 'syntax-panel-body help-section';
            body.textContent = 'Loading...';
            panel.appendChild(body);
            td.appendChild(panel);
            tr.appendChild(td);
            row.insertAdjacentElement('afterend', tr);
            loadSyntaxSection(kind).then(function(html) {
                body.innerHTML = html || 'The help page could not be loaded.';
            }).catch(function() {
                body.textContent = 'The help page could not be loaded.';
            });
            input.focus();
        }

        // Reopen the panel after a page change (a filter applied while it
        // was open), on the same column's field.
        function restoreSyntaxPanel() {
            let saved = null;
            try { saved = JSON.parse(sessionStorage.getItem(SYNTAX_KEY) || 'null'); } catch (e) {}
            if (!saved) return;
            if (saved.table !== currentUrl().searchParams.get('table')) { closeSyntaxPanel(true); return; }
            const sel = (saved.kind === 'filtering' ? '.filter-input' : saved.kind === 'grouping' ? '.having-input' : '.formula-input') + '[data-column="' + CSS.escape(saved.column || '') + '"]';
            const input = document.querySelector(sel);
            if (input) {
                showSyntaxButton(input);
                openSyntaxPanel(input);
            } else {
                closeSyntaxPanel(true);
            }
        }

        // The panel belongs to the field being typed in. Moving to another
        // filter box or formula moves it there; leaving otherwise (a click
        // outside the panel and the field, Tab to another control, Esc)
        // closes it. Its button shows only while its field has the focus.
        function syntaxPanelOpen() {
            return !!document.querySelector('tr.syntax-panel-row');
        }

        function insideSyntaxPanel(el) {
            return !!(el && el.closest && el.closest('tr.syntax-panel-row'));
        }

        document.addEventListener('focusin', function(e) {
            const t = e.target;
            if (t && t.matches && syntaxKind(t)) {
                showSyntaxButton(t);
                if (syntaxPanelOpen() && syntaxPanelInput !== t) openSyntaxPanel(t);
                return;
            }
            if (syntaxPanelOpen() && !insideSyntaxPanel(t)) closeSyntaxPanel(true);
        });
        document.addEventListener('focusout', function(e) {
            const t = e.target;
            if (!t || !t.matches || !syntaxKind(t)) return;
            setTimeout(() => { if (document.activeElement !== t) hideSyntaxButton(t); }, 150);
        });
        document.addEventListener('mousedown', function(e) {
            if (!syntaxPanelOpen()) return;
            const t = e.target;
            if (insideSyntaxPanel(t)) return;
            if (t && t.closest && t.closest('.syntax-btn')) return;
            if (t && t.matches && syntaxKind(t)) return; // another field: focusin moves the panel
            closeSyntaxPanel(true);
        });
        // Esc closes the panel first (capture phase, before a filter box's
        // own Esc, which clears the filter); a second Esc does that.
        document.addEventListener('keydown', function(e) {
            if (e.key === 'Escape' && syntaxPanelOpen()) {
                e.preventDefault();
                e.stopImmediatePropagation();
                closeSyntaxPanel(true);
            }
        }, true);
        document.addEventListener('keydown', function(e) {
            const t = e.target;
            if (e.key === 'F1' && t && t.matches && syntaxKind(t)) {
                e.preventDefault();
                openSyntaxPanel(t);
            }
        });

        // --- Journeys: guided walks written by the product's authors --------
        // The page carries its product's journeys (data-journeys on <body>,
        // checked by the server). A journey is a list of steps, each a view
        // (a table page query) and the control to look at, with a caption.
        // Starting one from the help legend (or "#journey=<name>") opens the
        // first view; a box shows the step with Back / Next / Exit and points
        // at its control. The current step survives page changes (session).
        const JOURNEY_KEY = 'taxinomia-journey';

        function pageJourneys() {
            try { return JSON.parse(document.body.dataset.journeys || '[]'); } catch (e) { return []; }
        }

        function journeyState() {
            try { return JSON.parse(sessionStorage.getItem(JOURNEY_KEY) || 'null'); } catch (e) { return null; }
        }

        function setJourneyState(state) {
            try {
                if (state) sessionStorage.setItem(JOURNEY_KEY, JSON.stringify(state));
                else sessionStorage.removeItem(JOURNEY_KEY);
            } catch (e) {}
        }

        function journeyStepURL(step) {
            return window.location.pathname + '?' + step.link;
        }

        function startJourney(name) {
            const j = pageJourneys().find(x => x.name === name);
            if (!j) return;
            hideHelp();
            goToJourneyStep(j, 0);
        }

        function goToJourneyStep(j, index) {
            setJourneyState({name: j.name, step: index});
            const url = new URL(journeyStepURL(j.steps[index]), window.location);
            if (url.search === window.location.search) {
                showJourneyStep(); // already on that view
            } else {
                navigate(url);
            }
        }

        function endJourney() {
            setJourneyState(null);
            clearJourneyUI();
        }

        function clearJourneyUI() {
            ['journey-box', 'journey-pointer'].forEach(id => {
                const el = document.getElementById(id);
                if (el) el.remove();
            });
            document.querySelectorAll('.journey-target').forEach(el => el.classList.remove('journey-target'));
        }

        // The control a step points at, by name (see JourneyStep.target).
        function journeyTarget(target) {
            if (!target) return null;
            const [kind, column, type] = target.split(':');
            if (kind === 'pane') return document.getElementById('sidebar');
            if (kind === 'help') return document.getElementById('help-toggle-pane');
            const headers = Array.from(headerCells());
            const idx = headers.findIndex(th => th.dataset.colName === column);
            if (idx === -1) return null;
            if (kind === 'column') return headers[idx];
            if (kind === 'filter') return document.querySelector('.filter-input[data-column="' + CSS.escape(column) + '"]');
            const cell = document.querySelectorAll('thead tr.grouping-row td.grouping-cell')[idx];
            if (!cell) return null;
            if (kind === 'group') return cell.querySelector('.group-toggle-btn');
            if (kind === 'sort') return cell.querySelector('.sort-toggle-btn');
            if (kind === 'groupsort') return cell.querySelector('.agg-sort-toggle-btn');
            if (kind === 'aggregate') return cell.querySelector('.agg-toggle-btn[data-agg="' + CSS.escape(type || '') + '"]');
            return null;
        }

        function showJourneyStep() {
            clearJourneyUI();
            const state = journeyState();
            if (!state) return;
            const j = pageJourneys().find(x => x.name === state.name);
            if (!j || !j.steps[state.step]) { endJourney(); return; }
            const step = j.steps[state.step];
            const last = state.step === j.steps.length - 1;

            const box = document.createElement('div');
            box.id = 'journey-box';
            box.className = 'journey-box';
            box.setAttribute('role', 'dialog');
            box.setAttribute('aria-label', j.title);
            const title = document.createElement('div');
            title.className = 'journey-title';
            title.textContent = j.title;
            const count = document.createElement('div');
            count.className = 'journey-count';
            count.textContent = 'Step ' + (state.step + 1) + ' of ' + j.steps.length;
            const caption = document.createElement('div');
            caption.className = 'journey-caption';
            caption.textContent = step.caption;
            const buttons = document.createElement('div');
            buttons.className = 'journey-buttons';
            const exit = document.createElement('button');
            exit.type = 'button';
            exit.textContent = last ? 'Done' : 'Exit';
            exit.addEventListener('click', endJourney);
            buttons.appendChild(exit);
            if (state.step > 0) {
                const back = document.createElement('button');
                back.type = 'button';
                back.textContent = 'Back';
                back.addEventListener('click', () => goToJourneyStep(j, state.step - 1));
                buttons.appendChild(back);
            }
            if (!last) {
                const next = document.createElement('button');
                next.type = 'button';
                next.className = 'journey-next';
                next.textContent = 'Next';
                next.addEventListener('click', () => goToJourneyStep(j, state.step + 1));
                buttons.appendChild(next);
            }
            box.append(title, count, caption, buttons);
            document.body.appendChild(box);

            const target = journeyTarget(step.target);
            if (target) {
                target.classList.add('journey-target');
                if (target.scrollIntoView) target.scrollIntoView({block: 'nearest', inline: 'nearest'});
                const r = target.getBoundingClientRect();
                const pointer = document.createElement('div');
                pointer.id = 'journey-pointer';
                pointer.className = 'journey-pointer';
                pointer.textContent = 'Here';
                document.body.appendChild(pointer);
                // In page coordinates (position: absolute), so it scrolls with
                // the control.
                const below = r.bottom + 40 < window.innerHeight;
                pointer.classList.add(below ? 'below' : 'above');
                pointer.style.left = Math.max(4, r.left + window.scrollX + r.width / 2 - pointer.offsetWidth / 2) + 'px';
                pointer.style.top = (below ? r.bottom + window.scrollY + 6 : r.top + window.scrollY - pointer.offsetHeight - 6) + 'px';
            }
        }

        // The journeys offered here, for the help legend.
        function journeyLegendItems() {
            const list = pageJourneys();
            if (!list.length) return null;
            const wrap = document.createElement('div');
            wrap.className = 'help-legend-journeys';
            const head = document.createElement('div');
            head.className = 'help-legend-intro';
            head.textContent = 'Guided journeys:';
            wrap.appendChild(head);
            for (const j of list) {
                const b = document.createElement('button');
                b.type = 'button';
                b.className = 'help-journey';
                b.textContent = j.title;
                if (j.description) b.title = j.description;
                b.addEventListener('click', e => { e.stopPropagation(); startJourney(j.name); });
                wrap.appendChild(b);
            }
            return wrap;
        }

        window.addEventListener('resize', function() {
            if (document.getElementById('journey-box')) showJourneyStep();
        });

        // --- Group conditions: "keep groups where ..." on a grouped column ---
        // Enter applies (URL having:<column>=<condition>), Esc clears,
        // leaving the field applies it when it changed. The syntax panel
        // (Syntax button, F1) shows the grouping help for it.
        function applyGroupCondition(column, value) {
            const url = currentUrl();
            const key = 'having:' + column;
            if (value && value.trim() !== '') url.searchParams.set(key, value.trim());
            else url.searchParams.delete(key);
            navigate(url);
        }

        document.addEventListener('keydown', function(e) {
            const t = e.target;
            if (!t.matches || !t.matches('.having-input')) return;
            if (e.key === 'Enter') {
                e.preventDefault();
                applyGroupCondition(t.dataset.column, t.value);
            } else if (e.key === 'Escape') {
                e.preventDefault();
                t.value = '';
                applyGroupCondition(t.dataset.column, '');
            }
        });
        document.addEventListener('focusin', function(e) {
            const t = e.target;
            if (t.matches && t.matches('.having-input')) t.dataset.originalValue = t.value;
        });
        document.addEventListener('focusout', function(e) {
            const t = e.target;
            if (t.matches && t.matches('.having-input') && t.value !== t.dataset.originalValue) {
                applyGroupCondition(t.dataset.column, t.value);
            }
        });

        // What a group condition will do, for the syntax panel's readout.
        function describeGroupCondition(v) {
            v = v.trim();
            if (!v) return 'Type a condition on the groups, e.g. count() > 10 or sum(amount) > 1000.';
            return 'Keeps the groups where ' + v + '; the rows of the other groups leave the view.';
        }
