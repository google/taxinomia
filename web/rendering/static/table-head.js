        // Toggle sidebar visibility
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
            const url = new URL(window.location);
            url.searchParams.delete('info');
            if (tabName === 'perf') {
                url.searchParams.set('infotab', 'perf');
            } else {
                url.searchParams.delete('infotab');
            }
            window.location.href = url.toString();
        }

        // Collapse the info pane to the status bar (updates URL)
        function hideInfoPane() {
            const url = new URL(window.location);
            url.searchParams.set('info', '0');
            window.location.href = url.toString();
        }

        // Toggle column types row visibility (updates URL)
        function toggleColumnTypes() {
            const url = new URL(window.location);
            const isCurrentlyVisible = url.searchParams.get('types') === '1';

            if (isCurrentlyVisible) {
                url.searchParams.delete('types');
            } else {
                url.searchParams.set('types', '1');
            }

            window.location.href = url.toString();
        }

        // Switch info pane tab (updates URL)
        function switchInfoTab(tabName) {
            showInfoPane(tabName);
        }

        // Parse URL parameters
        function parseUrlParams() {
            const urlParams = new URLSearchParams(window.location.search);
            const paramList = document.getElementById('param-list');

            // Clear existing items
            paramList.innerHTML = '';

            // Add each parameter. URLSearchParams already yields decoded
            // values; decoding again would throw on a literal '%' (e.g. a
            // filter for "100%") and abort the rest of page initialization.
            for (const [key, value] of urlParams) {
                const item = document.createElement('li');
                item.className = 'param-item';

                const nameSpan = document.createElement('span');
                nameSpan.className = 'param-name';
                nameSpan.textContent = key + ':';

                const valueSpan = document.createElement('span');
                valueSpan.className = 'param-value';

                // Special formatting for columns parameter
                if (key === 'columns') {
                    const columns = value.split(',');
                    const formattedColumns = columns.map(col => {
                        // Check if it's a joined column (format: fromColumn.toTable.toColumn.selectedColumn)
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

            // Update URL display (readable form; fall back to the raw href
            // if it contains a sequence that does not decode)
            const urlDisplay = document.getElementById('url-display');
            let readableHref = window.location.href;
            try {
                readableHref = decodeURIComponent(readableHref);
            } catch (e) {
                // keep the raw href
            }
            urlDisplay.textContent = readableHref;
        }

        // Handle filter input events
        function setupFilterInputs() {
            const filterInputs = document.querySelectorAll('.filter-input');
            filterInputs.forEach(input => {
                // Store original value for escape key
                input.dataset.originalValue = input.value;

                // Handle Enter key - apply filter immediately
                input.addEventListener('keydown', function(e) {
                    if (e.key === 'Enter') {
                        e.preventDefault();
                        applyFilter(this.dataset.column, this.value);
                    }
                    // Handle Escape key - clear filter and revert to original
                    else if (e.key === 'Escape') {
                        e.preventDefault();
                        this.value = '';
                        applyFilter(this.dataset.column, '');
                    }
                });

                // Handle blur (clicking away/tab out) - apply filter if changed
                input.addEventListener('blur', function() {
                    if (this.value !== this.dataset.originalValue) {
                        applyFilter(this.dataset.column, this.value);
                    }
                });

                // Handle focus - update original value when input is focused
                input.addEventListener('focus', function() {
                    this.dataset.originalValue = this.value;
                });
            });
        }

        // Handle filter clear button clicks
        function setupFilterClearButtons() {
            const clearButtons = document.querySelectorAll('.filter-clear');
            clearButtons.forEach(button => {
                button.addEventListener('click', function() {
                    const columnName = this.dataset.column;
                    // Clear the corresponding input
                    const input = document.querySelector(`.filter-input[data-column="${columnName}"]`);
                    if (input) {
                        input.value = '';
                    }
                    applyFilter(columnName, '');
                });
            });
        }

        // Multi-select filter mode handling
        function setupMultiselectMode() {
            const toggleButtons = document.querySelectorAll('.multiselect-toggle');
            const table = document.getElementById('data-table');
            if (!table) return;

            toggleButtons.forEach(button => {
                button.addEventListener('click', function() {
                    const columnName = this.dataset.column;
                    const isActive = this.classList.contains('active');

                    if (isActive) {
                        // Exiting multi-select mode - apply filter with selected values
                        const checkboxes = table.querySelectorAll(`.multiselect-checkbox[data-column="${columnName}"]:checked`);
                        const values = Array.from(checkboxes).map(cb => cb.dataset.value);

                        // Remove multiselect-active class from cells
                        table.querySelectorAll(`td[data-column="${columnName}"]`).forEach(td => {
                            td.classList.remove('multiselect-active');
                        });
                        this.classList.remove('active');

                        // Apply multi-value filter if any values selected
                        if (values.length > 0) {
                            applyFilter(columnName, values.join('|'));
                        }
                    } else {
                        // Entering multi-select mode
                        this.classList.add('active');
                        // Add multiselect-active class to cells of this column
                        table.querySelectorAll(`td[data-column="${columnName}"]`).forEach(td => {
                            td.classList.add('multiselect-active');
                        });
                        // Uncheck all checkboxes for this column
                        table.querySelectorAll(`.multiselect-checkbox[data-column="${columnName}"]`).forEach(cb => {
                            cb.checked = false;
                        });
                    }
                });
            });
        }

        // Apply filter by updating URL
        function applyFilter(columnName, filterValue) {
            const url = new URL(window.location);
            const paramKey = 'filter:' + columnName;

            if (filterValue && filterValue.trim() !== '') {
                // Add or update filter parameter
                url.searchParams.set(paramKey, filterValue.trim());
            } else {
                // Remove filter parameter if empty
                url.searchParams.delete(paramKey);
            }

            // Navigate to new URL
            window.location.href = url.toString();
        }

        // Restore scroll position from URL parameter
        function restoreScrollPosition() {
            const url = new URL(window.location);
            const scrollY = parseInt(url.searchParams.get('scrollY'), 10);
            if (scrollY > 0) {
                window.scrollTo(0, scrollY);
                // Remove scrollY from URL to keep it clean
                url.searchParams.delete('scrollY');
                history.replaceState(null, '', url.toString());
            }
        }

        // Calculate and display browser timing metrics
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

            // Phases of this navigation, in order. Each is a delta between two
            // Navigation Timing marks (all relative to navigation start).
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
            // The page's stylesheet and script: cached or fetched.
            performance.getEntriesByType('resource').forEach(function(r) {
                const file = r.name.split('/').pop().split('?')[0];
                if (file !== 'table.css' && file !== 'table.js') return;
                const fromCache = r.transferSize === 0;
                addRow(file + (fromCache ? ' (from cache)' : ' (fetched)'), ms(r.duration),
                    fromCache ? kb(r.decodedBodySize) : kb(r.transferSize) + ' on the wire → ' + kb(r.decodedBodySize), true);
            });
        }

        // Initialize on page load
        window.addEventListener('DOMContentLoaded', function() {
            parseUrlParams();
            setupFilterInputs();
            setupFilterClearButtons();
            setupMultiselectMode();
            initColumnResize();
            initColumnDragDrop();
            restoreScrollPosition();
            updateHiddenRowsCount();
        });

        // Calculate and display hidden rows count
        function updateHiddenRowsCount() {
            const table = document.getElementById('data-table');
            const hiddenRowsSpan = document.getElementById('hidden-rows-count');
            if (!table || !hiddenRowsSpan) return;

            const totalRows = parseInt(table.dataset.totalRows, 10) || 0;
            const displayedRows = parseInt(table.dataset.displayedRows, 10) || 0;
            const hiddenRows = totalRows - displayedRows;

            if (hiddenRows > 0) {
                hiddenRowsSpan.textContent = hiddenRows.toLocaleString();
            }
        }

        // Display browser timing after page is fully loaded
        window.addEventListener('load', function() {
            // Use setTimeout to ensure loadEventEnd is populated
            setTimeout(displayBrowserTiming, 0);
        });

        // =====================
        // Column Resize Feature
        // =====================

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
            const url = new URL(window.location);
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

                // Add new width if we have one
                if (widths[colName]) {
                    return colName + ':' + widths[colName];
                }
                return colName;
            });

            url.searchParams.set('columns', newColumns.join(','));

            if (reload) {
                // Save scroll position before reload
                url.searchParams.set('scrollY', Math.round(window.scrollY).toString());
                window.location.href = url.toString();
            } else {
                history.replaceState(null, '', url.toString());
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

        // Initialize column resize functionality
        function initColumnResize() {
            const table = document.getElementById('data-table');
            if (!table) return;

            // Apply widths from backend (stored in data attributes)
            applyColumnWidths();

            const headers = table.querySelectorAll('thead tr:first-child th');

            headers.forEach(function(th) {
                const handle = th.querySelector('.resize-handle');
                if (!handle) return;

                let startX, startWidth;

                handle.addEventListener('mousedown', function(e) {
                    e.preventDefault();
                    startX = e.pageX;
                    startWidth = th.offsetWidth;

                    // Add resizing class for visual feedback
                    handle.classList.add('resizing');
                    document.body.classList.add('resizing');

                    // Add mousemove and mouseup listeners to document
                    document.addEventListener('mousemove', onMouseMove);
                    document.addEventListener('mouseup', onMouseUp);
                });

                function onMouseMove(e) {
                    const diff = e.pageX - startX;
                    const newWidth = Math.max(50, startWidth + diff); // Minimum 50px width
                    th.style.width = newWidth + 'px';
                }

                function onMouseUp(e) {
                    // Remove event listeners
                    document.removeEventListener('mousemove', onMouseMove);
                    document.removeEventListener('mouseup', onMouseUp);

                    // Remove resizing classes
                    handle.classList.remove('resizing');
                    document.body.classList.remove('resizing');

                    // Update URL with new widths and reload
                    updateUrlWithWidths(true);
                }
            });
        }

        // ===========================
        // Column Drag-and-Drop Feature
        // ===========================

        // Initialize column drag-and-drop functionality
        function initColumnDragDrop() {
            const table = document.getElementById('data-table');
            if (!table) return;

            const headers = table.querySelectorAll('thead tr:first-child th');
            let draggedHeader = null;

            // The server orders columns by zone (filtered, grouped, others);
            // a drag only means something within one zone. Grouped columns
            // reorder the grouping hierarchy; the other zones reorder the
            // columns parameter.
            function columnZone(th) {
                if (th.dataset.isGrouped) return 'grouped';
                if (th.dataset.isFiltered) return 'filtered';
                return 'other';
            }

            headers.forEach(function(th) {
                // Make headers draggable
                th.setAttribute('draggable', 'true');

                th.addEventListener('dragstart', function(e) {
                    draggedHeader = th;
                    th.classList.add('dragging');
                    document.body.classList.add('column-dragging');

                    // Set drag data
                    e.dataTransfer.effectAllowed = 'move';
                    e.dataTransfer.setData('text/plain', th.dataset.colName);

                    // Use setTimeout to allow the drag image to be captured before adding opacity
                    setTimeout(function() {
                        th.classList.add('dragging');
                    }, 0);
                });

                th.addEventListener('dragend', function(e) {
                    th.classList.remove('dragging');
                    document.body.classList.remove('column-dragging');
                    draggedHeader = null;

                    // Remove all drag-over classes
                    headers.forEach(function(header) {
                        header.classList.remove('drag-over-left', 'drag-over-right');
                    });
                });

                th.addEventListener('dragover', function(e) {
                    if (!draggedHeader || draggedHeader === th) return;
                    // Cross-zone drops are no-ops (the server snaps zones
                    // back); without preventDefault the browser shows the
                    // no-drop cursor and no indicators appear.
                    if (columnZone(draggedHeader) !== columnZone(th)) return;
                    e.preventDefault();

                    e.dataTransfer.dropEffect = 'move';

                    // Determine if we're on the left or right half of the header
                    const rect = th.getBoundingClientRect();
                    const midpoint = rect.left + rect.width / 2;
                    const isLeftHalf = e.clientX < midpoint;

                    // Remove existing drag-over classes from all headers
                    headers.forEach(function(header) {
                        header.classList.remove('drag-over-left', 'drag-over-right');
                    });

                    // Add appropriate class
                    if (isLeftHalf) {
                        th.classList.add('drag-over-left');
                    } else {
                        th.classList.add('drag-over-right');
                    }
                });

                th.addEventListener('dragleave', function(e) {
                    th.classList.remove('drag-over-left', 'drag-over-right');
                });

                th.addEventListener('drop', function(e) {
                    e.preventDefault();
                    if (!draggedHeader || draggedHeader === th) return;
                    if (columnZone(draggedHeader) !== columnZone(th)) return;

                    // Determine drop position (left or right of target)
                    const rect = th.getBoundingClientRect();
                    const midpoint = rect.left + rect.width / 2;
                    const dropOnLeft = e.clientX < midpoint;

                    const url = new URL(window.location);

                    // Grouped columns: their display order is the grouping
                    // hierarchy (the grouped= parameter), not the columns=
                    // order — reorder the hierarchy itself. Expansion paths
                    // (gexp) encode the old level order, so drop them.
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
                        url.searchParams.set('scrollY', Math.round(window.scrollY).toString());
                        window.location.href = url.toString();
                        return;
                    }

                    // Get current column order from URL or build from table headers
                    let columnsParam = url.searchParams.get('columns');

                    // If no columns param, build it from current table headers
                    if (!columnsParam) {
                        const colNames = [];
                        headers.forEach(function(header) {
                            const colName = header.dataset.colName;
                            const width = header.style.width ? parseInt(header.style.width, 10) : 0;
                            if (colName) {
                                colNames.push(width > 0 ? colName + ':' + width : colName);
                            }
                        });
                        columnsParam = colNames.join(',');
                    }

                    // Parse columns (preserving widths)
                    const columns = columnsParam.split(',');
                    const draggedColName = draggedHeader.dataset.colName;
                    const targetColName = th.dataset.colName;

                    // Find indices
                    let draggedIdx = -1;
                    let targetIdx = -1;
                    columns.forEach(function(col, idx) {
                        const colName = col.split(':')[0];
                        if (colName === draggedColName) draggedIdx = idx;
                        if (colName === targetColName) targetIdx = idx;
                    });

                    if (draggedIdx === -1 || targetIdx === -1) return;

                    // Remove dragged column from array
                    const draggedCol = columns.splice(draggedIdx, 1)[0];

                    // Recalculate target index after removal
                    if (draggedIdx < targetIdx) {
                        targetIdx--;
                    }

                    // Insert at new position
                    const insertIdx = dropOnLeft ? targetIdx : targetIdx + 1;
                    columns.splice(insertIdx, 0, draggedCol);

                    // Update URL and reload
                    url.searchParams.set('columns', columns.join(','));
                    url.searchParams.set('scrollY', Math.round(window.scrollY).toString());
                    window.location.href = url.toString();
                });
            });
        }

        // Reset column widths to default (removes widths from URL)
        function resetColumnWidths() {
            const url = new URL(window.location);
            const columnsParam = url.searchParams.get('columns');
            if (!columnsParam) return;

            // Strip all widths from columns param
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

            // Remove inline widths from all headers
            const table = document.getElementById('data-table');
            if (table) {
                const headers = table.querySelectorAll('thead tr:first-child th');
                headers.forEach(function(th) {
                    th.style.width = '';
                });
            }

            history.replaceState(null, '', url.toString());
        }
