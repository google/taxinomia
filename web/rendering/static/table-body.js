        // Navigation helpers that build a new URL from the current one. They
        // run through navigate() (table-head.js): a fragment navigation for
        // table pages, a full one otherwise.

        // Total rows in the table (for capping limit) - read from data attribute
        function totalRowCount() {
            const table = document.getElementById('data-table');
            return table ? (parseInt(table.dataset.totalRows, 10) || 0) : 0;
        }

        // Change the row limit by a multiplier
        function changeLimit(multiplier) {
            const url = currentUrl();
            const currentLimit = parseInt(url.searchParams.get('limit'), 10) || 25;
            // At least 1, an integer, and no more than the table's rows
            const newLimit = Math.min(totalRowCount(), Math.max(1, Math.round(currentLimit * multiplier)));
            url.searchParams.set('limit', newLimit.toString());
            navigate(url);
        }

        function removeComputedColumn(name) {
            const url = currentUrl();

            // Remove from computed definitions
            const existingComputed = url.searchParams.get('computed') || '';
            const computedList = existingComputed.split(';').filter(def => !def.startsWith(name + '='));
            if (computedList.length > 0) {
                url.searchParams.set('computed', computedList.join(';'));
            } else {
                url.searchParams.delete('computed');
            }

            // Remove from visible columns
            const columns = url.searchParams.get('columns') || '';
            const columnList = columns.split(',').filter(col => col !== name && !col.startsWith(name + ':'));
            if (columnList.length > 0) {
                url.searchParams.set('columns', columnList.join(','));
            } else {
                url.searchParams.delete('columns');
            }

            navigate(url);
        }

        // Update the formula for an existing computed column
        function updateComputedFormula(columnName, newExpression) {
            newExpression = newExpression.trim();
            if (!newExpression) {
                return false;
            }

            const url = currentUrl();
            const existingComputed = url.searchParams.get('computed') || '';
            const computedList = existingComputed.split(';');

            const updatedList = computedList.map(def => {
                if (def.startsWith(columnName + '=')) {
                    return columnName + '=' + newExpression;
                }
                return def;
            });

            url.searchParams.set('computed', updatedList.join(';'));
            navigate(url);
            return true;
        }

        // Rename a computed column
        function renameComputedColumn(oldName, newName) {
            newName = newName.trim();
            if (!newName || newName === oldName) {
                return false;
            }

            if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(newName)) {
                alert('Invalid column name. Must start with a letter or underscore.');
                return false;
            }

            const url = currentUrl();

            // Update computed definitions
            const existingComputed = url.searchParams.get('computed') || '';
            const computedList = existingComputed.split(';');
            const updatedComputed = computedList.map(def => {
                if (def.startsWith(oldName + '=')) {
                    return newName + '=' + def.substring(oldName.length + 1);
                }
                return def;
            });
            url.searchParams.set('computed', updatedComputed.join(';'));

            // Update columns list
            const columns = url.searchParams.get('columns') || '';
            const columnList = columns.split(',').map(col => {
                // Handle column with width suffix
                const colonIdx = col.lastIndexOf(':');
                if (colonIdx !== -1) {
                    const colName = col.substring(0, colonIdx);
                    const width = col.substring(colonIdx + 1);
                    if (/^\d+$/.test(width) && colName === oldName) {
                        return newName + ':' + width;
                    }
                }
                return col === oldName ? newName : col;
            });
            url.searchParams.set('columns', columnList.join(','));

            navigate(url);
            return true;
        }

        // Create a new computed column with a unique name
        function createNewComputedColumn() {
            const url = currentUrl();
            const existingComputed = url.searchParams.get('computed') || '';

            // Find a unique name
            let counter = 1;
            let newName = 'new_column';
            const existingNames = existingComputed.split(';').map(def => {
                const eqIdx = def.indexOf('=');
                return eqIdx > 0 ? def.substring(0, eqIdx) : '';
            });
            while (existingNames.includes(newName)) {
                counter++;
                newName = 'new_column_' + counter;
            }

            // Add the new column with empty expression
            const newDef = newName + '=';
            const computedList = existingComputed ? existingComputed.split(';') : [];
            computedList.push(newDef);
            url.searchParams.set('computed', computedList.join(';'));

            // Add to visible columns - from the URL or from the current headers
            let columns = url.searchParams.get('columns') || '';
            let columnList;
            if (columns) {
                columnList = columns.split(',');
            } else {
                columnList = [];
                document.querySelectorAll('th[data-col-name]').forEach(function(th) {
                    const colName = th.getAttribute('data-col-name');
                    const width = th.getAttribute('data-col-width');
                    if (width && width !== '0') {
                        columnList.push(colName + ':' + width);
                    } else {
                        columnList.push(colName);
                    }
                });
            }
            columnList.push(newName);
            url.searchParams.set('columns', columnList.join(','));

            // The hash focuses the new column's formula input after the page arrives
            url.hash = 'focus=' + newName;
            navigate(url);
        }

        // Row selection: the detail panel opens for the row's primary key
        function selectRow(rowId) {
            sessionStorage.setItem('scrollPosition', window.scrollY);
            const url = currentUrl();
            url.searchParams.set('row', rowId);
            navigate(url);
        }

        function deselectRow() {
            sessionStorage.setItem('scrollPosition', window.scrollY);
            const url = currentUrl();
            url.searchParams.delete('row');
            navigate(url);
        }

        // Restore scroll position after a full-load row selection
        (function() {
            const savedPosition = sessionStorage.getItem('scrollPosition');
            if (savedPosition !== null) {
                sessionStorage.removeItem('scrollPosition');
                if (!fragmentState.swapped) {
                    window.scrollTo(0, parseInt(savedPosition, 10));
                }
            }
        })();
