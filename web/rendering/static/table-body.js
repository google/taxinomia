        // Total rows in the table (for capping limit) - read from data attribute
        const totalRows = parseInt(document.getElementById('data-table').dataset.totalRows, 10) || 0;

        // Change the row limit by a multiplier
        function changeLimit(multiplier) {
            const url = new URL(window.location);
            const currentLimit = parseInt(url.searchParams.get('limit'), 10) || 25;
            // Calculate new limit, ensure it's at least 1, is an integer, and doesn't exceed total rows
            const newLimit = Math.min(totalRows, Math.max(1, Math.round(currentLimit * multiplier)));
            url.searchParams.set('limit', newLimit.toString());
            window.location.href = url.toString();
        }

        function removeComputedColumn(name) {
            const url = new URL(window.location);

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

            window.location.href = url.toString();
        }

        // Update the formula for an existing computed column
        function updateComputedFormula(columnName, newExpression) {
            newExpression = newExpression.trim();
            if (!newExpression) {
                return false;
            }

            const url = new URL(window.location);
            const existingComputed = url.searchParams.get('computed') || '';
            const computedList = existingComputed.split(';');

            // Find and update the formula for this column
            const updatedList = computedList.map(def => {
                if (def.startsWith(columnName + '=')) {
                    return columnName + '=' + newExpression;
                }
                return def;
            });

            url.searchParams.set('computed', updatedList.join(';'));
            window.location.href = url.toString();
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

            const url = new URL(window.location);

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

            window.location.href = url.toString();
            return true;
        }

        // Create a new computed column with a unique name
        function createNewComputedColumn() {
            const url = new URL(window.location);
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

            // Add to visible columns - get from URL or from current table headers
            let columns = url.searchParams.get('columns') || '';
            let columnList;
            if (columns) {
                columnList = columns.split(',');
            } else {
                // No columns param - get current visible columns from the table headers
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

            // Add hash to focus on the new column's formula input after page load
            url.hash = 'focus=' + newName;
            window.location.href = url.toString();
        }

        // Handle computed column UI
        document.addEventListener('DOMContentLoaded', function() {
            // Check if we need to focus on a newly created column
            if (window.location.hash.startsWith('#focus=')) {
                const columnName = window.location.hash.substring(7); // Remove '#focus='
                const formulaInput = document.querySelector('.formula-input[data-column="' + columnName + '"]');
                if (formulaInput) {
                    formulaInput.focus();
                    formulaInput.select();
                    // Clear the hash to avoid re-focusing on refresh
                    history.replaceState(null, '', window.location.pathname + window.location.search);
                }
            }

            // Clean up _anim parameter from URL after animation plays
            // This prevents re-triggering the animation on page refresh
            const url = new URL(window.location);
            if (url.searchParams.has('_anim')) {
                // Wait for animation to complete (1.5s total), then clean URL
                setTimeout(function() {
                    url.searchParams.delete('_anim');
                    window.history.replaceState({}, '', url);
                }, 1500);
            }

            // Handle + button to create new column
            const addBtn = document.getElementById('add-computed-btn');
            if (addBtn) {
                addBtn.addEventListener('click', createNewComputedColumn);
            }

            // Handle column name inputs in header
            const nameInputs = document.querySelectorAll('.th-name-input');
            nameInputs.forEach(function(input) {
                const originalName = input.value;

                input.addEventListener('keydown', function(e) {
                    if (e.key === 'Enter') {
                        e.preventDefault();
                        const newName = this.value.trim();
                        if (newName !== originalName) {
                            renameComputedColumn(originalName, newName);
                        }
                        this.blur();
                    } else if (e.key === 'Escape') {
                        this.value = originalName;
                        this.blur();
                    }
                });

                input.addEventListener('blur', function() {
                    const newName = this.value.trim();
                    if (newName && newName !== originalName) {
                        renameComputedColumn(originalName, newName);
                    } else {
                        this.value = originalName;
                    }
                });
            });

            // Handle formula inputs in the formula row
            const formulaInputs = document.querySelectorAll('.formula-cell .formula-input');
            formulaInputs.forEach(function(input) {
                const originalValue = input.value;

                input.addEventListener('keydown', function(e) {
                    if (e.key === 'Enter') {
                        e.preventDefault();
                        const columnName = this.dataset.column;
                        const newValue = this.value.trim();
                        if (newValue !== originalValue) {
                            updateComputedFormula(columnName, newValue);
                        }
                        this.blur();
                    } else if (e.key === 'Escape') {
                        this.value = originalValue;
                        this.blur();
                    }
                });

                input.addEventListener('blur', function() {
                    const columnName = this.dataset.column;
                    const newValue = this.value.trim();
                    if (newValue !== originalValue && newValue !== '') {
                        updateComputedFormula(columnName, newValue);
                    } else if (newValue === '') {
                        this.value = originalValue;
                    }
                });
            });
        });

        // Handle remove computed column button clicks using event delegation
        document.addEventListener('click', function(e) {
            if (e.target.classList.contains('remove-computed-btn')) {
                const columnName = e.target.dataset.columnName;
                if (columnName) {
                    removeComputedColumn(columnName);
                }
            }
        });

        // Row selection handling - preserves scroll position
        function selectRow(rowId) {
            sessionStorage.setItem('scrollPosition', window.scrollY);
            const url = new URL(window.location);
            url.searchParams.set('row', rowId);
            window.location.href = url.toString();
        }

        function deselectRow() {
            sessionStorage.setItem('scrollPosition', window.scrollY);
            const url = new URL(window.location);
            url.searchParams.delete('row');
            window.location.href = url.toString();
        }

        // Restore scroll position after row selection
        (function() {
            const savedPosition = sessionStorage.getItem('scrollPosition');
            if (savedPosition !== null) {
                sessionStorage.removeItem('scrollPosition');
                window.scrollTo(0, parseInt(savedPosition, 10));
            }
        })();

        // Handle row clicks for selection (flat rows only)
        document.addEventListener('click', function(e) {
            // Find the clicked row - must be in tbody and have data-row-id
            const row = e.target.closest('tbody tr[data-row-id]');
            if (!row) return;

            // Don't select if clicking on a link, button, input, or interactive elements
            if (e.target.closest('a, button, input, .filter-link, .entity-link, .multiselect-checkbox')) return;

            const rowId = row.dataset.rowId;
            if (rowId) {
                // If already selected, deselect; otherwise select
                const currentRowId = new URL(window.location).searchParams.get('row') || '';
                if (currentRowId === rowId) {
                    deselectRow();
                } else {
                    selectRow(rowId);
                }
            }
        });
