document.addEventListener('DOMContentLoaded', function() {
	document.querySelectorAll('.groupon-input').forEach(function(input) {
		input.addEventListener('keypress', function(e) {
			if (e.key === 'Enter') {
				e.preventDefault();

				// Get current URL params
				const params = new URLSearchParams(window.location.search);

				// Get existing groupon columns from URL
				const existingGroupon = params.get('groupon') || '';
				const existingGrouponCols = new Set();
				if (existingGroupon) {
					existingGroupon.split(',').forEach(function(part) {
						const col = part.split(':')[0];
						if (col) existingGrouponCols.add(col);
					});
				}

				// Collect groupon values only for columns that have input or were already in groupon
				const groupons = {};
				document.querySelectorAll('.groupon-input').forEach(function(inp) {
					const col = inp.dataset.column;
					const val = inp.value.trim();
					// Only store if: has a value OR was already in the groupon parameter
					if (val !== "" || existingGrouponCols.has(col)) {
						groupons[col] = val;
					}
				});

				// Build groupon parameter (convert || to ;)
				const grouponParts = [];
				for (const [col, filters] of Object.entries(groupons)) {
					if (filters === "") {
						// Empty input = default grouping (no colon)
						grouponParts.push(col);
					} else {
						// Has filters - add with colon
						const cleaned = filters.replace(/\|\|/g, ";");
						grouponParts.push(col + ":" + encodeURIComponent(cleaned));
					}
				}

				// Extract raw columns value from current URL to preserve encoding
				const currentSearch = window.location.search.substring(1);
				let rawColumns = '';
				const searchParts = currentSearch.split('&');
				for (let part of searchParts) {
					if (part.startsWith('columns=')) {
						rawColumns = part.substring(8); // Skip 'columns='
						break;
					}
				}

				// Build query string manually to avoid encoding commas in columns/groupon
				const queryParts = [];
				for (const [key, value] of params.entries()) {
					if (key === 'columns' || key === 'groupon') {
						// Skip - we'll handle these specially
						continue;
					}
					queryParts.push(encodeURIComponent(key) + '=' + encodeURIComponent(value));
				}

				// Add columns with raw value (no re-encoding)
				if (rawColumns) {
					queryParts.push('columns=' + rawColumns);
				}

				// Add groupon without encoding commas
				if (grouponParts.length > 0) {
					queryParts.push('groupon=' + grouponParts.join(','));
				}

				// Reload with new params
				window.location.search = queryParts.join('&');
			}
		});
	});
});
