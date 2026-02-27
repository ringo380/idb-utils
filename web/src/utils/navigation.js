// Cross-tab navigation pub/sub
//
// Provides a thin coordination layer so that any component can request
// a tab switch or pass parameters (page number, index filter) to
// another tab without importing main.js directly.

let _switchTab = null;
let _getTabIndexById = null;
let _requestedPage = null;
let _requestedIndexFilter = null;

/**
 * Called once from main.js to wire up the navigation callbacks.
 * @param {function(number): void} switchTabFn - switches to a tab by numeric index
 * @param {function(string): number} getTabIndexByIdFn - resolves tab id string to index
 */
export function initNavigation(switchTabFn, getTabIndexByIdFn) {
  _switchTab = switchTabFn;
  _getTabIndexById = getTabIndexByIdFn;
}

/**
 * Navigate to a tab by its string ID (e.g. 'pages', 'health').
 * No-op if navigation has not been initialised or the tab ID is unknown.
 * @param {string} tabId
 */
export function navigateToTab(tabId) {
  if (!_switchTab || !_getTabIndexById) return;
  const idx = _getTabIndexById(tabId);
  if (idx >= 0) _switchTab(idx);
}

/**
 * Request the Pages tab to navigate to a specific page number.
 * The Pages component calls consumeRequestedPage() on mount to read it.
 * @param {number} pageNum
 */
export function requestPage(pageNum) {
  _requestedPage = pageNum;
}

/**
 * Consume (read & clear) the pending page navigation request.
 * @returns {number|null}
 */
export function consumeRequestedPage() {
  const p = _requestedPage;
  _requestedPage = null;
  return p;
}

/**
 * Request the Pages tab to filter by a specific index ID.
 * The Pages component calls consumeIndexFilter() on mount to read it.
 * @param {string|number} indexId
 */
export function requestIndexFilter(indexId) {
  _requestedIndexFilter = indexId;
}

/**
 * Consume (read & clear) the pending index filter request.
 * @returns {string|number|null}
 */
export function consumeIndexFilter() {
  const f = _requestedIndexFilter;
  _requestedIndexFilter = null;
  return f;
}
