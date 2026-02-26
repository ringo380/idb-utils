// Tab navigation bar

const TAB_DEFS = [
  { id: 'overview', label: 'Overview', key: '1' },
  { id: 'pages', label: 'Pages', key: '2' },
  { id: 'checksums', label: 'Checksums', key: '3' },
  { id: 'sdi', label: 'SDI', key: '4' },
  { id: 'schema', label: 'Schema', key: '5' },
  { id: 'hex', label: 'Hex Dump', key: '6' },
  { id: 'recovery', label: 'Recovery', key: '7' },
];

const HEATMAP_TAB = { id: 'heatmap', label: 'Heatmap', key: '8' };
const HEALTH_TAB = { id: 'health', label: 'Health', key: 'H' };
const VERIFY_TAB = { id: 'verify', label: 'Verify', key: 'V' };
const COMPAT_TAB = { id: 'compat', label: 'Compat', key: 'C' };
const DIFF_TAB = { id: 'diff', label: 'Diff', key: '9' };
const AUDIT_TAB = { id: 'audit', label: 'Audit', key: '0' };
const REDOLOG_TAB = { id: 'redolog', label: 'Redo Log', key: '1' };

function getVisibleTabs({ showDiff = false, showRedoLog = false, showAudit = false } = {}) {
  if (showRedoLog) return [REDOLOG_TAB];
  const tabs = [...TAB_DEFS, HEATMAP_TAB, HEALTH_TAB, VERIFY_TAB, COMPAT_TAB];
  if (showDiff) tabs.push(DIFF_TAB);
  if (showAudit) tabs.push(AUDIT_TAB);
  return tabs;
}

export function createTabs(onSwitch, opts = {}) {
  const tabs = getVisibleTabs(opts);
  const el = document.createElement('nav');
  el.className = 'flex gap-1 px-4 pt-3 pb-0 bg-gray-950 border-b border-gray-800 overflow-x-auto';
  el.setAttribute('role', 'tablist');

  tabs.forEach((tab, i) => {
    const btn = document.createElement('button');
    btn.dataset.tab = tab.id;
    btn.setAttribute('role', 'tab');
    btn.setAttribute('aria-selected', 'false');
    btn.setAttribute('aria-controls', 'tab-content');
    btn.setAttribute('tabindex', i === 0 ? '0' : '-1');
    btn.className =
      'px-3 py-2 text-sm text-gray-500 hover:text-gray-300 border-b-2 border-transparent transition-colors whitespace-nowrap';
    btn.textContent = `${tab.key} ${tab.label}`;
    btn.addEventListener('click', () => onSwitch(i));

    // Arrow key navigation within tablist
    btn.addEventListener('keydown', (e) => {
      const buttons = el.querySelectorAll('[role="tab"]');
      let newIndex = -1;
      if (e.key === 'ArrowRight') newIndex = (i + 1) % buttons.length;
      else if (e.key === 'ArrowLeft') newIndex = (i - 1 + buttons.length) % buttons.length;
      else if (e.key === 'Home') newIndex = 0;
      else if (e.key === 'End') newIndex = buttons.length - 1;
      if (newIndex >= 0) {
        e.preventDefault();
        buttons[newIndex].focus();
        onSwitch(newIndex);
      }
    });

    el.appendChild(btn);
  });

  return el;
}

export function setActiveTab(nav, index) {
  const buttons = nav.querySelectorAll('[role="tab"]');
  buttons.forEach((btn, i) => {
    if (i === index) {
      btn.classList.add('tab-active');
      btn.classList.remove('text-gray-500');
      btn.setAttribute('aria-selected', 'true');
      btn.setAttribute('tabindex', '0');
    } else {
      btn.classList.remove('tab-active');
      btn.classList.add('text-gray-500');
      btn.setAttribute('aria-selected', 'false');
      btn.setAttribute('tabindex', '-1');
    }
  });
}

export function getTabId(index, opts = {}) {
  const tabs = getVisibleTabs(opts);
  return tabs[index]?.id;
}

export function getTabCount(opts = {}) {
  return getVisibleTabs(opts).length;
}

/**
 * Returns a Set of all keyboard shortcut keys for currently visible tabs.
 * Keys are stored as-is (e.g. '1', '2', 'H', 'V', 'C').
 */
export function getVisibleTabKeys(opts = {}) {
  return new Set(getVisibleTabs(opts).map((t) => t.key));
}

/**
 * Returns the tab index for a given keyboard shortcut key (case-insensitive
 * for letters), or -1 if no visible tab matches.
 */
export function getTabIndexByKey(key, opts = {}) {
  const upper = key.toUpperCase();
  const tabs = getVisibleTabs(opts);
  return tabs.findIndex((t) => t.key.toUpperCase() === upper);
}

/**
 * Returns the tab index for a given tab ID string, or -1 if not found.
 */
export function getTabIndexById(id, opts = {}) {
  return getVisibleTabs(opts).findIndex((t) => t.id === id);
}
