// Help system — tooltips, tab intros, legends, and onboarding
import { trackFeatureUse } from './analytics.js';

/**
 * Create a small (?) icon that shows a tooltip on hover/focus.
 * @param {string} text — tooltip content
 * @returns {HTMLSpanElement}
 */
export function createHelpIcon(text) {
  const span = document.createElement('span');
  span.className = 'help-icon';
  span.textContent = '?';
  span.setAttribute('role', 'img');
  span.setAttribute('aria-label', text);
  span.setAttribute('tabindex', '0');

  let tip = null;
  let tracked = false;
  const show = () => {
    if (tip) return;
    if (!tracked) { tracked = true; trackFeatureUse('help_icon_hover', { context: text.slice(0, 40) }); }
    tip = document.createElement('div');
    tip.className = 'help-tooltip';
    tip.textContent = text;
    span.appendChild(tip);
  };
  const hide = () => {
    if (tip) { tip.remove(); tip = null; }
  };

  span.addEventListener('mouseenter', show);
  span.addEventListener('mouseleave', hide);
  span.addEventListener('focus', show);
  span.addEventListener('blur', hide);
  return span;
}

/**
 * Create a collapsible intro banner for a tab.
 * @param {string} tabId — used for localStorage persistence
 * @param {string} description — 1-2 sentence summary
 * @param {string[]} [tips] — optional bullet list of tips
 * @returns {HTMLDivElement}
 */
export function createTabIntro(tabId, description, tips = []) {
  const storageKey = `idb-tab-intro-${tabId}`;
  const isCollapsed = localStorage.getItem(storageKey) === 'collapsed';

  const wrapper = document.createElement('div');
  wrapper.className = isCollapsed ? '' : 'tab-intro';

  const update = (collapsed) => {
    if (collapsed) {
      wrapper.className = '';
      wrapper.innerHTML = '';
      const expandBtn = document.createElement('button');
      expandBtn.className = 'text-gray-600 hover:text-innodb-cyan text-xs flex items-center gap-1 mt-1 transition-colors';
      expandBtn.innerHTML = `<svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><circle cx="12" cy="12" r="10" stroke-width="2"/><path stroke-width="2" d="M12 16v-4m0-4h.01"/></svg> Show tab info`;
      expandBtn.setAttribute('aria-expanded', 'false');
      expandBtn.addEventListener('click', () => {
        localStorage.removeItem(storageKey);
        trackFeatureUse('tab_intro_toggle', { tab: tabId, action: 'expand' });
        update(false);
      });
      wrapper.appendChild(expandBtn);
    } else {
      wrapper.className = 'tab-intro';
      let html = `<div class="flex items-start justify-between gap-3">
        <div>${description}`;
      if (tips.length > 0) {
        html += '<ul>' + tips.map((t) => `<li>${t}</li>`).join('') + '</ul>';
      }
      html += `</div>
        <button class="text-gray-600 hover:text-gray-400 flex-shrink-0 mt-0.5" aria-label="Collapse tab info" aria-expanded="true" title="Hide">
          <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/>
          </svg>
        </button>
      </div>`;
      wrapper.innerHTML = html;
      wrapper.querySelector('button').addEventListener('click', () => {
        localStorage.setItem(storageKey, 'collapsed');
        trackFeatureUse('tab_intro_toggle', { tab: tabId, action: 'collapse' });
        update(true);
      });
    }
  };

  update(isCollapsed);
  return wrapper;
}

/**
 * Create a color legend row.
 * @param {{ className: string, label: string }[]} items
 * @returns {HTMLDivElement}
 */
export function createLegend(items) {
  const div = document.createElement('div');
  div.className = 'help-legend';
  for (const item of items) {
    const el = document.createElement('span');
    el.className = 'help-legend-item';
    el.innerHTML = `<span class="help-legend-swatch ${item.className}"></span>${item.label}`;
    div.appendChild(el);
  }
  return div;
}

/** @returns {boolean} */
export function isFirstVisit() {
  return localStorage.getItem('idb-welcomed') !== 'true';
}

export function markWelcomed() {
  localStorage.setItem('idb-welcomed', 'true');
}

/**
 * Insert a tab intro banner after the header in a tab container.
 * Call this after setting container.innerHTML.
 * @param {HTMLElement} container
 * @param {string} tabId
 */
export function insertTabIntro(container, tabId) {
  const info = TAB_DESCRIPTIONS[tabId];
  if (!info) return;
  const wrapper = container.querySelector('.p-6') || container;
  const header = wrapper.querySelector('.flex');
  if (header) {
    const intro = createTabIntro(tabId, info.description, info.tips);
    header.after(intro);
  }
}

/** Tab descriptions keyed by tab ID. */
export const TAB_DESCRIPTIONS = {
  overview: {
    description: 'Tablespace summary — page count, page types, space ID, and file-level metadata. Start here to understand your tablespace structure.',
    tips: ['Check Vendor to confirm MySQL, Percona, or MariaDB origin', 'Page type distribution shows what the tablespace is used for'],
  },
  pages: {
    description: 'Detailed page-level analysis — headers, record counts, fill factors, and decoded data. Click any page to inspect its contents.',
    tips: ['Use the page number input to jump to a specific page', 'Click "Decoded Records" on INDEX pages to see row data'],
  },
  checksums: {
    description: 'Validates page checksums using CRC-32C, legacy InnoDB, or MariaDB full_crc32 algorithms. Invalid checksums indicate possible data corruption.',
    tips: ['Green = valid, Red = corrupt or tampered', 'LSN mismatches may indicate torn writes'],
  },
  sdi: {
    description: 'Raw SDI (Serialized Dictionary Information) metadata from MySQL 8.0+ tablespaces. Contains table and index definitions in JSON format.',
    tips: ['Pre-8.0 tablespaces do not contain SDI data', 'Use the Schema tab for a formatted view of this data'],
  },
  schema: {
    description: 'Reconstructed DDL (CREATE TABLE) from tablespace metadata. Shows columns, indexes, and foreign keys.',
    tips: ['Copy the DDL to recreate the table structure', 'Column types are extracted from SDI metadata'],
  },
  hex: {
    description: 'Raw hex view of page bytes. Use byte offset search to locate specific data patterns.',
    tips: ['Navigate by page number to inspect specific pages', 'Colored output: cyan = offset, green = hex bytes, amber = ASCII'],
  },
  recovery: {
    description: 'Assesses page-level recoverability and estimates salvageable records. Shows corruption classification per page.',
    tips: ['Corruption types: zero-fill, random noise, torn write, bitrot', 'Higher salvageable record count = more data to recover'],
  },
  heatmap: {
    description: 'Visual map of all pages colored by type. Click any cell to jump to that page. In diff mode, highlights modified, added, and removed pages.',
    tips: ['Each cell represents one page — hover for details', 'Diff overlay shows changes between two tablespace files'],
  },
  health: {
    description: 'B+Tree index health metrics — fill factor, fragmentation, garbage ratio, and bloat grades (A through F). Low fill factors or high bloat suggest optimization opportunities.',
    tips: ['Click an index row to filter the Pages tab to that index', 'Toggle the B+Tree visualization for a graphical view'],
  },
  verify: {
    description: 'Structural integrity checks — page chain continuity, LSN ordering, and format validation. Findings are categorized by severity.',
    tips: ['Page chain breaks indicate structural damage', 'LSN ordering issues may signal incomplete crash recovery'],
  },
  compat: {
    description: 'Version compatibility analysis. Auto-detects the source MySQL version and checks for upgrade or downgrade issues.',
    tips: ['Source version is detected from SDI mysqld_version_id', 'Select a target version to see compatibility findings'],
  },
  undo: {
    description: 'Undo tablespace analysis — rollback segments, transaction history, and undo record details.',
    tips: ['Requires an undo tablespace file (.ibu)', 'Shows active vs cached rollback segments'],
  },
  spatial: {
    description: 'R-Tree index visualization for spatial (GIS) data. Shows minimum bounding rectangles for each spatial index entry.',
    tips: ['Only visible when spatial indexes are present', 'Zoom and pan to explore large spatial datasets'],
  },
  undelete: {
    description: 'Recovers deleted records from tablespace pages and undo logs. Confidence scores indicate how reliable each recovered record is.',
    tips: ['Filter by confidence to focus on high-quality recoveries', 'Export recovered data as CSV, JSON, or SQL INSERT statements'],
  },
  simulate: {
    description: 'Simulates InnoDB crash recovery at levels 1 through 6. Shows which pages would be affected and predicted recovery outcomes.',
    tips: ['Level 1 = safest (skip redo), Level 6 = most aggressive', 'Use this to understand crash recovery impact before modifying innodb_force_recovery'],
  },
  timeline: {
    description: 'Unified modification timeline correlating redo log, undo log, and binary log events into a single chronological view.',
    tips: ['Drop multiple log files to see cross-referenced events', 'Filter by page number or transaction to narrow the view'],
  },
  redolog: {
    description: 'Redo log block analysis — LSN ranges, checkpoint info, and log record types.',
    tips: ['Blocks are 512-byte units within the redo log', 'Empty blocks indicate unused log space'],
  },
  binlog: {
    description: 'Binary log event listing — transactions, row changes, DDL statements, and replication metadata.',
    tips: ['Events are shown in file order with timestamps', 'CRC-32C checksum validation per event'],
  },
  diff: {
    description: 'Side-by-side comparison of two tablespace files. Shows page-level differences in headers, checksums, and content.',
    tips: ['Drop two .ibd files to enable this tab', 'Modified pages show which header fields changed'],
  },
  backup: {
    description: 'Compares page LSNs between a backup and current tablespace to identify pages changed since the backup was taken.',
    tips: ['Drop two files: backup (older) and current', 'Useful for validating incremental backup coverage'],
  },
  audit: {
    description: 'Directory-wide integrity analysis across multiple tablespace files. Shows health summaries, checksum mismatches, and bloat scores.',
    tips: ['Drop 3 or more .ibd files to enable audit mode', 'Sort by fill factor or bloat grade to find problem tables'],
  },
};
