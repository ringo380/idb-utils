// Timeline tab — unified modification timeline from redo, undo, and binlog

import { getWasm } from '../wasm.js';
import { esc } from '../utils/html.js';
import { trackFeatureUse } from '../utils/analytics.js';
import { requestPage, navigateToTab } from '../utils/navigation.js';
import { insertTabIntro } from '../utils/help.js';

const PAGE_SIZE = 50;

/**
 * Render the Timeline tab.
 *
 * The tab accepts additional file drops (redo log, undo tablespace, binlog)
 * and correlates them into a single chronological timeline.
 *
 * @param {HTMLElement} container
 * @param {Uint8Array} _primaryData — the primary .ibd file (unused directly)
 */
export function createTimeline(container, _primaryData) {
  // State: additional files loaded by the user
  let redoData = null;
  let undoData = null;
  let binlogData = null;
  let tablespaceData = null;
  let report = null;
  let currentPage = 0;
  let sourceFilter = { redo: true, undo: true, binlog: true };
  let viewMode = 'visual'; // 'visual' or 'table'
  let vizZoom = 1;

  container.innerHTML = buildDropZoneHTML();
  insertTabIntro(container, 'timeline');
  wireDropZone(container);

  // ── Drop zone ─────────────────────────────────────────────────────

  function buildDropZoneHTML() {
    return `
      <div class="p-6 space-y-6 overflow-auto max-h-full">
        <h2 class="text-lg font-bold">Transaction Timeline</h2>
        <p class="text-sm text-gray-400">
          Drop one or more log files below to build a unified modification timeline.
        </p>
        <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
          ${dropCard('redo', 'Redo Log', 'ib_logfile0 or #ib_redo*', redoData)}
          ${dropCard('undo', 'Undo Tablespace', '.ibu or undo_001', undoData)}
          ${dropCard('binlog', 'Binary Log', 'mysql-bin.000001', binlogData)}
          ${dropCard('tablespace', 'Tablespace', '.ibd (for page correlation)', tablespaceData)}
        </div>
        <div class="flex gap-3">
          <button id="tl-analyze"
            class="px-4 py-2 text-sm font-medium rounded bg-innodb-cyan/20 text-innodb-cyan hover:bg-innodb-cyan/30 transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
            ${!(redoData || undoData || binlogData || tablespaceData) ? 'disabled' : ''}>
            Analyze Timeline
          </button>
          <button id="tl-clear"
            class="px-4 py-2 text-sm font-medium rounded bg-gray-800 text-gray-400 hover:bg-gray-700 transition-colors">
            Clear All
          </button>
        </div>
        <div id="tl-results"></div>
      </div>
    `;
  }

  function dropCard(id, label, hint, data) {
    const loaded = !!data;
    return `
      <div id="tl-drop-${id}"
        class="border-2 border-dashed rounded-lg p-4 text-center cursor-pointer transition-colors
          ${loaded ? 'border-innodb-cyan/50 bg-innodb-cyan/5' : 'border-gray-700 hover:border-gray-500'}">
        <div class="text-sm font-medium ${loaded ? 'text-innodb-cyan' : 'text-gray-300'}">${label}</div>
        <div class="text-xs text-gray-500 mt-1">${loaded ? 'Loaded' : hint}</div>
        <input type="file" class="hidden" data-source="${id}">
      </div>
    `;
  }

  function wireDropZone(el) {
    ['redo', 'undo', 'binlog', 'tablespace'].forEach(id => {
      const card = el.querySelector(`#tl-drop-${id}`);
      if (!card) return;
      const input = card.querySelector('input[type="file"]');

      card.addEventListener('click', () => input.click());
      card.addEventListener('dragover', e => { e.preventDefault(); card.classList.add('border-innodb-cyan'); });
      card.addEventListener('dragleave', () => card.classList.remove('border-innodb-cyan'));
      card.addEventListener('drop', e => {
        e.preventDefault();
        card.classList.remove('border-innodb-cyan');
        if (e.dataTransfer.files.length > 0) loadFile(id, e.dataTransfer.files[0]);
      });
      input.addEventListener('change', () => {
        if (input.files.length > 0) loadFile(id, input.files[0]);
      });
    });

    const analyzeBtn = el.querySelector('#tl-analyze');
    if (analyzeBtn) analyzeBtn.addEventListener('click', runAnalysis);

    const clearBtn = el.querySelector('#tl-clear');
    if (clearBtn) clearBtn.addEventListener('click', () => {
      redoData = null; undoData = null; binlogData = null; tablespaceData = null; report = null;
      container.innerHTML = buildDropZoneHTML();
      insertTabIntro(container, 'timeline');
      wireDropZone(container);
    });
  }

  function loadFile(source, file) {
    const reader = new FileReader();
    reader.onload = () => {
      const data = new Uint8Array(reader.result);
      if (source === 'redo') redoData = data;
      else if (source === 'undo') undoData = data;
      else if (source === 'binlog') binlogData = data;
      else if (source === 'tablespace') tablespaceData = data;
      trackFileUpload(file.name, data.length, source);
      // Re-render drop zone to show "Loaded" state
      container.innerHTML = buildDropZoneHTML();
      insertTabIntro(container, 'timeline');
      wireDropZone(container);
    };
    reader.readAsArrayBuffer(file);
  }

  // ── Analysis ──────────────────────────────────────────────────────

  function runAnalysis() {
    trackFeatureUse('timeline_analyze');
    const wasm = getWasm();
    const results = container.querySelector('#tl-results');
    if (!results) return;

    try {
      const empty = new Uint8Array(0);
      const json = wasm.build_timeline(
        redoData || empty,
        undoData || empty,
        binlogData || empty,
        tablespaceData || empty,
      );
      report = JSON.parse(json);
    } catch (e) {
      results.innerHTML = `<div class="p-4 bg-red-900/30 rounded text-red-400 text-sm">Error: ${esc(String(e))}</div>`;
      return;
    }

    currentPage = 0;
    renderResults(results);
  }

  // ── Results rendering ─────────────────────────────────────────────

  function renderResults(el) {
    const r = report;
    if (!r) return;

    const filtered = r.entries.filter(e => {
      if (e.source === 'RedoLog' && !sourceFilter.redo) return false;
      if (e.source === 'UndoLog' && !sourceFilter.undo) return false;
      if (e.source === 'Binlog' && !sourceFilter.binlog) return false;
      return true;
    });

    const totalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
    const pageEntries = filtered.slice(currentPage * PAGE_SIZE, (currentPage + 1) * PAGE_SIZE);

    el.innerHTML = `
      ${renderSummary(r)}
      ${renderFilters()}
      ${renderViewToggle()}
      ${viewMode === 'visual' ? renderVisualTimeline(filtered) : ''}
      ${viewMode === 'table' ? renderTable(pageEntries) : ''}
      ${viewMode === 'table' ? renderPagination(filtered.length, totalPages) : ''}
      ${renderPageSummaries(r.page_summaries)}
    `;

    wireFilters(el);
    wireViewToggle(el);
    if (viewMode === 'visual') wireVisualTimeline();
    if (viewMode === 'table') wirePagination(el, filtered.length, totalPages);
    wirePageLinks(el);
  }

  function renderSummary(r) {
    return `
      <div class="grid grid-cols-2 md:grid-cols-4 gap-4 mt-4">
        ${statCard('Redo Entries', r.redo_count)}
        ${statCard('Undo Entries', r.undo_count)}
        ${statCard('Binlog Entries', r.binlog_count)}
        ${statCard('Correlated Pages', r.correlated_count)}
      </div>
    `;
  }

  function statCard(label, value) {
    return `
      <div class="bg-surface-2 rounded-lg p-4">
        <div class="text-xs text-gray-500 uppercase">${label}</div>
        <div class="text-lg font-bold text-gray-200">${value.toLocaleString()}</div>
      </div>
    `;
  }

  function renderFilters() {
    return `
      <div class="flex items-center gap-4 mt-4">
        <span class="text-xs text-gray-500 uppercase">Sources:</span>
        ${filterCheckbox('redo', 'Redo', sourceFilter.redo, 'text-blue-400')}
        ${filterCheckbox('undo', 'Undo', sourceFilter.undo, 'text-yellow-400')}
        ${filterCheckbox('binlog', 'Binlog', sourceFilter.binlog, 'text-green-400')}
      </div>
    `;
  }

  function filterCheckbox(id, label, checked, colorClass) {
    return `
      <label class="flex items-center gap-1.5 text-sm ${colorClass} cursor-pointer">
        <input type="checkbox" data-source-filter="${id}" ${checked ? 'checked' : ''}
          class="rounded border-gray-600">
        ${label}
      </label>
    `;
  }

  function wireFilters(el) {
    el.querySelectorAll('[data-source-filter]').forEach(cb => {
      cb.addEventListener('change', () => {
        const src = cb.dataset.sourceFilter;
        sourceFilter[src] = cb.checked;
        trackFeatureUse('timeline_filter');
        currentPage = 0;
        renderResults(el);
      });
    });
  }

  function renderViewToggle() {
    return `
      <div class="flex items-center gap-2 mt-4">
        <span class="text-xs text-gray-500 uppercase">View:</span>
        <button data-view="visual"
          class="px-3 py-1 text-xs rounded ${viewMode === 'visual' ? 'bg-innodb-cyan/20 text-innodb-cyan' : 'bg-gray-800 text-gray-400 hover:bg-gray-700'}">
          Visual
        </button>
        <button data-view="table"
          class="px-3 py-1 text-xs rounded ${viewMode === 'table' ? 'bg-innodb-cyan/20 text-innodb-cyan' : 'bg-gray-800 text-gray-400 hover:bg-gray-700'}">
          Table
        </button>
        ${viewMode === 'visual' ? `
          <span class="ml-4 text-xs text-gray-500">Zoom:</span>
          <button data-viz-zoom="out" class="px-2 py-1 text-xs rounded bg-gray-800 text-gray-400 hover:bg-gray-700">−</button>
          <span class="text-xs text-gray-400">${vizZoom}x</span>
          <button data-viz-zoom="in" class="px-2 py-1 text-xs rounded bg-gray-800 text-gray-400 hover:bg-gray-700">+</button>
        ` : ''}
      </div>
    `;
  }

  function wireViewToggle(el) {
    el.querySelectorAll('[data-view]').forEach(btn => {
      btn.addEventListener('click', () => {
        viewMode = btn.dataset.view;
        renderResults(el);
      });
    });
    el.querySelectorAll('[data-viz-zoom]').forEach(btn => {
      btn.addEventListener('click', () => {
        if (btn.dataset.vizZoom === 'in') vizZoom = Math.min(vizZoom + 1, 8);
        else vizZoom = Math.max(vizZoom - 1, 1);
        renderResults(el);
      });
    });
  }

  function renderVisualTimeline(entries) {
    if (entries.length === 0) {
      return '<div class="text-sm text-gray-500 mt-4">No entries match the current filters.</div>';
    }

    // Group entries by page for the swimlane view
    const pageGroups = new Map();
    const noPage = [];
    for (const e of entries) {
      if (e.page_no != null) {
        const key = `${e.space_id ?? '-'}:${e.page_no}`;
        if (!pageGroups.has(key)) pageGroups.set(key, []);
        pageGroups.get(key).push(e);
      } else {
        noPage.push(e);
      }
    }

    // Sort page keys
    const sortedKeys = [...pageGroups.keys()].sort((a, b) => {
      const [, pa] = a.split(':');
      const [, pb] = b.split(':');
      return parseInt(pa) - parseInt(pb);
    });

    const barWidth = Math.max(4, 6 * vizZoom);
    const maxPerLane = 200;

    let html = '<div class="mt-4 space-y-3 overflow-x-auto">';

    // Render swimlanes for each page
    for (const key of sortedKeys) {
      const group = pageGroups.get(key).slice(0, maxPerLane);
      html += `
        <div class="flex items-center gap-2">
          <div class="w-20 flex-shrink-0 text-right">
            <a class="text-xs text-innodb-cyan hover:underline cursor-pointer font-mono"
              data-goto-page="${group[0].page_no}">${esc(key)}</a>
          </div>
          <div class="flex items-end gap-px overflow-x-auto py-1" style="min-height:28px;">
            ${group.map(e => vizBar(e, barWidth)).join('')}
          </div>
          <span class="text-xs text-gray-600 flex-shrink-0">${group.length}</span>
        </div>`;
    }

    // Unresolved entries (no page)
    if (noPage.length > 0) {
      const shown = noPage.slice(0, maxPerLane);
      html += `
        <div class="flex items-center gap-2">
          <div class="w-20 flex-shrink-0 text-right">
            <span class="text-xs text-gray-600 font-mono">no page</span>
          </div>
          <div class="flex items-end gap-px overflow-x-auto py-1" style="min-height:28px;">
            ${shown.map(e => vizBar(e, barWidth)).join('')}
          </div>
          <span class="text-xs text-gray-600 flex-shrink-0">${noPage.length}</span>
        </div>`;
    }

    html += '</div>';

    // Legend
    html += `
      <div class="flex items-center gap-4 mt-3 text-xs text-gray-500">
        <span class="flex items-center gap-1"><span class="inline-block w-3 h-3 rounded-sm bg-blue-500"></span> Redo</span>
        <span class="flex items-center gap-1"><span class="inline-block w-3 h-3 rounded-sm bg-yellow-500"></span> Undo</span>
        <span class="flex items-center gap-1"><span class="inline-block w-3 h-3 rounded-sm bg-green-500"></span> Binlog</span>
        <span class="ml-4 flex items-center gap-1"><span class="inline-block w-3 h-3 rounded-sm bg-green-400"></span> INSERT</span>
        <span class="flex items-center gap-1"><span class="inline-block w-3 h-3 rounded-sm bg-yellow-400"></span> UPDATE</span>
        <span class="flex items-center gap-1"><span class="inline-block w-3 h-3 rounded-sm bg-red-400"></span> DELETE</span>
      </div>
    `;

    return html;
  }

  function vizBar(entry, barWidth) {
    const color = vizColor(entry);
    const height = 16 + (vizZoom * 2);
    const title = `seq=${entry.seq} ${entry.source} ${formatAction(entry.action)}`;
    return `<div class="rounded-sm cursor-pointer hover:opacity-80 transition-opacity"
      style="width:${barWidth}px;height:${height}px;background:${color}"
      title="${esc(title)}"
      ${entry.page_no != null ? `data-goto-page="${entry.page_no}"` : ''}></div>`;
  }

  function vizColor(entry) {
    if (entry.source === 'RedoLog') return '#3b82f6';
    if (entry.source === 'UndoLog') return '#eab308';
    if (entry.source === 'Binlog') {
      const action = entry.action;
      if (!action) return '#22c55e';
      const et = (action.event_type || '').toUpperCase();
      if (et.includes('WRITE') || et.includes('INSERT')) return '#4ade80';
      if (et.includes('UPDATE')) return '#facc15';
      if (et.includes('DELETE')) return '#f87171';
      return '#22c55e';
    }
    return '#6b7280';
  }

  function wireVisualTimeline() {
    // Page links are wired by wirePageLinks — no extra wiring needed
  }

  function renderTable(entries) {
    if (entries.length === 0) {
      return '<div class="text-sm text-gray-500 mt-4">No entries match the current filters.</div>';
    }

    return `
      <div class="overflow-x-auto mt-4">
        <table class="w-full text-xs font-mono">
          <thead class="sticky top-0 bg-gray-950">
            <tr class="text-left text-gray-500 border-b border-gray-800">
              <th class="px-2 py-1.5">SEQ</th>
              <th class="px-2 py-1.5">LSN</th>
              <th class="px-2 py-1.5">SOURCE</th>
              <th class="px-2 py-1.5">SPACE:PAGE</th>
              <th class="px-2 py-1.5">ACTION</th>
            </tr>
          </thead>
          <tbody>
            ${entries.map(e => renderRow(e)).join('')}
          </tbody>
        </table>
      </div>
    `;
  }

  function renderRow(entry) {
    const lsn = entry.lsn != null ? entry.lsn.toString() : '-';
    const sourceClass = sourceColor(entry.source);
    const page = formatPage(entry);
    const action = formatAction(entry.action);

    return `
      <tr class="border-b border-gray-800/30 hover:bg-surface-2/50">
        <td class="px-2 py-1 text-gray-500">${entry.seq}</td>
        <td class="px-2 py-1 text-gray-300">${lsn}</td>
        <td class="px-2 py-1 ${sourceClass}">${esc(entry.source.replace('Log', ''))}</td>
        <td class="px-2 py-1">${page}</td>
        <td class="px-2 py-1 text-gray-300">${esc(action)}</td>
      </tr>
    `;
  }

  function sourceColor(source) {
    if (source === 'RedoLog') return 'text-blue-400';
    if (source === 'UndoLog') return 'text-yellow-400';
    if (source === 'Binlog') return 'text-green-400';
    return 'text-gray-400';
  }

  function formatPage(entry) {
    if (entry.space_id != null && entry.page_no != null) {
      return `<a class="text-innodb-cyan hover:underline cursor-pointer" data-goto-page="${entry.page_no}">${entry.space_id}:${entry.page_no}</a>`;
    }
    if (entry.page_no != null) return `-:${entry.page_no}`;
    return '-';
  }

  function formatAction(action) {
    if (!action) return '';
    switch (action.type) {
      case 'Redo':
        return action.mlog_type || '';
      case 'Undo':
        return `trx=${action.trx_id} ${action.record_type || ''}`;
      case 'Binlog': {
        let s = action.event_type || '';
        if (action.database && action.table) s += ` ${action.database}.${action.table}`;
        if (action.xid) s += ` (xid=${action.xid})`;
        if (action.pk_values && action.pk_values.length > 0) s += ` PK=(${action.pk_values.join(', ')})`;
        return s;
      }
      default:
        return JSON.stringify(action);
    }
  }

  function renderPagination(total, totalPages) {
    if (totalPages <= 1) return '';
    const start = currentPage * PAGE_SIZE + 1;
    const end = Math.min((currentPage + 1) * PAGE_SIZE, total);
    return `
      <div class="flex items-center justify-between mt-3 text-xs text-gray-500">
        <span>Showing ${start}-${end} of ${total.toLocaleString()}</span>
        <div class="flex gap-2">
          <button id="tl-prev" class="px-2 py-1 rounded bg-gray-800 hover:bg-gray-700 disabled:opacity-40"
            ${currentPage === 0 ? 'disabled' : ''}>Prev</button>
          <button id="tl-next" class="px-2 py-1 rounded bg-gray-800 hover:bg-gray-700 disabled:opacity-40"
            ${currentPage >= totalPages - 1 ? 'disabled' : ''}>Next</button>
        </div>
      </div>
    `;
  }

  function wirePagination(el, total, totalPages) {
    const prev = el.querySelector('#tl-prev');
    const next = el.querySelector('#tl-next');
    if (prev) prev.addEventListener('click', () => {
      if (currentPage > 0) { currentPage--; renderResults(el); }
    });
    if (next) next.addEventListener('click', () => {
      if (currentPage < totalPages - 1) { currentPage++; renderResults(el); }
    });
  }

  function wirePageLinks(el) {
    el.querySelectorAll('[data-goto-page]').forEach(link => {
      link.addEventListener('click', (e) => {
        e.preventDefault();
        const pageNo = parseInt(link.dataset.gotoPage, 10);
        trackFeatureUse('timeline_page_nav');
        requestPage(pageNo);
        navigateToTab('pages');
      });
    });
  }

  function renderPageSummaries(summaries) {
    if (!summaries || summaries.length === 0) return '';
    return `
      <div class="mt-6">
        <h3 class="text-sm font-semibold text-gray-300 mb-2">Page Summary</h3>
        <div class="overflow-x-auto">
          <table class="w-full text-xs font-mono">
            <thead class="sticky top-0 bg-gray-950">
              <tr class="text-left text-gray-500 border-b border-gray-800">
                <th class="px-2 py-1.5">SPACE:PAGE</th>
                <th class="px-2 py-1.5 text-right">REDO</th>
                <th class="px-2 py-1.5 text-right">UNDO</th>
                <th class="px-2 py-1.5 text-right">BINLOG</th>
                <th class="px-2 py-1.5">FIRST LSN</th>
                <th class="px-2 py-1.5">LAST LSN</th>
              </tr>
            </thead>
            <tbody>
              ${summaries.slice(0, 100).map(s => `
                <tr class="border-b border-gray-800/30 hover:bg-surface-2/50">
                  <td class="px-2 py-1 text-innodb-cyan">${s.space_id}:${s.page_no}</td>
                  <td class="px-2 py-1 text-right text-blue-400">${s.redo_entries}</td>
                  <td class="px-2 py-1 text-right text-yellow-400">${s.undo_entries}</td>
                  <td class="px-2 py-1 text-right text-green-400">${s.binlog_entries}</td>
                  <td class="px-2 py-1 text-gray-400">${s.first_lsn ?? '-'}</td>
                  <td class="px-2 py-1 text-gray-400">${s.last_lsn ?? '-'}</td>
                </tr>
              `).join('')}
            </tbody>
          </table>
        </div>
      </div>
    `;
  }
}
