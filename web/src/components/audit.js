// Audit dashboard — multi-file integrity and health analysis
import { getWasm } from '../wasm.js';
import { esc } from '../utils/html.js';
import { createExportBar } from '../utils/export.js';
import { renderIndexTable } from '../utils/health-ui.js';

/**
 * Create the audit dashboard, analyzing multiple .ibd files.
 * @param {HTMLElement} container
 * @param {Array<{name: string, data: Uint8Array}>} files
 */
export function createAudit(container, files) {
  // Show processing spinner while analyzing files
  container.innerHTML = `
    <div class="flex-1 flex items-center justify-center p-12">
      <div class="text-center">
        <div class="inline-block w-8 h-8 border-2 border-innodb-cyan border-t-transparent rounded-full animate-spin mb-4"></div>
        <p class="text-gray-400">Analyzing ${files.length} files\u2026</p>
      </div>
    </div>`;

  // Defer analysis to next frame so spinner renders
  requestAnimationFrame(() => {
    buildAuditDashboard(container, files);
  });
}

function buildAuditDashboard(container, files) {
  const wasm = getWasm();
  const results = [];

  for (const file of files) {
    const entry = { name: file.name, error: null, checksums: null, health: null };
    try {
      entry.checksums = JSON.parse(wasm.validate_checksums(file.data));
    } catch (e) {
      entry.error = String(e);
    }
    try {
      entry.health = JSON.parse(wasm.analyze_health(file.data));
    } catch {
      // health analysis optional — may fail for non-tablespace files
    }
    results.push(entry);
  }

  // Aggregate summary
  let totalFiles = results.length;
  let totalPages = 0;
  let totalCorrupt = 0;
  let totalValid = 0;
  let totalEmpty = 0;
  let fillFactors = [];
  let fragValues = [];

  for (const r of results) {
    if (r.checksums) {
      totalPages += r.checksums.total_pages;
      totalCorrupt += r.checksums.invalid_pages;
      totalValid += r.checksums.valid_pages;
      totalEmpty += r.checksums.empty_pages;
    }
    if (r.health) {
      fillFactors.push(r.health.summary.avg_fill_factor);
      fragValues.push(r.health.summary.avg_fragmentation);
    }
  }

  const nonEmpty = totalPages - totalEmpty;
  const integrityPct = nonEmpty > 0 ? Math.min(((nonEmpty - totalCorrupt) / nonEmpty) * 100, 100).toFixed(1) : '100.0';
  const avgFill = fillFactors.length > 0 ? (fillFactors.reduce((a, b) => a + b, 0) / fillFactors.length * 100).toFixed(1) : 'N/A';
  const avgFrag = fragValues.length > 0 ? (fragValues.reduce((a, b) => a + b, 0) / fragValues.length * 100).toFixed(1) : 'N/A';

  // Sort and filter state
  let sortCol = null;
  let sortAsc = true;
  let filterName = '';
  let filterStatus = '';

  container.innerHTML = `
    <div class="p-6 space-y-6 overflow-auto max-h-full">
      <div class="flex items-center gap-3">
        <h2 class="text-lg font-bold text-innodb-cyan">Audit Dashboard</h2>
        <span class="text-xs text-gray-500">${totalFiles} file${totalFiles !== 1 ? 's' : ''}</span>
        <span id="audit-export"></span>
      </div>

      <div class="grid grid-cols-2 md:grid-cols-5 gap-4">
        ${statCard('Total Files', totalFiles)}
        ${statCard('Total Pages', totalPages)}
        ${statCard('Corrupt Pages', totalCorrupt, totalCorrupt > 0 ? 'text-innodb-red' : '')}
        ${statCard('Integrity', integrityPct + '%', parseFloat(integrityPct) >= 100 ? 'text-innodb-green' : parseFloat(integrityPct) >= 95 ? 'text-innodb-amber' : 'text-innodb-red')}
        ${statCard('Avg Fill Factor', avgFill !== 'N/A' ? avgFill + '%' : avgFill, avgFill !== 'N/A' ? 'text-innodb-cyan' : '')}
      </div>

      <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
        <div class="bg-surface-2 rounded-lg p-4">
          <div class="flex items-center gap-2 mb-2">
            <span class="text-sm text-gray-400">Overall Integrity</span>
            <span class="text-sm font-bold ${parseFloat(integrityPct) >= 100 ? 'text-innodb-green' : parseFloat(integrityPct) >= 95 ? 'text-innodb-amber' : 'text-innodb-red'}">${integrityPct}%</span>
          </div>
          <div class="w-full bg-gray-800 rounded-full h-3 overflow-hidden">
            <div class="h-full rounded-full ${parseFloat(integrityPct) >= 100 ? 'bg-innodb-green' : parseFloat(integrityPct) >= 95 ? 'bg-innodb-amber' : 'bg-innodb-red'}" style="width:${Math.min(parseFloat(integrityPct), 100)}%"></div>
          </div>
        </div>
        <div class="bg-surface-2 rounded-lg p-4">
          <div class="flex items-center gap-2 mb-2">
            <span class="text-sm text-gray-400">Avg Fragmentation</span>
            <span class="text-sm font-bold text-gray-300">${avgFrag !== 'N/A' ? avgFrag + '%' : avgFrag}</span>
          </div>
          <div class="w-full bg-gray-800 rounded-full h-3 overflow-hidden">
            <div class="h-full rounded-full bg-innodb-amber" style="width:${avgFrag !== 'N/A' ? Math.min(parseFloat(avgFrag), 100) : 0}%"></div>
          </div>
        </div>
      </div>

      <h3 class="text-md font-semibold text-gray-300">Per-File Details</h3>
      <div class="flex items-center gap-3">
        <input id="audit-filter-name" type="text" placeholder="Filter by file name\u2026"
          class="px-2 py-1 bg-surface-3 border border-gray-700 rounded text-xs text-gray-300 placeholder-gray-600 w-48 focus:outline-none focus:border-innodb-cyan" />
        <select id="audit-filter-status"
          class="px-2 py-1 bg-surface-3 border border-gray-700 rounded text-xs text-gray-300 focus:outline-none focus:border-innodb-cyan">
          <option value="">All statuses</option>
          <option value="healthy">Healthy</option>
          <option value="warning">Warning</option>
          <option value="critical">Critical</option>
          <option value="error">Error</option>
        </select>
      </div>
      <div id="audit-table-wrap" class="overflow-x-auto max-h-96"></div>
    </div>
  `;

  // Export bar
  const exportSlot = container.querySelector('#audit-export');
  if (exportSlot) {
    const exportData = () => ({
      summary: { totalFiles, totalPages, totalCorrupt, totalValid, totalEmpty, integrityPct, avgFill, avgFrag },
      files: results.map((r) => ({
        name: r.name,
        error: r.error,
        pages: r.checksums?.total_pages ?? 0,
        corrupt: r.checksums?.invalid_pages ?? 0,
        valid: r.checksums?.valid_pages ?? 0,
        empty: r.checksums?.empty_pages ?? 0,
        fill_factor: r.health?.summary.avg_fill_factor ?? null,
        fragmentation: r.health?.summary.avg_fragmentation ?? null,
        index_count: r.health?.summary.index_count ?? null,
      })),
    });
    exportSlot.appendChild(createExportBar(exportData, 'audit'));
  }

  // Build file rows (include indexes for expand/collapse)
  const fileRows = results.map((r) => {
    const pages = r.checksums?.total_pages ?? 0;
    const corrupt = r.checksums?.invalid_pages ?? 0;
    const empty = r.checksums?.empty_pages ?? 0;
    const nonEmpty = pages - empty;
    const corruptPct = nonEmpty > 0 ? (corrupt / nonEmpty) * 100 : 0;
    const fill = r.health?.summary.avg_fill_factor ?? null;
    const frag = r.health?.summary.avg_fragmentation ?? null;
    const indexCount = r.health?.summary.index_count ?? null;
    const indexes = r.health?.indexes ?? null;
    return { name: r.name, pages, corrupt, corruptPct, fill, frag, indexCount, indexes, error: r.error };
  });
  const expandedNames = new Set();

  function getRowStatus(r) {
    if (r.error) return 'error';
    if (r.corruptPct === 0) return 'healthy';
    if (r.corruptPct < 5) return 'warning';
    return 'critical';
  }

  function renderTable() {
    let sorted = fileRows.filter((r) => {
      if (filterName && !r.name.toLowerCase().includes(filterName.toLowerCase())) return false;
      if (filterStatus && getRowStatus(r) !== filterStatus) return false;
      return true;
    });
    sorted = [...sorted];
    if (sortCol !== null) {
      sorted.sort((a, b) => {
        let va = a[sortCol];
        let vb = b[sortCol];
        if (va == null) va = -Infinity;
        if (vb == null) vb = -Infinity;
        if (typeof va === 'string') return sortAsc ? va.localeCompare(vb) : vb.localeCompare(va);
        return sortAsc ? va - vb : vb - va;
      });
    }

    const columns = [
      { key: 'name', label: 'File' },
      { key: 'pages', label: 'Pages' },
      { key: 'corrupt', label: 'Corrupt' },
      { key: 'corruptPct', label: 'Status' },
      { key: 'fill', label: 'Fill Factor' },
      { key: 'frag', label: 'Fragmentation' },
      { key: 'indexCount', label: 'Indexes' },
    ];

    const colCount = columns.length;
    const wrap = container.querySelector('#audit-table-wrap');
    wrap.innerHTML = `
      <table class="w-full text-xs font-mono">
        <thead class="sticky top-0 bg-gray-950">
          <tr class="text-left text-gray-500 border-b border-gray-800">
            ${columns.map((c) => `<th scope="col" class="py-1 pr-3 cursor-pointer hover:text-gray-300 select-none" data-sort="${c.key}">${c.label}${sortCol === c.key ? (sortAsc ? ' &#9650;' : ' &#9660;') : ''}</th>`).join('')}
          </tr>
        </thead>
        <tbody>
          ${sorted.map((r) => fileRow(r, colCount, expandedNames)).join('')}
        </tbody>
      </table>`;

    // Attach sort handlers
    wrap.querySelectorAll('th[data-sort]').forEach((th) => {
      th.addEventListener('click', () => {
        const col = th.dataset.sort;
        if (sortCol === col) {
          sortAsc = !sortAsc;
        } else {
          sortCol = col;
          sortAsc = true;
        }
        renderTable();
      });
    });

    // Attach expand/collapse handlers
    wrap.querySelectorAll('.audit-expand-toggle').forEach((btn) => {
      btn.addEventListener('click', (e) => {
        e.stopPropagation();
        const rowName = btn.dataset.rowName;
        const detailRow = wrap.querySelector(`[data-detail-name="${CSS.escape(rowName)}"]`);
        if (!detailRow) return;
        const isExpanded = !detailRow.classList.contains('hidden');
        if (isExpanded) {
          detailRow.classList.add('hidden');
          expandedNames.delete(rowName);
          btn.innerHTML = expandArrow(false);
        } else {
          detailRow.classList.remove('hidden');
          expandedNames.add(rowName);
          btn.innerHTML = expandArrow(true);
        }
      });
    });
  }

  renderTable();

  // Wire filter inputs
  const nameInput = container.querySelector('#audit-filter-name');
  const statusSelect = container.querySelector('#audit-filter-status');
  if (nameInput) {
    nameInput.addEventListener('input', () => {
      filterName = nameInput.value;
      renderTable();
    });
  }
  if (statusSelect) {
    statusSelect.addEventListener('change', () => {
      filterStatus = statusSelect.value;
      renderTable();
    });
  }
}

function expandArrow(expanded) {
  return expanded
    ? '<svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" /></svg>'
    : '<svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7" /></svg>';
}

function fileRow(r, colCount, expandedNames) {
  let statusClass, statusText;
  if (r.error) {
    statusClass = 'text-gray-500';
    statusText = 'error';
  } else if (r.corruptPct === 0) {
    statusClass = 'text-innodb-green';
    statusText = 'healthy';
  } else if (r.corruptPct < 5) {
    statusClass = 'text-innodb-amber';
    statusText = 'warning';
  } else {
    statusClass = 'text-innodb-red';
    statusText = 'critical';
  }

  const dot = r.error ? 'bg-gray-500' : r.corruptPct === 0 ? 'bg-innodb-green' : r.corruptPct < 5 ? 'bg-innodb-amber' : 'bg-innodb-red';
  const hasIndexes = r.indexes && r.indexes.length > 0;

  const isExpanded = hasIndexes && expandedNames.has(r.name);
  let html = `
    <tr class="border-b border-gray-800/30 hover:bg-surface-2/50">
      <td class="py-1 pr-3 text-gray-300">
        <span class="inline-flex items-center gap-1.5">
          ${hasIndexes ? `<button class="audit-expand-toggle text-gray-500 hover:text-gray-300" data-row-name="${esc(r.name)}">${expandArrow(isExpanded)}</button>` : '<span class="w-3.5"></span>'}
          ${esc(r.name)}
        </span>
      </td>
      <td class="py-1 pr-3 text-gray-400">${r.pages}</td>
      <td class="py-1 pr-3 ${r.corrupt > 0 ? 'text-innodb-red font-bold' : 'text-gray-400'}">${r.corrupt}</td>
      <td class="py-1 pr-3">
        <span class="inline-flex items-center gap-1.5">
          <span class="w-2 h-2 rounded-full ${dot}"></span>
          <span class="${statusClass} font-bold">${statusText}</span>
        </span>
      </td>
      <td class="py-1 pr-3 text-gray-400">${r.fill != null ? (r.fill * 100).toFixed(1) + '%' : '\u2014'}</td>
      <td class="py-1 pr-3 text-gray-400">${r.frag != null ? (r.frag * 100).toFixed(1) + '%' : '\u2014'}</td>
      <td class="py-1 pr-3 text-gray-400">${r.indexCount != null ? r.indexCount : '\u2014'}</td>
    </tr>`;

  if (hasIndexes) {
    html += `
      <tr data-detail-name="${esc(r.name)}" class="${isExpanded ? '' : 'hidden'}">
        <td colspan="${colCount}" class="py-2 px-4 bg-surface-3/30">
          <div class="text-xs text-gray-500 mb-1">Per-Index Health for ${esc(r.name)}</div>
          <div class="overflow-x-auto">${renderIndexTable(r.indexes)}</div>
        </td>
      </tr>`;
  }

  return html;
}

function statCard(label, value, colorClass = '') {
  return `
    <div class="bg-surface-2 rounded-lg p-4">
      <div class="text-xs text-gray-500 uppercase tracking-wide">${esc(label)}</div>
      <div class="text-lg font-bold ${colorClass || 'text-gray-100'} mt-1">${value}</div>
    </div>`;
}
