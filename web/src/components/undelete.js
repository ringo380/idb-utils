// Undelete tab — recover deleted records from tablespace
import { getWasm } from '../wasm.js';
import { esc } from '../utils/html.js';
import { createExportBar, downloadJson, downloadText } from '../utils/export.js';
import { requestPage, navigateToTab } from '../utils/navigation.js';
import { trackFeatureUse, trackExport } from '../utils/analytics.js';

/**
 * Create the undelete tab for a single tablespace file.
 * @param {HTMLElement} container
 * @param {Uint8Array} fileData
 */
export function createUndelete(container, fileData) {
  const wasm = getWasm();
  let result;
  try {
    const raw = wasm.scan_deleted_records(fileData, -1n);
    if (raw === 'null') {
      container.innerHTML = `<div class="p-6 text-gray-500">No SDI metadata found (pre-8.0 tablespace). Undelete requires MySQL 8.0+ tablespaces.</div>`;
      return;
    }
    result = JSON.parse(raw);
  } catch (e) {
    container.innerHTML = `<div class="p-6 text-red-400">Error scanning for deleted records: ${esc(String(e))}</div>`;
    return;
  }

  const summary = result.summary || {};
  const records = result.records || [];
  const columns = result.columns || [];
  let minConfidence = 0;

  container.innerHTML = `
    <div class="p-6 space-y-6 overflow-auto max-h-full">
      <div class="flex items-center gap-3">
        <h2 class="text-lg font-bold text-innodb-cyan">Undelete</h2>
        <span id="undelete-export"></span>
      </div>

      <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
        ${statCard('Total Recovered', summary.total ?? 0)}
        ${statCard('Delete-Marked', summary.delete_marked ?? 0, 'text-innodb-green')}
        ${statCard('Free List', summary.free_list ?? 0, 'text-innodb-amber')}
        ${statCard('Columns', columns.length)}
      </div>

      ${records.length > 0 ? `
        <div class="flex items-center gap-4">
          <label class="text-sm text-gray-400">Min Confidence:</label>
          <input type="range" id="undelete-conf-slider" min="0" max="100" value="0" class="w-48 accent-cyan-500">
          <span id="undelete-conf-label" class="text-sm text-gray-300 w-12">0.00</span>
        </div>
        <div id="undelete-table-wrap" class="overflow-x-auto max-h-[28rem]"></div>
        <div id="undelete-download-bar" class="flex items-center gap-2"></div>
      ` : '<div class="text-gray-500">No deleted records found.</div>'}
    </div>
  `;

  // Export bar
  const exportSlot = container.querySelector('#undelete-export');
  if (exportSlot) {
    exportSlot.appendChild(createExportBar(() => result, 'undelete'));
  }

  if (records.length === 0) return;

  const slider = container.querySelector('#undelete-conf-slider');
  const confLabel = container.querySelector('#undelete-conf-label');
  const tableWrap = container.querySelector('#undelete-table-wrap');
  const downloadBar = container.querySelector('#undelete-download-bar');

  function renderTable() {
    const filtered = records.filter(r => r.confidence >= minConfidence);
    if (filtered.length === 0) {
      tableWrap.innerHTML = '<div class="text-gray-500 py-4">No records match the confidence filter.</div>';
      return;
    }

    const headerCells = [
      '<th class="px-3 py-2 text-left text-xs font-medium text-gray-400 uppercase tracking-wider">Source</th>',
      '<th class="px-3 py-2 text-left text-xs font-medium text-gray-400 uppercase tracking-wider">Confidence</th>',
      '<th class="px-3 py-2 text-left text-xs font-medium text-gray-400 uppercase tracking-wider">TRX ID</th>',
      '<th class="px-3 py-2 text-left text-xs font-medium text-gray-400 uppercase tracking-wider">Page</th>',
    ];
    for (const col of columns) {
      headerCells.push(`<th class="px-3 py-2 text-left text-xs font-medium text-gray-400 uppercase tracking-wider">${esc(col)}</th>`);
    }

    const rows = filtered.map(rec => {
      const confClass = rec.confidence >= 0.8 ? 'text-innodb-green' :
                        rec.confidence >= 0.4 ? 'text-innodb-amber' : 'text-innodb-red';
      const sourceLabel = rec.source === 'delete_marked' ? 'Delete-Marked' :
                          rec.source === 'free_list' ? 'Free List' : 'Undo Log';
      const sourceBg = rec.source === 'delete_marked' ? 'bg-green-900/30' :
                       rec.source === 'free_list' ? 'bg-yellow-900/30' : 'bg-red-900/30';

      let cells = `<td class="px-3 py-1.5 text-xs whitespace-nowrap"><span class="px-1.5 py-0.5 rounded ${sourceBg}">${esc(sourceLabel)}</span></td>`;
      cells += `<td class="px-3 py-1.5 text-xs ${confClass} font-mono">${rec.confidence.toFixed(2)}</td>`;
      cells += `<td class="px-3 py-1.5 text-xs text-gray-300 font-mono">${rec.trx_id != null ? rec.trx_id : '-'}</td>`;
      cells += `<td class="px-3 py-1.5 text-xs text-innodb-cyan font-mono cursor-pointer hover:underline" data-page="${rec.page_number}">${rec.page_number}</td>`;

      const values = rec.values || [];
      for (let i = 0; i < columns.length; i++) {
        const val = i < values.length ? values[i] : null;
        const display = val === null ? '<span class="text-gray-600">NULL</span>' : esc(String(val));
        cells += `<td class="px-3 py-1.5 text-xs text-gray-300 font-mono max-w-[200px] truncate" title="${esc(String(val ?? ''))}">${display}</td>`;
      }

      return `<tr class="border-b border-gray-800 hover:bg-gray-800/50">${cells}</tr>`;
    });

    tableWrap.innerHTML = `
      <table class="min-w-full divide-y divide-gray-800">
        <thead class="bg-gray-900 sticky top-0"><tr>${headerCells.join('')}</tr></thead>
        <tbody>${rows.join('')}</tbody>
      </table>
    `;

    // Click page number to navigate
    tableWrap.querySelectorAll('[data-page]').forEach(el => {
      el.addEventListener('click', () => {
        const pn = parseInt(el.dataset.page, 10);
        requestPage(pn);
        navigateToTab('pages');
      });
    });
  }

  // Confidence slider
  slider.addEventListener('input', () => {
    minConfidence = parseInt(slider.value, 10) / 100;
    confLabel.textContent = minConfidence.toFixed(2);
    trackFeatureUse('undelete_confidence', { value: minConfidence });
    renderTable();
  });

  // Download buttons
  addDownloadButtons(downloadBar, result, columns);

  renderTable();
}

function statCard(label, value, colorClass = '') {
  return `
    <div class="bg-surface-2 rounded-lg p-4">
      <div class="text-xs text-gray-500 uppercase tracking-wider">${esc(label)}</div>
      <div class="text-2xl font-bold ${colorClass || 'text-gray-200'} mt-1">${value}</div>
    </div>
  `;
}

function addDownloadButtons(bar, result, columns) {
  // CSV download
  const csvBtn = document.createElement('button');
  csvBtn.className = 'px-3 py-1.5 bg-surface-3 hover:bg-gray-600 text-gray-300 rounded text-xs';
  csvBtn.textContent = 'Download CSV';
  csvBtn.addEventListener('click', () => {
    const header = ['_source', '_confidence', '_trx_id', '_page', ...columns];
    const rows = result.records.map(r => {
      const src = r.source;
      const vals = (r.values || []).map(v => v === null ? '' : String(v));
      return [src, r.confidence.toFixed(2), r.trx_id ?? '', r.page_number, ...vals];
    });
    const csvLines = [header, ...rows].map(row =>
      row.map(v => {
        const s = String(v);
        return s.includes(',') || s.includes('"') || s.includes('\n')
          ? '"' + s.replace(/"/g, '""') + '"' : s;
      }).join(',')
    );
    trackExport('csv', 'undelete');
    downloadText(csvLines.join('\n'), 'undelete.csv');
  });

  // JSON download
  const jsonBtn = document.createElement('button');
  jsonBtn.className = 'px-3 py-1.5 bg-surface-3 hover:bg-gray-600 text-gray-300 rounded text-xs';
  jsonBtn.textContent = 'Download JSON';
  jsonBtn.addEventListener('click', () => {
    trackExport('json', 'undelete');
    downloadJson(result, 'undelete');
  });

  // SQL download
  const sqlBtn = document.createElement('button');
  sqlBtn.className = 'px-3 py-1.5 bg-surface-3 hover:bg-gray-600 text-gray-300 rounded text-xs';
  sqlBtn.textContent = 'Download SQL';
  sqlBtn.addEventListener('click', () => {
    trackExport('sql', 'undelete');
    const tableName = result.table_name || 'unknown_table';
    const colList = columns.join(', ');
    const lines = result.records.map(r => {
      const vals = (r.values || []).map(v => {
        if (v === null) return 'NULL';
        if (typeof v === 'number') return String(v);
        return `'${String(v).replace(/'/g, "''")}'`;
      });
      return `-- source: ${r.source}, confidence: ${r.confidence.toFixed(2)}, page: ${r.page_number}\nINSERT INTO ${tableName} (${colList}) VALUES (${vals.join(', ')});`;
    });
    const blob = new Blob([lines.join('\n')], { type: 'text/sql' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `undelete_${tableName}.sql`;
    a.click();
    URL.revokeObjectURL(url);
  });

  bar.appendChild(csvBtn);
  bar.appendChild(jsonBtn);
  bar.appendChild(sqlBtn);
}
