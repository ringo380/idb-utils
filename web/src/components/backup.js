// Backup tab — LSN-based backup delta detection (mirrors `inno backup diff`)
import { getWasm } from '../wasm.js';
import { esc } from '../utils/html.js';
import { createExportBar } from '../utils/export.js';
import { trackFeatureUse } from '../utils/analytics.js';

/**
 * Create the backup delta tab for two tablespace files.
 * @param {HTMLElement} container
 * @param {Uint8Array} data1 - Base/backup tablespace data
 * @param {Uint8Array} data2 - Current/live tablespace data
 * @param {string} name1 - Base filename
 * @param {string} name2 - Current filename
 */
export function createBackup(container, data1, data2, name1, name2) {
  const wasm = getWasm();
  let report;
  try {
    report = JSON.parse(wasm.diff_backup_lsn(data1, data2));
  } catch (e) {
    container.innerHTML = `<div class="p-6 text-red-400">Error analyzing backup delta: ${esc(String(e))}</div>`;
    return;
  }

  trackFeatureUse('backup');

  const s = report.summary || {};
  const total = Math.max(report.base_page_count || 0, report.current_page_count || 0);
  const lsnAdvance = (report.current_max_lsn || 0) - (report.base_max_lsn || 0);
  const modTypes = report.modified_page_types || {};

  container.innerHTML = `
    <div class="p-6 space-y-6 overflow-auto max-h-full">
      <div class="flex items-center gap-3">
        <h2 class="text-lg font-bold text-innodb-cyan">Backup Delta</h2>
        <span id="backup-export"></span>
      </div>

      <div class="p-4 rounded-lg bg-gray-900/50 border border-gray-800 text-sm space-y-1">
        <div class="text-gray-400">Base: <span class="text-gray-200">${esc(name1)}</span></div>
        <div class="text-gray-400">Current: <span class="text-gray-200">${esc(name2)}</span></div>
        <div class="text-gray-400">Space ID: <span class="text-gray-200">${report.space_id ?? '-'}</span></div>
      </div>

      <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
        ${statCard('Total Pages', total)}
        ${statCard('Base Max LSN', report.base_max_lsn ?? 0)}
        ${statCard('Current Max LSN', report.current_max_lsn ?? 0)}
        ${statCard('LSN Advance', lsnAdvance > 0 ? '+' + lsnAdvance.toLocaleString() : lsnAdvance.toLocaleString())}
      </div>

      <h3 class="text-md font-semibold text-gray-300">Page Status</h3>
      <div class="grid grid-cols-2 md:grid-cols-5 gap-3">
        ${statusCard('Unchanged', s.unchanged ?? 0, total, 'text-innodb-green')}
        ${statusCard('Modified', s.modified ?? 0, total, 'text-innodb-amber')}
        ${statusCard('Added', s.added ?? 0, total, 'text-innodb-cyan')}
        ${statusCard('Removed', s.removed ?? 0, total, 'text-innodb-red')}
        ${statusCard('Regressed', s.regressed ?? 0, total, 'text-gray-500')}
      </div>

      ${Object.keys(modTypes).length > 0 ? `
        <h3 class="text-md font-semibold text-gray-300">Modified Pages by Type</h3>
        <div class="overflow-x-auto" id="backup-types"></div>
      ` : ''}
    </div>
  `;

  // Export bar
  const exportSlot = container.querySelector('#backup-export');
  if (exportSlot) {
    exportSlot.appendChild(createExportBar(() => report, 'backup'));
  }

  // Modified page types table
  const typesWrap = container.querySelector('#backup-types');
  if (typesWrap && Object.keys(modTypes).length > 0) {
    const sorted = Object.entries(modTypes).sort((a, b) => b[1] - a[1]);
    let rows = '';
    for (const [type, count] of sorted) {
      const pct = total > 0 ? ((count / total) * 100).toFixed(1) : '0.0';
      rows += `
        <tr class="border-b border-gray-800">
          <td class="px-3 py-2 text-gray-300">${esc(type)}</td>
          <td class="px-3 py-2 text-right text-gray-200">${count.toLocaleString()}</td>
          <td class="px-3 py-2 text-right text-gray-500">${pct}%</td>
          <td class="px-3 py-2">
            <div class="w-full bg-gray-800 rounded h-2">
              <div class="bg-innodb-amber rounded h-2" style="width: ${Math.min(100, (count / (s.modified || 1)) * 100)}%"></div>
            </div>
          </td>
        </tr>`;
    }
    typesWrap.innerHTML = `
      <table class="w-full text-sm">
        <thead>
          <tr class="text-left text-gray-500 text-xs uppercase border-b border-gray-700">
            <th class="px-3 py-2">Page Type</th>
            <th class="px-3 py-2 text-right">Count</th>
            <th class="px-3 py-2 text-right">% of Total</th>
            <th class="px-3 py-2">Distribution</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>`;
  }
}

// ---------------------------------------------------------------------------
// Rendering helpers
// ---------------------------------------------------------------------------

function statCard(label, value) {
  const display = typeof value === 'number' ? value.toLocaleString() : esc(String(value));
  return `
    <div class="p-3 rounded-lg bg-gray-900/50 border border-gray-800">
      <div class="text-xs text-gray-500 mb-1">${esc(label)}</div>
      <div class="text-lg font-bold text-gray-200">${display}</div>
    </div>`;
}

function statusCard(label, count, total, colorClass) {
  const pct = total > 0 ? ((count / total) * 100).toFixed(1) : '0.0';
  return `
    <div class="p-3 rounded-lg bg-gray-900/50 border border-gray-800">
      <div class="text-xs text-gray-500 mb-1">${esc(label)}</div>
      <div class="text-lg font-bold ${colorClass}">${count.toLocaleString()}</div>
      <div class="text-xs text-gray-600">${pct}%</div>
    </div>`;
}
