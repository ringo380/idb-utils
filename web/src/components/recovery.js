// Recovery assessment — mirrors `inno recover`
import { getWasm } from '../wasm.js';
import { esc } from '../utils/html.js';

export function createRecovery(container, fileData) {
  const wasm = getWasm();
  let report;
  try {
    report = JSON.parse(wasm.assess_recovery(fileData));
  } catch (e) {
    container.innerHTML = `<div class="p-6 text-red-400">Error assessing recovery: ${esc(e)}</div>`;
    return;
  }

  const total = report.page_count;
  const intactPct = total > 0 ? ((report.summary.intact / total) * 100).toFixed(1) : '0';
  const corruptPct = total > 0 ? ((report.summary.corrupt / total) * 100).toFixed(1) : '0';

  container.innerHTML = `
    <div class="p-6 space-y-6 overflow-auto max-h-full">
      <h2 class="text-lg font-bold text-innodb-cyan">Recovery Assessment</h2>

      <div class="grid grid-cols-2 md:grid-cols-5 gap-4">
        ${statCard('Total Pages', total)}
        ${statCard('Intact', report.summary.intact, 'text-innodb-green')}
        ${statCard('Corrupt', report.summary.corrupt, report.summary.corrupt > 0 ? 'text-innodb-red' : '')}
        ${statCard('Empty', report.summary.empty, 'text-gray-500')}
        ${statCard('Recoverable Records', report.recoverable_records, 'text-innodb-cyan')}
      </div>

      <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
        <div class="bg-surface-2 rounded-lg p-4">
          <div class="flex items-center gap-2 mb-2">
            <span class="text-sm text-gray-400">Intact Pages</span>
            <span class="text-sm font-bold text-innodb-green">${intactPct}%</span>
          </div>
          <div class="w-full bg-gray-800 rounded-full h-3 overflow-hidden">
            <div class="bg-innodb-green h-full rounded-full" style="width:${intactPct}%"></div>
          </div>
        </div>
        <div class="bg-surface-2 rounded-lg p-4">
          <div class="flex items-center gap-2 mb-2">
            <span class="text-sm text-gray-400">Corrupt Pages</span>
            <span class="text-sm font-bold text-innodb-red">${corruptPct}%</span>
          </div>
          <div class="w-full bg-gray-800 rounded-full h-3 overflow-hidden">
            <div class="bg-innodb-red h-full rounded-full" style="width:${corruptPct}%"></div>
          </div>
        </div>
      </div>

      <div class="flex items-center gap-2 mb-2">
        <h3 class="text-md font-semibold text-gray-300">Per-Page Details</h3>
        <label class="text-xs text-gray-600 flex items-center gap-1">
          <input type="checkbox" id="recovery-show-corrupt" class="rounded bg-gray-800 border-gray-600" />
          Show corrupt only
        </label>
      </div>
      <div id="recovery-table-wrap" class="overflow-x-auto max-h-96">
        ${renderTable(report.pages, false)}
      </div>
    </div>
  `;

  const checkbox = container.querySelector('#recovery-show-corrupt');
  const wrap = container.querySelector('#recovery-table-wrap');
  checkbox.addEventListener('change', () => {
    wrap.innerHTML = renderTable(report.pages, checkbox.checked);
  });
}

function renderTable(pages, corruptOnly) {
  const filtered = corruptOnly ? pages.filter((p) => p.status === 'corrupt') : pages;
  if (filtered.length === 0) {
    return `<div class="text-gray-500 text-sm py-4">No pages to display.</div>`;
  }
  return `
    <table class="w-full text-xs font-mono">
      <thead class="sticky top-0 bg-gray-950">
        <tr class="text-left text-gray-500 border-b border-gray-800">
          <th class="py-1 pr-3">#</th>
          <th class="py-1 pr-3">Status</th>
          <th class="py-1 pr-3">Type</th>
          <th class="py-1 pr-3">Checksum</th>
          <th class="py-1 pr-3">LSN Valid</th>
          <th class="py-1 pr-3">LSN</th>
          <th class="py-1 pr-3">Records</th>
        </tr>
      </thead>
      <tbody>
        ${filtered.map(recRow).join('')}
      </tbody>
    </table>`;
}

function recRow(p) {
  const statusClass =
    p.status === 'intact' ? 'text-innodb-green' : p.status === 'corrupt' ? 'text-innodb-red' : 'text-gray-500';
  return `
    <tr class="border-b border-gray-800/30 hover:bg-surface-2/50">
      <td class="py-1 pr-3 text-gray-400">${p.page_number}</td>
      <td class="py-1 pr-3 ${statusClass} font-bold">${p.status}</td>
      <td class="py-1 pr-3 text-innodb-cyan">${esc(p.page_type)}</td>
      <td class="py-1 pr-3">${p.checksum_valid ? 'OK' : 'FAIL'}</td>
      <td class="py-1 pr-3">${p.lsn_valid ? 'OK' : 'FAIL'}</td>
      <td class="py-1 pr-3">${p.lsn}</td>
      <td class="py-1 pr-3">${p.record_count != null ? p.record_count : '—'}</td>
    </tr>`;
}

function statCard(label, value, colorClass = '') {
  return `
    <div class="bg-surface-2 rounded-lg p-4">
      <div class="text-xs text-gray-500 uppercase tracking-wide">${esc(label)}</div>
      <div class="text-lg font-bold ${colorClass || 'text-gray-100'} mt-1">${value}</div>
    </div>`;
}

