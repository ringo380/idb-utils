// Checksum validation — mirrors `inno checksum`
import { getWasm } from '../wasm.js';
import { esc } from '../utils/html.js';
import { createExportBar } from '../utils/export.js';

export function createChecksums(container, fileData) {
  const wasm = getWasm();
  let report;
  try {
    report = JSON.parse(wasm.validate_checksums(fileData));
  } catch (e) {
    container.innerHTML = `<div class="p-6 text-red-400">Error validating checksums: ${esc(e)}</div>`;
    return;
  }

  const total = report.total_pages;
  const validPct = total > 0 ? ((report.valid_pages / total) * 100).toFixed(1) : '0';
  const allValid = report.invalid_pages === 0;

  container.innerHTML = `
    <div class="p-6 space-y-6 overflow-auto max-h-full">
      <div class="flex items-center gap-3">
        <h2 class="text-lg font-bold text-innodb-cyan">Checksum Validation</h2>
        <span id="checksums-export"></span>
        <span class="${allValid ? 'badge-valid' : 'badge-invalid'} px-2 py-0.5 rounded text-xs font-bold">
          ${allValid ? 'ALL VALID' : `${report.invalid_pages} INVALID`}
        </span>
      </div>

      <div class="grid grid-cols-2 md:grid-cols-5 gap-4">
        ${statCard('Total Pages', total)}
        ${statCard('Valid', report.valid_pages, 'text-innodb-green')}
        ${statCard('Invalid', report.invalid_pages, report.invalid_pages > 0 ? 'text-innodb-red' : '')}
        ${statCard('Empty', report.empty_pages, 'text-gray-500')}
        ${statCard('LSN Mismatches', report.lsn_mismatches, report.lsn_mismatches > 0 ? 'text-innodb-amber' : '')}
      </div>

      <div class="bg-surface-2 rounded-lg p-4">
        <div class="flex items-center gap-2 mb-2">
          <span class="text-sm text-gray-400">Integrity</span>
          <span class="text-sm font-bold ${allValid ? 'text-innodb-green' : 'text-innodb-red'}">${validPct}%</span>
        </div>
        <div class="w-full bg-gray-800 rounded-full h-3 overflow-hidden">
          <div class="h-full rounded-full ${allValid ? 'bg-innodb-green' : 'bg-innodb-red'}" style="width:${validPct}%"></div>
        </div>
      </div>

      <div class="flex items-center gap-2 mb-2">
        <h3 class="text-md font-semibold text-gray-300">Per-Page Results</h3>
        <label class="text-xs text-gray-600 flex items-center gap-1">
          <input type="checkbox" id="show-invalid-only" class="rounded bg-gray-800 border-gray-600" />
          Show invalid only
        </label>
      </div>
      <div id="checksum-table-wrap" class="overflow-x-auto max-h-96">
        ${renderTable(report.pages, false)}
      </div>
    </div>
  `;

  const exportSlot = container.querySelector('#checksums-export');
  if (exportSlot) {
    exportSlot.appendChild(createExportBar(() => report, 'checksums'));
  }

  const checkbox = container.querySelector('#show-invalid-only');
  const wrap = container.querySelector('#checksum-table-wrap');
  checkbox.addEventListener('change', () => {
    wrap.innerHTML = renderTable(report.pages, checkbox.checked);
  });
}

function renderTable(pages, invalidOnly) {
  const filtered = invalidOnly ? pages.filter((p) => p.status !== 'valid') : pages;
  if (filtered.length === 0) {
    return `<div class="text-gray-500 text-sm py-4">No pages to display.</div>`;
  }
  return `
    <table class="w-full text-xs font-mono">
      <thead class="sticky top-0 bg-gray-950">
        <tr class="text-left text-gray-500 border-b border-gray-800">
          <th scope="col" class="py-1 pr-3">#</th>
          <th scope="col" class="py-1 pr-3">Status</th>
          <th scope="col" class="py-1 pr-3">Algorithm</th>
          <th scope="col" class="py-1 pr-3">Stored</th>
          <th scope="col" class="py-1 pr-3">Calculated</th>
          <th scope="col" class="py-1 pr-3">LSN</th>
        </tr>
      </thead>
      <tbody>
        ${filtered.map(checksumRow).join('')}
      </tbody>
    </table>`;
}

function checksumRow(p) {
  const statusClass = p.status === 'valid' ? 'text-innodb-green' : 'text-innodb-red';
  const lsnClass = p.lsn_valid ? 'text-innodb-green' : 'text-innodb-amber';
  return `
    <tr class="border-b border-gray-800/30 hover:bg-surface-2/50">
      <td class="py-1 pr-3 text-gray-400">${p.page_number}</td>
      <td class="py-1 pr-3 ${statusClass} font-bold">${p.status}</td>
      <td class="py-1 pr-3 text-gray-400">${esc(p.algorithm)}</td>
      <td class="py-1 pr-3">0x${p.stored_checksum.toString(16).padStart(8, '0')}</td>
      <td class="py-1 pr-3">0x${p.calculated_checksum.toString(16).padStart(8, '0')}</td>
      <td class="py-1 pr-3 ${lsnClass}">${p.lsn_valid ? 'OK' : 'MISMATCH'}</td>
    </tr>`;
}

function statCard(label, value, colorClass = '') {
  return `
    <div class="bg-surface-2 rounded-lg p-4">
      <div class="text-xs text-gray-500 uppercase tracking-wide">${esc(label)}</div>
      <div class="text-lg font-bold ${colorClass || 'text-gray-100'} mt-1">${value}</div>
    </div>`;
}

