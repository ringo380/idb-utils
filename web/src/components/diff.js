// Two-file diff view — mirrors `inno diff`
import { getWasm } from '../wasm.js';
import { esc } from '../utils/html.js';
import { createExportBar } from '../utils/export.js';

export function createDiff(container, fileName1, fileData1, fileName2, fileData2) {
  const wasm = getWasm();
  let result;
  try {
    result = JSON.parse(wasm.diff_tablespaces(fileData1, fileData2));
  } catch (e) {
    container.innerHTML = `<div class="p-6 text-red-400">Error comparing files: ${esc(e)}</div>`;
    return;
  }

  const totalCompared = result.identical + result.modified + result.only_in_first + result.only_in_second;
  const identPct = totalCompared > 0 ? ((result.identical / totalCompared) * 100).toFixed(1) : '0';

  container.innerHTML = `
    <div class="p-6 space-y-6 overflow-auto max-h-full">
      <div class="flex items-center gap-3">
        <h2 class="text-lg font-bold text-innodb-cyan">Tablespace Diff</h2>
        <span id="diff-export"></span>
      </div>

      <div class="grid grid-cols-2 gap-4">
        <div class="bg-surface-2 rounded-lg p-4">
          <div class="text-xs text-gray-500 uppercase tracking-wide">File 1</div>
          <div class="text-sm font-bold text-gray-100 mt-1 truncate">${esc(fileName1)}</div>
          <div class="text-xs text-gray-500 mt-1">${result.page_count_1} pages, ${fmtSize(result.page_size_1)} page size</div>
        </div>
        <div class="bg-surface-2 rounded-lg p-4">
          <div class="text-xs text-gray-500 uppercase tracking-wide">File 2</div>
          <div class="text-sm font-bold text-gray-100 mt-1 truncate">${esc(fileName2)}</div>
          <div class="text-xs text-gray-500 mt-1">${result.page_count_2} pages, ${fmtSize(result.page_size_2)} page size</div>
        </div>
      </div>

      <div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
        ${statCard('Identical', result.identical, 'text-innodb-green')}
        ${statCard('Modified', result.modified, result.modified > 0 ? 'text-innodb-amber' : '')}
        ${statCard('Only in File 1', result.only_in_first, result.only_in_first > 0 ? 'text-innodb-red' : '')}
        ${statCard('Only in File 2', result.only_in_second, result.only_in_second > 0 ? 'text-innodb-red' : '')}
      </div>

      <div class="bg-surface-2 rounded-lg p-4">
        <div class="flex items-center gap-2 mb-2">
          <span class="text-sm text-gray-400">Similarity</span>
          <span class="text-sm font-bold text-innodb-green">${identPct}%</span>
        </div>
        <div class="w-full bg-gray-800 rounded-full h-3 overflow-hidden">
          <div class="bg-innodb-green h-full rounded-full" style="width:${identPct}%"></div>
        </div>
      </div>

      ${result.modified_pages.length > 0 ? `
        <h3 class="text-md font-semibold text-gray-300">Modified Pages</h3>
        <div class="overflow-x-auto max-h-96">
          <table class="w-full text-xs font-mono">
            <thead class="sticky top-0 bg-gray-950">
              <tr class="text-left text-gray-500 border-b border-gray-800">
                <th class="py-1 pr-3">#</th>
                <th class="py-1 pr-3">Type (File 1)</th>
                <th class="py-1 pr-3">Type (File 2)</th>
                <th class="py-1 pr-3">LSN 1</th>
                <th class="py-1 pr-3">LSN 2</th>
                <th class="py-1 pr-3">Bytes Changed</th>
              </tr>
            </thead>
            <tbody>
              ${result.modified_pages.map(modRow).join('')}
            </tbody>
          </table>
        </div>
      ` : `
        <div class="bg-surface-2 rounded-lg p-6 text-center text-gray-500">
          No modified pages — files are identical.
        </div>
      `}
    </div>
  `;

  const exportSlot = container.querySelector('#diff-export');
  if (exportSlot) {
    exportSlot.appendChild(createExportBar(() => result, 'diff'));
  }
}

function modRow(p) {
  return `
    <tr class="border-b border-gray-800/30 hover:bg-surface-2/50">
      <td class="py-1 pr-3 text-gray-400">${p.page_number}</td>
      <td class="py-1 pr-3 text-innodb-cyan">0x${p.header_1.page_type.toString(16)}</td>
      <td class="py-1 pr-3 text-innodb-cyan">0x${p.header_2.page_type.toString(16)}</td>
      <td class="py-1 pr-3">${p.header_1.lsn}</td>
      <td class="py-1 pr-3">${p.header_2.lsn}</td>
      <td class="py-1 pr-3 text-innodb-amber">${p.bytes_changed.toLocaleString()}</td>
    </tr>`;
}

function statCard(label, value, colorClass = '') {
  return `
    <div class="bg-surface-2 rounded-lg p-4">
      <div class="text-xs text-gray-500 uppercase tracking-wide">${esc(label)}</div>
      <div class="text-lg font-bold ${colorClass || 'text-gray-100'} mt-1">${value}</div>
    </div>`;
}

function fmtSize(bytes) {
  if (bytes >= 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  if (bytes >= 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${bytes} B`;
}

