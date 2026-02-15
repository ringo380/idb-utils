// Tablespace overview — mirrors `inno parse`
import { getWasm } from '../wasm.js';
import { esc } from '../utils/html.js';

export function createOverview(container, fileData) {
  const wasm = getWasm();
  let info, parsed;
  try {
    info = JSON.parse(wasm.get_tablespace_info(fileData));
    parsed = JSON.parse(wasm.parse_tablespace(fileData));
  } catch (e) {
    container.innerHTML = `<div class="p-6 text-red-400">Error parsing tablespace: ${esc(e)}</div>`;
    return;
  }

  const fmtSize = (bytes) => {
    if (bytes >= 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
    if (bytes >= 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${bytes} B`;
  };

  container.innerHTML = `
    <div class="p-6 space-y-6 overflow-auto max-h-full">
      <h2 class="text-lg font-bold text-innodb-cyan">Tablespace Overview</h2>
      <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
        ${statCard('File Size', fmtSize(info.file_size))}
        ${statCard('Page Size', fmtSize(info.page_size))}
        ${statCard('Pages', info.page_count.toLocaleString())}
        ${statCard('Vendor', info.vendor)}
        ${statCard('Space ID', info.space_id ?? 'N/A')}
        ${statCard('Encrypted', info.is_encrypted ? 'Yes' : 'No')}
        ${statCard('FSP Flags', info.fsp_flags != null ? `0x${info.fsp_flags.toString(16)}` : 'N/A')}
      </div>

      <h3 class="text-md font-semibold text-gray-300 mt-6">Page Type Distribution</h3>
      <div class="overflow-x-auto">
        <table class="w-full text-sm">
          <thead>
            <tr class="text-left text-gray-500 border-b border-gray-800">
              <th class="py-2 pr-4">Type</th>
              <th class="py-2 pr-4 text-right">Count</th>
              <th class="py-2 w-full">Distribution</th>
            </tr>
          </thead>
          <tbody>
            ${parsed.type_summary.map(t => typeRow(t, parsed.page_count)).join('')}
          </tbody>
        </table>
      </div>

      <h3 class="text-md font-semibold text-gray-300 mt-6">Page Headers</h3>
      <div class="overflow-x-auto max-h-96">
        <table class="w-full text-xs font-mono">
          <thead class="sticky top-0 bg-gray-950">
            <tr class="text-left text-gray-500 border-b border-gray-800">
              <th class="py-1 pr-3">#</th>
              <th class="py-1 pr-3">Type</th>
              <th class="py-1 pr-3">LSN</th>
              <th class="py-1 pr-3">Space ID</th>
              <th class="py-1 pr-3">Checksum</th>
              <th class="py-1 pr-3">Prev</th>
              <th class="py-1 pr-3">Next</th>
            </tr>
          </thead>
          <tbody>
            ${parsed.pages.map(pageRow).join('')}
          </tbody>
        </table>
      </div>
    </div>
  `;
}

function statCard(label, value) {
  return `
    <div class="bg-surface-2 rounded-lg p-4">
      <div class="text-xs text-gray-500 uppercase tracking-wide">${esc(label)}</div>
      <div class="text-lg font-bold text-gray-100 mt-1">${esc(String(value))}</div>
    </div>`;
}

function typeRow(t, total) {
  const pct = ((t.count / total) * 100).toFixed(1);
  return `
    <tr class="border-b border-gray-800/50 hover:bg-surface-2/50">
      <td class="py-1.5 pr-4 text-innodb-cyan">${esc(t.page_type)}</td>
      <td class="py-1.5 pr-4 text-right">${t.count}</td>
      <td class="py-1.5">
        <div class="flex items-center gap-2">
          <div class="flex-1 bg-gray-800 rounded-full h-2 overflow-hidden">
            <div class="bg-innodb-blue h-full rounded-full" style="width:${pct}%"></div>
          </div>
          <span class="text-gray-500 text-xs w-12 text-right">${pct}%</span>
        </div>
      </td>
    </tr>`;
}

function pageRow(p) {
  return `
    <tr class="border-b border-gray-800/30 hover:bg-surface-2/50">
      <td class="py-1 pr-3 text-gray-400">${p.page_number}</td>
      <td class="py-1 pr-3 text-innodb-cyan">${esc(p.page_type_name)}</td>
      <td class="py-1 pr-3">${p.lsn}</td>
      <td class="py-1 pr-3">${p.space_id}</td>
      <td class="py-1 pr-3 text-gray-500">0x${p.checksum.toString(16).padStart(8, '0')}</td>
      <td class="py-1 pr-3">${p.prev_page ?? '—'}</td>
      <td class="py-1 pr-3">${p.next_page ?? '—'}</td>
    </tr>`;
}

