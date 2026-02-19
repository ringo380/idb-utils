// Redo log analysis — mirrors `inno log`
import { getWasm } from '../wasm.js';
import { esc } from '../utils/html.js';
import { createExportBar } from '../utils/export.js';

export function createRedoLog(container, fileData) {
  const wasm = getWasm();
  let report;
  try {
    report = JSON.parse(wasm.parse_redo_log(fileData));
  } catch (e) {
    container.innerHTML = `<div class="p-6 text-red-400">Error parsing redo log: ${esc(e)}</div>`;
    return;
  }

  const fmtSize = (bytes) => {
    if (bytes >= 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
    if (bytes >= 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${bytes} B`;
  };

  // Aggregate mlog record type distribution
  const typeCounts = {};
  for (const block of report.blocks) {
    for (const rt of block.record_types) {
      typeCounts[rt] = (typeCounts[rt] || 0) + 1;
    }
  }
  const typeEntries = Object.entries(typeCounts).sort((a, b) => b[1] - a[1]);

  container.innerHTML = `
    <div class="p-6 space-y-6 overflow-auto max-h-full">
      <div class="flex items-center gap-3">
        <h2 class="text-lg font-bold text-innodb-cyan">Redo Log Analysis</h2>
        <span id="redolog-export"></span>
      </div>

      <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
        ${statCard('File Size', fmtSize(report.file_size))}
        ${statCard('Total Blocks', report.total_blocks.toLocaleString())}
        ${statCard('Data Blocks', report.data_blocks.toLocaleString())}
        ${statCard('Non-Empty', report.blocks.filter(b => b.has_data).length.toLocaleString())}
      </div>

      ${report.header ? `
        <h3 class="text-md font-semibold text-gray-300">File Header</h3>
        <div class="bg-surface-2 rounded-lg p-4">
          <table class="text-sm">
            <tr><td class="pr-4 py-0.5 text-gray-500 text-xs">Format</td><td class="py-0.5 text-sm">${esc(String(report.header.format_version ?? 'N/A'))}</td></tr>
            <tr><td class="pr-4 py-0.5 text-gray-500 text-xs">Start LSN</td><td class="py-0.5 text-sm">${report.header.start_lsn ?? 'N/A'}</td></tr>
            <tr><td class="pr-4 py-0.5 text-gray-500 text-xs">Creator</td><td class="py-0.5 text-sm">${esc(String(report.header.created_by ?? 'N/A'))}</td></tr>
          </table>
        </div>
      ` : ''}

      ${(report.checkpoint_1 || report.checkpoint_2) ? `
        <h3 class="text-md font-semibold text-gray-300">Checkpoints</h3>
        <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
          ${report.checkpoint_1 ? checkpointCard('Checkpoint 1', report.checkpoint_1) : ''}
          ${report.checkpoint_2 ? checkpointCard('Checkpoint 2', report.checkpoint_2) : ''}
        </div>
      ` : ''}

      ${typeEntries.length > 0 ? `
        <h3 class="text-md font-semibold text-gray-300">MLOG Record Distribution</h3>
        <div class="overflow-x-auto max-h-64">
          <table class="w-full text-sm">
            <thead>
              <tr class="text-left text-gray-500 border-b border-gray-800">
                <th scope="col" class="py-2 pr-4">Record Type</th>
                <th class="py-2 pr-4 text-right">Count</th>
              </tr>
            </thead>
            <tbody>
              ${typeEntries.map(([name, count]) => `
                <tr class="border-b border-gray-800/50 hover:bg-surface-2/50">
                  <td class="py-1.5 pr-4 text-innodb-cyan text-xs">${esc(name)}</td>
                  <td class="py-1.5 pr-4 text-right">${count}</td>
                </tr>
              `).join('')}
            </tbody>
          </table>
        </div>
      ` : ''}

      <div class="flex items-center gap-2 mb-2">
        <h3 class="text-md font-semibold text-gray-300">Log Blocks</h3>
        <label class="text-xs text-gray-600 flex items-center gap-1">
          <input type="checkbox" id="redolog-nonempty" class="rounded bg-gray-800 border-gray-600" />
          Show non-empty only
        </label>
      </div>
      <div id="redolog-table-wrap" class="overflow-x-auto max-h-96">
        ${renderBlockTable(report.blocks, false)}
      </div>
    </div>
  `;

  const exportSlot = container.querySelector('#redolog-export');
  if (exportSlot) {
    exportSlot.appendChild(createExportBar(() => report, 'redolog'));
  }

  const checkbox = container.querySelector('#redolog-nonempty');
  const wrap = container.querySelector('#redolog-table-wrap');
  checkbox.addEventListener('change', () => {
    wrap.innerHTML = renderBlockTable(report.blocks, checkbox.checked);
  });
}

function renderBlockTable(blocks, nonEmptyOnly) {
  const filtered = nonEmptyOnly ? blocks.filter((b) => b.has_data) : blocks;
  if (filtered.length === 0) {
    return `<div class="text-gray-500 text-sm py-4">No blocks to display.</div>`;
  }
  return `
    <table class="w-full text-xs font-mono">
      <thead class="sticky top-0 bg-gray-950">
        <tr class="text-left text-gray-500 border-b border-gray-800">
          <th scope="col" class="py-1 pr-3">Index</th>
          <th scope="col" class="py-1 pr-3">Block No</th>
          <th scope="col" class="py-1 pr-3">Flush</th>
          <th scope="col" class="py-1 pr-3">Data Len</th>
          <th scope="col" class="py-1 pr-3">1st Rec Group</th>
          <th scope="col" class="py-1 pr-3">Checksum</th>
          <th scope="col" class="py-1 pr-3">Record Types</th>
        </tr>
      </thead>
      <tbody>
        ${filtered.map(blockRow).join('')}
      </tbody>
    </table>`;
}

function blockRow(b) {
  const ckClass = b.checksum_valid ? 'text-innodb-green' : 'text-innodb-red';
  return `
    <tr class="border-b border-gray-800/30 hover:bg-surface-2/50">
      <td class="py-1 pr-3 text-gray-400">${b.block_index}</td>
      <td class="py-1 pr-3">${b.block_no}</td>
      <td class="py-1 pr-3">${b.flush_flag ? 'Y' : 'N'}</td>
      <td class="py-1 pr-3">${b.data_len}</td>
      <td class="py-1 pr-3">${b.first_rec_group}</td>
      <td class="py-1 pr-3 ${ckClass} font-bold">${b.checksum_valid ? 'OK' : 'FAIL'}</td>
      <td class="py-1 pr-3 text-gray-500">${b.record_types.map(r => esc(r)).join(', ') || '—'}</td>
    </tr>`;
}

function checkpointCard(title, cp) {
  return `
    <div class="bg-surface-2 rounded-lg p-4">
      <div class="text-xs text-gray-500 uppercase tracking-wide mb-2">${esc(title)}</div>
      <table class="text-sm">
        ${cp.number ? `<tr><td class="pr-4 py-0.5 text-gray-500 text-xs">Number</td><td class="py-0.5 text-sm">${cp.number}</td></tr>` : ''}
        <tr><td class="pr-4 py-0.5 text-gray-500 text-xs">LSN</td><td class="py-0.5 text-sm">${cp.lsn ?? 'N/A'}</td></tr>
        ${cp.offset ? `<tr><td class="pr-4 py-0.5 text-gray-500 text-xs">Offset</td><td class="py-0.5 text-sm">${cp.offset}</td></tr>` : ''}
      </table>
    </div>`;
}

function statCard(label, value) {
  return `
    <div class="bg-surface-2 rounded-lg p-4">
      <div class="text-xs text-gray-500 uppercase tracking-wide">${esc(label)}</div>
      <div class="text-lg font-bold text-gray-100 mt-1">${esc(String(value))}</div>
    </div>`;
}
