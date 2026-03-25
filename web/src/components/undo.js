// Undo tab — undo log segment analysis (mirrors `inno undo`)
import { getWasm } from '../wasm.js';
import { esc } from '../utils/html.js';
import { createExportBar } from '../utils/export.js';
import { trackFeatureUse } from '../utils/analytics.js';
import { insertTabIntro } from '../utils/help.js';

/**
 * Create the undo tab for a single tablespace file.
 * @param {HTMLElement} container
 * @param {Uint8Array} fileData
 */
export function createUndo(container, fileData) {
  const wasm = getWasm();
  let result;
  try {
    result = JSON.parse(wasm.analyze_undo(fileData));
  } catch (e) {
    container.innerHTML = `<div class="p-6 text-red-400">Error analyzing undo logs: ${esc(String(e))}</div>`;
    return;
  }

  const segments = result.segments || [];
  const rsegHeaders = result.rseg_headers || [];
  const totalTx = result.total_transactions || 0;
  const activeTx = result.active_transactions || 0;

  container.innerHTML = `
    <div class="p-6 space-y-6 overflow-auto max-h-full">
      <div class="flex items-center gap-3">
        <h2 class="text-lg font-bold text-innodb-cyan">Undo Log Analysis</h2>
        <span id="undo-export"></span>
      </div>

      <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
        ${statCard('Segments', segments.length)}
        ${statCard('Total Transactions', totalTx)}
        ${statCard('Active', activeTx, activeTx > 0 ? 'text-innodb-amber' : 'text-innodb-green')}
        ${statCard('RSEG Headers', rsegHeaders.length)}
      </div>

      ${rsegHeaders.length > 0 ? `
        <h3 class="text-md font-semibold text-gray-300">Rollback Segments</h3>
        <div class="overflow-x-auto max-h-64">
          ${renderRsegTable(rsegHeaders)}
        </div>
      ` : ''}

      ${segments.length > 0 ? `
        <h3 class="text-md font-semibold text-gray-300">Undo Segments</h3>
        <div class="overflow-x-auto max-h-96">
          ${renderSegmentsTable(segments)}
        </div>
      ` : '<div class="text-gray-500 text-sm py-4">No undo segments found.</div>'}

      ${totalTx > 0 ? `
        <h3 class="text-md font-semibold text-gray-300">Transaction Log Headers</h3>
        <div id="undo-log-headers" class="overflow-x-auto max-h-96">
          ${renderLogHeaders(segments)}
        </div>
      ` : ''}
    </div>
  `;
  insertTabIntro(container, 'undo');

  // Export bar
  const exportSlot = container.querySelector('#undo-export');
  if (exportSlot) {
    exportSlot.appendChild(createExportBar(() => result, 'undo'));
  }

  // Expandable segment rows
  container.querySelectorAll('tr[data-segment-idx]').forEach((tr) => {
    tr.classList.add('cursor-pointer');
    tr.addEventListener('click', () => {
      const detailRow = tr.nextElementSibling;
      if (detailRow && detailRow.classList.contains('segment-detail')) {
        detailRow.classList.toggle('hidden');
        trackFeatureUse('undo_segment_expand');
      }
    });
  });
}

function renderRsegTable(rsegHeaders) {
  return `
    <table class="w-full text-xs font-mono">
      <thead class="sticky top-0 bg-gray-950">
        <tr class="text-left text-gray-500 border-b border-gray-800">
          <th scope="col" class="py-1 pr-3">Page</th>
          <th scope="col" class="py-1 pr-3">Max Size</th>
          <th scope="col" class="py-1 pr-3">History Size</th>
          <th scope="col" class="py-1 pr-3">Active Slots</th>
        </tr>
      </thead>
      <tbody>
        ${rsegHeaders.map((r) => `
          <tr class="border-b border-gray-800/30 hover:bg-surface-2/50">
            <td class="py-1 pr-3 text-innodb-cyan">${r.page_no}</td>
            <td class="py-1 pr-3 text-gray-300">${r.max_size}</td>
            <td class="py-1 pr-3 text-gray-300">${r.history_size}</td>
            <td class="py-1 pr-3 text-gray-300">${r.active_slot_count}</td>
          </tr>
        `).join('')}
      </tbody>
    </table>`;
}

function renderSegmentsTable(segments) {
  return `
    <table class="w-full text-xs font-mono">
      <thead class="sticky top-0 bg-gray-950">
        <tr class="text-left text-gray-500 border-b border-gray-800">
          <th scope="col" class="py-1 pr-3">Page</th>
          <th scope="col" class="py-1 pr-3">State</th>
          <th scope="col" class="py-1 pr-3">Type</th>
          <th scope="col" class="py-1 pr-3">Log Headers</th>
          <th scope="col" class="py-1 pr-3">Records</th>
        </tr>
      </thead>
      <tbody>
        ${segments.map((seg, i) => {
          const state = seg.segment_header?.state || 'Unknown';
          const stateName = typeof state === 'string' ? state : (state.Unknown != null ? `Unknown(${state.Unknown})` : JSON.stringify(state));
          const stateClass = stateColorClass(stateName);
          const pageType = seg.page_header?.page_type || 'Unknown';
          const typeName = typeof pageType === 'string' ? pageType : (pageType.Unknown != null ? `Unknown(${pageType.Unknown})` : JSON.stringify(pageType));
          return `
            <tr class="border-b border-gray-800/30 hover:bg-surface-2/50" data-segment-idx="${i}">
              <td class="py-1 pr-3 text-innodb-cyan">${seg.page_no}</td>
              <td class="py-1 pr-3">
                <span class="px-2 py-0.5 rounded-full ${stateClass} text-xs font-bold">${esc(stateName)}</span>
              </td>
              <td class="py-1 pr-3 text-gray-300">${esc(typeName)}</td>
              <td class="py-1 pr-3 text-gray-300">${seg.log_headers?.length || 0}</td>
              <td class="py-1 pr-3 text-gray-300">${seg.record_count || 0}</td>
            </tr>
            ${seg.log_headers?.length > 0 ? `
              <tr class="segment-detail hidden">
                <td colspan="5" class="py-2 pl-8 pr-3">
                  <div class="text-xs text-gray-500 mb-1">Transaction log headers:</div>
                  <table class="w-full text-xs font-mono">
                    <thead>
                      <tr class="text-gray-600">
                        <th class="py-0.5 pr-2 text-left">TRX ID</th>
                        <th class="py-0.5 pr-2 text-left">TRX No</th>
                        <th class="py-0.5 pr-2 text-left">Del Marks</th>
                        <th class="py-0.5 pr-2 text-left">DDL</th>
                        <th class="py-0.5 pr-2 text-left">XID</th>
                      </tr>
                    </thead>
                    <tbody>
                      ${seg.log_headers.map((lh) => `
                        <tr class="border-t border-gray-800/20">
                          <td class="py-0.5 pr-2 text-gray-300">${lh.trx_id}</td>
                          <td class="py-0.5 pr-2 text-gray-300">${lh.trx_no}</td>
                          <td class="py-0.5 pr-2 ${lh.del_marks ? 'text-innodb-amber' : 'text-gray-500'}">${lh.del_marks ? 'Yes' : 'No'}</td>
                          <td class="py-0.5 pr-2 ${lh.dict_trans ? 'text-innodb-cyan' : 'text-gray-500'}">${lh.dict_trans ? 'Yes' : 'No'}</td>
                          <td class="py-0.5 pr-2 text-gray-500">${lh.xid_exists ? 'Yes' : 'No'}</td>
                        </tr>
                      `).join('')}
                    </tbody>
                  </table>
                </td>
              </tr>
            ` : ''}
          `;
        }).join('')}
      </tbody>
    </table>`;
}

function renderLogHeaders(segments) {
  const allHeaders = [];
  for (const seg of segments) {
    if (seg.log_headers) {
      for (const lh of seg.log_headers) {
        allHeaders.push({ ...lh, page_no: seg.page_no });
      }
    }
  }

  if (allHeaders.length === 0) return '';

  return `
    <table class="w-full text-xs font-mono">
      <thead class="sticky top-0 bg-gray-950">
        <tr class="text-left text-gray-500 border-b border-gray-800">
          <th scope="col" class="py-1 pr-3">Page</th>
          <th scope="col" class="py-1 pr-3">TRX ID</th>
          <th scope="col" class="py-1 pr-3">TRX No</th>
          <th scope="col" class="py-1 pr-3">Del Marks</th>
          <th scope="col" class="py-1 pr-3">DDL</th>
          <th scope="col" class="py-1 pr-3">XID</th>
          <th scope="col" class="py-1 pr-3">Table ID</th>
        </tr>
      </thead>
      <tbody>
        ${allHeaders.map((lh) => `
          <tr class="border-b border-gray-800/30 hover:bg-surface-2/50">
            <td class="py-1 pr-3 text-innodb-cyan">${lh.page_no}</td>
            <td class="py-1 pr-3 text-gray-300">${lh.trx_id}</td>
            <td class="py-1 pr-3 text-gray-300">${lh.trx_no}</td>
            <td class="py-1 pr-3 ${lh.del_marks ? 'text-innodb-amber' : 'text-gray-500'}">${lh.del_marks ? 'Yes' : 'No'}</td>
            <td class="py-1 pr-3 ${lh.dict_trans ? 'text-innodb-cyan' : 'text-gray-500'}">${lh.dict_trans ? 'Yes' : 'No'}</td>
            <td class="py-1 pr-3 text-gray-500">${lh.xid_exists ? 'Yes' : 'No'}</td>
            <td class="py-1 pr-3 text-gray-400">${lh.table_id || '\u2014'}</td>
          </tr>
        `).join('')}
      </tbody>
    </table>`;
}

function stateColorClass(state) {
  const s = String(state).toLowerCase();
  if (s === 'active') return 'bg-innodb-amber/20 text-innodb-amber';
  if (s === 'cached') return 'bg-innodb-green/20 text-innodb-green';
  if (s === 'topurge') return 'bg-innodb-red/20 text-innodb-red';
  if (s === 'tofree') return 'bg-innodb-cyan/20 text-innodb-cyan';
  if (s === 'prepared') return 'bg-purple-500/20 text-purple-400';
  return 'bg-gray-700/30 text-gray-400';
}

function statCard(label, value, colorClass = '') {
  return `
    <div class="bg-surface-2 rounded-lg p-4">
      <div class="text-xs text-gray-500 uppercase tracking-wide">${esc(label)}</div>
      <div class="text-lg font-bold ${colorClass || 'text-gray-100'} mt-1">${value}</div>
    </div>`;
}
