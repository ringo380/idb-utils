// Binlog tab — MySQL binary log analysis (mirrors `inno binlog`)
import { getWasm } from '../wasm.js';
import { esc } from '../utils/html.js';
import { createExportBar } from '../utils/export.js';
import { trackFeatureUse } from '../utils/analytics.js';
import { insertTabIntro } from '../utils/help.js';

const PAGE_SIZE = 100;

/**
 * Create the binlog tab for a binary log file.
 * @param {HTMLElement} container
 * @param {Uint8Array} fileData
 */
export function createBinlog(container, fileData) {
  const wasm = getWasm();
  let result;
  try {
    result = JSON.parse(wasm.analyze_binlog(fileData));
  } catch (e) {
    container.innerHTML = `<div class="p-6 text-red-400">Error analyzing binary log: ${esc(String(e))}</div>`;
    return;
  }

  const fd = result.format_description || {};
  const events = result.events || [];
  const tableMaps = result.table_maps || [];
  const typeCounts = result.event_type_counts || {};
  let currentPage = 0;
  const totalPages = Math.max(1, Math.ceil(events.length / PAGE_SIZE));

  // Sort type counts by count descending
  const sortedTypes = Object.entries(typeCounts).sort((a, b) => b[1] - a[1]);

  container.innerHTML = `
    <div class="p-6 space-y-6 overflow-auto max-h-full">
      <div class="flex items-center gap-3">
        <h2 class="text-lg font-bold text-innodb-cyan">Binary Log Analysis</h2>
        <span id="binlog-export"></span>
      </div>

      <div class="bg-surface-2 rounded-lg p-4">
        <div class="text-xs text-gray-500 uppercase tracking-wide mb-2">Format Description</div>
        <div class="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
          <div><span class="text-gray-500">Server:</span> <span class="text-gray-200">${esc(fd.server_version || 'N/A')}</span></div>
          <div><span class="text-gray-500">Binlog Version:</span> <span class="text-gray-200">${fd.binlog_version ?? 'N/A'}</span></div>
          <div><span class="text-gray-500">Header Length:</span> <span class="text-gray-200">${fd.header_length ?? 'N/A'}</span></div>
          <div><span class="text-gray-500">Checksum:</span> <span class="text-gray-200">${fd.checksum_alg === 1 ? 'CRC32' : fd.checksum_alg === 0 ? 'None' : fd.checksum_alg ?? 'N/A'}</span></div>
        </div>
      </div>

      <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
        ${statCard('Total Events', result.event_count || 0)}
        ${statCard('Event Types', sortedTypes.length)}
        ${statCard('Table Maps', tableMaps.length)}
        ${statCard('Server ID', events.length > 0 ? events[0].server_id : '\u2014')}
      </div>

      ${sortedTypes.length > 0 ? `
        <h3 class="text-md font-semibold text-gray-300">Event Type Distribution</h3>
        <div class="overflow-x-auto max-h-64">
          ${renderTypeDistribution(sortedTypes, result.event_count || 1)}
        </div>
      ` : ''}

      ${tableMaps.length > 0 ? `
        <h3 class="text-md font-semibold text-gray-300">Table Maps</h3>
        <div class="overflow-x-auto max-h-64">
          ${renderTableMaps(tableMaps)}
        </div>
      ` : ''}

      ${events.length > 0 ? `
        <div class="flex items-center gap-3">
          <h3 class="text-md font-semibold text-gray-300">Events</h3>
          <input id="binlog-filter" type="text" placeholder="Filter by type..."
            class="px-2 py-1 bg-surface-3 border border-gray-700 rounded text-xs text-gray-300 w-40" />
        </div>
        <div id="binlog-events-wrap" class="overflow-x-auto max-h-96"></div>
        <div id="binlog-pagination" class="flex items-center gap-3 text-sm"></div>
      ` : ''}
    </div>
  `;
  insertTabIntro(container, 'binlog');

  // Export bar
  const exportSlot = container.querySelector('#binlog-export');
  if (exportSlot) {
    exportSlot.appendChild(createExportBar(() => result, 'binlog'));
  }

  // Event listing with filter and pagination
  if (events.length > 0) {
    let filteredEvents = events;
    const filterInput = container.querySelector('#binlog-filter');

    function applyFilter() {
      const term = (filterInput?.value || '').toUpperCase();
      filteredEvents = term
        ? events.filter((e) => e.event_type.toUpperCase().includes(term))
        : events;
      currentPage = 0;
      renderEvents();
    }

    function renderEvents() {
      const totalFiltered = Math.max(1, Math.ceil(filteredEvents.length / PAGE_SIZE));
      const start = currentPage * PAGE_SIZE;
      const pageEvents = filteredEvents.slice(start, start + PAGE_SIZE);
      const wrap = container.querySelector('#binlog-events-wrap');

      wrap.innerHTML = renderEventsTable(pageEvents);

      const pag = container.querySelector('#binlog-pagination');
      pag.innerHTML = `
        <button id="binlog-prev" class="px-2 py-1 bg-surface-3 hover:bg-gray-600 text-gray-300 rounded text-xs ${currentPage === 0 ? 'opacity-50 cursor-not-allowed' : ''}">Prev</button>
        <span class="text-gray-400 text-xs">Page ${currentPage + 1} of ${totalFiltered} (${filteredEvents.length} events)</span>
        <button id="binlog-next" class="px-2 py-1 bg-surface-3 hover:bg-gray-600 text-gray-300 rounded text-xs ${currentPage >= totalFiltered - 1 ? 'opacity-50 cursor-not-allowed' : ''}">Next</button>
      `;

      pag.querySelector('#binlog-prev').addEventListener('click', () => {
        if (currentPage > 0) { currentPage--; renderEvents(); }
      });
      pag.querySelector('#binlog-next').addEventListener('click', () => {
        if (currentPage < totalFiltered - 1) { currentPage++; renderEvents(); }
      });
    }

    filterInput.addEventListener('input', () => { trackFeatureUse('binlog_filter'); applyFilter(); });
    renderEvents();
  }
}

function renderTypeDistribution(sortedTypes, total) {
  return `
    <table class="w-full text-xs font-mono">
      <thead class="sticky top-0 bg-gray-950">
        <tr class="text-left text-gray-500 border-b border-gray-800">
          <th scope="col" class="py-1 pr-3">Event Type</th>
          <th scope="col" class="py-1 pr-3 text-right">Count</th>
          <th scope="col" class="py-1 pr-3 w-48">Distribution</th>
        </tr>
      </thead>
      <tbody>
        ${sortedTypes.map(([name, count]) => {
          const pct = ((count / total) * 100).toFixed(1);
          return `
            <tr class="border-b border-gray-800/30 hover:bg-surface-2/50">
              <td class="py-1 pr-3 text-gray-300">${esc(name)}</td>
              <td class="py-1 pr-3 text-gray-300 text-right">${count}</td>
              <td class="py-1 pr-3">
                <div class="flex items-center gap-2">
                  <div class="flex-1 bg-gray-800 rounded-full h-2">
                    <div class="bg-innodb-cyan rounded-full h-2" style="width: ${pct}%"></div>
                  </div>
                  <span class="text-gray-500 w-12 text-right">${pct}%</span>
                </div>
              </td>
            </tr>`;
        }).join('')}
      </tbody>
    </table>`;
}

function renderTableMaps(tableMaps) {
  return `
    <table class="w-full text-xs font-mono">
      <thead class="sticky top-0 bg-gray-950">
        <tr class="text-left text-gray-500 border-b border-gray-800">
          <th scope="col" class="py-1 pr-3">Table ID</th>
          <th scope="col" class="py-1 pr-3">Database</th>
          <th scope="col" class="py-1 pr-3">Table</th>
          <th scope="col" class="py-1 pr-3">Columns</th>
        </tr>
      </thead>
      <tbody>
        ${tableMaps.map((tm) => `
          <tr class="border-b border-gray-800/30 hover:bg-surface-2/50">
            <td class="py-1 pr-3 text-innodb-cyan">${tm.table_id}</td>
            <td class="py-1 pr-3 text-gray-300">${esc(tm.database_name)}</td>
            <td class="py-1 pr-3 text-gray-300">${esc(tm.table_name)}</td>
            <td class="py-1 pr-3 text-gray-300">${tm.column_count}</td>
          </tr>
        `).join('')}
      </tbody>
    </table>`;
}

function renderEventsTable(events) {
  return `
    <table class="w-full text-xs font-mono">
      <thead class="sticky top-0 bg-gray-950">
        <tr class="text-left text-gray-500 border-b border-gray-800">
          <th scope="col" class="py-1 pr-3">Offset</th>
          <th scope="col" class="py-1 pr-3">Type</th>
          <th scope="col" class="py-1 pr-3">Size</th>
          <th scope="col" class="py-1 pr-3">Timestamp</th>
          <th scope="col" class="py-1 pr-3">Server ID</th>
        </tr>
      </thead>
      <tbody>
        ${events.map((evt) => `
          <tr class="border-b border-gray-800/30 hover:bg-surface-2/50">
            <td class="py-1 pr-3 text-innodb-cyan">${evt.offset}</td>
            <td class="py-1 pr-3 text-gray-300">${esc(evt.event_type)}</td>
            <td class="py-1 pr-3 text-gray-400">${evt.event_length}</td>
            <td class="py-1 pr-3 text-gray-400">${formatTimestamp(evt.timestamp)}</td>
            <td class="py-1 pr-3 text-gray-500">${evt.server_id}</td>
          </tr>
        `).join('')}
      </tbody>
    </table>`;
}

function formatTimestamp(ts) {
  if (!ts) return '\u2014';
  try {
    return new Date(ts * 1000).toISOString().replace('T', ' ').replace(/\.\d+Z$/, '');
  } catch {
    return String(ts);
  }
}

function statCard(label, value, colorClass = '') {
  return `
    <div class="bg-surface-2 rounded-lg p-4">
      <div class="text-xs text-gray-500 uppercase tracking-wide">${esc(label)}</div>
      <div class="text-lg font-bold ${colorClass || 'text-gray-100'} mt-1">${value}</div>
    </div>`;
}
