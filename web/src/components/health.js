// Health tab — single-file B+Tree metrics (mirrors `inno health`)
import { getWasm } from '../wasm.js';
import { esc } from '../utils/html.js';
import { createExportBar } from '../utils/export.js';
import { fillFactorClass } from '../utils/health-ui.js';
import { requestIndexFilter, navigateToTab } from '../utils/navigation.js';
import { createBTree } from './btree.js';

/**
 * Create the health dashboard for a single tablespace file.
 * @param {HTMLElement} container
 * @param {Uint8Array} fileData
 */
export function createHealth(container, fileData) {
  const wasm = getWasm();
  let report;
  try {
    report = JSON.parse(wasm.analyze_health(fileData));
  } catch (e) {
    container.innerHTML = `<div class="p-6 text-red-400">Error analyzing health: ${esc(String(e))}</div>`;
    return;
  }

  // Resolve index names from schema SDI
  const indexNames = buildIndexNameMap(wasm, fileData);

  const s = report.summary;
  const avgFillPct = (s.avg_fill_factor * 100).toFixed(1);
  const avgFragPct = (s.avg_fragmentation * 100).toFixed(1);
  const avgGarbagePct = (s.avg_garbage_ratio * 100).toFixed(1);

  // Annotate indexes with resolved names
  const indexes = (report.indexes || []).map((idx) => ({
    ...idx,
    _name: indexNames.get(idx.index_id) || null,
  }));

  // Sort state
  let sortCol = null;
  let sortAsc = true;

  container.innerHTML = `
    <div class="p-6 space-y-6 overflow-auto max-h-full">
      <div class="flex items-center gap-3">
        <h2 class="text-lg font-bold text-innodb-cyan">B+Tree Health</h2>
        <span class="text-xs text-gray-500">${s.index_count} index${s.index_count !== 1 ? 'es' : ''}</span>
        <span id="health-export"></span>
      </div>

      <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
        ${statCard('Index Count', s.index_count)}
        ${statCard('Avg Fill Factor', avgFillPct + '%', fillFactorClass(s.avg_fill_factor))}
        ${statCard('Avg Fragmentation', avgFragPct + '%', 'text-innodb-amber')}
        ${statCard('Avg Garbage Ratio', avgGarbagePct + '%', 'text-gray-300')}
      </div>

      <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
        ${statCard('Total Pages', s.total_pages)}
        ${statCard('Index Pages', s.index_pages)}
        ${statCard('Non-Index Pages', s.non_index_pages)}
        ${statCard('Empty Pages', s.empty_pages)}
      </div>

      <div class="flex items-center gap-3">
        <h3 class="text-md font-semibold text-gray-300">Per-Index Details</h3>
        <span class="text-xs text-gray-600">Click a row to filter in Pages tab</span>
      </div>
      <div id="health-index-table" class="overflow-x-auto max-h-96"></div>

      <div>
        <button id="health-btree-toggle" class="px-3 py-1.5 bg-surface-3 hover:bg-gray-600 text-gray-300 rounded text-xs">
          Show B+Tree
        </button>
        <div id="health-btree-container" class="hidden mt-4"></div>
      </div>
    </div>
  `;

  // Export bar
  const exportSlot = container.querySelector('#health-export');
  if (exportSlot) {
    exportSlot.appendChild(createExportBar(() => report, 'health'));
  }

  // Render sortable index table
  function renderTable() {
    let sorted = [...indexes];
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

    const wrap = container.querySelector('#health-index-table');
    // Build custom table with name column and sortable headers
    const colDefs = [
      { key: 'index_id', label: 'Index ID' },
      { key: '_name', label: 'Name' },
      { key: 'tree_depth', label: 'Depth' },
      { key: 'leaf_pages', label: 'Leaf Pages' },
      { key: 'total_records', label: 'Records' },
      { key: 'avg_fill_factor', label: 'Avg Fill' },
      { key: 'min_fill_factor', label: 'Min Fill' },
      { key: 'max_fill_factor', label: 'Max Fill' },
      { key: 'fragmentation', label: 'Frag %' },
      { key: 'avg_garbage_ratio', label: 'Garbage %' },
      { key: 'empty_leaf_pages', label: 'Empty Leaves' },
    ];

    wrap.innerHTML = `
      <table class="w-full text-xs font-mono">
        <thead class="sticky top-0 bg-gray-950">
          <tr class="text-left text-gray-500 border-b border-gray-800">
            ${colDefs.map((c) => `<th scope="col" class="py-1 pr-3 cursor-pointer hover:text-gray-300 select-none" data-sort="${c.key}">${c.label}${sortCol === c.key ? (sortAsc ? ' &#9650;' : ' &#9660;') : ''}</th>`).join('')}
          </tr>
        </thead>
        <tbody>
          ${sorted.map((idx) => healthIndexRow(idx)).join('')}
        </tbody>
      </table>`;

    // Sort handlers
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

    // Row click -> navigate to Pages tab with index filter
    wrap.querySelectorAll('tr[data-index-id]').forEach((tr) => {
      tr.classList.add('cursor-pointer');
      tr.addEventListener('click', () => {
        const indexId = parseInt(tr.dataset.indexId, 10);
        requestIndexFilter(indexId);
        navigateToTab('pages');
      });
    });
  }

  renderTable();

  // B+Tree toggle — lazy-load on first click
  const btreeBtn = container.querySelector('#health-btree-toggle');
  const btreeContainer = container.querySelector('#health-btree-container');
  let btreeLoaded = false;
  btreeBtn.addEventListener('click', () => {
    if (!btreeLoaded) {
      try {
        const pagesData = JSON.parse(wasm.analyze_pages(fileData, -1n));
        createBTree(btreeContainer, fileData, pagesData);
        btreeLoaded = true;
      } catch (e) {
        btreeContainer.innerHTML = `<div class="p-4 text-red-400 text-sm">Error loading B+Tree: ${esc(String(e))}</div>`;
      }
    }
    const isHidden = btreeContainer.classList.toggle('hidden');
    btreeBtn.textContent = isHidden ? 'Show B+Tree' : 'Hide B+Tree';
  });
}

/**
 * Build a map of index_id -> index name by parsing raw SDI JSON.
 * The extract_schema output (TableSchema/IndexDef) does not include
 * se_private_data, so we use extract_sdi which returns the raw SDI records
 * containing dd_object.indexes with se_private_data "id=N;root=M;..." strings.
 */
function buildIndexNameMap(wasm, fileData) {
  const map = new Map();
  try {
    const raw = wasm.extract_sdi(fileData);
    const records = JSON.parse(raw);
    for (const rec of records) {
      const dd = rec.dd_object;
      if (!dd || !dd.indexes) continue;
      for (const idx of dd.indexes) {
        const match = (idx.se_private_data || '').match(/id=(\d+)/);
        if (match && idx.name) {
          map.set(parseInt(match[1], 10), idx.name);
        }
      }
    }
  } catch {
    // SDI extraction is optional — may not be available for pre-8.0 files
  }
  return map;
}

function healthIndexRow(idx) {
  const avg = idx.avg_fill_factor ?? 0;
  const min = idx.min_fill_factor ?? 0;
  const max = idx.max_fill_factor ?? 0;
  const frag = idx.fragmentation ?? 0;
  const garbage = idx.avg_garbage_ratio ?? 0;

  return `
    <tr class="border-b border-gray-800/30 hover:bg-surface-2/50" data-index-id="${idx.index_id}">
      <td class="py-1 pr-3 text-innodb-cyan">${idx.index_id}</td>
      <td class="py-1 pr-3 text-gray-300">${idx._name ? esc(idx._name) : '\u2014'}</td>
      <td class="py-1 pr-3 text-gray-400">${idx.tree_depth ?? '\u2014'}</td>
      <td class="py-1 pr-3 text-gray-400">${idx.leaf_pages ?? '\u2014'}</td>
      <td class="py-1 pr-3 text-gray-400">${idx.total_records ?? '\u2014'}</td>
      <td class="py-1 pr-3">
        <div class="flex items-center gap-2">
          <span class="${fillFactorClass(avg)}">${(avg * 100).toFixed(1)}%</span>
          <div class="w-16"><div class="w-full bg-gray-800 rounded-full h-2.5"><div class="${avg >= 0.70 ? 'bg-innodb-green' : avg >= 0.40 ? 'bg-innodb-amber' : 'bg-innodb-red'} h-full rounded-full" style="width:${(avg * 100).toFixed(1)}%"></div></div></div>
        </div>
      </td>
      <td class="py-1 pr-3">
        <div class="flex items-center gap-2">
          <span class="${fillFactorClass(min)}">${(min * 100).toFixed(1)}%</span>
          <div class="w-16"><div class="w-full bg-gray-800 rounded-full h-2.5"><div class="${min >= 0.70 ? 'bg-innodb-green' : min >= 0.40 ? 'bg-innodb-amber' : 'bg-innodb-red'} h-full rounded-full" style="width:${(min * 100).toFixed(1)}%"></div></div></div>
        </div>
      </td>
      <td class="py-1 pr-3">
        <div class="flex items-center gap-2">
          <span class="${fillFactorClass(max)}">${(max * 100).toFixed(1)}%</span>
          <div class="w-16"><div class="w-full bg-gray-800 rounded-full h-2.5"><div class="${max >= 0.70 ? 'bg-innodb-green' : max >= 0.40 ? 'bg-innodb-amber' : 'bg-innodb-red'} h-full rounded-full" style="width:${(max * 100).toFixed(1)}%"></div></div></div>
        </div>
      </td>
      <td class="py-1 pr-3 text-gray-400">${(frag * 100).toFixed(1)}%</td>
      <td class="py-1 pr-3 text-gray-400">${(garbage * 100).toFixed(1)}%</td>
      <td class="py-1 pr-3 text-gray-400">${idx.empty_leaf_pages ?? 0}</td>
    </tr>`;
}

function statCard(label, value, colorClass = '') {
  return `
    <div class="bg-surface-2 rounded-lg p-4">
      <div class="text-xs text-gray-500 uppercase tracking-wide">${esc(label)}</div>
      <div class="text-lg font-bold ${colorClass || 'text-gray-100'} mt-1">${value}</div>
    </div>`;
}
