// Simulate tab — crash recovery level simulation (mirrors `inno simulate`)
import { getWasm } from '../wasm.js';
import { esc } from '../utils/html.js';
import { createExportBar } from '../utils/export.js';
import { requestPage } from '../utils/navigation.js';
import { trackFeatureUse } from '../utils/analytics.js';
import { insertTabIntro } from '../utils/help.js';

/**
 * Create the simulate tab for a single tablespace file.
 * @param {HTMLElement} container
 * @param {Uint8Array} fileData
 */
export function createSimulate(container, fileData) {
  const wasm = getWasm();
  let report;
  try {
    report = JSON.parse(wasm.simulate_recovery(fileData));
  } catch (e) {
    container.innerHTML = `<div class="p-6 text-red-400">Error simulating recovery: ${esc(String(e))}</div>`;
    return;
  }

  trackFeatureUse('simulate');

  const plan = report.plan || {};
  const levels = plan.levels || [];
  const tables = report.tables || [];
  const ps = report.page_summary || {};
  const rec = plan.recommended_level;

  container.innerHTML = `
    <div class="p-6 space-y-6 overflow-auto max-h-full">
      <div class="flex items-center gap-3">
        <h2 class="text-lg font-bold text-innodb-cyan">Crash Recovery Simulation</h2>
        <span id="simulate-export"></span>
      </div>

      ${renderBanner(rec, plan.rationale)}

      <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
        ${statCard('Total Pages', report.total_pages)}
        ${statCard('Intact', ps.intact ?? 0, 'text-innodb-green')}
        ${statCard('Corrupt', ps.corrupt ?? 0, ps.corrupt > 0 ? 'text-innodb-red' : '')}
        ${statCard('Empty', ps.empty ?? 0)}
      </div>

      <h3 class="text-md font-semibold text-gray-300">Level Comparison</h3>
      <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-3" id="simulate-levels"></div>

      ${tables.length > 0 ? `
        <h3 class="text-md font-semibold text-gray-300">Per-Table Impact</h3>
        <div class="overflow-x-auto" id="simulate-tables"></div>
      ` : ''}
    </div>
  `;
  insertTabIntro(container, 'simulate');

  // Export bar
  const exportSlot = container.querySelector('#simulate-export');
  if (exportSlot) {
    exportSlot.appendChild(createExportBar(() => report, 'simulate'));
  }

  // Level cards
  const levelsWrap = container.querySelector('#simulate-levels');
  if (levelsWrap) {
    levelsWrap.innerHTML = levels.map(la => renderLevelCard(la, rec)).join('');
  }

  // Per-table impact table
  const tablesWrap = container.querySelector('#simulate-tables');
  if (tablesWrap && tables.length > 0) {
    tablesWrap.innerHTML = renderTablesTable(tables);

    // Click handler for page navigation
    tablesWrap.querySelectorAll('[data-page]').forEach(el => {
      el.addEventListener('click', () => {
        const pg = parseInt(el.dataset.page, 10);
        if (!isNaN(pg)) requestPage(pg);
      });
    });
  }
}

// ---------------------------------------------------------------------------
// Rendering helpers
// ---------------------------------------------------------------------------

function renderBanner(level, rationale) {
  if (level === 0) {
    return `
      <div class="flex items-center gap-2 p-4 rounded-lg bg-green-900/30 border border-green-700/40">
        <span class="text-innodb-green font-bold text-lg">No Recovery Needed</span>
        <span class="text-gray-400 text-sm">${esc(rationale || '')}</span>
      </div>`;
  }
  const color = level <= 3 ? 'amber' : 'red';
  const bgClass = `bg-${color}-900/30 border-${color}-700/40`;
  const textClass = `text-innodb-${color}`;
  return `
    <div class="flex flex-col gap-1 p-4 rounded-lg ${bgClass}">
      <div class="flex items-center gap-2">
        <span class="${textClass} font-bold text-lg">Recovery Level ${level} Recommended</span>
      </div>
      <span class="text-gray-400 text-sm">${esc(rationale || '')}</span>
    </div>`;
}

function renderLevelCard(la, recommended) {
  const isRec = la.level === recommended;
  const borderClass = isRec ? 'border-innodb-cyan ring-1 ring-innodb-cyan/30' : 'border-gray-700';
  const badge = isRec ? '<span class="text-xs bg-innodb-cyan/20 text-innodb-cyan px-2 py-0.5 rounded">Recommended</span>' : '';
  const riskColor = la.pct_overall_risk === 0 ? 'text-innodb-green'
    : la.pct_overall_risk < 5 ? 'text-innodb-amber'
    : 'text-innodb-red';

  const warningsHtml = la.warnings.length > 0
    ? `<div class="mt-2 text-xs text-gray-500">${la.warnings.map(w => `<div>- ${esc(w)}</div>`).join('')}</div>`
    : '';

  return `
    <div class="p-3 rounded-lg border ${borderClass} bg-gray-900/50">
      <div class="flex items-center gap-2 mb-1">
        <span class="text-sm font-bold text-gray-200">Level ${la.level}</span>
        ${badge}
      </div>
      <div class="text-xs text-gray-500 mb-2">${esc(la.name)}</div>
      <div class="text-xs text-gray-400 mb-2">${esc(la.description)}</div>
      <div class="grid grid-cols-2 gap-x-3 gap-y-1 text-xs">
        <span class="text-gray-500">Tables OK</span>
        <span class="text-gray-300">${la.tables_accessible}/${la.total_tables}</span>
        <span class="text-gray-500">Data at Risk</span>
        <span class="${riskColor}">${la.pct_overall_risk.toFixed(1)}%</span>
        <span class="text-gray-500">Records at Risk</span>
        <span class="text-gray-300">${la.total_records_at_risk.toLocaleString()}</span>
      </div>
      ${warningsHtml}
    </div>`;
}

function renderTablesTable(tables) {
  let rows = '';
  for (const table of tables) {
    const name = table.table_name || '(unknown)';
    for (const idx of table.indexes) {
      const idxName = idx.index_name || `#${idx.index_id}`;
      const type = idx.is_clustered ? 'Clustered' : 'Secondary';
      const riskAtL1 = idx.lost_records_by_level?.['1'] ?? 0;
      const riskClass = idx.corrupt_pages > 0 ? 'text-innodb-red' : '';
      rows += `
        <tr class="border-b border-gray-800 hover:bg-gray-800/50">
          <td class="px-3 py-2 text-gray-300">${esc(name)}</td>
          <td class="px-3 py-2 text-gray-400">${esc(idxName)}</td>
          <td class="px-3 py-2 text-gray-500 text-xs">${type}</td>
          <td class="px-3 py-2 text-right text-gray-300">${idx.total_pages}</td>
          <td class="px-3 py-2 text-right ${riskClass}">${idx.corrupt_pages}</td>
          <td class="px-3 py-2 text-right text-gray-300">${idx.total_records.toLocaleString()}</td>
          <td class="px-3 py-2 text-right ${riskClass}">~${riskAtL1.toLocaleString()}</td>
        </tr>`;
    }
  }

  return `
    <table class="w-full text-sm">
      <thead>
        <tr class="text-left text-gray-500 text-xs uppercase border-b border-gray-700">
          <th class="px-3 py-2">Table</th>
          <th class="px-3 py-2">Index</th>
          <th class="px-3 py-2">Type</th>
          <th class="px-3 py-2 text-right">Pages</th>
          <th class="px-3 py-2 text-right">Corrupt</th>
          <th class="px-3 py-2 text-right">Total Records</th>
          <th class="px-3 py-2 text-right">At Risk (L1)</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>`;
}

function statCard(label, value, colorClass = '') {
  return `
    <div class="p-3 rounded-lg bg-gray-900/50 border border-gray-800">
      <div class="text-xs text-gray-500 mb-1">${esc(label)}</div>
      <div class="text-lg font-bold ${colorClass || 'text-gray-200'}">${typeof value === 'number' ? value.toLocaleString() : esc(String(value))}</div>
    </div>`;
}
