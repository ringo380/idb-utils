// Shared health UI helpers — used by health.js and audit.js
import { esc } from './html.js';

/**
 * Return a Tailwind text-color class based on fill-factor value (0-1 float).
 * @param {number} value - fill factor 0..1
 * @returns {string}
 */
export function fillFactorClass(value) {
  if (value >= 0.70) return 'text-innodb-green';
  if (value >= 0.40) return 'text-innodb-amber';
  return 'text-innodb-red';
}

/**
 * Return HTML for a color-coded progress bar representing a fill-factor value.
 * @param {number} value - fill factor 0..1
 * @returns {string}
 */
export function renderFillFactorBar(value) {
  const pct = Math.min(value * 100, 100).toFixed(1);
  let colorClass;
  if (value >= 0.70) colorClass = 'bg-innodb-green';
  else if (value >= 0.40) colorClass = 'bg-innodb-amber';
  else colorClass = 'bg-innodb-red';

  return `<div class="w-full bg-gray-800 rounded-full h-2.5"><div class="${colorClass} h-full rounded-full" style="width:${pct}%"></div></div>`;
}

/**
 * Return HTML for a per-index health table.
 * @param {Array} indexes - array of index health objects
 * @returns {string}
 */
export function renderIndexTable(indexes) {
  if (!indexes || indexes.length === 0) {
    return `<div class="text-gray-500 text-sm py-4">No index data available.</div>`;
  }

  const headers = [
    'Index ID', 'Depth', 'Leaf Pages', 'Records',
    'Avg Fill', 'Min Fill', 'Max Fill', 'Frag %', 'Garbage %', 'Empty Leaves',
  ];

  return `
    <table class="w-full text-xs font-mono">
      <thead class="sticky top-0 bg-gray-950">
        <tr class="text-left text-gray-500 border-b border-gray-800">
          ${headers.map((h) => `<th scope="col" class="py-1 pr-3">${esc(h)}</th>`).join('')}
        </tr>
      </thead>
      <tbody>
        ${indexes.map((idx) => indexRow(idx)).join('')}
      </tbody>
    </table>`;
}

function indexRow(idx) {
  const avg = idx.avg_fill_factor ?? 0;
  const min = idx.min_fill_factor ?? 0;
  const max = idx.max_fill_factor ?? 0;
  const frag = idx.fragmentation ?? 0;
  const garbage = idx.avg_garbage_ratio ?? 0;

  return `
    <tr class="border-b border-gray-800/30 hover:bg-surface-2/50" data-index-id="${idx.index_id}">
      <td class="py-1 pr-3 text-innodb-cyan">${idx.index_id}</td>
      <td class="py-1 pr-3 text-gray-400">${idx.tree_depth ?? '\u2014'}</td>
      <td class="py-1 pr-3 text-gray-400">${idx.leaf_pages ?? '\u2014'}</td>
      <td class="py-1 pr-3 text-gray-400">${idx.total_records ?? '\u2014'}</td>
      <td class="py-1 pr-3">
        <div class="flex items-center gap-2">
          <span class="${fillFactorClass(avg)}">${(avg * 100).toFixed(1)}%</span>
          <div class="w-16">${renderFillFactorBar(avg)}</div>
        </div>
      </td>
      <td class="py-1 pr-3">
        <div class="flex items-center gap-2">
          <span class="${fillFactorClass(min)}">${(min * 100).toFixed(1)}%</span>
          <div class="w-16">${renderFillFactorBar(min)}</div>
        </div>
      </td>
      <td class="py-1 pr-3">
        <div class="flex items-center gap-2">
          <span class="${fillFactorClass(max)}">${(max * 100).toFixed(1)}%</span>
          <div class="w-16">${renderFillFactorBar(max)}</div>
        </div>
      </td>
      <td class="py-1 pr-3 text-gray-400">${(frag * 100).toFixed(1)}%</td>
      <td class="py-1 pr-3 text-gray-400">${(garbage * 100).toFixed(1)}%</td>
      <td class="py-1 pr-3 text-gray-400">${idx.empty_leaf_pages ?? 0}</td>
    </tr>`;
}
