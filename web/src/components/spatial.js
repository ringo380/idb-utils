// Spatial tab — R-tree index visualization with MBR canvas
import { getWasm } from '../wasm.js';
import { esc } from '../utils/html.js';
import { createExportBar } from '../utils/export.js';
import { trackFeatureUse } from '../utils/analytics.js';
import { insertTabIntro } from '../utils/help.js';

const CANVAS_W = 800;
const CANVAS_H = 500;
const PADDING = 40;

/**
 * Create the spatial tab for a single tablespace file.
 * @param {HTMLElement} container
 * @param {Uint8Array} fileData
 */
export function createSpatial(container, fileData) {
  const wasm = getWasm();
  let rtreePages;
  try {
    rtreePages = JSON.parse(wasm.analyze_rtree(fileData));
  } catch (e) {
    container.innerHTML = `<div class="p-6 text-red-400">Error analyzing spatial data: ${esc(String(e))}</div>`;
    return;
  }

  if (!rtreePages || rtreePages.length === 0) {
    container.innerHTML = `<div class="p-6 text-gray-500">No R-tree (spatial index) pages found in this tablespace.</div>`;
    return;
  }

  // Group pages by level
  const levels = new Map();
  let allMbrs = [];
  for (const page of rtreePages) {
    const lvl = page.level ?? 0;
    if (!levels.has(lvl)) levels.set(lvl, []);
    levels.get(lvl).push(page);
    if (page.mbrs) allMbrs = allMbrs.concat(page.mbrs);
  }

  const leafPages = levels.get(0) || [];
  const totalMbrs = allMbrs.length;

  // Compute enclosing bounds
  let bounds = null;
  if (allMbrs.length > 0) {
    let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
    for (const m of allMbrs) {
      if (m.min_x < minX) minX = m.min_x;
      if (m.min_y < minY) minY = m.min_y;
      if (m.max_x > maxX) maxX = m.max_x;
      if (m.max_y > maxY) maxY = m.max_y;
    }
    bounds = { minX, minY, maxX, maxY };
  }

  const maxLevel = Math.max(...levels.keys());
  let selectedLevel = 0;

  container.innerHTML = `
    <div class="p-6 space-y-6 overflow-auto max-h-full">
      <div class="flex items-center gap-3">
        <h2 class="text-lg font-bold text-innodb-cyan">Spatial Index</h2>
        <span id="spatial-export"></span>
      </div>

      <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
        ${statCard('RTREE Pages', rtreePages.length)}
        ${statCard('Tree Levels', maxLevel + 1)}
        ${statCard('Leaf Pages', leafPages.length)}
        ${statCard('Total MBRs', totalMbrs)}
      </div>

      ${bounds ? `
        <div class="bg-surface-2 rounded-lg p-4">
          <div class="text-xs text-gray-500 uppercase tracking-wide mb-2">Spatial Extent</div>
          <div class="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm font-mono">
            <div><span class="text-gray-500">Min X:</span> <span class="text-gray-200">${bounds.minX.toFixed(6)}</span></div>
            <div><span class="text-gray-500">Min Y:</span> <span class="text-gray-200">${bounds.minY.toFixed(6)}</span></div>
            <div><span class="text-gray-500">Max X:</span> <span class="text-gray-200">${bounds.maxX.toFixed(6)}</span></div>
            <div><span class="text-gray-500">Max Y:</span> <span class="text-gray-200">${bounds.maxY.toFixed(6)}</span></div>
          </div>
        </div>
      ` : ''}

      <div class="flex items-center gap-3">
        <h3 class="text-md font-semibold text-gray-300">MBR Visualization</h3>
        ${maxLevel > 0 ? `
          <label class="text-xs text-gray-500">Level:
            <select id="spatial-level" class="ml-1 px-2 py-1 bg-surface-3 border border-gray-700 rounded text-xs text-gray-300">
              ${Array.from({ length: maxLevel + 1 }, (_, i) => `<option value="${i}" ${i === 0 ? 'selected' : ''}>Level ${i}${i === 0 ? ' (leaf)' : ''}</option>`).join('')}
            </select>
          </label>
        ` : ''}
      </div>

      <div class="bg-surface-2 rounded-lg p-2 overflow-auto">
        <canvas id="spatial-canvas" width="${CANVAS_W}" height="${CANVAS_H}" class="block mx-auto"></canvas>
      </div>

      <h3 class="text-md font-semibold text-gray-300">Page Summary</h3>
      <div class="overflow-x-auto max-h-64">
        ${renderPageTable(rtreePages)}
      </div>
    </div>
  `;
  insertTabIntro(container, 'spatial');

  // Export bar
  const exportSlot = container.querySelector('#spatial-export');
  if (exportSlot) {
    exportSlot.appendChild(createExportBar(() => rtreePages, 'spatial'));
  }

  // Canvas rendering
  const canvas = container.querySelector('#spatial-canvas');
  if (canvas && bounds) {
    drawMbrs(canvas, getMbrsForLevel(rtreePages, selectedLevel), bounds);

    const levelSelect = container.querySelector('#spatial-level');
    if (levelSelect) {
      levelSelect.addEventListener('change', () => {
        selectedLevel = parseInt(levelSelect.value, 10);
        trackFeatureUse('spatial_level', { level: selectedLevel });
        drawMbrs(canvas, getMbrsForLevel(rtreePages, selectedLevel), bounds);
      });
    }
  }
}

function getMbrsForLevel(pages, level) {
  const mbrs = [];
  for (const page of pages) {
    if ((page.level ?? 0) === level && page.mbrs) {
      for (const m of page.mbrs) {
        mbrs.push({ ...m, page_no: page.page_no });
      }
    }
  }
  return mbrs;
}

function drawMbrs(canvas, mbrs, bounds) {
  const ctx = canvas.getContext('2d');
  const w = canvas.width;
  const h = canvas.height;

  ctx.clearRect(0, 0, w, h);

  if (mbrs.length === 0) {
    ctx.fillStyle = '#6b7280';
    ctx.font = '14px sans-serif';
    ctx.textAlign = 'center';
    ctx.fillText('No MBRs at this level', w / 2, h / 2);
    return;
  }

  const drawW = w - PADDING * 2;
  const drawH = h - PADDING * 2;
  const dataW = bounds.maxX - bounds.minX || 1;
  const dataH = bounds.maxY - bounds.minY || 1;
  const scaleX = drawW / dataW;
  const scaleY = drawH / dataH;

  function toCanvasX(x) { return PADDING + (x - bounds.minX) * scaleX; }
  function toCanvasY(y) { return PADDING + drawH - (y - bounds.minY) * scaleY; }

  // Draw grid
  ctx.strokeStyle = '#374151';
  ctx.lineWidth = 0.5;
  for (let i = 0; i <= 4; i++) {
    const x = PADDING + (drawW * i) / 4;
    const y = PADDING + (drawH * i) / 4;
    ctx.beginPath();
    ctx.moveTo(x, PADDING);
    ctx.lineTo(x, PADDING + drawH);
    ctx.stroke();
    ctx.beginPath();
    ctx.moveTo(PADDING, y);
    ctx.lineTo(PADDING + drawW, y);
    ctx.stroke();
  }

  // Draw axis labels
  ctx.fillStyle = '#6b7280';
  ctx.font = '10px monospace';
  ctx.textAlign = 'center';
  for (let i = 0; i <= 4; i++) {
    const xVal = bounds.minX + (dataW * i) / 4;
    const yVal = bounds.minY + (dataH * i) / 4;
    ctx.fillText(xVal.toFixed(2), PADDING + (drawW * i) / 4, h - 5);
    ctx.textAlign = 'right';
    ctx.fillText(yVal.toFixed(2), PADDING - 5, PADDING + drawH - (drawH * i) / 4 + 3);
    ctx.textAlign = 'center';
  }

  // Draw MBRs with semi-transparent fill
  const hueStep = 360 / Math.max(mbrs.length, 1);
  for (let i = 0; i < mbrs.length; i++) {
    const m = mbrs[i];
    const x1 = toCanvasX(m.min_x);
    const y1 = toCanvasY(m.max_y);
    const x2 = toCanvasX(m.max_x);
    const y2 = toCanvasY(m.min_y);
    const rw = Math.max(x2 - x1, 1);
    const rh = Math.max(y2 - y1, 1);

    const hue = (hueStep * i) % 360;
    ctx.fillStyle = `hsla(${hue}, 70%, 50%, 0.15)`;
    ctx.strokeStyle = `hsla(${hue}, 70%, 50%, 0.6)`;
    ctx.lineWidth = 1;
    ctx.fillRect(x1, y1, rw, rh);
    ctx.strokeRect(x1, y1, rw, rh);
  }

  // Draw summary
  ctx.fillStyle = '#9ca3af';
  ctx.font = '11px sans-serif';
  ctx.textAlign = 'left';
  ctx.fillText(`${mbrs.length} MBRs`, PADDING, 15);
}

function renderPageTable(pages) {
  return `
    <table class="w-full text-xs font-mono">
      <thead class="sticky top-0 bg-gray-950">
        <tr class="text-left text-gray-500 border-b border-gray-800">
          <th scope="col" class="py-1 pr-3">Page</th>
          <th scope="col" class="py-1 pr-3">Level</th>
          <th scope="col" class="py-1 pr-3">Records</th>
          <th scope="col" class="py-1 pr-3">MBRs</th>
          <th scope="col" class="py-1 pr-3">Enclosing MBR</th>
        </tr>
      </thead>
      <tbody>
        ${pages.map((p) => {
          const enc = p.enclosing_mbr;
          const encStr = enc
            ? `(${enc.min_x.toFixed(2)}, ${enc.min_y.toFixed(2)}) \u2014 (${enc.max_x.toFixed(2)}, ${enc.max_y.toFixed(2)})`
            : '\u2014';
          return `
            <tr class="border-b border-gray-800/30 hover:bg-surface-2/50">
              <td class="py-1 pr-3 text-innodb-cyan">${p.page_no}</td>
              <td class="py-1 pr-3 text-gray-300">${p.level ?? 0}${(p.level ?? 0) === 0 ? ' (leaf)' : ''}</td>
              <td class="py-1 pr-3 text-gray-300">${p.record_count}</td>
              <td class="py-1 pr-3 text-gray-300">${p.mbrs?.length || 0}</td>
              <td class="py-1 pr-3 text-gray-400 text-[10px]">${encStr}</td>
            </tr>`;
        }).join('')}
      </tbody>
    </table>`;
}

function statCard(label, value, colorClass = '') {
  return `
    <div class="bg-surface-2 rounded-lg p-4">
      <div class="text-xs text-gray-500 uppercase tracking-wide">${esc(label)}</div>
      <div class="text-lg font-bold ${colorClass || 'text-gray-100'} mt-1">${value}</div>
    </div>`;
}
