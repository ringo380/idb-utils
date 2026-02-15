// Page type heatmap — canvas-based visualization of page types
import { getWasm } from '../wasm.js';
import { esc } from '../utils/html.js';

const PAGE_COLORS = {
  'INDEX': '#3b82f6',
  'UNDO_LOG': '#f59e0b',
  'FSP_HDR': '#22d3ee',
  'INODE': '#a855f7',
  'ALLOCATED': '#4b5563',
  'BLOB': '#f97316',
  'SDI': '#10b981',
  'IBUF_BITMAP': '#ec4899',
  'IBUF_FREE_LIST': '#f472b6',
  'XDES': '#06b6d4',
  'TRX_SYS': '#8b5cf6',
  'SYS': '#6366f1',
  'ZBLOB': '#fb923c',
  'ZBLOB2': '#fdba74',
  'RTREE': '#84cc16',
  'LOB_FIRST': '#14b8a6',
  'LOB_DATA': '#2dd4bf',
  'LOB_INDEX': '#5eead4',
  'COMPRESSED': '#78716c',
  'ENCRYPTED': '#ef4444',
  'COMPRESSED_ENCRYPTED': '#dc2626',
  'ENCRYPTED_RTREE': '#b91c1c',
};
const DEFAULT_COLOR = '#6b7280';

export function createHeatmap(container, fileData) {
  const wasm = getWasm();
  let parsed;
  try {
    parsed = JSON.parse(wasm.parse_tablespace(fileData));
  } catch (e) {
    container.innerHTML = `<div class="p-6 text-red-400">Error parsing tablespace: ${esc(e)}</div>`;
    return;
  }

  const pages = parsed.pages;
  const N = pages.length;
  if (N === 0) {
    container.innerHTML = `<div class="p-6 text-gray-500">No pages to display.</div>`;
    return;
  }

  // Build type→color map and count
  const typeCounts = {};
  for (const p of pages) {
    typeCounts[p.page_type_name] = (typeCounts[p.page_type_name] || 0) + 1;
  }
  const sortedTypes = Object.entries(typeCounts).sort((a, b) => b[1] - a[1]);

  container.innerHTML = `
    <div class="p-6 space-y-4 overflow-auto max-h-full">
      <div class="flex items-center gap-3">
        <h2 class="text-lg font-bold text-innodb-cyan">Page Type Heatmap</h2>
        <span class="text-xs text-gray-500">${N.toLocaleString()} pages</span>
        ${N > 1000 ? '<span class="text-xs text-gray-600">Scroll to zoom, drag to pan</span>' : ''}
      </div>
      <div id="heatmap-wrap" class="relative bg-surface-1 rounded-lg overflow-hidden" style="height:400px;">
        <canvas id="heatmap-canvas"></canvas>
        <div id="heatmap-tooltip" class="absolute hidden pointer-events-none bg-gray-900 border border-gray-700 rounded px-2 py-1 text-xs text-gray-200 z-10"></div>
      </div>
      ${N > 1000 ? '<button id="heatmap-reset" class="px-2 py-1 bg-surface-3 hover:bg-gray-600 text-gray-300 rounded text-xs">Reset Zoom</button>' : ''}
      <div id="heatmap-legend" class="flex flex-wrap gap-3 text-xs"></div>
    </div>
  `;

  const wrap = container.querySelector('#heatmap-wrap');
  const canvas = container.querySelector('#heatmap-canvas');
  const tooltip = container.querySelector('#heatmap-tooltip');
  const legendEl = container.querySelector('#heatmap-legend');
  const ctx = canvas.getContext('2d');

  // Layout calculation
  const cols = Math.ceil(Math.sqrt(N * 1.5));
  const rows = Math.ceil(N / cols);

  // Zoom/pan state
  let zoom = 1;
  let panX = 0;
  let panY = 0;
  let dragging = false;
  let dragStartX = 0;
  let dragStartY = 0;
  let dragPanX = 0;
  let dragPanY = 0;

  function cellSize() {
    const wrapW = wrap.clientWidth;
    const wrapH = wrap.clientHeight;
    const raw = Math.min(wrapW / cols, wrapH / rows);
    return Math.max(2, Math.min(24, raw));
  }

  function render() {
    const cs = cellSize() * zoom;
    canvas.width = wrap.clientWidth;
    canvas.height = wrap.clientHeight;
    ctx.clearRect(0, 0, canvas.width, canvas.height);

    const offsetX = panX;
    const offsetY = panY;

    // Determine visible range
    const startCol = Math.max(0, Math.floor(-offsetX / cs));
    const endCol = Math.min(cols, Math.ceil((canvas.width - offsetX) / cs));
    const startRow = Math.max(0, Math.floor(-offsetY / cs));
    const endRow = Math.min(rows, Math.ceil((canvas.height - offsetY) / cs));

    for (let row = startRow; row < endRow; row++) {
      for (let col = startCol; col < endCol; col++) {
        const idx = row * cols + col;
        if (idx >= N) break;
        const p = pages[idx];
        ctx.fillStyle = PAGE_COLORS[p.page_type_name] || DEFAULT_COLOR;
        const x = col * cs + offsetX;
        const y = row * cs + offsetY;
        if (cs > 3) {
          ctx.fillRect(x + 0.5, y + 0.5, cs - 1, cs - 1);
        } else {
          ctx.fillRect(x, y, cs, cs);
        }
      }
    }
  }

  function getPageAtMouse(mx, my) {
    const cs = cellSize() * zoom;
    const col = Math.floor((mx - panX) / cs);
    const row = Math.floor((my - panY) / cs);
    if (col < 0 || col >= cols || row < 0 || row >= rows) return null;
    const idx = row * cols + col;
    if (idx >= N) return null;
    return pages[idx];
  }

  canvas.addEventListener('mousemove', (e) => {
    if (dragging) {
      panX = dragPanX + (e.offsetX - dragStartX);
      panY = dragPanY + (e.offsetY - dragStartY);
      requestAnimationFrame(render);
      tooltip.classList.add('hidden');
      return;
    }
    const p = getPageAtMouse(e.offsetX, e.offsetY);
    if (p) {
      tooltip.classList.remove('hidden');
      tooltip.innerHTML = `<strong>Page ${p.page_number}</strong><br>${esc(p.page_type_name)}<br>LSN: ${p.lsn}`;
      tooltip.style.left = `${Math.min(e.offsetX + 12, wrap.clientWidth - 150)}px`;
      tooltip.style.top = `${Math.min(e.offsetY + 12, wrap.clientHeight - 60)}px`;
    } else {
      tooltip.classList.add('hidden');
    }
  });

  canvas.addEventListener('mouseleave', () => {
    tooltip.classList.add('hidden');
  });

  if (N > 1000) {
    canvas.addEventListener('wheel', (e) => {
      e.preventDefault();
      const factor = e.deltaY > 0 ? 0.9 : 1.1;
      const newZoom = Math.max(0.5, Math.min(20, zoom * factor));
      // Zoom toward mouse position
      const mx = e.offsetX;
      const my = e.offsetY;
      panX = mx - (mx - panX) * (newZoom / zoom);
      panY = my - (my - panY) * (newZoom / zoom);
      zoom = newZoom;
      requestAnimationFrame(render);
    }, { passive: false });

    canvas.addEventListener('mousedown', (e) => {
      dragging = true;
      dragStartX = e.offsetX;
      dragStartY = e.offsetY;
      dragPanX = panX;
      dragPanY = panY;
      canvas.style.cursor = 'grabbing';
    });

    window.addEventListener('mouseup', () => {
      dragging = false;
      canvas.style.cursor = '';
    });

    const resetBtn = container.querySelector('#heatmap-reset');
    if (resetBtn) {
      resetBtn.addEventListener('click', () => {
        zoom = 1;
        panX = 0;
        panY = 0;
        requestAnimationFrame(render);
      });
    }
  }

  // Legend
  legendEl.innerHTML = sortedTypes.map(([name, count]) => {
    const color = PAGE_COLORS[name] || DEFAULT_COLOR;
    return `<div class="flex items-center gap-1">
      <span class="inline-block w-3 h-3 rounded-sm" style="background:${color}"></span>
      <span class="text-gray-400">${esc(name)}</span>
      <span class="text-gray-600">(${count})</span>
    </div>`;
  }).join('');

  // Initial render + resize handler
  requestAnimationFrame(render);
  const ro = new ResizeObserver(() => requestAnimationFrame(render));
  ro.observe(wrap);
}
