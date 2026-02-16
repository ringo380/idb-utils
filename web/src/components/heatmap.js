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

const COLOR_MODES = [
  { id: 'type', label: 'Page Type' },
  { id: 'lsn', label: 'LSN Age' },
  { id: 'checksum', label: 'Checksum Status' },
];

export function createHeatmap(container, fileData, onPageClick) {
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

  // Precompute LSN range for age coloring
  let minLsn = Infinity, maxLsn = 0;
  for (const p of pages) {
    if (p.lsn > 0) {
      if (p.lsn < minLsn) minLsn = p.lsn;
      if (p.lsn > maxLsn) maxLsn = p.lsn;
    }
  }
  const lsnRange = maxLsn - minLsn || 1;

  // Lazy-loaded checksum data
  let checksumMap = null;

  let colorMode = 'type';

  container.innerHTML = `
    <div class="p-6 space-y-4 overflow-auto max-h-full">
      <div class="flex items-center gap-3 flex-wrap">
        <h2 class="text-lg font-bold text-innodb-cyan">Page Type Heatmap</h2>
        <span class="text-xs text-gray-500">${N.toLocaleString()} pages</span>
        <select id="heatmap-mode" class="px-2 py-1 bg-surface-3 text-gray-300 rounded text-xs border border-gray-700">
          ${COLOR_MODES.map(m => `<option value="${m.id}">${esc(m.label)}</option>`).join('')}
        </select>
        ${N > 1000 ? '<span class="text-xs text-gray-600">Scroll to zoom, drag to pan</span>' : ''}
      </div>
      <div id="heatmap-wrap" class="relative bg-surface-1 rounded-lg overflow-hidden cursor-pointer" style="height:400px;">
        <canvas id="heatmap-canvas"></canvas>
        <div id="heatmap-tooltip" class="absolute hidden pointer-events-none bg-gray-900 border border-gray-700 rounded px-2 py-1 text-xs text-gray-200 z-10"></div>
      </div>
      <div class="flex items-center gap-2 flex-wrap">
        ${N > 1000 ? '<button id="heatmap-reset" class="px-2 py-1 bg-surface-3 hover:bg-gray-600 text-gray-300 rounded text-xs">Reset Zoom</button>' : ''}
        <span class="text-xs text-gray-600">Click a cell to inspect the page</span>
      </div>
      <div id="heatmap-legend" class="flex flex-wrap gap-3 text-xs"></div>
    </div>
  `;

  const wrap = container.querySelector('#heatmap-wrap');
  const canvas = container.querySelector('#heatmap-canvas');
  const tooltip = container.querySelector('#heatmap-tooltip');
  const legendEl = container.querySelector('#heatmap-legend');
  const modeSelect = container.querySelector('#heatmap-mode');
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
  let didDrag = false;

  function cellSize() {
    const wrapW = wrap.clientWidth;
    const wrapH = wrap.clientHeight;
    const raw = Math.min(wrapW / cols, wrapH / rows);
    return Math.max(2, Math.min(24, raw));
  }

  function lsnColor(lsn) {
    if (lsn === 0) return '#1e293b';
    const t = (lsn - minLsn) / lsnRange;
    // Cold (blue) to hot (red) gradient
    const r = Math.round(t * 239 + (1 - t) * 30);
    const g = Math.round(t * 68 + (1 - t) * 64);
    const b = Math.round(t * 68 + (1 - t) * 175);
    return `rgb(${r},${g},${b})`;
  }

  function checksumColor(pageNum) {
    if (!checksumMap) return DEFAULT_COLOR;
    const entry = checksumMap.get(pageNum);
    if (!entry) return '#1e293b'; // empty page
    return entry.status === 'valid' ? '#10b981' : '#ef4444';
  }

  function getColor(p) {
    if (colorMode === 'lsn') return lsnColor(p.lsn);
    if (colorMode === 'checksum') return checksumColor(p.page_number);
    return PAGE_COLORS[p.page_type_name] || DEFAULT_COLOR;
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
        ctx.fillStyle = getColor(p);
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

  function loadChecksums() {
    if (checksumMap) return;
    try {
      const report = JSON.parse(wasm.validate_checksums(fileData));
      checksumMap = new Map();
      for (const p of report.pages) {
        checksumMap.set(p.page_number, p);
      }
    } catch {
      checksumMap = new Map();
    }
  }

  function updateLegend() {
    if (colorMode === 'type') {
      legendEl.innerHTML = sortedTypes.map(([name, count]) => {
        const color = PAGE_COLORS[name] || DEFAULT_COLOR;
        return `<div class="flex items-center gap-1">
          <span class="inline-block w-3 h-3 rounded-sm" style="background:${color}"></span>
          <span class="text-gray-400">${esc(name)}</span>
          <span class="text-gray-600">(${count})</span>
        </div>`;
      }).join('');
    } else if (colorMode === 'lsn') {
      legendEl.innerHTML = `
        <div class="flex items-center gap-2">
          <span class="text-gray-400">Oldest LSN</span>
          <div class="w-32 h-3 rounded" style="background:linear-gradient(to right, rgb(30,64,175), rgb(239,68,68))"></div>
          <span class="text-gray-400">Newest LSN</span>
          <span class="text-gray-600 ml-2">Range: ${minLsn.toLocaleString()} — ${maxLsn.toLocaleString()}</span>
        </div>`;
    } else if (colorMode === 'checksum') {
      const valid = checksumMap ? [...checksumMap.values()].filter(e => e.status === 'valid').length : 0;
      const invalid = checksumMap ? [...checksumMap.values()].filter(e => e.status !== 'valid').length : 0;
      legendEl.innerHTML = `
        <div class="flex items-center gap-1">
          <span class="inline-block w-3 h-3 rounded-sm" style="background:#10b981"></span>
          <span class="text-gray-400">Valid</span>
          <span class="text-gray-600">(${valid})</span>
        </div>
        <div class="flex items-center gap-1">
          <span class="inline-block w-3 h-3 rounded-sm" style="background:#ef4444"></span>
          <span class="text-gray-400">Invalid</span>
          <span class="text-gray-600">(${invalid})</span>
        </div>
        <div class="flex items-center gap-1">
          <span class="inline-block w-3 h-3 rounded-sm" style="background:#1e293b"></span>
          <span class="text-gray-400">Empty</span>
        </div>`;
    }
  }

  // Color mode selector
  modeSelect.addEventListener('change', () => {
    colorMode = modeSelect.value;
    if (colorMode === 'checksum') loadChecksums();
    updateLegend();
    requestAnimationFrame(render);
  });

  canvas.addEventListener('mousemove', (e) => {
    if (dragging) {
      panX = dragPanX + (e.offsetX - dragStartX);
      panY = dragPanY + (e.offsetY - dragStartY);
      didDrag = true;
      requestAnimationFrame(render);
      tooltip.classList.add('hidden');
      return;
    }
    const p = getPageAtMouse(e.offsetX, e.offsetY);
    if (p) {
      tooltip.classList.remove('hidden');
      tooltip.innerHTML = `<strong>Page ${esc(String(p.page_number))}</strong><br>${esc(p.page_type_name)}<br>LSN: ${esc(String(p.lsn))}`;
      tooltip.style.left = `${Math.min(e.offsetX + 12, wrap.clientWidth - 150)}px`;
      tooltip.style.top = `${Math.min(e.offsetY + 12, wrap.clientHeight - 60)}px`;
    } else {
      tooltip.classList.add('hidden');
    }
  });

  canvas.addEventListener('mouseleave', () => {
    tooltip.classList.add('hidden');
  });

  // Click-to-inspect: navigate to Pages tab for the clicked page
  canvas.addEventListener('click', (e) => {
    if (didDrag) {
      didDrag = false;
      return;
    }
    const p = getPageAtMouse(e.offsetX, e.offsetY);
    if (p && onPageClick) {
      onPageClick(p.page_number);
    }
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
      didDrag = false;
      dragStartX = e.offsetX;
      dragStartY = e.offsetY;
      dragPanX = panX;
      dragPanY = panY;
      canvas.style.cursor = 'grabbing';
    });

    // Use canvas-scoped listener instead of window to avoid memory leak
    canvas.addEventListener('mouseup', () => {
      dragging = false;
      canvas.style.cursor = '';
    });

    canvas.addEventListener('mouseleave', () => {
      if (dragging) {
        dragging = false;
        canvas.style.cursor = '';
      }
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

  // Initial render + legend
  updateLegend();
  requestAnimationFrame(render);
  const ro = new ResizeObserver(() => requestAnimationFrame(render));
  ro.observe(wrap);
}
