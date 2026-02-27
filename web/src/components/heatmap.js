// Page type heatmap — canvas-based visualization of page types
import { getWasm } from '../wasm.js';
import { esc } from '../utils/html.js';
import { requestPage, navigateToTab } from '../utils/navigation.js';

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

const BASE_COLOR_MODES = [
  { id: 'type', label: 'Page Type' },
  { id: 'lsn', label: 'LSN Age' },
  { id: 'checksum', label: 'Checksum Status' },
];

export function createHeatmap(container, fileData, onPageClick, diffResult = null) {
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

  // Build color modes list — include diff when diff data is available
  const COLOR_MODES = [...BASE_COLOR_MODES];
  if (diffResult) {
    COLOR_MODES.push({ id: 'diff', label: 'Diff Status' });
  }

  // Build diff lookup map for O(1) access — modified_pages only contains pages that differ
  let diffMap = null;
  if (diffResult && diffResult.modified_pages) {
    diffMap = new Map();
    for (const entry of diffResult.modified_pages) {
      diffMap.set(entry.page_number, entry);
    }
  }

  // Page size for intensity scaling (default 16384)
  const pageSize = parsed.page_size || 16384;

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
        <button id="lsn-timeline-toggle" class="px-2 py-1 bg-surface-3 hover:bg-gray-600 text-gray-300 rounded text-xs">LSN Timeline</button>
        <span class="text-xs text-gray-600">Click a cell to inspect the page</span>
      </div>
      <div id="lsn-timeline-wrap" class="hidden relative bg-surface-1 rounded-lg overflow-hidden" style="height:200px;">
        <canvas id="lsn-timeline-canvas"></canvas>
        <div id="lsn-timeline-tooltip" class="absolute hidden pointer-events-none bg-gray-900 border border-gray-700 rounded px-2 py-1 text-xs text-gray-200 z-10"></div>
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

  function diffColor(pageNum) {
    if (!diffResult) return '#1e293b';
    // Pages beyond one file's range
    const pc1 = diffResult.page_count_1 || 0;
    const pc2 = diffResult.page_count_2 || 0;
    if (pageNum >= pc2 && pageNum < pc1) return '#f97316'; // only in file 1
    if (pageNum >= pc1 && pageNum < pc2) return '#8b5cf6'; // only in file 2
    // Modified pages are in the diffMap; absent pages are identical
    if (!diffMap) return '#10b981';
    const entry = diffMap.get(pageNum);
    if (!entry) return '#10b981'; // identical
    // Modified — intensity by bytes changed
    const intensity = Math.min(1, (entry.bytes_changed || 0) / pageSize);
    const r = Math.round(127 + intensity * 112); // 127..239
    const g = Math.round(40 + (1 - intensity) * 28); // 40..68
    const b = Math.round(40 + (1 - intensity) * 28); // 40..68
    return `rgb(${r},${g},${b})`;
  }

  function getColor(p) {
    if (colorMode === 'diff') return diffColor(p.page_number);
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
    } else if (colorMode === 'diff') {
      legendEl.innerHTML = `
        <div class="flex items-center gap-1">
          <span class="inline-block w-3 h-3 rounded-sm" style="background:#10b981"></span>
          <span class="text-gray-400">Identical</span>
          <span class="text-gray-600">(${diffResult.identical || 0})</span>
        </div>
        <div class="flex items-center gap-1">
          <span class="inline-block w-3 h-3 rounded-sm" style="background:rgb(239,68,68)"></span>
          <span class="text-gray-400">Modified</span>
          <span class="text-gray-600">(${diffResult.modified || 0})</span>
        </div>
        <div class="flex items-center gap-1">
          <span class="inline-block w-3 h-3 rounded-sm" style="background:#f97316"></span>
          <span class="text-gray-400">Only in file 1</span>
          <span class="text-gray-600">(${diffResult.only_in_first || 0})</span>
        </div>
        <div class="flex items-center gap-1">
          <span class="inline-block w-3 h-3 rounded-sm" style="background:#8b5cf6"></span>
          <span class="text-gray-400">Only in file 2</span>
          <span class="text-gray-600">(${diffResult.only_in_second || 0})</span>
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

  // ── LSN Timeline ──────────────────────────────────────────────────────
  const tlToggle = container.querySelector('#lsn-timeline-toggle');
  const tlWrap = container.querySelector('#lsn-timeline-wrap');
  const tlCanvas = container.querySelector('#lsn-timeline-canvas');
  const tlTooltip = container.querySelector('#lsn-timeline-tooltip');
  const tlCtx = tlCanvas.getContext('2d');
  let tlVisible = false;

  // Filter out pages with LSN === 0 for the scatter plot
  const tlPages = pages.filter(p => p.lsn > 0);

  // Compute LSN range with 5% padding
  let tlMinLsn = Infinity, tlMaxLsn = 0;
  for (const p of tlPages) {
    if (p.lsn < tlMinLsn) tlMinLsn = p.lsn;
    if (p.lsn > tlMaxLsn) tlMaxLsn = p.lsn;
  }
  const tlLsnSpan = tlMaxLsn - tlMinLsn || 1;
  const tlPadding = tlLsnSpan * 0.05;
  const tlLsnMin = tlMinLsn - tlPadding;
  const tlLsnMax = tlMaxLsn + tlPadding;
  const tlLsnRange = tlLsnMax - tlLsnMin || 1;

  // Layout constants for timeline
  const TL_LEFT_MARGIN = 80;
  const TL_RIGHT_MARGIN = 16;
  const TL_TOP_MARGIN = 12;
  const TL_BOTTOM_MARGIN = 24;

  function renderTimeline() {
    if (!tlVisible) return;
    const w = tlWrap.clientWidth;
    const h = tlWrap.clientHeight;
    tlCanvas.width = w;
    tlCanvas.height = h;

    const plotW = w - TL_LEFT_MARGIN - TL_RIGHT_MARGIN;
    const plotH = h - TL_TOP_MARGIN - TL_BOTTOM_MARGIN;
    if (plotW <= 0 || plotH <= 0) return;

    // Background
    tlCtx.fillStyle = '#0f172a';
    tlCtx.fillRect(0, 0, w, h);

    // Grid lines
    tlCtx.strokeStyle = 'rgba(255,255,255,0.05)';
    tlCtx.lineWidth = 1;
    const numTicks = 4;
    for (let i = 0; i <= numTicks; i++) {
      const y = TL_TOP_MARGIN + (plotH * i) / numTicks;
      tlCtx.beginPath();
      tlCtx.moveTo(TL_LEFT_MARGIN, y);
      tlCtx.lineTo(w - TL_RIGHT_MARGIN, y);
      tlCtx.stroke();
    }

    // Y axis labels (LSN values)
    tlCtx.fillStyle = '#9ca3af';
    tlCtx.font = '10px monospace';
    tlCtx.textAlign = 'right';
    tlCtx.textBaseline = 'middle';
    for (let i = 0; i <= numTicks; i++) {
      const y = TL_TOP_MARGIN + (plotH * i) / numTicks;
      // i=0 is top (max LSN), i=numTicks is bottom (min LSN)
      const lsnVal = tlLsnMax - ((tlLsnMax - tlLsnMin) * i) / numTicks;
      tlCtx.fillText(formatLsnLabel(lsnVal), TL_LEFT_MARGIN - 6, y);
    }

    // X axis label
    tlCtx.fillStyle = '#6b7280';
    tlCtx.font = '10px sans-serif';
    tlCtx.textAlign = 'center';
    tlCtx.textBaseline = 'top';
    tlCtx.fillText('Page Number', TL_LEFT_MARGIN + plotW / 2, h - 12);

    // Plot dots
    const maxPage = N - 1 || 1;
    for (const p of tlPages) {
      const x = TL_LEFT_MARGIN + (p.page_number / maxPage) * plotW;
      const y = TL_TOP_MARGIN + plotH - ((p.lsn - tlLsnMin) / tlLsnRange) * plotH;
      tlCtx.fillStyle = PAGE_COLORS[p.page_type_name] || DEFAULT_COLOR;
      tlCtx.beginPath();
      tlCtx.arc(x, y, 2, 0, Math.PI * 2);
      tlCtx.fill();
    }
  }

  function formatLsnLabel(val) {
    if (val >= 1e9) return (val / 1e9).toFixed(1) + 'G';
    if (val >= 1e6) return (val / 1e6).toFixed(1) + 'M';
    if (val >= 1e3) return (val / 1e3).toFixed(1) + 'K';
    return Math.round(val).toString();
  }

  function getTimelinePageAt(mx, my) {
    const w = tlWrap.clientWidth;
    const h = tlWrap.clientHeight;
    const plotW = w - TL_LEFT_MARGIN - TL_RIGHT_MARGIN;
    const plotH = h - TL_TOP_MARGIN - TL_BOTTOM_MARGIN;
    if (plotW <= 0 || plotH <= 0) return null;

    const maxPage = N - 1 || 1;
    let closest = null;
    let closestDist = Infinity;

    for (const p of tlPages) {
      const x = TL_LEFT_MARGIN + (p.page_number / maxPage) * plotW;
      const y = TL_TOP_MARGIN + plotH - ((p.lsn - tlLsnMin) / tlLsnRange) * plotH;
      const dist = Math.sqrt((mx - x) ** 2 + (my - y) ** 2);
      if (dist < closestDist && dist < 10) {
        closestDist = dist;
        closest = p;
      }
    }
    return closest;
  }

  tlToggle.addEventListener('click', () => {
    tlVisible = !tlVisible;
    tlWrap.classList.toggle('hidden', !tlVisible);
    tlToggle.textContent = tlVisible ? 'Hide LSN Timeline' : 'LSN Timeline';
    if (tlVisible) requestAnimationFrame(renderTimeline);
  });

  tlCanvas.addEventListener('mousemove', (e) => {
    const rect = tlCanvas.getBoundingClientRect();
    const mx = e.clientX - rect.left;
    const my = e.clientY - rect.top;
    const p = getTimelinePageAt(mx, my);
    if (p) {
      tlTooltip.classList.remove('hidden');
      tlTooltip.innerHTML = `<strong>Page ${esc(String(p.page_number))}</strong><br>${esc(p.page_type_name)}<br>LSN: ${esc(String(p.lsn))}`;
      tlTooltip.style.left = `${Math.min(mx + 12, tlWrap.clientWidth - 160)}px`;
      tlTooltip.style.top = `${Math.min(my + 12, tlWrap.clientHeight - 50)}px`;
    } else {
      tlTooltip.classList.add('hidden');
    }
  });

  tlCanvas.addEventListener('mouseleave', () => {
    tlTooltip.classList.add('hidden');
  });

  tlCanvas.addEventListener('click', (e) => {
    const rect = tlCanvas.getBoundingClientRect();
    const mx = e.clientX - rect.left;
    const my = e.clientY - rect.top;
    const p = getTimelinePageAt(mx, my);
    if (p) {
      requestPage(p.page_number);
      navigateToTab('pages');
    }
  });

  const tlRo = new ResizeObserver(() => {
    if (tlVisible) requestAnimationFrame(renderTimeline);
  });
  tlRo.observe(tlWrap);

  // ── Initial render + legend ───────────────────────────────────────────
  updateLegend();
  requestAnimationFrame(render);
  const ro = new ResizeObserver(() => requestAnimationFrame(render));
  ro.observe(wrap);
}
