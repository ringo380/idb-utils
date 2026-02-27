// B+Tree canvas visualization for the Health tab
import { esc } from '../utils/html.js';
import { requestPage, navigateToTab } from '../utils/navigation.js';

const NODE_W = 70;
const NODE_H = 50;
const GAP_X = 10;
const GAP_Y = 60;
const BORDER_R = 4;
const DEFAULT_PAGE_SIZE = 16384;
const PAGE_DATA_OFFSET = 94; // FIL header (38) + INDEX header (36) + FSEG headers (20)
const FIL_TRAILER_SIZE = 8;
const NULL_PAGE = 4294967295; // 0xFFFFFFFF

/**
 * Build index groups from pre-parsed page analysis data.
 * Returns a Map of index_id -> { levels: Map<level, pageNode[]>, maxLevel }
 */
function buildIndexes(pages) {
  const indexPages = pages.filter(
    (p) => p.page_type_name === 'INDEX' && p.index_header
  );
  if (indexPages.length === 0) return new Map();

  // Group by index_id
  const byIndex = new Map();
  for (const p of indexPages) {
    const id = p.index_header.index_id;
    if (!byIndex.has(id)) byIndex.set(id, []);
    byIndex.get(id).push(p);
  }

  const result = new Map();
  for (const [indexId, pagesArr] of byIndex) {
    // Group by level
    const byLevel = new Map();
    let maxLevel = 0;
    for (const p of pagesArr) {
      const lvl = p.index_header.level;
      if (lvl > maxLevel) maxLevel = lvl;
      if (!byLevel.has(lvl)) byLevel.set(lvl, []);
      byLevel.get(lvl).push(p);
    }

    // Order leaf pages (level 0) by prev/next chain
    if (byLevel.has(0)) {
      byLevel.set(0, orderLeafPages(byLevel.get(0)));
    }

    // Non-leaf levels: order by page number
    for (const [lvl, arr] of byLevel) {
      if (lvl !== 0) {
        arr.sort((a, b) => a.page_number - b.page_number);
      }
    }

    result.set(indexId, { levels: byLevel, maxLevel });
  }
  return result;
}

/**
 * Order leaf pages by following the prev_page/next_page linked list.
 * Falls back to page_number ordering if the chain is broken.
 */
function orderLeafPages(leaves) {
  if (leaves.length <= 1) return leaves;

  const byNum = new Map();
  for (const p of leaves) byNum.set(p.page_number, p);

  // Find the first leaf: no valid prev_page pointing to another leaf in our set
  let first = null;
  for (const p of leaves) {
    const prev = p.header.prev_page;
    if (prev === NULL_PAGE || !byNum.has(prev)) {
      first = p;
      break;
    }
  }

  if (!first) {
    // Chain is circular or broken, fall back to page number sort
    return [...leaves].sort((a, b) => a.page_number - b.page_number);
  }

  const ordered = [];
  const visited = new Set();
  let current = first;
  while (current && !visited.has(current.page_number)) {
    visited.add(current.page_number);
    ordered.push(current);
    const next = current.header.next_page;
    current = (next !== NULL_PAGE && byNum.has(next)) ? byNum.get(next) : null;
  }

  // If some pages weren't reached, append them sorted by page number
  if (ordered.length < leaves.length) {
    const remaining = leaves
      .filter((p) => !visited.has(p.page_number))
      .sort((a, b) => a.page_number - b.page_number);
    ordered.push(...remaining);
  }

  return ordered;
}

/**
 * Compute fill factor for an INDEX page.
 * Matches the canonical formula from src/innodb/health.rs:
 *   usable = page_size - PAGE_DATA_OFFSET - FIL_TRAILER_SIZE
 *   used   = heap_top - PAGE_DATA_OFFSET - garbage
 */
function fillFactor(p) {
  if (!p.index_header) return 0;
  const pageSize = DEFAULT_PAGE_SIZE;
  const usable = pageSize - PAGE_DATA_OFFSET - FIL_TRAILER_SIZE;
  if (usable <= 0) return 0;
  const heapTop = p.index_header.heap_top || 0;
  const garbage = p.index_header.garbage || 0;
  const used = heapTop - PAGE_DATA_OFFSET - garbage;
  return Math.max(0, Math.min(1, used / usable));
}

/**
 * Get fill color as HSL string based on fill factor.
 */
function fillColor(ff) {
  if (ff >= 0.70) return 'hsl(120, 60%, 35%)';
  if (ff >= 0.40) return 'hsl(50, 70%, 40%)';
  return 'hsl(0, 65%, 40%)';
}

/**
 * Draw a rounded rectangle path.
 */
function roundRect(ctx, x, y, w, h, r) {
  ctx.beginPath();
  ctx.moveTo(x + r, y);
  ctx.lineTo(x + w - r, y);
  ctx.arc(x + w - r, y + r, r, -Math.PI / 2, 0);
  ctx.lineTo(x + w, y + h - r);
  ctx.arc(x + w - r, y + h - r, r, 0, Math.PI / 2);
  ctx.lineTo(x + r, y + h);
  ctx.arc(x + r, y + h - r, r, Math.PI / 2, Math.PI);
  ctx.lineTo(x, y + r);
  ctx.arc(x + r, y + r, r, Math.PI, (3 * Math.PI) / 2);
  ctx.closePath();
}

/**
 * Create an interactive B+Tree visualization.
 * @param {HTMLElement} container - Element to render into
 * @param {Uint8Array} fileData - Raw .ibd file bytes (kept for future use)
 * @param {Array} parsedPagesData - Pre-parsed pages array from wasm.analyze_pages()
 */
export function createBTree(container, _fileData, parsedPagesData) {
  const indexes = buildIndexes(parsedPagesData);

  if (indexes.size === 0) {
    container.innerHTML =
      '<div class="p-4 text-gray-500 text-sm">No INDEX pages found in this tablespace.</div>';
    return;
  }

  const indexIds = [...indexes.keys()].sort((a, b) => a - b);
  let selectedIndexId = indexIds[0];

  // Build DOM
  container.innerHTML = `
    <div class="space-y-2">
      ${indexIds.length > 1 ? `
        <div class="flex items-center gap-2">
          <label class="text-xs text-gray-500">Index:</label>
          <select id="btree-index-select" class="px-2 py-1 bg-surface-3 text-gray-300 rounded text-xs border border-gray-700">
            ${indexIds.map((id) => `<option value="${id}">Index ${id}</option>`).join('')}
          </select>
          <span class="text-xs text-gray-600">Scroll to zoom, drag to pan</span>
        </div>
      ` : '<span class="text-xs text-gray-600">Scroll to zoom, drag to pan</span>'}
      <div id="btree-wrap" class="relative bg-surface-1 rounded-lg overflow-hidden" style="height:400px;cursor:grab;">
        <canvas id="btree-canvas"></canvas>
        <div id="btree-tooltip" class="absolute hidden pointer-events-none bg-gray-900 border border-gray-700 rounded px-2 py-1 text-xs text-gray-200 z-10 whitespace-nowrap"></div>
      </div>
      <div class="flex items-center gap-3 flex-wrap">
        <button id="btree-reset" class="px-2 py-1 bg-surface-3 hover:bg-gray-600 text-gray-300 rounded text-xs">Reset Zoom</button>
        <span class="text-xs text-gray-600">Click a node to inspect the page</span>
        <div class="flex items-center gap-3 ml-auto">
          <div class="flex items-center gap-1">
            <span class="inline-block w-3 h-3 rounded-sm" style="background:hsl(120,60%,35%)"></span>
            <span class="text-xs text-gray-500">Fill &ge; 70%</span>
          </div>
          <div class="flex items-center gap-1">
            <span class="inline-block w-3 h-3 rounded-sm" style="background:hsl(50,70%,40%)"></span>
            <span class="text-xs text-gray-500">Fill &ge; 40%</span>
          </div>
          <div class="flex items-center gap-1">
            <span class="inline-block w-3 h-3 rounded-sm" style="background:hsl(0,65%,40%)"></span>
            <span class="text-xs text-gray-500">Fill &lt; 40%</span>
          </div>
        </div>
      </div>
    </div>
  `;

  const wrap = container.querySelector('#btree-wrap');
  const canvas = container.querySelector('#btree-canvas');
  const tooltip = container.querySelector('#btree-tooltip');
  const ctx = canvas.getContext('2d');
  const indexSelect = container.querySelector('#btree-index-select');

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

  // Cached layout: array of { page, x, y, level } in world coordinates
  let nodeLayout = [];

  /**
   * Compute node positions for the currently selected index.
   * Root at top, leaves at bottom. Centered horizontally.
   */
  function computeLayout() {
    nodeLayout = [];
    const indexData = indexes.get(selectedIndexId);
    if (!indexData) return;

    const { levels, maxLevel } = indexData;

    // Find the widest level to determine total width
    let maxLevelWidth = 0;
    for (const [, arr] of levels) {
      const w = arr.length * (NODE_W + GAP_X) - GAP_X;
      if (w > maxLevelWidth) maxLevelWidth = w;
    }

    // Layout each level, top = root (maxLevel), bottom = leaf (0)
    for (let displayRow = 0; displayRow <= maxLevel; displayRow++) {
      const treeLevel = maxLevel - displayRow;
      const arr = levels.get(treeLevel) || [];
      const levelWidth = arr.length * (NODE_W + GAP_X) - GAP_X;
      const offsetX = (maxLevelWidth - levelWidth) / 2;
      const y = displayRow * (NODE_H + GAP_Y);

      for (let i = 0; i < arr.length; i++) {
        const x = offsetX + i * (NODE_W + GAP_X);
        nodeLayout.push({ page: arr[i], x, y, level: treeLevel });
      }
    }
  }

  /**
   * Auto-fit zoom and pan so the tree is centered and visible.
   */
  function autoFit() {
    if (nodeLayout.length === 0) return;

    const canvasW = wrap.clientWidth;
    const canvasH = wrap.clientHeight;
    if (canvasW === 0 || canvasH === 0) return;

    let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
    for (const n of nodeLayout) {
      if (n.x < minX) minX = n.x;
      if (n.y < minY) minY = n.y;
      if (n.x + NODE_W > maxX) maxX = n.x + NODE_W;
      if (n.y + NODE_H > maxY) maxY = n.y + NODE_H;
    }

    const treeW = maxX - minX;
    const treeH = maxY - minY;
    const padding = 40;

    const scaleX = (canvasW - padding * 2) / treeW;
    const scaleY = (canvasH - padding * 2) / treeH;
    zoom = Math.min(scaleX, scaleY, 2);
    zoom = Math.max(0.5, Math.min(20, zoom));

    panX = (canvasW - treeW * zoom) / 2 - minX * zoom;
    panY = (canvasH - treeH * zoom) / 2 - minY * zoom;
  }

  /**
   * Render the B+Tree on the canvas.
   */
  function render() {
    canvas.width = wrap.clientWidth;
    canvas.height = wrap.clientHeight;
    ctx.clearRect(0, 0, canvas.width, canvas.height);

    if (nodeLayout.length === 0) {
      ctx.fillStyle = '#6b7280';
      ctx.font = '13px sans-serif';
      ctx.textAlign = 'center';
      ctx.fillText('No pages for this index.', canvas.width / 2, canvas.height / 2);
      return;
    }

    const indexData = indexes.get(selectedIndexId);
    if (!indexData) return;
    const { maxLevel } = indexData;

    ctx.save();

    // Draw edges between levels (non-leaf -> child level)
    ctx.strokeStyle = 'rgba(107, 114, 128, 0.35)';
    ctx.lineWidth = 1;
    for (let displayRow = 0; displayRow < maxLevel; displayRow++) {
      const parentLevel = maxLevel - displayRow;
      const childLevel = parentLevel - 1;
      const parents = nodeLayout.filter((n) => n.level === parentLevel);
      const children = nodeLayout.filter((n) => n.level === childLevel);

      if (parents.length === 0 || children.length === 0) continue;

      // Distribute children evenly among parents
      const childrenPerParent = Math.ceil(children.length / parents.length);
      for (let pi = 0; pi < parents.length; pi++) {
        const parent = parents[pi];
        const px = parent.x * zoom + panX + (NODE_W * zoom) / 2;
        const py = parent.y * zoom + panY + NODE_H * zoom;

        const startChild = pi * childrenPerParent;
        const endChild = Math.min(startChild + childrenPerParent, children.length);
        for (let ci = startChild; ci < endChild; ci++) {
          const child = children[ci];
          const cx = child.x * zoom + panX + (NODE_W * zoom) / 2;
          const cy = child.y * zoom + panY;

          ctx.beginPath();
          ctx.moveTo(px, py);
          ctx.lineTo(cx, cy);
          ctx.stroke();
        }
      }
    }

    // Draw leaf sibling links (dashed cyan horizontal arrows)
    const leafNodes = nodeLayout.filter((n) => n.level === 0);
    if (leafNodes.length > 1) {
      // Build a lookup from page_number to node for chain following
      const leafByPageNum = new Map();
      for (const n of leafNodes) leafByPageNum.set(n.page.page_number, n);

      ctx.save();
      ctx.strokeStyle = 'rgba(34, 211, 238, 0.5)';
      ctx.lineWidth = 1;
      ctx.setLineDash([4, 3]);
      for (const n of leafNodes) {
        const nextPageNum = n.page.header.next_page;
        if (nextPageNum === NULL_PAGE) continue;
        const nextNode = leafByPageNum.get(nextPageNum);
        if (!nextNode) continue;

        const x1 = n.x * zoom + panX + NODE_W * zoom;
        const y1 = n.y * zoom + panY + (NODE_H * zoom) / 2;
        const x2 = nextNode.x * zoom + panX;
        const y2 = nextNode.y * zoom + panY + (NODE_H * zoom) / 2;

        ctx.beginPath();
        ctx.moveTo(x1, y1);
        ctx.lineTo(x2, y2);
        ctx.stroke();

        // Arrow head
        const arrowLen = 5 * zoom;
        const angle = Math.atan2(y2 - y1, x2 - x1);
        ctx.beginPath();
        ctx.moveTo(x2, y2);
        ctx.lineTo(
          x2 - arrowLen * Math.cos(angle - Math.PI / 6),
          y2 - arrowLen * Math.sin(angle - Math.PI / 6)
        );
        ctx.moveTo(x2, y2);
        ctx.lineTo(
          x2 - arrowLen * Math.cos(angle + Math.PI / 6),
          y2 - arrowLen * Math.sin(angle + Math.PI / 6)
        );
        ctx.stroke();
      }
      ctx.restore();
    }

    // Draw nodes
    for (const n of nodeLayout) {
      const sx = n.x * zoom + panX;
      const sy = n.y * zoom + panY;
      const sw = NODE_W * zoom;
      const sh = NODE_H * zoom;

      // Cull off-screen nodes
      if (sx + sw < 0 || sx > canvas.width || sy + sh < 0 || sy > canvas.height) continue;

      const ff = fillFactor(n.page);
      const bgColor = fillColor(ff);

      // Fill
      roundRect(ctx, sx, sy, sw, sh, BORDER_R * zoom);
      ctx.fillStyle = bgColor;
      ctx.fill();

      // Border
      ctx.strokeStyle = 'rgba(156, 163, 175, 0.4)';
      ctx.lineWidth = 1;
      ctx.stroke();

      // Text (only if node is large enough)
      if (sw > 20) {
        const fontSize1 = Math.max(7, Math.min(11, 11 * zoom));
        const fontSize2 = Math.max(6, Math.min(10, 10 * zoom));

        ctx.textAlign = 'center';
        ctx.textBaseline = 'middle';

        // Line 1: page number
        ctx.fillStyle = '#ffffff';
        ctx.font = `bold ${fontSize1}px monospace`;
        ctx.fillText(`P${n.page.page_number}`, sx + sw / 2, sy + sh * 0.35);

        // Line 2: record count
        ctx.fillStyle = '#d1d5db';
        ctx.font = `${fontSize2}px monospace`;
        const nRecs = n.page.index_header ? n.page.index_header.n_recs : 0;
        ctx.fillText(`${nRecs} recs`, sx + sw / 2, sy + sh * 0.65);
      }
    }

    ctx.restore();
  }

  /**
   * Find node at canvas coordinates.
   */
  function getNodeAt(mx, my) {
    for (let i = nodeLayout.length - 1; i >= 0; i--) {
      const n = nodeLayout[i];
      const sx = n.x * zoom + panX;
      const sy = n.y * zoom + panY;
      const sw = NODE_W * zoom;
      const sh = NODE_H * zoom;
      if (mx >= sx && mx <= sx + sw && my >= sy && my <= sy + sh) {
        return n;
      }
    }
    return null;
  }

  // --- Event handlers ---

  // Mouse move: tooltip or drag
  canvas.addEventListener('mousemove', (e) => {
    if (dragging) {
      panX = dragPanX + (e.offsetX - dragStartX);
      panY = dragPanY + (e.offsetY - dragStartY);
      didDrag = true;
      requestAnimationFrame(render);
      tooltip.classList.add('hidden');
      return;
    }

    const n = getNodeAt(e.offsetX, e.offsetY);
    if (n) {
      const ff = fillFactor(n.page);
      const nRecs = n.page.index_header ? n.page.index_header.n_recs : 0;
      tooltip.innerHTML = [
        `<strong>Page ${esc(String(n.page.page_number))}</strong>`,
        `Level: ${esc(String(n.level))}`,
        `Index: ${esc(String(n.page.index_header.index_id))}`,
        `Records: ${esc(String(nRecs))}`,
        `Fill: ${(ff * 100).toFixed(1)}%`,
      ].join('<br>');
      tooltip.classList.remove('hidden');
      tooltip.style.left = `${Math.min(e.offsetX + 12, wrap.clientWidth - 160)}px`;
      tooltip.style.top = `${Math.min(e.offsetY + 12, wrap.clientHeight - 90)}px`;
      canvas.style.cursor = 'pointer';
    } else {
      tooltip.classList.add('hidden');
      canvas.style.cursor = dragging ? 'grabbing' : 'grab';
    }
  });

  canvas.addEventListener('mouseleave', () => {
    tooltip.classList.add('hidden');
    if (dragging) {
      dragging = false;
      canvas.style.cursor = 'grab';
    }
  });

  // Click: navigate to page detail
  canvas.addEventListener('click', (e) => {
    if (didDrag) {
      didDrag = false;
      return;
    }
    const n = getNodeAt(e.offsetX, e.offsetY);
    if (n) {
      requestPage(n.page.page_number);
      navigateToTab('pages');
    }
  });

  // Wheel: zoom toward mouse
  canvas.addEventListener(
    'wheel',
    (e) => {
      e.preventDefault();
      const factor = e.deltaY > 0 ? 0.9 : 1.1;
      const newZoom = Math.max(0.5, Math.min(20, zoom * factor));
      const mx = e.offsetX;
      const my = e.offsetY;
      panX = mx - (mx - panX) * (newZoom / zoom);
      panY = my - (my - panY) * (newZoom / zoom);
      zoom = newZoom;
      requestAnimationFrame(render);
    },
    { passive: false }
  );

  // Drag: pan
  canvas.addEventListener('mousedown', (e) => {
    dragging = true;
    didDrag = false;
    dragStartX = e.offsetX;
    dragStartY = e.offsetY;
    dragPanX = panX;
    dragPanY = panY;
    canvas.style.cursor = 'grabbing';
  });

  canvas.addEventListener('mouseup', () => {
    dragging = false;
    canvas.style.cursor = 'grab';
  });

  // Reset zoom button
  const resetBtn = container.querySelector('#btree-reset');
  if (resetBtn) {
    resetBtn.addEventListener('click', () => {
      computeLayout();
      autoFit();
      requestAnimationFrame(render);
    });
  }

  // Index selector
  if (indexSelect) {
    indexSelect.addEventListener('change', () => {
      selectedIndexId = parseInt(indexSelect.value, 10);
      computeLayout();
      autoFit();
      requestAnimationFrame(render);
    });
  }

  // Initial layout + render
  computeLayout();
  autoFit();
  requestAnimationFrame(render);

  // Responsive resize
  const ro = new ResizeObserver(() => {
    requestAnimationFrame(render);
  });
  ro.observe(wrap);
}
