// Page detail view — mirrors `inno pages`
import { getWasm } from '../wasm.js';

export function createPages(container, fileData) {
  const wasm = getWasm();

  // Initial render: page selector + all-pages summary
  let analysisAll;
  try {
    analysisAll = JSON.parse(wasm.analyze_pages(fileData, -1));
  } catch (e) {
    container.innerHTML = `<div class="p-6 text-red-400">Error analyzing pages: ${esc(e)}</div>`;
    return;
  }

  container.innerHTML = `
    <div class="p-6 space-y-4 overflow-auto max-h-full">
      <div class="flex items-center gap-4">
        <h2 class="text-lg font-bold text-innodb-cyan">Page Analysis</h2>
        <div class="flex items-center gap-2">
          <label class="text-sm text-gray-500">Page:</label>
          <input id="page-select" type="number" min="0" max="${analysisAll.length - 1}" value="0"
            class="w-24 px-2 py-1 bg-surface-2 border border-gray-700 rounded text-sm text-gray-200 focus:border-innodb-cyan focus:outline-none" />
          <span class="text-xs text-gray-600">of ${analysisAll.length - 1}</span>
        </div>
      </div>
      <div id="page-detail"></div>
      <h3 class="text-md font-semibold text-gray-300">All Pages Summary</h3>
      <div class="overflow-x-auto max-h-80">
        <table class="w-full text-xs font-mono">
          <thead class="sticky top-0 bg-gray-950">
            <tr class="text-left text-gray-500 border-b border-gray-800">
              <th class="py-1 pr-3">#</th>
              <th class="py-1 pr-3">Type</th>
              <th class="py-1 pr-3">LSN</th>
              <th class="py-1 pr-3">Checksum</th>
              <th class="py-1 pr-3">Prev</th>
              <th class="py-1 pr-3">Next</th>
              <th class="py-1 pr-3">Extra</th>
            </tr>
          </thead>
          <tbody>
            ${analysisAll.map(summaryRow).join('')}
          </tbody>
        </table>
      </div>
    </div>
  `;

  const input = container.querySelector('#page-select');
  const detail = container.querySelector('#page-detail');

  function showPage(num) {
    if (num < 0 || num >= analysisAll.length) return;
    const p = analysisAll[num];
    detail.innerHTML = renderDetail(p);
  }

  showPage(0);
  input.addEventListener('change', () => showPage(parseInt(input.value) || 0));
  input.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') showPage(parseInt(input.value) || 0);
  });

  // Click on summary row to select page
  container.querySelectorAll('tbody tr[data-page]').forEach((row) => {
    row.addEventListener('click', () => {
      const pn = parseInt(row.dataset.page);
      input.value = pn;
      showPage(pn);
      detail.scrollIntoView({ behavior: 'smooth' });
    });
  });
}

function renderDetail(p) {
  let extra = '';
  if (p.fsp_header) {
    extra += section('FSP Header', kvTable({
      'Space ID': p.fsp_header.space_id,
      'Size (pages)': p.fsp_header.size,
      'Flags': `0x${(p.fsp_header.flags ?? 0).toString(16)}`,
    }));
  }
  if (p.index_header) {
    const ih = p.index_header;
    extra += section('INDEX Header', kvTable({
      'Index ID': ih.index_id,
      'Level': ih.level,
      'N Recs': ih.n_recs,
      'Format': ih.format,
      'Heap Top': ih.heap_top,
      'N Heap': ih.n_heap,
      'Free': ih.free,
      'Garbage': ih.garbage,
      'Last Insert': ih.last_insert,
      'Direction': ih.direction,
      'N Direction': ih.n_direction,
    }));
  }
  if (p.undo_page_header) {
    extra += section('Undo Page Header', kvTable({
      'Type': p.undo_page_header.undo_page_type,
      'Last Log Offset': p.undo_page_header.last_log_offset,
      'Free Space': p.undo_page_header.free_space,
    }));
  }
  if (p.undo_segment_header) {
    extra += section('Undo Segment Header', kvTable({
      'State': p.undo_segment_header.state,
      'Last Log Offset': p.undo_segment_header.last_log_offset,
    }));
  }
  if (p.blob_header) {
    extra += section('BLOB Header', kvTable({
      'Part Len': p.blob_header.part_len,
      'Next Page': p.blob_header.next_page_no,
    }));
  }
  if (p.lob_header) {
    extra += section('LOB First Page Header', kvTable({
      'Version': p.lob_header.version,
      'Data Len': p.lob_header.data_len,
      'TRX ID': p.lob_header.trx_id,
    }));
  }

  return `
    <div class="bg-surface-2 rounded-lg p-4 space-y-3">
      <div class="flex items-center gap-3">
        <span class="text-innodb-cyan font-bold">Page ${p.page_number}</span>
        <span class="text-gray-400">${esc(p.page_type_name)}</span>
        <span class="text-xs text-gray-600">${esc(p.page_type_description)}</span>
      </div>
      ${kvTable({
        'Checksum': `0x${p.header.checksum.toString(16).padStart(8, '0')}`,
        'Page Number': p.header.page_number,
        'Prev': p.header.prev_page === 0xFFFFFFFF ? '—' : p.header.prev_page,
        'Next': p.header.next_page === 0xFFFFFFFF ? '—' : p.header.next_page,
        'LSN': p.header.lsn,
        'Page Type': `0x${p.header.page_type.toString(16)} (${esc(p.page_type_name)})`,
        'Flush LSN': p.header.flush_lsn,
        'Space ID': p.header.space_id,
      })}
      ${extra}
    </div>`;
}

function section(title, content) {
  return `<h4 class="text-sm font-semibold text-gray-400 mt-3">${esc(title)}</h4>${content}`;
}

function kvTable(obj) {
  const rows = Object.entries(obj)
    .map(([k, v]) => `<tr><td class="pr-4 py-0.5 text-gray-500 text-xs">${esc(k)}</td><td class="py-0.5 text-sm">${esc(String(v))}</td></tr>`)
    .join('');
  return `<table class="text-sm">${rows}</table>`;
}

function summaryRow(p) {
  let extra = '';
  if (p.index_header) extra = `idx:${p.index_header.index_id} lv:${p.index_header.level} recs:${p.index_header.n_recs}`;
  if (p.fsp_header) extra = `size:${p.fsp_header.size}`;
  return `
    <tr data-page="${p.page_number}" class="border-b border-gray-800/30 hover:bg-surface-2/50 cursor-pointer">
      <td class="py-1 pr-3 text-gray-400">${p.page_number}</td>
      <td class="py-1 pr-3 text-innodb-cyan">${esc(p.page_type_name)}</td>
      <td class="py-1 pr-3">${p.header.lsn}</td>
      <td class="py-1 pr-3 text-gray-500">0x${p.header.checksum.toString(16).padStart(8, '0')}</td>
      <td class="py-1 pr-3">${p.header.prev_page === 0xFFFFFFFF ? '—' : p.header.prev_page}</td>
      <td class="py-1 pr-3">${p.header.next_page === 0xFFFFFFFF ? '—' : p.header.next_page}</td>
      <td class="py-1 pr-3 text-gray-600 text-xs">${esc(extra)}</td>
    </tr>`;
}

function esc(s) {
  const d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}
