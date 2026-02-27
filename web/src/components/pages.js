// Page detail view — mirrors `inno pages`
import { getWasm } from '../wasm.js';
import { esc } from '../utils/html.js';
import { createExportBar, downloadText, downloadJson, copyToClipboard } from '../utils/export.js';
import { consumeRequestedPage, consumeIndexFilter } from '../utils/navigation.js';

export function createPages(container, fileData) {
  const wasm = getWasm();

  // Initial render: page selector + all-pages summary
  let analysisAll;
  try {
    analysisAll = JSON.parse(wasm.analyze_pages(fileData, -1n));
  } catch (e) {
    container.innerHTML = `<div class="p-6 text-red-400">Error analyzing pages: ${esc(e)}</div>`;
    return;
  }

  // Build page_number -> analysis lookup for O(1) access by page number.
  // This handles gaps from corrupt/unparseable pages where array indices
  // would not match page numbers.
  const pageMap = new Map();
  for (const p of analysisAll) pageMap.set(p.page_number, p);
  const maxPageNum = analysisAll.length > 0
    ? Math.max(...analysisAll.map((p) => p.page_number))
    : 0;

  // Check for cross-tab navigation requests
  const requestedPage = consumeRequestedPage();
  const indexFilter = consumeIndexFilter();

  // Filter summary data if an index filter was requested
  const filteredAnalysis = indexFilter != null
    ? analysisAll.filter((p) => p.index_header && String(p.index_header.index_id) === String(indexFilter))
    : analysisAll;

  container.innerHTML = `
    <div class="p-6 space-y-4 overflow-auto max-h-full">
      ${indexFilter != null ? `
        <div id="index-filter-banner" class="flex items-center gap-2 px-3 py-2 rounded bg-innodb-cyan/10 border border-innodb-cyan/30 text-sm text-innodb-cyan">
          <span>Filtered to Index ID: ${esc(String(indexFilter))}</span>
          <button id="clear-index-filter" class="px-2 py-0.5 text-xs bg-surface-3 hover:bg-gray-600 text-gray-300 rounded">clear</button>
        </div>
      ` : ''}
      <div class="flex items-center gap-4">
        <h2 class="text-lg font-bold text-innodb-cyan">Page Analysis</h2>
        <span id="pages-export"></span>
        <div class="flex items-center gap-2">
          <label class="text-sm text-gray-500">Page:</label>
          <input id="page-select" type="number" min="0" max="${maxPageNum}" value="0"
            class="w-24 px-2 py-1 bg-surface-2 border border-gray-700 rounded text-sm text-gray-200 focus:border-innodb-cyan focus:outline-none" />
          <span class="text-xs text-gray-600">of ${maxPageNum}</span>
        </div>
      </div>
      <div id="page-detail"></div>
      <h3 class="text-md font-semibold text-gray-300">All Pages Summary</h3>
      <div class="overflow-x-auto max-h-80">
        <table class="w-full text-xs font-mono">
          <thead class="sticky top-0 bg-gray-950">
            <tr class="text-left text-gray-500 border-b border-gray-800">
              <th scope="col" class="py-1 pr-3">#</th>
              <th scope="col" class="py-1 pr-3">Type</th>
              <th scope="col" class="py-1 pr-3">LSN</th>
              <th scope="col" class="py-1 pr-3">Checksum</th>
              <th scope="col" class="py-1 pr-3">Prev</th>
              <th scope="col" class="py-1 pr-3">Next</th>
              <th scope="col" class="py-1 pr-3">Extra</th>
            </tr>
          </thead>
          <tbody>
            ${filteredAnalysis.map(summaryRow).join('')}
          </tbody>
        </table>
      </div>
    </div>
  `;

  // Clear index filter handler
  const clearBtn = container.querySelector('#clear-index-filter');
  if (clearBtn) {
    clearBtn.addEventListener('click', () => {
      // Re-render without filter by calling createPages again
      container.innerHTML = '';
      createPages(container, fileData);
    });
  }

  const exportSlot = container.querySelector('#pages-export');
  if (exportSlot) {
    exportSlot.appendChild(createExportBar(() => analysisAll, 'pages'));
  }

  const input = container.querySelector('#page-select');
  const detail = container.querySelector('#page-detail');

  // Cache for decoded records per page (avoids re-fetching)
  const decodedCache = {};

  function showPage(num) {
    const p = pageMap.get(num);
    if (!p) return;
    detail.innerHTML = renderDetail(p);

    // Wire up "View Records" button for INDEX pages (raw records)
    const viewRecsBtn = detail.querySelector('#view-records-btn');
    if (viewRecsBtn) {
      viewRecsBtn.addEventListener('click', () => {
        const recsDiv = detail.querySelector('#records-section');
        if (!recsDiv) return;
        if (recsDiv.dataset.loaded === 'true') {
          recsDiv.classList.toggle('hidden');
          viewRecsBtn.textContent = recsDiv.classList.contains('hidden') ? 'View Records' : 'Hide Records';
          return;
        }
        try {
          const report = JSON.parse(wasm.inspect_index_records(fileData, BigInt(num)));
          recsDiv.dataset.loaded = 'true';
          recsDiv.classList.remove('hidden');
          viewRecsBtn.textContent = 'Hide Records';
          recsDiv.innerHTML = renderRecords(report);
        } catch (e) {
          recsDiv.classList.remove('hidden');
          recsDiv.innerHTML = `<div class="text-red-400 text-xs py-2">Error: ${esc(String(e))}</div>`;
        }
      });
    }

    // Wire up "Decoded Records" button
    const decodedBtn = detail.querySelector('#decoded-records-btn');
    if (decodedBtn) {
      decodedBtn.addEventListener('click', () => {
        const decodedDiv = detail.querySelector('#decoded-section');
        if (!decodedDiv) return;

        // Toggle visibility if already loaded
        if (decodedDiv.dataset.loaded === 'decoded') {
          decodedDiv.classList.toggle('hidden');
          decodedBtn.textContent = decodedDiv.classList.contains('hidden') ? 'Decoded Records' : 'Hide Decoded';
          toggleExportButtons(detail, !decodedDiv.classList.contains('hidden'));
          return;
        }

        try {
          const raw = wasm.export_records(fileData, BigInt(num), false, false);
          if (raw === 'null') {
            decodedDiv.classList.remove('hidden');
            decodedDiv.innerHTML = `<div class="text-gray-500 text-xs py-2">No SDI metadata available for decoded records.</div>`;
            decodedDiv.dataset.loaded = 'decoded';
            decodedBtn.textContent = 'Hide Decoded';
            return;
          }
          const decoded = JSON.parse(raw);
          decodedCache[num] = decoded;
          decodedDiv.dataset.loaded = 'decoded';
          decodedDiv.classList.remove('hidden');
          decodedBtn.textContent = 'Hide Decoded';
          decodedDiv.innerHTML = renderDecodedRecords(decoded);
          toggleExportButtons(detail, true);
        } catch (e) {
          decodedDiv.classList.remove('hidden');
          decodedDiv.innerHTML = `<div class="text-red-400 text-xs py-2">Error: ${esc(String(e))}</div>`;
        }
      });
    }

    // Wire up Download CSV button
    const dlCsvBtn = detail.querySelector('#dl-csv-btn');
    if (dlCsvBtn) {
      dlCsvBtn.addEventListener('click', () => {
        const decoded = decodedCache[num];
        if (!decoded) return;
        const csv = generateCsv(decoded);
        downloadText(csv, 'records.csv');
      });
    }

    // Wire up Download JSON button
    const dlJsonBtn = detail.querySelector('#dl-json-btn');
    if (dlJsonBtn) {
      dlJsonBtn.addEventListener('click', () => {
        const decoded = decodedCache[num];
        if (!decoded) return;
        downloadJson(decoded, 'records');
      });
    }

    // Wire up Copy SQL INSERT button
    const copySqlBtn = detail.querySelector('#copy-sql-btn');
    if (copySqlBtn) {
      copySqlBtn.addEventListener('click', () => {
        const decoded = decodedCache[num];
        if (!decoded) return;
        const sql = generateSqlInserts(decoded);
        copyToClipboard(sql, copySqlBtn);
      });
    }
  }

  // Navigate to requested page if set, otherwise start at page 0
  const initialPage = requestedPage != null ? requestedPage : 0;
  input.value = initialPage;
  showPage(initialPage);

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

function toggleExportButtons(detail, visible) {
  const dlCsv = detail.querySelector('#dl-csv-btn');
  const dlJson = detail.querySelector('#dl-json-btn');
  const copySql = detail.querySelector('#copy-sql-btn');
  if (dlCsv) dlCsv.classList.toggle('hidden', !visible);
  if (dlJson) dlJson.classList.toggle('hidden', !visible);
  if (copySql) copySql.classList.toggle('hidden', !visible);
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
        'Prev': p.header.prev_page === 0xFFFFFFFF ? '\u2014' : p.header.prev_page,
        'Next': p.header.next_page === 0xFFFFFFFF ? '\u2014' : p.header.next_page,
        'LSN': p.header.lsn,
        'Page Type': `0x${p.header.page_type.toString(16)} (${esc(p.page_type_name)})`,
        'Flush LSN': p.header.flush_lsn,
        'Space ID': p.header.space_id,
      })}
      ${extra}
      ${p.index_header ? `
        <div class="flex items-center gap-2 mt-2 flex-wrap">
          <button id="view-records-btn" class="px-2 py-1 bg-surface-3 hover:bg-gray-600 text-gray-300 rounded text-xs">View Records</button>
          <button id="decoded-records-btn" class="px-2 py-1 bg-surface-3 hover:bg-gray-600 text-gray-300 rounded text-xs">Decoded Records</button>
          <button id="dl-csv-btn" class="px-2 py-1 bg-surface-3 hover:bg-gray-600 text-gray-300 rounded text-xs hidden">Download CSV</button>
          <button id="dl-json-btn" class="px-2 py-1 bg-surface-3 hover:bg-gray-600 text-gray-300 rounded text-xs hidden">Download JSON</button>
          <button id="copy-sql-btn" class="px-2 py-1 bg-surface-3 hover:bg-gray-600 text-gray-300 rounded text-xs hidden">Copy SQL INSERT</button>
          <span class="text-xs text-gray-600">${p.index_header.n_recs} records</span>
        </div>
        <div id="records-section" class="hidden mt-2"></div>
        <div id="decoded-section" class="hidden mt-2"></div>
      ` : ''}
    </div>`;
}

function renderRecords(report) {
  if (!report.records || report.records.length === 0) {
    return `<div class="text-gray-500 text-xs py-2">No user records found on this page.</div>`;
  }
  return `
    <div class="text-xs text-gray-500 mb-1">
      Index ID: ${report.index_id} | Level: ${report.level} | Format: ${report.is_compact ? 'Compact' : 'Redundant'}
    </div>
    <div class="overflow-x-auto max-h-64">
      <table class="w-full text-xs font-mono">
        <thead class="sticky top-0 bg-gray-950">
          <tr class="text-left text-gray-500 border-b border-gray-800">
            <th scope="col" class="py-1 pr-2">#</th>
            <th scope="col" class="py-1 pr-2">Type</th>
            <th scope="col" class="py-1 pr-2">Heap#</th>
            <th scope="col" class="py-1 pr-2">Owned</th>
            <th scope="col" class="py-1 pr-2">Del</th>
            <th scope="col" class="py-1 pr-2">MinRec</th>
            <th scope="col" class="py-1 pr-2">Next</th>
            <th scope="col" class="py-1 pr-2">Raw Bytes</th>
          </tr>
        </thead>
        <tbody>
          ${report.records.map((r, i) => `
            <tr class="border-b border-gray-800/30 hover:bg-surface-2/50">
              <td class="py-1 pr-2 text-gray-400">${i + 1}</td>
              <td class="py-1 pr-2 text-innodb-cyan">${esc(r.rec_type)}</td>
              <td class="py-1 pr-2">${r.heap_no}</td>
              <td class="py-1 pr-2">${r.n_owned}</td>
              <td class="py-1 pr-2 ${r.delete_mark ? 'text-innodb-red' : ''}">${r.delete_mark ? 'Y' : 'N'}</td>
              <td class="py-1 pr-2">${r.min_rec ? 'Y' : 'N'}</td>
              <td class="py-1 pr-2">${r.next_offset}</td>
              <td class="py-1 pr-2 text-gray-500">${esc(r.raw_hex)}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>`;
}

function renderDecodedRecords(decoded) {
  if (!decoded.columns || !decoded.rows || decoded.rows.length === 0) {
    return `<div class="text-gray-500 text-xs py-2">No decoded records found on this page.</div>`;
  }
  return `
    <div class="text-xs text-gray-500 mb-1">
      Table: ${esc(decoded.table_name)} | ${decoded.total_rows} row${decoded.total_rows !== 1 ? 's' : ''} | ${decoded.columns.length} column${decoded.columns.length !== 1 ? 's' : ''}
    </div>
    <div class="overflow-x-auto max-h-80">
      <table class="w-full text-xs font-mono">
        <thead class="sticky top-0 bg-gray-950">
          <tr class="text-left text-gray-500 border-b border-gray-800">
            <th scope="col" class="py-1 pr-2">#</th>
            ${decoded.columns.map((c) => `<th scope="col" class="py-1 pr-2">${esc(c)}</th>`).join('')}
          </tr>
        </thead>
        <tbody>
          ${decoded.rows.map((row, i) => `
            <tr class="border-b border-gray-800/30 hover:bg-surface-2/50">
              <td class="py-1 pr-2 text-gray-400">${i + 1}</td>
              ${row.map((val) => {
                if (val === null) {
                  return `<td class="py-1 pr-2"><span class="text-gray-500 italic">NULL</span></td>`;
                }
                const isNum = typeof val === 'number';
                return `<td class="py-1 pr-2${isNum ? ' text-right' : ''}">${esc(String(val))}</td>`;
              }).join('')}
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>`;
}

function generateCsv(decoded) {
  const header = decoded.columns.map(csvEscape).join(',');
  const rows = decoded.rows.map((row) =>
    row.map((val) => {
      if (val === null) return '';
      return csvEscape(String(val));
    }).join(',')
  );
  return [header, ...rows].join('\n');
}

function csvEscape(val) {
  if (typeof val !== 'string') return String(val);
  if (val.includes(',') || val.includes('"') || val.includes('\n') || val.includes('\r')) {
    return '"' + val.replace(/"/g, '""') + '"';
  }
  return val;
}

function generateSqlInserts(decoded) {
  const tableName = decoded.table_name || 'unknown_table';
  const quotedTable = '`' + tableName.replace(/`/g, '``') + '`';
  const quotedCols = decoded.columns.map((c) => '`' + c.replace(/`/g, '``') + '`').join(', ');

  return decoded.rows.map((row) => {
    const values = row.map((val) => {
      if (val === null) return 'NULL';
      if (typeof val === 'number') return String(val);
      // String value: single-quote with internal single quotes doubled
      const escaped = String(val).replace(/'/g, "''");
      return "'" + escaped + "'";
    }).join(', ');
    return `INSERT INTO ${quotedTable} (${quotedCols}) VALUES (${values});`;
  }).join('\n');
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
      <td class="py-1 pr-3">${p.header.prev_page === 0xFFFFFFFF ? '\u2014' : p.header.prev_page}</td>
      <td class="py-1 pr-3">${p.header.next_page === 0xFFFFFFFF ? '\u2014' : p.header.next_page}</td>
      <td class="py-1 pr-3 text-gray-600 text-xs">${esc(extra)}</td>
    </tr>`;
}
