// Verify tab — structural integrity checks (mirrors `inno verify`)
import { getWasm } from '../wasm.js';
import { esc } from '../utils/html.js';
import { createExportBar } from '../utils/export.js';
import { requestPage, navigateToTab } from '../utils/navigation.js';

const PAGE_SIZE = 50;

/**
 * Create the verify tab for a single tablespace file.
 * @param {HTMLElement} container
 * @param {Uint8Array} fileData
 */
export function createVerify(container, fileData) {
  const wasm = getWasm();
  let result;
  try {
    result = JSON.parse(wasm.verify_tablespace(fileData));
  } catch (e) {
    container.innerHTML = `<div class="p-6 text-red-400">Error verifying tablespace: ${esc(String(e))}</div>`;
    return;
  }

  const summary = result.summary || {};
  const checks = result.checks || [];
  const findings = result.findings || [];
  let currentPage = 0;
  const totalPages = Math.max(1, Math.ceil(findings.length / PAGE_SIZE));

  container.innerHTML = `
    <div class="p-6 space-y-6 overflow-auto max-h-full">
      <div class="flex items-center gap-3">
        <h2 class="text-lg font-bold text-innodb-cyan">Verify</h2>
        <span id="verify-export"></span>
      </div>

      ${renderBanner(result.passed)}

      <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
        ${statCard('Total Checks', summary.total_checks ?? checks.length)}
        ${statCard('Passed', summary.passed_checks ?? 0, 'text-innodb-green')}
        ${statCard('Failed', summary.failed_checks ?? 0, (summary.failed_checks ?? 0) > 0 ? 'text-innodb-red' : '')}
        ${statCard('Total Findings', summary.total_findings ?? findings.length)}
      </div>

      <div class="grid grid-cols-3 gap-4">
        ${statCard('Errors', summary.error_count ?? 0, (summary.error_count ?? 0) > 0 ? 'text-innodb-red' : '')}
        ${statCard('Warnings', summary.warning_count ?? 0, (summary.warning_count ?? 0) > 0 ? 'text-innodb-amber' : '')}
        ${statCard('Info', summary.info_count ?? 0, 'text-innodb-cyan')}
      </div>

      <h3 class="text-md font-semibold text-gray-300">Check Summary</h3>
      <div class="overflow-x-auto">
        ${renderChecksTable(checks)}
      </div>

      ${findings.length > 0 ? `
        <div class="flex items-center gap-3">
          <h3 class="text-md font-semibold text-gray-300">Findings</h3>
          <span class="text-xs text-gray-600">Click a row to jump to that page</span>
        </div>
        <div id="verify-findings-wrap" class="overflow-x-auto max-h-96"></div>
        <div id="verify-pagination" class="flex items-center gap-3 text-sm"></div>
      ` : ''}
    </div>
  `;

  // Export bar
  const exportSlot = container.querySelector('#verify-export');
  if (exportSlot) {
    exportSlot.appendChild(createExportBar(() => result, 'verify'));
  }

  // Render findings with pagination
  if (findings.length > 0) {
    function renderFindings() {
      const start = currentPage * PAGE_SIZE;
      const pageFindings = findings.slice(start, start + PAGE_SIZE);
      const wrap = container.querySelector('#verify-findings-wrap');
      wrap.innerHTML = renderFindingsTable(pageFindings);

      // Click handler for findings rows
      wrap.querySelectorAll('tr[data-page-num]').forEach((tr) => {
        tr.classList.add('cursor-pointer');
        tr.addEventListener('click', () => {
          const pageNum = parseInt(tr.dataset.pageNum, 10);
          if (!isNaN(pageNum)) {
            requestPage(pageNum);
            navigateToTab('pages');
          }
        });
      });

      // Pagination controls
      const pag = container.querySelector('#verify-pagination');
      pag.innerHTML = `
        <button id="verify-prev" class="px-2 py-1 bg-surface-3 hover:bg-gray-600 text-gray-300 rounded text-xs ${currentPage === 0 ? 'opacity-50 cursor-not-allowed' : ''}">Prev</button>
        <span class="text-gray-400 text-xs">Page ${currentPage + 1} of ${totalPages}</span>
        <button id="verify-next" class="px-2 py-1 bg-surface-3 hover:bg-gray-600 text-gray-300 rounded text-xs ${currentPage >= totalPages - 1 ? 'opacity-50 cursor-not-allowed' : ''}">Next</button>
      `;

      const prevBtn = pag.querySelector('#verify-prev');
      const nextBtn = pag.querySelector('#verify-next');
      prevBtn.addEventListener('click', () => {
        if (currentPage > 0) { currentPage--; renderFindings(); }
      });
      nextBtn.addEventListener('click', () => {
        if (currentPage < totalPages - 1) { currentPage++; renderFindings(); }
      });
    }

    renderFindings();
  }
}

function renderBanner(passed) {
  if (passed) {
    return `
      <div class="bg-innodb-green/10 border border-innodb-green/30 rounded-lg p-4 flex items-center gap-3">
        <svg class="w-6 h-6 text-innodb-green flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7" />
        </svg>
        <span class="text-innodb-green font-bold">All checks passed</span>
      </div>`;
  }
  return `
    <div class="bg-innodb-red/10 border border-innodb-red/30 rounded-lg p-4 flex items-center gap-3">
      <svg class="w-6 h-6 text-innodb-red flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
      </svg>
      <span class="text-innodb-red font-bold">Some checks failed</span>
    </div>`;
}

function renderChecksTable(checks) {
  if (!checks || checks.length === 0) {
    return `<div class="text-gray-500 text-sm py-4">No checks available.</div>`;
  }

  return `
    <table class="w-full text-xs font-mono">
      <thead class="sticky top-0 bg-gray-950">
        <tr class="text-left text-gray-500 border-b border-gray-800">
          <th scope="col" class="py-1 pr-3">Check</th>
          <th scope="col" class="py-1 pr-3">Status</th>
          <th scope="col" class="py-1 pr-3">Details</th>
        </tr>
      </thead>
      <tbody>
        ${checks.map((c) => `
          <tr class="border-b border-gray-800/30 hover:bg-surface-2/50">
            <td class="py-1 pr-3 text-gray-300">${esc(c.name)}</td>
            <td class="py-1 pr-3">${c.passed
              ? '<span class="px-2 py-0.5 rounded-full bg-innodb-green/20 text-innodb-green text-xs font-bold">PASS</span>'
              : '<span class="px-2 py-0.5 rounded-full bg-innodb-red/20 text-innodb-red text-xs font-bold">FAIL</span>'
            }</td>
            <td class="py-1 pr-3 text-gray-400">${c.details ? esc(c.details) : '\u2014'}</td>
          </tr>
        `).join('')}
      </tbody>
    </table>`;
}

function renderFindingsTable(findings) {
  return `
    <table class="w-full text-xs font-mono">
      <thead class="sticky top-0 bg-gray-950">
        <tr class="text-left text-gray-500 border-b border-gray-800">
          <th scope="col" class="py-1 pr-3">Page</th>
          <th scope="col" class="py-1 pr-3">Check</th>
          <th scope="col" class="py-1 pr-3">Severity</th>
          <th scope="col" class="py-1 pr-3">Message</th>
        </tr>
      </thead>
      <tbody>
        ${findings.map((f) => {
          const sevClass = severityClass(f.severity);
          const hasPage = f.page_number != null;
          return `
            <tr class="border-b border-gray-800/30 hover:bg-surface-2/50" ${hasPage ? `data-page-num="${f.page_number}"` : ''}>
              <td class="py-1 pr-3 text-innodb-cyan">${hasPage ? f.page_number : '\u2014'}</td>
              <td class="py-1 pr-3 text-gray-300">${esc(f.check)}</td>
              <td class="py-1 pr-3">
                <span class="px-2 py-0.5 rounded-full ${sevClass.bg} ${sevClass.text} text-xs font-bold">${esc(f.severity)}</span>
              </td>
              <td class="py-1 pr-3 text-gray-400">${esc(f.message)}</td>
            </tr>`;
        }).join('')}
      </tbody>
    </table>`;
}

function severityClass(severity) {
  const s = (severity || '').toLowerCase();
  if (s === 'error') return { text: 'text-innodb-red', bg: 'bg-innodb-red/20' };
  if (s === 'warning') return { text: 'text-innodb-amber', bg: 'bg-innodb-amber/20' };
  return { text: 'text-innodb-cyan', bg: 'bg-innodb-cyan/20' };
}

function statCard(label, value, colorClass = '') {
  return `
    <div class="bg-surface-2 rounded-lg p-4">
      <div class="text-xs text-gray-500 uppercase tracking-wide">${esc(label)}</div>
      <div class="text-lg font-bold ${colorClass || 'text-gray-100'} mt-1">${value}</div>
    </div>`;
}
