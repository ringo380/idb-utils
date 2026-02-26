// Compat tab — version compatibility checks (mirrors `inno compat`)
import { getWasm } from '../wasm.js';
import { esc } from '../utils/html.js';
import { createExportBar } from '../utils/export.js';

const TARGET_VERSIONS = ['5.7.0', '8.0.0', '8.4.0', '9.0.0', '9.1.0'];

/**
 * Create the compat tab for a single tablespace file.
 * @param {HTMLElement} container
 * @param {Uint8Array} fileData
 */
export function createCompat(container, fileData) {
  const wasm = getWasm();

  // Detect source version from SDI metadata
  let detectedVersion = null;
  try {
    const sdiRecords = JSON.parse(wasm.extract_sdi(fileData));
    if (sdiRecords && sdiRecords.length > 0) {
      const first = sdiRecords[0];
      const dataObj = typeof first.data === 'string' ? JSON.parse(first.data) : first.data;
      const versionId = dataObj?.mysqld_version_id ?? dataObj?.dd_object?.mysqld_version_id;
      if (versionId) {
        const major = Math.floor(versionId / 10000);
        const minor = Math.floor((versionId % 10000) / 100);
        const patch = versionId % 100;
        detectedVersion = `${major}.${minor}.${patch}`;
      }
    }
  } catch {
    // SDI extraction may fail for older tablespaces
  }

  // Determine default target version (next version up from detected)
  let defaultTarget = TARGET_VERSIONS[TARGET_VERSIONS.length - 1];
  if (detectedVersion) {
    const majorMinor = detectedVersion.split('.').slice(0, 2).join('.');
    if (majorMinor === '5.7') defaultTarget = '8.0.0';
    else if (majorMinor === '8.0') defaultTarget = '8.4.0';
    else if (majorMinor === '8.4') defaultTarget = '9.0.0';
    else if (majorMinor === '9.0') defaultTarget = '9.1.0';
  }

  let selectedVersion = defaultTarget;
  let compatResult = null;

  container.innerHTML = `
    <div class="p-6 space-y-6 overflow-auto max-h-full">
      <div class="flex items-center gap-3">
        <h2 class="text-lg font-bold text-innodb-cyan">Compatibility Check</h2>
        <span id="compat-export"></span>
      </div>

      <div class="bg-surface-2 rounded-lg p-4 flex items-center gap-4 flex-wrap">
        <div>
          <span class="text-xs text-gray-500 uppercase tracking-wide">Source Version</span>
          <div class="text-sm font-bold text-gray-200 mt-1">${detectedVersion ? esc(detectedVersion) : 'Unknown'}</div>
        </div>
        <div class="text-gray-600 text-lg">\u2192</div>
        <div>
          <label class="text-xs text-gray-500 uppercase tracking-wide block">Target Version</label>
          <select id="compat-version-select" class="mt-1 px-2 py-1 bg-surface-3 border border-gray-700 rounded text-sm text-gray-300 focus:outline-none focus:border-innodb-cyan">
            ${TARGET_VERSIONS.map((v) => `<option value="${v}" ${v === selectedVersion ? 'selected' : ''}>${v}</option>`).join('')}
          </select>
        </div>
      </div>

      <div id="compat-result"></div>
    </div>
  `;

  // Export bar
  const exportSlot = container.querySelector('#compat-export');
  if (exportSlot) {
    exportSlot.appendChild(createExportBar(() => compatResult, 'compat'));
  }

  function runCheck() {
    const resultDiv = container.querySelector('#compat-result');
    try {
      compatResult = JSON.parse(wasm.check_compatibility(fileData, selectedVersion));
    } catch (e) {
      resultDiv.innerHTML = `<div class="text-red-400">Error checking compatibility: ${esc(String(e))}</div>`;
      return;
    }

    const summary = compatResult.summary || {};
    const checks = compatResult.checks || [];

    resultDiv.innerHTML = `
      <div class="space-y-6">
        <div class="flex items-center gap-4 flex-wrap">
          <span class="text-sm text-gray-400">
            ${esc(compatResult.source_version || detectedVersion || 'Unknown')}
            \u2192
            ${esc(compatResult.target_version || selectedVersion)}
          </span>
          ${compatResult.compatible
            ? '<span class="px-3 py-1 rounded-full bg-innodb-green/20 text-innodb-green text-sm font-bold">Compatible</span>'
            : '<span class="px-3 py-1 rounded-full bg-innodb-red/20 text-innodb-red text-sm font-bold">Incompatible</span>'
          }
        </div>

        <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
          ${statCard('Total Checks', summary.total_checks ?? checks.length)}
          ${statCard('Errors', summary.error_count ?? 0, (summary.error_count ?? 0) > 0 ? 'text-innodb-red' : '')}
          ${statCard('Warnings', summary.warning_count ?? 0, (summary.warning_count ?? 0) > 0 ? 'text-innodb-amber' : '')}
          ${statCard('Info', summary.info_count ?? 0, 'text-innodb-cyan')}
        </div>

        ${checks.length > 0 ? `
          <h3 class="text-md font-semibold text-gray-300">Checks</h3>
          <div class="overflow-x-auto">
            ${renderCompatChecks(checks)}
          </div>
        ` : `
          <div class="text-gray-500 text-sm py-4">No compatibility checks returned.</div>
        `}
      </div>
    `;
  }

  // Initial check
  runCheck();

  // Version selector change handler
  const versionSelect = container.querySelector('#compat-version-select');
  versionSelect.addEventListener('change', () => {
    selectedVersion = versionSelect.value;
    runCheck();
  });
}

function renderCompatChecks(checks) {
  return `
    <table class="w-full text-xs font-mono">
      <thead class="sticky top-0 bg-gray-950">
        <tr class="text-left text-gray-500 border-b border-gray-800">
          <th scope="col" class="py-1 pr-3">Check</th>
          <th scope="col" class="py-1 pr-3">Severity</th>
          <th scope="col" class="py-1 pr-3">Message</th>
        </tr>
      </thead>
      <tbody>
        ${checks.map((c) => {
          const sev = severityClass(c.severity);
          return `
            <tr class="border-b border-gray-800/30 hover:bg-surface-2/50">
              <td class="py-1 pr-3 text-gray-300">${esc(c.name)}</td>
              <td class="py-1 pr-3">
                <span class="px-2 py-0.5 rounded-full ${sev.bg} ${sev.text} text-xs font-bold">${esc(c.severity)}</span>
              </td>
              <td class="py-1 pr-3 text-gray-400">${esc(c.message)}</td>
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
