// App initialization — WASM loading, routing, component orchestration
import { initWasm, getWasm } from './wasm.js';
import { esc } from './utils/html.js';
import { initTheme, createThemeToggle } from './utils/theme.js';
import { initKeyboard } from './utils/keyboard.js';
import { createDropzone } from './components/dropzone.js';
import { createTabs, setActiveTab, getTabId, getTabCount } from './components/tabs.js';
import { createOverview } from './components/overview.js';
import { createPages } from './components/pages.js';
import { createChecksums } from './components/checksums.js';
import { createSdi } from './components/sdi.js';
import { createHex } from './components/hex.js';
import { createDiff } from './components/diff.js';
import { createRecovery } from './components/recovery.js';
import { createRedoLog } from './components/redolog.js';
import { createHeatmap } from './components/heatmap.js';
import { downloadJson } from './utils/export.js';

import './style.css';

const app = document.getElementById('app');
let currentTab = 0;
let fileData = null;
let fileName = null;
let diffData = null; // { name1, data1, name2, data2 }
let pageCount = 0;
let isRedoLog = false;
let decryptedData = null;
let encryptionInfo = null;

// ── Bootstrap ────────────────────────────────────────────────────────
initTheme();

(async () => {
  showLoading('Loading WASM module...');
  try {
    await initWasm();
  } catch (e) {
    app.innerHTML = `<div class="flex-1 flex items-center justify-center text-red-400 p-8">
      <div class="text-center"><p class="text-lg mb-2">Failed to load WASM module</p><p class="text-sm text-gray-500">${esc(String(e))}</p></div>
    </div>`;
    return;
  }
  showDropzone();
})();

initKeyboard(switchTab);

// ── Helpers ──────────────────────────────────────────────────────────

function tabOpts() {
  return { showDiff: !!diffData, showRedoLog: isRedoLog };
}

/** Returns the effective data for analysis (decrypted if available). */
function effectiveData() {
  return decryptedData || fileData;
}

// ── Views ────────────────────────────────────────────────────────────

function showLoading(msg) {
  app.innerHTML = `
    <div class="flex-1 flex items-center justify-center">
      <div class="text-center">
        <div class="inline-block w-8 h-8 border-2 border-innodb-cyan border-t-transparent rounded-full animate-spin mb-4"></div>
        <p class="text-gray-400">${esc(msg)}</p>
      </div>
    </div>`;
}

function showDropzone() {
  app.innerHTML = '';
  // Header
  const header = document.createElement('header');
  header.className = 'flex items-center justify-between px-6 py-4 border-b border-gray-800';
  header.innerHTML = `
    <div class="flex items-center gap-3">
      <h1 class="text-xl font-bold text-innodb-cyan">InnoDB Analyzer</h1>
      <span class="text-xs text-gray-600">v2.0 — powered by idb-utils WASM</span>
    </div>
    <div class="text-xs text-gray-600">Press <kbd class="px-1 py-0.5 bg-surface-3 rounded">D</kbd> to toggle theme</div>
  `;
  app.appendChild(header);
  app.appendChild(createDropzone(onFile, onDiffFiles));
}

function onFile(name, data) {
  fileName = name;
  fileData = data;
  diffData = null;
  isRedoLog = false;
  decryptedData = null;
  encryptionInfo = null;

  const wasm = getWasm();

  // Try tablespace first, then redo log
  try {
    const info = JSON.parse(wasm.get_tablespace_info(data));
    pageCount = info.page_count;

    // Check for encryption
    if (info.is_encrypted) {
      try {
        encryptionInfo = JSON.parse(wasm.get_encryption_info(data));
      } catch {
        // Encryption info parsing optional
      }
    }
  } catch {
    // Not a tablespace — try redo log
    try {
      JSON.parse(wasm.parse_redo_log(data));
      isRedoLog = true;
      pageCount = 0;
    } catch {
      pageCount = 0;
    }
  }

  currentTab = 0;
  renderAnalyzer();
}

function onDiffFiles(name1, data1, name2, data2) {
  fileName = name1;
  fileData = data1;
  diffData = { name1, data1, name2, data2 };
  isRedoLog = false;
  decryptedData = null;
  encryptionInfo = null;
  try {
    const info = JSON.parse(getWasm().get_tablespace_info(data1));
    pageCount = info.page_count;
  } catch {
    pageCount = 0;
  }
  currentTab = 0;
  renderAnalyzer();
}

function onDecrypt(kData) {
  try {
    const wasm = getWasm();
    const result = wasm.decrypt_tablespace(fileData, kData);
    decryptedData = result;
    // Update page count from decrypted data
    try {
      const info = JSON.parse(wasm.get_tablespace_info(decryptedData));
      pageCount = info.page_count;
    } catch { /* keep existing */ }
    renderAnalyzer();
  } catch (e) {
    // Show error in keyring area
    const errEl = document.getElementById('keyring-error');
    if (errEl) errEl.textContent = `Decryption failed: ${e}`;
  }
}

function renderAnalyzer() {
  app.innerHTML = '';
  const opts = tabOpts();

  // Header with file name + back button
  const header = document.createElement('header');
  header.className = 'flex items-center justify-between px-6 py-3 border-b border-gray-800';

  const leftDiv = document.createElement('div');
  leftDiv.className = 'flex items-center gap-3';
  leftDiv.innerHTML = `
    <button id="back-btn" class="text-gray-500 hover:text-gray-300 transition-colors" title="Back to file picker" aria-label="Back to file picker">
      <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 19l-7-7 7-7" />
      </svg>
    </button>
    <h1 class="text-lg font-bold text-innodb-cyan">InnoDB Analyzer</h1>
    <span class="text-sm text-gray-400 truncate max-w-xs">${esc(fileName)}</span>
    ${diffData ? `<span class="text-xs text-innodb-amber">+ ${esc(diffData.name2)}</span>` : ''}
    ${encryptionInfo && !decryptedData ? '<span class="text-xs px-2 py-0.5 rounded bg-innodb-amber/20 text-innodb-amber">Encrypted</span>' : ''}
    ${decryptedData ? '<span class="text-xs px-2 py-0.5 rounded bg-innodb-green/20 text-innodb-green">Decrypted</span>' : ''}
  `;

  const rightDiv = document.createElement('div');
  rightDiv.className = 'flex items-center gap-3';
  if (!isRedoLog) {
    rightDiv.innerHTML = `<div class="text-xs text-gray-600">${pageCount} pages</div>`;
  }

  // Export All button (not for redo logs)
  if (!isRedoLog && fileData) {
    const exportBtn = document.createElement('button');
    exportBtn.className = 'px-2 py-1 bg-surface-3 hover:bg-gray-600 text-gray-300 rounded text-xs';
    exportBtn.textContent = 'Export All';
    exportBtn.addEventListener('click', () => exportAll());
    rightDiv.appendChild(exportBtn);
  }

  rightDiv.appendChild(createThemeToggle());
  header.appendChild(leftDiv);
  header.appendChild(rightDiv);
  app.appendChild(header);

  header.querySelector('#back-btn').addEventListener('click', () => {
    fileData = null;
    diffData = null;
    isRedoLog = false;
      decryptedData = null;
    encryptionInfo = null;
    showDropzone();
  });

  // Encryption keyring upload banner
  if (encryptionInfo && !decryptedData && !isRedoLog) {
    const banner = document.createElement('div');
    banner.className = 'px-6 py-3 bg-innodb-amber/10 border-b border-innodb-amber/30 flex items-center gap-4 flex-wrap';
    banner.innerHTML = `
      <div class="text-sm text-innodb-amber">
        Encrypted tablespace — Server UUID: <span class="font-mono text-xs">${esc(encryptionInfo.server_uuid || 'N/A')}</span>,
        Key ID: ${encryptionInfo.master_key_id ?? 'N/A'}
      </div>
      <label class="px-3 py-1 bg-surface-3 hover:bg-gray-600 text-gray-300 rounded text-xs cursor-pointer">
        Upload Keyring
        <input type="file" id="keyring-input" class="hidden" />
      </label>
      <span id="keyring-error" class="text-xs text-innodb-red"></span>
    `;
    app.appendChild(banner);

    banner.querySelector('#keyring-input').addEventListener('change', (e) => {
      const file = e.target.files[0];
      if (!file) return;
      const reader = new FileReader();
      reader.onload = () => onDecrypt(new Uint8Array(reader.result));
      reader.readAsArrayBuffer(file);
    });
  }

  // Tabs
  const tabNav = createTabs(switchTab, opts);
  app.appendChild(tabNav);
  setActiveTab(tabNav, currentTab);

  // Content area
  const content = document.createElement('div');
  content.id = 'tab-content';
  content.className = 'flex-1 overflow-hidden';
  content.setAttribute('role', 'tabpanel');
  content.setAttribute('aria-labelledby', getTabId(currentTab, opts) || '');
  app.appendChild(content);

  renderTab();
}

function switchTab(index) {
  const maxTab = getTabCount(tabOpts()) - 1;
  if (index < 0 || index > maxTab) return;
  if (!fileData) return;
  currentTab = index;
  const tabNav = app.querySelector('nav');
  if (tabNav) setActiveTab(tabNav, index);
  renderTab();
}

function renderTab() {
  const content = document.getElementById('tab-content');
  if (!content || !fileData) return;
  content.innerHTML = '';

  const data = effectiveData();
  const id = getTabId(currentTab, tabOpts());
  switch (id) {
    case 'overview':
      createOverview(content, data);
      break;
    case 'pages':
      createPages(content, data);
      break;
    case 'checksums':
      createChecksums(content, data);
      break;
    case 'sdi':
      createSdi(content, data);
      break;
    case 'hex':
      createHex(content, data, pageCount);
      break;
    case 'recovery':
      createRecovery(content, data);
      break;
    case 'heatmap':
      createHeatmap(content, data);
      break;
    case 'diff':
      if (diffData) {
        createDiff(content, diffData.name1, diffData.data1, diffData.name2, diffData.data2);
      } else {
        content.innerHTML = `<div class="p-6 text-gray-500">Drop two files to compare them.</div>`;
      }
      break;
    case 'redolog':
      createRedoLog(content, fileData);
      break;
  }
}

function exportAll() {
  const wasm = getWasm();
  const data = effectiveData();
  const result = {};
  try { result.overview = JSON.parse(wasm.get_tablespace_info(data)); } catch { /* skip */ }
  try { result.pages = JSON.parse(wasm.analyze_pages(data, -1)); } catch { /* skip */ }
  try { result.checksums = JSON.parse(wasm.validate_checksums(data)); } catch { /* skip */ }
  try { result.sdi = JSON.parse(wasm.extract_sdi(data)); } catch { /* skip */ }
  try { result.recovery = JSON.parse(wasm.assess_recovery(data)); } catch { /* skip */ }

  const baseName = fileName.replace(/\.[^.]+$/, '');
  downloadJson(result, `${baseName}_analysis`);
}
