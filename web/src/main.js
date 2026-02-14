// App initialization — WASM loading, routing, component orchestration
import { initWasm, getWasm } from './wasm.js';
import { initTheme } from './utils/theme.js';
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

import './style.css';

const app = document.getElementById('app');
let currentTab = 0;
let fileData = null;
let fileName = null;
let diffData = null; // { name1, data1, name2, data2 }
let pageCount = 0;

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
  try {
    const info = JSON.parse(getWasm().get_tablespace_info(data));
    pageCount = info.page_count;
  } catch {
    pageCount = 0;
  }
  currentTab = 0;
  renderAnalyzer();
}

function onDiffFiles(name1, data1, name2, data2) {
  fileName = name1;
  fileData = data1;
  diffData = { name1, data1, name2, data2 };
  try {
    const info = JSON.parse(getWasm().get_tablespace_info(data1));
    pageCount = info.page_count;
  } catch {
    pageCount = 0;
  }
  currentTab = 0;
  renderAnalyzer();
}

function renderAnalyzer() {
  app.innerHTML = '';

  // Header with file name + back button
  const header = document.createElement('header');
  header.className = 'flex items-center justify-between px-6 py-3 border-b border-gray-800';
  header.innerHTML = `
    <div class="flex items-center gap-3">
      <button id="back-btn" class="text-gray-500 hover:text-gray-300 transition-colors" title="Back to file picker">
        <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 19l-7-7 7-7" />
        </svg>
      </button>
      <h1 class="text-lg font-bold text-innodb-cyan">InnoDB Analyzer</h1>
      <span class="text-sm text-gray-400 truncate max-w-xs">${esc(fileName)}</span>
      ${diffData ? `<span class="text-xs text-innodb-amber">+ ${esc(diffData.name2)}</span>` : ''}
    </div>
    <div class="text-xs text-gray-600">${pageCount} pages</div>
  `;
  app.appendChild(header);

  header.querySelector('#back-btn').addEventListener('click', () => {
    fileData = null;
    diffData = null;
    showDropzone();
  });

  // Tabs
  const showDiff = !!diffData;
  const tabNav = createTabs(switchTab, { showDiff });
  app.appendChild(tabNav);
  setActiveTab(tabNav, currentTab);

  // Content area
  const content = document.createElement('div');
  content.id = 'tab-content';
  content.className = 'flex-1 overflow-hidden';
  app.appendChild(content);

  renderTab();
}

function switchTab(index) {
  const maxTab = getTabCount(!!diffData) - 1;
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

  const id = getTabId(currentTab);
  switch (id) {
    case 'overview':
      createOverview(content, fileData);
      break;
    case 'pages':
      createPages(content, fileData);
      break;
    case 'checksums':
      createChecksums(content, fileData);
      break;
    case 'sdi':
      createSdi(content, fileData);
      break;
    case 'hex':
      createHex(content, fileData, pageCount);
      break;
    case 'recovery':
      createRecovery(content, fileData);
      break;
    case 'diff':
      if (diffData) {
        createDiff(content, diffData.name1, diffData.data1, diffData.name2, diffData.data2);
      } else {
        content.innerHTML = `<div class="p-6 text-gray-500">Drop two files to compare them.</div>`;
      }
      break;
  }
}

function esc(s) {
  const d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}
