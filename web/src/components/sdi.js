// SDI metadata viewer — mirrors `inno sdi`
import { getWasm } from '../wasm.js';

export function createSdi(container, fileData) {
  const wasm = getWasm();
  let records;
  try {
    records = JSON.parse(wasm.extract_sdi(fileData));
  } catch (e) {
    container.innerHTML = `<div class="p-6 text-red-400">Error extracting SDI: ${esc(e)}</div>`;
    return;
  }

  if (!records || records.length === 0) {
    container.innerHTML = `
      <div class="p-6">
        <h2 class="text-lg font-bold text-innodb-cyan mb-4">SDI Metadata</h2>
        <div class="bg-surface-2 rounded-lg p-6 text-center text-gray-500">
          <p class="mb-2">No SDI records found.</p>
          <p class="text-xs">SDI metadata is only present in MySQL 8.0+ tablespaces.</p>
        </div>
      </div>`;
    return;
  }

  container.innerHTML = `
    <div class="p-6 space-y-4 overflow-auto max-h-full">
      <div class="flex items-center gap-3">
        <h2 class="text-lg font-bold text-innodb-cyan">SDI Metadata</h2>
        <span class="text-xs text-gray-500">${records.length} record${records.length === 1 ? '' : 's'}</span>
      </div>
      <div class="flex gap-2 mb-2">
        <button id="sdi-expand-all" class="px-2 py-1 bg-surface-3 hover:bg-gray-600 text-gray-300 rounded text-xs">Expand All</button>
        <button id="sdi-collapse-all" class="px-2 py-1 bg-surface-3 hover:bg-gray-600 text-gray-300 rounded text-xs">Collapse All</button>
        <button id="sdi-copy-all" class="px-2 py-1 bg-surface-3 hover:bg-gray-600 text-gray-300 rounded text-xs">Copy JSON</button>
      </div>
      <div id="sdi-records" class="space-y-3">
        ${records.map((r, i) => renderRecord(r, i)).join('')}
      </div>
    </div>
  `;

  // Toggle individual records
  container.querySelectorAll('.sdi-toggle').forEach((btn) => {
    btn.addEventListener('click', () => {
      const target = container.querySelector(`#sdi-body-${btn.dataset.idx}`);
      target.classList.toggle('hidden');
      btn.textContent = target.classList.contains('hidden') ? '+' : '-';
    });
  });

  container.querySelector('#sdi-expand-all').addEventListener('click', () => {
    container.querySelectorAll('[id^="sdi-body-"]').forEach((el) => el.classList.remove('hidden'));
    container.querySelectorAll('.sdi-toggle').forEach((b) => (b.textContent = '-'));
  });

  container.querySelector('#sdi-collapse-all').addEventListener('click', () => {
    container.querySelectorAll('[id^="sdi-body-"]').forEach((el) => el.classList.add('hidden'));
    container.querySelectorAll('.sdi-toggle').forEach((b) => (b.textContent = '+'));
  });

  container.querySelector('#sdi-copy-all').addEventListener('click', () => {
    navigator.clipboard.writeText(JSON.stringify(records, null, 2)).then(() => {
      const btn = container.querySelector('#sdi-copy-all');
      btn.textContent = 'Copied!';
      setTimeout(() => (btn.textContent = 'Copy JSON'), 1500);
    });
  });
}

function renderRecord(record, idx) {
  const name = record?.dd_object?.name || record?.object_type || `Record ${idx}`;
  const type = record?.dd_object?.type || record?.type || '';
  const json = JSON.stringify(record, null, 2);

  return `
    <div class="bg-surface-2 rounded-lg overflow-hidden">
      <div class="flex items-center gap-2 px-4 py-2 cursor-pointer hover:bg-surface-3/50">
        <button class="sdi-toggle text-gray-500 hover:text-gray-300 w-5 text-center font-mono" data-idx="${idx}">+</button>
        <span class="text-innodb-cyan font-bold text-sm">${esc(String(name))}</span>
        ${type ? `<span class="text-xs text-gray-500">${esc(String(type))}</span>` : ''}
      </div>
      <div id="sdi-body-${idx}" class="hidden">
        <pre class="px-4 pb-4 text-xs text-gray-300 overflow-x-auto max-h-96">${esc(json)}</pre>
      </div>
    </div>`;
}

function esc(s) {
  const d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}
