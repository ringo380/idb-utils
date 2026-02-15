// Hex dump viewer — mirrors `inno dump`
import { getWasm } from '../wasm.js';
import { esc } from '../utils/html.js';
import { createExportBar } from '../utils/export.js';

export function createHex(container, fileData, pageCount) {
  container.innerHTML = `
    <div class="p-6 space-y-4 overflow-auto max-h-full">
      <div class="flex items-center gap-4 flex-wrap">
        <h2 class="text-lg font-bold text-innodb-cyan">Hex Dump</h2>
        <div class="flex items-center gap-2">
          <label class="text-sm text-gray-500">Page:</label>
          <input id="hex-page" type="number" min="0" max="${pageCount - 1}" value="0"
            class="w-24 px-2 py-1 bg-surface-2 border border-gray-700 rounded text-sm text-gray-200 focus:border-innodb-cyan focus:outline-none" />
        </div>
        <div class="flex items-center gap-2">
          <label class="text-sm text-gray-500">Offset:</label>
          <input id="hex-offset" type="number" min="0" value="0"
            class="w-24 px-2 py-1 bg-surface-2 border border-gray-700 rounded text-sm text-gray-200 focus:border-innodb-cyan focus:outline-none" />
        </div>
        <div class="flex items-center gap-2">
          <label class="text-sm text-gray-500">Length:</label>
          <input id="hex-length" type="number" min="0" value="256"
            class="w-24 px-2 py-1 bg-surface-2 border border-gray-700 rounded text-sm text-gray-200 focus:border-innodb-cyan focus:outline-none" />
          <span class="text-xs text-gray-600">0 = full page</span>
        </div>
        <button id="hex-go" class="px-3 py-1 bg-innodb-blue hover:bg-blue-600 text-white rounded text-sm">Dump</button>
        <span id="hex-export"></span>
      </div>
      <div id="hex-output" class="bg-surface-1 rounded-lg p-4 font-mono text-xs overflow-auto max-h-[calc(100vh-16rem)]">
        <span class="text-gray-600">Select a page and click Dump to view hex data.</span>
      </div>
    </div>
  `;

  const pageInput = container.querySelector('#hex-page');
  const offsetInput = container.querySelector('#hex-offset');
  const lengthInput = container.querySelector('#hex-length');
  const goBtn = container.querySelector('#hex-go');
  const output = container.querySelector('#hex-output');

  let lastDumpRaw = '';

  function doDump() {
    const wasm = getWasm();
    const page = parseInt(pageInput.value) || 0;
    const offset = parseInt(offsetInput.value) || 0;
    const length = parseInt(lengthInput.value) || 0;

    try {
      const dump = wasm.hex_dump_page(fileData, page, offset, length);
      lastDumpRaw = dump;
      output.innerHTML = formatHexDump(dump);
    } catch (e) {
      lastDumpRaw = '';
      output.innerHTML = `<span class="text-red-400">${esc(String(e))}</span>`;
    }
  }

  goBtn.addEventListener('click', doDump);
  [pageInput, offsetInput, lengthInput].forEach((input) => {
    input.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') doDump();
    });
  });

  const exportSlot = container.querySelector('#hex-export');
  if (exportSlot) {
    exportSlot.appendChild(createExportBar(() => lastDumpRaw, 'hex_dump', { text: true }));
  }

  // Auto-dump page 0
  doDump();
}

function formatHexDump(raw) {
  // The hex_dump output has lines like:
  // 00000000  89 50 4E 47 0D 0A 1A 0A  00 00 00 0D 49 48 44 52  |.PNG........IHDR|
  return raw
    .split('\n')
    .map((line) => {
      if (!line.trim()) return '';
      // Match offset | hex bytes | ascii
      const match = line.match(/^([0-9A-Fa-f]+)\s+((?:[0-9A-Fa-f]{2}\s*)+)\s*\|(.+)\|$/);
      if (match) {
        return `<span class="hex-offset">${esc(match[1])}</span>  <span class="hex-byte">${esc(match[2].trim())}</span>  <span class="hex-ascii">|${esc(match[3])}|</span>`;
      }
      // Fallback: just escape and return
      return esc(line);
    })
    .join('\n');
}

