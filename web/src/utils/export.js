// Export utilities — JSON download, text download, clipboard copy

/**
 * Download data as a JSON file.
 * @param {*} data - Data to serialize as JSON
 * @param {string} filename - Download filename (without extension)
 */
export function downloadJson(data, filename) {
  const json = JSON.stringify(data, null, 2);
  const blob = new Blob([json], { type: 'application/json' });
  triggerDownload(blob, `${filename}.json`);
}

/**
 * Download raw text as a file.
 * @param {string} text - Text content
 * @param {string} filename - Download filename (with extension)
 */
export function downloadText(text, filename) {
  const blob = new Blob([text], { type: 'text/plain' });
  triggerDownload(blob, filename);
}

/**
 * Copy text to clipboard with button feedback.
 * @param {string} text - Text to copy
 * @param {HTMLElement} buttonEl - Button element for "Copied!" flash
 */
export function copyToClipboard(text, buttonEl) {
  navigator.clipboard.writeText(text).then(() => {
    const original = buttonEl.textContent;
    buttonEl.textContent = 'Copied!';
    setTimeout(() => { buttonEl.textContent = original; }, 1500);
  });
}

/**
 * Create an export bar with Download and Copy buttons.
 * @param {() => *} getData - Callback returning data (called lazily on click)
 * @param {string} baseFilename - Base filename for downloads (without extension)
 * @param {{ text?: boolean }} [opts] - If text is true, download as .txt instead of .json
 * @returns {HTMLElement}
 */
export function createExportBar(getData, baseFilename, opts = {}) {
  const el = document.createElement('div');
  el.className = 'flex items-center gap-2';

  const dlBtn = document.createElement('button');
  dlBtn.className = 'px-2 py-1 bg-surface-3 hover:bg-gray-600 text-gray-300 rounded text-xs';
  dlBtn.textContent = 'Download';

  const copyBtn = document.createElement('button');
  copyBtn.className = 'px-2 py-1 bg-surface-3 hover:bg-gray-600 text-gray-300 rounded text-xs';
  copyBtn.textContent = 'Copy';

  dlBtn.addEventListener('click', () => {
    const data = getData();
    if (opts.text) {
      downloadText(String(data), `${baseFilename}.txt`);
    } else {
      downloadJson(data, baseFilename);
    }
  });

  copyBtn.addEventListener('click', () => {
    const data = getData();
    const text = opts.text ? String(data) : JSON.stringify(data, null, 2);
    copyToClipboard(text, copyBtn);
  });

  el.appendChild(dlBtn);
  el.appendChild(copyBtn);
  return el;
}

function triggerDownload(blob, filename) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}
