// Optional, opt-in panel for sharing the current file with the maintainers.
//
// Renders nothing unless SHARING_ENABLED is true. See utils/consent.js for the
// rules this panel is built to honour.
//
// Two behaviours here are deliberate:
//
//   * Turnstile is loaded only after the box is ticked. Someone who never opts
//     in makes zero third-party requests. Loading a Cloudflare script for every
//     visitor of a tool that advertises local-only processing would undercut the
//     claim, even though the script itself uploads nothing.
//   * A Turnstile token is single-use. Any failed send must reset the widget,
//     or the retry reuses a spent token and fails as `timeout-or-duplicate`.
import { esc } from '../utils/html.js';
import { trackFeatureUse } from '../utils/analytics.js';
import { renderTurnstile } from '../utils/turnstile.js';
import {
  SHARING_ENABLED,
  SUBMIT_ENDPOINT,
  TURNSTILE_SITEKEY,
  MAX_SUBMIT_BYTES,
  RETENTION_DAYS,
  PURPOSES,
  PAYLOAD,
  ATTESTATION,
  grantConsent,
} from '../utils/consent.js';

const BTN_ON =
  'px-4 py-1.5 bg-innodb-cyan/20 hover:bg-innodb-cyan/30 text-innodb-cyan rounded text-xs font-semibold transition-colors';
const BTN_OFF =
  'px-4 py-1.5 bg-surface-3 text-gray-600 rounded text-xs font-semibold cursor-not-allowed transition-colors';

/**
 * Build the share panel for a given file.
 * @param {string} fileName
 * @param {Uint8Array} fileData
 * @param {string} [context] what the user was looking at, for the maintainers
 * @returns {HTMLElement|null} null when sharing is disabled.
 */
export function createSharePanel(fileName, fileData, context = '') {
  if (!SHARING_ENABLED) return null;
  if (!fileName || !fileData) return null;

  const el = document.createElement('section');
  el.className = 'mx-6 mb-6 mt-2 border border-gray-800 rounded-lg bg-surface-2 p-5';
  el.setAttribute('aria-labelledby', 'share-heading');

  // Analysing and sharing have different limits on purpose. Say so rather than
  // letting someone tick a box and fail at the end of a 400 MB upload.
  if (fileData.length > MAX_SUBMIT_BYTES) {
    el.innerHTML = `
      <h2 id="share-heading" class="text-sm font-semibold text-gray-300 mb-2">Help improve InnoDB Analyzer</h2>
      <p class="text-xs text-gray-500 max-w-2xl leading-relaxed">
        This file is ${(fileData.length / 1024 / 1024).toFixed(1)} MB, which is over the
        ${Math.round(MAX_SUBMIT_BYTES / 1024 / 1024)} MB limit for sharing, so it cannot be sent.
        It still analyzes normally here in your browser. If you hit a bug with it, a smaller
        file that reproduces the same problem is more useful to us anyway.
      </p>
    `;
    return el;
  }

  const sizeMb = (fileData.length / 1024 / 1024).toFixed(1);

  el.innerHTML = `
    <h2 id="share-heading" class="text-sm font-semibold text-gray-300 mb-2">
      Help improve InnoDB Analyzer
    </h2>

    <p class="text-xs text-gray-400 mb-4 max-w-2xl leading-relaxed">
      This is entirely optional and the analyzer works exactly the same either way.
      Nothing has been uploaded. <span class="font-mono text-gray-300">${esc(fileName)}</span>
      is still only on your machine. If you want to send it to us so we can fix
      what you ran into, you can do that below.
    </p>

    <div class="bg-innodb-amber/10 border border-innodb-amber/30 rounded p-3 mb-4 max-w-2xl">
      <p class="text-xs text-innodb-amber leading-relaxed">
        <span class="font-semibold">Read this first.</span>
        A tablespace file contains your table's real contents: actual rows, column
        values, and schema. If this came from a production database, it may hold
        customer or personal data. Sending it means sending all of that. If the
        data is not yours to share, please do not share it.
      </p>
    </div>

    <div class="grid gap-4 sm:grid-cols-2 mb-4 max-w-2xl">
      <div>
        <p class="text-xs font-semibold text-gray-400 mb-1.5">What gets sent</p>
        <ul class="text-xs text-gray-500 space-y-1">
          ${PAYLOAD.map((p) => `<li>${esc(p)}</li>`).join('')}
          <li class="text-gray-600">(about ${sizeMb} MB in total)</li>
        </ul>
      </div>
      <div>
        <p class="text-xs font-semibold text-gray-400 mb-1.5">What it is used for</p>
        <ul class="text-xs text-gray-500 space-y-1">
          ${PURPOSES.map((p) => `<li>${esc(p)}</li>`).join('')}
        </ul>
      </div>
    </div>

    <p class="text-xs text-gray-500 mb-4 max-w-2xl">
      Kept for ${RETENTION_DAYS} days, then deleted automatically. Not used for
      anything outside the list above. Not sold, and not shared with anyone else.
      Applies to this one file only: if you analyze another file, we will ask again.
    </p>

    <label class="flex items-start gap-2 text-xs text-gray-400 cursor-pointer max-w-lg mb-3">
      <input type="checkbox" id="share-consent" class="mt-0.5 rounded border-gray-600" />
      <span>${esc(ATTESTATION)}</span>
    </label>

    <div id="share-turnstile" class="mb-3"></div>

    <div class="flex items-center gap-4 flex-wrap">
      <button id="share-send" class="${BTN_OFF}" disabled>Send this file</button>
      <span id="share-status" class="text-xs text-gray-500" role="status" aria-live="polite"></span>
    </div>
  `;

  const checkbox = el.querySelector('#share-consent');
  const sendBtn = el.querySelector('#share-send');
  const status = el.querySelector('#share-status');
  const tsMount = el.querySelector('#share-turnstile');

  let token = null;
  let widget = null;

  const refreshButton = () => {
    const ready = checkbox.checked && !!token;
    sendBtn.disabled = !ready;
    sendBtn.className = ready ? BTN_ON : BTN_OFF;
  };

  checkbox.addEventListener('change', async () => {
    trackFeatureUse('share_consent_toggle', { checked: checkbox.checked });

    if (!checkbox.checked) {
      token = null;
      status.textContent = '';
      if (widget) widget.reset();
      refreshButton();
      return;
    }

    // Only now does anything reach out to Cloudflare.
    if (!widget) {
      status.textContent = 'Loading verification...';
      try {
        widget = await renderTurnstile(tsMount, TURNSTILE_SITEKEY, (t) => {
          token = t;
          status.textContent = t ? '' : 'Verification expired, solve it again.';
          refreshButton();
        });
        status.textContent = '';
      } catch (err) {
        status.textContent = `Verification unavailable: ${err.message || err}`;
      }
    }
    refreshButton();
  });

  sendBtn.addEventListener('click', async () => {
    // Re-check both conditions rather than trusting the disabled attribute.
    if (!checkbox.checked || !token) return;

    grantConsent(fileName);
    trackFeatureUse('share_submit', { size_kb: Math.round(fileData.length / 1024) });

    sendBtn.disabled = true;
    sendBtn.className = BTN_OFF;
    status.textContent = 'Sending...';

    try {
      await submitFile(fileName, fileData, context, token);
      status.textContent = `Sent. Thank you. Deleted after ${RETENTION_DAYS} days.`;
      checkbox.disabled = true;
      tsMount.innerHTML = '';
    } catch (err) {
      status.textContent = `Could not send: ${err.message || err}`;
      // The token is spent either way. Force a fresh challenge before any retry.
      token = null;
      if (widget) widget.reset();
      refreshButton();
    }
  });

  return el;
}

/**
 * Transmit a consented file to the Worker, which verifies the Turnstile token
 * server-side before storing anything.
 */
async function submitFile(fileName, fileData, context, token) {
  let res;
  try {
    res = await fetch(SUBMIT_ENDPOINT, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/octet-stream',
        'X-Turnstile-Token': token,
        'X-File-Name': fileName,
        'X-Context': context || '',
      },
      body: fileData,
    });
  } catch {
    throw new Error('network error');
  }

  if (!res.ok) {
    let detail = `HTTP ${res.status}`;
    try {
      const body = await res.json();
      if (body && body.error) detail = body.error;
    } catch {
      // Non-JSON error body; the status is all we have.
    }
    throw new Error(detail);
  }
  return res.json();
}
