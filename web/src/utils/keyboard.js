// Keyboard shortcut handler
import { toggleTheme } from './theme.js';

let shortcutsVisible = false;

/**
 * Initialise global keyboard shortcuts.
 *
 * @param {function(number): void} onTabSwitch - switch to tab by numeric index
 * @param {function(): Set<string>} getVisibleTabKeysFn - returns Set of active shortcut keys
 * @param {function(string): number} getTabIndexByKeyFn - resolves a shortcut key to tab index (-1 if none)
 */
export function initKeyboard(onTabSwitch, getVisibleTabKeysFn, getTabIndexByKeyFn) {
  document.addEventListener('keydown', (e) => {
    // Don't capture when typing in inputs
    if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA' || e.target.tagName === 'SELECT') return;

    if (e.ctrlKey || e.metaKey) return;

    // Escape returns to dropzone or closes shortcuts panel
    if (e.key === 'Escape') {
      const panel = document.getElementById('shortcuts-panel');
      if (panel && !panel.classList.contains('hidden')) {
        e.preventDefault();
        panel.classList.add('hidden');
        shortcutsVisible = false;
        return;
      }
      const backBtn = document.getElementById('back-btn');
      if (backBtn) {
        e.preventDefault();
        backBtn.click();
      }
      return;
    }

    // D toggles dark mode (case-insensitive, but D is not a tab key)
    if (e.key === 'd' || e.key === 'D') {
      e.preventDefault();
      toggleTheme();
      return;
    }

    // ? toggles keyboard shortcuts panel
    if (e.key === '?') {
      e.preventDefault();
      toggleShortcuts();
      return;
    }

    // Tab switching — number keys (0-9) and letter keys (H, V, C, etc.)
    // Check against the set of visible tab shortcut keys.
    const key = e.key;
    if (getVisibleTabKeysFn && getTabIndexByKeyFn) {
      const visibleKeys = getVisibleTabKeysFn();
      const normalised = key.toUpperCase();
      // Match digits exactly, letters case-insensitively
      if (visibleKeys.has(key) || visibleKeys.has(normalised)) {
        const idx = getTabIndexByKeyFn(key);
        if (idx >= 0) {
          e.preventDefault();
          onTabSwitch(idx);
          return;
        }
      }
    }
  });
}

function toggleShortcuts() {
  let panel = document.getElementById('shortcuts-panel');
  if (!panel) {
    panel = document.createElement('div');
    panel.id = 'shortcuts-panel';
    panel.className = 'fixed inset-0 z-50 flex items-center justify-center bg-black/60';
    panel.innerHTML = `
      <div class="bg-surface-2 border border-gray-700 rounded-lg p-6 max-w-sm w-full mx-4 shadow-xl" role="dialog" aria-label="Keyboard shortcuts">
        <div class="flex items-center justify-between mb-4">
          <h2 class="text-lg font-bold text-innodb-cyan">Keyboard Shortcuts</h2>
          <button id="shortcuts-close" class="text-gray-500 hover:text-gray-300 text-xl" aria-label="Close">&times;</button>
        </div>
        <dl class="space-y-2 text-sm">
          <div class="flex justify-between"><dt class="text-gray-400">Switch tabs</dt><dd class="font-mono text-gray-300">1 – 9, 0</dd></div>
          <div class="flex justify-between"><dt class="text-gray-400">Health tab</dt><dd class="font-mono text-gray-300">H</dd></div>
          <div class="flex justify-between"><dt class="text-gray-400">Verify tab</dt><dd class="font-mono text-gray-300">V</dd></div>
          <div class="flex justify-between"><dt class="text-gray-400">Compat tab</dt><dd class="font-mono text-gray-300">C</dd></div>
          <div class="flex justify-between"><dt class="text-gray-400">Toggle theme</dt><dd class="font-mono text-gray-300">D</dd></div>
          <div class="flex justify-between"><dt class="text-gray-400">Back to file picker</dt><dd class="font-mono text-gray-300">Esc</dd></div>
          <div class="flex justify-between"><dt class="text-gray-400">Show shortcuts</dt><dd class="font-mono text-gray-300">?</dd></div>
          <div class="flex justify-between"><dt class="text-gray-400">Navigate tabs</dt><dd class="font-mono text-gray-300">&larr; &rarr;</dd></div>
        </dl>
      </div>
    `;
    document.body.appendChild(panel);
    panel.querySelector('#shortcuts-close').addEventListener('click', () => {
      panel.classList.add('hidden');
      shortcutsVisible = false;
    });
    panel.addEventListener('click', (e) => {
      if (e.target === panel) {
        panel.classList.add('hidden');
        shortcutsVisible = false;
      }
    });
    shortcutsVisible = true;
  } else {
    shortcutsVisible = !shortcutsVisible;
    panel.classList.toggle('hidden', !shortcutsVisible);
  }
}
