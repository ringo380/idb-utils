// Keyboard shortcut handler
import { toggleTheme } from './theme.js';

let shortcutsVisible = false;

export function initKeyboard(onTabSwitch) {
  document.addEventListener('keydown', (e) => {
    // Don't capture when typing in inputs
    if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA' || e.target.tagName === 'SELECT') return;

    // Number keys 1-9 switch tabs
    if (e.key >= '1' && e.key <= '9' && !e.ctrlKey && !e.metaKey) {
      e.preventDefault();
      onTabSwitch(parseInt(e.key) - 1);
      return;
    }

    // Escape returns to dropzone or closes shortcuts panel
    if (e.key === 'Escape' && !e.ctrlKey && !e.metaKey) {
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

    // D toggles dark mode
    if (e.key === 'd' && !e.ctrlKey && !e.metaKey) {
      e.preventDefault();
      toggleTheme();
      return;
    }

    // ? toggles keyboard shortcuts panel
    if (e.key === '?' && !e.ctrlKey && !e.metaKey) {
      e.preventDefault();
      toggleShortcuts();
      return;
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
          <div class="flex justify-between"><dt class="text-gray-400">Switch tabs</dt><dd class="font-mono text-gray-300">1 – 9</dd></div>
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
