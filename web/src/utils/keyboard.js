// Keyboard shortcut handler
import { toggleTheme } from './theme.js';

export function initKeyboard(onTabSwitch) {
  document.addEventListener('keydown', (e) => {
    // Don't capture when typing in inputs
    if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;

    // Number keys 1-9 switch tabs
    if (e.key >= '1' && e.key <= '9' && !e.ctrlKey && !e.metaKey) {
      e.preventDefault();
      onTabSwitch(parseInt(e.key) - 1);
      return;
    }

    // Escape returns to dropzone
    if (e.key === 'Escape' && !e.ctrlKey && !e.metaKey) {
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
  });
}
