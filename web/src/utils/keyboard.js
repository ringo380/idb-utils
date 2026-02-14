// Keyboard shortcut handler
import { toggleTheme } from './theme.js';

export function initKeyboard(onTabSwitch) {
  document.addEventListener('keydown', (e) => {
    // Don't capture when typing in inputs
    if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;

    // Number keys 1-7 switch tabs
    if (e.key >= '1' && e.key <= '7' && !e.ctrlKey && !e.metaKey) {
      e.preventDefault();
      onTabSwitch(parseInt(e.key) - 1);
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
