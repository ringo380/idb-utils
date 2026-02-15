// Dark/light mode toggle with system preference detection
const STORAGE_KEY = 'idb-theme';

export function initTheme() {
  const stored = localStorage.getItem(STORAGE_KEY);
  if (stored === 'light') {
    document.documentElement.classList.add('light');
  } else if (stored === 'dark') {
    document.documentElement.classList.remove('light');
  } else {
    // System preference
    if (window.matchMedia('(prefers-color-scheme: light)').matches) {
      document.documentElement.classList.add('light');
    }
  }
}

export function toggleTheme() {
  const isLight = document.documentElement.classList.toggle('light');
  localStorage.setItem(STORAGE_KEY, isLight ? 'light' : 'dark');
}

export function createThemeToggle() {
  const btn = document.createElement('button');
  btn.className = 'px-2 py-1 bg-surface-3 hover:bg-gray-600 text-gray-300 rounded text-xs';
  btn.setAttribute('aria-label', 'Toggle theme');
  btn.setAttribute('aria-pressed', String(document.documentElement.classList.contains('light')));
  btn.textContent = document.documentElement.classList.contains('light') ? 'Dark' : 'Light';
  btn.addEventListener('click', () => {
    toggleTheme();
    const isLight = document.documentElement.classList.contains('light');
    btn.setAttribute('aria-pressed', String(isLight));
    btn.textContent = isLight ? 'Dark' : 'Light';
  });
  return btn;
}
