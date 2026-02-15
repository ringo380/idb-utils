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
