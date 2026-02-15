// Shared HTML escaping utility

/**
 * Escape a string for safe insertion into HTML.
 * Uses the DOM to ensure correct entity encoding.
 * @param {string} s
 * @returns {string}
 */
export function esc(s) {
  const d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}
