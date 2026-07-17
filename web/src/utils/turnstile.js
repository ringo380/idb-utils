// Turnstile widget loader.
//
// The token produced here proves nothing on its own. It is only meaningful once
// the Worker has passed it to siteverify. Never treat a token's presence in the
// browser as authorisation for anything -- the check happens server-side.
//
// The script is loaded lazily, on first use, so that visitors who never open the
// share panel never fetch it.

const SCRIPT_URL = 'https://challenges.cloudflare.com/turnstile/v0/api.js?render=explicit';

let loadPromise = null;

/** Load the Turnstile script once. Resolves when window.turnstile is ready. */
function loadTurnstile() {
  if (loadPromise) return loadPromise;
  loadPromise = new Promise((resolve, reject) => {
    if (window.turnstile) return resolve(window.turnstile);
    const s = document.createElement('script');
    s.src = SCRIPT_URL;
    s.async = true;
    s.defer = true;
    s.onload = () => (window.turnstile ? resolve(window.turnstile) : reject(new Error('turnstile failed to initialise')));
    s.onerror = () => reject(new Error('could not load turnstile'));
    document.head.appendChild(s);
  });
  return loadPromise;
}

/**
 * Render a Turnstile widget into a container.
 * @param {HTMLElement} container
 * @param {string} sitekey
 * @param {(token: string|null) => void} onToken called with a token, or null when it expires
 * @returns {Promise<{reset: () => void}>}
 */
export async function renderTurnstile(container, sitekey, onToken) {
  const turnstile = await loadTurnstile();
  const id = turnstile.render(container, {
    sitekey,
    action: 'turnstile-spin-v2',
    theme: 'auto',
    callback: (token) => onToken(token),
    'expired-callback': () => onToken(null),
    'error-callback': () => onToken(null),
  });
  return {
    reset: () => {
      try {
        turnstile.reset(id);
      } catch {
        // Widget already gone (panel re-rendered). Nothing to reset.
      }
    },
  };
}
