// Centralized Google Analytics 4 event tracking
// Thin wrapper around gtag() for consistent, privacy-respecting event tracking.

/** Send a custom GA4 event. */
export function trackEvent(name, params = {}) {
  if (typeof window.gtag === 'function') {
    window.gtag('event', name, params);
  }
}

/** Anonymize file info — only send extension and size, never the full name. */
function anonymizeFile(name, size) {
  const ext = name.includes('.') ? name.slice(name.lastIndexOf('.')) : 'unknown';
  return { ext, size_kb: Math.round(size / 1024) };
}

/** Track a file upload event. */
export function trackFileUpload(fileName, fileSize, fileType) {
  const { ext, size_kb } = anonymizeFile(fileName, fileSize);
  trackEvent('file_upload', { file_ext: ext, size_kb, file_type: fileType });
}

/** Track a tab view. */
export function trackTabView(tabId) {
  trackEvent('tab_view', { tab_id: tabId });
}

/** Track an export/download action. */
export function trackExport(format, tabId) {
  trackEvent('export', { format, tab_id: tabId });
}

/** Track a feature interaction. */
export function trackFeatureUse(feature, details = {}) {
  trackEvent('feature_use', { feature, ...details });
}

/** Track an error. */
export function trackError(context, message) {
  trackEvent('error', { context, message: String(message).slice(0, 100) });
}

/** Track a performance metric. */
export function trackPerformance(metric, durationMs) {
  trackEvent('performance', { metric, duration_ms: Math.round(durationMs) });
}
