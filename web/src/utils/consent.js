// Consent copy and state for optional file sharing.
//
// Nothing in this module uploads anything. It defines the terms the user is
// asked to agree to, and tracks whether they have agreed for the current file.
//
// Design rules for this feature, in case they are not obvious later:
//   1. Consent is per-file, never ambient. Analysing a second file requires
//      asking again. There is no "remember this choice".
//   2. The checkbox alone never uploads. An explicit Send click is also
//      required, so a stray click cannot transmit a database file.
//   3. The default is always unchecked. Do not add a "checked" default, and do
//      not pre-select on any code path.
//   4. PURPOSES below is the complete list of what a submitted file may be used
//      for. If someone wants to use submissions for something not on this list,
//      the list changes and users are asked again -- the consent does not
//      silently stretch to cover it.

/** Master switch. Stays false until a receiving endpoint exists and has been
 *  reviewed. While false, the share panel does not render at all. Shipping a
 *  consent UI with no backend behind it would train users to agree to nothing.
 *
 *  Flipping this to true is the moment the "no uploads" claims in index.html,
 *  main.js (welcome modal) and docs/src/web/overview.md stop being true. Change
 *  them in the SAME commit that flips this. */
export const SHARING_ENABLED = true;

/** Upload endpoint. Verifies Turnstile server-side, then writes to a private
 *  R2 bucket. Never called unless the user has ticked the box and hit Send. */
export const SUBMIT_ENDPOINT = 'https://innodb-fyi-submissions.ringo380.workers.dev/submit';

/** Turnstile sitekey. Public by design -- it is meant to ship in the bundle.
 *  The paired secret lives only as a Worker secret (and in 1Password), and is
 *  what makes the token mean anything. A token is worthless until the Worker
 *  has run it past siteverify; never gate anything on it client-side. */
export const TURNSTILE_SITEKEY = '0x4AAAAAAD3dF2zsh6358BQB';

/** Largest file we accept. MUST stay in sync with MAX_UPLOAD_BYTES in
 *  worker/wrangler.toml, which is the real enforcement point -- this copy only
 *  exists so the UI can explain the limit instead of failing at the end of an
 *  upload. Note this is well below the 500 MB the analyzer will happily parse
 *  locally: analysing and sharing are deliberately not the same limit. */
export const MAX_SUBMIT_BYTES = 52428800; // 50 MB

/** How long a submitted file is kept before automatic deletion. Enforced by the
 *  R2 bucket's own "delete-after-90-days" lifecycle rule, not by app code. */
export const RETENTION_DAYS = 90;

/** The complete list of what a submitted file may be used for. */
export const PURPOSES = [
  'Reproducing a parsing or analysis bug you hit',
  'Adding the file to our regression test fixtures, so the bug stays fixed',
  'Verifying checksum and page-format handling against real MySQL versions',
];

/** What actually gets transmitted, stated plainly. */
export const PAYLOAD = [
  'The complete file, byte for byte',
  'Its filename',
  'The error or analysis result you were looking at',
];

/** The attestation text. Covers both consent and authority to give it: the
 *  person clicking is often not the person whose data is in the file. */
export const ATTESTATION =
  'I am authorised to share this file, and I agree to it being used as described above.';

let consentedFor = null;

/** Record consent for a specific file. Scoped to that file only. */
export function grantConsent(fileName) {
  consentedFor = fileName;
}

/** True only if consent was granted for this exact file. */
export function hasConsent(fileName) {
  return consentedFor !== null && consentedFor === fileName;
}

/** Drop any consent. Call on every new file, and on any state reset. */
export function clearConsent() {
  consentedFor = null;
}
