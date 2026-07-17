// Upload endpoint for opt-in .ibd submissions from innodb.fyi.
//
// Contract, in order:
//   browser (Turnstile widget) -> this Worker -> siteverify -> R2
//
// Things that are deliberate and should not be "simplified" later:
//
//   * Turnstile is verified BEFORE the body is read. Buffering 50 MB from an
//     unverified caller is the whole attack.
//   * The declared Content-Length is treated as a hint for an early reject. The
//     authoritative size check happens after the bytes are in hand, because a
//     client can lie about the header.
//   * The submitter's IP is NOT persisted. It is used transiently for rate
//     limiting and passed to siteverify, but the consent copy shown to the user
//     lists exactly three things we receive (file, filename, error context) and
//     an IP is not one of them. Storing it would exceed what they agreed to.
//   * Retention is NOT implemented here. The bucket's own lifecycle rule deletes
//     objects at 90 days. Do not add app-level deletion; do not assume this code
//     is what keeps that promise.

const TURNSTILE_VERIFY_URL = 'https://challenges.cloudflare.com/turnstile/v0/siteverify';

export default {
  async fetch(request, env) {
    const origin = request.headers.get('Origin') || '';
    const allowed = (env.ALLOWED_ORIGINS || '').split(',').map((s) => s.trim()).filter(Boolean);
    const originOk = allowed.includes(origin);

    if (request.method === 'OPTIONS') {
      return preflight(origin, originOk);
    }

    const url = new URL(request.url);
    if (request.method !== 'POST' || url.pathname !== '/submit') {
      return json({ error: 'not found' }, 404, origin, originOk);
    }
    if (!originOk) {
      return json({ error: 'origin not allowed' }, 403, origin, false);
    }

    // Early reject on the declared size, before touching the body at all.
    const maxBytes = parseInt(env.MAX_UPLOAD_BYTES || '52428800', 10);
    const declared = parseInt(request.headers.get('Content-Length') || '0', 10);
    if (declared > maxBytes) {
      return json({ error: 'file too large', max_bytes: maxBytes }, 413, origin, originOk);
    }

    const ip = request.headers.get('CF-Connecting-IP') || '0.0.0.0';

    // Rate limit before any expensive work.
    if (env.UPLOAD_LIMITER) {
      const { success } = await env.UPLOAD_LIMITER.limit({ key: ip });
      if (!success) {
        return json({ error: 'rate limited, try again shortly' }, 429, origin, originOk);
      }
    }

    // Turnstile, before the body is read.
    const token = request.headers.get('X-Turnstile-Token');
    if (!token) {
      return json({ error: 'missing turnstile token' }, 403, origin, originOk);
    }
    const verdict = await verifyTurnstile(token, ip, env.TURNSTILE_SECRET);
    if (!verdict.success) {
      return json(
        { error: 'turnstile verification failed', codes: verdict['error-codes'] || [] },
        403, origin, originOk,
      );
    }

    // Body is now worth reading. byteLength is authoritative; the header was not.
    let bytes;
    try {
      bytes = new Uint8Array(await request.arrayBuffer());
    } catch {
      return json({ error: 'could not read body' }, 400, origin, originOk);
    }
    if (bytes.byteLength === 0) {
      return json({ error: 'empty body' }, 400, origin, originOk);
    }
    if (bytes.byteLength > maxBytes) {
      return json({ error: 'file too large', max_bytes: maxBytes }, 413, origin, originOk);
    }

    const rawName = request.headers.get('X-File-Name') || 'unnamed';
    const safeName = sanitizeName(rawName);
    const context = (request.headers.get('X-Context') || '').slice(0, 500);

    const id = crypto.randomUUID();
    const day = new Date().toISOString().slice(0, 10);
    const key = `submissions/${day}/${id}-${safeName}`;

    try {
      await env.SUBMISSIONS.put(key, bytes, {
        httpMetadata: { contentType: 'application/octet-stream' },
        customMetadata: {
          original_name: rawName.slice(0, 200),
          context,
          submitted_at: new Date().toISOString(),
          size_bytes: String(bytes.byteLength),
        },
      });
    } catch (err) {
      console.error('r2 put failed', { key, message: String(err) });
      return json({ error: 'storage failed' }, 500, origin, originOk);
    }

    console.log('submission stored', { key, size: bytes.byteLength });
    return json({ ok: true, id }, 200, origin, originOk);
  },
};

async function verifyTurnstile(token, ip, secret) {
  if (!secret) {
    // Fail closed. A missing secret must never mean "let everything through".
    console.error('TURNSTILE_SECRET is not set');
    return { success: false, 'error-codes': ['missing-input-secret'] };
  }
  try {
    const res = await fetch(TURNSTILE_VERIFY_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({ secret, response: token, remoteip: ip }),
    });
    return await res.json();
  } catch (err) {
    console.error('siteverify request failed', String(err));
    return { success: false, 'error-codes': ['internal-error'] };
  }
}

/** Reduce a client-supplied filename to something safe to use in an object key. */
function sanitizeName(name) {
  const base = name.split(/[/\\]/).pop() || 'unnamed';
  const cleaned = base.replace(/[^A-Za-z0-9._-]/g, '_').replace(/^\.+/, '');
  return (cleaned || 'unnamed').slice(0, 80);
}

function corsHeaders(origin, originOk) {
  if (!originOk) return {};
  return {
    'Access-Control-Allow-Origin': origin,
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, X-Turnstile-Token, X-File-Name, X-Context',
    'Access-Control-Max-Age': '86400',
    Vary: 'Origin',
  };
}

function preflight(origin, originOk) {
  return new Response(null, { status: originOk ? 204 : 403, headers: corsHeaders(origin, originOk) });
}

function json(body, status, origin, originOk) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json', ...corsHeaders(origin, originOk) },
  });
}
