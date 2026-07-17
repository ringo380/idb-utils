# Submissions worker

Receives opt-in `.ibd` submissions from the analyzer at innodb.fyi and writes them to a
private R2 bucket. Users reach this only by ticking an unchecked consent box and clicking
Send; see `web/src/utils/consent.js` for the rules the frontend honours.

- Endpoint: `POST https://innodb-fyi-submissions.ringo380.workers.dev/submit`
- Bucket: `innodb-fyi-submissions` (private, no public domain)
- Key format: `submissions/<YYYY-MM-DD>/<uuid>-<sanitised-name>`

## The 90-day retention promise is NOT in this code

`docs/src/web/overview.md` and the consent panel both tell users a submitted file is
"kept for 90 days, then deleted automatically". Nothing in `src/index.js` implements that.
It is enforced by the bucket's own lifecycle rule:

```
name:    delete-after-90-days
action:  Expire objects after 90 days
prefix:  (all prefixes)
enabled: Yes
```

This is deliberate. A promise kept by infrastructure survives bugs in application code.
The trade-off is that the rule is invisible from this repo, so it can be deleted in the
dashboard without any signal here, and the published claim silently becomes false.

Check it before believing the docs:

```sh
npx wrangler r2 bucket lifecycle list innodb-fyi-submissions
```

If that rule is missing, either restore it or change the user-facing copy. Do not "fix" it
by adding deletion logic to the Worker.

## Configuration

`MAX_UPLOAD_BYTES` (in `wrangler.toml`) is the real size limit. `MAX_SUBMIT_BYTES` in
`web/src/utils/consent.js` only exists so the UI can explain the cap up front; the two must
stay in sync, and this one wins.

`TURNSTILE_SECRET` is a Worker secret, never committed:

```sh
wrangler secret put TURNSTILE_SECRET
```

Verification fails closed - a missing secret rejects every request rather than letting
them through.

## Gotchas

- `wrangler r2 object get/put/delete` defaults to a **local simulated** bucket and prints
  `Delete complete.` having touched nothing real. Pass `--remote`, and verify through the
  API listing rather than wrangler's own output.
- After `wrangler deploy` or `wrangler secret put`, the old version can serve for a while.
  A secret you just set may still read as unset. Confirm `(100%)` on the new version via
  `wrangler deployments list` before concluding anything is broken.
