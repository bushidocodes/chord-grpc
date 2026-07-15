import assert from "node:assert/strict";
import test from "node:test";
import { loadTlsCredentials } from "../app/utils.ts";

// Regression test for #171: loadTlsCredentials must not re-read the cert files
// on every call. We assert reference stability — when certs are present, two
// calls return the very same object, proving the read is memoized. Without the
// cache each call builds a fresh object from fresh fs.readFileSync buffers, so
// the references would differ. (With no certs present both calls return null,
// which is trivially stable, so this guard is meaningful when certs exist.)

test("loadTlsCredentials returns a stable (memoized) reference", () => {
  const a = loadTlsCredentials();
  const b = loadTlsCredentials();
  assert.equal(a, b, "repeated calls should return the identical cached value");
});

test("loadTlsCredentials returns null or a complete {ca,cert,key} set", () => {
  const creds = loadTlsCredentials();
  if (creds === null) return; // insecure fallback when no certs are present
  assert.ok(Buffer.isBuffer(creds.ca), "ca should be a Buffer");
  assert.ok(Buffer.isBuffer(creds.cert), "cert should be a Buffer");
  assert.ok(Buffer.isBuffer(creds.key), "key should be a Buffer");
});
