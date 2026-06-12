import test from "node:test";
import assert from "node:assert/strict";
import { withTimeout } from "../app/utils.ts";

// Regression tests for #176: graceful shutdown must not hang forever when a
// peer is unreachable. withTimeout() bounds destructor()'s deadline-less gRPC
// calls; these cover its three outcomes.

test("withTimeout resolves with the value when the promise settles in time", async () => {
  const result = await withTimeout(Promise.resolve("ok"), 1000, "timed out");
  assert.equal(result, "ok");
});

test("withTimeout rejects with the timeout error when the promise hangs", async () => {
  // A promise that never settles — the timeout must fire instead of hanging.
  await assert.rejects(
    withTimeout(new Promise<never>(() => {}), 20, "shutdown timed out"),
    /shutdown timed out/,
  );
});

test("withTimeout propagates the promise's own rejection", async () => {
  await assert.rejects(
    withTimeout(Promise.reject(new Error("boom")), 1000, "timed out"),
    /boom/,
  );
});
