import assert from "node:assert/strict";
import test from "node:test";
import pino from "pino";
import { connect, handleGRPCErrors } from "../app/utils.ts";

// Regression tests for #172: connect() must reuse one gRPC client per
// host:port and evict it when handleGRPCErrors sees UNAVAILABLE (code 14).
// gRPC clients are lazy (no connection until an RPC), so creating them with
// bogus hosts is fine; we compare references and close them afterwards.
const silent = pino({ level: "silent" });

test("connect() returns the same cached client for the same host:port", () => {
  const a = connect({ host: "cache-test-1", port: 1111 });
  const b = connect({ host: "cache-test-1", port: 1111 });
  assert.equal(a, b, "same host:port should return the identical client");
  a.close?.();
});

test("connect() returns distinct clients for different host:port", () => {
  const a = connect({ host: "cache-test-2", port: 2222 });
  const b = connect({ host: "cache-test-2", port: 3333 });
  assert.notEqual(a, b, "different host:port should return different clients");
  a.close?.();
  b.close?.();
});

test("handleGRPCErrors evicts the cached client on UNAVAILABLE (code 14)", () => {
  const before = connect({ host: "cache-test-3", port: 4444 });
  handleGRPCErrors(silent, "test", "ping", "cache-test-3", 4444, { code: 14 });
  const after = connect({ host: "cache-test-3", port: 4444 });
  assert.notEqual(before, after, "code 14 should evict, forcing a new client");
  before.close?.();
  after.close?.();
});

test("handleGRPCErrors does not evict on non-UNAVAILABLE errors", () => {
  const before = connect({ host: "cache-test-4", port: 5555 });
  handleGRPCErrors(silent, "test", "ping", "cache-test-4", 5555, { code: 5 });
  const after = connect({ host: "cache-test-4", port: 5555 });
  assert.equal(before, after, "non-14 errors should leave the cache intact");
  before.close?.();
});
