import test from "node:test";
import assert from "node:assert/strict";
import { isConnectionError } from "../client/common.ts";

// Regression tests for #206: bulkInsert must recognise an unreachable node so
// it can fail fast instead of printing one stack trace per record.

test("isConnectionError is true for gRPC UNAVAILABLE (code 14)", () => {
  assert.equal(isConnectionError({ code: 14 }), true);
});

test("isConnectionError is true for a raw ECONNREFUSED message", () => {
  assert.equal(
    isConnectionError(new Error("connect ECONNREFUSED 127.0.0.1:8440")),
    true,
  );
});

test("isConnectionError is true for grpc-js 'No connection established'", () => {
  assert.equal(
    isConnectionError({
      code: 14,
      message: "14 UNAVAILABLE: No connection established.",
    }),
    true,
  );
});

test("isConnectionError is false for ALREADY_EXISTS (code 6)", () => {
  assert.equal(isConnectionError({ code: 6 }), false);
});

test("isConnectionError is false for NOT_FOUND (code 5)", () => {
  assert.equal(isConnectionError({ code: 5 }), false);
});

test("isConnectionError is false for unrelated / empty errors", () => {
  assert.equal(isConnectionError(new Error("validation failed")), false);
  assert.equal(isConnectionError(undefined), false);
  assert.equal(isConnectionError(null), false);
  assert.equal(isConnectionError({}), false);
});
