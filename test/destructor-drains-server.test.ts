import test from "node:test";
import assert from "node:assert/strict";
import { UserService } from "../app/UserService.ts";

// destructor() logs a couple of expected confirmExist failures for an empty
// (single-node) cluster; silence the pino logger to keep test output clean.
const noopLogger = new Proxy({}, { get: () => () => {} });

function makeNode() {
  const node = new UserService({ id: 1, host: "localhost", port: 9999 });
  (node as any).logger = noopLogger;
  return node;
}

// Regression tests for #243: graceful shutdown must drain the gRPC server
// (tryShutdown) so in-flight RPCs complete, with forceShutdown as the
// fallback when the drain hangs.
test("destructor() drains the server via tryShutdown", async () => {
  const node = makeNode();
  let tryShutdownCalls = 0;
  let forceShutdownCalls = 0;
  node.server = {
    tryShutdown(callback: (error?: Error) => void) {
      tryShutdownCalls++;
      callback();
    },
    forceShutdown() {
      forceShutdownCalls++;
    },
  };

  await node.destructor();

  assert.equal(tryShutdownCalls, 1, "tryShutdown should be called once");
  assert.equal(
    forceShutdownCalls,
    0,
    "forceShutdown should not run when the drain succeeds",
  );
});

test("destructor() falls back to forceShutdown when the drain hangs", async () => {
  const node = makeNode();
  node.drainTimeoutMs = 20;
  let forceShutdownCalls = 0;
  node.server = {
    tryShutdown(_callback: (error?: Error) => void) {
      // Never invoke the callback: simulates a wedged in-flight call.
    },
    forceShutdown() {
      forceShutdownCalls++;
    },
  };

  await node.destructor();

  assert.equal(
    forceShutdownCalls,
    1,
    "forceShutdown should run when tryShutdown exceeds the drain timeout",
  );
});
