import test from "node:test";
import assert from "node:assert/strict";
import { UserService } from "../app/UserService.ts";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
// destructor() logs a couple of expected confirmExist failures for an empty
// (single-node) cluster; silence the pino logger to keep test output clean.
const noopLogger = new Proxy({}, { get: () => () => {} });

// Regression test for #187/#239: the self-rescheduling maintenance loops
// started in joinCluster() must be cancelled by destructor() so they cannot
// race the teardown sequence.
test("destructor() cancels the periodic maintenance loops", async () => {
  const node = new UserService({ id: 1, host: "localhost", port: 9999 });
  (node as any).logger = noopLogger;

  let stabilizeCalls = 0;
  let fixFingersCalls = 0;
  let checkPredecessorCalls = 0;

  // Swap the maintenance methods for fast, network-free counters so the loops
  // exercise the same wiring joinCluster() sets up, minus the gRPC traffic.
  (node as any).stabilize = async () => {
    stabilizeCalls++;
  };
  (node as any).fixFingers = async () => {
    fixFingersCalls++;
  };
  (node as any).checkPredecessor = async () => {
    checkPredecessorCalls++;
    return true;
  };
  node.stabilizeIntervalMs = 5;
  node.fixFingersIntervalMs = 5;
  node.checkPredecessorIntervalMs = 5;

  // Joining self forms a single-node ring without any network traffic and
  // starts the real maintenance loops.
  await node.joinCluster({ id: null, host: "localhost", port: 9999 });

  try {
    await sleep(40);
    assert.ok(stabilizeCalls > 0, "loops should be firing before shutdown");
    assert.ok(fixFingersCalls > 0, "loops should be firing before shutdown");
    assert.ok(
      checkPredecessorCalls > 0,
      "loops should be firing before shutdown",
    );

    await node.destructor();

    const afterShutdown = {
      stabilizeCalls,
      fixFingersCalls,
      checkPredecessorCalls,
    };

    // Wait well beyond the interval; with the loops cancelled the counts freeze.
    await sleep(40);
    assert.deepEqual(
      { stabilizeCalls, fixFingersCalls, checkPredecessorCalls },
      afterShutdown,
      "no maintenance callback should fire after destructor()",
    );
    assert.equal(node.maintenanceTimers.size, 0);
  } finally {
    // No-op once destructor cancelled the loops; keeps the runner alive-free
    // even if an assertion above fails first.
    node.stopMaintenance();
  }
});

// Regression test for #239: a maintenance pass that throws must not disable
// the loop — the old boolean locks were only cleared on the happy path, so
// one uncaught throw silently wedged self-healing forever.
test("a throwing maintenance pass does not wedge the loop", async () => {
  const node = new UserService({ id: 1, host: "localhost", port: 9999 });
  (node as any).logger = noopLogger;

  let calls = 0;
  node.startMaintenanceLoop(
    "throwing",
    async () => {
      calls++;
      throw new Error("boom");
    },
    5,
  );

  try {
    await sleep(60);
    assert.ok(
      calls >= 2,
      `loop should keep re-scheduling after throws, ran ${calls} time(s)`,
    );
  } finally {
    node.stopMaintenance();
  }
});
