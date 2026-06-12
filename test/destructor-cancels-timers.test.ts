import test from "node:test";
import assert from "node:assert/strict";
import { UserService } from "../app/UserService.ts";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
// destructor() logs a couple of expected confirmExist failures for an empty
// (single-node) cluster; silence the pino logger to keep test output clean.
const noopLogger = new Proxy({}, { get: () => () => {} });

// Regression test for #187: the stabilize/fixFingers/checkPredecessor timers
// started in joinCluster() must be cancelled by destructor() so they cannot
// race the teardown sequence.
test("destructor() cancels the periodic maintenance timers", async () => {
  const node = new UserService({ id: 1, host: "localhost", port: 9999 });
  (node as any).logger = noopLogger;

  // destructor() ends with process.exit(0); neutralize it so the assertions
  // after the call still run (restored in finally).
  const realExit = process.exit;
  (process as any).exit = () => {};

  let stabilizeCalls = 0;
  let fixFingersCalls = 0;
  let checkPredecessorCalls = 0;

  // Swap the maintenance methods for fast, network-free counters so the timers
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

  // Mirror joinCluster(): store the interval handles on the very fields
  // destructor() is expected to clear (short period to keep the test fast).
  node.stabilizeTimer = setInterval(() => node.stabilize(), 5);
  node.fixFingersTimer = setInterval(() => node.fixFingers(), 5);
  node.checkPredecessorTimer = setInterval(() => node.checkPredecessor(), 5);

  try {
    await sleep(40);
    assert.ok(stabilizeCalls > 0, "timers should be firing before shutdown");

    await node.destructor();

    const afterShutdown = {
      stabilizeCalls,
      fixFingersCalls,
      checkPredecessorCalls,
    };

    // Wait well beyond the interval; with the timers cancelled the counts freeze.
    await sleep(40);
    assert.deepEqual(
      { stabilizeCalls, fixFingersCalls, checkPredecessorCalls },
      afterShutdown,
      "no maintenance callback should fire after destructor()",
    );
  } finally {
    // Clear timers (no-op once destructor cancelled them) and restore exit.
    clearInterval(node.stabilizeTimer);
    clearInterval(node.fixFingersTimer);
    clearInterval(node.checkPredecessorTimer);
    process.exit = realExit;
  }
});
