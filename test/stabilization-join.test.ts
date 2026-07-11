import test from "node:test";
import assert from "node:assert/strict";
import path from "path";

// Integration test for #237: a node joins an existing ring with the lazy
// (stabilization-based) protocol — successor from findSuccessor, predecessor
// repaired by stabilize/notify — and a correct two-node ring forms.
//
// node --test runs each file in its own process, so the env overrides below
// apply before app/config.ts loads: short cadences make convergence fast, and
// GRPC_CERTS_DIR pointing nowhere forces insecure transport for the
// test-local servers.
process.env.CHORD_STABILIZE_INTERVAL_MS = "50";
process.env.CHORD_FIX_FINGERS_INTERVAL_MS = "50";
process.env.CHORD_CHECK_PREDECESSOR_INTERVAL_MS = "50";
process.env.GRPC_CERTS_DIR = path.resolve(
  import.meta.dirname,
  "./no-such-certs-dir",
);

const { UserService } = await import("../app/UserService.ts");
const { closeAllClients } = await import("../app/utils.ts");

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
const noopLogger = new Proxy({}, { get: () => () => {} });

// Fixed high ports; two distinct ports also yield two distinct hashed IDs.
const PORT_A = 28440;
const PORT_B = 28441;

async function waitFor(
  condition: () => boolean,
  timeoutMs: number,
  message: string,
) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (condition()) return;
    await sleep(25);
  }
  assert.ok(condition(), message);
}

test("a second node joins via stabilization and a two-node ring converges", async () => {
  const nodeA = new UserService({ host: "localhost", port: PORT_A });
  const nodeB = new UserService({ host: "localhost", port: PORT_B });
  (nodeA as any).logger = noopLogger;
  (nodeB as any).logger = noopLogger;

  try {
    nodeA.serve();
    nodeB.serve();

    // A starts a new ring; B joins through A.
    await nodeA.joinCluster({ id: null, host: "localhost", port: PORT_A });
    await nodeB.joinCluster({ id: null, host: "localhost", port: PORT_A });

    // The join itself must already have adopted A as B's successor and
    // notified A (B's one-shot stabilize pass).
    assert.equal(nodeB.fingerTable[0].successor.id, nodeA.id);
    assert.equal(nodeA.predecessor.id, nodeB.id);

    // The remaining pointers converge via the maintenance loops:
    // A's stabilizeSelf kick adopts B as successor, then notifies B.
    await waitFor(
      () =>
        nodeA.fingerTable[0].successor.id === nodeB.id &&
        nodeB.predecessor.id === nodeA.id,
      5000,
      `ring did not converge: A.successor=${nodeA.fingerTable[0].successor.id} (want ${nodeB.id}), B.predecessor=${nodeB.predecessor?.id} (want ${nodeA.id})`,
    );

    // Every finger of the joining node must point somewhere real (the join
    // seeds them all with the successor; fixFingers refines them).
    for (const entry of nodeB.fingerTable) {
      assert.notEqual(entry.successor.id, null);
    }
  } finally {
    await nodeB.destructor();
    await nodeA.destructor();
    // Close cached outbound channels so the test process can exit; open
    // channels keep the event loop alive after the servers shut down.
    closeAllClients();
  }
});
