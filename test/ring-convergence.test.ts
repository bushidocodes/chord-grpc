import assert from "node:assert/strict";
import test from "node:test";
import type { UserService } from "../app/UserService.ts";
import {
  fingersAreConverged,
  InMemoryNetwork,
  ringIsConverged,
  ringOwner,
  waitFor,
} from "./helpers/in-memory-network.ts";

// Multi-node integration tests (#234) over the in-memory transport (#233):
// deterministic, no sockets, fast enough for CI. Node ids are spread evenly
// across the 32-bit ring so keys distribute across nodes.

const N = 5;
const CONVERGENCE_TIMEOUT_MS = 15000;
const nodeId = (k: number) => Math.floor(((k + 1) * 2 ** 32) / (N + 1));

async function buildRing(net: InMemoryNetwork): Promise<UserService[]> {
  const nodes: UserService[] = [];
  const seed = { id: null, host: "mem", port: 9000 };
  for (let k = 0; k < N; k++) {
    const node = net.createNode(nodeId(k), 9000 + k);
    // First node forms the ring; the rest join through it.
    await node.joinCluster(
      k === 0 ? { ...seed } : { id: null, host: "mem", port: 9000 },
    );
    nodes.push(node);
  }
  await waitFor(
    () => ringIsConverged(nodes),
    CONVERGENCE_TIMEOUT_MS,
    `ring did not converge: ${describeRing(nodes)}`,
  );
  return nodes;
}

function describeRing(nodes: UserService[]): string {
  return nodes
    .map(
      (n) =>
        `{${n.id}: succ=${n.fingerTable[0].successor.id} pred=${n.predecessor.id}}`,
    )
    .join(" ");
}

test("N nodes joined sequentially converge to the sorted ring order", async () => {
  const net = new InMemoryNetwork();
  try {
    const nodes = await buildRing(net);
    assert.ok(ringIsConverged(nodes), describeRing(nodes));

    // Routing agrees with ring ownership from every vantage point.
    for (const probeHash of [0, 123456789, 2 ** 31, 2 ** 32 - 1]) {
      for (const node of nodes) {
        const found = await node.findSuccessor(probeHash);
        assert.equal(
          found.id,
          ringOwner(nodes, probeHash).id,
          `findSuccessor(${probeHash}) from {${node.id}}`,
        );
      }
    }
  } finally {
    net.shutdown();
  }
});

test("graceful departure migrates every key to the remaining ring", async () => {
  const net = new InMemoryNetwork();
  try {
    const nodes = await buildRing(net);
    const userIds = Array.from({ length: 25 }, (_, i) => i + 1);
    for (const userId of userIds) {
      const err = await nodes[0].insertReplicated({
        user: { id: userId, displayName: `User ${userId}` },
        edit: false,
      });
      assert.equal(err, null, `insert of user ${userId} should succeed`);
    }

    // Leave with the node holding the most keys, so the migration path is
    // actually exercised.
    const departing = nodes
      .map((node) => ({
        node,
        held: Object.values(node.userMap).flat().length,
      }))
      .sort((a, b) => b.held - a.held)[0].node;
    assert.ok(
      Object.values(departing.userMap).flat().length > 0,
      "departing node should hold keys",
    );

    await net.leave(departing);
    const remaining = nodes.filter((n) => n !== departing);
    await waitFor(
      () => ringIsConverged(remaining),
      CONVERGENCE_TIMEOUT_MS,
      `ring did not re-converge after departure: ${describeRing(remaining)}`,
    );
    // Lookups route through finger tables, which fixFingers repairs a
    // bounded number of sweeps after the successor chain heals.
    await waitFor(
      () => fingersAreConverged(remaining),
      CONVERGENCE_TIMEOUT_MS,
      "finger tables did not re-converge after departure",
    );

    // No key is lost: every user is still retrievable from any live node.
    for (const userId of userIds) {
      const { err, user } = await remaining[0].lookupReplicated(userId);
      assert.equal(err, null, `user ${userId} lost after graceful departure`);
      assert.ok(user, `user ${userId} lost after graceful departure`);
    }
  } finally {
    net.shutdown();
  }
});

test("ungraceful crash: ring re-converges and every key with a live replica survives", async () => {
  const net = new InMemoryNetwork();
  try {
    const nodes = await buildRing(net);
    const userIds = Array.from({ length: 25 }, (_, i) => i + 1);
    for (const userId of userIds) {
      const err = await nodes[0].insertReplicated({
        user: { id: userId, displayName: `User ${userId}` },
        edit: false,
      });
      assert.equal(err, null, `insert of user ${userId} should succeed`);
    }

    const crashed = nodes[2];
    // Which users still have a live replica? A user survives iff at least one
    // of its two hash locations was owned by a node other than the crashed
    // one. (Users with BOTH replicas on the crashed node are expected losses
    // until background re-replication exists — tracked in #225.)
    const survivors = userIds.filter((userId) => {
      const primaryOwner = ringOwner(
        nodes,
        nodes[0].computeUserIdHashPrimary(userId),
      );
      const secondaryOwner = ringOwner(
        nodes,
        nodes[0].computeUserIdHashSecondary(userId),
      );
      return primaryOwner !== crashed || secondaryOwner !== crashed;
    });
    assert.ok(
      survivors.length > 0,
      "test corpus should include users with a live replica",
    );

    net.crash(crashed);
    const remaining = nodes.filter((n) => n !== crashed);
    await waitFor(
      () => ringIsConverged(remaining),
      CONVERGENCE_TIMEOUT_MS,
      `ring did not re-converge after crash: ${describeRing(remaining)}`,
    );
    await waitFor(
      () => fingersAreConverged(remaining),
      CONVERGENCE_TIMEOUT_MS,
      "finger tables did not re-converge after crash",
    );

    for (const userId of survivors) {
      const { err, user } = await remaining[0].lookupReplicated(userId);
      assert.equal(
        err,
        null,
        `user ${userId} had a live replica but was lost after the crash`,
      );
      assert.ok(user);
    }
  } finally {
    net.shutdown();
  }
});
