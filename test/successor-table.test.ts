import test from "node:test";
import assert from "node:assert/strict";
import { UserService } from "../app/UserService.ts";
import { SUCCESSOR_TABLE_MAX_LENGTH, type Node } from "../app/utils.ts";

// Regression test for #167: the grow phase of updateSuccessorTable() must not
// build the successor table past SUCCESSOR_TABLE_MAX_LENGTH, nor issue more
// getSuccessor() RPCs than needed to fill it. The old `i <= MAX` bound (and
// even a naive `i < MAX`) overshot because the table grows as `i` advances.
//
// We stand up a node whose ring is a simple ascending chain (0 -> 1 -> 2 -> …)
// and stub the three gRPC helpers updateSuccessorTable() depends on, so the
// test is deterministic and needs no live cluster.

function makeNode() {
  const node = new UserService({ id: 0, host: "h", port: 1000 });
  node.logger.level = "silent";

  let getSuccessorCalls = 0;
  // successor of node k is node k+1 — an endless distinct chain.
  node.getSuccessor = async (n: Node): Promise<Node> => {
    getSuccessorCalls++;
    const id = (n.id as number) + 1;
    return { id, host: "h", port: 1000 + id };
  };
  node.isOkSuccessor = async () => true;
  node.confirmExist = async () => true;

  const firstSuccessor: Node = { id: 1, host: "h", port: 1001 };
  node.fingerTable = [{ start: 1, successor: firstSuccessor }];
  node.successorTable = [firstSuccessor];

  return { node, getSuccessorCalls: () => getSuccessorCalls };
}

test("updateSuccessorTable fills the table to exactly MAX, no further", async () => {
  const { node } = makeNode();
  await node.updateSuccessorTable();
  assert.equal(
    node.successorTable.length,
    SUCCESSOR_TABLE_MAX_LENGTH,
    "table should be filled to exactly the max length",
  );
});

test("updateSuccessorTable never overshoots MAX during the grow phase", async () => {
  const { node, getSuccessorCalls } = makeNode();
  await node.updateSuccessorTable();
  // Filling a table from length 1 to MAX needs exactly MAX-1 appends, hence
  // MAX-1 getSuccessor() calls. The off-by-one bug made MAX+1 calls and grew
  // the table to MAX+2 before pruning it back down.
  assert.equal(
    getSuccessorCalls(),
    SUCCESSOR_TABLE_MAX_LENGTH - 1,
    "should issue exactly MAX-1 getSuccessor RPCs (no wasted calls)",
  );
});

test("updateSuccessorTable produces the contiguous successor chain", async () => {
  const { node } = makeNode();
  await node.updateSuccessorTable();
  const ids = node.successorTable.map((n) => n.id);
  assert.deepEqual(
    ids,
    Array.from({ length: SUCCESSOR_TABLE_MAX_LENGTH }, (_, k) => k + 1),
    "table should be the ring chain 1..MAX with no duplicates or gaps",
  );
});
