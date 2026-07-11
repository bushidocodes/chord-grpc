import test from "node:test";
import assert from "node:assert/strict";
import path from "path";

// Regression tests for #236: routing failures must surface as typed
// ChordRoutingErrors instead of the NULL_NODE sentinel, which callers had to
// remember to check and which proto3 mangles into { id: 0, host: "", port: 0 }
// on the wire.
process.env.CHORD_RPC_DEADLINE_MS = "300";
process.env.GRPC_CERTS_DIR = path.resolve(
  import.meta.dirname,
  "./no-such-certs-dir",
);

const { UserService } = await import("../app/UserService.ts");
const { ChordRoutingError, isNullNode, NULL_NODE, closeAllClients } =
  await import("../app/utils.ts");

const noopLogger = new Proxy({}, { get: () => () => {} });

function makeNode() {
  const node = new UserService({ id: 1, host: "localhost", port: 9999 });
  (node as any).logger = noopLogger;
  return node;
}

test("isNullNode catches the sentinel, its wire-mangled form, and partial nodes", () => {
  assert.equal(isNullNode(NULL_NODE), true);
  assert.equal(isNullNode({ id: 0, host: "", port: 0 }), true); // wire-mangled
  assert.equal(isNullNode({ id: 5, host: null, port: 9999 }), true); // partial
  assert.equal(isNullNode(null), true);
  assert.equal(isNullNode(undefined), true);
  assert.equal(isNullNode({ id: 5, host: "localhost", port: 9999 }), false);
  // id 0 is a valid ring position when host/port are usable
  assert.equal(isNullNode({ id: 0, host: "localhost", port: 9999 }), false);
});

test("getSuccessor throws ChordRoutingError when the local successor is uninitialized", async () => {
  const node = makeNode();
  // Fresh node, never joined: fingerTable[0].successor is NULL_NODE.
  await assert.rejects(
    node.getSuccessor(node.encapsulateSelf()),
    ChordRoutingError,
  );
});

test("findSuccessor throws ChordRoutingError when the queried node is unreachable", async () => {
  const node = makeNode();
  try {
    await assert.rejects(
      // Port 1 on localhost: connection refused, surfacing as UNAVAILABLE.
      node.findSuccessor(123, { id: 42, host: "localhost", port: 1 }),
      ChordRoutingError,
    );
  } finally {
    closeAllClients();
  }
});

test("a failed routing leg surfaces through insert instead of a sentinel write", async () => {
  const node = makeNode();
  (node as any).findSuccessor = async () => {
    throw new ChordRoutingError("no route");
  };

  const err = await new Promise<any>((resolve) => {
    node.insert({ request: { user: { id: 7 }, edit: false } }, (e: any) =>
      resolve(e),
    );
  });
  assert.ok(err, "insert with no usable route must not report success");
});
