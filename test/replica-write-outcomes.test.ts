import assert from "node:assert/strict";
import test from "node:test";
import { UserService } from "../app/UserService.ts";

// Regression tests for #238: insert/remove perform two replica writes
// (primary + secondary hash); the combined outcome reported to the client
// must be honest about half-applied operations instead of claiming success
// when either leg succeeded.

const noopLogger = new Proxy({}, { get: () => () => {} });
const NOT_FOUND = 5;
const INTERNAL = 13;
const UNAVAILABLE = 14;

function makeNode() {
  const node = new UserService({ id: 1, host: "localhost", port: 9999 });
  (node as any).logger = noopLogger;
  return node;
}

function callInsert(node: UserService, user: { id: number }) {
  return node.insertReplicated({ user, edit: false }) as Promise<any>;
}

function callRemove(node: UserService, userId: number) {
  return node.removeReplicated(userId) as Promise<any>;
}

test("insert succeeds when both replica writes succeed", async () => {
  const node = makeNode();
  (node as any).insertWithHash = async () => null;
  assert.equal(await callInsert(node, { id: 7 }), null);
});

test("insert reports a degraded write when one replica fails", async () => {
  const node = makeNode();
  (node as any).insertWithHash = async (_u: any, isPrimaryHash: boolean) =>
    isPrimaryHash ? { code: UNAVAILABLE } : null;

  const err = await callInsert(node, { id: 7 });
  assert.ok(err, "one failed leg must not be reported as full success");
  assert.equal(err.code, INTERNAL);
  assert.match(err.details, /primary replica failed/);
  assert.match(err.details, /secondary hash location/);
});

test("insert fails when both replica writes fail", async () => {
  const node = makeNode();
  (node as any).insertWithHash = async () => ({ code: UNAVAILABLE });
  const err = await callInsert(node, { id: 7 });
  assert.equal(err?.code, UNAVAILABLE);
});

test("remove retries a failed leg once and succeeds when the retry lands", async () => {
  const node = makeNode();
  let primaryAttempts = 0;
  (node as any).removeWithHash = async (
    _id: number,
    isPrimaryHash: boolean,
  ) => {
    if (!isPrimaryHash) return null;
    primaryAttempts++;
    return primaryAttempts === 1 ? { code: UNAVAILABLE } : null;
  };

  const err = await callRemove(node, 7);
  assert.equal(err, null);
  assert.equal(primaryAttempts, 2, "failed leg should be retried exactly once");
});

test("remove reports an error when a leg still fails after the retry", async () => {
  const node = makeNode();
  (node as any).removeWithHash = async (_id: number, isPrimaryHash: boolean) =>
    isPrimaryHash ? { code: UNAVAILABLE } : null;

  const err = await callRemove(node, 7);
  assert.equal(
    err?.code,
    UNAVAILABLE,
    "a half-applied remove must surface as an error (lookup fallback would resurrect the user)",
  );
});

test("remove treats NOT_FOUND on one leg as success (no surviving copy)", async () => {
  const node = makeNode();
  let retries = 0;
  (node as any).removeWithHash = async (
    _id: number,
    isPrimaryHash: boolean,
  ) => {
    retries++;
    return isPrimaryHash ? { code: NOT_FOUND } : null;
  };

  const err = await callRemove(node, 7);
  assert.equal(err, null);
  assert.equal(retries, 2, "NOT_FOUND is a terminal state, not retried");
});

test("remove still reports NOT_FOUND when the user existed at neither location", async () => {
  const node = makeNode();
  (node as any).removeWithHash = async () => ({ code: NOT_FOUND });
  const err = await callRemove(node, 7);
  assert.equal(err?.code, NOT_FOUND);
});
