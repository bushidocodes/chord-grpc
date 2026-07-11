import test from "node:test";
import assert from "node:assert/strict";
import { chordProto, healthProto } from "../app/proto.ts";
import { closeAllClients, connect } from "../app/utils.ts";

// Regression tests for #240: the promisified client is driven by the loaded
// service definition (the same source the generated types come from), not by
// hand-maintained method-name string sets that could drift from chord.proto.

test("every RPC in chord.proto is wrapped on the promisified client", () => {
  try {
    // Creating a client does not dial; gRPC connects lazily on first call.
    const client = connect({ host: "localhost", port: 65500 }) as any;
    const methods = Object.keys(chordProto.Node.service);
    assert.ok(methods.length > 0, "service definition should have methods");
    for (const method of methods) {
      assert.equal(
        typeof client[method],
        "function",
        `client.${method} should be wrapped`,
      );
    }
  } finally {
    closeAllClients();
  }
});

test("streaming-ness is derived from the service definition", () => {
  const service = chordProto.Node.service as any;
  // Spot-check the definition against what chord.proto declares, so a
  // regression in loader options (e.g. keepCase) is caught here.
  assert.equal(service.getFingerTableEntries.responseStream, true);
  assert.equal(service.getUserIds.responseStream, true);
  assert.equal(service.bulkInsertUsersRemoteHelper.requestStream, true);
  assert.equal(service.findSuccessorRemoteHelper.requestStream, false);
  assert.equal(service.findSuccessorRemoteHelper.responseStream, false);
});

test("the shared proto module exposes the health service", () => {
  assert.equal(typeof healthProto.Health, "function");
  assert.ok(healthProto.Health.service.Check);
  assert.ok(healthProto.Health.service.Watch);
});
