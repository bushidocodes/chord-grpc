import assert from "node:assert/strict";
import test from "node:test";
import path from "path";

// Regression test for #235: outbound RPCs must carry a deadline so a peer
// that accepts the connection but never responds fails the call instead of
// blocking the caller (and its maintenance loop) forever.
//
// node --test runs each file in its own process, so the env overrides below
// take effect before app/config.ts (and everything importing it) loads:
// a short deadline keeps the test fast, and pointing GRPC_CERTS_DIR at a
// nonexistent directory forces insecure transport to match the test server.
process.env.CHORD_RPC_DEADLINE_MS = "300";
process.env.GRPC_CERTS_DIR = path.resolve(
  import.meta.dirname,
  "./no-such-certs-dir",
);

const grpc = await import("@grpc/grpc-js");
const { loadSync } = await import("@grpc/proto-loader");
const { connect } = await import("../app/utils.ts");

const packageDefinition = loadSync(
  path.resolve(import.meta.dirname, "../protos/chord.proto"),
  {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
  },
);
const chordProto = grpc.loadPackageDefinition(packageDefinition).chord as any;

test("unary calls fail with DEADLINE_EXCEEDED when the peer never responds", async () => {
  const server = new grpc.Server();
  server.addService(chordProto.Node.service, {
    // Accept the call, never invoke the callback: simulates a wedged peer.
    getPredecessor: () => {},
  });
  const port = await new Promise<number>((resolve, reject) => {
    server.bindAsync(
      "127.0.0.1:0",
      grpc.ServerCredentials.createInsecure(),
      (err, boundPort) => (err ? reject(err) : resolve(boundPort)),
    );
  });

  try {
    const client = connect({ host: "127.0.0.1", port });
    const startedAt = Date.now();
    await assert.rejects(
      client.getPredecessor(),
      (err: { code?: number }) => err.code === grpc.status.DEADLINE_EXCEEDED,
      "expected DEADLINE_EXCEEDED (code 4)",
    );
    const elapsed = Date.now() - startedAt;
    assert.ok(
      elapsed < 5000,
      `call should fail near the configured 300ms deadline, took ${elapsed}ms`,
    );
  } finally {
    server.forceShutdown();
  }
});
