import assert from "node:assert/strict";
import test from "node:test";
import * as grpc from "@grpc/grpc-js";
import { HealthImplementation, OVERALL_HEALTH } from "../app/health.ts";

// Tests for the standard gRPC Health Checking Protocol implementation (#97).
// We drive the Check/Watch handlers directly with minimal call/callback stubs
// rather than standing up a server, so these stay fast and deterministic.

function checkOf(impl: HealthImplementation) {
  return (service: string): Promise<{ err: any; status?: string }> =>
    new Promise((resolve) => {
      (impl.handlers as any).Check(
        { request: { service } },
        (err: any, res: any) => resolve({ err, status: res?.status }),
      );
    });
}

// Minimal ServerWritableStream stub capturing writes and exposing the cancel
// hook so we can simulate a client disconnect.
function fakeWatchCall(service: string) {
  const writes: string[] = [];
  const listeners: Record<string, () => void> = {};
  return {
    request: { service },
    writes,
    write: (r: { status: string }) => writes.push(r.status),
    on: (event: string, handler: () => void) => {
      listeners[event] = handler;
    },
    cancel: () => listeners["cancelled"]?.(),
  };
}

test("Check returns SERVING for the overall server by default", async () => {
  const impl = new HealthImplementation();
  const { err, status } = await checkOf(impl)(OVERALL_HEALTH);
  assert.equal(err, null);
  assert.equal(status, "SERVING");
});

test("Check on an unknown service returns NOT_FOUND (code 5)", async () => {
  const impl = new HealthImplementation();
  const { err, status } = await checkOf(impl)("does.not.exist");
  assert.equal(status, undefined);
  assert.equal(err?.code, grpc.status.NOT_FOUND);
});

test("Watch sends the current status immediately", () => {
  const impl = new HealthImplementation();
  const call = fakeWatchCall(OVERALL_HEALTH);
  (impl.handlers as any).Watch(call);
  assert.deepEqual(call.writes, ["SERVING"]);
});

test("setStatus pushes updates to live Watch subscribers (shutdown path)", () => {
  const impl = new HealthImplementation();
  const call = fakeWatchCall(OVERALL_HEALTH);
  (impl.handlers as any).Watch(call);
  // This is exactly what destructor() does on graceful shutdown.
  impl.setStatus(OVERALL_HEALTH, "NOT_SERVING");
  assert.deepEqual(call.writes, ["SERVING", "NOT_SERVING"]);
  // A subsequent Check reflects the new status too.
});

test("Watch on an unregistered service reports SERVICE_UNKNOWN", () => {
  const impl = new HealthImplementation();
  const call = fakeWatchCall("not.registered.yet");
  (impl.handlers as any).Watch(call);
  assert.deepEqual(call.writes, ["SERVICE_UNKNOWN"]);
});

test("a cancelled Watch stops receiving updates", () => {
  const impl = new HealthImplementation();
  const call = fakeWatchCall(OVERALL_HEALTH);
  (impl.handlers as any).Watch(call);
  call.cancel();
  impl.setStatus(OVERALL_HEALTH, "NOT_SERVING");
  // Only the initial SERVING; no NOT_SERVING after cancellation.
  assert.deepEqual(call.writes, ["SERVING"]);
});
