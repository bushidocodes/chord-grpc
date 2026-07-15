import assert from "node:assert/strict";
import test from "node:test";
import { UserService } from "../app/UserService.ts";

// Regression test for #241: library code must throw instead of calling
// process.exit(), so misuse is observable (and testable) in-process. The
// entrypoint (app/node.ts) owns catch-and-exit.
test("constructor throws when port is missing instead of exiting", () => {
  assert.throws(
    () => new UserService({ host: "localhost", port: 0 }),
    /did not receive host or port/,
  );
});
