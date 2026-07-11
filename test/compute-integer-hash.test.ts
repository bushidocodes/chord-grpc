import test from "node:test";
import assert from "node:assert/strict";
import crypto from "crypto";
import {
  computeIntegerHash,
  computeHostPortHash,
  sha1,
  HASH_BIT_LENGTH,
} from "../app/utils.ts";

// Direct unit tests for the hash truncation paths (#234, follow-up to #121).
// computeIntegerHash had a real historical bug (#98): inputs whose truncated
// SHA-1 has the top bit set produced NEGATIVE ids, which poison ring
// arithmetic. Both the high-order (primary) and low-order (secondary)
// truncation paths are covered against an independent reference.

const SAMPLE_INPUTS = [
  "1",
  "42",
  "localhost:8440",
  "localhost:8441",
  "a",
  "",
  "user-2188697",
  "0",
  "chord",
  ...Array.from({ length: 200 }, (_, i) => `input-${i}`),
];

function referenceHash(input: string, highOrderBits: boolean): number {
  const hex = crypto.createHash("sha1").update(input).digest("hex");
  // 32 bits = 8 hex characters, from either end of the digest.
  const slice = highOrderBits ? hex.slice(0, 8) : hex.slice(-8);
  // >>> 0 coerces to an unsigned 32-bit integer — the #98 fix's semantics.
  return parseInt(slice, 16) >>> 0;
}

test("HASH_BIT_LENGTH is 32 in this configuration (test assumes it)", () => {
  assert.equal(HASH_BIT_LENGTH, 32);
});

test("high-order truncation matches the reference and is never negative (#98)", () => {
  for (const input of SAMPLE_INPUTS) {
    const actual = computeIntegerHash(input, true);
    assert.equal(actual, referenceHash(input, true), `input "${input}"`);
    assert.ok(actual >= 0, `hash of "${input}" must be non-negative`);
    assert.ok(actual < 2 ** 32, `hash of "${input}" must fit the ring`);
    assert.ok(Number.isInteger(actual));
  }
});

test("low-order truncation matches the reference and is never negative (#98)", () => {
  for (const input of SAMPLE_INPUTS) {
    const actual = computeIntegerHash(input, false);
    assert.equal(actual, referenceHash(input, false), `input "${input}"`);
    assert.ok(actual >= 0, `hash of "${input}" must be non-negative`);
    assert.ok(actual < 2 ** 32, `hash of "${input}" must fit the ring`);
    assert.ok(Number.isInteger(actual));
  }
});

test("at least one sample would be negative without the unsigned coercion", () => {
  // Guards the test corpus itself: if no sample has the top bit set, the
  // non-negativity assertions above would pass vacuously.
  const highBitSet = SAMPLE_INPUTS.some(
    (input) => computeIntegerHash(input, true) >= 2 ** 31,
  );
  const lowBitSet = SAMPLE_INPUTS.some(
    (input) => computeIntegerHash(input, false) >= 2 ** 31,
  );
  assert.ok(highBitSet, "corpus should exercise the high-order sign bit");
  assert.ok(lowBitSet, "corpus should exercise the low-order sign bit");
});

test("high- and low-order paths generally disagree (dual-hash premise)", () => {
  const differing = SAMPLE_INPUTS.filter(
    (input) =>
      computeIntegerHash(input, true) !== computeIntegerHash(input, false),
  );
  assert.ok(
    differing.length > SAMPLE_INPUTS.length / 2,
    "primary and secondary hashes should usually differ",
  );
});

test("hashing is deterministic and computeHostPortHash is case-insensitive", () => {
  assert.equal(computeIntegerHash("stable"), computeIntegerHash("stable"));
  assert.equal(
    computeHostPortHash("LOCALHOST", 8440),
    computeHostPortHash("localhost", 8440),
  );
  assert.equal(sha1("abc"), "a9993e364706816aba3e25717850c26c9cd0d89d");
});
