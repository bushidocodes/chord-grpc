import assert from "node:assert/strict";
import test from "node:test";
import { isInModuloRange } from "../app/utils.ts";

// Direct unit tests for isInModuloRange (#234, follow-up to #121). This
// function underpins every routing decision in the codebase — findPredecessor,
// closestPrecedingFinger, notify, stabilize, and key-migration filtering all
// depend on it — across four inclusion variants and wraparound-through-zero.
//
// Strategy: instead of sampling, compare EXHAUSTIVELY against a slow
// reference implementation on a small ring. isInModuloRange is
// modulus-agnostic (pure comparisons), so agreement on a size-16 ring for
// every (input, lower, upper) triple and every inclusion combination covers
// all orderings the real 2^32 ring can produce: input/lower/upper distinct in
// every relative order, pairwise equal, all equal, and wraparound intervals.

const RING_SIZE = 16;

/**
 * Reference: enumerate the clockwise walk from lower to upper and collect the
 * members. When lower === upper the walk has zero steps, so the interval is
 * empty unless both endpoints are inclusive ([a, a] = {a}). (The "(n, n) is
 * the whole ring" reading used by Chord's notify() is handled explicitly at
 * that call site — see notifiedBy() in ChordNode.ts.)
 */
function referenceMembers(
  lower: number,
  includeLower: boolean,
  upper: number,
  includeUpper: boolean,
): Set<number> {
  const members = new Set<number>();
  const span = (upper - lower + RING_SIZE) % RING_SIZE;
  for (let step = 0; step <= span; step++) {
    if (step === 0 && !includeLower) continue;
    if (step === span && !includeUpper) continue;
    members.add((lower + step) % RING_SIZE);
  }
  return members;
}

test("isInModuloRange agrees with the reference on every size-16 ring triple", () => {
  let checked = 0;
  for (const includeLower of [true, false]) {
    for (const includeUpper of [true, false]) {
      for (let lower = 0; lower < RING_SIZE; lower++) {
        for (let upper = 0; upper < RING_SIZE; upper++) {
          const members = referenceMembers(
            lower,
            includeLower,
            upper,
            includeUpper,
          );
          for (let input = 0; input < RING_SIZE; input++) {
            const actual = isInModuloRange(
              input,
              lower,
              includeLower,
              upper,
              includeUpper,
            );
            assert.equal(
              actual,
              members.has(input),
              `isInModuloRange(${input}, ${lower}, ${includeLower}, ${upper}, ${includeUpper}) ` +
                `should be ${members.has(input)} (interval = {${[...members].join(",")}})`,
            );
            checked++;
          }
        }
      }
    }
  }
  // 4 flag combos x 16^3 triples
  assert.equal(checked, 4 * RING_SIZE ** 3);
});

test("wraparound-through-zero intervals include both sides of zero", () => {
  // (14, 2]: members 15, 0, 1, 2
  assert.equal(isInModuloRange(15, 14, false, 2, true), true);
  assert.equal(isInModuloRange(0, 14, false, 2, true), true);
  assert.equal(isInModuloRange(2, 14, false, 2, true), true);
  assert.equal(isInModuloRange(14, 14, false, 2, true), false);
  assert.equal(isInModuloRange(7, 14, false, 2, true), false);
});

test("lower === upper is empty except for the doubly-inclusive singleton", () => {
  assert.equal(isInModuloRange(5, 5, true, 5, true), true); // [a, a] = {a}
  assert.equal(isInModuloRange(5, 5, false, 5, true), false); // (a, a]
  assert.equal(isInModuloRange(5, 5, true, 5, false), false); // [a, a)
  assert.equal(isInModuloRange(5, 5, false, 5, false), false); // (a, a)
  assert.equal(isInModuloRange(6, 5, true, 5, true), false);
});

test("null operands are never in range", () => {
  assert.equal(isInModuloRange(null, 1, true, 5, true), false);
  assert.equal(isInModuloRange(3, null, true, 5, true), false);
  assert.equal(isInModuloRange(3, 1, true, null, true), false);
});
