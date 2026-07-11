import test from "node:test";
import assert from "node:assert/strict";
import {
  boolFromEnv,
  config,
  floatFromEnv,
  intFromEnv,
  stringFromEnv,
} from "../app/config.ts";

// The helpers read process.env at call time, so they can be exercised
// directly; the config object itself is validated at module load (#242).

function withEnv(name: string, value: string | undefined, fn: () => void) {
  const previous = process.env[name];
  if (value === undefined) delete process.env[name];
  else process.env[name] = value;
  try {
    fn();
  } finally {
    if (previous === undefined) delete process.env[name];
    else process.env[name] = previous;
  }
}

test("intFromEnv returns the default when the variable is unset or empty", () => {
  withEnv("CHORD_TEST_INT", undefined, () => {
    assert.equal(intFromEnv("CHORD_TEST_INT", 42), 42);
  });
  withEnv("CHORD_TEST_INT", "", () => {
    assert.equal(intFromEnv("CHORD_TEST_INT", 42), 42);
  });
});

test("intFromEnv parses integers and rejects garbage", () => {
  withEnv("CHORD_TEST_INT", "250", () => {
    assert.equal(intFromEnv("CHORD_TEST_INT", 42), 250);
  });
  withEnv("CHORD_TEST_INT", "not-a-number", () => {
    assert.throws(() => intFromEnv("CHORD_TEST_INT", 42), RangeError);
  });
  withEnv("CHORD_TEST_INT", "1.5", () => {
    assert.throws(() => intFromEnv("CHORD_TEST_INT", 42), RangeError);
  });
});

test("floatFromEnv parses numbers and rejects garbage", () => {
  withEnv("CHORD_TEST_FLOAT", "0.25", () => {
    assert.equal(floatFromEnv("CHORD_TEST_FLOAT", 0.7), 0.25);
  });
  withEnv("CHORD_TEST_FLOAT", "seven", () => {
    assert.throws(() => floatFromEnv("CHORD_TEST_FLOAT", 0.7), RangeError);
  });
});

test("boolFromEnv accepts true/false/1/0 and rejects anything else", () => {
  withEnv("CHORD_TEST_BOOL", "true", () => {
    assert.equal(boolFromEnv("CHORD_TEST_BOOL", false), true);
  });
  withEnv("CHORD_TEST_BOOL", "0", () => {
    assert.equal(boolFromEnv("CHORD_TEST_BOOL", true), false);
  });
  withEnv("CHORD_TEST_BOOL", "yes", () => {
    assert.throws(() => boolFromEnv("CHORD_TEST_BOOL", false), RangeError);
  });
});

test("stringFromEnv falls back on unset or empty", () => {
  withEnv("CHORD_TEST_STRING", undefined, () => {
    assert.equal(stringFromEnv("CHORD_TEST_STRING", "fallback"), "fallback");
  });
  withEnv("CHORD_TEST_STRING", "custom", () => {
    assert.equal(stringFromEnv("CHORD_TEST_STRING", "fallback"), "custom");
  });
});

test("config carries the documented defaults", () => {
  // These match the historical hard-coded values the env vars replaced.
  assert.equal(config.hashBitLength, 32);
  assert.equal(config.successorTableMaxLength, 8);
  assert.equal(config.stabilizeIntervalMs, 1000);
  assert.equal(config.fixFingersIntervalMs, 3000);
  assert.equal(config.checkPredecessorIntervalMs, 1000);
  assert.equal(config.shutdownTimeoutMs, 5000);
  assert.equal(config.drainTimeoutMs, 2000);
  assert.equal(config.crawlerIntervalMs, 3000);
  assert.equal(config.tlsTargetName, "chord-node");
});
