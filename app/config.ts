import process from "process";

/**
 * Central, env-configurable operational tunables (issue #242).
 *
 * Every value reads an environment variable, falling back to the historical
 * hard-coded value, and is validated at module load so a bad value fails
 * fast at startup instead of misbehaving at runtime. LOG_LEVEL and
 * GRPC_CERTS_DIR established the env-var pattern; the CHORD_* names below
 * follow it.
 *
 * The parse helpers are exported for direct unit testing (they read
 * process.env at call time).
 */

export function intFromEnv(name: string, defaultValue: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw === "") return defaultValue;
  const value = Number(raw);
  if (!Number.isInteger(value)) {
    throw new RangeError(`${name} must be an integer, got "${raw}"`);
  }
  return value;
}

export function floatFromEnv(name: string, defaultValue: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw === "") return defaultValue;
  const value = Number(raw);
  if (!Number.isFinite(value)) {
    throw new RangeError(`${name} must be a number, got "${raw}"`);
  }
  return value;
}

export function boolFromEnv(name: string, defaultValue: boolean): boolean {
  const raw = process.env[name];
  if (raw === undefined || raw === "") return defaultValue;
  if (raw === "true" || raw === "1") return true;
  if (raw === "false" || raw === "0") return false;
  throw new RangeError(`${name} must be true/false/1/0, got "${raw}"`);
}

export function stringFromEnv(name: string, defaultValue: string): string {
  const raw = process.env[name];
  return raw === undefined || raw === "" ? defaultValue : raw;
}

function assertPositive(name: string, value: number): number {
  if (value <= 0) {
    throw new RangeError(`${name} must be positive, got ${value}`);
  }
  return value;
}

// JavaScript bitwise operations only work on 32-bit numbers, so the hash
// space cannot exceed 32 bits (see computeIntegerHash in utils.ts).
const MAX_HASH_BIT_LENGTH = 32;
const hashBitLength = intFromEnv("CHORD_HASH_BIT_LENGTH", 32);
if (hashBitLength < 1 || hashBitLength > MAX_HASH_BIT_LENGTH) {
  throw new RangeError(
    `CHORD_HASH_BIT_LENGTH must be between 1 and ${MAX_HASH_BIT_LENGTH} (JavaScript bitwise operations are 32-bit), got ${hashBitLength}`,
  );
}

const fibonacciAlpha = floatFromEnv("CHORD_FIBONACCI_ALPHA", 0.7);
if (fibonacciAlpha < 0 || fibonacciAlpha > 1) {
  throw new RangeError(
    `CHORD_FIBONACCI_ALPHA must be between 0 and 1, got ${fibonacciAlpha}`,
  );
}

export const config = {
  // --- Hash space (app/utils.ts) ---
  hashBitLength,
  fibonacciAlpha,
  isFibonacciChord: boolFromEnv("CHORD_IS_FIBONACCI", false),
  successorTableMaxLength: Math.max(Math.ceil(hashBitLength / 4), 1),

  // --- Maintenance cadence (app/ChordNode.ts) ---
  stabilizeIntervalMs: assertPositive(
    "CHORD_STABILIZE_INTERVAL_MS",
    intFromEnv("CHORD_STABILIZE_INTERVAL_MS", 1000),
  ),
  fixFingersIntervalMs: assertPositive(
    "CHORD_FIX_FINGERS_INTERVAL_MS",
    intFromEnv("CHORD_FIX_FINGERS_INTERVAL_MS", 3000),
  ),
  checkPredecessorIntervalMs: assertPositive(
    "CHORD_CHECK_PREDECESSOR_INTERVAL_MS",
    intFromEnv("CHORD_CHECK_PREDECESSOR_INTERVAL_MS", 1000),
  ),

  // --- Outbound RPC deadlines (app/utils.ts, app/health.ts) ---
  // Unary calls: how long a peer may take to answer before the call fails
  // with DEADLINE_EXCEEDED instead of blocking the caller forever (#235).
  rpcDeadlineMs: assertPositive(
    "CHORD_RPC_DEADLINE_MS",
    intFromEnv("CHORD_RPC_DEADLINE_MS", 3000),
  ),
  // Streaming calls (getFingerTableEntries, getUserIds, bulkInsert): sized
  // for stream duration rather than a single round trip.
  streamDeadlineMs: assertPositive(
    "CHORD_STREAM_DEADLINE_MS",
    intFromEnv("CHORD_STREAM_DEADLINE_MS", 30000),
  ),

  // --- Shutdown (app/node.ts, app/ChordNode.ts) ---
  shutdownTimeoutMs: assertPositive(
    "CHORD_SHUTDOWN_TIMEOUT_MS",
    intFromEnv("CHORD_SHUTDOWN_TIMEOUT_MS", 5000),
  ),
  drainTimeoutMs: assertPositive(
    "CHORD_DRAIN_TIMEOUT_MS",
    intFromEnv("CHORD_DRAIN_TIMEOUT_MS", 2000),
  ),

  // --- Web crawler (web/web.ts); --interval flag overrides ---
  crawlerIntervalMs: assertPositive(
    "CHORD_CRAWLER_INTERVAL_MS",
    intFromEnv("CHORD_CRAWLER_INTERVAL_MS", 3000),
  ),

  // --- TLS (app/utils.ts): canonical name used in ssl_target_name_override;
  // must match a SAN in certs/server.crt ---
  tlsTargetName: stringFromEnv("CHORD_TLS_TARGET_NAME", "chord-node"),
} as const;
