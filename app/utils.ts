import * as grpc from "@grpc/grpc-js";
import crypto from "crypto";
import fs from "fs";
import path from "path";
import pino from "pino";
import process from "process";
import { config } from "./config.ts";
import type { FingerTableEntry__Output } from "./generated/chord/FingerTableEntry.ts";
import type { NodeAddress__Output } from "./generated/chord/NodeAddress.ts";
import type { User__Output, User as WireUser } from "./generated/chord/User.ts";
import type { UserIdWithMetadata__Output } from "./generated/chord/UserIdWithMetadata.ts";
import { chordProto } from "./proto.ts";

// JavaScript bitwise operations only work on 32-bit numbers; config.ts
// validates hashBitLength against this bound at load (#241/#242).
const MAX_JS_INT_BIT_LENGTH = 32;

// Operational tunables live in app/config.ts (env-configurable, validated at
// load). Re-exported under their historical names for the many existing call
// sites.
export const HASH_BIT_LENGTH = config.hashBitLength;
export const FIBONACCI_ALPHA = config.fibonacciAlpha;
export const IS_FIBONACCI_CHORD: boolean = config.isFibonacciChord;

export interface Node {
  id: number | null;
  host: string | null;
  port: number | null;
}

// The one legitimate protocol meaning of NULL_NODE is "no predecessor yet"
// (notify/checkPredecessor). It is no longer used as an error channel —
// routing failures throw ChordRoutingError instead (#236).
export const NULL_NODE: Node = { id: null, host: null, port: null };
export const SUCCESSOR_TABLE_MAX_LENGTH = config.successorTableMaxLength;

/**
 * True when a node value cannot be used as an RPC target. Catches both the
 * local NULL_NODE sentinel and its wire-mangled form: proto3 serializes
 * null id/host/port as 0/""/0 (defaults: true), so a NULL_NODE returned by
 * a remote arrives as { id: 0, host: "", port: 0 } and `id !== null` guards
 * never catch it (#236).
 */
export function isNullNode(node: Node | null | undefined): boolean {
  return !node || node.id == null || !node.host || !node.port;
}

/**
 * Thrown when a routing operation (findSuccessor / findPredecessor /
 * closestPrecedingFinger / getSuccessor) cannot produce a usable node.
 * Replaces the NULL_NODE error sentinel, which every caller had to remember
 * to check and several didn't (#236).
 */
export class ChordRoutingError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = "ChordRoutingError";
  }
}

// ---------------------------------------------------------------------------
// Typed promisified client surface (issue #240). Request types tolerate the
// domain's null-able Node fields because protobufjs treats null exactly like
// undefined (unset) when serializing; response types are the generated
// __Output shapes, which is what the wire actually delivers.
// ---------------------------------------------------------------------------

export interface WireNode {
  id?: number | null;
  host?: string | null;
  port?: number | null;
}

export interface WireRemoteId {
  id?: number | null;
  node?: WireNode | null;
}

export interface WireUserId {
  id?: number | null;
  userId?: number | null;
}

export interface WireUserEdit {
  user?: unknown;
  edit?: boolean;
  update_mask?: { paths?: string[] } | null;
}

/**
 * A promisified unary method: request-only returns a Promise; passing a
 * callback (with or without a request) uses classic callback style.
 */
interface UnaryCall<Req, Res> {
  (request?: Req): Promise<Res>;
  (request: Req, callback: (err: unknown, response: Res) => void): unknown;
  (callback: (err: unknown, response: Res) => void): unknown;
}

export interface PromisifiedNodeClient {
  // Chord protocol
  getNodeIdRemoteHelper: UnaryCall<WireNode, NodeAddress__Output>;
  findSuccessorRemoteHelper: UnaryCall<WireRemoteId, NodeAddress__Output>;
  getSuccessorRemoteHelper: UnaryCall<{}, NodeAddress__Output>;
  setSuccessor: UnaryCall<WireNode, {}>;
  getPredecessor: UnaryCall<{}, NodeAddress__Output>;
  setPredecessor: UnaryCall<WireNode, {}>;
  closestPrecedingFingerRemoteHelper: UnaryCall<
    WireRemoteId,
    NodeAddress__Output
  >;
  notify: UnaryCall<WireNode, {}>;
  // Application level
  fetch: UnaryCall<WireUserId, User__Output>;
  insert: UnaryCall<WireUserEdit, {}>;
  insertUserRemoteHelper: UnaryCall<WireUserEdit, {}>;
  lookup: UnaryCall<WireUserId, User__Output>;
  lookupUserRemoteHelper: UnaryCall<WireUserId, User__Output>;
  remove: UnaryCall<WireUserId, {}>;
  removeUserRemoteHelper: UnaryCall<WireUserId, {}>;
  migrateUsersToPredecessorRemoteHelper: UnaryCall<{}, {}>;
  // Streaming
  getFingerTableEntries(
    request?: WireNode,
  ): grpc.ClientReadableStream<FingerTableEntry__Output>;
  getUserIds(
    request?: WireNode,
  ): grpc.ClientReadableStream<UserIdWithMetadata__Output>;
  bulkInsertUsersRemoteHelper(
    callback: (err: unknown, response?: {}) => void,
  ): grpc.ClientWritableStream<WireUser>;
  // Base grpc.Client surface we rely on
  close(): void;
}

export function createLogger(host: string, port: number) {
  return pino({ level: process.env.LOG_LEVEL ?? "info" }).child({
    node: `${host}:${port}`,
  });
}

/**
 * Accounts for the modulo arithmetic to determine whether the input value is within the bounds.
 * Implements inclusive and exclusive properties for each bound, as specified.
 *
 *      USAGE
 *      includeLower == true means [lowerBound, ...
 *      includeLower == false means (lowerBound, ...
 *      includeUpper == true means ..., upperBound]
 *      includeUpper == false means ..., upperBound)
 *
 * returns true if the input value is in - modulo - bounds; false otherwise
 */
export function isInModuloRange(
  inputValue: number | null,
  lowerBound: number | null,
  includeLower: boolean = true,
  upperBound: number | null,
  includeUpper: boolean = false,
): boolean {
  if (inputValue === null || lowerBound === null || upperBound === null)
    return false;
  if (includeLower && includeUpper) {
    if (lowerBound > upperBound) {
      //looping through 0
      return inputValue >= lowerBound || inputValue <= upperBound;
    } else {
      return inputValue >= lowerBound && inputValue <= upperBound;
    }
  } else if (includeLower && !includeUpper) {
    if (lowerBound > upperBound) {
      //looping through 0
      return inputValue >= lowerBound || inputValue < upperBound;
    } else {
      return inputValue >= lowerBound && inputValue < upperBound;
    }
  } else if (!includeLower && includeUpper) {
    if (lowerBound > upperBound) {
      //looping through 0
      return inputValue > lowerBound || inputValue <= upperBound;
    } else {
      return inputValue > lowerBound && inputValue <= upperBound;
    }
  } else {
    if (lowerBound > upperBound) {
      //looping through 0
      return inputValue > lowerBound || inputValue < upperBound;
    } else {
      return inputValue > lowerBound && inputValue < upperBound;
    }
  }
}

/**
 * Computes the SHA-1 digest of the input as a lowercase hex string.
 * Hashing a short string is a microsecond-scale, CPU-only operation, so it
 * runs synchronously on the main thread.
 */
export function sha1(source: string): string {
  return crypto.createHash("sha1").update(source).digest("hex");
}

/** Compute a hash of desired length for the input string.
 * The function uses SHA-1 to compute an intermmediate string output,
 * then truncates to the user-specified size from the high-order bits.
 */
export function computeIntegerHash(
  stringForHashing: string,
  highOrderBits: boolean = true,
): number {
  const BIT_PER_HEX_CHARACTER = 4;
  let hashOutput = sha1(stringForHashing);
  // truncate because JavaScript only does bitwise operations on 32-bit numbers
  if (!highOrderBits) {
    // keep the low-order bits
    hashOutput = hashOutput.slice(
      -MAX_JS_INT_BIT_LENGTH / BIT_PER_HEX_CHARACTER,
    );
  } else {
    // keep the high-order bits
    hashOutput = hashOutput.slice(
      0,
      MAX_JS_INT_BIT_LENGTH / BIT_PER_HEX_CHARACTER,
    );
  }

  let integerHash: number;
  // convert from hexadecimal to decimal
  integerHash = parseInt("0x" + hashOutput);

  // truncate the hash to the desired number of bits
  if (!highOrderBits) {
    // by picking the low-order bits
    integerHash = (integerHash & (2 ** HASH_BIT_LENGTH - 1)) >>> 0;
  } else {
    // by picking the high-order bits
    integerHash = integerHash >>> (MAX_JS_INT_BIT_LENGTH - HASH_BIT_LENGTH);
  }

  return integerHash;
}

export function computeHostPortHash(host: string, port: number): number {
  return computeIntegerHash(`${host}:${port}`.toLowerCase());
}

interface GRPCError {
  code: number;
}

export function handleGRPCErrors(
  logger: pino.Logger,
  scope: string,
  call: string,
  host: string | null,
  port: number | null,
  err: unknown,
) {
  const grpcErr = err as GRPCError;
  const target = `${host}:${port}`;
  switch (grpcErr.code) {
    case 0:
      logger.warn(
        { scope, call, target },
        `${scope}: call to ${call} on ${target} returned OK. Should not have thrown`,
      );
      break;
    case 1:
      logger.warn(
        { scope, call, target },
        `${scope}: call to ${call} on ${target} was cancelled`,
      );
      break;
    case 2:
      logger.error(
        { scope, call, target },
        `${scope}: call to ${call} on ${target} returned unknown error`,
      );
      break;
    case 3:
      logger.error(
        { scope, call, target },
        `${scope}: call to ${call} on ${target} rejected due to invalid arguments`,
      );
      break;
    case 4:
      logger.error(
        { scope, call, target },
        `${scope}: call to ${call} on ${target} exceeded deadline`,
      );
      break;
    case 5:
      logger.warn(
        { scope, call, target },
        `${scope}: call to ${call} on ${target} requested an entity that was not found`,
      );
      break;
    case 6:
      logger.warn(
        { scope, call, target },
        `${scope}: call to ${call} on ${target} attempted to create an entity that already exists`,
      );
      break;
    case 7:
      logger.error(
        { scope, call, target },
        `${scope}: call to ${call} on ${target} rejected because permission was denied`,
      );
      break;
    case 8:
      logger.error(
        { scope, call, target, err: grpcErr },
        `${scope}: call to ${call} on ${target} failed because a resource is exhausted`,
      );
      break;
    case 9:
      logger.error(
        { scope, call, target, err: grpcErr },
        `${scope}: call to ${call} on ${target} failed due to failed precondition`,
      );
      break;
    case 10:
      logger.error(
        { scope, call, target, err: grpcErr },
        `${scope}: call to ${call} on ${target} was aborted`,
      );
      break;
    case 11:
      logger.error(
        { scope, call, target, err: grpcErr },
        `${scope}: call to ${call} on ${target} rejected because out of range`,
      );
      break;
    case 12:
      logger.error(
        { scope, call, target },
        `${scope}: call to ${call} on ${target}, which is unimplemented`,
      );
      break;
    case 13:
      logger.error(
        { scope, call, target, err: grpcErr },
        `${scope}: call to ${call} on ${target} caused Internal Error`,
      );
      break;
    case 14:
      // Drop the dead node's cached channel so the next connect() rebuilds it.
      channelCache.delete(`${host}:${port}`);
      logger.warn(
        { scope, call, target },
        `${scope}: Unable to connect to ${target}`,
      );
      break;
    case 15:
      logger.error(
        { scope, call, target },
        `${scope}: call to ${call} on ${target} failed due to unrecoverable data loss or corruption`,
      );
      break;
    case 16:
      logger.error(
        { scope, call, target },
        `${scope}: call to ${call} on ${target} rejected because authentication credentials were missing`,
      );
      break;
    default:
      logger.error({ scope, err: grpcErr }, `${scope}: unexpected gRPC error`);
  }
}

/**
 * Races a promise against a timeout. Rejects with `new Error(message)` if the
 * promise has not settled within `ms`; otherwise resolves/rejects with the
 * promise's own outcome. Used to bound graceful shutdown so an unreachable
 * peer can't block process exit forever (see #176). The timer is always
 * cleared so it never keeps the event loop alive after the race settles.
 */
export function withTimeout<T>(
  promise: Promise<T>,
  ms: number,
  message: string,
): Promise<T> {
  let timer: ReturnType<typeof setTimeout>;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(message)), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

// Canonical name used in ssl_target_name_override — must match a SAN in certs/server.crt.
const TLS_TARGET_NAME = config.tlsTargetName;

// Cache the loaded credentials: the cert files don't change while a process
// runs, so re-reading three files on every channel (re)creation is wasted disk
// I/O — connect() reads them on every cache miss / post-eviction reconnect, and
// connectHealth() reads them on every call (#171). Only the positive result is
// cached; while certs are absent we keep returning null (a cheap existsSync) so
// a later `gen-certs` is still picked up without restarting the process.
let cachedTlsCredentials: { ca: Buffer; cert: Buffer; key: Buffer } | null =
  null;

/**
 * Loads TLS credentials from the certs directory (GRPC_CERTS_DIR env var, or
 * <project-root>/certs by default). Returns null when certs are absent so
 * callers can fall back to insecure transport. The loaded result is memoized
 * (see cachedTlsCredentials above).
 */
export function loadTlsCredentials(): {
  ca: Buffer;
  cert: Buffer;
  key: Buffer;
} | null {
  if (cachedTlsCredentials) return cachedTlsCredentials;
  const certsDir =
    process.env.GRPC_CERTS_DIR ?? path.resolve(import.meta.dirname, "../certs");
  const caCert = path.join(certsDir, "ca.crt");
  if (!fs.existsSync(caCert)) return null;
  cachedTlsCredentials = {
    ca: fs.readFileSync(caCert),
    cert: fs.readFileSync(path.join(certsDir, "server.crt")),
    key: fs.readFileSync(path.join(certsDir, "server.key")),
  };
  return cachedTlsCredentials;
}

// Reuse one promisified gRPC client per host:port. Creating a client opens a
// TCP (and, with certs, TLS) connection, and connect() runs before every
// outbound RPC — in a multi-node ring that is dozens of new channels per
// second. Entries are evicted by handleGRPCErrors on an UNAVAILABLE (code 14)
// result so a dead node doesn't linger in the cache (issue #172).
const channelCache = new Map<string, PromisifiedNodeClient>();

/**
 * Creates a gRPC client for the Node service with promisified unary methods.
 * Server-streaming methods (getFingerTableEntries, getUserIds) and
 * client-streaming methods (bulkInsertUsersRemoteHelper) remain unwrapped.
 * Clients are cached per host:port and reused (see channelCache above).
 *
 * Uses TLS when certs/ca.crt exists; falls back to insecure transport
 * (useful for environments where certs have not been generated yet).
 */
export function connect({
  host,
  port,
}: {
  host: string | null;
  port: number | null;
}): PromisifiedNodeClient {
  if (!host || !port)
    throw new Error(`Cannot connect: null node (host=${host}, port=${port})`);
  const key = `${host}:${port}`;
  const cached = channelCache.get(key);
  if (cached) return cached;

  const tls = loadTlsCredentials();
  const credentials = tls
    ? grpc.credentials.createSsl(tls.ca)
    : grpc.credentials.createInsecure();
  const channelOptions = tls
    ? { "grpc.ssl_target_name_override": TLS_TARGET_NAME }
    : {};
  const raw = new chordProto.Node(key, credentials, channelOptions);
  const client = promisifyClient(raw);
  channelCache.set(key, client);
  return client;
}

// Per-call gRPC deadlines (issue #235): a peer that accepts the TCP
// connection but never responds must fail the call with DEADLINE_EXCEEDED
// instead of blocking the caller forever — a single hung call would otherwise
// permanently freeze the maintenance loop that issued it. Deadlines are
// absolute timestamps, so they are computed at call time, not wrap time.
function unaryCallOptions(): grpc.CallOptions {
  return { deadline: new Date(Date.now() + config.rpcDeadlineMs) };
}

function streamCallOptions(): grpc.CallOptions {
  return { deadline: new Date(Date.now() + config.streamDeadlineMs) };
}

/**
 * Closes every cached outbound channel and empties the cache. Intended for
 * process teardown (tests, CLI): open client channels otherwise keep the
 * Node event loop alive even after every server has shut down. Not called
 * from destructor() because multiple in-process nodes share the cache.
 */
export function closeAllClients() {
  for (const client of channelCache.values()) {
    client.close();
  }
  channelCache.clear();
}

function promisifyClient(client: grpc.Client): PromisifiedNodeClient {
  // Method kinds (unary / server-streaming / client-streaming) come from the
  // loaded service definition — the same source the types in app/generated
  // are generated from — instead of hand-maintained method-name string sets
  // that had to be kept in sync with chord.proto manually (issue #240). Add
  // an RPC to the proto and it is classified correctly here with no further
  // bookkeeping.
  const anyClient = client as any;
  for (const [method, definition] of Object.entries(
    chordProto.Node.service as grpc.ServiceDefinition,
  )) {
    const origMethod = anyClient[method].bind(client);
    if (definition.requestStream && definition.responseStream) {
      // No bidi-streaming RPCs in chord.proto; leave any future one raw.
      continue;
    }
    if (definition.requestStream) {
      // client-streaming: callers get the raw writable stream and provide
      // their own callback
      anyClient[method] = (callback: any) =>
        origMethod(streamCallOptions(), callback);
    } else if (definition.responseStream) {
      // server-streaming: wrap to allow zero-arg invocation
      anyClient[method] = (req?: any) =>
        origMethod(req || {}, streamCallOptions());
    } else {
      // Promisify unary calls, supporting optional request arg and optional callback
      anyClient[method] = (reqOrCb?: any, maybeCb?: any) => {
        // If called with a callback as second arg, use callback style
        if (typeof maybeCb === "function") {
          return origMethod(reqOrCb || {}, unaryCallOptions(), maybeCb);
        }
        // If called with a callback as first arg (no request), use callback style
        if (typeof reqOrCb === "function") {
          return origMethod({}, unaryCallOptions(), reqOrCb);
        }
        // Otherwise return a promise
        return new Promise((resolve, reject) => {
          origMethod(
            reqOrCb || {},
            unaryCallOptions(),
            (err: any, response: any) => {
              if (err) reject(err);
              else resolve(response);
            },
          );
        });
      };
    }
  }
  return anyClient as PromisifiedNodeClient;
}
