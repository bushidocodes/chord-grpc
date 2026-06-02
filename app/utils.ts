import fs from "fs";
import path from "path";
import process from "process";
import crypto from "crypto";
import * as grpc from "@grpc/grpc-js";
import { loadSync } from "@grpc/proto-loader";
import pino from "pino";

const PROTO_PATH = path.resolve(import.meta.dirname, "../protos/chord.proto");

const packageDefinition = loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});
const chordProto = grpc.loadPackageDefinition(packageDefinition).chord as any;

export const HASH_BIT_LENGTH = 52;
export const FIBONACCI_ALPHA = 0.7;
export const IS_FIBONACCI_CHORD: boolean = false;
export const NULL_NODE = { id: null, host: null, port: null };
export const SUCCESSOR_TABLE_MAX_LENGTH = Math.max(
  Math.ceil(HASH_BIT_LENGTH / 4),
  1,
);

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
  inputValue: number,
  lowerBound: number,
  includeLower: boolean = true,
  upperBound: number,
  includeUpper: boolean = false,
): boolean {
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

/**
 * Hash helpers — 52-bit ceiling for JS double arithmetic.
 *
 * Ring arithmetic intermediates: (2^52 - 1) + 2^51 = 3×2^51 - 1 < MAX_SAFE_INTEGER.
 * At 53 bits that sum would exceed MAX_SAFE_INTEGER and silently corrupt
 * finger-table start values.  JS bitwise ops (>>>, &) are not used because
 * they truncate to 32 bits; Math.floor and % are used instead.
 */
function guardHashBitLength() {
  if (HASH_BIT_LENGTH > 52) {
    console.error(
      `HASH_BIT_LENGTH=${HASH_BIT_LENGTH} exceeds the safe 52-bit ceiling for JS double arithmetic.`,
    );
    process.exit(-9);
  }
}

/** Hash using the most-significant 52 bits of SHA-1. Used for node IDs and primary user keys. */
export async function computeHashHighBits(str: string): Promise<number> {
  guardHashBitLength();
  // 13 hex chars = 52 bits; at most 2^52 - 1, safely below MAX_SAFE_INTEGER.
  const raw = parseInt("0x" + sha1(str).slice(0, 13));
  return Math.floor(raw / 2 ** (52 - HASH_BIT_LENGTH));
}

/** Hash using the least-significant 52 bits of SHA-1. Used for secondary user keys. */
export async function computeHashLowBits(str: string): Promise<number> {
  guardHashBitLength();
  const raw = parseInt("0x" + sha1(str).slice(-13));
  return raw % 2 ** HASH_BIT_LENGTH;
}

export async function computeHostPortHash(
  host: string,
  port: number,
): Promise<number> {
  return computeHashHighBits(`${host}:${port}`.toLowerCase());
}

interface GRPCError {
  code: number;
}

export function handleGRPCErrors(
  logger: pino.Logger,
  scope: string,
  call: string,
  host: string,
  port: number,
  err: GRPCError,
) {
  const target = `${host}:${port}`;
  switch (err.code) {
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
        { scope, call, target, err },
        `${scope}: call to ${call} on ${target} failed because a resource is exhausted`,
      );
      break;
    case 9:
      logger.error(
        { scope, call, target, err },
        `${scope}: call to ${call} on ${target} failed due to failed precondition`,
      );
      break;
    case 10:
      logger.error(
        { scope, call, target, err },
        `${scope}: call to ${call} on ${target} was aborted`,
      );
      break;
    case 11:
      logger.error(
        { scope, call, target, err },
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
        { scope, call, target, err },
        `${scope}: call to ${call} on ${target} caused Internal Error`,
      );
      break;
    case 14:
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
      logger.error({ scope, err }, `${scope}: unexpected gRPC error`);
  }
}

// Canonical name used in ssl_target_name_override — must match a SAN in certs/server.crt.
const TLS_TARGET_NAME = "chord-node";

/**
 * Loads TLS credentials from the certs directory (GRPC_CERTS_DIR env var, or
 * <project-root>/certs by default). Returns null when certs are absent so
 * callers can fall back to insecure transport.
 */
export function loadTlsCredentials(): {
  ca: Buffer;
  cert: Buffer;
  key: Buffer;
} | null {
  const certsDir =
    process.env.GRPC_CERTS_DIR ?? path.resolve(import.meta.dirname, "../certs");
  const caCert = path.join(certsDir, "ca.crt");
  if (!fs.existsSync(caCert)) return null;
  return {
    ca: fs.readFileSync(caCert),
    cert: fs.readFileSync(path.join(certsDir, "server.crt")),
    key: fs.readFileSync(path.join(certsDir, "server.key")),
  };
}

/**
 * Creates a gRPC client for the Node service with promisified unary methods.
 * Streaming methods (getFingerTableEntries, getUserIds) remain unchanged.
 *
 * Uses TLS when certs/ca.crt exists; falls back to insecure transport
 * (useful for environments where certs have not been generated yet).
 */
export function connect({ host, port }: { host: string; port: number }) {
  const tls = loadTlsCredentials();
  const credentials = tls
    ? grpc.credentials.createSsl(tls.ca)
    : grpc.credentials.createInsecure();
  const channelOptions = tls
    ? { "grpc.ssl_target_name_override": TLS_TARGET_NAME }
    : {};
  const raw = new chordProto.Node(
    `${host}:${port}`,
    credentials,
    channelOptions,
  );
  return promisifyClient(raw);
}

function promisifyClient(client: any) {
  const streamMethods = new Set(["getFingerTableEntries", "getUserIds"]);
  const proto = Object.getPrototypeOf(client);

  for (const method of Object.keys(proto)) {
    if (method.startsWith("$") || method.startsWith("_")) continue;
    const original = proto[method];
    if (typeof original !== "function") continue;
    if (streamMethods.has(method)) {
      // Wrap streaming calls to allow zero-arg invocation
      const origMethod = client[method].bind(client);
      client[method] = (req?: any) => origMethod(req || {});
    } else {
      // Promisify unary calls, supporting optional request arg and optional callback
      const origMethod = client[method].bind(client);
      client[method] = (reqOrCb?: any, maybeCb?: any) => {
        // If called with a callback as second arg, use callback style
        if (typeof maybeCb === "function") {
          return origMethod(reqOrCb || {}, maybeCb);
        }
        // If called with a callback as first arg (no request), use callback style
        if (typeof reqOrCb === "function") {
          return origMethod({}, reqOrCb);
        }
        // Otherwise return a promise
        return new Promise((resolve, reject) => {
          origMethod(reqOrCb || {}, (err: any, response: any) => {
            if (err) reject(err);
            else resolve(response);
          });
        });
      };
    }
  }
  return client;
}
