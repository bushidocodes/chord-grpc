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

export const HASH_BIT_LENGTH = 32; //TBD
export const FIBONACCI_ALPHA = 0.7;
export const IS_FIBONACCI_CHORD: boolean = false;

export interface Node {
  id: number | null;
  host: string | null;
  port: number | null;
}

export const NULL_NODE: Node = { id: null, host: null, port: null };
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
export async function computeIntegerHash(
  stringForHashing: string,
  highOrderBits: boolean = true,
): Promise<number> {
  const MAX_JS_INT_BIT_LENGTH = 32;
  const BIT_PER_HEX_CHARACTER = 4;
  if (HASH_BIT_LENGTH > MAX_JS_INT_BIT_LENGTH) {
    console.error(
      `Warning. Requested ${HASH_BIT_LENGTH} bits `,
      `but only ${MAX_JS_INT_BIT_LENGTH} bits available due to numerical simplification.`,
    );
    process.exit(-9);
  }
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

export async function computeHostPortHash(
  host: string,
  port: number,
): Promise<number> {
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
 * Server-streaming methods (getFingerTableEntries, getUserIds) and
 * client-streaming methods (bulkInsertUsersRemoteHelper) remain unwrapped.
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
}) {
  if (!host || !port)
    throw new Error(`Cannot connect: null node (host=${host}, port=${port})`);
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
  // server-streaming: client sends one request, server sends a stream back
  const serverStreamMethods = new Set(["getFingerTableEntries", "getUserIds"]);
  // client-streaming: client sends a stream, server sends one response back
  const clientStreamMethods = new Set(["bulkInsertUsersRemoteHelper"]);
  const proto = Object.getPrototypeOf(client);

  for (const method of Object.keys(proto)) {
    if (method.startsWith("$") || method.startsWith("_")) continue;
    const original = proto[method];
    if (typeof original !== "function") continue;
    if (clientStreamMethods.has(method)) {
      // Bind only — callers get the raw writable stream and provide their own callback
      client[method] = client[method].bind(client);
    } else if (serverStreamMethods.has(method)) {
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
