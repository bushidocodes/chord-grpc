import * as grpc from "@grpc/grpc-js";
import { loadSync } from "@grpc/proto-loader";
import path from "path";
import type { ProtoGrpcType as ChordProtoGrpcType } from "./generated/chord.ts";
import type { ProtoGrpcType as HealthProtoGrpcType } from "./generated/health.ts";

// Single place the protos are loaded (issue #240): utils.ts, UserService.ts,
// and health.ts previously each ran loadSync with hand-duplicated options.
// These options MUST match the flags in scripts/gen-proto-types.js, or the
// generated types in app/generated stop describing the runtime objects.
const LOADER_OPTIONS = {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
} as const;

function load(protoFile: string) {
  return loadSync(
    path.resolve(import.meta.dirname, "../protos", protoFile),
    LOADER_OPTIONS,
  );
}

export const chordPackage = grpc.loadPackageDefinition(
  load("chord.proto"),
) as unknown as ChordProtoGrpcType;

/** The chord.Node service: typed client constructor + service definition. */
export const chordProto = chordPackage.chord;

export const healthPackage = grpc.loadPackageDefinition(
  load("health.proto"),
) as unknown as HealthProtoGrpcType;

/** The grpc.health.v1.Health service (standard health checking protocol). */
export const healthProto = healthPackage.grpc.health.v1;
