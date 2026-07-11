import path from "path";
import * as grpc from "@grpc/grpc-js";
import { loadSync } from "@grpc/proto-loader";
import { loadTlsCredentials } from "./utils.ts";
import { config } from "./config.ts";

// Server-side implementation of and client factory for the standard gRPC
// Health Checking Protocol (grpc.health.v1.Health). This replaces the bespoke
// `summary` liveness RPC (issue #97) so nodes interoperate with standard
// tooling (grpc_health_probe, k8s probes, etc.).
// Spec: https://github.com/grpc/grpc/blob/master/doc/health-checking.md

const HEALTH_PROTO_PATH = path.resolve(
  import.meta.dirname,
  "../protos/health.proto",
);

const packageDefinition = loadSync(HEALTH_PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});

const healthProto = (grpc.loadPackageDefinition(packageDefinition) as any).grpc
  .health.v1;

// Serving-status names as emitted/accepted by proto-loader (enums: String).
export type ServingStatus =
  "UNKNOWN" | "SERVING" | "NOT_SERVING" | "SERVICE_UNKNOWN";

// The empty service name denotes the health of the whole server, per the spec.
export const OVERALL_HEALTH = "";

// Canonical name in ssl_target_name_override — must match a SAN in
// certs/server.crt. Mirrors TLS_TARGET_NAME in utils.ts (kept local to avoid
// widening that module's export surface for one string).
const TLS_TARGET_NAME = "chord-node";

type HealthRequest = { service?: string };
type HealthResponse = { status: ServingStatus };
type WatchStream = grpc.ServerWritableStream<HealthRequest, HealthResponse>;

/**
 * Tracks a serving status per service name and serves Check + Watch. A node
 * advertises SERVING for the whole server (the "" service) once it is up and
 * flips to NOT_SERVING during graceful shutdown so probes and live Watch
 * streams observe the departure before key migration begins.
 */
export class HealthImplementation {
  private statusMap = new Map<string, ServingStatus>();
  private watchers = new Map<string, Set<WatchStream>>();

  constructor(initial: ServingStatus = "SERVING") {
    this.statusMap.set(OVERALL_HEALTH, initial);
  }

  // gRPC service definition to hand to server.addService().
  get service() {
    return healthProto.Health.service;
  }

  // Handlers keyed by RPC name for server.addService().
  get handlers() {
    return { Check: this.check, Watch: this.watch };
  }

  setStatus(service: string, status: ServingStatus) {
    this.statusMap.set(service, status);
    const subscribers = this.watchers.get(service);
    if (subscribers) for (const call of subscribers) call.write({ status });
  }

  private check: grpc.handleUnaryCall<HealthRequest, HealthResponse> = (
    call,
    callback,
  ) => {
    const service = call.request.service ?? OVERALL_HEALTH;
    const status = this.statusMap.get(service);
    if (status === undefined) {
      callback(
        {
          code: grpc.status.NOT_FOUND,
          details: `unknown service "${service}"`,
        } as grpc.ServerErrorResponse,
        null,
      );
      return;
    }
    callback(null, { status });
  };

  private watch: grpc.handleServerStreamingCall<HealthRequest, HealthResponse> =
    (call: WatchStream) => {
      const service = call.request.service ?? OVERALL_HEALTH;
      // Per spec: send the current status immediately, then on every change.
      call.write({ status: this.statusMap.get(service) ?? "SERVICE_UNKNOWN" });

      let subscribers = this.watchers.get(service);
      if (!subscribers) {
        subscribers = new Set();
        this.watchers.set(service, subscribers);
      }
      subscribers.add(call);
      const unsubscribe = () => subscribers!.delete(call);
      call.on("cancelled", unsubscribe);
      call.on("error", unsubscribe);
    };
}

/**
 * Builds a Health client for one node with a promisified Check. Mirrors
 * connect() in utils.ts: TLS when certs/ca.crt exists, insecure otherwise.
 * Used by the CLI client's `health`/`summary` commands.
 */
export function connectHealth({
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
  const client = new healthProto.Health(
    `${host}:${port}`,
    credentials,
    channelOptions,
  );

  return {
    check: (
      service: string = OVERALL_HEALTH,
    ): Promise<{ status: ServingStatus }> =>
      new Promise((resolve, reject) => {
        // Deadline so a wedged node fails the probe instead of hanging it (#235).
        client.Check(
          { service },
          { deadline: new Date(Date.now() + config.rpcDeadlineMs) },
          (err: unknown, res: any) => {
            if (err) reject(err);
            else resolve(res);
          },
        );
      }),
  };
}
