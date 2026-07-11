import * as grpc from "@grpc/grpc-js";
import { NULL_NODE, type Node } from "./utils.ts";
import type { NodeAddress__Output } from "./generated/chord/NodeAddress.ts";
import type { RemoteId__Output } from "./generated/chord/RemoteId.ts";
import type { UserId__Output } from "./generated/chord/UserId.ts";
import type { UserService } from "./UserService.ts";

/**
 * Server-side gRPC adapter (issue #233): translates incoming gRPC calls into
 * calls on the domain object. The domain classes (ChordNode / UserService)
 * carry no gRPC handler signatures; everything wire-shaped for inbound
 * traffic lives here.
 *
 * Error convention: routing handlers fail the RPC with INTERNAL rather than
 * answering with a NULL_NODE sentinel, which proto3 would mangle into
 * { 0, "", 0 } on the wire (#236). Application handlers pass through the
 * domain's error objects, whose shapes are already gRPC status-compatible.
 */

// Effectively `unknown | null`: domain outcomes are gRPC-status-shaped, but
// transport failures rethrow the peer's original error object as-is.
type ErrLike = { code: number; details?: string } | Error | unknown | null;
type UnaryCallback<Res> = (err: ErrLike, value: Res | null) => void;

export function buildNodeServiceHandlers(
  service: UserService,
): grpc.UntypedServiceImplementation {
  return {
    // ---- Chord protocol ----

    getNodeIdRemoteHelper: (
      _call: unknown,
      callback: UnaryCallback<Node>,
    ): void => {
      callback(null, service.encapsulateSelf());
    },

    findSuccessorRemoteHelper: async (
      call: { request: RemoteId__Output },
      callback: UnaryCallback<Node>,
    ): Promise<void> => {
      const id = call.request.id;
      try {
        callback(null, await service.findSuccessor(id));
      } catch (err) {
        service.logger.error(
          { err },
          "findSuccessorRemoteHelper: findSuccessor failed",
        );
        callback(
          {
            code: grpc.status.INTERNAL,
            details: `findSuccessor(${id}) failed on ${service.host}:${service.port}`,
          },
          NULL_NODE,
        );
      }
    },

    getSuccessorRemoteHelper: (
      _call: unknown,
      callback: UnaryCallback<Node>,
    ): void => {
      callback(null, service.fingerTable[0].successor);
    },

    setSuccessor: (
      call: { request: NodeAddress__Output },
      callback: UnaryCallback<{}>,
    ): void => {
      service.adoptSuccessor(call.request);
      callback(null, {});
    },

    getPredecessor: (_call: unknown, callback: UnaryCallback<Node>): void => {
      callback(null, service.predecessor);
    },

    setPredecessor: (
      call: { request: NodeAddress__Output },
      callback: UnaryCallback<{}>,
    ): void => {
      service.adoptPredecessor(call.request);
      callback(null, {});
    },

    closestPrecedingFingerRemoteHelper: (
      call: { request: RemoteId__Output },
      callback: UnaryCallback<Node>,
    ): void => {
      callback(null, service.closestPrecedingFinger(call.request.id));
    },

    notify: (
      call: { request: NodeAddress__Output },
      callback: UnaryCallback<{}>,
    ): void => {
      service.notifiedBy(call.request);
      callback(null, {});
    },

    getFingerTableEntries: (call: {
      write: (chunk: { index: number; node: Node }) => void;
      end: () => void;
    }): void => {
      for (const entry of service.fingerTable) {
        call.write({ index: entry.start!, node: entry.successor });
      }
      call.end();
    },

    // ---- Application level (user data) ----

    fetch: (
      call: { request: UserId__Output },
      callback: UnaryCallback<unknown>,
    ): void => {
      const { id, userId } = call.request;
      service.logger.info(`fetch: Requested User ${userId} at hash ${id}`);
      const { err, user } = service.lookupUser(id, userId);
      callback(err, user);
    },

    insert: async (
      call: { request: any },
      callback: UnaryCallback<{}>,
    ): Promise<void> => {
      callback(await service.insertReplicated(call.request), {});
    },

    insertUserRemoteHelper: (
      call: { request: any },
      callback: UnaryCallback<{}>,
    ): void => {
      service.logger.debug({ request: call.request }, "insertUserRemoteHelper");
      callback(service.insertUser(call.request), {});
    },

    lookup: async (
      call: { request: UserId__Output },
      callback: UnaryCallback<unknown>,
    ): Promise<void> => {
      const { err, user } = await service.lookupReplicated(call.request.id);
      callback(err, user);
    },

    lookupUserRemoteHelper: (
      call: { request: UserId__Output },
      callback: UnaryCallback<unknown>,
    ): void => {
      service.logger.debug(
        { id: call.request.id },
        "beginning lookupUserRemoteHelper",
      );
      const { err, user } = service.lookupUser(
        call.request.id,
        call.request.userId,
      );
      service.logger.debug({ user }, "finishing lookupUserRemoteHelper");
      callback(err, user);
    },

    remove: async (
      call: { request: UserId__Output },
      callback: UnaryCallback<{}>,
    ): Promise<void> => {
      callback(await service.removeReplicated(call.request.id), {});
    },

    removeUserRemoteHelper: (
      call: { request: UserId__Output },
      callback: UnaryCallback<{}>,
    ): void => {
      service.logger.debug({ request: call.request }, "removeUserRemoteHelper");
      callback(service.removeUser(call.request.id, call.request.userId), {});
    },

    migrateUsersToPredecessorRemoteHelper: async (
      _call: unknown,
      callback: UnaryCallback<{}>,
    ): Promise<void> => {
      try {
        await service.migrateUsersToPredecessor();
        callback(null, {});
      } catch (error) {
        callback(error as ErrLike, {});
      }
    },

    // Client-streaming: receives a stream of User messages, inserts each
    bulkInsertUsersRemoteHelper: (
      call: {
        on: {
          (event: "data", listener: (user: any) => void): void;
          (event: "end", listener: () => void): void;
          (event: "error", listener: (err: unknown) => void): void;
        };
      },
      callback: UnaryCallback<{}>,
    ): void => {
      call.on("data", (user: any) => {
        const err = service.insertUser({ user, edit: false });
        if (err) {
          service.logger.warn(
            { err, userId: user.id },
            "bulkInsertUsersRemoteHelper: insertUser failed",
          );
        }
      });
      call.on("end", () => callback(null, {}));
      call.on("error", (err: unknown) => callback(err as ErrLike, {}));
    },

    getUserIds: (call: {
      write: (chunk: { id: number; metadata: unknown }) => void;
      end: () => void;
    }): void => {
      for (const user of Object.values(service.userMap).flat()) {
        call.write({ id: user.id, metadata: user.metadata });
      }
      call.end();
    },
  } as unknown as grpc.UntypedServiceImplementation;
}
