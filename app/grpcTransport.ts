import process from "process";
import pino from "pino";
import {
  ChordRoutingError,
  connect,
  handleGRPCErrors,
  isNullNode,
  type Node,
} from "./utils.ts";
import type { PeerTransport, UserTransport } from "./transport.ts";

/**
 * gRPC adapter for the domain-owned transport ports (issue #233). Owns
 * everything wire-specific for outbound calls: the channel cache lives in
 * connect() (utils.ts), per-call deadlines are attached by the promisified
 * client (#235), and gRPC status codes are logged/mapped here so the domain
 * layer never sees them.
 *
 * Routing methods wrap failures in ChordRoutingError (#236); user-data
 * methods rethrow the peer's original error so application status codes
 * (NOT_FOUND, ALREADY_EXISTS) survive for the #238 outcome logic.
 */
export class GrpcTransport implements PeerTransport, UserTransport {
  logger: pino.Logger;

  constructor(logger?: pino.Logger) {
    this.logger = logger ?? pino({ level: process.env.LOG_LEVEL ?? "info" });
  }

  /** Logs the gRPC failure and converts it into a ChordRoutingError. */
  private routingFailure(call: string, peer: Node, err: unknown): never {
    handleGRPCErrors(
      this.logger,
      "GrpcTransport",
      call,
      peer.host,
      peer.port,
      err,
    );
    throw new ChordRoutingError(
      `${call} on "${peer.host}:${peer.port}" failed`,
      { cause: err },
    );
  }

  private assertUsable(call: string, peer: Node, node: Node): Node {
    if (isNullNode(node)) {
      throw new ChordRoutingError(
        `${call} on "${peer.host}:${peer.port}" returned an unusable node`,
      );
    }
    return node;
  }

  // --- PeerTransport ---

  async getNodeId(peer: Node): Promise<Node> {
    try {
      const node = await connect(peer).getNodeIdRemoteHelper(peer);
      return this.assertUsable("getNodeId", peer, node);
    } catch (err) {
      if (err instanceof ChordRoutingError) throw err;
      this.routingFailure("getNodeIdRemoteHelper", peer, err);
    }
  }

  async findSuccessor(peer: Node, id: number): Promise<Node> {
    try {
      const node = await connect(peer).findSuccessorRemoteHelper({
        id,
        node: peer,
      });
      return this.assertUsable("findSuccessor", peer, node);
    } catch (err) {
      if (err instanceof ChordRoutingError) throw err;
      this.routingFailure("findSuccessorRemoteHelper", peer, err);
    }
  }

  async getSuccessor(peer: Node): Promise<Node> {
    try {
      const node = await connect(peer).getSuccessorRemoteHelper();
      return this.assertUsable("getSuccessor", peer, node);
    } catch (err) {
      if (err instanceof ChordRoutingError) throw err;
      this.routingFailure("getSuccessorRemoteHelper", peer, err);
    }
  }

  async setSuccessor(peer: Node, successor: Node): Promise<void> {
    try {
      await connect(peer).setSuccessor(successor);
    } catch (err) {
      this.routingFailure("setSuccessor", peer, err);
    }
  }

  async getPredecessor(peer: Node): Promise<Node> {
    try {
      // No usability check: "no predecessor yet" legitimately comes back as
      // a null-ish node (wire-mangled NULL_NODE); callers guard with
      // isNullNode().
      return await connect(peer).getPredecessor();
    } catch (err) {
      this.routingFailure("getPredecessor", peer, err);
    }
  }

  async setPredecessor(peer: Node, predecessor: Node): Promise<void> {
    try {
      await connect(peer).setPredecessor(predecessor);
    } catch (err) {
      this.routingFailure("setPredecessor", peer, err);
    }
  }

  async closestPrecedingFinger(peer: Node, id: number): Promise<Node> {
    try {
      const node = await connect(peer).closestPrecedingFingerRemoteHelper({
        id,
        node: peer,
      });
      return this.assertUsable("closestPrecedingFinger", peer, node);
    } catch (err) {
      if (err instanceof ChordRoutingError) throw err;
      this.routingFailure("closestPrecedingFingerRemoteHelper", peer, err);
    }
  }

  async notify(peer: Node, self: Node): Promise<void> {
    try {
      await connect(peer).notify(self);
    } catch (err) {
      this.routingFailure("notify", peer, err);
    }
  }

  // --- UserTransport ---
  // These rethrow the original error object: the caller's outcome logic
  // (#238) reads .code (NOT_FOUND vs transport failure), so wrapping would
  // destroy information. Logging still happens here.

  async insertUser(peer: Node, userEdit: unknown): Promise<void> {
    try {
      await connect(peer).insertUserRemoteHelper(userEdit as any);
    } catch (err) {
      handleGRPCErrors(
        this.logger,
        "GrpcTransport",
        "insertUserRemoteHelper",
        peer.host,
        peer.port,
        err,
      );
      throw err;
    }
  }

  async removeUser(peer: Node, hashKey: number, userId: number): Promise<void> {
    try {
      await connect(peer).removeUserRemoteHelper({ id: hashKey, userId });
    } catch (err) {
      handleGRPCErrors(
        this.logger,
        "GrpcTransport",
        "removeUserRemoteHelper",
        peer.host,
        peer.port,
        err,
      );
      throw err;
    }
  }

  async lookupUser(
    peer: Node,
    hashKey: number,
    userId: number,
  ): Promise<unknown> {
    try {
      return await connect(peer).lookupUserRemoteHelper({
        id: hashKey,
        userId,
      });
    } catch (err) {
      handleGRPCErrors(
        this.logger,
        "GrpcTransport",
        "lookupUserRemoteHelper",
        peer.host,
        peer.port,
        err,
      );
      throw err;
    }
  }

  async requestMigrationToPredecessor(peer: Node): Promise<void> {
    try {
      await connect(peer).migrateUsersToPredecessorRemoteHelper();
    } catch (err) {
      handleGRPCErrors(
        this.logger,
        "GrpcTransport",
        "migrateUsersToPredecessorRemoteHelper",
        peer.host,
        peer.port,
        err,
      );
      throw err;
    }
  }

  async bulkInsertUsers(peer: Node, users: unknown[]): Promise<void> {
    const client = connect(peer);
    await new Promise<void>((resolve, reject) => {
      const call = client.bulkInsertUsersRemoteHelper((err: unknown) => {
        if (err) reject(err);
        else resolve();
      });
      for (const user of users) {
        call.write(user as any);
      }
      call.end();
    });
  }
}
