import type { Node } from "./utils.ts";

/**
 * Ports for peer communication, owned by the domain layer (issue #233).
 *
 * ChordNode and UserService speak to peers exclusively through these
 * interfaces; GrpcTransport (app/grpcTransport.ts) is the production
 * adapter, and an in-memory implementation can wire a multi-node ring in a
 * single process for deterministic tests.
 */

/**
 * Chord-protocol operations against a remote peer.
 *
 * Error contract: every method throws ChordRoutingError when the peer is
 * unreachable or answers with an unusable node (#236). getPredecessor is the
 * exception on the answer side: "no predecessor yet" is a legitimate answer,
 * so its result may be a null-ish node and callers must guard with
 * isNullNode().
 */
export interface PeerTransport {
  /** Returns the peer's identity ({ id, host, port }). */
  getNodeId(peer: Node): Promise<Node>;
  /** Asks the peer to run findSuccessor(id) on our behalf. */
  findSuccessor(peer: Node, id: number): Promise<Node>;
  getSuccessor(peer: Node): Promise<Node>;
  setSuccessor(peer: Node, successor: Node): Promise<void>;
  getPredecessor(peer: Node): Promise<Node>;
  setPredecessor(peer: Node, predecessor: Node): Promise<void>;
  closestPrecedingFinger(peer: Node, id: number): Promise<Node>;
  notify(peer: Node, self: Node): Promise<void>;
}

/**
 * Application-level (user data) operations against a remote peer.
 *
 * Error contract: methods rethrow the peer's original error object so
 * application status codes survive (the #238 outcome logic distinguishes
 * NOT_FOUND from transport failures, and duplicate-insert surfaces
 * ALREADY_EXISTS).
 */
export interface UserTransport {
  insertUser(peer: Node, userEdit: unknown): Promise<void>;
  removeUser(peer: Node, hashKey: number, userId: number): Promise<void>;
  lookupUser(peer: Node, hashKey: number, userId: number): Promise<unknown>;
  /** Asks the peer to migrate its predecessor-owned keys (to us). */
  requestMigrationToPredecessor(peer: Node): Promise<void>;
  /** Streams a batch of users to the peer for insertion. */
  bulkInsertUsers(peer: Node, users: unknown[]): Promise<void>;
}
