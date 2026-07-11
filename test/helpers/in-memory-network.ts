import { UserService } from "../../app/UserService.ts";
import { ChordRoutingError, isNullNode, type Node } from "../../app/utils.ts";
import type { PeerTransport, UserTransport } from "../../app/transport.ts";

// In-memory transport (issue #233's payoff, used by #234's tests): wires a
// multi-node Chord ring inside one process with no sockets. Peer calls are
// dispatched straight to the target UserService instance; a node removed
// from the registry behaves like a crashed peer (every call to it throws
// ChordRoutingError, like the gRPC adapter does on UNAVAILABLE).

const noopLogger = new Proxy({}, { get: () => () => {} });

export class InMemoryNetwork {
  nodes = new Map<string, UserService>();

  private key(peer: Node): string {
    return `${peer.host}:${peer.port}`;
  }

  private reach(peer: Node): UserService {
    const target = this.nodes.get(this.key(peer));
    if (!target) {
      throw new ChordRoutingError(
        `peer "${peer.host}:${peer.port}" is unreachable`,
      );
    }
    return target;
  }

  /** Simulates an ungraceful crash: the node vanishes from the network. */
  crash(node: UserService) {
    node.stopMaintenance();
    this.nodes.delete(this.key(node.encapsulateSelf()));
  }

  /** Graceful departure: run the real destructor, then leave the network. */
  async leave(node: UserService) {
    await node.destructor();
    this.nodes.delete(this.key(node.encapsulateSelf()));
  }

  /** Every node stops its loops (test teardown). */
  shutdown() {
    for (const node of this.nodes.values()) node.stopMaintenance();
    this.nodes.clear();
  }

  /**
   * Creates a node with fast maintenance cadences, registered on this
   * network. Explicit ids keep ring geometry deterministic.
   */
  createNode(id: number, port: number): UserService {
    const node = new UserService({
      id,
      host: "mem",
      port,
      transport: this.createTransport(),
    });
    (node as { logger: unknown }).logger = noopLogger;
    node.stabilizeIntervalMs = 10;
    node.fixFingersIntervalMs = 10;
    node.checkPredecessorIntervalMs = 10;
    this.nodes.set(this.key(node.encapsulateSelf()), node);
    return node;
  }

  createTransport(): PeerTransport & UserTransport {
    const net = this;
    // Copy node records at the boundary so two nodes never share a mutable
    // object, mirroring serialization across a real wire.
    const copy = (n: Node): Node => ({ id: n.id, host: n.host, port: n.port });
    return {
      async getNodeId(peer: Node): Promise<Node> {
        return net.reach(peer).encapsulateSelf();
      },
      async findSuccessor(peer: Node, id: number): Promise<Node> {
        const node = await net.reach(peer).findSuccessor(id);
        if (isNullNode(node)) {
          throw new ChordRoutingError(
            `findSuccessor(${id}) via "${peer.host}:${peer.port}" returned an unusable node`,
          );
        }
        return copy(node);
      },
      async getSuccessor(peer: Node): Promise<Node> {
        const successor = net.reach(peer).fingerTable[0].successor;
        if (isNullNode(successor)) {
          throw new ChordRoutingError(
            `getSuccessor of "${peer.host}:${peer.port}" returned an unusable node`,
          );
        }
        return copy(successor);
      },
      async setSuccessor(peer: Node, successor: Node): Promise<void> {
        net.reach(peer).adoptSuccessor(copy(successor));
      },
      async getPredecessor(peer: Node): Promise<Node> {
        return copy(net.reach(peer).predecessor);
      },
      async setPredecessor(peer: Node, predecessor: Node): Promise<void> {
        net.reach(peer).adoptPredecessor(copy(predecessor));
      },
      async closestPrecedingFinger(peer: Node, id: number): Promise<Node> {
        return copy(net.reach(peer).closestPrecedingFinger(id));
      },
      async notify(peer: Node, self: Node): Promise<void> {
        net.reach(peer).notifiedBy(copy(self));
      },
      async insertUser(peer: Node, userEdit: unknown): Promise<void> {
        const err = net.reach(peer).insertUser(userEdit);
        if (err) throw err;
      },
      async removeUser(
        peer: Node,
        hashKey: number,
        userId: number,
      ): Promise<void> {
        const err = net.reach(peer).removeUser(hashKey, userId);
        if (err) throw err;
      },
      async lookupUser(
        peer: Node,
        hashKey: number,
        userId: number,
      ): Promise<unknown> {
        const { err, user } = net.reach(peer).lookupUser(hashKey, userId);
        if (err) throw err;
        return user;
      },
      async requestMigrationToPredecessor(peer: Node): Promise<void> {
        await net.reach(peer).migrateUsersToPredecessor();
      },
      async bulkInsertUsers(peer: Node, users: unknown[]): Promise<void> {
        const target = net.reach(peer);
        for (const user of users) {
          target.insertUser({ user: structuredClone(user), edit: false });
        }
      },
    };
  }
}

export const sleep = (ms: number) =>
  new Promise((resolve) => setTimeout(resolve, ms));

export async function waitFor(
  condition: () => boolean,
  timeoutMs: number,
  message: string,
) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (condition()) return;
    await sleep(10);
  }
  if (!condition()) throw new Error(`waitFor timed out: ${message}`);
}

/**
 * True when the nodes form the correct ring for their sorted ids: each
 * node's successor and predecessor match the sorted circular order.
 */
export function ringIsConverged(nodes: UserService[]): boolean {
  const sorted = [...nodes].sort((a, b) => a.id - b.id);
  return sorted.every((node, i) => {
    const next = sorted[(i + 1) % sorted.length];
    const prev = sorted[(i - 1 + sorted.length) % sorted.length];
    return (
      node.fingerTable[0].successor.id === next.id &&
      node.predecessor.id === prev.id
    );
  });
}

/** The node (by sorted circular order) responsible for a hash. */
export function ringOwner(nodes: UserService[], hash: number): UserService {
  const sorted = [...nodes].sort((a, b) => a.id - b.id);
  return sorted.find((node) => node.id >= hash) ?? sorted[0];
}

/**
 * True when every finger of every node points at the true owner of its
 * start — i.e. fixFingers has fully repaired routing after a membership
 * change. Successor/predecessor convergence (ringIsConverged) happens first;
 * fingers converge within a bounded number of fixFingers sweeps after that.
 */
export function fingersAreConverged(nodes: UserService[]): boolean {
  return nodes.every((node) =>
    node.fingerTable.every(
      (entry) =>
        entry.start !== null &&
        entry.successor.id === ringOwner(nodes, entry.start).id,
    ),
  );
}
