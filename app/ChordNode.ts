import pino from "pino";
import {
  ChordRoutingError,
  isInModuloRange,
  isNullNode,
  computeHostPortHash,
  createLogger,
  HASH_BIT_LENGTH,
  FIBONACCI_ALPHA,
  IS_FIBONACCI_CHORD,
  SUCCESSOR_TABLE_MAX_LENGTH,
  NULL_NODE,
  withTimeout,
  type Node,
} from "./utils.ts";
import { config } from "./config.ts";
import { OVERALL_HEALTH, type HealthImplementation } from "./health.ts";
import type { PeerTransport } from "./transport.ts";
const phi = (1 + Math.sqrt(5)) / 2;

interface FingerTableEntry {
  start: number | null;
  successor: Node;
}

/**
 * The Chord routing state machine, free of transport concerns (issue #233).
 *
 * All peer communication goes through the injected PeerTransport; the gRPC
 * server handlers that expose this node to peers live in the server adapter
 * (app/grpcServer.ts). With an in-memory transport, a multi-node ring can be
 * exercised in a single process with no sockets.
 */
export abstract class ChordNode {
  id: number;
  host: string;
  port: number;
  logger: pino.Logger;
  transport: PeerTransport;
  fingerTable: Array<FingerTableEntry> = [
    {
      start: null,
      successor: NULL_NODE,
    },
  ];
  successorTable: Array<Node> = [NULL_NODE];
  predecessor: Node = NULL_NODE;
  fingerToFix: number = 0;
  // Self-rescheduling maintenance loop timers, keyed by loop name; started in
  // joinCluster() and cancelled in destructor() so they can't race the
  // teardown sequence (see issues #187, #239).
  maintenanceTimers = new Map<string, ReturnType<typeof setTimeout>>();
  maintenanceStopped = false;
  // Standard gRPC health reporter; set in UserService.serve() and flipped to
  // NOT_SERVING in destructor() so probes observe departure (issue #97).
  health?: HealthImplementation;
  // The listening gRPC server; set in UserService.serve() and drained in
  // destructor() so in-flight RPCs complete before exit (issue #243). Typed
  // structurally so the domain class doesn't depend on grpc-js directly.
  server?: {
    tryShutdown(callback: (error?: Error) => void): void;
    forceShutdown(): void;
  };
  // Bound on the drain: an unresponsive in-flight call (e.g. a wedged
  // bulkInsert stream) must not stall shutdown past the entrypoint's own
  // shutdown timeout. Instance field so tests can shorten it.
  drainTimeoutMs: number = config.drainTimeoutMs;
  // Maintenance cadences; instance fields (defaulting to the env-configurable
  // values in app/config.ts, #242) so tests can shorten them per node before
  // joinCluster() starts the timers.
  stabilizeIntervalMs: number = config.stabilizeIntervalMs;
  fixFingersIntervalMs: number = config.fixFingersIntervalMs;
  checkPredecessorIntervalMs: number = config.checkPredecessorIntervalMs;

  constructor({
    id,
    host,
    port,
    transport,
  }: {
    id?: number;
    host?: string;
    port?: number;
    transport: PeerTransport;
  }) {
    if (!host || !port) {
      // Throw rather than exit: only the entrypoint owns process lifecycle
      // (app/node.ts catches construction errors and exits non-zero, #175).
      throw new Error(
        "ChordNode constructor did not receive host or port as expected",
      );
    }
    this.id = id ?? 0;
    this.host = host;
    this.port = port;
    this.logger = createLogger(host, port);
    this.transport = transport;
  }

  iAmTheNode(theNode: Node): boolean {
    return this.id == theNode.id;
  }

  iAmMyOwnSuccessor(): boolean {
    return this.id == this.fingerTable[0].successor.id;
  }

  iAmMyOwnPredecessor(): boolean {
    return this.id == this.predecessor.id;
  }

  encapsulateSelf(): Node {
    return {
      id: this.id,
      host: this.host,
      port: this.port,
    };
  }

  /**
   * Directly implement the pseudocode's findSuccessor() method: run the
   * routing algorithm locally. (Asking a *remote* node to run findSuccessor
   * is transport.findSuccessor(peer, id).)
   *
   * Failures propagate as ChordRoutingError instead of collapsing into the
   * NULL_NODE sentinel that callers had to remember to check (#236).
   */
  async findSuccessor(id: number): Promise<Node> {
    const nPrime = await this.findPredecessor(id);
    this.logger.debug(`findSuccessor(${id}): n' is ${nPrime.id}`);
    const nPrimeSuccessor = await this.getSuccessor(nPrime);
    this.logger.debug(
      `findSuccessor: departing n'.successor = ${nPrimeSuccessor.id}`,
    );
    return nPrimeSuccessor;
  }

  /**
   * This function directly implements the pseudocode's findPredecessor() method.
   *
   * The Chord paper (Stoica et al., SIGCOMM '01, §4.2 and Theorem IV.2) proves
   * that findPredecessor converges in O(log N) hops with high probability, where
   * N <= 2 ** HASH_BIT_LENGTH, so log N is bounded above by HASH_BIT_LENGTH. The
   * while loop therefore needs only a small multiple of HASH_BIT_LENGTH as a
   * safety valve: enough headroom to absorb transient routing inconsistencies
   * during churn, but small enough that a genuinely non-converging loop bails
   * quickly rather than spinning through billions of remote round trips.
   * @param {number} id the key sought
   */
  async findPredecessor(id: number): Promise<Node> {
    this.logger.debug(`findPredecessor: id = ${id}`);

    // Any hop failure propagates as ChordRoutingError (#236).
    let nPrime = this.encapsulateSelf();
    let nPrimeSuccessor = await this.getSuccessor(nPrime);

    this.logger.debug(
      `findPredecessor: before while: nPrime = ${nPrime.id}; nPrimeSuccessor = ${nPrimeSuccessor.id}`,
    );

    // Worst-case hop count is O(log N) <= HASH_BIT_LENGTH; the 4x multiplier is
    // generous headroom for routing inconsistencies during churn. If we ever
    // exhaust this budget, the ring is inconsistent and we stop rather than spin.
    const maxIterations = HASH_BIT_LENGTH * 4;
    let iterationCounter = maxIterations;
    while (
      !isInModuloRange(id, nPrime.id, false, nPrimeSuccessor.id, true) &&
      nPrime.id !== nPrimeSuccessor.id &&
      iterationCounter > 0
    ) {
      // loop should exit if n' and its successor are the same
      // loop should exit if the iterations are ridiculous
      // update loop protection
      iterationCounter--;
      nPrime = await this.closestPrecedingFingerOf(id, nPrime);

      this.logger.debug(
        `findPredecessor: At iterator ${iterationCounter} nPrime = ${nPrime.id}`,
      );

      nPrimeSuccessor = await this.getSuccessor(nPrime);

      this.logger.debug(
        `findPredecessor: nPrimeSuccessor = ${nPrimeSuccessor.id}`,
      );
    }

    if (iterationCounter === 0) {
      this.logger.warn(
        `findPredecessor: exhausted iteration budget of ${maxIterations} hops for id = ${id}; ring is likely inconsistent, returning nPrime = ${nPrime.id}`,
      );
    }

    return nPrime;
  }

  /**
   * Return the successor of a given node: our own successor pointer when the
   * node is us, otherwise the peer's via the transport. This is the single
   * place local-vs-remote dispatch happens for successor queries.
   * @throws ChordRoutingError when the successor cannot be determined (#236)
   */
  async getSuccessor(nodeQueried: Node): Promise<Node> {
    this.logger.debug(`getSuccessor(${nodeQueried.id})`);

    if (this.iAmTheNode(nodeQueried)) {
      const nSuccessor = this.fingerTable[0].successor;
      if (isNullNode(nSuccessor)) {
        // The local successor pointer isn't initialized yet (pre-join).
        throw new ChordRoutingError(
          `getSuccessor of {${nodeQueried.id}} returned an unusable node`,
        );
      }
      return nSuccessor;
    }
    return this.transport.getSuccessor(nodeQueried);
  }

  /**
   * Directly implement the pseudocode's closestPrecedingFinger() method as a
   * pure local scan of this node's finger table.
   *
   * @returns the closest preceding node to ID, or self if no finger precedes it
   */
  closestPrecedingFinger(id: number): Node {
    // skip unusable finger entries rather than routing toward them (#236)
    for (let i = this.fingerTable.length - 1; i >= 0; i--) {
      const finger = this.fingerTable[i].successor;
      if (isNullNode(finger)) continue;
      // The scan interval is the ring interval (n, id). When id == n that is
      // everything except n itself — but isInModuloRange treats (a, a) as
      // empty (cf. notifiedBy), so without the special case a lookup for a
      // key equal to this node's own id would return self forever, spin
      // findPredecessor through its whole iteration budget, and come back
      // with the wrong owner (found by the #234 convergence tests).
      const precedes =
        this.id === id
          ? finger.id !== this.id
          : isInModuloRange(finger.id, this.id, false, id, false);
      if (precedes) {
        return finger;
      }
    }
    return this.encapsulateSelf();
  }

  /** Local-vs-remote dispatch for closestPrecedingFinger during routing. */
  async closestPrecedingFingerOf(id: number, peer: Node): Promise<Node> {
    if (this.iAmTheNode(peer)) {
      return this.closestPrecedingFinger(id);
    }
    return this.transport.closestPrecedingFinger(peer, id);
  }

  /**
   * Replace this node's successor pointer (peer-initiated, e.g. by a
   * departing predecessor handing us its successor). Unusable candidates —
   * including wire-mangled NULL_NODEs — are ignored.
   */
  adoptSuccessor(candidate: Node) {
    this.logger.debug(
      {
        self: this.encapsulateSelf(),
        originalSuccessor: this.fingerTable[0].successor.id,
      },
      "adoptSuccessor",
    );
    if (!isNullNode(candidate)) {
      this.fingerTable[0].successor = candidate;
    }
    this.logger.debug(
      `adoptSuccessor: new successor = ${this.fingerTable[0].successor.id}`,
    );
  }

  /** Replace this node's predecessor pointer (peer-initiated). */
  adoptPredecessor(candidate: Node) {
    this.logger.debug(
      {
        self: this.encapsulateSelf(),
        originalPredecessor: this.predecessor,
        newPredecessor: candidate,
      },
      "adoptPredecessor",
    );
    this.predecessor = candidate;
  }

  /**
   * Directly implements the pseudocode's notify() method: a peer believes it
   * is our predecessor.
   *
   * A predecessor of self means "single-node ring" and counts as having no
   * predecessor: the ring interval (n, n) is everything except n, so any
   * notifier is a better predecessor. This matters since #237 — a node
   * joining the very first node is adopted via this path, where the
   * aggressive join used to force it with setPredecessor.
   */
  notifiedBy(nPrime: Node) {
    if (
      this.predecessor.id == null ||
      this.iAmMyOwnPredecessor() ||
      isInModuloRange(nPrime.id, this.predecessor.id, false, this.id, false)
    ) {
      this.predecessor = nPrime;
    }
  }

  async joinCluster(knownNode: Node) {
    let errorString = null;
    let knownNodeId = null;
    let possibleCollidingNode = NULL_NODE;

    // If host and port are not passed, assume they are identical to the node's host or port
    if (!knownNode.host) knownNode.host = this.host;
    if (!knownNode.port) knownNode.port = this.port;

    // Generate the ID for this node from the host connection strings if not already forced by user
    if (!this.id) {
      this.id = computeHostPortHash(this.host, this.port);
    }

    // initialize finger table with reasonable values
    this.fingerTable.pop();

    const base = IS_FIBONACCI_CHORD ? phi : 2;
    const numberOfEntries = Math.round(HASH_BIT_LENGTH / Math.log2(base));
    // Pruning: we prune starting from the first entries, up to fibonacciAlpha entries
    for (let i = 0; i < numberOfEntries; i++) {
      // We only prune 1 - alpha percentage of the entries, and only odd ones
      if (
        IS_FIBONACCI_CHORD &&
        i < (1 - FIBONACCI_ALPHA) * numberOfEntries * 2 &&
        i % 2 == 1
      )
        continue;
      this.fingerTable.push({
        start: (this.id + Math.round(base ** i)) % 2 ** HASH_BIT_LENGTH,
        successor: this.encapsulateSelf(),
      });
    }

    // join a chord or create a new one
    if (
      `${this.host}:${this.port}`.toLowerCase() ===
      `${knownNode.host}:${knownNode.port}`.toLowerCase()
    ) {
      // this is the first node in a new cluster
      this.predecessor = this.encapsulateSelf();
      knownNode.id = this.id;
    } else if (await this.confirmExist(knownNode)) {
      // joining an existing chord so
      // + get the known node's ID
      knownNodeId = await this.getNodeId(knownNode);
      knownNode.id = knownNodeId;
      // Ask the cluster for the successor of this node's ID. This doubles as
      // the collision check (an existing node already owns the ID) and as the
      // lazy join itself: successor(self.id) is exactly the node to sit in
      // front of.
      try {
        possibleCollidingNode = await this.transport.findSuccessor(
          knownNode,
          this.id,
        );
      } catch (err) {
        errorString = `Error joining node "${this.host}:${this.port}" to node "${knownNode.host}:${knownNode.port}" because the cluster could not resolve a successor for ID {${this.id}}.`;
        this.logger.error({ err }, errorString);
        throw new RangeError(errorString);
      }
      if (this.iAmTheNode(possibleCollidingNode)) {
        // node collision
        errorString = `Error joining node "${this.host}:${this.port}" with ID {${this.id}} to node "${knownNode.host}:${knownNode.port}" because of a collision with node "${possibleCollidingNode.host}:${possibleCollidingNode.port}" having ID={${possibleCollidingNode.id}}.`;
        this.logger.error(errorString);
        throw new RangeError(errorString);
      }
      // Stabilization-based (lazy) join, per the TR revision of the Chord
      // paper (§E.1): adopt the successor, leave the predecessor unset, and
      // let stabilize/notify repair the ring while fixFingers converges the
      // finger table. The aggressive initFingerTable/updateOthers join was
      // removed in #237 — it had remote nodes mutating each other's finger
      // tables mid-join with no coordination (see #168, #224, #156). Until
      // fixFingers completes its first sweep, lookups from this node take
      // more hops (successor-walking); that is the standard tradeoff.
      this.predecessor = NULL_NODE;
      for (const entry of this.fingerTable) {
        entry.successor = possibleCollidingNode;
      }
      // Run one stabilization pass right away: its notify() tells the
      // successor to adopt us as predecessor, which must happen before
      // migrateKeysAfterJoining() — the successor scopes the key migration
      // by its predecessor pointer.
      await this.stabilize();
    } else {
      // the node doesn't exist so exit on error
      errorString = `Error joining node "${this.host}:${this.port}" to node "${knownNode.host}:${knownNode.port}" because the latter can't be confirmed to exist.`;
      this.logger.error(errorString);
      throw new RangeError(errorString);
    }

    // initialize successor table
    this.successorTable[0] = this.fingerTable[0].successor;

    try {
      this.logger.debug("join: calling migrateKeys");
      await this.migrateKeysAfterJoining();
    } catch (error) {
      this.logger.error({ err: error }, "Migrate keys failed");
    }

    // And now that we've joined a cluster, we need to maintain our state.
    // Each loop self-reschedules after its pass completes, so overlapping
    // passes are impossible by construction (issue #239).
    this.startMaintenanceLoop(
      "stabilize",
      () => this.stabilize(),
      this.stabilizeIntervalMs,
    );
    this.startMaintenanceLoop(
      "fixFingers",
      () => this.fixFingers(),
      this.fixFingersIntervalMs,
    );
    this.startMaintenanceLoop(
      "checkPredecessor",
      () => this.checkPredecessor(),
      this.checkPredecessorIntervalMs,
    );

    this.logger.debug(
      {
        fingerTable: this.fingerTable,
        predecessor: this.predecessor.id,
      },
      `joinCluster: {${this.id}}.joinCluster(${knownNode.id}) complete`,
    );
  }

  /**
   * Runs `work` and re-schedules it `intervalMs` after the pass settles.
   * Because the next pass isn't scheduled until the current one finishes —
   * even when it throws — overlapping passes are impossible by construction.
   * This replaces the manually-cleared boolean locks, where one uncaught
   * throw between set and clear silently disabled the loop forever, and also
   * prevents setInterval pile-up when RPCs run slower than the cadence
   * (issue #239).
   */
  startMaintenanceLoop(
    name: string,
    work: () => Promise<unknown>,
    intervalMs: number,
  ) {
    const run = async () => {
      try {
        await work();
      } catch (err) {
        this.logger.error({ err }, `${name}: maintenance pass failed`);
      } finally {
        if (!this.maintenanceStopped) {
          this.maintenanceTimers.set(name, setTimeout(run, intervalMs));
        }
      }
    };
    this.maintenanceTimers.set(name, setTimeout(run, intervalMs));
  }

  /**
   * Cancels the maintenance loops and prevents in-flight passes from
   * re-scheduling themselves.
   */
  stopMaintenance() {
    this.maintenanceStopped = true;
    for (const timer of this.maintenanceTimers.values()) clearTimeout(timer);
    this.maintenanceTimers.clear();
  }

  /**
   * Determine whether a node exists by asking for its ID.
   * @returns - true if the node has a valid ID
   */
  async confirmExist(knownNode: Node): Promise<boolean> {
    const nodeId = await this.getNodeId(knownNode);
    return nodeId !== null && nodeId >= 0;
  }

  /**
   * Returns a node's ID, using the transport when the node isn't us.
   * @returns - node's ID, or null if it couldn't be determined
   */
  async getNodeId(knownNode: Node): Promise<number | null> {
    const selfNodeString = (this.host + ":" + this.port).toLowerCase();
    const knownNodeString = (
      knownNode.host +
      ":" +
      knownNode.port
    ).toLowerCase();
    if (selfNodeString === knownNodeString) {
      // use local value
      return this.id;
    }
    try {
      const knownNodeObject = await this.transport.getNodeId(knownNode);
      return knownNodeObject.id;
    } catch (err) {
      this.logger.debug(
        { err },
        `Error getting ID of node "${knownNode.host}:${knownNode.port}"`,
      );
      return null;
    }
  }

  /**
   * Update fault-tolerance structure discussed in E.3 'Failure and Replication' of IEEE paper.
   *
   * "Node reconciles its list with its successor by:
   *      [1-] copying successor's successor list,
   *      [2-] removing its last entry,
   *      [3-] and prepending to it.
   * If node notices that its successor has failed,
   *      [1-] it replaces it with the first live entry in its successor list
   *      [2-] and reconciles its successor list with its new successor."
   *
   * @returns true if it was successful; false otherwise.
   *
   */
  async updateSuccessorTable(): Promise<boolean> {
    this.logger.debug(
      {
        successorTable: this.successorTable,
        successorId: this.fingerTable[0].successor.id,
      },
      `updateSuccessorTable: {${this.id}}`,
    );

    // check whether the successor is available
    let successorSeemsOK = false;
    try {
      successorSeemsOK = await this.isOkSuccessor();
    } catch (err) {
      successorSeemsOK = false;
      this.logger.error({ err }, `updateSuccessorTable: isOkSuccessor failed`);
    }
    if (successorSeemsOK) {
      // synchronize with finger table because its successor still seemed OK
      this.successorTable[0] = this.fingerTable[0].successor;
    } else {
      // or prune because the successor seemed not OK
      while (!successorSeemsOK && this.successorTable.length > 0) {
        // try current successor again to account for contention or bad luck
        try {
          successorSeemsOK = await this.isOkSuccessor();
        } catch (err) {
          successorSeemsOK = false;
          this.logger.error(
            { err },
            `updateSuccessorTable: isOkSuccessor failed`,
          );
        }
        if (successorSeemsOK) {
          // synchronize with finger table because its successor still seemed OK
          this.successorTable[0] = this.fingerTable[0].successor;
        } else {
          // drop the first successor candidate
          this.successorTable.shift();
          // update the finger table accordingly
          this.fingerTable[0].successor = this.successorTable[0];
        }
      }
    }
    // deal with an isolated node
    if (this.successorTable.length < 1) {
      this.successorTable.push({
        id: this.id,
        host: this.host,
        port: this.port,
      });
      // update the finger table accordingly
      this.fingerTable[0].successor = this.successorTable[0];
    }
    // try to bulk up the table
    let successorSuccessor = NULL_NODE;
    if (
      this.successorTable.length < SUCCESSOR_TABLE_MAX_LENGTH &&
      this.id !== this.fingerTable[0].successor.id
    ) {
      this.logger.debug(
        `updateSuccessorTable: Short successorTable[]: [ current length ${this.successorTable.length} ] < [ ${SUCCESSOR_TABLE_MAX_LENGTH} preferred length ]`,
      );
      // Bound on the table length, not on `i`: each iteration appends at most
      // one entry, so stopping when the table reaches its max keeps it from
      // overshooting and issuing wasted getSuccessor() RPCs. (A simple
      // `i < MAX` still overshoots to MAX+1, since the table grows as i does —
      // see #167.) The `i < length` term guards the table[i] read.
      for (
        let i = 0;
        i < this.successorTable.length &&
        this.successorTable.length < SUCCESSOR_TABLE_MAX_LENGTH;
        i++
      ) {
        try {
          successorSuccessor = await this.getSuccessor(this.successorTable[i]);
        } catch (err) {
          this.logger.error(
            { err },
            `updateSuccessorTable: getSuccessor failed`,
          );
          successorSuccessor = NULL_NODE;
        }
        this.logger.debug(
          `updateSuccessorTable: {${this.id}}.successorTable[${i}] = ${this.successorTable[i].id} and {${this.successorTable[i].id}}.successor[0] = ${successorSuccessor.id}`,
        );

        if (
          !isNullNode(successorSuccessor) &&
          !isInModuloRange(
            successorSuccessor.id,
            this.id,
            true,
            this.successorTable[i].id,
            true,
          )
        ) {
          // append the additional value
          this.successorTable.splice(i + 1, 1, successorSuccessor);
          successorSeemsOK = true;
        }
      }
    }
    // prune from the bottom
    let i = this.successorTable.length - 1;
    successorSeemsOK = false;
    while (
      (!successorSeemsOK ||
        this.successorTable.length > SUCCESSOR_TABLE_MAX_LENGTH) &&
      i > 0
    ) {
      try {
        successorSeemsOK = await this.confirmExist(this.successorTable[i]);
      } catch (err) {
        this.logger.error(
          { err },
          `updateSuccessorTable: call to confirmExist failed`,
        );
        successorSeemsOK = false;
      }
      if (!successorSeemsOK || i >= SUCCESSOR_TABLE_MAX_LENGTH) {
        // remove successor candidate
        this.successorTable.pop();
      }
      i -= 1;
    }

    this.logger.debug(
      { successorTable: this.successorTable },
      `updateSuccessorTable: new {${this.id}}.successorTable[]`,
    );

    return successorSeemsOK;
  }

  /**
   * Modified implementation of pseudocode's stabilize() method
   *   as described in Figure 7 of the SIGCOMM paper.
   * Modifications consist:
   *  1- additional logic to stabilize a node whose predecessor is itself
   *      as would be the case for the initial node in a chord.
   *  2- additional step of updating the successor table as recommended by the IEEE paper.
   */
  async stabilize() {
    let x: Node;
    if (this.iAmMyOwnSuccessor()) {
      // use local value
      await this.stabilizeSelf();
      x = this.encapsulateSelf();
    } else {
      // ask the successor for its predecessor; on failure fall back to self
      // ("no better candidate") — the transport already logged the failure
      try {
        x = await this.transport.getPredecessor(this.fingerTable[0].successor);
      } catch (err) {
        x = this.encapsulateSelf();
      }
    }

    // A remote with no predecessor answers getPredecessor with a NULL_NODE,
    // which arrives wire-mangled as { id: 0, host: "", port: 0 } — and 0 is a
    // valid ring position, so guard with isNullNode before adopting (#236).
    if (
      !isNullNode(x) &&
      isInModuloRange(
        x.id,
        this.id,
        false,
        this.fingerTable[0].successor.id,
        false,
      )
    ) {
      this.fingerTable[0].successor = x;
    }

    this.logger.debug(
      {
        predecessor: this.predecessor.id,
        fingerTable: this.fingerTable,
        successorTable: this.successorTable,
      },
      `stabilize: leaving stabilize()`,
    );

    if (!this.iAmMyOwnSuccessor()) {
      try {
        await this.transport.notify(
          this.fingerTable[0].successor,
          this.encapsulateSelf(),
        );
      } catch (err) {
        this.logger.debug({ err }, "stabilize: notify failed");
      }
    }

    // update successor table - deviates from SIGCOMM
    try {
      await this.updateSuccessorTable();
    } catch (err) {
      this.logger.error({ err }, `stabilize: updateSuccessorTable failed`);
    }

    this.logger.debug(
      `stabilize: {${this.id}}.predecessor = ${this.predecessor.id}; successor = ${this.fingerTable[0].successor.id}`,
    );
    return true;
  }

  /**
   * Attempts to kick a node with a successor of self, as would be the case in the first node in a chord.
   * The kick comes from setting the successor to be equal to the predecessor.
   *
   * This is an original function, not described in either version of the paper - added 20191021.
   * @returns true if it was a good kick; false if bad kick.
   */
  async stabilizeSelf(): Promise<boolean> {
    let predecessorSeemsOK = false;
    if (this.predecessor.id == null) {
      // this node is in real trouble since its predecessor is no good either
      predecessorSeemsOK = false;
      return predecessorSeemsOK;
    }
    if (!this.iAmMyOwnPredecessor()) {
      try {
        // confirm that the predecessor is actually there
        predecessorSeemsOK = await this.checkPredecessor();
      } catch (err) {
        predecessorSeemsOK = false;
        this.logger.error({ err }, `stabilizeSelf: checkPredecessor failed`);
      }
      if (predecessorSeemsOK) {
        // then kick by setting the successor to the same as the predecessor
        this.fingerTable[0].successor = this.predecessor;
        this.successorTable[0] = this.fingerTable[0].successor;
      }
    } else {
      this.logger.debug(
        `stabilizeSelf: Warning: {${this.id}} is isolated because predecessor is ${this.predecessor.id} and successor is ${this.fingerTable[0].successor.id}.`,
      );
      predecessorSeemsOK = true;
    }
    return predecessorSeemsOK;
  }

  /**
   * Directly implements the pseudocode's fixFingers() method.
   */
  async fixFingers() {
    try {
      // findSuccessor now throws on failure instead of returning a sentinel,
      // so a successful return is always safe to install (#236).
      this.fingerTable[this.fingerToFix].successor = await this.findSuccessor(
        this.fingerTable[this.fingerToFix].start!,
      );
    } catch (err) {
      this.logger.error({ err }, `fixFingers: findSuccessor failed`);
    }
    this.logger.debug(
      `fixFingers: Fix {${this.id}}.fingerTable[${this.fingerToFix}], with start = ${this.fingerTable[this.fingerToFix].start}; successor = ${this.fingerTable[this.fingerToFix].successor?.id}`,
    );
    if (this.fingerToFix < this.fingerTable.length - 1) {
      this.fingerToFix++;
    } else {
      this.fingerToFix = 0;
    }
  }

  /**
   * Checks to make sure that the predecessor is still responsive
   */
  async checkPredecessor(): Promise<boolean> {
    if (this.predecessor.id !== null && !this.iAmMyOwnPredecessor()) {
      try {
        // just ping it — any answer (even "no predecessor") proves liveness
        await this.transport.getPredecessor(this.predecessor);
      } catch (err) {
        this.logger.warn(
          { err },
          `checkPredecessor: predecessor {${this.predecessor.id}} did not respond; clearing it`,
        );
        // Wipe out the predecessor if it doesn't respond
        this.predecessor = NULL_NODE;
        return false;
      }
    }
    return true;
  }

  /**
   * Checks whether the successor is still responding.
   */
  async isOkSuccessor() {
    this.logger.debug(
      `{${this.id}}.isOkSuccessor(${this.fingerTable[0].successor.id})`,
    );

    let successorSeemsOK = false;
    if (this.fingerTable[0].successor.id == null) {
      successorSeemsOK = false;
    } else if (this.iAmMyOwnSuccessor()) {
      successorSeemsOK = true;
    } else {
      try {
        // just ask anything
        successorSeemsOK = await this.confirmExist(
          this.fingerTable[0].successor,
        );
      } catch (err) {
        successorSeemsOK = false;
        this.logger.error(
          { err },
          `isOkSuccessor({${this.id}}): call to confirmExist({${this.fingerTable[0].successor.id}}) failed`,
        );
      }
    }
    return successorSeemsOK;
  }

  /**
   * Remove node from the chord gracefully by migrating keys to the remaining nodes.
   */
  async destructor() {
    // Advertise NOT_SERVING up front so health probes and live Watch streams
    // see the node leaving before key migration / peer notification begins
    // (issue #97).
    this.health?.setStatus(OVERALL_HEALTH, "NOT_SERVING");
    // Stop periodic maintenance before tearing down so stabilize/fixFingers/
    // checkPredecessor can't race the migration: mutate successor/predecessor
    // mid-flight or fire gRPC calls at departing nodes (issue #187).
    this.stopMaintenance();

    let migrationSeemsOK = false;
    let successor = NULL_NODE;
    let successorSeemsOK = false;
    // pick successor from successor table
    for (let i = 0; !successorSeemsOK && i < this.successorTable.length; i++) {
      if (this.successorTable[i].id == null) {
        successorSeemsOK = false;
        successor = NULL_NODE;
      } else if (this.iAmTheNode(this.successorTable[i])) {
        successorSeemsOK = false;
        successor = NULL_NODE;
      } else {
        try {
          successorSeemsOK = await this.confirmExist(this.successorTable[i]);
        } catch (err) {
          successorSeemsOK = false;
          this.logger.error(
            { err },
            `destructor({${this.id}}): call to confirmExist({${this.successorTable[i].id}}) failed`,
          );
        }
        successor = this.successorTable[i];
      }
    }
    // alternatively pick successor from finger table
    for (let i = 0; !successorSeemsOK && i < this.fingerTable.length; i++) {
      if (this.fingerTable[i].successor.id == null) {
        successorSeemsOK = false;
        successor = NULL_NODE;
      } else if (this.iAmMyOwnSuccessor()) {
        successorSeemsOK = false;
        successor = NULL_NODE;
      } else {
        try {
          successorSeemsOK = await this.confirmExist(
            this.fingerTable[i].successor,
          );
        } catch (err) {
          successorSeemsOK = false;
          this.logger.error(
            { err },
            `destructor({${this.id}}): call to confirmExist({${this.fingerTable[i].successor.id}}) failed`,
          );
        }
        successor = this.fingerTable[i].successor;
      }
    }
    // as a last resort, pick the predecessor
    if (!successorSeemsOK && !this.iAmMyOwnPredecessor()) {
      try {
        successorSeemsOK = await this.confirmExist(this.predecessor);
      } catch (err) {
        successorSeemsOK = false;
        this.logger.error(
          { err },
          `destructor({${this.id}}): call to confirmExist({${this.predecessor?.id}}) failed`,
        );
      }
      successor = this.predecessor;
    }
    // migrate keys
    let migrationError = null;
    if (successorSeemsOK) {
      try {
        migrationSeemsOK = await this.migrateKeysBeforeDeparture();
      } catch (err) {
        migrationSeemsOK = false;
        migrationError = err;
        this.logger.error(
          { err },
          "destructor: migrateKeysBeforeDeparture failed",
        );
      }
    }
    // notify predecessor
    if (successorSeemsOK) {
      try {
        await this.transport.setSuccessor(this.predecessor, successor);
      } catch (err) {
        this.logger.warn(
          { err },
          `destructor: could not hand successor {${successor.id}} to predecessor {${this.predecessor.id}}`,
        );
      }
    }
    // notify successor
    if (successorSeemsOK) {
      try {
        await this.transport.setPredecessor(successor, this.predecessor);
      } catch (err) {
        this.logger.warn(
          { err },
          `destructor: could not hand predecessor {${this.predecessor.id}} to successor {${successor.id}}`,
        );
      }
    }
    // Drain the gRPC server: stop accepting new calls and wait for in-flight
    // ones to complete (issue #243). This runs after key migration and peer
    // notification because those are outbound calls, not inbound; the drain
    // only affects calls we serve. Bounded so a wedged in-flight call can't
    // stall shutdown, with forceShutdown as the fallback.
    if (this.server) {
      const server = this.server;
      try {
        await withTimeout(
          new Promise<void>((resolve, reject) =>
            server.tryShutdown((err) => (err ? reject(err) : resolve())),
          ),
          this.drainTimeoutMs,
          `gRPC server drain timed out after ${this.drainTimeoutMs}ms`,
        );
        this.logger.info("destructor: gRPC server drained");
      } catch (err) {
        this.logger.warn(
          { err },
          "destructor: graceful drain failed; forcing shutdown",
        );
        server.forceShutdown();
      }
    }

    // report what's up; the caller (entrypoint) owns process exit, so that
    // its shutdown timeout can actually observe the success path (#241)
    this.logger.info(
      `Node {${this.id}} at "${this.host}:${this.port}" is exiting the chord.`,
    );
    if (successorSeemsOK && migrationSeemsOK) {
      this.logger.info(`Keys are migrating to node {${successor.id}}.`);
    } else if (!successorSeemsOK) {
      this.logger.warn(
        `Keys are not migrating because a successor couldn't be contacted.`,
      );
    } else if (!migrationSeemsOK) {
      this.logger.error(
        { err: migrationError },
        `Keys are not migrating because the migration failed.`,
      );
    }
  }

  abstract migrateKeysAfterJoining(): Promise<void>;
  abstract migrateKeysBeforeDeparture(): Promise<boolean>;
}
