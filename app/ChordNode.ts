import process from "process";
import pino from "pino";
import {
  connect,
  isInModuloRange,
  computeHostPortHash,
  handleGRPCErrors,
  createLogger,
  HASH_BIT_LENGTH,
  FIBONACCI_ALPHA,
  IS_FIBONACCI_CHORD,
  SUCCESSOR_TABLE_MAX_LENGTH,
  NULL_NODE,
  type Node,
} from "./utils.ts";
import { OVERALL_HEALTH, type HealthImplementation } from "./health.ts";
const phi = (1 + Math.sqrt(5)) / 2;

interface FingerTableEntry {
  start: number | null;
  successor: Node;
}

export abstract class ChordNode {
  id: number;
  host: string;
  port: number;
  logger: pino.Logger;
  fingerTable: Array<FingerTableEntry> = [
    {
      start: null,
      successor: NULL_NODE,
    },
  ];
  successorTable: Array<Node> = [NULL_NODE];
  predecessor: Node = NULL_NODE;
  stabilizeIsLocked: boolean = false;
  fixFingersIsLocked: boolean = false;
  fingerToFix: number = 0;
  checkPredecessorIsLocked: boolean = false;
  // Periodic maintenance timers started in joinCluster(); cancelled in
  // destructor() so they can't race the teardown sequence (see issue #187).
  stabilizeTimer?: ReturnType<typeof setInterval>;
  fixFingersTimer?: ReturnType<typeof setInterval>;
  checkPredecessorTimer?: ReturnType<typeof setInterval>;
  // Standard gRPC health reporter; set in UserService.serve() and flipped to
  // NOT_SERVING in destructor() so probes observe departure (issue #97).
  health?: HealthImplementation;

  constructor({
    id,
    host,
    port,
  }: {
    id?: number;
    host?: string;
    port?: number;
  }) {
    if (!host || !port) {
      console.error(
        "ChordNode constructor did not receive host or port as expected",
      );
      process.exit(-9);
    }
    this.id = id ?? 0;
    this.host = host;
    this.port = port;
    this.logger = createLogger(host, port);
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
   * Directly implement the pseudocode's findSuccessor() method.
   *
   * However, it is able to discern whether to do a local lookup or an RPC.
   * If the querying node is the same as the queried node, it will stay local.
   *
   * @param nodeQueried node being queried for the ID
   *
   */
  async findSuccessor(id: number, nodeQueried: Node) {
    let nPrime = NULL_NODE;
    let nPrimeSuccessor = NULL_NODE;
    this.logger.debug(`findSuccessor: node queried {${nodeQueried.id}}.`);
    if (this.id != undefined && this.iAmTheNode(nodeQueried)) {
      try {
        nPrime = await this.findPredecessor(id);
      } catch (err) {
        this.logger.error({ err }, `findSuccessor: findPredecessor failed`);
        nPrime = NULL_NODE;
      }

      this.logger.debug(`findSuccessor: n' is ${nPrime.id}`);

      try {
        nPrimeSuccessor = await this.getSuccessor(nPrime);
      } catch (err) {
        this.logger.error(
          { err },
          `findSuccessor: call to getSuccessor failed`,
        );
        nPrimeSuccessor = NULL_NODE;
      }

      this.logger.debug(`findSuccessor: n'.successor is ${nPrimeSuccessor.id}`);
    } else {
      const nodeQueriedClient = connect(nodeQueried);
      try {
        nPrimeSuccessor = await nodeQueriedClient.findSuccessorRemoteHelper({
          id: id,
          node: nodeQueried,
        });
      } catch (err) {
        nPrimeSuccessor = NULL_NODE;
        handleGRPCErrors(
          this.logger,
          "findSuccessor",
          "findSuccessorRemoteHelper",
          nodeQueried.host,
          nodeQueried.port,
          err,
        );
      }
    }

    this.logger.debug(
      `findSuccessor: departing n'.successor = ${nPrimeSuccessor.id}`,
    );
    return nPrimeSuccessor;
  }

  /**
   * RPC equivalent of the pseudocode's findSuccessor() method.
   * It is implemented as simply a wrapper for the local findSuccessor() method.
   *
   * @param idAndNodeQueried {id:, node:}, where ID is the key sought
   * @param callback grpc callback function
   */
  async findSuccessorRemoteHelper(
    idAndNodeQueried: { request: { id: number; node: Node } },
    callback: (arg0: any, arg1: Node) => void,
  ) {
    const id = idAndNodeQueried.request.id;
    const nodeQueried = idAndNodeQueried.request.node;

    this.logger.debug(
      `findSuccessorRemoteHelper: id = ${id} nodeQueried = ${nodeQueried.id}.`,
    );

    let nPrimeSuccessor = NULL_NODE;
    try {
      nPrimeSuccessor = await this.findSuccessor(id, nodeQueried);
    } catch (err) {
      this.logger.error(
        { err },
        "findSuccessorRemoteHelper: findSuccessor failed",
      );
      nPrimeSuccessor = NULL_NODE;
    }
    callback(null, nPrimeSuccessor);

    this.logger.debug(
      `findSuccessorRemoteHelper: nPrimeSuccessor = ${nPrimeSuccessor.id}`,
    );
  }

  /**
   * This function directly implements the pseudocode's findPredecessor() method,
   *  with the exception of the limits on the while loop.
   * @param {number} id the key sought
   */
  async findPredecessor(id: number) {
    this.logger.debug(`findPredecessor: id = ${id}`);

    let nPrime = this.encapsulateSelf();
    let nPrimeSuccessor = NULL_NODE;
    try {
      nPrimeSuccessor = await this.getSuccessor(nPrime);
    } catch (err) {
      this.logger.error({ err }, "findPredecessor: getSuccessor failed");
      nPrimeSuccessor = NULL_NODE;
    }

    this.logger.debug(
      `findPredecessor: before while: nPrime = ${nPrime.id}; nPrimeSuccessor = ${nPrimeSuccessor.id}`,
    );

    let iterationCounter = 2 ** HASH_BIT_LENGTH * HASH_BIT_LENGTH;
    while (
      !isInModuloRange(id, nPrime.id, false, nPrimeSuccessor.id, true) &&
      nPrime.id !== nPrimeSuccessor.id &&
      iterationCounter >= 0
    ) {
      // loop should exit if n' and its successor are the same
      // loop should exit if the iterations are ridiculous
      // update loop protection
      iterationCounter--;
      try {
        nPrime = await this.closestPrecedingFinger(id, nPrime);
      } catch (err) {
        nPrime = NULL_NODE;
      }

      this.logger.debug(
        `findPredecessor: At iterator ${iterationCounter} nPrime = ${nPrime}`,
      );

      try {
        nPrimeSuccessor = await this.getSuccessor(nPrime);
      } catch (err) {
        this.logger.error(
          { err },
          "findPredecessor: call to getSuccessor (2) failed",
        );
        nPrimeSuccessor = NULL_NODE;
      }

      this.logger.debug(
        `findPredecessor: nPrimeSuccessor = ${nPrimeSuccessor.id}`,
      );
    }

    return nPrime;
  }

  /**
   * Return the successor of a given node by either a local lookup or an RPC.
   * If the querying node is the same as the queried node, it will be a local lookup.
   * @returns : the successor if the successor seems valid, or a null node otherwise
   */
  async getSuccessor(nodeQueried: Node) {
    this.logger.debug(`getSuccessor(${nodeQueried.id})`);

    // get n.successor either locally or remotely
    let nSuccessor = NULL_NODE;
    if (this.iAmTheNode(nodeQueried)) {
      // use local value
      nSuccessor = this.fingerTable[0].successor;
    } else {
      // use remote value
      const nodeQueriedClient = connect(nodeQueried);
      try {
        nSuccessor = await nodeQueriedClient.getSuccessorRemoteHelper();
      } catch (err) {
        nSuccessor = { id: null, host: null, port: null };
        handleGRPCErrors(
          this.logger,
          "getSuccessor",
          "getSuccessorRemoteHelper",
          nodeQueried.host,
          nodeQueried.port,
          err,
        );
      }
    }

    this.logger.debug(
      `getSuccessor: returning {${nodeQueried.id}}.successor = ${nSuccessor.id}`,
    );

    return nSuccessor;
  }

  /**
   * RPC equivalent of the getSuccessor() method.
   * It is implemented as simply a wrapper for the getSuccessor() function.
   */
  async getSuccessorRemoteHelper(
    _: any,
    callback: (arg0: any, arg1: Node) => void,
  ) {
    callback(null, this.fingerTable[0].successor);
  }

  /**
   * RPC to replace the value of the node's successor.
   */
  async setSuccessor(
    message: { request: any },
    callback: (arg0: any, arg1: {}) => void,
  ) {
    this.logger.debug(
      {
        self: this.encapsulateSelf(),
        originalSuccessor: this.fingerTable[0].successor.id,
      },
      "setSuccessor",
    );
    const successorCandidate = message.request;
    if (
      successorCandidate.id !== null &&
      successorCandidate.host !== null &&
      successorCandidate.port !== null
    ) {
      this.fingerTable[0].successor = successorCandidate;
    }
    this.logger.debug(
      `setSuccessor: new successor = ${this.fingerTable[0].successor.id}`,
    );

    callback(null, {});
  }

  /**
   * Directly implement the pseudocode's closestPrecedingFinger() method.
   *
   * However, it is able to discern whether to do a local lookup or an RPC.
   * If the querying node is the same as the queried node, it will stay local.
   *
   * @returns the closest preceding node to ID
   *
   */
  async closestPrecedingFinger(id: number, nodeQueried: Node) {
    let nPreceding = NULL_NODE;
    if (this.iAmTheNode(nodeQueried)) {
      // use local value
      for (let i = this.fingerTable.length - 1; i >= 0; i--) {
        if (
          isInModuloRange(
            this.fingerTable[i].successor.id,
            nodeQueried.id,
            false,
            id,
            false,
          )
        ) {
          nPreceding = this.fingerTable[i].successor;
          return nPreceding;
        }
      }
      nPreceding = nodeQueried;
      return nPreceding;
    } else {
      const nodeQueriedClient = connect(nodeQueried);
      try {
        nPreceding = await nodeQueriedClient.closestPrecedingFingerRemoteHelper(
          {
            id: id,
            node: nodeQueried,
          },
        );
      } catch (err) {
        nPreceding = NULL_NODE;
        handleGRPCErrors(
          this.logger,
          "closestPrecedingFinger",
          "closestPrecedingFingerRemoteHelper",
          nodeQueried.host,
          nodeQueried.port,
          err,
        );
      }
      return nPreceding;
    }
  }

  /**
   * RPC equivalent of the pseudocode's closestPrecedingFinger() method.
   * It is implemented as simply a wrapper for the local closestPrecedingFinger() function.
   *
   * @param idAndNodeQueried {id:, node:}, where ID is the key sought
   */
  async closestPrecedingFingerRemoteHelper(
    idAndNodeQueried: { request: { id: any; node: Node } },
    callback: (arg0: any, arg1: Node) => void,
  ) {
    const id = idAndNodeQueried.request.id;
    const nodeQueried = idAndNodeQueried.request.node;
    let nPreceding = NULL_NODE;
    try {
      nPreceding = await this.closestPrecedingFinger(id, nodeQueried);
    } catch (err) {
      this.logger.error(
        { err },
        "closestPrecedingFingerRemoteHelper: closestPrecedingFinger failed",
      );
      nPreceding = NULL_NODE;
    }
    callback(null, nPreceding);
  }

  /**
   * RPC to return the node's predecessor.
   */
  async getPredecessor(_: any, callback: (arg0: any, arg1: Node) => void) {
    callback(null, this.predecessor);
  }

  async getFingerTableEntries(call: {
    write: (arg0: { index: number; node: Node }) => void;
    end: () => void;
  }) {
    this.fingerTable.forEach((fingerTableEntry) => {
      call.write({
        index: fingerTableEntry.start!,
        node: fingerTableEntry.successor,
      });
    });
    call.end();
  }

  /**
   * RPC to replace the value of the node's predecessor.
   */
  async setPredecessor(
    message: { request: Node },
    callback: (arg0: any, arg1: {}) => void,
  ) {
    this.logger.debug(
      {
        self: this.encapsulateSelf(),
        originalPredecessor: this.predecessor,
      },
      "setPredecessor",
    );

    this.predecessor = message.request;

    this.logger.debug(
      { newPredecessor: this.predecessor },
      "setPredecessor: updated",
    );

    callback(null, {});
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
      try {
        knownNodeId = await this.getNodeId(knownNode);
        knownNode.id = knownNodeId;
      } catch (err) {
        knownNode.id = null;
        handleGRPCErrors(
          this.logger,
          "joinCluster",
          "getNodeId",
          knownNode.host,
          knownNode.port,
          err,
        );
      }
      // then check for a collision between the ID intended for this new node and an existing node
      try {
        possibleCollidingNode = await this.findSuccessor(this.id, knownNode);
      } catch (err) {
        possibleCollidingNode = NULL_NODE;
      }
      if (this.iAmTheNode(possibleCollidingNode)) {
        // node collision
        errorString = `Error joining node "${this.host}:${this.port}" with ID {${this.id}} to node "${knownNode.host}:${knownNode.port}" because of a collision with node "${possibleCollidingNode.host}:${possibleCollidingNode.port}" having ID={${possibleCollidingNode.id}}.`;
        this.logger.error(errorString);
        throw new RangeError(errorString);
      }
      // now go ahead with the join
      await this.initFingerTable(knownNode);
      await this.updateOthers();
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

    // And now that we've joined a cluster, we need maintain our state
    // There might be some "critical section" type issues
    // we need to use a gate to protect in these functions
    this.stabilizeTimer = setInterval(this.stabilize.bind(this), 1000);
    this.fixFingersTimer = setInterval(this.fixFingers.bind(this), 3000);
    this.checkPredecessorTimer = setInterval(
      this.checkPredecessor.bind(this),
      1000,
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
   * Determine whether a node exists by asking for its ID.
   * @returns - true if the node has a valid ID
   */
  async confirmExist(knownNode: Node): Promise<boolean> {
    let nodeId = null;
    let nodeExists = false;
    try {
      nodeId = await this.getNodeId(knownNode);
    } catch (err) {
      nodeId = null;
      this.logger.error(
        { err },
        `Error confirming existence of node "${knownNode.host}:${knownNode.port}"`,
      );
    }
    if (nodeId !== null && nodeId >= 0) {
      nodeExists = true;
    } else {
      nodeExists = false;
    }
    return nodeExists;
  }

  /**
   * Returns a node's ID, making an RPC if necessary.
   * @returns - node's ID, or null if error
   */
  async getNodeId(knownNode: Node): Promise<number | null> {
    let nodeId = null;
    let knownNodeObject = NULL_NODE;
    let selfNodeString = (this.host + ":" + this.port).toLowerCase();
    let knownNodeString = (knownNode.host + ":" + knownNode.port).toLowerCase();
    if (selfNodeString === knownNodeString) {
      // use local value
      nodeId = this.id;
    } else {
      // use remote value
      let knownNodeClient = connect(knownNode);
      try {
        knownNodeObject =
          await knownNodeClient.getNodeIdRemoteHelper(knownNode);
        nodeId = knownNodeObject.id;
      } catch (err) {
        nodeId = null;
        this.logger.debug(
          { err },
          `Error getting ID of node "${knownNode.host}:${knownNode.port}"`,
        );
      }
    }
    return nodeId;
  }

  /**
   * RPC to return the node's ID.
   *
   * @param {any} _           - unused dummy argument
   * @param {string} callback - grpc callback
   */
  async getNodeIdRemoteHelper(
    _: any,
    callback: (arg0: any, arg1: Node) => void,
  ): Promise<void> {
    callback(null, { id: this.id, host: this.host, port: this.port });
  }

  /**
   * Directly implement the pseudocode's initFingerTable() method.
   */
  async initFingerTable(nPrime: Node) {
    this.logger.debug(
      `initFingerTable: self = ${this.id}; self.successor = ${this.fingerTable[0].successor.id}; finger[0].start = ${this.fingerTable[0].start} n' = ${nPrime.id}`,
    );

    let nPrimeSuccessor = NULL_NODE;
    try {
      nPrimeSuccessor = await this.findSuccessor(
        this.fingerTable[0].start!,
        nPrime,
      );
    } catch (err) {
      nPrimeSuccessor = NULL_NODE;
      this.logger.error({ err }, "initFingerTable: findSuccessor failed");
    }
    this.fingerTable[0].successor = nPrimeSuccessor;

    this.logger.debug(
      { nPrimeSuccessor },
      "initFingerTable: n'.successor (now self.successor)",
    );

    let successorClient = connect(this.fingerTable[0].successor);
    try {
      this.predecessor = await successorClient.getPredecessor();
    } catch (err) {
      this.predecessor = NULL_NODE;
      handleGRPCErrors(
        this.logger,
        "initFingerTable",
        "getPredecessor",
        this.fingerTable[0].successor.host,
        this.fingerTable[0].successor.port,
        err,
      );
    }
    try {
      await successorClient.setPredecessor(this.encapsulateSelf());
    } catch (err) {
      handleGRPCErrors(
        this.logger,
        "initFingerTable",
        "setPredecessor",
        this.fingerTable[0].successor.host,
        this.fingerTable[0].successor.port,
        err,
      );
    }

    this.logger.debug(
      { predecessor: this.predecessor },
      "initFingerTable: predecessor",
    );

    for (let i = 0; i < this.fingerTable.length - 1; i++) {
      if (
        isInModuloRange(
          this.fingerTable[i + 1].start,
          this.id,
          true,
          this.fingerTable[i].successor.id,
          false,
        )
      ) {
        this.fingerTable[i + 1].successor = this.fingerTable[i].successor;
      } else {
        try {
          this.fingerTable[i + 1].successor = await this.findSuccessor(
            this.fingerTable[i + 1].start!,
            nPrime,
          );
        } catch (err) {
          this.fingerTable[i + 1].successor = NULL_NODE;
          this.logger.error({ err }, "initFingerTable: findSuccessor() failed");
        }
      }
    }
    this.logger.debug(
      { fingerTable: this.fingerTable },
      "initFingerTable: fingerTable[]",
    );
  }

  /**
   * Directly implement the pseudocode's updateOthers() method.
   */
  async updateOthers() {
    this.logger.debug("updateOthers");
    let pNodeSearchID: number,
      pNodeClient: {
        updateFingerTable: (arg0: { node: Node; index: number }) => any;
      };
    let pNode = NULL_NODE;

    for (let i = 0; i < this.fingerTable.length; i++) {
      pNodeSearchID =
        (this.id - 2 ** i + 2 ** HASH_BIT_LENGTH) % 2 ** HASH_BIT_LENGTH;
      this.logger.debug(
        `updateOthers: i = ${i}; findPredecessor(${pNodeSearchID}) --> pNode`,
      );

      try {
        pNode = await this.findPredecessor(pNodeSearchID);
      } catch (err) {
        pNode = NULL_NODE;
        this.logger.error(
          { err },
          `updateOthers: Error from findPredecessor(${pNodeSearchID})`,
        );
      }

      this.logger.debug(`updateOthers: pNode = ${pNode?.id}`);

      if (this.id !== pNode.id) {
        pNodeClient = connect(pNode);
        try {
          await pNodeClient.updateFingerTable({
            node: this.encapsulateSelf(),
            index: i,
          });
        } catch (err) {
          handleGRPCErrors(
            this.logger,
            "updateOthers",
            "updateFingerTable",
            pNode.host,
            pNode.port,
            err,
          );
        }
      }
    }
  }

  /**
   * RPC that directly implements the pseudocode's updateFingerTable() method.
   */
  async updateFingerTable(
    message: { request: { node: Node; index: any } },
    callback: (arg0: any, arg1: {}) => void,
  ) {
    const sNode = message.request.node;
    const fingerIndex = message.request.index;

    this.logger.debug(
      {
        fingerTable: this.fingerTable,
        sNodeId: message.request.node.id,
        fingerIndex,
      },
      "updateFingerTable",
    );

    if (
      isInModuloRange(
        sNode.id,
        this.id,
        true,
        this.fingerTable[fingerIndex].successor.id,
        false,
      )
    ) {
      this.fingerTable[fingerIndex].successor = sNode;
      const pClient = connect(this.predecessor);
      try {
        await pClient.updateFingerTable({
          node: sNode,
          index: fingerIndex,
        });
      } catch (err) {
        handleGRPCErrors(
          this.logger,
          "updateFingerTable",
          "updateFingerTable",
          this.predecessor.host,
          this.predecessor.port,
          err,
        );
      }

      this.logger.debug(
        `updateFingerTable: Updated {${this.id}}.fingerTable[${fingerIndex}] to ${sNode?.id}`,
      );

      callback(null, {});
      return;
    }

    callback(null, {});
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
      for (
        let i = 0;
        i < this.successorTable.length && i <= SUCCESSOR_TABLE_MAX_LENGTH;
        i++
      ) {
        try {
          successorSuccessor = await this.getSuccessor(this.successorTable[i]);
        } catch (err) {
          this.logger.error(
            { err },
            `updateSuccessorTable: getSuccessor failed`,
          );
          successorSuccessor = { id: null, host: null, port: null };
        }
        this.logger.debug(
          `updateSuccessorTable: {${this.id}}.successorTable[${i}] = ${this.successorTable[i].id} and {${this.successorTable[i].id}}.successor[0] = ${successorSuccessor.id}`,
        );

        if (
          successorSuccessor &&
          successorSuccessor.id !== null &&
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
    if (!this.stabilizeIsLocked) {
      this.stabilizeIsLocked = true;
      let successorClient: {
          getPredecessor: () => any;
          notify: (arg0: Node) => any;
        },
        x: Node;
      if (this.iAmMyOwnSuccessor()) {
        // use local value
        await this.stabilizeSelf();
        x = this.encapsulateSelf();
      } else {
        // use remote value
        try {
          successorClient = connect(this.fingerTable[0].successor);
        } catch (err) {
          this.logger.error(
            { err },
            `stabilize: call to connect {${this.fingerTable[0].successor.id}} failed`,
          );
          this.stabilizeIsLocked = false;
          return false;
        }
        try {
          x = await successorClient.getPredecessor();
        } catch (err) {
          x = this.encapsulateSelf();
          handleGRPCErrors(
            this.logger,
            "stabilize",
            "getPredecessor",
            this.fingerTable[0].successor.host,
            this.fingerTable[0].successor.port,
            err,
          );
        }
      }

      if (
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
          successorClient = connect(this.fingerTable[0].successor);
          await successorClient.notify(this.encapsulateSelf());
        } catch (err) {
          handleGRPCErrors(
            this.logger,
            "stabilize",
            "successorClient",
            this.fingerTable[0].successor.host,
            this.fingerTable[0].successor.port,
            err,
          );
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
      this.stabilizeIsLocked = false;
      return true;
    }
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
   * Directly implements the pseudocode's notify() method.
   */
  async notify(
    message: { request: any },
    callback: (arg0: any, arg1: {}) => void,
  ) {
    const nPrime = message.request;
    if (
      this.predecessor.id == null ||
      isInModuloRange(nPrime.id, this.predecessor.id, false, this.id, false)
    ) {
      this.predecessor = nPrime;
    }
    callback(null, {});
  }

  /**
   * Directly implements the pseudocode's fixFingers() method.
   */
  async fixFingers() {
    if (!this.fixFingersIsLocked) {
      this.fixFingersIsLocked = true;
      let nSuccessor = NULL_NODE;
      try {
        nSuccessor = await this.findSuccessor(
          this.fingerTable[this.fingerToFix].start!,
          this.encapsulateSelf(),
        );
        if (nSuccessor.id !== null) {
          this.fingerTable[this.fingerToFix].successor = nSuccessor;
        }
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
      this.fixFingersIsLocked = false;
    }
  }

  /**
   * Checks to make sure that the predecessor is still responsive
   */
  async checkPredecessor(): Promise<boolean> {
    if (!this.checkPredecessorIsLocked) {
      this.checkPredecessorIsLocked = true;
      if (this.predecessor.id !== null && !this.iAmMyOwnPredecessor()) {
        const predecessorClient = connect(this.predecessor);
        try {
          const _ = await predecessorClient.getPredecessor();
        } catch (err) {
          handleGRPCErrors(
            this.logger,
            "checkPredecessor",
            "getPredecessor",
            this.predecessor.host,
            this.predecessor.port,
            err,
          );
          // Wipe out the predecessor if it doesn't respond
          this.predecessor = NULL_NODE;
          this.checkPredecessorIsLocked = false;
          return false;
        }
      }
      this.checkPredecessorIsLocked = false;
      return true;
    }
    return false;
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
    clearInterval(this.stabilizeTimer);
    clearInterval(this.fixFingersTimer);
    clearInterval(this.checkPredecessorTimer);

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
        const predecessorClient = connect(this.predecessor);
        await predecessorClient.setSuccessor(successor);
      } catch (err) {
        handleGRPCErrors(
          this.logger,
          "destructor",
          "setSuccessor",
          this.predecessor.host,
          this.predecessor.port,
          err,
        );
      }
    }
    // notify successor
    if (successorSeemsOK) {
      try {
        const successorClient = connect(successor);
        await successorClient.setPredecessor(this.predecessor);
      } catch (err) {
        handleGRPCErrors(
          this.logger,
          "destructor",
          "setPredecessor",
          successor.host,
          successor.port,
          err,
        );
      }
    }
    // report what's up and destroy the node by exiting the process
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
    process.exit(0);
  }

  abstract migrateKeysAfterJoining(): Promise<void>;
  abstract migrateKeysBeforeDeparture(): Promise<boolean>;
}
