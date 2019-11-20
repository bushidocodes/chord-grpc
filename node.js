const os = require("os");
const process = require("process");
const path = require("path");
const grpc = require("grpc");
const caller = require("grpc-caller");
const protoLoader = require("@grpc/proto-loader");
const minimist = require("minimist");
const {
  isInModuloRange,
  computeIntegerHash,
  handleGRPCErrors
} = require("./utils.js");

const CHECK_NODE_TIMEOUT_ms = 1000;
const DEBUGGING_LOCAL = false;
const DEFAULT_HOST_PORT = 8440;
const HASH_BIT_LENGTH = 8;
const PROTO_PATH = path.resolve(__dirname, "./protos/chord.proto");

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true
});
const chord = grpc.loadPackageDefinition(packageDefinition).chord;
const NULL_NODE = { id: null, host: null, port: null };

/**
 * Print Summary of state of node
 * @param userRequestObject
 * @param callback gRPC callback
 */

const nodesAreNotIdentical = (
  sourceHost,
  sourcePort,
  destinationHost,
  destinationPort
) => !(sourceHost == destinationHost && sourcePort == destinationPort);

class ChordNode {
  constructor({ id, host, port, knownId, knownHost, knownPort }) {
    this.host = host || os.hostname();
    this.port = port || DEFAULT_HOST_PORT;
    this.id = id || null;
    this.knownId = knownId || null;
    this.knownHost = knownHost || os.hostname();
    this.knownPort = knownPort || DEFAULT_HOST_PORT;
    this.fingerTable = [
      {
        start: null,
        successor: NULL_NODE
      }
    ];
    this.successorTable = [NULL_NODE];
    this.predecessor = NULL_NODE;

    setImmediate(async () => {
      if (!this.id) {
        this.id = await computeIntegerHash(
          this.host + this.port,
          HASH_BIT_LENGTH
        );
      }
      // recompute known identity parameters from hash function
      if (!this.knownId) {
        this.knownId = await computeIntegerHash(
          knownHost + knownPort,
          HASH_BIT_LENGTH
        );
      }
      this.join({
        id: this.knownId,
        host: this.knownHost,
        port: this.knownPort
      });
    });

    setInterval(() => this.stabilize(), 1000);
    setInterval(() => this.fixFingers(), 3000);
    setInterval(() => this.checkPredecessor(), CHECK_NODE_TIMEOUT_ms);
  }

  iAmMyOwnSuccessor() {
    return this.id == this.fingerTable[0].successor.id;
  }

  iAmMyOwnPredecessor() {
    return this.id == this.predecessor.id;
  }

  encapsulateSelf() {
    return {
      id: this.id,
      host: this.host,
      port: this.port
    };
  }

  /**
   * Print Summary of state of node
   * @param userRequestObject
   * @param callback gRPC callback
   */
  summary(_, callback) {
    console.log("Summary: fingerTable: \n", this.fingerTable);
    console.log("Summary: Predecessor: ", predecessor);
    callback(null, this.encapsulateSelf());
  }
  /**
   * Directly implement the pseudocode's findSuccessor() method.
   *
   * However, it is able to discern whether to do a local lookup or an RPC.
   * If the querying node is the same as the queried node, it will stay local.
   *
   * @param {number} id value being searched
   * @param nodeQueried node being queried for the ID
   *
   */
  async findSuccessor(id, nodeQueried) {
    let nPrime = NULL_NODE;
    let nPrimeSuccessor = NULL_NODE;
    if (DEBUGGING_LOCAL)
      console.log(`findSuccessor: node queried {${nodeQueried.id}}.`);
    if (this.id != undefined && this.id == nodeQueried.id) {
      try {
        nPrime = await this.findPredecessor(id);
      } catch (err) {
        console.error(`findSuccessor: findPredecessor failed with `, err);
        nPrime = NULL_NODE;
      }

      if (DEBUGGING_LOCAL) console.log("findSuccessor: n' is ", nPrime.id);

      try {
        nPrimeSuccessor = await this.getSuccessor(nPrime);
      } catch (err) {
        console.error(`findSuccessor: call to getSuccessor failed with `, err);
        nPrimeSuccessor = NULL_NODE;
      }

      if (DEBUGGING_LOCAL) {
        console.log("findSuccessor: n'.successor is ", nPrimeSuccessor.id);
      }
    } else {
      const nodeQueriedClient = caller(
        `${nodeQueried.host}:${nodeQueried.port}`,
        PROTO_PATH,
        "Node"
      );
      try {
        nPrimeSuccessor = await nodeQueriedClient.findSuccessorRemoteHelper({
          id: id,
          node: nodeQueried
        });
      } catch (err) {
        nPrimeSuccessor = NULL_NODE;
        handleGRPCErrors(
          "findSuccessor",
          "findSuccessorRemoteHelper",
          nodeQueried.host,
          nodeQueried.port,
          err
        );
      }
    }

    if (DEBUGGING_LOCAL)
      console.log(
        "findSuccessor: departing n'.successor = ",
        nPrimeSuccessor.id
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
  async findSuccessorRemoteHelper(idAndNodeQueried, callback) {
    const id = idAndNodeQueried.request.id;
    const nodeQueried = idAndNodeQueried.request.node;

    if (DEBUGGING_LOCAL)
      console.log(
        `findSuccessorRemoteHelper: id = ${id} nodeQueried = ${nodeQueried.id}.`
      );

    let nPrimeSuccessor = NULL_NODE;
    try {
      nPrimeSuccessor = await this.findSuccessor(id, nodeQueried);
    } catch (err) {
      console.error(
        "findSuccessorRemoteHelper: findSuccessor failed with",
        err
      );
      nPrimeSuccessor = NULL_NODE;
    }
    callback(null, nPrimeSuccessor);

    if (DEBUGGING_LOCAL) {
      console.log(
        `findSuccessorRemoteHelper: nPrimeSuccessor = ${nPrimeSuccessor.id}`
      );
    }
  }

  /**
   * This function directly implements the pseudocode's findPredecessor() method,
   *  with the exception of the limits on the while loop.
   * @param {number} id the key sought
   */
  async findPredecessor(id) {
    if (DEBUGGING_LOCAL) console.log("findPredecessor: id = ", id);

    let nPrime = this.encapsulateSelf();
    let nPrimeSuccessor = NULL_NODE;
    try {
      nPrimeSuccessor = await this.getSuccessor(nPrime);
    } catch (err) {
      console.error("findPredecessor: getSuccessor failed with", err);
      nPrimeSuccessor = NULL_NODE;
    }

    if (DEBUGGING_LOCAL) {
      console.log(
        `findPredecessor: before while: nPrime = ${nPrime.id}; nPrimeSuccessor = ${nPrimeSuccessor.id}`
      );
    }

    let iterationCounter = 2 ** HASH_BIT_LENGTH * HASH_BIT_LENGTH;
    while (
      !isInModuloRange(id, nPrime.id, false, nPrimeSuccessor.id, true) &&
      nPrime.id !== nPrimeSuccessor.id &&
      iterationCounter >= 0
    ) {
      // loop should exit if n' and its successor are the same
      // loop should exit if n' and the prior n' are the same
      // loop should exit if the iterations are ridiculous
      // update loop protection
      iterationCounter--;
      try {
        nPrime = await this.closestPrecedingFinger(id, nPrime);
      } catch (err) {
        nPrime = NULL_NODE;
      }

      if (DEBUGGING_LOCAL)
        console.log(
          `findPredecessor: At iterator ${iterationCounter} nPrime = ${nPrime}`
        );

      try {
        nPrimeSuccessor = await this.getSuccessor(nPrime);
      } catch (err) {
        console.error(
          "findPredecessor call to getSuccessor (2) failed with",
          err
        );
        nPrimeSuccessor = NULL_NODE;
      }

      if (DEBUGGING_LOCAL)
        console.log("findPredecessor: nPrimeSuccessor = ", nPrimeSuccessor);
    }

    return nPrime;
  }

  /**
   * Return the successor of a given node by either a local lookup or an RPC.
   * If the querying node is the same as the queried node, it will be a local lookup.
   * @param nodeQueried
   * @returns : the successor if the successor seems valid, or a null node otherwise
   */
  async getSuccessor(nodeQueried) {
    if (DEBUGGING_LOCAL) console.log(`getSuccessor(${nodeQueried.id})`);

    // get n.successor either locally or remotely
    let nSuccessor = NULL_NODE;
    if (this.id == nodeQueried.id) {
      // use local value
      nSuccessor = this.fingerTable[0].successor;
    } else {
      // use remote value
      const nodeQueriedClient = caller(
        `${nodeQueried.host}:${nodeQueried.port}`,
        PROTO_PATH,
        "Node"
      );
      try {
        nSuccessor = await nodeQueriedClient.getSuccessorRemoteHelper(
          nodeQueried
        );
      } catch (err) {
        // TBD 20191103.hk: why does "nSuccessor = NULL_NODE;" not do the same as explicit?!?!
        nSuccessor = { id: null, host: null, port: null };
        handleGRPCErrors(
          "getSuccessor",
          "getSuccessorRemoteHelper",
          nodeQueried.host,
          nodeQueried.port,
          err
        );
      }
    }

    if (DEBUGGING_LOCAL)
      console.log(
        `getSuccessor: returning {${nodeQueried.id}}.successor = ${nSuccessor.id}`
      );

    return nSuccessor;
  }

  /**
   * RPC equivalent of the getSuccessor() method.
   * It is implemented as simply a wrapper for the getSuccessor() function.
   * @param _ - dummy parameter
   * @param callback - grpc callback
   */
  async getSuccessorRemoteHelper(_, callback) {
    callback(null, this.fingerTable[0].successor);
  }
  /**
   * Directly implement the pseudocode's closestPrecedingFinger() method.
   *
   * However, it is able to discern whether to do a local lookup or an RPC.
   * If the querying node is the same as the queried node, it will stay local.
   *
   * @param id
   * @param nodeQueried
   * @returns the closest preceding node to ID
   *
   */
  async closestPrecedingFinger(id, nodeQueried) {
    let nPreceding = NULL_NODE;
    if (this.id == nodeQueried.id) {
      // use local value
      for (let i = HASH_BIT_LENGTH - 1; i >= 0; i--) {
        if (
          isInModuloRange(
            this.fingerTable[i].successor.id,
            nodeQueried.id,
            false,
            id,
            false
          )
        ) {
          nPreceding = this.fingerTable[i].successor;
          return nPreceding;
        }
      }
      nPreceding = nodeQueried;
      return nPreceding;
    } else {
      // use remote value
      const nodeQueriedClient = caller(
        `${nodeQueried.host}:${nodeQueried.port}`,
        PROTO_PATH,
        "Node"
      );
      try {
        nPreceding = await nodeQueriedClient.closestPrecedingFingerRemoteHelper(
          {
            id: id,
            node: nodeQueried
          }
        );
      } catch (err) {
        nPreceding = NULL_NODE;
        handleGRPCErrors(
          "closestPrecedingFinger",
          "closestPrecedingFingerRemoteHelper",
          nodeQueried.host,
          nodeQueried.port,
          err
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
   * @param callback - grpc callback
   *
   */
  async closestPrecedingFingerRemoteHelper(idAndNodeQueried, callback) {
    const id = idAndNodeQueried.request.id;
    const nodeQueried = idAndNodeQueried.request.node;
    let nPreceding = NULL_NODE;
    try {
      nPreceding = await this.closestPrecedingFinger(id, nodeQueried);
    } catch (err) {
      console.error(
        "closestPrecedingFingerRemoteHelper: closestPrecedingFinger failed with ",
        err
      );
      nPreceding = NULL_NODE;
    }
    callback(null, nPreceding);
  }
  /**
   * RPC to return the node's predecessor.
   *
   * @param _ - unused dummy argument
   * @param callback - grpc callback
   * @returns predecessor node
   */
  async getPredecessor(_, callback) {
    callback(null, predecessor);
  }
  /**
   * RPC to replace the value of the node's predecessor.
   *
   * @param message is a node object
   * @param callback
   */
  async setPredecessor(message, callback) {
    if (DEBUGGING_LOCAL) {
      console.log("setPredecessor: Self = ", this.encapsulateSelf());
      console.log(
        "setPredecessor: Self's original predecessor = ",
        predecessor
      );
    }

    predecessor = message.request; //message.request is node

    if (DEBUGGING_LOCAL)
      console.log("setPredecessor: Self's new predecessor = ", predecessor);

    callback(null, {});
  }
  /**
   * Modified implementation of pseudocode's "heavyweight" version of the join() method
   *   as described in Figure 6 of the SIGCOMM paper.
   * Modification consists of an additional step of initializing the successor table
   *   as described in the IEEE paper.
   *
   * @param knownNode: knownNode structure; e.g., {id, host, port}
   *   Pass a null known node to force the node to be the first in a new chord.
   */
  async join(knownNode) {
    // remove dummy template initializer from table
    this.fingerTable.pop();
    // initialize table with reasonable values
    for (let i = 0; i < HASH_BIT_LENGTH; i++) {
      this.fingerTable.push({
        start: (this.id + 2 ** i) % 2 ** HASH_BIT_LENGTH,
        successor: this.encapsulateSelf()
      });
    }

    if (knownNode.id && this.confirmExist(knownNode)) {
      await this.initFingerTable(knownNode);
      await this.updateOthers();
    } else {
      // this is the first node
      this.predecessor = this.encapsulateSelf();
    }

    await this.migrateKeys();

    // initialize successor table
    this.successorTable[0] = this.fingerTable[0].successor;

    if (DEBUGGING_LOCAL) {
      console.log(">>>>>     join          ");
      console.log(
        `The fingerTable[] leaving {${this.id}}.join(${knownNode.id}) is:\n`,
        this.fingerTable
      );
      console.log(
        `The {${this.id}}.predecessor leaving join() is ${predecessor}`
      );
      console.log("          join     <<<<<\n");
    }
  }
  /**
   * Determine whether a node exists by pinging it.
   * @param knownNode: knownNode structure; e.g., {id, host, port}
   * @returns {boolean}
   */
  confirmExist(knownNode) {
    return !(this.id == knownNode.id);
  }
  /**
   * Directly implement the pseudocode's initFingerTable() method.
   * @param nPrime
   */
  async initFingerTable(nPrime) {
    if (DEBUGGING_LOCAL) {
      console.log(
        `initFingerTable: self = ${this.id}; self.successor = ${this.fingerTable[0].successor.id}; finger[0].start = ${this.fingerTable[0].start} n' = ${nPrime.id}`
      );
    }

    let nPrimeSuccessor = NULL_NODE;
    try {
      nPrimeSuccessor = await this.findSuccessor(
        this.fingerTable[0].start,
        nPrime
      );
    } catch (err) {
      nPrimeSuccessor = NULL_NODE;
      console.error("initFingerTable: findSuccessor failed with ", err);
    }
    this.fingerTable[0].successor = nPrimeSuccessor;

    if (DEBUGGING_LOCAL)
      console.log(
        "initFingerTable: n'.successor (now  self.successor) = ",
        nPrimeSuccessor
      );

    let successorClient = caller(
      `${this.fingerTable[0].successor.host}:${this.fingerTable[0].successor.port}`,
      PROTO_PATH,
      "Node"
    );
    try {
      this.predecessor = await successorClient.getPredecessor(
        this.fingerTable[0].successor
      );
    } catch (err) {
      this.predecessor = NULL_NODE;
      handleGRPCErrors(
        "initFingerTable",
        "getPredecessor",
        this.fingerTable[0].successor.host,
        this.fingerTable[0].successor.port,
        err
      );
    }
    try {
      await successorClient.setPredecessor(this.encapsulateSelf());
    } catch (err) {
      handleGRPCErrors(
        "initFingerTable",
        "setPredecessor",
        this.fingerTable[0].successor.host,
        this.fingerTable[0].successor.port,
        err
      );
    }

    if (DEBUGGING_LOCAL)
      console.log("initFingerTable: predecessor  ", predecessor);

    for (let i = 0; i < HASH_BIT_LENGTH - 1; i++) {
      if (
        isInModuloRange(
          this.fingerTable[i + 1].start,
          this.id,
          true,
          this.fingerTable[i].successor.id,
          false
        )
      ) {
        this.fingerTable[i + 1].successor = this.fingerTable[i].successor;
      } else {
        try {
          this.fingerTable[i + 1].successor = await this.findSuccessor(
            this.fingerTable[i + 1].start,
            nPrime
          );
        } catch (err) {
          this.fingerTable[i + 1].successor = NULL_NODE;
          console.error("initFingerTable: findSuccessor() failed with ", err);
        }
      }
    }
    if (DEBUGGING_LOCAL)
      console.log("initFingerTable: fingerTable[] =\n", this.fingerTable);
  }
  /**
   * Directly implement the pseudocode's updateOthers() method.
   */
  async updateOthers() {
    if (DEBUGGING_LOCAL) console.log("updateOthers");
    let pNodeSearchID, pNodeClient;
    let pNode = NULL_NODE;

    for (let i = 0; i < HASH_BIT_LENGTH; i++) {
      pNodeSearchID =
        (this.id - 2 ** i + 2 ** HASH_BIT_LENGTH) % 2 ** HASH_BIT_LENGTH;
      if (DEBUGGING_LOCAL)
        console.log(
          `updateOthers: i = ${i}; findPredecessor(${pNodeSearchID}) --> pNode`
        );

      try {
        pNode = await this.findPredecessor(pNodeSearchID);
      } catch (err) {
        pNode = NULL_NODE;
        console.error(
          `updateOthers: Error from findPredecessor(${pNodeSearchID}) in updateOthers().`,
          err
        );
      }

      if (DEBUGGING_LOCAL) console.log("updateOthers: pNode = ", pNode);

      if (this.id !== pNode.id) {
        pNodeClient = caller(`${pNode.host}:${pNode.port}`, PROTO_PATH, "Node");
        try {
          await pNodeClient.updateFingerTable({
            node: this.encapsulateSelf(),
            index: i
          });
        } catch (err) {
          handleGRPCErrors(
            "updateOthers",
            "updateFingerTable",
            pNode.host,
            pNode.port,
            err
          );
        }
      }
    }
  }
  /**
   * RPC that directly implements the pseudocode's updateFingerTable() method.
   * @param message - consists of {sNode, fingerIndex} *
   * @param callback - grpc callback
   */
  async updateFingerTable(message, callback) {
    const sNode = message.request.node;
    const fingerIndex = message.request.index;

    if (DEBUGGING_LOCAL) {
      console.log(
        `updateFingerTable: {${this.id}}.fingerTable[] =\n`,
        this.fingerTable
      );
      console.log(
        `updateFingerTable: sNode = ${message.request.node.id}; fingerIndex =${fingerIndex}`
      );
    }

    if (
      isInModuloRange(
        sNode.id,
        this.id,
        true,
        this.fingerTable[fingerIndex].successor.id,
        false
      )
    ) {
      this.fingerTable[fingerIndex].successor = sNode;
      const pClient = caller(
        `${predecessor.host}:${predecessor.port}`,
        PROTO_PATH,
        "Node"
      );
      try {
        await pClient.updateFingerTable({ node: sNode, index: fingerIndex });
      } catch (err) {
        handleGRPCErrors(
          "updateFingerTable",
          "updateFingerTable",
          predecessor.host,
          predecessor.port,
          err
        );
      }

      if (DEBUGGING_LOCAL)
        console.log(
          `updateFingerTable: Updated {${this.id}}.fingerTable[${fingerIndex}] to ${sNode}`
        );

      // TODO: Figure out how to determine if the above had an RC of 0
      // If so call callback({status: 0, message: "OK"}, {});
      callback(null, {});
      return;
    }

    // TODO: Figure out how to determine if the above had an RC of 0
    //callback({ status: 0, message: "OK" }, {});
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
   * @returns {boolean} true if it was successful; false otherwise.
   *
   */
  async updateSuccessorTable() {
    if (DEBUGGING_LOCAL) {
      console.log(
        `{updateSuccessorTable: ${this.id}}.successorTable[] =\n`,
        this.successorTable
      );
      console.log(
        `updateSuccessorTable: successor node id = ${this.fingerTable[0].successor.id}`
      );
    }

    // check whether the successor is available
    let successorSeemsOK = false;
    try {
      successorSeemsOK = await this.checkSuccessor();
    } catch (err) {
      console.error(`updateSuccessorTable: checkSuccessor failed with `, err);
      successorSeemsOK = false;
    }
    if (successorSeemsOK) {
      // synchronize immediate successor if it is valid
      this.successorTable[0] = this.fingerTable[0].successor;
    } else {
      // or prune if the successor is not valid
      while (!successorSeemsOK && this.successorTable.length > 0) {
        // try current successor again to account for contention or bad luck
        try {
          successorSeemsOK = await this.checkSuccessor();
        } catch (err) {
          console.error(
            `updateSuccessorTable: checkSuccessor failed with `,
            err
          );
          successorSeemsOK = false;
        }
        if (successorSeemsOK) {
          // synchronize immediate successor if it is valid
          this.successorTable[0] = this.fingerTable[0].successor;
        } else {
          // drop the first successor candidate
          this.successorTable.shift();
          // update the finger table to the next candidate
          this.fingerTable[0].successor = this.successorTable[0];
        }
      }
    }
    if (this.successorTable.length < 1) {
      // this node is isolated
      this.successorTable.push({
        id: this.id,
        host: this.host,
        port: this.port
      });
    }
    // try to bulk up the table
    let successorSuccessor = NULL_NODE;
    if (
      this.successorTable.length < HASH_BIT_LENGTH &&
      this.id !== this.fingerTable[0].successor
    ) {
      if (DEBUGGING_LOCAL) {
        console.log(
          `updateSuccessorTable: Short successorTable[]: prefer length ${HASH_BIT_LENGTH} but actual length is ${this.successorTable.length}.`
        );
      }
      for (
        let i = 0;
        i < this.successorTable.length && i <= HASH_BIT_LENGTH;
        i++
      ) {
        try {
          successorSuccessor = await this.getSuccessor(this.successorTable[i]);
        } catch (err) {
          console.error(`updateSuccessorTable: getSuccessor failed with `, err);
          successorSuccessor = { id: null, host: null, port: null };
        }
        if (DEBUGGING_LOCAL)
          console.log(
            `updateSuccessorTable: {${this.id}}.st[${i}] = ${this.successorTable[i].id}; {${this.successorTable[i].id}}.successor[0] = ${successorSuccessor.id}`
          );

        if (
          successorSuccessor &&
          successorSuccessor.id !== null &&
          !isInModuloRange(
            successorSuccessor.id,
            this.id,
            true,
            this.successorTable[i].id,
            true
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
    successorSuccessor = { id: null, host: null, port: null };
    while (
      (!successorSeemsOK || this.successorTable.length > HASH_BIT_LENGTH) &&
      i > 0
    ) {
      try {
        successorSuccessor = await this.getSuccessor(this.successorTable[i]);
        if (successorSuccessor.id !== null) {
          successorSeemsOK = true;
        }
      } catch (err) {
        console.error(
          `updateSuccessorTable call to getSuccessor failed with `,
          err
        );
        successorSeemsOK = false;
        successorSuccessor = NULL_NODE;
      }
      if (!successorSeemsOK || i >= HASH_BIT_LENGTH) {
        // remove successor candidate
        this.successorTable.pop();
      }
      i -= 1;
    }

    if (DEBUGGING_LOCAL)
      console.log(
        `updateSuccessorTable: New {${this.id}}.successorTable[] =\n`,
        this.successorTable
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
    let successorClient, x;
    if (!this.fingerTable[0].successor) {
      process.exit("stabilize: fingerTable[0].successor was undefined");
    }
    try {
      successorClient = caller(
        `${this.fingerTable[0].successor.host}:${this.fingerTable[0].successor.port}`,
        PROTO_PATH,
        "Node"
      );
    } catch (err) {
      console.error(`stabilize: call to caller failed with `, err);
      return false;
    }
    if (this.fingerTable[0].successor.id == this.id) {
      // use local value
      await this.stabilizeSelf();
      x = { id: this.id, host: this.host, port: this.port };
    } else {
      // use remote value
      try {
        x = await successorClient.getPredecessor(this.fingerTable[0].successor);
      } catch (err) {
        x = { id: this.id, host: this.host, port: this.port };
        handleGRPCErrors(
          "stabilize",
          "getPredecessor",
          this.fingerTable[0].successor.host,
          this.fingerTable[0].successor.port,
          err
        );
      }
    }

    if (
      isInModuloRange(
        x.id,
        this.id,
        false,
        this.fingerTable[0].successor.id,
        false
      )
    ) {
      this.fingerTable[0].successor = x;
    }

    if (DEBUGGING_LOCAL) {
      console.log(
        `stabilize: {${this.id}}.predecessor leaving stabilize() is ${predecessor}`
      );
      console.log(
        "stabilize: {",
        this.id,
        "}.fingerTable[] is:\n",
        this.fingerTable
      );
      console.log(
        `stabilize: {${this.id}}.successorTable[] is\n`,
        this.successorTable
      );
    }

    if (this.id !== this.fingerTable[0].successor.id) {
      successorClient = caller(
        `${this.fingerTable[0].successor.host}:${this.fingerTable[0].successor.port}`,
        PROTO_PATH,
        "Node"
      );
      try {
        await successorClient.notify({
          id: this.id,
          host: this.host,
          port: this.port
        });
      } catch (err) {
        handleGRPCErrors(
          "stabilize",
          "successorClient",
          this.fingerTable[0].successor.host,
          this.fingerTable[0].successor.port,
          err
        );
      }
    }

    // update successor table - deviates from SIGCOMM
    try {
      await this.updateSuccessorTable();
    } catch (err) {
      console.error(`stabilize: updateSuccessorTable failed with `, err);
    }
    return true;
  }
  /**
   * Attempts to kick a node with a successor of self, as would be the case in the first node in a chord.
   * The kick comes from setting the successor to be equal to the predecessor.
   *
   * This is an original function, not described in either version of the paper - added 20191021.
   * @returns {boolean} true if it was a good kick; false if bad kick.
   */
  async stabilizeSelf() {
    let predecessorSeemsOK = false;
    if (this.predecessor.id == null) {
      // this node is in real trouble since its predecessor is no good either
      // TODO try to rescue it by stepping through the rest of its finger table, else destroy it
      predecessorSeemsOK = false;
      return predecessorSeemsOK;
    }
    if (!this.iAmMyOwnPredecessor()) {
      try {
        // confirm that the predecessor is actually there
        predecessorSeemsOK = await this.checkPredecessor();
      } catch (err) {
        predecessorSeemsOK = false;
        console.error(`stabilizeSelf: checkPredecessor failed with `, err);
      }
      if (predecessorSeemsOK) {
        // then kick by setting the successor to the same as the predecessor
        this.fingerTable[0].successor = predecessor;
        this.successorTable[0] = this.fingerTable[0].successor;
      }
    } else {
      if (DEBUGGING_LOCAL)
        console.log(
          `stabilizeSelf: Warning: {${this.id}} is isolated because predecessor is ${this.predecessor.id} and successor is ${this.fingerTable[0].successor.id}.`
        );
      predecessorSeemsOK = true;
    }
    return predecessorSeemsOK;
  }
  /**
   * Directly implements the pseudocode's notify() method.
   * @param message
   * @param callback the gRPC callback
   */
  async notify(message, callback) {
    const nPrime = message.request;
    if (
      predecessor.id == null ||
      isInModuloRange(nPrime.id, predecessor.id, false, this.id, false)
    ) {
      predecessor = nPrime;
    }
    callback(null, {});
  }
  /**
   * Directly implements the pseudocode's fixFingers() method.
   */
  async fixFingers() {
    let nSuccessor = NULL_NODE;
    const randomId = Math.ceil(Math.random() * (HASH_BIT_LENGTH - 1));
    try {
      nSuccessor = await this.findSuccessor(
        this.fingerTable[randomId].start,
        this.encapsulateSelf()
      );
      if (nSuccessor.id !== null) {
        this.fingerTable[randomId].successor = nSuccessor;
      }
    } catch (err) {
      console.error(`fixFingers: findSuccessor failed with `, err);
    }
    if (DEBUGGING_LOCAL) {
      console.log(
        `fixFingers: Fix {${this.id}}.fingerTable[${randomId}], with start = ${this.fingerTable[randomId].start}.`
      );
      console.log(
        `fixFingers: fingerTable[${randomId}] =${this.fingerTable[i].successor}`
      );
    }
  }
  /**
   * Checks to make sure that the predecessor is still responsive
   */
  async checkPredecessor() {
    if (this.predecessor.id !== null && !this.iAmMyOwnPredecessor()) {
      const predecessor = caller(
        `${this.predecessor.host}:${this.predecessor.port}`,
        PROTO_PATH,
        "Node"
      );
      try {
        const _ = await predecessor.getPredecessor(this.id);
      } catch (err) {
        handleGRPCErrors(
          "checkPredecessor",
          "getPredecessor",
          this.predecessor.host,
          this.predecessor.port,
          err
        );
        // Wipe out the predecessor if it doesn't respond
        this.predecessor = { id: null, host: null, port: null };
        return false;
      }
    }
    return true;
  }
  /**
   * Checks whether the successor is still responding.
   * @returns {boolean} true if successor was still reasonable; false otherwise.
   */
  async checkSuccessor() {
    if (DEBUGGING_LOCAL)
      console.log(
        `{${this.id}}.checkSuccessor(${this.fingerTable[0].successor.id})`
      );

    let nSuccessor = NULL_NODE;
    let successorSeemsOK = false;
    if (this.fingerTable[0].successor.id == null) {
      successorSeemsOK = false;
    } else if (this.fingerTable[0].successor.id == this.id) {
      successorSeemsOK = true;
    } else {
      try {
        // just ask anything
        nSuccessor = await this.getSuccessor(this.fingerTable[0].successor);
        successorSeemsOK = nSuccessor.id != null;
      } catch (err) {
        successorSeemsOK = false;
        console.log(
          `Error in checkSuccessor({${this.id}}) call to getSuccessor`,
          err
        );
      }
    }
    return successorSeemsOK;
  }
  /**
   * Placeholder for data migration within the join() call.
   */
  async migrateKeys() {}
}

class UserService extends ChordNode {
  constructor({ id, host, port, knownId, knownHost, knownPort }) {
    super({ id, host, port, knownId, knownHost, knownPort });
    // Extend with state
    this.userMap = {};
  }

  // gRPC handler that returns a user locally from this node
  fetch(message, callback) {
    const {
      request: { id }
    } = message;
    console.log(`Requested User ${id}`);
    if (!this.userMap[id]) {
      callback({ code: 5 }, null); // NOT_FOUND error
    } else {
      callback(null, this.userMap[id]);
    }
  }

  // Removes a User from local state
  removeUser(id) {
    if (this.userMap[id]) {
      delete this.userMap[id];
      console.log("removeUser: user removed");
      return null;
    } else {
      console.log("removeUser, user DNE");
      return { code: 5 };
    }
  }

  // gRPC Handler to allow other nodes to remove users from our local state
  async removeUserRemoteHelper(message, callback) {
    if (DEBUGGING_LOCAL) console.log("removeUserRemoteHelper: ", message);
    const err = removeUser(message.request.id);
    callback(err, {});
  }

  // Removes a User regardless of location in cluster
  async remove(message, callback) {
    const userId = message.request.id;
    let successor = NULL_NODE;
    console.log("remove: Attempting to remove user ", userId);

    try {
      successor = await this.findSuccessor(userId, this.encapsulateSelf());
    } catch (err) {
      successor = NULL_NODE;
      console.error("remove: findSuccessor failed with ", err);
    }

    if (this.iAmMyOwnSuccessor()) {
      if (DEBUGGING_LOCAL) console.log("remove: remove user from local node");
      const err = this.removeUser(userId);
      callback(err, {});
    } else {
      try {
        if (DEBUGGING_LOCAL)
          console.log("remove: remove user from remote node");
        const successorClient = caller(
          `${successor.host}:${successor.port}`,
          PROTO_PATH,
          "Node"
        );
        await successorClient.removeUserRemoteHelper(
          { id: userId },
          (err, _) => {
            callback(err, {});
          }
        );
      } catch (err) {
        handleGRPCErrors(
          "remove",
          "removeUserRemoteHelper",
          successor.host,
          successor.port,
          err
        );
        callback(err, null);
      }
    }
  }

  // Insert User in local state
  insertUser(userEdit) {
    if (DEBUGGING_LOCAL) console.log("insertUser: ", userEdit);
    const user = userEdit.user;
    const edit = userEdit.edit;
    if (this.userMap[user.id] && !edit) {
      console.log(`insertUser: ${user.id} already exits`);
      return { code: 6 };
    } else {
      this.userMap[user.id] = user;
      if (edit) {
        console.log(`insertUser: Edited User ${user.id}`);
      } else {
        console.log(`insertUser: Inserted User ${user.id}`);
      }
      return null;
    }
  }

  // gRPC Handler to allow other nodes to insert users into our local state
  async insertUserRemoteHelper(message, callback) {
    if (DEBUGGING_LOCAL) console.log("insertUserRemoteHelper: ", message);
    const err = this.insertUser(message.request);
    callback(err, {});
  }

  // Inserts a User regardless of location in cluster
  async insert(message, callback) {
    const userEdit = message.request;
    const user = userEdit.user;
    const lookupKey = user.id;
    let successor = NULL_NODE;

    console.log(`insert: Attempting to insert user`, user.id);
    if (DEBUGGING_LOCAL) console.log(user);
    try {
      successor = await this.findSuccessor(lookupKey, this.encapsulateSelf());
    } catch (err) {
      successor = NULL_NODE;
      console.error("insert: findSuccessor failed with ", err);
    }

    if (this.iAmMyOwnSuccessor()) {
      if (DEBUGGING_LOCAL) console.log("insert: insert user to local node");
      const err = this.insertUser(userEdit);
      callback(err, {});
    } else {
      try {
        console.log("insert: insert user to remote node", lookupKey);
        const successorClient = caller(
          `${successor.host}:${successor.port}`,
          PROTO_PATH,
          "Node"
        );
        await successorClient.insertUserRemoteHelper(userEdit, (err, _) => {
          console.log("insert finishing");
          callback(err, {});
        });
      } catch (err) {
        handleGRPCErrors(
          "insert",
          "insertUser",
          successor.host,
          successor.port,
          err
        );
        callback(err, null);
      }
    }
  }

  // gRPC handler that returns a user locally from this node
  fetch(message, callback) {
    const {
      request: { id }
    } = message;
    console.log(`Requested User ${id}`);
    if (!this.userMap[id]) {
      callback({ code: 5 }, null); // NOT_FOUND error
    } else {
      callback(null, this.userMap[id]);
    }
  }

  lookupUser(userId) {
    if (this.userMap[userId]) {
      const user = this.userMap[userId];
      if (DEBUGGING_LOCAL) console.log(`User found ${user.id}`);
      return { err: null, user };
    } else {
      if (DEBUGGING_LOCAL) console.log(`User with user ID ${userId} not found`);
      return { err: { code: 5 }, user: null };
    }
  }

  async lookupUserRemoteHelper(message, callback) {
    console.log("beginning lookupUserRemoteHelper: ", message.request.id);
    const { err, user } = lookupUser(message.request.id);
    console.log("finishing lookupUserRemoteHelper: ", user);
    callback(err, user);
  }
  async lookup(message, callback) {
    const userId = message.request.id;
    console.log(`lookup: Looking up user ${userId}`);
    const lookupKey = userId;
    let successor = NULL_NODE;

    try {
      successor = await this.findSuccessor(lookupKey, this.encapsulateSelf());
    } catch (err) {
      successor = NULL_NODE;
      console.error("lookup: findSuccessor failed with ", err);
    }

    if (this.iAmMyOwnSuccessor()) {
      if (DEBUGGING_LOCAL) console.log("lookup: lookup user to local node");
      const { err, user } = this.lookupUser(userId);
      if (DEBUGGING_LOCAL)
        console.log(
          "lookup: finished Server-side lookup, returning: ",
          err,
          user
        );
      callback(err, user);
    } else {
      try {
        console.log("In lookup: lookup user to remote node");
        const successorClient = caller(
          `${successor.host}:${successor.port}`,
          PROTO_PATH,
          "Node"
        );
        const user = await successorClient.lookupUserRemoteHelper({
          id: userId
        });
        callback(null, user);
      } catch (err) {
        handleGRPCErrors(
          "lookup",
          "lookupUserRemotehelper",
          successor.host,
          successor.port,
          err
        );
        callback(err, null);
      }
    }
  }
}

async function endpointIsResponsive(host, port) {
  const client = caller(`${host}:${port}`, PROTO_PATH, "Node");
  try {
    const _ = await client.summary(this.id);
    return true;
  } catch (err) {
    handleGRPCErrors("endpointIsResponsive", "summary", host, port, err);
    return false;
  }
}

async function hashDryRun(sourceValue) {
  try {
    const integerHash = await computeIntegerHash(sourceValue, HASH_BIT_LENGTH);
    console.log(`ID {${integerHash}} computed from hash of {${sourceValue}}`);
  } catch (err) {
    console.error(
      `Error computing hash of ${sourceValue}. Thus, terminating...\n`,
      err
    );
    return -13;
  }
  return 0;
}

let node;

/**
 * Starts an RPC server that receives requests for the Greeter service at the
 * sample server port
 *
 * Takes the following mandatory flags
 * --host       - This node's host name
 * --port       - This node's TCP Port
 * --knownId   - The ID of a node in the cluster
 * --knownHost   - The host name of a node in the cluster
 * --knownPort - The TCP Port of a node in the cluster
 *
 * And takes the following optional flags
 * --id         - This node's id
 */
async function main() {
  const args = minimist(process.argv.slice(2));

  if (args.hashOnly) {
    const rc = await hashDryRun(args.hashOnly);
    process.exit(rc);
  }

  // bail immediately if knownHost can't be reached
  if (
    nodesAreNotIdentical(args.host, args.port, args.knownHost, args.knownPort)
  ) {
    if (!(await endpointIsResponsive(args.knownHost, args.knownPort))) {
      console.error(
        `${args.knownHost}:${args.knownPort} is not responsive. Exiting`
      );
      process.exit();
    } else {
      console.log(`${args.knownHost}:${args.knownPort} responded`);
    }
  }

  // protect against bad ID inputs
  if (args.id && args.id > 2 ** HASH_BIT_LENGTH - 1) {
    console.error(
      `Error. Bad ID {${args.id}} > 2^m-1 {${2 ** HASH_BIT_LENGTH -
        1}}. Terminating...\n`
    );
    return -13;
  }

  // protect against bad Known ID inputs
  if (args.knownId && args.knownId > 2 ** HASH_BIT_LENGTH - 1) {
    console.error(
      `Error. Bad known ID {${args.knownId}} > 2^m-1 {${2 ** HASH_BIT_LENGTH -
        1}}. Thus, terminating...\n`
    );
    return -13;
  }

  try {
    node = new UserService(args);
    const server = new grpc.Server();
    server.addService(chord.Node.service, {
      summary: node.summary.bind(node),
      fetch: node.fetch.bind(node),
      remove: node.remove.bind(node),
      removeUserRemoteHelper: node.removeUserRemoteHelper.bind(node),
      insert: node.insert.bind(node),
      insertUserRemoteHelper: node.insertUserRemoteHelper.bind(node),
      lookup: node.lookup.bind(node),
      lookupUserRemoteHelper: node.lookupUserRemoteHelper.bind(node),
      findSuccessorRemoteHelper: node.findSuccessorRemoteHelper.bind(node),
      getSuccessorRemoteHelper: node.getSuccessorRemoteHelper.bind(node),
      getPredecessor: node.getPredecessor.bind(node),
      setPredecessor: node.setPredecessor.bind(node),
      closestPrecedingFingerRemoteHelper: node.closestPrecedingFingerRemoteHelper.bind(
        node
      ),
      updateFingerTable: node.updateFingerTable.bind(node),
      notify: node.notify.bind(node)
    });
    console.log(`Serving on ${args.host}:${args.port}`);
    server.bind(
      `0.0.0.0:${args.port}`,
      grpc.ServerCredentials.createInsecure()
    );
    server.start();
  } catch (err) {
    console.error(err);
    process.exit();
  }
}

main();
