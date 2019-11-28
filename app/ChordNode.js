const process = require("process");
const {
  connect,
  isInModuloRange,
  computeHostPortHash,
  handleGRPCErrors,
  DEBUGGING_LOCAL,
  HASH_BIT_LENGTH,
  SUCCESSOR_TABLE_MAX_LENGTH,
  NULL_NODE
} = require("./utils.js");

class ChordNode {
  constructor({ id, host, port }) {
    if (!host || !port) {
      console.error(
        "ChordNode constructor did not receive host or port as expected"
      );
      process.exit(-9);
    }
    this.id = id;
    this.host = host;
    this.port = port;

    this.fingerTable = [
      {
        start: null,
        successor: NULL_NODE
      }
    ];
    this.successorTable = [NULL_NODE];
    //TBD 20191127this.predecessor = { id: null, host: null, port: null };
    this.predecessor = NULL_NODE;
  }

  iAmTheSuccessor(successor) {
    return this.id == successor.id;
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
    console.log("Summary: Predecessor: ", this.predecessor);
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
      const nodeQueriedClient = connect(nodeQueried);
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
      const nodeQueriedClient = connect(nodeQueried);
      try {
        nSuccessor = await nodeQueriedClient.getSuccessorRemoteHelper(
          nodeQueried
        );
      } catch (err) {
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
      const nodeQueriedClient = connect(nodeQueried);
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
    callback(null, this.predecessor);
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
        this.predecessor
      );
    }

    this.predecessor = message.request; //message.request is node

    if (DEBUGGING_LOCAL)
      console.log(
        "setPredecessor: Self's new predecessor = ",
        this.predecessor
      );

    callback(null, {});
  }

  async joinCluster(knownNode) {
    let errorString = null;
    let knownNodeId = null;
    let possibleCollidingNode = NULL_NODE;

    // Generate the for this node ID from the host connection strings if not already forced by user
    if (!this.id) {
      this.id = await computeHostPortHash(this.host, this.port);
    }

    // initialize finger table with reasonable values
    this.fingerTable.pop();
    for (let i = 0; i < HASH_BIT_LENGTH; i++) {
      this.fingerTable.push({
        start: (this.id + 2 ** i) % 2 ** HASH_BIT_LENGTH,
        successor: this.encapsulateSelf()
      });
    }

    // join a chord or create a new one
    if (
      `${this.host}:${this.port}`.toLowerCase() ===
      `${knownNode.host}:${knownNode.port}`.toLowerCase()
    ) {
      // this is the first node in a new cluster
      this.predecessor = this.encapsulateSelf();
    } else if (await this.confirmExist(knownNode)) {
      // joining an existing chord so
      // + get the known node's ID
      try {
        knownNodeId = await this.getNodeId(knownNode);
        knownNode.id = knownNodeId;
      } catch (err) {
        knownNode.id = null;
        handleGRPCErrors(
          "joinCluster",
          "getNodeId",
          knownNode.host,
          knownNode.port,
          err
        );
      }
      // then check for a collision between the ID intended for this new node and an existing node
      try {
        possibleCollidingNode = await this.findSuccessor(this.id, knownNode);
      } catch (err) {
        possibleCollidingNode = null;
      }
      if (this.id == possibleCollidingNode.id) {
        // node collision
        errorString = `Error joining node "${this.host}:${this.port}" with ID {${this.id}} to node "${knownNode.host}:${knownNode.port}" because of a collision with node "${possibleCollidingNode.host}:${possibleCollidingNode.port}" having ID={${possibleCollidingNode.id}}.`;
        if (DEBUGGING_LOCAL) {
          console.log(errorString);
        }
        throw new RangeError(errorString);
      }
      // now go ahead with the join
      await this.initFingerTable(knownNode);
      await this.updateOthers();
    } else {
      // the node doesn't exist so exit on error
      errorString = `Error joining node "${this.host}:${this.port}" to node "${knownNode.host}:${knownNode.port}" because the latter can't be confirmed to exist.\n`;
      if (DEBUGGING_LOCAL) {
        console.log(errorString);
      }
      throw new RangeError(errorString);
    }

    // initialize successor table
    this.successorTable[0] = this.fingerTable[0].successor;

    try {
      if (DEBUGGING_LOCAL) console.log("join: calling migrateKys");
      await this.migrateKeysAfterJoin();
    } catch (error) {
      console.error("Migrate keys failed with error:", error);
    }

    // And now that we've joined a cluster, we need maintain our state
    // There might be some "critical section" type issues
    // we need to use a gate to protect in these functions
    setInterval(this.stabilize.bind(this), 1000);
    setInterval(this.fixFingers.bind(this), 3000);
    setInterval(this.checkPredecessor.bind(this), 1000);

    if (DEBUGGING_LOCAL) {
      console.log(">>>>>     joinCluster          ");
      console.log(
        `The fingerTable[] leaving {${this.id}}.joinCluster(${knownNode.id}) is:\n`,
        this.fingerTable
      );
      console.log(
        `The {${this.id}}.predecessor leaving joinCluster() is ${this.predecessor.id}`
      );
      console.log("          joinCluster     <<<<<\n");
    }
  }

  /**
   * Determine whether a node exists by asking for its ID.
   *
   * @param knownNode   - node defined as {id, host, port}
   * @returns {boolean} - true if the node has a valid ID
   */
  async confirmExist(knownNode) {
    let nodeId = null;
    let nodeExists = false;
    try {
      nodeId = await this.getNodeId(knownNode);
    } catch (err) {
      nodeId = null;
      console.error(
        `Error confirming existence of node "${knownNode.host}:${knownNode.port}"\n`,
        err
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
   * @param knownNode   - node defined as {id, host, port}
   * @returns {number}  - node's ID, or null if error
   */
  async getNodeId(knownNode) {
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
        knownNodeObject = await knownNodeClient.getNodeIdRemoteHelper(
          knownNode
        );
        nodeId = knownNodeObject.id;
      } catch (err) {
        nodeId = null;
        if (DEBUGGING_LOCAL) {
          console.error(
            `Error getting ID of node "${knownNode.host}:${knownNode.port}"\n`,
            err
          );
        }
      }
    }
    return nodeId;
  }

  /**
   * RPC to return the node's ID.
   *
   * @param {any} _           - unused dummy argument
   * @param {string} callback - grpc callback
   * @returns {number}        - node's ID, or null if error
   */
  async getNodeIdRemoteHelper(_, callback) {
    callback(null, { id: this.id, host: this.host, port: this.port });
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

    let successorClient = connect(this.fingerTable[0].successor);
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
      console.log("initFingerTable: predecessor  ", this.predecessor);

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
        pNodeClient = connect(pNode);
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
      const pClient = connect(this.predecessor);
      try {
        await pClient.updateFingerTable({ node: sNode, index: fingerIndex });
      } catch (err) {
        handleGRPCErrors(
          "updateFingerTable",
          "updateFingerTable",
          this.predecessor.host,
          this.predecessor.port,
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
        `updateSuccessorTable: {${this.id}}.successorTable[] =\n`,
        this.successorTable
      );
      console.log(
        `updateSuccessorTable: successor node id = ${this.fingerTable[0].successor.id}`
      );
    }

    // check whether the successor is available
    let successorSeemsOK = false;
    try {
      successorSeemsOK = await this.isOkSuccessor();
    } catch (err) {
      successorSeemsOK = false;
      console.error(`updateSuccessorTable: isOkSuccessor failed with `, err);
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
          console.error(
            `updateSuccessorTable: isOkSuccessor failed with `,
            err
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
    if (this.successorTable.length < 1) {
      // this node is isolated
      this.successorTable.push({
        id: this.id,
        host: this.host,
        port: this.port
      });
      // update the finger table accordingly
      this.fingerTable[0].successor = this.successorTable[0];
    }
    // try to bulk up the table
    let successorSuccessor = NULL_NODE;
    if (
      this.successorTable.length < SUCCESSOR_TABLE_MAX_LENGTH &&
      this.id !== this.fingerTable[0].successor
    ) {
      if (DEBUGGING_LOCAL) {
        console.log(
          `updateSuccessorTable: Short successorTable[]: [ current length ${this.successorTable.length} ] < [ ${SUCCESSOR_TABLE_MAX_LENGTH} preferred length ]`
        );
      }
      for (
        let i = 0;
        i < this.successorTable.length && i <= SUCCESSOR_TABLE_MAX_LENGTH;
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
            `updateSuccessorTable: {${this.id}}.successorTable[${i}] = ${this.successorTable[i].id} and {${this.successorTable[i].id}}.successor[0] = ${successorSuccessor.id}`
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
    while (
      (!successorSeemsOK ||
        this.successorTable.length > SUCCESSOR_TABLE_MAX_LENGTH) &&
      i > 0
    ) {
      try {
        successorSeemsOK = await this.confirmExist(this.successorTable[i]);
      } catch (err) {
        console.error(
          `updateSuccessorTable: call to confirmExist failed with `,
          err
        );
        successorSeemsOK = false;
      }
      if (!successorSeemsOK || i >= SUCCESSOR_TABLE_MAX_LENGTH) {
        // remove successor candidate
        this.successorTable.pop();
      }
      i -= 1;
    }

    if (DEBUGGING_LOCAL)
      console.log(
        `updateSuccessorTable: new {${this.id}}.successorTable[] =\n`,
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
    try {
      successorClient = connect(this.fingerTable[0].successor);
    } catch (err) {
      console.error(
        `stabilize: call to connect {${this.fingerTable[0].successor.id}} failed with `,
        err
      );
      return false;
    }
    if (this.iAmMyOwnSuccessor()) {
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
        `\nstabilize: leaving stabilize()`,
        `\n\t{${this.id}}.predecessor = ${this.predecessor.id}`,
        `\n\t{${this.id}}.fingerTable[] is:\n${this.fingerTable}`,
        `\n\t{${this.id}}.successorTable[] is:\n${this.successorTable}\n`
      );
    }

    if (this.id !== this.fingerTable[0].successor.id) {
      try {
        successorClient = connect(this.fingerTable[0].successor);
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

    console.log(`--\n{${this.id}}.predecessor = ${this.predecessor.id}`);
    console.log(`{${this.id}}.successor = ${this.fingerTable[0].successor.id}`);
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
        this.fingerTable[0].successor = this.predecessor;
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
        `fixFingers: fingerTable[${randomId}] = ${this.fingerTable[randomId].successor}`
      );
    }
  }
  /**
   * Checks to make sure that the predecessor is still responsive
   */
  async checkPredecessor() {
    if (this.predecessor.id !== null && !this.iAmMyOwnPredecessor()) {
      const predecessorClient = connect(this.predecessor);
      try {
        const _ = await predecessorClient.getPredecessor(this.id);
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
   */
  async isOkSuccessor() {
    if (DEBUGGING_LOCAL)
      console.log(
        `{${this.id}}.isOkSuccessor(${this.fingerTable[0].successor.id})`
      );

    let nSuccessor = NULL_NODE;
    let successorSeemsOK = false;
    if (this.fingerTable[0].successor.id == null) {
      successorSeemsOK = false;
    } else if (this.iAmMyOwnSuccessor()) {
      successorSeemsOK = true;
    } else {
      try {
        // just ask anything
        successorSeemsOK = await this.confirmExist(
          this.fingerTable[0].successor
        );
      } catch (err) {
        successorSeemsOK = false;
        console.log(
          `Error in isOkSuccessor({${this.id}}) call to getSuccessor`,
          err
        );
      }
    }
    return successorSeemsOK;
  }
  /**
   * Placeholder for data migration within the joinCluster() call.
   */
  async migrateKeysAfterJoin() {
    throw new Error("Method migrateKeysAfterJoin has not been implemented");
  }
}

module.exports = {
  ChordNode
};
