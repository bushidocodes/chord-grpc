const os = require("os");
const path = require("path");
const grpc = require("grpc");
const caller = require("grpc-caller");
const protoLoader = require("@grpc/proto-loader");
const minimist = require("minimist");
const { isInModuloRange, computeIntegerHash } = require("./utils.js");

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
const userMap = {};
const NULL_NODE = { id: null, host: null, port: null };
let fingerTable = [
  {
    start: null,
    successor: NULL_NODE
  }
];
let successorTable = [NULL_NODE];
let _self = NULL_NODE;
let predecessor = NULL_NODE;

/**
 * Print Summary of state of node
 * @param userRequestObject
 * @param callback gRPC callback
 */
function summary(_, callback) {
  console.log("Summary: fingerTable: \n", fingerTable);
  console.log("Summary: Predecessor: ", predecessor);
  callback(null, _self);
}

const iAmMyOwnSuccessor = () => _self.id == successor.id;
const iAmMyOwnPredecessor = () => _self.id == predecessor.id;

/**
 * Fetch a user
 * @param userRequestObject
 * @param callback gRPC callback
 */
function fetch({ request: { id } }, callback) {
  console.log(`Requested User ${id}`);
  if (!userMap[id]) {
    callback({ code: 5 }, null); // NOT_FOUND error
  } else {
    callback(null, userMap[id]);
  }
}

/**
 * remove a user
 * @param grpcRequest
 * @param callback gRPC callback
 */
async function remove(message, callback) {
  const userId = message.request.id;
  let successor = NULL_NODE;

  console.log("remove: userId", userId);

  try {
    successor = await findSuccessor(userId, _self, _self);
  } catch (err) {
    successor = NULL_NODE;
    console.error("remove: findSuccessor failed with ", err);
  }

  if (iAmMyOwnSuccessor()) {
    console.log("remove: remove user from local node");
    const err = removeUser(userId);
    callback(err, {});
  } else {
    try {
      console.log("remove: remove user from remote node");
      const successorClient = caller(
        `${successor.host}:${successor.port}`,
        PROTO_PATH,
        "Node"
      );
      await successorClient.removeUserRemoteHelper({ id: userId }, (err, _) => {
        callback(err, {});
      });
    } catch (err) {
      console.error("remove: removeUserRemoteHelper failed with ", err);
      callback(err, null);
    }
  }
}

async function removeUserRemoteHelper(message, callback) {
  if (DEBUGGING_LOCAL) console.log("removeUserRemoteHelper: ", message);
  const err = removeUser(message.request.id);
  callback(err, {});
}

function removeUser(id) {
  if (userMap[id]) {
    delete userMap[id];
    console.log("removeUser: user removed");
    return null;
  } else {
    console.log("removeUser, user DNE");
    return { code: 5 };
  }
}

/**
 * Insert a user
 * @param grpcRequest
 * @param callback gRPC callback
 */
async function insert(message, callback) {
  const userEdit = message.request;
  const user = userEdit.user;
  const lookupKey = user.id;
  let successor = NULL_NODE;

  console.log(`insert: Attempting to insert`, user);

  try {
    successor = await findSuccessor(lookupKey, _self, _self);
  } catch (err) {
    successor = NULL_NODE;
    console.error("insert: findSuccessor failed with ", err);
  }

  if (iAmMyOwnSuccessor()) {
    console.log("insert: insert user to local node");
    const err = insertUser(userEdit);
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
      console.error("insert call to insertUser failed with ", err);
      callback(err, null);
    }
  }
}

async function insertUserRemoteHelper(message, callback) {
  if (DEBUGGING_LOCAL) console.log("insertUserRemoteHelper: ", message);
  const err = insertUser(message.request);
  callback(err, {});
}

function insertUser(userEdit) {
  console.log("insertUser: ", userEdit);
  const user = userEdit.user;
  const edit = userEdit.edit;
  if (userMap[user.id] && !edit) {
    console.log(`insertUser: ${user.id} already exits`);
    return { code: 6 };
  } else {
    userMap[user.id] = user;
    console.log(`insertUser: Inserted User ${user.id}:`);
    return null;
  }
}

/**
 * Insert a user
 * @param grpcRequest
 * @param callback gRPC callback
 */
async function lookup(message, callback) {
  const userId = message.request.id;
  console.log(`lookup: Looking up user ${userId}`);
  const lookupKey = userId;
  let successor = NULL_NODE;

  try {
    successor = await findSuccessor(lookupKey, _self, _self);
  } catch (err) {
    successor = NULL_NODE;
    console.error("lookup: findSuccessor failed with ", err);
  }

  if (iAmMyOwnSuccessor()) {
    console.log("lookup: lookup user to local node");
    const { err, user } = lookupUser(userId);
    console.log("lookup: finished Server-side lookup, returning: ", err, user);
    callback(err, user);
  } else {
    try {
      console.log("In lookup: lookup user to remote node");
      const successorClient = caller(
        `${successor.host}:${successor.port}`,
        PROTO_PATH,
        "Node"
      );
      const user = await successorClient.lookupUserRemoteHelper({ id: userId });
      callback(null, user);
    } catch (err) {
      console.error("lookup: call to lookupUser failed with ", err);
      callback(err, null);
    }
  }
}

async function lookupUserRemoteHelper(message, callback) {
  console.log("beginning lookupUserRemoteHelper: ", message.request.id);
  const { err, user } = lookupUser(message.request.id);
  console.log("finishing lookupUserRemoteHelper: ", user);
  callback(err, user);
}

function lookupUser(userId) {
  if (userMap[userId]) {
    const user = userMap[userId];
    console.log(`User found ${user.id}`);
    return { err: null, user };
  } else {
    console.log(`User with user ID ${userId} not found`);
    return { err: { code: 5 }, user: null };
  }
}

/**
 * Directly implement the pseudocode's findSuccessor() method.
 *
 * However, it is able to discern whether to do a local lookup or an RPC.
 * If the querying node is the same as the queried node, it will stay local.
 *
 * @param {number} id value being searched
 * @param nodeQuerying node initiating the query
 * @param nodeQueried node being queried for the ID
 * @returns id.successor
 *
 */
async function findSuccessor(id, nodeQuerying, nodeQueried) {
  let nPrime = NULL_NODE;
  let nPrimeSuccessor = NULL_NODE;

  if (DEBUGGING_LOCAL)
    console.log(
      `findSuccessor: Node querying {${nodeQuerying.id}}; node queried {${nodeQueried.id}}.`
    );

  if (nodeQuerying.id == nodeQueried.id) {
    try {
      nPrime = await findPredecessor(id);
    } catch (err) {
      console.error(`findSuccessor: findPredecessor failed with `, err);
      nPrime = NULL_NODE;
    }

    if (DEBUGGING_LOCAL) console.log("findSuccessor: n' is ", nPrime.id);

    try {
      nPrimeSuccessor = await getSuccessor(_self, nPrime);
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
      console.error(
        "findSuccessor: call to findSuccessorRemoteHelper failed with",
        err
      );
    }
  }

  if (DEBUGGING_LOCAL)
    console.log("findSuccessor: departing n'.successor = ", nPrimeSuccessor.id);
  return nPrimeSuccessor;
}

/**
 * RPC equivalent of the pseudocode's findSuccessor() method.
 * It is implemented as simply a wrapper for the local findSuccessor() method.
 *
 * @param idAndNodeQueried {id:, node:}, where ID is the key sought
 * @param callback grpc callback function
 */
async function findSuccessorRemoteHelper(idAndNodeQueried, callback) {
  const id = idAndNodeQueried.request.id;
  const nodeQueried = idAndNodeQueried.request.node;

  if (DEBUGGING_LOCAL)
    console.log(
      `findSuccessorRemoteHelper: id = ${id} nodeQueried = ${nodeQueried.id}.`
    );

  let nPrimeSuccessor = NULL_NODE;
  try {
    nPrimeSuccessor = await findSuccessor(id, _self, nodeQueried);
  } catch (err) {
    console.error("findSuccessorRemoteHelper: findSuccessor failed with", err);
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
async function findPredecessor(id) {
  if (DEBUGGING_LOCAL) console.log("findPredecessor: id = ", id);

  let nPrime = _self;
  let nPrimeSuccessor = NULL_NODE;
  try {
    nPrimeSuccessor = await getSuccessor(_self, nPrime);
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
      nPrime = await closestPrecedingFinger(id, _self, nPrime);
    } catch (err) {
      nPrime = NULL_NODE;
    }

    if (DEBUGGING_LOCAL)
      console.log(
        `findPredecessor: At iterator ${iterationCounter} nPrime = ${nPrime}`
      );

    try {
      nPrimeSuccessor = await getSuccessor(_self, nPrime);
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
 * @param nodeQuerying
 * @param nodeQueried
 * @returns : the successor if the successor seems valid, or a null node otherwise
 */
async function getSuccessor(nodeQuerying, nodeQueried) {
  if (DEBUGGING_LOCAL)
    console.log(
      `getSuccessor: {${nodeQuerying.id}}.getSuccessor(${nodeQueried.id})`
    );

  // get n.successor either locally or remotely
  let nSuccessor = NULL_NODE;
  if (nodeQuerying.id == nodeQueried.id) {
    // use local value
    nSuccessor = fingerTable[0].successor;
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
      console.trace("getSuccessor: Remote error", err);
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
async function getSuccessorRemoteHelper(_, callback) {
  callback(null, fingerTable[0].successor);
}

/**
 * Directly implement the pseudocode's closestPrecedingFinger() method.
 *
 * However, it is able to discern whether to do a local lookup or an RPC.
 * If the querying node is the same as the queried node, it will stay local.
 *
 * @param id
 * @param nodeQuerying
 * @param nodeQueried
 * @returns the closest preceding node to ID
 *
 */
async function closestPrecedingFinger(id, nodeQuerying, nodeQueried) {
  let nPreceding = NULL_NODE;
  if (nodeQuerying.id == nodeQueried.id) {
    // use local value
    for (let i = HASH_BIT_LENGTH - 1; i >= 0; i--) {
      if (
        isInModuloRange(
          fingerTable[i].successor.id,
          nodeQueried.id,
          false,
          id,
          false
        )
      ) {
        nPreceding = fingerTable[i].successor;
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
      nPreceding = await nodeQueriedClient.closestPrecedingFingerRemoteHelper({
        id: id,
        node: nodeQueried
      });
    } catch (err) {
      nPreceding = NULL_NODE;
      console.error(
        "closestPrecedingFinger: closestPrecedingFingerRemoteHelper failed with ",
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
async function closestPrecedingFingerRemoteHelper(idAndNodeQueried, callback) {
  const id = idAndNodeQueried.request.id;
  const nodeQueried = idAndNodeQueried.request.node;
  let nPreceding = NULL_NODE;
  try {
    nPreceding = await closestPrecedingFinger(id, _self, nodeQueried);
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
async function getPredecessor(_, callback) {
  callback(null, predecessor);
}

/**
 * RPC to replace the value of the node's predecessor.
 *
 * @param message is a node object
 * @param callback
 */
async function setPredecessor(message, callback) {
  if (DEBUGGING_LOCAL) {
    console.log("setPredecessor: Self = ", _self);
    console.log("setPredecessor: Self's original predecessor = ", predecessor);
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
async function join(knownNode) {
  // remove dummy template initializer from table
  fingerTable.pop();
  // initialize table with reasonable values
  for (let i = 0; i < HASH_BIT_LENGTH; i++) {
    fingerTable.push({
      start: (_self.id + 2 ** i) % 2 ** HASH_BIT_LENGTH,
      successor: _self
    });
  }

  if (knownNode.id && confirmExist(knownNode)) {
    await initFingerTable(knownNode);
    await updateOthers();
  } else {
    // this is the first node
    predecessor = _self;
  }

  await migrateKeys();

  // initialize successor table
  successorTable[0] = fingerTable[0].successor;

  if (DEBUGGING_LOCAL) {
    console.log(">>>>>     join          ");
    console.log(
      `The fingerTable[] leaving {${_self.id}}.join(${knownNode.id}) is:\n`,
      fingerTable
    );
    console.log(
      `The {${_self.id}}.predecessor leaving join() is ${predecessor}`
    );
    console.log("          join     <<<<<\n");
  }
}

/**
 * Determine whether a node exists by pinging it.
 * @param knownNode: knownNode structure; e.g., {id, host, port}
 * @returns {boolean}
 */
function confirmExist(knownNode) {
  return !(_self.id == knownNode.id);
}

/**
 * Directly implement the pseudocode's initFingerTable() method.
 * @param nPrime
 */
async function initFingerTable(nPrime) {
  if (DEBUGGING_LOCAL) {
    console.log(
      `initFingerTable: self = ${_self.id}; self.successor = ${fingerTable[0].successor.id}; finger[0].start = ${fingerTable[0].start} n' = ${nPrime.id}`
    );
  }

  let nPrimeSuccessor = NULL_NODE;
  try {
    nPrimeSuccessor = await findSuccessor(fingerTable[0].start, _self, nPrime);
  } catch (err) {
    nPrimeSuccessor = NULL_NODE;
    console.error("initFingerTable: findSuccessor failed with ", err);
  }
  fingerTable[0].successor = nPrimeSuccessor;

  if (DEBUGGING_LOCAL)
    console.log(
      "initFingerTable: n'.successor (now  self.successor) = ",
      nPrimeSuccessor
    );

  let successorClient = caller(
    `${fingerTable[0].successor.host}:${fingerTable[0].successor.port}`,
    PROTO_PATH,
    "Node"
  );
  try {
    predecessor = await successorClient.getPredecessor(
      fingerTable[0].successor
    );
  } catch (err) {
    predecessor = NULL_NODE;
    console.error("initFingerTable: getPredecessor failed with", err);
  }
  try {
    await successorClient.setPredecessor(_self);
  } catch (err) {
    console.error("initFingerTable: setPredecessor() failed with ", err);
  }

  if (DEBUGGING_LOCAL)
    console.log("initFingerTable: predecessor  ", predecessor);

  for (let i = 0; i < HASH_BIT_LENGTH - 1; i++) {
    if (
      isInModuloRange(
        fingerTable[i + 1].start,
        _self.id,
        true,
        fingerTable[i].successor.id,
        false
      )
    ) {
      fingerTable[i + 1].successor = fingerTable[i].successor;
    } else {
      try {
        fingerTable[i + 1].successor = await findSuccessor(
          fingerTable[i + 1].start,
          _self,
          nPrime
        );
      } catch (err) {
        fingerTable[i + 1].successor = NULL_NODE;
        console.error("initFingerTable: findSuccessor() failed with ", err);
      }
    }
  }
  if (DEBUGGING_LOCAL)
    console.log("initFingerTable: fingerTable[] =\n", fingerTable);
}

/**
 * Directly implement the pseudocode's updateOthers() method.
 */
async function updateOthers() {
  if (DEBUGGING_LOCAL) console.log("updateOthers: _self = ", _self);
  let pNodeSearchID, pNodeClient;
  let pNode = NULL_NODE;

  for (let i = 0; i < HASH_BIT_LENGTH; i++) {
    pNodeSearchID =
      (_self.id - 2 ** i + 2 ** HASH_BIT_LENGTH) % 2 ** HASH_BIT_LENGTH;
    if (DEBUGGING_LOCAL)
      console.log(
        `updateOthers: i = ${i}; findPredecessor(${pNodeSearchID}) --> pNode`
      );

    try {
      pNode = await findPredecessor(pNodeSearchID);
    } catch (err) {
      pNode = NULL_NODE;
      console.error(
        `updateOthers: Error from findPredecessor(${pNodeSearchID}) in updateOthers().`,
        err
      );
    }

    if (DEBUGGING_LOCAL) console.log("updateOthers: pNode = ", pNode);

    if (_self.id !== pNode.id) {
      pNodeClient = caller(`${pNode.host}:${pNode.port}`, PROTO_PATH, "Node");
      try {
        await pNodeClient.updateFingerTable({ node: _self, index: i });
      } catch (err) {
        console.error("updateOthers: client.updateFingerTable error ", err);
      }
    }
  }
}

/**
 * RPC that directly implements the pseudocode's updateFingerTable() method.
 * @param message - consists of {sNode, fingerIndex} *
 * @param callback - grpc callback
 */
async function updateFingerTable(message, callback) {
  const sNode = message.request.node;
  const fingerIndex = message.request.index;

  if (DEBUGGING_LOCAL) {
    console.log(
      `updateFingerTable: {${_self.id}}.fingerTable[] =\n`,
      fingerTable
    );
    console.log(
      `updateFingerTable: sNode = ${message.request.node.id}; fingerIndex =${fingerIndex}`
    );
  }

  if (
    isInModuloRange(
      sNode.id,
      _self.id,
      true,
      fingerTable[fingerIndex].successor.id,
      false
    )
  ) {
    fingerTable[fingerIndex].successor = sNode;
    const pClient = caller(
      `${predecessor.host}:${predecessor.port}`,
      PROTO_PATH,
      "Node"
    );
    try {
      await pClient.updateFingerTable({ node: sNode, index: fingerIndex });
    } catch (err) {
      console.error(
        `updateFingerTable: Error updating the finger table of {${sNode.id}}.\n\n`,
        err
      );
    }

    if (DEBUGGING_LOCAL)
      console.log(
        `updateFingerTable: Updated {${_self.id}}.fingerTable[${fingerIndex}] to ${sNode}`
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
async function updateSuccessorTable() {
  if (DEBUGGING_LOCAL) {
    console.log(
      `{updateSuccessorTable: ${_self.id}}.successorTable[] =\n`,
      successorTable
    );
    console.log(
      `updateSuccessorTable: successor node id = ${fingerTable[0].successor.id}`
    );
  }

  // check whether the successor is available
  let successorSeemsOK = false;
  try {
    successorSeemsOK = await checkSuccessor();
  } catch (err) {
    console.error(`updateSuccessorTable: checkSuccessor failed with `, err);
    successorSeemsOK = false;
  }
  if (successorSeemsOK) {
    // synchronize immediate successor if it is valid
    successorTable[0] = fingerTable[0].successor;
  } else {
    // or prune if the successor is not valid
    while (!successorSeemsOK && successorTable.length > 0) {
      // try current successor again to account for contention or bad luck
      try {
        successorSeemsOK = await checkSuccessor();
      } catch (err) {
        console.error(`updateSuccessorTable: checkSuccessor failed with `, err);
        successorSeemsOK = false;
      }
      if (successorSeemsOK) {
        // synchronize immediate successor if it is valid
        successorTable[0] = fingerTable[0].successor;
      } else {
        // drop the first successor candidate
        successorTable.shift();
        // update the finger table to the next candidate
        fingerTable[0].successor = successorTable[0];
      }
    }
  }
  if (successorTable.length < 1) {
    // this node is isolated
    successorTable.push({ id: _self.id, host: _self.host, port: _self.port });
  }
  // try to bulk up the table
  let successorSuccessor = NULL_NODE;
  if (
    successorTable.length < HASH_BIT_LENGTH &&
    _self.id !== fingerTable[0].successor
  ) {
    if (DEBUGGING_LOCAL) {
      console.log(
        `updateSuccessorTable: Short successorTable[]: prefer length ${HASH_BIT_LENGTH} but actual length is ${successorTable.length}.`
      );
    }
    for (let i = 0; i < successorTable.length && i <= HASH_BIT_LENGTH; i++) {
      try {
        successorSuccessor = await getSuccessor(_self, successorTable[i]);
      } catch (err) {
        console.error(`updateSuccessorTable: getSuccessor failed with `, err);
        successorSuccessor = { id: null, host: null, port: null };
      }
      if (DEBUGGING_LOCAL)
        console.log(
          `updateSuccessorTable: {${_self.id}}.st[${i}] = ${successorTable[i].id}; {${successorTable[i].id}}.successor[0] = ${successorSuccessor.id}`
        );

      if (
        successorSuccessor.id !== null &&
        !isInModuloRange(
          successorSuccessor.id,
          _self.id,
          true,
          successorTable[i].id,
          true
        )
      ) {
        // append the additional value
        successorTable.splice(i + 1, 1, successorSuccessor);
        successorSeemsOK = true;
      }
    }
  }
  // prune from the bottom
  let i = successorTable.length - 1;
  successorSeemsOK = false;
  successorSuccessor = { id: null, host: null, port: null };
  while (
    (!successorSeemsOK || successorTable.length > HASH_BIT_LENGTH) &&
    i > 0
  ) {
    try {
      successorSuccessor = await getSuccessor(_self, successorTable[i]);
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
      successorTable.pop();
    }
    i -= 1;
  }

  if (DEBUGGING_LOCAL)
    console.log(
      `updateSuccessorTable: New {${_self.id}}.successorTable[] =\n`,
      successorTable
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
 *  @returns {boolean}
 */
async function stabilize() {
  let successorClient, x;
  try {
    successorClient = caller(
      `${fingerTable[0].successor.host}:${fingerTable[0].successor.port}`,
      PROTO_PATH,
      "Node"
    );
  } catch {
    console.error(`stabilize: call to caller failed with `, err);
    return false;
  }
  if (fingerTable[0].successor.id == _self.id) {
    // use local value
    await stabilizeSelf();
    x = _self;
  } else {
    // use remote value
    try {
      x = await successorClient.getPredecessor(fingerTable[0].successor);
    } catch (err) {
      x = _self;
      console.log(
        `stabilize: Warning! "successor.predecessor" (i.e., {${fingerTable[0].successor.id}}.predecessor), failed in stabilize({${_self.id}}).`
      );
    }
  }

  if (
    isInModuloRange(x.id, _self.id, false, fingerTable[0].successor.id, false)
  ) {
    fingerTable[0].successor = x;
  }

  if (DEBUGGING_LOCAL) {
    console.log(
      `stabilize: {${_self.id}}.predecessor leaving stabilize() is ${predecessor}`
    );
    console.log("stabilize: {", _self.id, "}.fingerTable[] is:\n", fingerTable);
    console.log(
      `stabilize: {${_self.id}}.successorTable[] is\n`,
      successorTable
    );
  }

  if (_self.id !== fingerTable[0].successor.id) {
    successorClient = caller(
      `${fingerTable[0].successor.host}:${fingerTable[0].successor.port}`,
      PROTO_PATH,
      "Node"
    );
    try {
      await successorClient.notify(_self);
    } catch (err) {
      console.error(`stabilize: successorClient.notify failed with `, err);
    }
  }

  // update successor table - deviates from SIGCOMM
  try {
    await updateSuccessorTable();
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
async function stabilizeSelf() {
  let predecessorSeemsOK = false;
  if (predecessor.id == null) {
    // this node is in real trouble since its predecessor is no good either
    // TODO try to rescue it by stepping through the rest of its finger table, else destroy it
    predecessorSeemsOK = false;
    return predecessorSeemsOK;
  }
  if (!iAmMyOwnPredecessor()) {
    try {
      // confirm that the predecessor is actually there
      predecessorSeemsOK = await checkPredecessor();
    } catch (err) {
      predecessorSeemsOK = false;
      console.error(`stabilizeSelf: checkPredecessor failed with `, err);
    }
    if (predecessorSeemsOK) {
      // then kick by setting the successor to the same as the predecessor
      fingerTable[0].successor = predecessor;
      successorTable[0] = fingerTable[0].successor;
    }
  } else {
    if (DEBUGGING_LOCAL)
      console.log(
        `stabilizeSelf: Warning: {${_self.id}} is isolated because predecessor is ${predecessor.id} and successor is ${fingerTable[0].successor.id}.`
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
async function notify(message, callback) {
  const nPrime = message.request;
  if (
    predecessor.id == null ||
    isInModuloRange(nPrime.id, predecessor.id, false, _self.id, false)
  ) {
    predecessor = nPrime;
  }
  callback(null, {});
}

/**
 * Directly implements the pseudocode's fixFingers() method.
 */
async function fixFingers() {
  let nSuccessor = NULL_NODE;

  const randomId = Math.ceil(Math.random() * (HASH_BIT_LENGTH - 1));
  try {
    nSuccessor = await findSuccessor(fingerTable[randomId].start, _self, _self);
    if (nSuccessor.id !== null) {
      fingerTable[randomId].successor = nSuccessor;
    }
  } catch (err) {
    console.error(`fixFingers call to findSuccessor failed with `, err);
  }
  if (DEBUGGING_LOCAL) {
    console.log(
      `\n>>>>>     Fix {${_self.id}}.fingerTable[${randomId}], with start = ${fingerTable[randomId].start}.`
    );
    console.log(
      `fingerTable[${randomId}] =${fingerTable[i].successor}     <<<<<\n`
    );
  }
}

/**
 * true if predecessor was still reasonable; false otherwise.
 * @returns {boolean}
 */
async function checkPredecessor() {
  if (predecessor.id !== null && !iAmMyOwnPredecessor()) {
    const predecessorClient = caller(
      `${predecessor.host}:${predecessor.port}`,
      PROTO_PATH,
      "Node"
    );
    try {
      // just ask anything
      const _ = await predecessorClient.getPredecessor(_self.id);
    } catch (err) {
      console.error(
        `checkPredecessor call to getPredecessor failed with `,
        err
      );
      predecessor = { id: null, host: null, port: null };
      return false;
    }
  }
  return true;
}

/**
 * Checks whether the successor is still responding.
 * @returns {boolean} true if successor was still reasonable; false otherwise.
 */
async function checkSuccessor() {
  if (DEBUGGING_LOCAL)
    console.log(`{${_self.id}}.checkSuccessor(${fingerTable[0].successor.id})`);

  let nSuccessor = NULL_NODE;
  let successorSeemsOK = false;
  if (fingerTable[0].successor.id == null) {
    successorSeemsOK = false;
  } else if (fingerTable[0].successor.id == _self.id) {
    successorSeemsOK = true;
  } else {
    try {
      // just ask anything
      nSuccessor = await getSuccessor(_self, fingerTable[0].successor);
      if (nSuccessor.id == null) {
        successorSeemsOK = false;
      } else {
        console.log(
          `{${fingerTable[0].successor.id}}.successor =${nSuccessor.id}`
        );
        successorSeemsOK = true;
      }
    } catch (err) {
      successorSeemsOK = false;
      console.log(
        `Error in checkSuccessor({${_self.id}}) call to getSuccessor`,
        err
      );
    }
  }
  return successorSeemsOK;
}

/**
 * Placeholder for data migration within the join() call.
 */
async function migrateKeys() {}

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

  // kludge to deconflict node IDs from hashed values
  if (args.hashOnly) {
    try {
      _self.id = await computeIntegerHash(args.hashOnly, HASH_BIT_LENGTH);
      console.log(`ID {${_self.id}} computed from hash of {${args.hashOnly}}`);
    } catch (err) {
      console.error(
        `Error computing hash of ${args.hashOnly}. Thus, terminating...\n`,
        err
      );
      return -13;
    }
    return 0;
  }

  _self.id = args.id ? args.id : null;
  _self.host = args.host ? args.host : os.hostname();
  _self.port = args.port ? args.port : DEFAULT_HOST_PORT;
  let knownNodeId = args.knownId ? args.knownId : null;
  let knownNodeHost = args.knownHost ? args.knownHost : os.hostname();
  let knownNodePort = args.knownPort ? args.knownPort : DEFAULT_HOST_PORT;

  // protect against bad ID inputs
  if (_self.id && _self.id > 2 ** HASH_BIT_LENGTH - 1) {
    console.error(
      `Error. Bad ID {${_self.id}} > 2^m-1 {${2 ** HASH_BIT_LENGTH -
        1}}. Terminating...\n`
    );
    return -13;
  }
  // recompute identity parameters from hash function
  if (!_self.id) {
    try {
      _self.id = await computeIntegerHash(
        _self.host + _self.port,
        HASH_BIT_LENGTH
      );
      if (DEBUGGING_LOCAL)
        console.log(`ID = { (from args) ${args.id} | (from hash) ${_self.id}}`);
    } catch (err) {
      console.error(
        `Error computing node ID from hash. Input was ${_self.host +
          _self.port} but hash output was ${_self.id}. Thus, terminating...\n`,
        err
      );
      return -13;
    }
  }

  // protect against bad Known ID inputs
  if (knownNodeId && knownNodeId > 2 ** HASH_BIT_LENGTH - 1) {
    console.error(
      `Error. Bad known ID {${args.knownId}} > 2^m-1 {${2 ** HASH_BIT_LENGTH -
        1}}. Thus, terminating...\n`
    );
    return -13;
  }
  // recompute known identity parameters from hash function
  if (!knownNodeId) {
    try {
      knownNodeId = await computeIntegerHash(
        knownNodeHost + knownNodePort,
        HASH_BIT_LENGTH
      );
      if (DEBUGGING_LOCAL)
        console.log(
          `Known ID = { (from args) ${args.knownId} | (from hash) ${knownNodeId} }`
        );
    } catch (err) {
      console.error(
        `Error computing the ID of the known node from hash. Input was ${knownNodeHost +
          knownNodePort} but hash output was ${knownNodeId}, so terminating\n`,
        err
      );
      return -13;
    }
  }

  // attempt to join new node
  await join({
    id: knownNodeId,
    host: knownNodeHost,
    port: knownNodePort
  });

  setInterval(async () => await stabilize(), 1000);
  setInterval(async () => await fixFingers(), 3000);
  setInterval(async () => await checkPredecessor(), CHECK_NODE_TIMEOUT_ms);

  const server = new grpc.Server();
  server.addService(chord.Node.service, {
    summary,
    fetch,
    remove,
    removeUserRemoteHelper,
    insert,
    insertUserRemoteHelper,
    lookup,
    lookupUserRemoteHelper,
    findSuccessorRemoteHelper,
    getSuccessorRemoteHelper,
    getPredecessor,
    setPredecessor,
    closestPrecedingFingerRemoteHelper,
    updateFingerTable,
    notify
  });
  console.log(`Serving on ${_self.host}:${_self.port}`);
  server.bind(`0.0.0.0:${_self.port}`, grpc.ServerCredentials.createInsecure());
  server.start();
}

main();
