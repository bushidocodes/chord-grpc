/**
 * Implements a node in a Chord, per Stoica et al., ca 2001.
 *
 */

const path = require("path");
const grpc = require("grpc");
const caller = require("grpc-caller");
const protoLoader = require("@grpc/proto-loader");
const minimist = require("minimist");
const { isInModuloRange, sha1 } = require("./utils.js");
// import * as dataAPI from "dataAPI";

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
const HASH_BIT_LENGTH = 3;
const CHECK_NODE_TIMEOUT_ms = 1000;
const NULL_NODE = { id: null, ip: null, port: null };
const NULL_USER = { id: null };
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
  console.log("vvvvv     vvvvv     Summary     vvvvv     vvvvv");
  console.log("fingerTable: \n", fingerTable);
  console.log("Predecessor: ", predecessor);
  console.log("^^^^^     ^^^^^     End Summary     ^^^^^     ^^^^^");
  callback(null, _self);
}

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
  // TODO: Use hashing to get the key
  let successor = NULL_NODE;

  console.log("In remove: userId");
  console.log(userId);

  try {
    successor = await findSuccessor(userId, _self, _self);
  } catch (err) {
    successor = NULL_NODE;
    console.error("in remove call: findSuccessor failed with ", err);
  }

  if (successor.id == _self.id) {
    console.log("In remove: remove user from local node");
    const err = removeUser(userId);
    callback(err, {});
  } else {
    // create client
    try {
      console.log("In remove: remove user from remote node");
      console.log(userId);
      const successorClient = caller(
        `localhost:${successor.port}`,
        PROTO_PATH,
        "Node"
      );
      await successorClient.removeUserRemoteHelper({ id: userId });
      callback(null, {});
    } catch (err) {
      console.error("remove call to removeUser failed with ", err);
      callback(err, null);
    }
  }
}

async function removeUserRemoteHelper(message, callback) {
  console.log("removeUserRemoteHelper beginning: ", message);
  removeUser(message.request.id);
  console.log("removeUserRemoteHelper finishing");
  callback(null, {});
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
  // TODO: Use hasing to get the key
  const lookupKey = user.id;
  let successor = NULL_NODE;

  console.log("In insert: user");
  console.log(user);

  try {
    successor = await findSuccessor(lookupKey, _self, _self);
  } catch (err) {
    successor = NULL_NODE;
    console.error("insert call to findSuccessor failed with ", err);
  }

  if (successor.id == _self.id) {
    console.log("In insert: insert user to local node");
    const err = insertUser(userEdit);
    console.log("insert finishing");
    callback(err, {});
  } else {
    // create client
    try {
      console.log("In insert: insert user to remote node");
      console.log(user, lookupKey);
      const successorClient = caller(
        `localhost:${successor.port}`,
        PROTO_PATH,
        "Node"
      );
      const err = await successorClient.insertUserRemoteHelper(userEdit);
      console.log("insert finishing");
      callback(err, {});
    } catch (err) {
      console.error("insert call to insertUser failed with ", err);
      callback(err, null);
    }
  }
}

async function insertUserRemoteHelper(message, callback) {
  console.log("insertUserRemoteHelper starting");
  const err = insertUser(message.request);
  callback(err, {});
}

function insertUser(userEdit) {
  console.log("insertUser userEdit: ", userEdit);
  const user = userEdit.user;
  const edit = userEdit.edit;
  if (userMap[user.id] && !edit) {
    console.log(`Err: ${user.id} already exits and overwrite = false`);
    return { code: 6 };
  } else {
    userMap[user.id] = user;
    console.log(`Inserted User ${user.id}:`);
    return null;
  }
}

/**
 * Insert a user
 * @param grpcRequest
 * @param callback gRPC callback
 */
//called by client/webapp
async function lookup(message, callback) {
  console.log("In lookup");
  const userId = message.request.id;
  // TODO: Use hasing to get the key
  const lookupKey = userId;
  let successor = NULL_NODE;

  try {
    successor = await findSuccessor(lookupKey, _self, _self);
  } catch (err) {
    successor = NULL_NODE;
    console.error("insert call to findSuccessor failed with ", err);
  }

  // once i have successor, either i call my self addUser or I use a client
  if (successor.id == _self.id) {
    console.log("In lookup: lookup user to local node");
    const { err, user } = lookupUser(userId);
    console.log("finished Server-side lookup, returning: ", err, user);
    callback(err, user);
  } else {
    // create client
    try {
      console.log("In lookup: lookup user to remote node");
      const successorClient = caller(
        `localhost:${successor.port}`,
        PROTO_PATH,
        "Node"
      );
      await successorClient.lookupUserRemoteHelper(
        { id: userId },
        (err, user) => {
          if (err) {
            callback(err, null);
            console.log(err);
          } else {
            console.log("lookup: user from remote: ", user);
            callback(err, user);
          }
        }
      );
    } catch (err) {
      // I'm not sure if the try/catch is necessary
      // the idea is in case the client does not work, not the user is not found
      console.error("lookup call to lookupUser failed with ", err);
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
  //if we have the user
  if (userMap[userId]) {
    const user = userMap[userId];
    const message = `User found ${user.id}`;
    console.log(message);
    return { err: null, user };
  } else {
    //we don't have user
    const message = `User with user ID ${userId} not found`;
    console.log(message);
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
  // enable debugging output
  const DEBUGGING_LOCAL = false;

  let nPrime = NULL_NODE;
  let nPrimeSuccessor = NULL_NODE;

  if (DEBUGGING_LOCAL) {
    console.log("vvvvv     vvvvv     findSuccessor     vvvvv     vvvvv");
  }

  if (nodeQuerying.id == nodeQueried.id) {
    // use local value
    // n' = findPredecessor(id);
    try {
      nPrime = await findPredecessor(id);
    } catch (err) {
      console.error(
        `findSuccessor's call to findPredecessor failed with `,
        err
      );
      nPrime = NULL_NODE;
    }

    if (DEBUGGING_LOCAL) {
      console.log("findSuccessor: n' is ", nPrime.id);
    }

    // get n'.successor either locally or remotely
    try {
      nPrimeSuccessor = await getSuccessor(_self, nPrime);
    } catch (err) {
      console.error(`findSuccessor's call to getSuccessor failed with `, err);
      nPrimeSuccessor = NULL_NODE;
    }

    if (DEBUGGING_LOCAL) {
      console.log("findSuccessor: n'.successor is ", nPrimeSuccessor.id);
    }
  } else {
    // create client for remote call
    const nodeQueriedClient = caller(
      `${nodeQueried.ip}:${nodeQueried.port}`,
      PROTO_PATH,
      "Node"
    );
    // now grab the remote value
    try {
      nPrimeSuccessor = await nodeQueriedClient.findSuccessorRemoteHelper({
        id: id,
        node: nodeQueried
      });
    } catch (err) {
      nPrimeSuccessor = NULL_NODE;
      console.error(
        "findSuccessor call to findSuccessorRemoteHelper failed with",
        err
      );
    }
  }

  if (DEBUGGING_LOCAL) {
    console.log("findSuccessor: departing n'.successor = ", nPrimeSuccessor.id);
    console.log("^^^^^     ^^^^^     findSuccessor     ^^^^^     ^^^^^");
  }

  // return n'.successor;
  return nPrimeSuccessor;
}

/**
 * RPC equivalent of the pseudocode's findSuccessor() method.
 * It is implemented as simply a wrapper for the local findSuccessor() method.
 *
 * @param idAndNodeQueried {id:, node:}, where ID is the key sought
 * @param callback grpc callback function
 *
 */
async function findSuccessorRemoteHelper(idAndNodeQueried, callback) {
  // enable debugging output
  const DEBUGGING_LOCAL = false;

  const id = idAndNodeQueried.request.id;
  const nodeQueried = idAndNodeQueried.request.node;

  if (DEBUGGING_LOCAL) {
    console.log(
      "vvvvv     vvvvv     findSuccessorRemoteHelper     vvvvv     vvvvv"
    );
    console.log("id = ", id, "nodeQueried = ", nodeQueried.id, ".");
  }

  let nPrimeSuccessor = NULL_NODE;
  try {
    nPrimeSuccessor = await findSuccessor(id, _self, nodeQueried);
  } catch (err) {
    console.error("nPrimeSuccessor call to findSuccessor failed with", err);
    nPrimeSuccessor = NULL_NODE;
  }
  callback(null, nPrimeSuccessor);

  if (DEBUGGING_LOCAL) {
    console.log(
      "findSuccessorRemoteHelper: nPrimeSuccessor = ",
      nPrimeSuccessor.id
    );
    console.log(
      "^^^^^     ^^^^^     findSuccessorRemoteHelper     ^^^^^     ^^^^^"
    );
  }
}

/**
 * This function directly implements the pseudocode's findPredecessor() method,
 *  with the exception of the limits on the while loop.
 *
 * @todo 20191103.hk: re-examine the limits on the while loop
 * @param {number} id the key sought
 */
async function findPredecessor(id) {
  // enable debugging output
  const DEBUGGING_LOCAL = false;

  if (DEBUGGING_LOCAL) {
    console.log("vvvvv     vvvvv     findPredecessor     vvvvv     vvvvv");
    console.log("id = ", id);
  }

  let nPrime = _self;
  let priorNPrime = NULL_NODE;
  let nPrimeSuccessor = NULL_NODE;
  // n' = n;
  try {
    nPrimeSuccessor = await getSuccessor(_self, nPrime);
  } catch (err) {
    console.error("findPredecessor call to getSuccessor failed with", err);
    nPrimeSuccessor = NULL_NODE;
  }

  if (DEBUGGING_LOCAL) {
    console.log(
      "before while: nPrime = ",
      nPrime.id,
      "; nPrimeSuccessor = ",
      nPrimeSuccessor.id
    );
  }

  // (maximum chord nodes = 2^m) * (length of finger table = m)
  let iterationCounter = 2 ** HASH_BIT_LENGTH * HASH_BIT_LENGTH;
  // while (id 'not-in' (n', n'.successor] )
  while (
    !isInModuloRange(id, nPrime.id, false, nPrimeSuccessor.id, true) &&
    nPrime.id !== nPrimeSuccessor.id &&
    // && (nPrime.id !== priorNPrime.id)
    iterationCounter >= 0
  ) {
    // loop should exit if n' and its successor are the same
    // loop should exit if n' and the prior n' are the same
    // loop should exit if the iterations are ridiculous
    // update loop protection
    iterationCounter--;
    // n' = n'.closestPrecedingFinger(id);
    try {
      nPrime = await closestPrecedingFinger(id, _self, nPrime);
    } catch (err) {
      nPrime = NULL_NODE;
    }

    if (DEBUGGING_LOCAL) {
      console.log("=== while iteration ", iterationCounter, " ===");
      console.log("nPrime = ", nPrime);
    }

    try {
      nPrimeSuccessor = await getSuccessor(_self, nPrime);
    } catch (err) {
      console.error(
        "findPredecessor call to getSuccessor (2) failed with",
        err
      );
      nPrimeSuccessor = NULL_NODE;
    }

    if (DEBUGGING_LOCAL) {
      console.log("nPrimeSuccessor = ", nPrimeSuccessor);
    }

    // store state
    priorNPrime = nPrime;
  }

  if (DEBUGGING_LOCAL) {
    console.log("^^^^^     ^^^^^     findPredecessor     ^^^^^     ^^^^^");
  }

  // return n';
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
  // enable debugging output
  const DEBUGGING_LOCAL = false;

  if (DEBUGGING_LOCAL) {
    console.log("vvvvv     vvvvv     getSuccessor     vvvvv     vvvvv");
    console.log("{", nodeQuerying.id, "}.getSuccessor(", nodeQueried.id, ")");
  }

  // get n.successor either locally or remotely
  let nSuccessor = NULL_NODE;
  if (nodeQuerying.id == nodeQueried.id) {
    // use local value
    nSuccessor = fingerTable[0].successor;
  } else {
    // use remote value
    // create client for remote call
    const nodeQueriedClient = caller(
      `${nodeQueried.ip}:${nodeQueried.port}`,
      PROTO_PATH,
      "Node"
    );
    // now grab the remote value
    try {
      nSuccessor = await nodeQueriedClient.getSuccessorRemoteHelper(
        nodeQueried
      );
    } catch (err) {
      // TBD 20191103.hk: why does "nSuccessor = NULL_NODE;" not do the same as explicit?!?!
      nSuccessor = { id: null, ip: null, port: null };
      console.trace("Remote error in getSuccessor() ", err);
    }
  }

  if (DEBUGGING_LOCAL) {
    console.log("returning {", nodeQueried.id, "}.successor = ", nSuccessor.id);
    console.log("^^^^^     ^^^^^     getSuccessor     ^^^^^     ^^^^^");
  }

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
    // for i = m downto 1
    for (let i = HASH_BIT_LENGTH - 1; i >= 0; i--) {
      // if ( finger[i].node 'is-in' (n, id) )
      if (
        isInModuloRange(
          fingerTable[i].successor.id,
          nodeQueried.id,
          false,
          id,
          false
        )
      ) {
        // return finger[i].node;
        nPreceding = fingerTable[i].successor;
        return nPreceding;
      }
    }
    // return n;
    nPreceding = nodeQueried;
    return nPreceding;
  } else {
    // use remote value
    // create client for remote call
    const nodeQueriedClient = caller(
      `${nodeQueried.ip}:${nodeQueried.port}`,
      PROTO_PATH,
      "Node"
    );
    // now grab the remote value
    try {
      nPreceding = await nodeQueriedClient.closestPrecedingFingerRemoteHelper({
        id: id,
        node: nodeQueried
      });
    } catch (err) {
      nPreceding = NULL_NODE;
      console.error(
        "closestPrecedingFinger call to closestPrecedingFingerRemoteHelper failed with ",
        err
      );
    }
    // return n;
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
      "closestPrecedingFingerRemoteHelper call to closestPrecedingFinger failed with ",
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
  // enable debugging output
  const DEBUGGING_LOCAL = false;

  if (DEBUGGING_LOCAL) {
    console.log("vvvvv     vvvvv     setPredecessor     vvvvv     vvvvv");
    console.log("Self = ", _self);
    console.log("Self's original predecessor = ", predecessor);
  }

  predecessor = message.request; //message.request is node

  if (DEBUGGING_LOCAL) {
    console.log("Self's new predecessor = ", predecessor);
    console.log("^^^^^     ^^^^^     setPredecessor     ^^^^^     ^^^^^");
  }

  callback(null, {});
}

/**
 * Modified implementation of pseudocode's "heavyweight" version of the join() method
 *   as described in Figure 6 of the SIGCOMM paper.
 * Modification consists of an additional step of initializing the successor table
 *   as described in the IEEE paper.
 *
 * @param knownNode: knownNode structure; e.g., {id, ip, port}
 *   Pass a null known node to force the node to be the first in a new chord.
 */
async function join(knownNode) {
  // enable debugging output
  const DEBUGGING_LOCAL = false;
  // remove dummy template initializer from table
  fingerTable.pop();
  // initialize table with reasonable values
  for (let i = 0; i < HASH_BIT_LENGTH; i++) {
    fingerTable.push({
      start: (_self.id + 2 ** i) % 2 ** HASH_BIT_LENGTH,
      successor: _self
    });
  }
  // if (n')
  if (knownNode && confirmExist(knownNode)) {
    // (n');
    await initFingerTable(knownNode);
    // updateOthers();
    await updateOthers();
  } else {
    // this is the first node
    // initialize predecessor
    predecessor = _self;
  }

  // TODO migrate keys: (predecessor, n]; i.e., (predecessor, _self]
  await migrateKeys();

  // initialize successor table - deviates from SIGCOMM
  successorTable[0] = fingerTable[0].successor;

  if (DEBUGGING_LOCAL) {
    console.log(">>>>>     join          ");
    console.log(
      "The fingerTable[] leaving {",
      _self.id,
      "}.join(",
      knownNode.id,
      ") is:\n",
      fingerTable
    );
    console.log(
      "The {",
      _self.id,
      "}.predecessor leaving join() is ",
      predecessor
    );
    console.log("          join     <<<<<\n");
  }
}

/**
 * Determine whether a node exists by pinging it.
 * @param knownNode: knownNode structure; e.g., {id, ip, port}
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
  // enable debugging output
  const DEBUGGING_LOCAL = false;

  if (DEBUGGING_LOCAL) {
    console.log("vvvvv     vvvvv     initFingerTable     vvvvv     vvvvv");
    console.log(
      "self = ",
      _self.id,
      "; self.successor = ",
      fingerTable[0].successor.id,
      "; finger[0].start = ",
      fingerTable[0].start
    );
    console.log("n' = ", nPrime.id);
  }

  let nPrimeSuccessor = NULL_NODE;
  try {
    nPrimeSuccessor = await findSuccessor(fingerTable[0].start, _self, nPrime);
  } catch (err) {
    nPrimeSuccessor = NULL_NODE;
    console.error("initFingerTable call to findSuccessor failed with ", err);
  }
  // finger[1].node = n'.findSuccessor(finger[1].start);
  fingerTable[0].successor = nPrimeSuccessor;

  if (DEBUGGING_LOCAL) {
    console.log("n'.successor (now  self.successor) = ", nPrimeSuccessor);
  }

  // client for newly-determined successor
  let successorClient = caller(
    `${fingerTable[0].successor.ip}:${fingerTable[0].successor.port}`,
    PROTO_PATH,
    "Node"
  );
  // predecessor = successor.predecessor;
  try {
    predecessor = await successorClient.getPredecessor(
      fingerTable[0].successor
    );
  } catch (err) {
    predecessor = NULL_NODE;
    console.error("initFingerTable call to getPredecessor failed with", err);
  }
  // successor.predecessor = n;
  try {
    await successorClient.setPredecessor(_self);
  } catch (err) {
    console.error("initFingerTable call to setPredecessor() failed with ", err);
  }

  if (DEBUGGING_LOCAL) {
    console.log("initFingerTable: predecessor  ", predecessor);
  }

  // for (i=1 to m-1){}, where 1 is really 0, and skip last element
  for (let i = 0; i < HASH_BIT_LENGTH - 1; i++) {
    // if ( finger[i+1].start 'is in' [n, finger[i].node) )
    if (
      isInModuloRange(
        fingerTable[i + 1].start,
        _self.id,
        true,
        fingerTable[i].successor.id,
        false
      )
    ) {
      // finger[i+1].node = finger[i].node;
      fingerTable[i + 1].successor = fingerTable[i].successor;
    } else {
      // finger[i+1].node = n'.findSuccessor(finger[i+1].start);
      try {
        fingerTable[i + 1].successor = await findSuccessor(
          fingerTable[i + 1].start,
          _self,
          nPrime
        );
      } catch (err) {
        fingerTable[i + 1].successor = NULL_NODE;
        console.error(
          "initFingerTable call to findSuccessor() failed with ",
          err
        );
      }
    }
  }
  if (DEBUGGING_LOCAL) {
    console.log("initFingerTable: fingerTable[] =\n", fingerTable);
    console.log("^^^^^     ^^^^^     initFingerTable     ^^^^^     ^^^^^");
  }
}

/**
 * Directly implement the pseudocode's updateOthers() method.
 *
 */
async function updateOthers() {
  // enable debugging output
  const DEBUGGING_LOCAL = false;
  if (DEBUGGING_LOCAL) {
    console.log("vvvvv     vvvvv     updateOthers     vvvvv     vvvvv");
    console.log("_self = ", _self);
  }

  let pNode = NULL_NODE;
  let pNodeSearchID;
  let pNodeClient;
  // for i = 1 to m
  for (let i = 0; i < HASH_BIT_LENGTH; i++) {
    /* argument for "p = findPredecessor(n - 2^(i - 1))"
            but really 2^(i) because the index is now 0-based
            nonetheless, avoid ambiguity with negative numbers by:
                1- pegging 0 to 2^m with "+ 2**HASH_BIT_LENGTH
                2- taking the mod with "% 2**HASH_BIT_LENGTH"
        */
    pNodeSearchID =
      (_self.id - 2 ** i + 2 ** HASH_BIT_LENGTH) % 2 ** HASH_BIT_LENGTH;
    if (DEBUGGING_LOCAL) {
      console.log(
        "i = ",
        i,
        "; findPredecessor(",
        pNodeSearchID,
        ") --> pNode"
      );
    }

    // p = findPredecessor(n - 2^(i - 1));
    try {
      pNode = await findPredecessor(pNodeSearchID);
    } catch (err) {
      pNode = NULL_NODE;
      console.error(
        "Error from findPredecessor(",
        pNodeSearchID,
        ") in updateOthers().",
        err
      );
    }

    if (DEBUGGING_LOCAL) {
      console.log("pNode = ", pNode);
    }

    // p.updateFingerTable(n, i);
    if (_self.id !== pNode.id) {
      pNodeClient = caller(`${pNode.ip}:${pNode.port}`, PROTO_PATH, "Node");
      try {
        await pNodeClient.updateFingerTable({ node: _self, index: i });
      } catch (err) {
        console.error("updateOthers: client.updateFingerTable error ", err);
      }
    }
  }

  if (DEBUGGING_LOCAL) {
    console.log("^^^^^     ^^^^^     updateOthers     ^^^^^     ^^^^^");
  }
}

/**
 * RPC that directly implements the pseudocode's updateFingerTable() method.
 *
 * @param message - consists of {sNode, fingerIndex} *
 * @param callback - grpc callback
 */
async function updateFingerTable(message, callback) {
  // enable debugging output
  const DEBUGGING_LOCAL = false;

  const sNode = message.request.node;
  const fingerIndex = message.request.index;

  if (DEBUGGING_LOCAL) {
    console.log("vvvvv     vvvvv     updateFingerTable     vvvvv     vvvvv");
    console.log("{", _self.id, "}.fingerTable[] =\n", fingerTable);
    console.log(
      "sNode = ",
      message.request.node.id,
      "; fingerIndex =",
      fingerIndex
    );
  }

  // if ( s 'is in' [n, finger[i].node) )
  if (
    isInModuloRange(
      sNode.id,
      _self.id,
      true,
      fingerTable[fingerIndex].successor.id,
      false
    )
  ) {
    // finger[i].node = s;
    fingerTable[fingerIndex].successor = sNode;
    // p = predecessor;
    const pClient = caller(
      `${predecessor.ip}:${predecessor.port}`,
      PROTO_PATH,
      "Node"
    );
    // p.updateFingerTable(s, i);
    try {
      await pClient.updateFingerTable({ node: sNode, index: fingerIndex });
    } catch (err) {
      console.error(
        "Error updating the finger table of {",
        sNode.id,
        "}.\n\n",
        err
      );
    }

    if (DEBUGGING_LOCAL) {
      console.log(
        "Updated {",
        _self.id,
        "}.fingerTable[",
        fingerIndex,
        "] to ",
        sNode
      );
      console.log("^^^^^     ^^^^^     updateFingerTable     ^^^^^     ^^^^^");
    }

    // TODO: Figure out how to determine if the above had an RC of 0
    // If so call callback({status: 0, message: "OK"}, {});
    callback(null, {});
    return;
  }

  if (DEBUGGING_LOCAL) {
    console.log("^^^^^     ^^^^^     updateFingerTable     ^^^^^     ^^^^^");
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
  // enable debugging output
  const DEBUGGING_LOCAL = false;
  if (DEBUGGING_LOCAL) {
    console.log("vvvvv     vvvvv     updateSuccessorTable     vvvvv     vvvvv");
    console.log("{", _self.id, "}.successorTable[] =\n", successorTable);
    console.log("successor node id = ", fingerTable[0].successor.id);
  }

  // check whether the successor is available
  let successorSeemsOK = false;
  try {
    successorSeemsOK = await checkSuccessor();
  } catch (err) {
    console.error(
      `updateSuccessorTable call to checkSuccessor failed with `,
      err
    );
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
        console.error(
          `updateSuccessorTable call to checkSuccessor failed with `,
          err
        );
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
    successorTable.push({ id: _self.id, ip: _self.ip, port: _self.port });
  }
  // try to bulk up the table
  let successorSuccessor = NULL_NODE;
  if (
    successorTable.length < HASH_BIT_LENGTH &&
    _self.id !== fingerTable[0].successor
  ) {
    if (DEBUGGING_LOCAL) {
      console.log(
        "Short successorTable[]: prefer length ",
        HASH_BIT_LENGTH,
        " but actual length is ",
        successorTable.length,
        "."
      );
    }
    for (let i = 0; i < successorTable.length && i <= HASH_BIT_LENGTH; i++) {
      try {
        successorSuccessor = await getSuccessor(_self, successorTable[i]);
      } catch (err) {
        console.error(
          `updateSuccessorTable call to getSuccessor failed with `,
          err
        );
        successorSuccessor = { id: null, ip: null, port: null };
      }
      if (DEBUGGING_LOCAL) {
        console.log(
          "{",
          _self.id,
          "}.st[",
          i,
          "] = ",
          successorTable[i].id,
          "; {",
          successorTable[i].id,
          "}.successor[0] = ",
          successorSuccessor.id
        );
      }
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
  successorSuccessor = { id: null, ip: null, port: null };
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

  if (DEBUGGING_LOCAL) {
    console.log("New {", _self.id, "}.successorTable[] =\n", successorTable);
    console.log("^^^^^     ^^^^^     updateSuccessorTable     ^^^^^     ^^^^^");
  }

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
  // enable debugging output
  const DEBUGGING_LOCAL = false;
  let successorClient;

  let x;
  try {
    successorClient = caller(
      `${fingerTable[0].successor.ip}:${fingerTable[0].successor.port}`,
      PROTO_PATH,
      "Node"
    );
  } catch {
    console.error(`stabilize call to caller failed with `, err);
    return false;
  }
  // x = successor.predecessor;
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
        'Warning! "successor.predecessor" (i.e., {',
        fingerTable[0].successor.id,
        "}.predecessor), failed in stabilize({",
        _self.id,
        "})."
      );
    }
  }

  // if (x 'is in' (n, n.successor))
  if (
    isInModuloRange(x.id, _self.id, false, fingerTable[0].successor.id, false)
  ) {
    // successor = x;
    fingerTable[0].successor = x;
  }

  if (DEBUGGING_LOCAL) {
    console.log(">>>>>     stabilize          ");
    console.log(
      "{",
      _self.id,
      "}.predecessor leaving stabilize() is ",
      predecessor
    );
    console.log("{", _self.id, "}.fingerTable[] is:\n", fingerTable);
    console.log("{", _self.id, "}.successorTable[] is \n", successorTable);
    console.log("          stabilize     <<<<<");
  }

  // successor.notify(n);
  if (_self.id !== fingerTable[0].successor.id) {
    successorClient = caller(
      `${fingerTable[0].successor.ip}:${fingerTable[0].successor.port}`,
      PROTO_PATH,
      "Node"
    );
    try {
      await successorClient.notify(_self);
    } catch (err) {
      console.error(
        `stabilize call to successorClient.notify failed with `,
        err
      );
    }
  }

  /* TBD 20191103 */
  // update successor table - deviates from SIGCOMM
  try {
    await updateSuccessorTable();
  } catch (err) {
    console.error(`stabilize call to updateSuccessorTable failed with `, err);
  }
  /* TBD 20191103 */
  return true;
}

/**
 * Attempts to kick a node with a successor of self, as would be the case in the first node in a chord.
 * The kick comes from setting the successor to be equal to the predecessor.
 *
 * This is an original function, not described in either version of the paper - added 20191021.
 *
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
  if (predecessor.id !== _self.id) {
    try {
      // confirm that the predecessor is actually there
      predecessorSeemsOK = await checkPredecessor();
    } catch (err) {
      predecessorSeemsOK = false;
      console.error(`stabilizeSelf call to checkPredecessor failed with `, err);
    }
    if (predecessorSeemsOK) {
      // then kick by setting the successor to the same as the predecessor
      fingerTable[0].successor = predecessor;
      successorTable[0] = fingerTable[0].successor;
    }
  } else {
    console.log(
      "\nWarning: {",
      _self.id,
      "} is isolated because",
      "predecessor is",
      predecessor.id,
      "and successor is",
      fingerTable[0].successor.id,
      "."
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
  // if (predecessor is nil or n' 'is in' (predecessor, n))
  if (
    predecessor.id == null ||
    isInModuloRange(nPrime.id, predecessor.id, false, _self.id, false)
  ) {
    // predecessor = n';
    predecessor = nPrime;
  }
  callback(null, {});
}

/**
 * Directly implements the pseudocode's fixFingers() method.
 *
 */
async function fixFingers() {
  // enable debugging output
  const DEBUGGING_LOCAL = false;
  let nSuccessor = NULL_NODE;

  // i = random index > 1 into finger[]; but really >0 because 0-based
  // random integer within the range (0, m)
  const i = Math.ceil(Math.random() * (HASH_BIT_LENGTH - 1));
  // finger[i].node = findSuccessor(finger[i].start);
  try {
    nSuccessor = await findSuccessor(fingerTable[i].start, _self, _self);
    if (nSuccessor.id !== null) {
      fingerTable[i].successor = nSuccessor;
    }
  } catch (err) {
    console.error(`fixFingers call to findSuccessor failed with `, err);
  }
  if (DEBUGGING_LOCAL) {
    console.log(
      "\n>>>>>     Fix {",
      _self.id,
      "}.fingerTable[",
      i,
      "], with start = ",
      fingerTable[i].start,
      "."
    );
    console.log(
      "     fingerTable[",
      i,
      "] =",
      fingerTable[i].successor,
      "     <<<<<\n"
    );
  }
}

/**
 * Directly implements the checkPredecessor() method from the IEEE version of the paper.
 *
 * @returns {boolean} true if predecessor was still reasonable; false otherwise.
 */
async function checkPredecessor() {
  if (predecessor.id !== null && predecessor.id !== _self.id) {
    const predecessorClient = caller(
      `${predecessor.ip}:${predecessor.port}`,
      PROTO_PATH,
      "Node"
    );
    try {
      // just ask anything
      const x = await predecessorClient.getPredecessor(_self.id);
    } catch (err) {
      console.error(
        `checkPredecessor call to getPredecessor failed with `,
        err
      );
      predecessor = { id: null, ip: null, port: null };
      return false;
    }
  }
  return true;
}

/**
 * Checks whether the successor is still responding.
 *
 * This is an original function, not described in either version of the paper - added 20191103.
 *
 * @returns {boolean} true if successor was still reasonable; false otherwise.
 */
async function checkSuccessor() {
  // enable debugging output
  const DEBUGGING_LOCAL = false;

  if (DEBUGGING_LOCAL) {
    console.log(
      "{",
      _self.id,
      "}.checkSuccessor(",
      fingerTable[0].successor.id,
      ")"
    );
  }

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
          "{",
          fingerTable[0].successor.id,
          "}.successor =",
          nSuccessor.id
        );
        successorSeemsOK = true;
      }
    } catch (err) {
      successorSeemsOK = false;
      console.log(
        "Error in checkSuccessor({",
        _self.id,
        "}) call to getSuccessor",
        err
      );
    }
  }
  return successorSeemsOK;
}

/**
 * Placeholder for data migration within the join() call.
 *
 */
async function migrateKeys() {}

/**
 * Starts an RPC server that receives requests for the Greeter service at the
 * sample server port
 *
 * Takes the following optional flags
 * --id         - This node's id
 * --ip         - This node's IP Address'
 * --port       - This node's Port
 *
 * --targetId   - The ID of a node in the cluster
 * --targetIp   - The IP of a node in the cluster
 * --targetPort - The Port of a node in the cluster
 *
 */
async function main() {
  // enable debugging output
  const DEBUGGING_LOCAL = true;

  const args = minimist(process.argv.slice(2));
  _self.id = args.id ? args.id : 0;
  _self.ip = args.ip ? args.ip : `0.0.0.0`;
  _self.port = args.port ? args.port : 1337;

  if (
    args.targetIp !== null &&
    args.targetPort !== null &&
    args.targetId !== null
  ) {
    await join({ id: args.targetId, ip: args.targetIp, port: args.targetPort });
  } else {
    await join(null);
  }

  // periodically run stabilization functions
  setInterval(async () => {
    await stabilize();
  }, 3000);
  setInterval(async () => {
    await fixFingers();
  }, 3000);
  setInterval(async () => {
    await checkPredecessor();
  }, CHECK_NODE_TIMEOUT_ms);

  // server instantiation
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
  // Bind to all addresses because of DNS
  server.bind(`0.0.0.0:${_self.port}`, grpc.ServerCredentials.createInsecure());
  server.start();
  if (DEBUGGING_LOCAL) {
    console.log(`Serving on ${_self.ip}:${_self.port}`);
  }
}

main();
