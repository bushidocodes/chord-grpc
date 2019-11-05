/**
 * Implements a node in a Chord, per Stoica et al., ca 2001.
 *
 */

const path = require("path");
const grpc = require("grpc");
// const userMap = require("./data/tinyuserMap.json");
const userMap = {};
const protoLoader = require("@grpc/proto-loader");
const minimist = require("minimist");
const { isInModuloRange, sha1 } = require("./utils.js");

const PROTO_PATH = path.resolve(__dirname, "./protos/chord.proto");

// import * as dataAPI from "dataAPI";

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true
});

const chord = grpc.loadPackageDefinition(packageDefinition).chord;

const caller = require("grpc-caller");

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
    successor = await find_successor(userId, _self, _self);
  } catch (err) {
    successor = NULL_NODE;
    console.error("in remove call: find_successor failed with ", err);
  }

  if (successor.id == _self.id) {
    console.log("In remove: remove user from local node");
    removeUser(userId);
    console.log("remove finishing");
    callback(null, {});
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
      await successorClient.removeUser_remoteHelper({ id: userId });
      callback(null, {});
    } catch (err) {
      console.error("remove call to removeUser failed with ", err);
      callback(err, null);
    }
  }
}

async function removeUser_remoteHelper(message, callback) {
  console.log("removeUser_remoteHelper beginning: ", message);
  removeUser(message.request.id);
  console.log("reomveUser_remoteHelper finishing");
  callback(null, {});
}

function removeUser(id) {
  console.log("removeUser beginning: ", id);
  if (userMap[id]) {
    delete userMap[id];
    console.log("removeUser finishing");
  } else {
    console.log("in removeUser, user DNE");
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
    successor = await find_successor(lookupKey, _self, _self);
  } catch (err) {
    successor = NULL_NODE;
    console.error("insert call to find_successor failed with ", err);
  }

  if (successor.id == _self.id) {
    console.log("In insert: insert user to local node");
    insertUser(userEdit);
    console.log("insert finishing");
    callback(null, {});
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
      await successorClient.insertUser_remoteHelper(userEdit);
      callback(null, {});
    } catch (err) {
      console.error("insert call to insertUser failed with ", err);
      callback(err, null);
    }
  }
}

async function insertUser_remoteHelper(message, callback) {
  insertUser(message.request);
  console.log("insertUser_remoteHelper finishing");
  callback(null, {});
}

function insertUser(userEdit) {
  console.log("insertUser userEdit: ", userEdit);
  const user = userEdit.user;
  const edit = userEdit.edit;
  if (userMap[user.id] && !edit) {
    const message = `Err: ${user.id} already exits and overwrite = false`;
    console.log(message);
  } else {
    userMap[user.id] = user;
    const message = `Inserted User ${user.id}:`;
    console.log(message);
    //console.log(userMap);
  }
  console.log("insertUser finishing");
  return null;
}

/**
 * Insert a user
 * @param grpcRequest
 * @param callback gRPC callback
 */
//called by client/webapp
async function lookup(message, callback) {
  console.log("In lookup");
  console.log(message);
  const userId = message.request.id;
  // TODO: Use hasing to get the key
  const lookupKey = userId;
  let successor = NULL_NODE;

  console.log(userId);

  try {
    successor = await find_successor(lookupKey, _self, _self);
  } catch (err) {
    successor = NULL_NODE;
    console.error("insert call to find_successor failed with ", err);
  }
  // once i have successor, either i call my self addUser or I use a client
  if (successor.id == _self.id) {
    console.log("In lookup: lookup user to local node");
    const foundUser = await lookupUser(userId);
    console.log("finished Server-side lookup, returning: ", foundUser);
    //callback(null, lookupUser(message.request.id));
    callback(null, foundUser);
  } else {
    // create client
    try {
      console.log("In lookup: lookup user to remote node");
      const successorClient = caller(
        `localhost:${successor.port}`,
        PROTO_PATH,
        "Node"
      );
      const user = await successorClient.lookupUser_remoteHelper({
        id: userId
      });
      callback(null, user);
    } catch (err) {
      console.error("lookup call to lookupUser failed with ", err);
      callback(err, null);
    }
  }
}

async function lookupUser_remoteHelper(message, callback) {
  console.log("beginning lookupUser_remoteHelper: ", message.request.id);
  let temp = lookupUser(message.request.id);
  console.log("finishing lookupuser_remoteHelper: ", temp);
  callback(null, temp);
}

function lookupUser(userId) {
  //if we have the user
  if (userMap[userId]) {
    const user = userMap[userId];
    const message = `User found ${user.id}`;
    console.log(message);
    return user;
  } else {
    //we don't have user
    const message = `User with user ID ${userId} not found`;
    console.log(message);
    return NULL_USER;
  }
}

/**
 * Directly implement the pseudocode's find_successor() method.
 *
 * However, it is able to discern whether to do a local lookup or an RPC.
 * If the querying node is the same as the queried node, it will stay local.
 *
 * @param {number} id value being searched
 * @param node_querying node initiating the query
 * @param node_queried node being queried for the ID
 * @returns id.successor
 *
 */
async function find_successor(id, node_querying, node_queried) {
  // enable debugging output
  const DEBUGGING_LOCAL = false;

  let n_prime = NULL_NODE;
  let n_prime_successor = NULL_NODE;

  if (DEBUGGING_LOCAL) {
    console.log("vvvvv     vvvvv     find_successor     vvvvv     vvvvv");
  }

  if (node_querying.id == node_queried.id) {
    // use local value
    // n' = find_predecessor(id);
    try {
      n_prime = await find_predecessor(id);
    } catch (err) {
      console.error(
        `find_successor's call to find_predecessor failed with `,
        err
      );
      n_prime = NULL_NODE;
    }

    if (DEBUGGING_LOCAL) {
      console.log("find_successor: n' is ", n_prime.id);
    }

    // get n'.successor either locally or remotely
    try {
      n_prime_successor = await getSuccessor(_self, n_prime);
    } catch (err) {
      console.error(`find_successor's call to getSuccessor failed with `, err);
      n_prime_successor = NULL_NODE;
    }

    if (DEBUGGING_LOCAL) {
      console.log("find_successor: n'.successor is ", n_prime_successor.id);
    }
  } else {
    // create client for remote call
    const node_queried_client = caller(
      `localhost:${node_queried.port}`,
      PROTO_PATH,
      "Node"
    );
    // now grab the remote value
    try {
      n_prime_successor = await node_queried_client.find_successor_remotehelper(
        { id: id, node: node_queried }
      );
    } catch (err) {
      n_prime_successor = NULL_NODE;
      console.error(
        "find_successor call to find_successor_remotehelper failed with",
        err
      );
    }
  }

  if (DEBUGGING_LOCAL) {
    console.log(
      "find_successor: departing n'.successor = ",
      n_prime_successor.id
    );
    console.log("^^^^^     ^^^^^     find_successor     ^^^^^     ^^^^^");
  }

  // return n'.successor;
  return n_prime_successor;
}

/**
 * RPC equivalent of the pseudocode's find_successor() method.
 * It is implemented as simply a wrapper for the local find_successor() method.
 *
 * @param id_and_node_queried {id:, node:}, where ID is the key sought
 * @param callback grpc callback function
 *
 */
async function find_successor_remotehelper(id_and_node_queried, callback) {
  // enable debugging output
  const DEBUGGING_LOCAL = false;

  const id = id_and_node_queried.request.id;
  const node_queried = id_and_node_queried.request.node;

  if (DEBUGGING_LOCAL) {
    console.log(
      "vvvvv     vvvvv     find_successor_remotehelper     vvvvv     vvvvv"
    );
    console.log("id = ", id, "node_queried = ", node_queried.id, ".");
  }

  let n_prime_successor = NULL_NODE;
  try {
    n_prime_successor = await find_successor(id, _self, node_queried);
  } catch (err) {
    console.error("n_prime_successor call to find_successor failed with", err);
    n_prime_successor = NULL_NODE;
  }
  callback(null, n_prime_successor);

  if (DEBUGGING_LOCAL) {
    console.log(
      "find_successor_remotehelper: n_prime_successor = ",
      n_prime_successor.id
    );
    console.log(
      "^^^^^     ^^^^^     find_successor_remotehelper     ^^^^^     ^^^^^"
    );
  }
}

/**
 * This function directly implements the pseudocode's find_predecessor() method,
 *  with the exception of the limits on the while loop.
 *
 * @todo 20191103.hk: re-examine the limits on the while loop
 * @param {number} id the key sought
 */
async function find_predecessor(id) {
  // enable debugging output
  const DEBUGGING_LOCAL = false;

  if (DEBUGGING_LOCAL) {
    console.log("vvvvv     vvvvv     find_predecessor     vvvvv     vvvvv");
    console.log("id = ", id);
  }

  let n_prime = _self;
  let prior_n_prime = NULL_NODE;
  let n_prime_successor = NULL_NODE;
  // n' = n;
  try {
    n_prime_successor = await getSuccessor(_self, n_prime);
  } catch (err) {
    console.error("find_predecessor call to getSuccessor failed with", err);
    n_prime_successor = NULL_NODE;
  }

  if (DEBUGGING_LOCAL) {
    console.log(
      "before while: n_prime = ",
      n_prime.id,
      "; n_prime_successor = ",
      n_prime_successor.id
    );
  }

  // (maximum chord nodes = 2^m) * (length of finger table = m)
  let iteration_counter = 2 ** HASH_BIT_LENGTH * HASH_BIT_LENGTH;
  // while (id 'not-in' (n', n'.successor] )
  while (
    !isInModuloRange(id, n_prime.id, false, n_prime_successor.id, true) &&
    n_prime.id !== n_prime_successor.id &&
    // && (n_prime.id !== prior_n_prime.id)
    iteration_counter >= 0
  ) {
    // loop should exit if n' and its successor are the same
    // loop should exit if n' and the prior n' are the same
    // loop should exit if the iterations are ridiculous
    // update loop protection
    iteration_counter--;
    // n' = n'.closest_preceding_finger(id);
    try {
      n_prime = await closest_preceding_finger(id, _self, n_prime);
    } catch (err) {
      n_prime = NULL_NODE;
    }

    if (DEBUGGING_LOCAL) {
      console.log("=== while iteration ", iteration_counter, " ===");
      console.log("n_prime = ", n_prime);
    }

    try {
      n_prime_successor = await getSuccessor(_self, n_prime);
    } catch (err) {
      console.error(
        "find_predecessor call to getSuccessor (2) failed with",
        err
      );
      n_prime_successor = NULL_NODE;
    }

    if (DEBUGGING_LOCAL) {
      console.log("n_prime_successor = ", n_prime_successor);
    }

    // store state
    prior_n_prime = n_prime;
  }

  if (DEBUGGING_LOCAL) {
    console.log("^^^^^     ^^^^^     find_predecessor     ^^^^^     ^^^^^");
  }

  // return n';
  return n_prime;
}

/**
 * Return the successor of a given node by either a local lookup or an RPC.
 * If the querying node is the same as the queried node, it will be a local lookup.
 * @param node_querying
 * @param node_queried
 * @returns : the successor if the successor seems valid, or a null node otherwise
 */
async function getSuccessor(node_querying, node_queried) {
  // enable debugging output
  const DEBUGGING_LOCAL = false;

  if (DEBUGGING_LOCAL) {
    console.log("vvvvv     vvvvv     getSuccessor     vvvvv     vvvvv");
    console.log("{", node_querying.id, "}.getSuccessor(", node_queried.id, ")");
  }

  // get n.successor either locally or remotely
  let n_successor = NULL_NODE;
  if (node_querying.id == node_queried.id) {
    // use local value
    n_successor = fingerTable[0].successor;
  } else {
    // use remote value
    // create client for remote call
    const node_queried_client = caller(
      `localhost:${node_queried.port}`,
      PROTO_PATH,
      "Node"
    );
    // now grab the remote value
    try {
      n_successor = await node_queried_client.getSuccessor_remotehelper(
        node_queried
      );
    } catch (err) {
      // TBD 20191103.hk: why does "n_successor = NULL_NODE;" not do the same as explicit?!?!
      n_successor = { id: null, ip: null, port: null };
      console.trace("Remote error in getSuccessor() ", err);
    }
  }

  if (DEBUGGING_LOCAL) {
    console.log(
      "returning {",
      node_queried.id,
      "}.successor = ",
      n_successor.id
    );
    console.log("^^^^^     ^^^^^     getSuccessor     ^^^^^     ^^^^^");
  }

  return n_successor;
}

/**
 * RPC equivalent of the getSuccessor() method.
 * It is implemented as simply a wrapper for the getSuccessor() function.
 * @param _ - dummy parameter
 * @param callback - grpc callback
 */
async function getSuccessor_remotehelper(_, callback) {
  callback(null, fingerTable[0].successor);
}

/**
 * Directly implement the pseudocode's closest_preceding_finger() method.
 *
 * However, it is able to discern whether to do a local lookup or an RPC.
 * If the querying node is the same as the queried node, it will stay local.
 *
 * @param id
 * @param node_querying
 * @param node_queried
 * @returns the closest preceding node to ID
 *
 */
async function closest_preceding_finger(id, node_querying, node_queried) {
  let n_preceding = NULL_NODE;
  if (node_querying.id == node_queried.id) {
    // use local value
    // for i = m downto 1
    for (let i = HASH_BIT_LENGTH - 1; i >= 0; i--) {
      // if ( finger[i].node 'is-in' (n, id) )
      if (
        isInModuloRange(
          fingerTable[i].successor.id,
          node_queried.id,
          false,
          id,
          false
        )
      ) {
        // return finger[i].node;
        n_preceding = fingerTable[i].successor;
        return n_preceding;
      }
    }
    // return n;
    n_preceding = node_queried;
    return n_preceding;
  } else {
    // use remote value
    // create client for remote call
    const node_queried_client = caller(
      `localhost:${node_queried.port}`,
      PROTO_PATH,
      "Node"
    );
    // now grab the remote value
    try {
      n_preceding = await node_queried_client.closest_preceding_finger_remotehelper(
        { id: id, node: node_queried }
      );
    } catch (err) {
      n_preceding = NULL_NODE;
      console.error(
        "closest_preceding_finger call to closest_preceding_finger_remotehelper failed with ",
        err
      );
    }
    // return n;
    return n_preceding;
  }
}

/**
 * RPC equivalent of the pseudocode's closest_preceding_finger() method.
 * It is implemented as simply a wrapper for the local closest_preceding_finger() function.
 *
 * @param id_and_node_queried {id:, node:}, where ID is the key sought
 * @param callback - grpc callback
 *
 */
async function closest_preceding_finger_remotehelper(
  id_and_node_queried,
  callback
) {
  const id = id_and_node_queried.request.id;
  const node_queried = id_and_node_queried.request.node;
  let n_preceding = NULL_NODE;
  try {
    n_preceding = await closest_preceding_finger(id, _self, node_queried);
  } catch (err) {
    console.error(
      "closest_preceding_finger_remotehelper call to closest_preceding_finger failed with ",
      err
    );
    n_preceding = NULL_NODE;
  }
  callback(null, n_preceding);
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
 * @param known_node: known_node structure; e.g., {id, ip, port}
 *   Pass a null known node to force the node to be the first in a new chord.
 */
async function join(known_node) {
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
  if (known_node && confirm_exist(known_node)) {
    // (n');
    await init_finger_table(known_node);
    // update_others();
    await update_others();
  } else {
    // this is the first node
    // initialize predecessor
    predecessor = _self;
  }

  // TODO migrate keys: (predecessor, n]; i.e., (predecessor, _self]
  await migrate_keys();

  // initialize successor table - deviates from SIGCOMM
  successorTable[0] = fingerTable[0].successor;

  if (DEBUGGING_LOCAL) {
    console.log(">>>>>     join          ");
    console.log(
      "The fingerTable[] leaving {",
      _self.id,
      "}.join(",
      known_node.id,
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
 * @param known_node: known_node structure; e.g., {id, ip, port}
 * @returns {boolean}
 */
function confirm_exist(known_node) {
  return !(_self.id == known_node.id);
}

/**
 * Directly implement the pseudocode's init_finger_table() method.
 * @param n_prime
 */
async function init_finger_table(n_prime) {
  // enable debugging output
  const DEBUGGING_LOCAL = false;

  if (DEBUGGING_LOCAL) {
    console.log("vvvvv     vvvvv     init_finger_table     vvvvv     vvvvv");
    console.log(
      "self = ",
      _self.id,
      "; self.successor = ",
      fingerTable[0].successor.id,
      "; finger[0].start = ",
      fingerTable[0].start
    );
    console.log("n' = ", n_prime.id);
  }

  let n_prime_successor = NULL_NODE;
  try {
    n_prime_successor = await find_successor(
      fingerTable[0].start,
      _self,
      n_prime
    );
  } catch (err) {
    n_prime_successor = NULL_NODE;
    console.error("init_finger_table call to find_successor failed with ", err);
  }
  // finger[1].node = n'.find_successor(finger[1].start);
  fingerTable[0].successor = n_prime_successor;

  if (DEBUGGING_LOCAL) {
    console.log("n'.successor (now  self.successor) = ", n_prime_successor);
  }

  // client for newly-determined successor
  let successor_client = caller(
    `localhost:${fingerTable[0].successor.port}`,
    PROTO_PATH,
    "Node"
  );
  // predecessor = successor.predecessor;
  try {
    predecessor = await successor_client.getPredecessor(
      fingerTable[0].successor
    );
  } catch (err) {
    predecessor = NULL_NODE;
    console.error("init_finger_table call to getPredecessor failed with", err);
  }
  // successor.predecessor = n;
  try {
    await successor_client.setPredecessor(_self);
  } catch (err) {
    console.error(
      "init_finger_table call to setPredecessor() failed with ",
      err
    );
  }

  if (DEBUGGING_LOCAL) {
    console.log("init_finger_table: predecessor  ", predecessor);
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
      // finger[i+1].node = n'.find_successor(finger[i+1].start);
      try {
        fingerTable[i + 1].successor = await find_successor(
          fingerTable[i + 1].start,
          _self,
          n_prime
        );
      } catch (err) {
        fingerTable[i + 1].successor = NULL_NODE;
        console.error(
          "init_finger_table call to find_successor() failed with ",
          err
        );
      }
    }
  }
  if (DEBUGGING_LOCAL) {
    console.log("init_finger_table: fingerTable[] =\n", fingerTable);
    console.log("^^^^^     ^^^^^     init_finger_table     ^^^^^     ^^^^^");
  }
}

/**
 * Directly implement the pseudocode's update_others() method.
 *
 */
async function update_others() {
  // enable debugging output
  const DEBUGGING_LOCAL = false;
  if (DEBUGGING_LOCAL) {
    console.log("vvvvv     vvvvv     update_others     vvvvv     vvvvv");
    console.log("_self = ", _self);
  }

  let p_node = NULL_NODE;
  let p_node_search_id;
  let p_node_client;
  // for i = 1 to m
  for (let i = 0; i < HASH_BIT_LENGTH; i++) {
    /* argument for "p = find_predecessor(n - 2^(i - 1))"
            but really 2^(i) because the index is now 0-based
            nonetheless, avoid ambiguity with negative numbers by:
                1- pegging 0 to 2^m with "+ 2**HASH_BIT_LENGTH
                2- taking the mod with "% 2**HASH_BIT_LENGTH"
        */
    p_node_search_id =
      (_self.id - 2 ** i + 2 ** HASH_BIT_LENGTH) % 2 ** HASH_BIT_LENGTH;
    if (DEBUGGING_LOCAL) {
      console.log(
        "i = ",
        i,
        "; find_predecessor(",
        p_node_search_id,
        ") --> p_node"
      );
    }

    // p = find_predecessor(n - 2^(i - 1));
    try {
      p_node = await find_predecessor(p_node_search_id);
    } catch (err) {
      p_node = NULL_NODE;
      console.error(
        "Error from find_predecessor(",
        p_node_search_id,
        ") in update_others().",
        err
      );
    }

    if (DEBUGGING_LOCAL) {
      console.log("p_node = ", p_node);
    }

    // p.update_finger_table(n, i);
    if (_self.id !== p_node.id) {
      p_node_client = caller(`localhost:${p_node.port}`, PROTO_PATH, "Node");
      try {
        await p_node_client.update_finger_table({ node: _self, index: i });
      } catch (err) {
        console.error("update_others: client.update_finger_table error ", err);
      }
    }
  }

  if (DEBUGGING_LOCAL) {
    console.log("^^^^^     ^^^^^     update_others     ^^^^^     ^^^^^");
  }
}

/**
 * RPC that directly implements the pseudocode's update_finger_table() method.
 *
 * @param message - consists of {s_node, finger_index} *
 * @param callback - grpc callback
 */
async function update_finger_table(message, callback) {
  // enable debugging output
  const DEBUGGING_LOCAL = false;

  const s_node = message.request.node;
  const finger_index = message.request.index;

  if (DEBUGGING_LOCAL) {
    console.log("vvvvv     vvvvv     update_finger_table     vvvvv     vvvvv");
    console.log("{", _self.id, "}.fingerTable[] =\n", fingerTable);
    console.log(
      "s_node = ",
      message.request.node.id,
      "; finger_index =",
      finger_index
    );
  }

  // if ( s 'is in' [n, finger[i].node) )
  if (
    isInModuloRange(
      s_node.id,
      _self.id,
      true,
      fingerTable[finger_index].successor.id,
      false
    )
  ) {
    // finger[i].node = s;
    fingerTable[finger_index].successor = s_node;
    // p = predecessor;
    const p_client = caller(
      `localhost:${predecessor.port}`,
      PROTO_PATH,
      "Node"
    );
    // p.update_finger_table(s, i);
    try {
      await p_client.update_finger_table({ node: s_node, index: finger_index });
    } catch (err) {
      console.error(
        "Error updating the finger table of {",
        s_node.id,
        "}.\n\n",
        err
      );
    }

    if (DEBUGGING_LOCAL) {
      console.log(
        "Updated {",
        _self.id,
        "}.fingerTable[",
        finger_index,
        "] to ",
        s_node
      );
      console.log(
        "^^^^^     ^^^^^     update_finger_table     ^^^^^     ^^^^^"
      );
    }

    // TODO: Figure out how to determine if the above had an RC of 0
    // If so call callback({status: 0, message: "OK"}, {});
    callback(null, {});
    return;
  }

  if (DEBUGGING_LOCAL) {
    console.log("^^^^^     ^^^^^     update_finger_table     ^^^^^     ^^^^^");
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
async function update_successor_table() {
  // enable debugging output
  const DEBUGGING_LOCAL = false;
  if (DEBUGGING_LOCAL) {
    console.log(
      "vvvvv     vvvvv     update_successor_table     vvvvv     vvvvv"
    );
    console.log("{", _self.id, "}.successorTable[] =\n", successorTable);
    console.log("s_node = ", fingerTable[0].successor.id);
  }

  // check whether the successor is available
  let successor_seems_ok = false;
  try {
    successor_seems_ok = await check_successor();
  } catch (err) {
    console.error(
      `update_successor_table call to check_successor failed with `,
      err
    );
    successor_seems_ok = false;
  }
  if (successor_seems_ok) {
    // synchronize immediate successor if it is valid
    successorTable[0] = fingerTable[0].successor;
  } else {
    // or prune if the successor is not valid
    while (!successor_seems_ok && successorTable.length > 0) {
      // try current successor again to account for contention or bad luck
      try {
        successor_seems_ok = await check_successor();
      } catch (err) {
        console.error(
          `update_successor_table call to check_successor failed with `,
          err
        );
        successor_seems_ok = false;
      }
      if (successor_seems_ok) {
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
  let successor_successor = NULL_NODE;
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
        successor_successor = await getSuccessor(_self, successorTable[i]);
      } catch (err) {
        console.error(
          `update_successor_table call to getSuccessor failed with `,
          err
        );
        successor_successor = { id: null, ip: null, port: null };
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
          successor_successor.id
        );
      }
      if (
        successor_successor.id !== null &&
        !isInModuloRange(
          successor_successor.id,
          _self.id,
          true,
          successorTable[i].id,
          true
        )
      ) {
        // append the additional value
        successorTable.splice(i + 1, 1, successor_successor);
        successor_seems_ok = true;
      }
    }
  }
  // prune from the bottom
  let i = successorTable.length - 1;
  successor_seems_ok = false;
  successor_successor = { id: null, ip: null, port: null };
  while (
    (!successor_seems_ok || successorTable.length > HASH_BIT_LENGTH) &&
    i > 0
  ) {
    try {
      successor_successor = await getSuccessor(_self, successorTable[i]);
      if (successor_successor.id !== null) {
        successor_seems_ok = true;
      }
    } catch (err) {
      console.error(
        `update_successor_table call to getSuccessor failed with `,
        err
      );
      successor_seems_ok = false;
      successor_successor = NULL_NODE;
    }
    if (!successor_seems_ok || i >= HASH_BIT_LENGTH) {
      // remove successor candidate
      successorTable.pop();
    }
    i -= 1;
  }

  if (DEBUGGING_LOCAL) {
    console.log("New {", _self.id, "}.successorTable[] =\n", successorTable);
    console.log(
      "^^^^^     ^^^^^     update_successor_table     ^^^^^     ^^^^^"
    );
  }

  return successor_seems_ok;
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
  let successor_client;

  let x;
  try {
    successor_client = caller(
      `localhost:${fingerTable[0].successor.port}`,
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
    await stabilize_self();
    x = _self;
  } else {
    // use remote value
    try {
      x = await successor_client.getPredecessor(fingerTable[0].successor);
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
    successor_client = caller(
      `localhost:${fingerTable[0].successor.port}`,
      PROTO_PATH,
      "Node"
    );
    try {
      await successor_client.notify(_self);
    } catch (err) {
      console.error(
        `stabilize call to successor_client.notify failed with `,
        err
      );
    }
  }

  /* TBD 20191103 */
  // update successor table - deviates from SIGCOMM
  try {
    await update_successor_table();
  } catch (err) {
    console.error(`stabilize call to update_successor_table failed with `, err);
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
async function stabilize_self() {
  let predecessor_seems_ok = false;
  if (predecessor.id == null) {
    // this node is in real trouble since its predecessor is no good either
    // TODO try to rescue it by stepping through the rest of its finger table, else destroy it
    predecessor_seems_ok = false;
    return predecessor_seems_ok;
  }
  if (predecessor.id !== _self.id) {
    try {
      // confirm that the predecessor is actually there
      predecessor_seems_ok = await check_predecessor();
    } catch (err) {
      predecessor_seems_ok = false;
      console.error(
        `stabilize_self call to check_predecessor failed with `,
        err
      );
    }
    if (predecessor_seems_ok) {
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
    predecessor_seems_ok = true;
  }
  return predecessor_seems_ok;
}

/**
 * Directly implements the pseudocode's notify() method.
 * @param message
 * @param callback the gRPC callback
 */
async function notify(message, callback) {
  const n_prime = message.request;
  // if (predecessor is nil or n' 'is in' (predecessor, n))
  if (
    predecessor.id == null ||
    isInModuloRange(n_prime.id, predecessor.id, false, _self.id, false)
  ) {
    // predecessor = n';
    predecessor = n_prime;
  }
  callback(null, {});
}

/**
 * Directly implements the pseudocode's fix_fingers() method.
 *
 */
async function fix_fingers() {
  // enable debugging output
  const DEBUGGING_LOCAL = false;
  let n_successor = NULL_NODE;

  // i = random index > 1 into finger[]; but really >0 because 0-based
  // random integer within the range (0, m)
  const i = Math.ceil(Math.random() * (HASH_BIT_LENGTH - 1));
  // finger[i].node = find_successor(finger[i].start);
  try {
    n_successor = await find_successor(fingerTable[i].start, _self, _self);
    if (n_successor.id !== null) {
      fingerTable[i].successor = n_successor;
    }
  } catch (err) {
    console.error(`fix_fingers call to find_successor failed with `, err);
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
 * Directly implements the check_predecessor() method from the IEEE version of the paper.
 *
 * @returns {boolean} true if predecessor was still reasonable; false otherwise.
 */
async function check_predecessor() {
  if (predecessor.id !== null && predecessor.id !== _self.id) {
    const predecessor_client = caller(
      `localhost:${predecessor.port}`,
      PROTO_PATH,
      "Node"
    );
    try {
      // just ask anything
      const x = await predecessor_client.getPredecessor(_self.id);
    } catch (err) {
      console.error(
        `check_predecessor call to getPredecessor failed with `,
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
async function check_successor() {
  // enable debugging output
  const DEBUGGING_LOCAL = false;

  if (DEBUGGING_LOCAL) {
    console.log(
      "{",
      _self.id,
      "}.check_successor(",
      fingerTable[0].successor.id,
      ")"
    );
  }

  let n_successor = NULL_NODE;
  let successor_seems_ok = false;
  if (fingerTable[0].successor.id == null) {
    successor_seems_ok = false;
  } else if (fingerTable[0].successor.id == _self.id) {
    successor_seems_ok = true;
  } else {
    try {
      // just ask anything
      n_successor = await getSuccessor(_self, fingerTable[0].successor);
      if (n_successor.id == null) {
        successor_seems_ok = false;
      } else {
        console.log(
          "{",
          fingerTable[0].successor.id,
          "}.successor =",
          n_successor.id
        );
        successor_seems_ok = true;
      }
    } catch (err) {
      successor_seems_ok = false;
      console.log(
        "Error in check_successor({",
        _self.id,
        "}) call to getSuccessor",
        err
      );
    }
  }
  return successor_seems_ok;
}

/**
 * Placeholder for data migration within the join() call.
 *
 */
async function migrate_keys() {}

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
    await fix_fingers();
  }, 3000);
  setInterval(async () => {
    await check_predecessor();
  }, CHECK_NODE_TIMEOUT_ms);

  // server instantiation
  const server = new grpc.Server();
  server.addService(chord.Node.service, {
    summary,
    fetch,
    remove,
    removeUser_remoteHelper,
    insert,
    insertUser_remoteHelper,
    lookup,
    lookupUser_remoteHelper,
    find_successor_remotehelper,
    getSuccessor_remotehelper,
    getPredecessor,
    setPredecessor,
    closest_preceding_finger_remotehelper,
    update_finger_table,
    notify
  });
  server.bind(
    `${_self.ip}:${_self.port}`,
    grpc.ServerCredentials.createInsecure()
  );
  server.start();
  if (DEBUGGING_LOCAL) {
    console.log(`Serving on ${_self.ip}:${_self.port}`);
  }
}

main();
