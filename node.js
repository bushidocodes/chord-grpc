const path = require("path");
const grpc = require("grpc");
const users = require("./data/tinyUsers.json");
const protoLoader = require("@grpc/proto-loader");
const minimist = require("minimist");

const PROTO_PATH = path.resolve(__dirname, "./protos/chord.proto");

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

const FingerTable = [
  {
    start: null,
    successor: { id: null, ip: null, port: null }
  }
];

let predecessor = { id: null, ip: null, port: null };

const _self = { id: null, ip: null, port: null };

function isInModuloRange(input_value, lower_bound, include_lower, upper_bound, include_upper) {
    /*
        USAGE
        include_start == true means [start, ...
        include_start == false means (start, ...
        include_end == true means ..., end]
        include_end == false means ..., end)
    */
    if (include_lower && include_upper) {
        if (lower_bound > upper_bound) {
            //looping through 0
            return (input_value >= lower_bound || input_value <= upper_bound);
        } else {
            return (input_value >= lower_bound && input_value <= upper_bound);
        }
    } else if (include_lower && !include_upper) {
        if (lower_bound > upper_bound) {
            //looping through 0
            return (input_value >= lower_bound || input_value < upper_bound);
        } else {
            return (input_value >= lower_bound && input_value < upper_bound);
        }
    } else if (!include_lower && include_upper) {
        if (lower_bound > upper_bound) {
            //looping through 0
            return (input_value > lower_bound || input_value <= upper_bound);
        } else {
            // start < end
            return (input_value > lower_bound && input_value <= upper_bound);
        }
    } else {
        //include neither
        if (lower_bound > upper_bound) {
            //looping through 0
            return (input_value > lower_bound || input_value < upper_bound);
        } else {
            // start < end
            return (input_value > lower_bound && input_value < upper_bound);
        }
    }    
}  

function summary(_, callback) {
  console.log("vvvvv     vvvvv     Summary     vvvvv     vvvvv");
  console.log("FingerTable: \n", FingerTable);
  console.log("Predecessor: ", predecessor);
  console.log("^^^^^     ^^^^^     End Summary     ^^^^^     ^^^^^")
  callback(null, _self);
}

function fetch({ request: { id } }, callback) {
  console.log(`Requested User ${id}`);
  if (!users[id]) {
    callback({ code: 5 }, null); // NOT_FOUND error
  } else {
    callback(null, users[id]);
  }
}

function insert({ request: user }, callback) {
  if (users[user.id]) {
    const message = `Err: ${user.id} already exits`;
    console.log(message);
    callback({ code: 6, message }, null); // ALREADY_EXISTS error
  } else {
    users[user.id] = user;
    const message = `Inserted User ${user.id}:`;
    console.log(message);
    callback({ status: 0, message }, null);
  }
}

/* added 20191019 */
async function find_successor_remote(message, callback) {
    /** 
     * RPC equivalent of the pseudocode's find_successor().
     * It is implemented as simply a wrapper for the find_successor_local() function.
     */
    // enable debugging output
    const DEBUGGING_LOCAL = false;

    // extract message parameter
    const id = message.request.id;

    /* vvvvv     vvvvv     debugging code     vvvvv     vvvvv */
    if (DEBUGGING_LOCAL) {
        console.log("vvvvv     vvvvv     find_successor_remote     vvvvv     vvvvv");
        console.log("id = ", message.request.id);
    }
    /* ^^^^^     ^^^^^     TBD debugging code     ^^^^^     ^^^^^ */

    // n' = find_predecessor(id);
    // return n'.successor;
    let n_prime_successor = await find_successor_local(id);

    /* vvvvv     vvvvv     TBD debugging code     vvvvv     vvvvv */
    if (DEBUGGING_LOCAL) {
        console.log("find_successor_remote:  n_prime_successor: ", n_prime_successor.id);
        console.log("^^^^^     ^^^^^     find_successor_remote     ^^^^^     ^^^^^");
    }
    /* ^^^^^     ^^^^^     TBD debugging code     ^^^^^     ^^^^^ */
    callback(null, n_prime_successor);
}

/* added 20191019 */
async function find_successor_local(id) {
    /** 
     * This function directly implements the pseudocode's find_successor() method.
     * 
    */
    // enable debugging output
    const DEBUGGING_LOCAL = false;

    /* vvvvv     vvvvv     TBD debugging code     vvvvv     vvvvv */
    if (DEBUGGING_LOCAL) {
        console.log("vvvvv     vvvvv     find_successor_local     vvvvv     vvvvv");
    }
    /* ^^^^^     ^^^^^     TBD debugging code     ^^^^^     ^^^^^ */

    // n' = find_predecessor(id);
    let n_prime = await find_predecessor(id);

    /* vvvvv     vvvvv     TBD debugging code     vvvvv     vvvvv */
    if (DEBUGGING_LOCAL) {
        console.log("find_successor_local: _self is ", _self.id);
        console.log("find_successor_local: n_prime is ", n_prime.id);
    }
    /* ^^^^^     ^^^^^     TBD debugging code     ^^^^^     ^^^^^ */

    // get n'.successor either locally or remotely
    let n_prime_successor = await getSuccessor(id, _self, n_prime);

    /* vvvvv     vvvvv     TBD debugging code     vvvvv     vvvvv */
    if (DEBUGGING_LOCAL) {
        console.log("find_successor_local: n_prime_successor = ", n_prime_successor.id);
        console.log("^^^^^     ^^^^^     find_successor_local     ^^^^^     ^^^^^");
    }
    /* ^^^^^     ^^^^^     TBD debugging code     ^^^^^     ^^^^^ */

    // return n'.successor;
    return n_prime_successor;
}

/* added 20191023 */
async function find_predecessor(id) {
    /** 
     * This function directly implements the pseudocode's find_predecessor() method with the exception of the limits on the while loop.
     * 
    */
    // enable debugging output
    const DEBUGGING_LOCAL = false;

    /* vvvvv     vvvvv     TBD debugging code     vvvvv     vvvvv */
    if (DEBUGGING_LOCAL) {
        console.log("vvvvv     vvvvv     find_predecessor     vvvvv     vvvvv");
        console.log("id = ", id);
    }
    /* ^^^^^     ^^^^^     TBD debugging code     ^^^^^     ^^^^^ */

    // n' = n;
    let n_prime = _self;
    let prior_n_prime = { id: null, ip: null, port: null };
    let n_prime_successor = await getSuccessor(id, _self, n_prime);

    /* vvvvv     vvvvv     TBD debugging code     vvvvv     vvvvv */
    if (DEBUGGING_LOCAL) {
        console.log("before while: n_prime = ", n_prime.id,  "; n_prime_successor = ", n_prime_successor.id);
    }
    /* ^^^^^     ^^^^^     TBD debugging code     ^^^^^     ^^^^^ */

    // (maximum chord nodes = 2^m) * (length of finger table = m)
    let iteration_counter = 2 ** HASH_BIT_LENGTH * HASH_BIT_LENGTH;
    // while (id 'not-in' (n', n'.successor] )
    while (!(isInModuloRange(id, n_prime.id, false, n_prime_successor.id, true))
        && (n_prime.id !== n_prime_successor.id)
        && (n_prime.id !== prior_n_prime.id)
        && (iteration_counter >= 0)) {
        // loop should exit if n' and its successor are the same
        // loop should exit if n' and the prior n' are the same
        // loop should exit if the iterations are ridiculous
        // update loop protection
        iteration_counter--;
        // n' = n'.closest_preceding_finger(id);
        n_prime = await closest_preceding_finger(id, _self, n_prime);

        /* vvvvv     vvvvv     TBD debugging code     vvvvv     vvvvv */
        if (DEBUGGING_LOCAL) {
            console.log("=== while iteration ", iteration_counter, " ===");
            console.log("n_prime = ", n_prime);
        }
        /* ^^^^^     ^^^^^     TBD debugging code     ^^^^^     ^^^^^ */

        n_prime_successor = await getSuccessor(id, _self, n_prime);
        // store state
        prior_n_prime = n_prime;

        /* vvvvv     vvvvv     TBD debugging code     vvvvv     vvvvv */
        if (DEBUGGING_LOCAL) {
            console.log("n_prime_successor = ", n_prime_successor);
        }
        /* ^^^^^     ^^^^^     TBD debugging code     ^^^^^     ^^^^^ */

    }

    /* vvvvv     vvvvv     TBD debugging code     vvvvv     vvvvv */
    if (DEBUGGING_LOCAL) {
        console.log("^^^^^     ^^^^^     find_predecessor     ^^^^^     ^^^^^");
    }
    /* ^^^^^     ^^^^^     TBD debugging code     ^^^^^     ^^^^^ */

    // return n';
    return n_prime;
}

/* added 20191019 */
async function getSuccessor(id, node_querying, node_to_query) {
    /**
     * Return the successor of a given node id by either a local lookup or an RPC.
     * If the querying node is the same as the queried node, it will be a local lookup.
     */
    // enable debugging output
    const DEBUGGING_LOCAL = false;

    /* vvvvv     vvvvv     TBD debugging code     vvvvv     vvvvv */
    if (DEBUGGING_LOCAL) {
        console.log("vvvvv     vvvvv     getSuccessor     vvvvv     vvvvv");
        console.log("id = ", id, "node_querying is ", node_querying, "node being queried is ", node_to_query);
    }
    /* ^^^^^     ^^^^^     TBD debugging code     ^^^^^     ^^^^^ */

    // get n.successor either locally or remotely
    let n_successor;
    if (node_querying.id == node_to_query.id) {
        // use local value
        n_successor = FingerTable[0].successor;
    } else {
        // use remote value
        // create client for remote call
        const node_to_query_client = caller(`localhost:${node_to_query.port}`, PROTO_PATH, "Node");
        // now grab the remote value
        try {
            n_successor = await node_to_query_client.getSuccessor_remote(node_to_query);
        } catch (err) {
            console.error("getSuccessor_remote error in getSuccessor() ", err);
        }
    }

    /* vvvvv     vvvvv     TBD debugging code     vvvvv     vvvvv */
    if (DEBUGGING_LOCAL) {
        console.log("returning n_successor = ", n_successor);
        console.log("^^^^^     ^^^^^     getSuccessor     ^^^^^     ^^^^^");
    }
    /* ^^^^^     ^^^^^     TBD debugging code     ^^^^^     ^^^^^ */

    return n_successor;
}

/* added 20191019 */
async function closest_preceding_finger(id, node_querying, node_to_query) {
    /**
     * Directly implement the pseudocode's closest_preceding_finger() method.
     * However, it is able to discern whether to do a local lookup or an RPC.
     * If the querying node is the same as the queried node, it will stay local.
     */
  let n_preceding;
  if (node_querying.id == node_to_query.id) {
    // use local value
    // for i = m downto 1
    for (let i = HASH_BIT_LENGTH - 1; i >= 0; i--) {
      // if ( finger[i].node 'is-in' (n, id) )
      if ( isInModuloRange(FingerTable[i].successor.id, node_to_query.id, false, id, false) ) {
        // return finger[i].node;
        n_preceding = FingerTable[i].successor;
        return n_preceding;
      }
    }
    // return n;
    n_preceding = node_to_query;
    return n_preceding;
  } else {
    // use remote value
    // create client for remote call
    const node_to_query_client = caller(`localhost:${node_to_query.port}`, PROTO_PATH, "Node");
    // now grab the remote value
    try {
      n_preceding = await node_to_query_client.getClosestPrecedingFinger({id: id, node: node_to_query});
    } catch (err) {
      console.error("getClosestPrecedingFinger error in closest_preceding_finger() ", err);
    }
    // return n;
    return n_preceding;
  }
}

/* added 20191019 */
async function getClosestPrecedingFinger(id_and_node_to_query, callback) {
    /** 
     * RPC equivalent of the pseudocode's closest_preceding_finger() method.
     * It is implemented as simply a wrapper for the local closest_preceding_finger() function.
     */
    const id = id_and_node_to_query.request.id;
    const node_to_query = id_and_node_to_query.request.node;
    const n_preceding = await closest_preceding_finger(id, _self, node_to_query);
    callback(null, n_preceding);
}

async function getSuccessor_remote(thing, callback) {
    callback(null, FingerTable[0].successor);
}

async function getPredecessor(thing, callback) {
  callback(null, predecessor);
}

/* modified 20191019 */
// TODO: Determine proper use of RC0 with gRPC
//  /*{status: 0, message: "OK"}*/
function setPredecessor(message, callback) {
    /**
     * RPC to replace the value of the node's predecessor.
     */
    // enable debugging output
    const DEBUGGING_LOCAL = false;

    /* vvvvv     vvvvv     debugging code     vvvvv     vvvvv */
    if (DEBUGGING_LOCAL) {
        console.log("vvvvv     vvvvv     setPredecessor     vvvvv     vvvvv");
        console.log("Self = ", _self);
        console.log("Self's original predecessor = ", predecessor);
    }
    /* ^^^^^     ^^^^^     debugging code     ^^^^^     ^^^^^ */

    predecessor = message.request; //message.request is node

    /* vvvvv     vvvvv     debugging code     vvvvv     vvvvv */
    if (DEBUGGING_LOCAL) {
        console.log("Self's new predecessor = ", predecessor);
        console.log("^^^^^     ^^^^^     setPredecessor     ^^^^^     ^^^^^");
    }
    /* ^^^^^     ^^^^^     debugging code     ^^^^^     ^^^^^ */

    callback(null, {});
}

/* modified 20191022 */
// Pass a null as known_node to force the node to be the first in the cluster
async function join(known_node) {
    /**
     * Implement the pseudocode's "heavyweight" version of the join() method.
     * Described in Figure 6.
     */
    // enable debugging output
    const DEBUGGING_LOCAL = true;

    // remove dummy template initializer from finger table
    FingerTable.pop();
    // initialize finger table with reasonable values
    for (let i = 0; i < HASH_BIT_LENGTH; i++) {
        FingerTable.push({
            start: (_self.id + 2 ** i) % (2 ** HASH_BIT_LENGTH),
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

    // TODO move keys: (predecessor, n]; i.e., (predecessor, _self]

    /* vvvvv     vvvvv     TBD debugging code     vvvvv     vvvvv */
    if (DEBUGGING_LOCAL) {
        console.log(">>>>>     join          ");
        console.log("The FingerTable[] leaving join() is:\n", FingerTable);
        console.log("The predecessor leaving join() is ", predecessor);
        console.log("          join     <<<<<\n");
    }
    /* ^^^^^     ^^^^^     TBD debugging code     ^^^^^     ^^^^^ */
}

function confirm_exist(known_node) {
    /**
     * Determine whether a node exists by pinging it.
     */
  // TODO: confirm_exist actually needs to ping the endpoint to ensure it's real
  return !(_self.id == known_node.id);
}

/* modified 20191021 */
async function init_finger_table(n_prime) {
    /**
     * Directly implement the pseudocode's init_finger_table() method.
     */
    // enable debugging output
    const DEBUGGING_LOCAL = true;

    /* vvvvv     vvvvv     TBD debugging code     vvvvv     vvvvv */
    if (DEBUGGING_LOCAL) {
        console.log("vvvvv     vvvvv     init_finger_table     vvvvv     vvvvv");
        console.log("self = ", _self.id, "; self.successor = ", FingerTable[0].successor.id, "; finger[0].start = ", FingerTable[0].start);
        console.log("n' = ", n_prime.id);
    }
    /* ^^^^^     ^^^^^     TBD debugging code     ^^^^^     ^^^^^ */

    let n_prime_successor;
    // client for possible known node
    const n_prime_client = caller(`localhost:${n_prime.port}`, PROTO_PATH, "Node");
    try {
        n_prime_successor = await n_prime_client.find_successor_remote({ id: FingerTable[0].start });
    } catch (err) {
        console.error("find_successor_remote() error in init_finger_table() ", err);
    }
    // finger[1].node = n'.find_successor(finger[1].start);
    FingerTable[0].successor = n_prime_successor;

    /* vvvvv     vvvvv     TBD debugging code     vvvvv     vvvvv */
    if (DEBUGGING_LOCAL) {
        console.log("n'.successor (now  self.successor) = ", n_prime_successor);
    }
    /* ^^^^^     ^^^^^     TBD debugging code     ^^^^^     ^^^^^ */

    // client for newly-determined successor
    let successor_client = caller(`localhost:${FingerTable[0].successor.port}`, PROTO_PATH, "Node");
    // predecessor = successor.predecessor;
    try {
        predecessor = await successor_client.getPredecessor(FingerTable[0].successor);
    } catch (err) {
        console.error("getPredecessor() error in init_finger_table() ", err);
    }
    // successor.predecessor = n;
    try {
        await successor_client.setPredecessor(_self);
    } catch (err) {
        console.error("setPredecessor() error in init_finger_table() ", err);
    }

    /* vvvvv     vvvvv     TBD debugging code     vvvvv     vvvvv */
    if (DEBUGGING_LOCAL) {
        console.log("init_finger_table: predecessor  ", predecessor);
    }
    /* ^^^^^     ^^^^^     TBD debugging code     ^^^^^     ^^^^^ */

    // for (i=1 to m-1){}, where 1 is really 0, and skip last element
    for (let i = 0; i < HASH_BIT_LENGTH - 1; i++) {
        // if ( finger[i+1].start 'is in' [n, finger[i].node) )
        if (isInModuloRange(FingerTable[i + 1].start, _self.id, true, FingerTable[i].successor.id, false)) {
            // finger[i+1].node = finger[i].node;
            FingerTable[i + 1].successor = FingerTable[i].successor;
        } else {
            // finger[i+1].node = n'.find_successor(finger[i+1].start);
            try {
                FingerTable[i + 1].successor = await n_prime_client.find_successor_remote(FingerTable[i + 1].start);
            } catch (err) {
                console.error("find_successor_remote error in init_finger_table ", err);
            }
        }
    }

    /* vvvvv     vvvvv     TBD debugging code     vvvvv     vvvvv */
    if (DEBUGGING_LOCAL) {
        console.log("init_finger_table: FingerTable[] =\n", FingerTable);
        console.log("^^^^^     ^^^^^     init_finger_table     ^^^^^     ^^^^^");
    }
    /* ^^^^^     ^^^^^     TBD debugging code     ^^^^^     ^^^^^ */
}

/* modified 20191022 */
async function update_others() {
    /**
     * Directly implement the pseudocode's update_others() method.
     */
    // enable debugging output
    const DEBUGGING_LOCAL = true;

    /* vvvvv     vvvvv     debugging code     vvvvv     vvvvv */
    if (DEBUGGING_LOCAL) {
        console.log("vvvvv     vvvvv     update_others     vvvvv     vvvvv");
        console.log("_self = ", _self);
    }
    /* ^^^^^     ^^^^^     debugging code     ^^^^^     ^^^^^ */

    let p_node;
    let p_node_search_id;
    let p_node_client;
    // for i = 1 to m
    for (let i = 0; i < HASH_BIT_LENGTH; i++) {
        // argument for "p = find_predecessor(n - 2^(i - 1))"
        //    but really 2^(i) because the index is now 0-based
        //    nonetheless, avoid ambiguity with negative numbers by:
        //      pegging 0 to 2^m with "+ 2**HASH_BIT_LENGTH
        //      and then taking the mod with "% 2**HASH_BIT_LENGTH"
        p_node_search_id = (_self.id - 2**i + 2**HASH_BIT_LENGTH) % (2**HASH_BIT_LENGTH);

        /* vvvvv     vvvvv     debugging code     vvvvv     vvvvv */
        if (DEBUGGING_LOCAL) {
            console.log("i = ", i, "; find_predecessor(", p_node_search_id, ") --> p_node");
        }
        /* ^^^^^     ^^^^^     debugging code     ^^^^^     ^^^^^ */

        // p = find_predecessor(n - 2^(i - 1));
         try {
            p_node = await find_predecessor(p_node_search_id);
        } catch (err) {
            console.error("\nError from find_predecessor(", p_node_search_id, ") in update_others().\n");
        }

        /* vvvvv     vvvvv     debugging code     vvvvv     vvvvv */
        if (DEBUGGING_LOCAL) {
            console.log("p_node = ", p_node);
        }
        /* ^^^^^     ^^^^^     debugging code     ^^^^^     ^^^^^ */

        // p.update_finger_table(n, i);
        if (_self.id !== p_node.id) {
            p_node_client = caller(`localhost:${p_node.port}`, PROTO_PATH, "Node");
            try {
                await p_node_client.update_finger_table({ node: _self, index: i });
            } catch (err) {
                console.log(`localhost:${p_node.port}`);
                console.error("update_others: client.update_finger_table error ", err);
            }
        }
    }

    /* vvvvv     vvvvv     debugging code     vvvvv     vvvvv */
    if (DEBUGGING_LOCAL) {
        console.log("^^^^^     ^^^^^     update_others     ^^^^^     ^^^^^");
    }
    /* ^^^^^     ^^^^^     debugging code     ^^^^^     ^^^^^ */
}

/* modified 20191022 */
async function update_finger_table(message, callback) {
    /**
     * RPC that directly implements the pseudocode's update_finger_table() method.
     */
    // enable debugging output
    const DEBUGGING_LOCAL = true;

    const s_node = message.request.node;
    const index = message.request.index;

    /* vvvvv     vvvvv     debugging code     vvvvv     vvvvv */
    if (DEBUGGING_LOCAL) {
        console.log("vvvvv     vvvvv     update_finger_table     vvvvv     vvvvv");
        console.log("{", _self.id, "}.FingerTable[] =\n", FingerTable);
        console.log("s_node = ", message.request.node, "; index =", index);
        if (message.request.node == null) {
            console.log("Message:\n", message);
        }
    }
    /* ^^^^^     ^^^^^     debugging code     ^^^^^     ^^^^^ */

    // if ( s 'is in' [n, finger[i].node) )
    if (isInModuloRange(s_node.id, _self.id, true, FingerTable[index].successor.id, false)) {
        // finger[i].node = s;
        FingerTable[index].successor = s_node;
        // p = predecessor;
        const p_client = caller(`localhost:${predecessor.port}`, PROTO_PATH, "Node");
        // p.update_finger_table(s, i);
        console.log(`localhost:${predecessor.port}`);
        try {
            await p_client.update_finger_table({ s_node, index });
        } catch (err) {
            console.error("Error updating the finger table of {", s_node.id, "}.\n\n", err);
        }

        /* vvvvv     vvvvv     debugging code     vvvvv     vvvvv */
        if (DEBUGGING_LOCAL) {
            console.log("Updated {", _self.id, "}.FingerTable[", index, "] to ", s_node);
            console.log("^^^^^     ^^^^^     update_finger_table     ^^^^^     ^^^^^");
        }
        /* ^^^^^     ^^^^^     debugging code     ^^^^^     ^^^^^ */

        // TODO: Figure out how to determine if the above had an RC of 0
        // If so call callback({status: 0, message: "OK"}, {});
        callback(null, {});
    }

    /* vvvvv     vvvvv     debugging code     vvvvv     vvvvv */
    if (DEBUGGING_LOCAL) {
        console.log("^^^^^     ^^^^^     update_finger_table     ^^^^^     ^^^^^");
    }
    /* ^^^^^     ^^^^^     debugging code     ^^^^^     ^^^^^ */

    // TODO: Figure out how to determine if the above had an RC of 0
    //callback({ status: 0, message: "OK" }, {});
    callback(null, {});
}

/* modified 20191022 */
async function stabilize() {
    /**
     * Directly implements the pseudocode's stabilize() method.
     * Except that it adds a logic to stabilize a node whose predecessor is itself,
     *   as would be the case for the initial node in a chord.
     */
    let x;
    let successor_client = caller(`localhost:${FingerTable[0].successor.port}`, PROTO_PATH, "Node");
    // x = successor.predecessor;
    if (FingerTable[0].successor.id == _self.id) {
        // use local value
        stabilize_self();
        x = _self;
    } else {
        // use remote value
        try {
            x = await successor_client.getPredecessor(FingerTable[0].successor);
        } catch (err) {
            x = _self;
            console.log("Warning! \"successor.predecessor\" failed in stabilize().");
            // TODO: consider looping through the rest of the fingers or asking the predecessor.
        }
    }
    // if (x 'is in' (n, n.successor))
    if (isInModuloRange(x.id, _self.id, false, FingerTable[0].successor.id, false)) {
        // successor = x;
        FingerTable[0].successor = x;
    }
 
    /* vvvvv     vvvvv     debugging code     vvvvv     vvvvv */
    console.log(">>>>>     stabilize          ");
    console.log("{", _self.id, "}.FingerTable[] leaving stabilize() is:\n", FingerTable);
    console.log("And predecessor is ", predecessor);
    console.log("          stabilize     <<<<<");
    /* ^^^^^     ^^^^^     debugging code     ^^^^^     ^^^^^ */  
  
    // successor.notify(n);
    if (_self.id !== FingerTable[0].successor.id) {
        successor_client = caller(`localhost:${FingerTable[0].successor.port}`, PROTO_PATH, "Node");
        try {
            await successor_client.notify(_self);
        } catch (err) {
            // no need for handler
        }
    }
}

/* added 20191021 */
async function stabilize_self() {
    /**
     * This function tries to kick a node, such as the original, with a successor of self.
     * The kick comes from setting the successor to be equal to the predecessor.
     * 
     * Return true if it was a good kick; false if bad kick.
    */
    let other_node_client;
    if (predecessor.id == null) {
        // this node is in real trouble since its predecessor is no good either
        // TODO try to rescue it by stepping through the rest of its finger table else destroy it
        return false;
    }
    if (predecessor.id !== _self.id) {
        other_node_client = caller(`localhost:${predecessor.port}`, PROTO_PATH, "Node");
        try {
            // confirm that the predecessor is actually there
            await other_node_client.getPredecessor(_self);
            // then kick by setting the successor to the same as the predecessor
            FingerTable[0].successor = predecessor;
        } catch (err) {
            console.error(err);
        }
    } else {
        console.log("\nWarning: {", _self.id, "} is isolated because",
            "predecessor is", predecessor.id,
            "and successor is", FingerTable[0].successor.id, ".");
        return false;
    }
    return true;
}

/* modified 20191021 */
async function notify(message, callback) {
    /**
     * Directly implements the pseudocode's notify() method.
     */
    const n_prime = message.request;
    // if (predecessor is nil or n' 'is in' (predecessor, n))
    if ( (predecessor.id == null)
        || isInModuloRange(n_prime.id, predecessor.id, false, _self.id, false) ) {
        // predecessor = n';
        predecessor = n_prime;
    }
    callback(null, {});
}

/* modified 20191022 */
async function fix_fingers() {
    /**
     * Directly implements the pseudocode's fix_fingers() method.
     */
    // enable debugging output
    const DEBUGGING_LOCAL = true;

    // i = random index > 1 into finger[]; but really >0 because 0-based
    // random integer within the range (0, m)
    const i = Math.ceil(Math.random() * (HASH_BIT_LENGTH - 1));
    // finger[i].node = find_successor(finger[i].start);
    FingerTable[i].successor = await find_successor_local(FingerTable[i].start);
    if (DEBUGGING_LOCAL) {
        console.log("\n>>>>>     Fix {", _self.id, "}.FingerTable[", i, "], with start = ", FingerTable[i].start, ".");
        console.log("     FingerTable[", i, "] =", FingerTable[i].successor, "     <<<<<\n");
    }
}

/* added 20191021 */
async function check_predecessor() {
    /**
     * Directly implements the check_predecessor() method from the MIT version of the paper.
     */
    if ( (predecessor.id !== null) && (predecessor.id !== _self.id) ) {
        const predecessor_client = caller(`localhost:${predecessor.port}`, PROTO_PATH, "Node");
        try {
            // just ask anything
            const x = await predecessor_client.getPredecessor(_self.id);
        } catch (err) {
            // predecessor = nil;
            predecessor = { id: null, ip: null, port: null };
        }
    }
}

/* modified 20191021 */
async function main() {
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

    // Periodically run stabilize and fix_fingers
    // TODO this application of async/await may not be appropriate
    setInterval(async () => { await stabilize() }, 3000);
    setInterval(async () => { await fix_fingers() }, 3000);
    setInterval(async () => { await check_predecessor() }, 1000);

    const server = new grpc.Server();
    server.addService(chord.Node.service, {
        summary,
        fetch,
        insert,
        find_successor_remote,
        getSuccessor_remote,
        getPredecessor,
        setPredecessor,
        getClosestPrecedingFinger,
        update_finger_table,
        notify
    });
    server.bind(
        `${_self.ip}:${_self.port}`,
        grpc.ServerCredentials.createInsecure()
    );
    console.log(`Serving on ${_self.ip}:${_self.port}`);
    server.start();
}

main();



