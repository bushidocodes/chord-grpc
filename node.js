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
    start: 0,
    //stop: 0,
    successor: { id: 0, ip: null, port: null }
  }
];

let predecessor = { id: 0, ip: null, port: null };

/* 
  Careful with standalone 'successor'
  It needs to always point to FingerTable[0].successor
  //const successor = FingerTable[0].successor;*/
const _self = { id: 0, ip: null, port: null };

function summary(_, callback) {
  console.log("Summary request received");
  console.log("FingerTable: ");
  for (let i = 0; i < HASH_BIT_LENGTH; i++) {
    console.log(FingerTable[i]);
  }
  console.log("Predecessor: ");
  console.log(predecessor);
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

async function findSuccessor(message, callback) {
  const id = message.request.id;
  let predecessorNode;
  try {
    predecessorNode = await findPredecessor(id);
  } catch (err) {
    console.error("findPredecessor: ", err);
  }
  const predClient = caller(
    `localhost:${predecessorNode.port}`,
    PROTO_PATH,
    "Node"
  );
  // return n'.successor
  let temp;
  try {
    temp = await predClient.getSuccessor(_self);//_self is trash data
  } catch (err) {
    console.error("findSuccessor client.getSuccessor: ", err);
  }
  callback(null, temp);
}

function closestPrecedingFinger(message, callback) {
  const id = message.request.id;
  //console.log("CPF id: ");
  //console.log(id);

  callback(null, localClosestPrecedingFinger(id));
}

function getSuccessor(thing, callback) {
  callback(null, FingerTable[0].successor);
}

function getPredecessor(thing, callback) {
  //console.log(thing);
  callback(null, predecessor);
}

// TODO: Determine proper use of RC0 with gRPC
//  /*{status: 0, message: "OK"}*/
function setPredecessor(message, callback) {
  //console.log("setPedecesssor message: ");
  //console.log(message);
  predecessor = message.request; //message.request is node
  callback(null, {});
}

function setSuccessor(message, callback) {
  FingerTable[0].successor = message.request;
  callback(null, {});
}

async function update_finger_table({request : {node, index}}, callback) {
  console.log("node: ");
  console.log(node);
  console.log("_self: ");
  console.log(_self);
  console.log("index: ");
  console.log(index);
  console.log("FingerTable[index]: ");
  console.log(FingerTable[index]);
  // console.log("index: ");
  // console.log(index);
  // console.log("Callback: ");
  // console.log(callback);
  if (node.id >= _self.id && node.id < FingerTable[index].successor.id) {
    // finger[i].node = s
    FingerTable[index].successor = node;
    // p = predecessor
    const client = caller(`localhost:${predecessor.port}`, PROTO_PATH, "Node");
    // p.update_finger_table(s, i)
    console.log(`localhost:${predecessor.port}`);
    try {
      await client.update_finger_table({node, index});
    } catch (err) {
      console.error("client.update_finger_table error ", err);
    }
    // TODO: Figure out how to determine if the above had an RC of 0
    // If so call callback({status: 0, message: "OK"}, {});
  } else {
    //callback({ status: 0, message: "OK" }, {});
    callback(null, {});
  }
}

// This is used by other node's to update our predecessor
// Think... setPredecessor... with a check
function notify(node, callback) {
  if (node.id > predecessor.id && node.id < _self.id) {
    // predecessor = n';
    predecessor = node;
    callback(null, {});
    //{ status: 0, message: "OK" }
  }
}

// TODO: Does this need to be a gRPC call or just a local function?
// Understand the implementation and decide if there is anything is the
// commented out code that we've missed (or can we delete it)

async function findPredecessor(id) {
  console.log("findPredecessor, id: ", id);
  // n' = n
  let n_prime = _self;
  // while(); a.k.a., step through the finger table
  for (let i = 0; i < HASH_BIT_LENGTH; i++) {
    console.log("n_prime: ")
    console.log(n_prime);
    const client = caller(`localhost:${n_prime.port}`, PROTO_PATH, "Node");
    let n_prime_successor;
    try {
      n_prime_successor =
        n_prime == _self
          ? FingerTable[0].successor
          : await client.getSuccessor(_self);
    } catch (err) {
      console.error(
        `I am localhost:${_self.port} and I am connecting to localhost:${n_prime.port}`
      );
      console.error("findPredecessor client.getSuccessor error ", err);
    }
    // find first range that doesn't contain the key
    if (!rangeContainsKey(id, n_prime, n_prime_successor)) {
      // if target not between prime and prime's successor
      // n' = n'.closestPrecedingFinger(id)
      try {
        //console.log("before CPF, n_prime = ", n_prime);
        n_prime =
          n_prime == _self
            ? localClosestPrecedingFinger(id)
            : await client.closestPrecedingFinger(id);
        //console.log("after CPF, n_prime = ", n_prime);
      } catch (err) {
        console.error("client.closestPrecedingFinger error ", err);
      }
    } else {
      break;
    }
  }
  // callback(null, {n_prime});
  return n_prime;

  // Note: This was code that predated the stuff above... tried to recurse

  // current = closestPrecedingFinger(node);
  // if( node.id <= _self.id || node.id > FingerTable[0].id){
  //   const client = new chord.Node(`localhost:${_self.port}`, grpc.credentials.createInsecure());
  //   return client.findPredecessor(node);
  // }
  // else{
  //   return _self.id;
  // }
}

//////////////////////////////
// Local Functions          //
//////////////////////////////

function localClosestPrecedingFinger(id) {
  // step through finger table in reverse
  for (let i = HASH_BIT_LENGTH - 1; i >= 0; i--) {
    // TODO: Check this again - fails in a case of node 4 with succ 0 asking for closest to 1
    console.log("self, successor, target, i: ", _self.id, FingerTable[i].successor.id, id, i);
    
    if(_self.id > id){
      if (FingerTable[i].successor.id > _self.id || FingerTable[i].successor.id < id)
      {
        return FingerTable[i].successor;
      }
    } else if (FingerTable[i].successor.id > _self.id && FingerTable[i].successor.id < id)
      {
        return FingerTable[i].successor;
      }

  }
  return _self;
}

// Pass a null as known_node to force the node to be the first in the cluster
async function join(known_node) {
  // if (n')
  // remove dummy template initializer
  FingerTable.pop();
  // initialize table with reasonable values
  for (let i = 0; i < HASH_BIT_LENGTH; i++) {
    FingerTable.push({
      start: (_self.id + 2 ** i) % 2 ** HASH_BIT_LENGTH,
      successor: _self
    });
  }
  predecessor = _self;

  if (known_node && confirm_exist(known_node)) {
    await initFingerTable(known_node);
    await update_others();
  }

  console.log("FingerTable: ");
  for (let i = 0; i < HASH_BIT_LENGTH; i++) {
    console.log(FingerTable[i]);
  }
}

function confirm_exist(known_node) {
  // TODO: confirm_exist actually needs to ping the endpoint to ensure it's real
  return !_self.id == known_node.id;
}

async function initFingerTable(known_node) {
  // client for possible known node
  const knownClient = caller(`localhost:${known_node.port}`, PROTO_PATH, "Node");

  // finger[1].node = n'.find_successor(finger[1].start)
  let temp;
  try {
    temp = await knownClient.findSuccessor(FingerTable[0].start);
  } catch (err) {
    console.error("knownClient.findSuccessor error ", err);
  }
  //if _self is a better succcessor than temp
  if ( _self.id < temp.id || temp.id < FingerTable[0].start)
        {
          temp = _self;
        }
  FingerTable[0].successor = temp;

  // client for newly-determined successor
  const successorClient = caller(
    `localhost:${FingerTable[0].successor.port}`,
    PROTO_PATH,
    "Node"
  );
  // predecessor = successor.predecessor
  try {
    predecessor = await successorClient.getPredecessor(_self);
  } catch (err) {
    console.error("successorClient.getPredecessor error ", err);
  }
  // successor.predecessor = n
  try {
    await successorClient.setPredecessor(_self);
  } catch (err) {
    console.error("client.setPredecessor error ", err);
  }
  // predecessor.successorr = n
  try {
    await successorClient.setSuccessor(_self);
  } catch (err) {
    console.error("client.setSuccessor error ", err);
  }

  // for (i=1 to m-1){}, where 1 is really 0, and skip last element
  for (let i = 0; i < HASH_BIT_LENGTH - 1; i++) {
    if (
      FingerTable[i + 1].start >= _self.id &&
      FingerTable[i + 1].start < FingerTable[i].successor.id
    ) {
      // finger[i+1].node = finger[i].node
      FingerTable[i + 1].successor = FingerTable[i].successor;
    } else {
      // finger[i+1].node = n'.find_successor(finger[i+1].start)
      try {
        //if temp is 
        temp = await successorClient.findSuccessor(
          FingerTable[i + 1].start
        );
        // if _self is the better successor
        if ( _self.id < temp.id || temp.id < FingerTable[i+1].start)
        {
          temp = _self;
        }
        FingerTable[i + 1].successor = temp;
      } catch (err) {
        console.error("initFingerTable: successorClient.findSuccessor error ", err);
      }
    }
  }
}

async function update_others() {

  //updating predecessors and successors
  for (let i = 0; i < HASH_BIT_LENGTH; i++) {
    // p = find_predecessors(n - 2^(i - 1))
    //console.log("check the math ", (_self.id - 2**i + 2**HASH_BIT_LENGTH) % 2 ** HASH_BIT_LENGTH);
    const known_node = await findPredecessor(
      (_self.id - 2**i + 2**HASH_BIT_LENGTH) % 2 ** HASH_BIT_LENGTH
    );
    console.log("in update others, should be node0: ")
    console.log(known_node);

    if(_self.id !== known_node.id){
      const knownClient = caller(`localhost:${known_node.port}`, PROTO_PATH, "Node");
      // p.update_finger_table(n, i)
      
      try {
        await knownClient.update_finger_table({node: _self, index: i});
      } catch (err) {
        console.log(`localhost:${known_node.port}`);
        console.error("update_others: client.update_finger_table error ", err);
      }
    }
  }
  //Forcefully update predecessor
  // const client = caller(`localhost:${predecessor.port}`, PROTO_PATH, "Node");
  // try {
  //   await client.update_finger_table({node : _self, index : 0});
  //   await client.update_finger_table({node : _self, index : 1});
  //   await client.update_finger_table({node : _self, index : 2});
  // } catch (err) {
  //   console.log(`localhost:${known_node.port}`);
  //   console.error("update_others: client.update_finger_table error ", err);
  // }
}

// end is exclusive, start is inclusive
function rangeContainsKey(key, start, end)
{ 
  // start = 7, key = 0, end = 1 ...true 
  // 6 7 1... true
  // 6 0 1... true
  if (start > end) { //looping through 0
    return (key >= start || key < end);
  }
  else {  // start < end
    return (key >= start && key < end);
  }
}


async function stabilize() {
  const client = caller(
    `localhost:${FingerTable[0].successor.port}`,
    PROTO_PATH,
    "Node"
  );
  // x = successor.predecessor
  let known_node;
  try {
    known_node = await client.getPredecessor(_self);
  } catch (err) {
    console.error("getPredecessor error: ", err);
  }
  if (
    known_node &&
    known_node.id > _self.id &&
    known_node.id < FingerTable[0].successor.id
  ) {
    FingerTable[0].successor = known_node;
  }
  // successor.notify(n)
  try {
    await client.notify(_self);
  } catch (err) {
    console.error("stabilize failed to call notify ", err);
  }
}

async function fix_fingers() {
  // random integer within the range [0, m)
  const i = Math.floor(Math.random() * HASH_BIT_LENGTH);
  // finger[i].node = find_successor(finger[i].start)

  // TODO: Figure out what client this should be...
  try {
    FingerTable[i].successor = await client.findSuccessor(FingerTable[i].start);
  } catch (err) {
    console.error("fix_fingers failed to call findSuccessor ", err);
  }
}

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
  // setInterval(()=>{
  //   stabilize();
  //   fix_fingers();
  // }, 3000);

  const server = new grpc.Server();
  server.addService(chord.Node.service, {
    fetch,
    insert,
    findSuccessor,
    getSuccessor,
    getPredecessor,
    setPredecessor,
    setSuccessor,
    closestPrecedingFinger,
    notify,
    update_finger_table,
    summary
  });
  server.bind(
    `${_self.ip}:${_self.port}`,
    grpc.ServerCredentials.createInsecure()
  );
  console.log(`Serving on ${_self.ip}:${_self.port}`);
  server.start();
}

main();
