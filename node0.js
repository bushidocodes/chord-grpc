const grpc = require('grpc');
const users = require('./data/tinyUsers.json')
const protoLoader = require('@grpc/proto-loader');
const minimist = require('minimist')
const packageDefinition = protoLoader.loadSync(
    `${__dirname}/protos/chord.proto`,
    {keepCase: true,
     longs: String,
     enums: String, 
     defaults: true,
     oneofs: true
    });
const chord = grpc.loadPackageDefinition(packageDefinition).chord;

const HASH_BIT_LENGTH = 3

const FingerTable = [
  {
    start: 0, 
    stop: 0, 
    successor: {id: 0, ip: null, port: null}
  }
]

let predecessor = {id: 0, ip: null, port: null};

/* 
  Careful with standalone 'successor'
  It needs to always point to FingerTable[0].successor
  //const successor = FingerTable[0].successor;*/
const _self = {id: 0, ip: null, port: null};


function fetch({request: {id}}, callback) {
  console.log(`Requested User ${id}`);
  if (!users[id]){
    callback({code: 5}, null); // NOT_FOUND error
  } else {
    callback(null, users[id]);
  }
}

function insert({request: user}, callback) {
  if (users[user.id]){
    const message = `Err: ${user.id} already exits`;
    console.log(message);
    callback({code: 6, message}, null); // ALREADY_EXISTS error
  } else {
    users[user.id] = user;
    const message = `Inserted User ${user.id}:`;
    console.log(message);
    callback({status: 0, message}, null);
  }
}

function findSuccessor({id}, callback){
  // n' = find_predecessor(id)
  const predecessorNode = findPredecessor(id);
  const client = new chord.Node(`localhost:${predecessorNode.port}`, grpc.credentials.createInsecure());
  // return n'.successor
  callback(null, client.getSuccessor({}));
}

function closestPrecedingFinger({id}){
  // step through finger table in reverse
  for (let i = HASH_BIT_LENGTH - 1; i >= 0; i--) {
    if ((FingerTable[i].successor.id > _self.id) && (FingerTable[i].successor.id < id)) {
      // return finger[i].node
      callback(null, FingerTable[i].successor);
      return;
    }
  }
  // return n;
  callback(null, _self);;
}

function getSuccessor(_, callback){
  callback(null, FingerTable[0].successor);
}

function getPredecessor(_, callback){ 
  callback(null, predecessor);
}

function setPredecessor(node, callback){
  predecessor = node; 
  callback({status: 0, message: "OK"}, {});
}

function update_finger_table ({NodeAddress: node, idx}, callback) {
  if ((node.id >= _self.id) && (node.id < FingerTable[idx].successor.id)) {
    // finger[i].node = s
    FingerTable[idx].successor = node;
    // p = predecessor
    const client = new chord.Node(`localhost:${predecessor.port}`, grpc.credentials.createInsecure());
    // p.update_finger_table(s, i)
    client.update_finger_table(node, idx);
    // TODO: Figure out how to determine if the above had an RC of 0
    // If so call callback({status: 0, message: "OK"}, {});

  } else {
    callback({status: 0, message: "OK"}, {});
  }
}

// This is used by other node's to update our predecessor
// Think... setPredecessor... with a check
function notify(node, callback) {
  if ((node.id > predecessor.id) && (node.id < _self.id)) {
    // predecessor = n';
    predecessor = node;
    callback({status: 0, message: "OK"}, {});
  }
}

// TODO: Does this need to be a gRPC call or just a local function?
// Understand the implementation and decide if there is anything is the 
// commented out code that we've missed (or can we delete it)

function findPredecessor(node, callback){
  // n' = n
  const n_prime = _self;
  // while(); a.k.a., step through the finger table
  for (let i = 0; i < HASH_BIT_LENGTH; i++) {
    const client = new chord.Node(`localhost:${n_prime.port}`, grpc.credentials.createInsecure());
    const n_prime_successor = client.getSuccessor({});
    // find first range that doesn't contain the key
    if (!((node.id > n_prime.id) && (node.id < n_prime_successor.id))) {
      // n' = n'.closest_preceding_finger(id)
      n_prime = client.closest_preceding_finger(node.id);
    }
  }
  callback(null, {n_prime});

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

// Pass a null as known_node to force the node to be the first in the cluster
function join(known_node) {
  // if (n')
  if (known_node && confirm_exist(known_node)){
    // init_finger_table(n')
    init_finger_table(known_node);
    // update_others
    update_others();
  } else {
    // known_node wasn't really there
    for (let i = 0; i < HASH_BIT_LENGTH; i++) {
      // finger[i].node = n
      FingerTable.push(_self);
    }
    // predecessor = n
    predecessor = _self;
  }
}

function confirm_exist(known_node) {
  // TODO: confirm_exist actually needs to ping the endpoint to ensure it's real
  return true;
}

function init_finger_table(known_node) {
  // client for possible known node
  const client = new chord.Node(`localhost:${known_node.port}`, grpc.credentials.createInsecure());
  // finger[1].node = n'.find_successor(finger[1].start)
  FingerTable[0].successor = client.findSuccessor(FingerTable[0].start);
  // client for newly-determined successor
  const client = new chord.Node(`localhost:${FingerTable[0].successor.port}`, grpc.credentials.createInsecure());
  // predecessor = successor.predecessor
  predecessor = client.getPredecessor({});
  // successor.predecessor = n
  client.setPredecessor(_self);
  // for (i=1 to m-1){}, where 1 is really 0, and skip last element
  for (let i = 0; i < (HASH_BIT_LENGTH - 1); i++) {
    if ((FingerTable[i + 1].start >= _self.id) && (FingerTable[i + 1].start < FingerTable[i].successor.id)) {
      // finger[i+1].node = finger[i].node
      FingerTable[i + 1].successor = FingerTable[i].successor;
    } else {
      // finger[i+1].node = n'.find_successor(finger[i+1].start)
      FingerTable[i + 1].successor = client.findSuccessor(FingerTable[i + 1].start);
    }
  }
}

function update_others() {
  for (let i = 0; i < HASH_BIT_LENGTH; i++) {
    // p = find_predecessors(n - 2^(i - 1))
    known_node = findPredecessor((_self.id - 2**(i - 1)) % (2**HASH_BIT_LENGTH));
    const client = new chord.Node(`localhost:${known_node.port}`, grpc.credentials.createInsecure());
    // p.update_finger_table(n, i)
    client.update_finger_table(_self, i)
  }
}


function stabilize() {
  const client = new chord.Node(`localhost:${FingerTable[0].successor.port}`, grpc.credentials.createInsecure());
  // x = successor.predecessor
  known_node = client.getPredecessor({});
  if ((known_node.id > _self.id) && (known_node.id < FingerTable[0].successor.id)) {
    FingerTable[0].successor = known_node;
  }
  // successor.notify(n)
  client.notify(_self);
}


function fix_fingers() {
  // random integer within the range [0, m)
  const i = Math.floor(Math.random() * HASH_BIT_LENGTH);
  // finger[i].node = find_successor(finger[i].start)
  FingerTable[i].successor = findSuccessor(FingerTable[i].start);
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
function main() {
  const args = minimist(process.argv.slice(2));
  _self.id = args.id ? args.id : 0;
  _self.ip = args.ip ? args.ip : `0.0.0.0`;
  _self.port = args.port ? args.port : 1337;

  if (args.targetIp && args.targetPort && args.targetId){
    join({id: args.targetId, ip: args.targetIp, port: targetPort});
  } else {
    join(null);
  }
  // TODO: Periodically run stabilize and fix_fingers
  stabilize();
  fix_fingers();
  
  const server = new grpc.Server();
  server.addService(chord.Node.service, {fetch, insert, findSuccessor, getSuccessor, getPredecessor, setPredecessor, closestPrecedingFinger, notify, update_finger_table});
  server.bind(`${_self.ip}:${_self.port}`, grpc.ServerCredentials.createInsecure());
  console.log(`Serving on ${_self.ip}:${_self.port}`);
  server.start();
}

main();
