const path = require('path');
const grpc = require('grpc');
const users = require('./data/tinyUsers.json');
const protoLoader = require('@grpc/proto-loader');
const minimist = require('minimist');
const PROTO_PATH = path.resolve(__dirname, './protos/chord.proto');
const packageDefinition = protoLoader.loadSync(
    PROTO_PATH,
    {keepCase: true,
     longs: String,
     enums: String, 
     defaults: true,
     oneofs: true
    });
const chord = grpc.loadPackageDefinition(packageDefinition).chord;


const caller = require('grpc-caller');


const HASH_BIT_LENGTH = 3;

const FingerTable = [
  {
    start: 0, 
    //stop: 0, 
    successor: {id: 0, ip: null, port: null}
  }
]

let predecessor = {id: 0, ip: null, port: null};

/* 
  Careful with standalone 'successor'
  It needs to always point to FingerTable[0].successor
  //const successor = FingerTable[0].successor;*/
const _self = {id: 0, ip: null, port: null};

function summary(_, callback){
  console.log("Summary request received");
  callback(null, _self);
}



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

function findSuccessor(id, callback){
  console.log("beginning findSuccessor");
  // n' = find_predecessor(id)
  const predecessorNode = findPredecessor(id);
  console.log(`findSuccessor predecessorNode ${predecessorNode}`);
  const client = new chord.Node(`localhost:${predecessorNode.port}`, grpc.credentials.createInsecure());
  // return n'.successor
  const temp = getClientSuccessor(client);
  console.log(`findSuccessor temp: ${temp}`);
  callback(null, temp);
}

function closestPrecedingFinger(id){
  // step through finger table in reverse
  for (let i = HASH_BIT_LENGTH - 1; i >= 0; i--) {
    if ((FingerTable[i].successor.id > _self.id) && (FingerTable[i].successor.id < id)) {
      // return finger[i].node
      callback(null, FingerTable[i].successor);
      return;
    }
  }
  // return n;
  callback(null, _self);
}

function getSuccessor(thing, callback){
  callback(null, FingerTable[0].successor);
}

function getPredecessor(thing, callback){
  console.log(thing); 
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

function findPredecessor(id){
  // n' = n
  const n_prime = _self;
  // while(); a.k.a., step through the finger table
  for (let i = 0; i < HASH_BIT_LENGTH; i++) {
    const client = new chord.Node(`localhost:${n_prime.port}`, grpc.credentials.createInsecure());
    const n_prime_successor = getClientSuccessor(client);
    // find first range that doesn't contain the key
    if (!((id > n_prime.id) && (id < n_prime_successor.id))) {  // if target not between prime and prime's successor
      // n' = n'.closestPrecedingFinger(id)
      n_prime = client.closestPrecedingFinger(id);
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

// Pass a null as known_node to force the node to be the first in the cluster
async function join(known_node) {
  // if (n')
  console.log(`Known node: ${known_node}`);
  console.log("Just Fishing...");
  const client = caller('localhost:8448', PROTO_PATH, 'Node');

  const thing = await client.summary({ id: 33 });
  
  console.log(thing);


  // client.sayHello({ name: 'Bob' }, (err, res) => {
  //   console.log(res)
  // })


  // let client = new chord.Node(`localhost:8448`, grpc.credentials.createInsecure()); 
  // client.summary({id: 33}, () => console.log("PC LOAD LETTER"));
  /* vvvvv these lines used to be in the else vvvvv */
  // remove dummy template initializer
  // FingerTable.pop();
  // // initialize table with reasonable values
  // for (let i = 0; i < HASH_BIT_LENGTH; i++) {
  //   // finger[i].node = n
  //   FingerTable.push({start: (2**i) % (2**HASH_BIT_LENGTH), successor: _self});
  // }
  // // predecessor = n
  // predecessor = _self;
  // /* ^^^^^ these lines used to be in the else ^^^^^ */
  // if (known_node && confirm_exist(known_node)){
  //   // init_finger_table(n')
  //   init_finger_table(known_node);
  //   // update_others
  //   update_others();
  // } else {
  //   // TODO: maybe we don't need this anymore
  //   // known_node wasn't really there
  //   for (let i = 0; i < HASH_BIT_LENGTH; i++) {
  //     // finger[i].node = n
  //     FingerTable[i].successor = _self;
  //   }
  //   // predecessor = n
  //   predecessor = _self;
  // }
  // console.log("FingerTable: ");
  // for (let i = 0; i < HASH_BIT_LENGTH;  i++) {
  //   console.log(FingerTable[i]);
  // }
}

function confirm_exist(known_node) {
  // TODO: confirm_exist actually needs to ping the endpoint to ensure it's real
  return !_self.id == known_node.id;
}

function init_finger_table(known_node) {
  // client for possible known node
  console.log(`We are connecting from localhost:${_self.port}`);
  console.log(`We are connecting to localhost:${known_node.port}`);

  let client = new chord.Node(`localhost:8448`, grpc.credentials.createInsecure()); 
  console.log("requesting summary");
  client.summary({id: 33}, () => console.log("PC LOAD LETTER"));
  
  console.log("requesting summary 2");
  client.summary({id: 1}, (err, node) => {
    console.log("HELLO WORLD IS THIS HERE");
    if (err) {
      console.log(err);
    } else {
      //console.log(node);
      console.log(`The node returned id: ${node.id}, ip: ${node.ip}, port: ${node.port}`);
    }
  });


  // finger[1].node = n'.find_successor(finger[1].start)
  console.log(`FingerTable[0].start = ${FingerTable[0].start}`)
  let temp = findClientSuccessor(client, FingerTable[0].start);
  console.log(`findClientSuccessor returned ${temp}`);
  FingerTable[0].successor = temp;
  // client for newly-determined successor
  client = new chord.Node(`localhost:${FingerTable[0].successor.port}`, grpc.credentials.createInsecure());
  // predecessor = successor.predecessor
  predecessor = getClientPredecessor(client);
  // successor.predecessor = n
  client.setPredecessor(_self);
  // for (i=1 to m-1){}, where 1 is really 0, and skip last element
  for (let i = 0; i < (HASH_BIT_LENGTH - 1); i++) {
    if ((FingerTable[i + 1].start >= _self.id) && (FingerTable[i + 1].start < FingerTable[i].successor.id)) {
      // finger[i+1].node = finger[i].node
      FingerTable[i + 1].successor = FingerTable[i].successor;
    } else {
      // finger[i+1].node = n'.find_successor(finger[i+1].start)
      FingerTable[i + 1].successor = findClientSuccessor(client, FingerTable[i + 1].start);
    }
  }
}

function getClientPredecessor(client){
  client.getPredecessor(_self, (err, pred) => 
  {
    console.log(`pred is ${pred}`);
    return pred;
  });
}

function getClientSuccessor(client){
  client.getSuccessor(_self, (err, succ) =>
  {
    console.log(`succ is ${succ}`);
    return succ;
  });
}

function findClientSuccessor (client, id){
  console.log(`beginning findClientSuccessor`);
  console.log(client);
  console.log(id);
  client.findSuccessor(id, (err, succ) =>
  {
    console.log("Not executing....");
    if(err){
      console.log(`findClientSuccessor error: ${err}`);
    }
    console.log(`findClientSuccessor succ: ${succ}`);
    return succ;
  });
  console.log("end of findClientSuccessor");
}

function update_others() {
  for (let i = 0; i < HASH_BIT_LENGTH; i++) {
    // p = find_predecessors(n - 2^(i - 1))
    known_node = findPredecessor((_self.id - (2**i)) % (2**HASH_BIT_LENGTH));
    const client = new chord.Node(`localhost:${known_node.port}`, grpc.credentials.createInsecure());
    // p.update_finger_table(n, i)
    client.update_finger_table(_self, i)
  }
}


function stabilize() {
  const client = new chord.Node(`localhost:${FingerTable[0].successor.port}`, grpc.credentials.createInsecure());
  // x = successor.predecessor
  known_node = getClientPredecessor(client);
  if (known_node && (known_node.id > _self.id) && (known_node.id < FingerTable[0].successor.id)) {
    FingerTable[0].successor = known_node;
  }
  // successor.notify(n)
  client.notify(_self);
}


function fix_fingers() {
  // random integer within the range [0, m)
  const i = Math.floor(Math.random() * HASH_BIT_LENGTH);
  // finger[i].node = find_successor(finger[i].start)
  FingerTable[i].successor = findClientSuccessor(client, FingerTable[i].start);
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

  console.log(`targetIp: ${args.targetIp}, targetId: ${args.targetId}, targetPort: ${args.targetPort}`);
  if (args.targetIp !== null && args.targetPort !== null && args.targetId !== null){
    join({id: args.targetId, ip: args.targetIp, port: args.targetPort});
  } else {
    join(null);
  }
  // TODO: Periodically run stabilize and fix_fingers
  // stabilize();
  // fix_fingers();
  
  const server = new grpc.Server();
  server.addService(chord.Node.service, {fetch, insert, findSuccessor, /*findPredecessor,*/ getSuccessor, getPredecessor, setPredecessor, closestPrecedingFinger, notify, update_finger_table, summary});
  server.bind(`${_self.ip}:${_self.port}`, grpc.ServerCredentials.createInsecure());
  console.log(`Serving on ${_self.ip}:${_self.port}`);
  server.start();
}

main();
