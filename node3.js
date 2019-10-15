// Copy of original Server
// I creat it to test calls between servers

const grpc = require('grpc');
const users = require('./data/tinyUsers.json')
const protoLoader = require('@grpc/proto-loader');
const _ = require('lodash');
const packageDefinition = protoLoader.loadSync(
    `${__dirname}/protos/chord.proto`,
    {keepCase: true,
     longs: String,
     enums: String,
     defaults: true,
     oneofs: true
    });
const chord = grpc.loadPackageDefinition(packageDefinition).chord;

var FingerTable = {4:0, 5:0, 7:0};
var predecessor = 1;
var successor = 0;


function fetch({request: {id}}, callback) {
  console.log(`Requested User ${id}`);
  if (!users[id]){
    callback({code: 5}, null);
  } else {
    callback(null, users[id]);
  }
}

function insert({request: user}, callback) {
  console.log(`Inserting User ${user.id}:`);
  console.log(user);
  if (users[user.id]){
    console.log(`Err: ${user.id} already exits`);
    // ID is already taken, so return ALREADY_EXISTS error
    callback({code: 6}, null);
  } else {
    users[user.id] = user;
    callback(null, {id: users[user.id].id});
  }
}

function summary({}, callback){
  summary = {summary: `This is node 50054`};
  console.log(summary.summary); 
  callback(null, summary); 
}

function chordInformation({}, callback){
  const client = new chord.Node('localhost:50054',
  grpc.credentials.createInsecure());
  
  client.summary({}, (err, summary))

  summary.summary = summary.summary + "This is node 50054"

}

function findSuccessor({}, callback){

}


/**
 * Starts an RPC server that receives requests for the Greeter service at the
 * sample server port
 */
function main() {
  const server = new grpc.Server();
  server.addService(chord.Node.service, {fetch, insert, summary, chordInformation});
  server.bind('0.0.0.0:50054', grpc.ServerCredentials.createInsecure());
  server.start();
}

main();


// As I understand it, we identify users by ID and we identify nodes by IPv4+Port combinations
// We should encode our IP as follows IPADDR:PORT, "127.0.0.1:80"

// I'm unclear about how this might work for NAT traversal


// All Implementations use the SHA-1 algorithm to generate hashes from keys.
// Chords have a finger table
