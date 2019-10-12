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

/**
 * Starts an RPC server that receives requests for the Greeter service at the
 * sample server port
 */
function main() {
  const server = new grpc.Server();
  server.addService(chord.Node.service, {fetch, insert});
  server.bind('0.0.0.0:50053', grpc.ServerCredentials.createInsecure());
  server.start();
}

main();


// As I understand it, we identify users by ID and we identify nodes by IPv4+Port combinations
// We should encode our IP as follows IPADDR:PORT, "127.0.0.1:80"

// I'm unclear about how this might work for NAT traversal


// All Implementations use the SHA-1 algorithm to generate hashes from keys.
// Chords have a finger table
