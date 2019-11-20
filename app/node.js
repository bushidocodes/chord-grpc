const process = require("process");
const path = require("path");
const grpc = require("grpc");
const caller = require("grpc-caller");
const protoLoader = require("@grpc/proto-loader");
const minimist = require("minimist");
const { UserService } = require("./UserService");
const {
  computeIntegerHash,
  handleGRPCErrors,
  HASH_BIT_LENGTH
} = require("./utils.js");
const PROTO_PATH = path.resolve(__dirname, "../protos/chord.proto");
const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true
});
const chord = grpc.loadPackageDefinition(packageDefinition).chord;

async function endpointIsResponsive(host, port) {
  const client = caller(`${host}:${port}`, PROTO_PATH, "Node");
  try {
    const _ = await client.summary(this.id);
    return true;
  } catch (err) {
    handleGRPCErrors("endpointIsResponsive", "summary", host, port, err);
    return false;
  }
}

async function hashDryRun(sourceValue) {
  try {
    const integerHash = await computeIntegerHash(sourceValue, HASH_BIT_LENGTH);
    console.log(`ID {${integerHash}} computed from hash of {${sourceValue}}`);
  } catch (err) {
    console.error(
      `Error computing hash of ${sourceValue}. Thus, terminating...\n`,
      err
    );
    return -13;
  }
  return 0;
}

let node;

const nodesAreNotIdentical = ({ host, port, knownHost, knownPort }) =>
  !(host == knownHost && port == knownPort);

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

  if (args.hashOnly) {
    const rc = await hashDryRun(args.hashOnly);
    process.exit(rc);
  }

  // bail immediately if knownHost can't be reached
  if (
    nodesAreNotIdentical(args.host, args.port, args.knownHost, args.knownPort)
  ) {
    if (!(await endpointIsResponsive(args.knownHost, args.knownPort))) {
      console.error(
        `${args.knownHost}:${args.knownPort} is not responsive. Exiting`
      );
      process.exit();
    } else {
      console.log(`${args.knownHost}:${args.knownPort} responded`);
    }
  }

  // protect against bad ID inputs
  if (args.id && args.id > 2 ** HASH_BIT_LENGTH - 1) {
    console.error(
      `Error. Bad ID {${args.id}} > 2^m-1 {${2 ** HASH_BIT_LENGTH -
        1}}. Terminating...\n`
    );
    return -13;
  }

  // protect against bad Known ID inputs
  if (args.knownId && args.knownId > 2 ** HASH_BIT_LENGTH - 1) {
    console.error(
      `Error. Bad known ID {${args.knownId}} > 2^m-1 {${2 ** HASH_BIT_LENGTH -
        1}}. Thus, terminating...\n`
    );
    return -13;
  }

  try {
    node = new UserService(args);
    const server = new grpc.Server();
    server.addService(chord.Node.service, {
      summary: node.summary.bind(node),
      fetch: node.fetch.bind(node),
      remove: node.remove.bind(node),
      removeUserRemoteHelper: node.removeUserRemoteHelper.bind(node),
      insert: node.insert.bind(node),
      insertUserRemoteHelper: node.insertUserRemoteHelper.bind(node),
      lookup: node.lookup.bind(node),
      lookupUserRemoteHelper: node.lookupUserRemoteHelper.bind(node),
      findSuccessorRemoteHelper: node.findSuccessorRemoteHelper.bind(node),
      getSuccessorRemoteHelper: node.getSuccessorRemoteHelper.bind(node),
      getPredecessor: node.getPredecessor.bind(node),
      setPredecessor: node.setPredecessor.bind(node),
      closestPrecedingFingerRemoteHelper: node.closestPrecedingFingerRemoteHelper.bind(
        node
      ),
      updateFingerTable: node.updateFingerTable.bind(node),
      notify: node.notify.bind(node)
    });
    console.log(`Serving on ${args.host}:${args.port}`);
    server.bind(
      `0.0.0.0:${args.port}`,
      grpc.ServerCredentials.createInsecure()
    );
    server.start();
  } catch (err) {
    console.error(err);
    process.exit();
  }
}

main();
