import process from "process";
import minimist from "minimist";
import { UserService } from "./UserService";

import {
  connect,
  computeIntegerHash,
  handleGRPCErrors,
  HASH_BIT_LENGTH
} from "./utils";

export async function endpointIsResponsive(host: string, port: number) {
  const client = connect({ host, port });
  try {
    await client.summary(this.id);
    return true;
  } catch (err) {
    handleGRPCErrors("endpointIsResponsive", "summary", host, port, err);
    return false;
  }
}

async function hashDryRun(sourceValue) {
  try {
    const integerHash = await computeIntegerHash(sourceValue);
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

/**
 * Starts an RPC server that receives requests for the Greeter service at the
 * sample server port
 *
 * Takes the following mandatory flags
 * --host       - This node's host name
 * --port       - This node's TCP Port
 *
 * And takes the following optional flags
 * --id         - This node's id
 * --knownId   - The ID of a node in the cluster
 * --knownHost   - The host name of a node in the cluster
 * --knownPort - The TCP Port of a node in the cluster
 */
async function main() {
  const args = minimist(process.argv.slice(2), {
    string: ["host", "knownHost"],
    // @ts-ignore
    number: ["port", "knownPort", "id", "knownId", "knownId"]
  });

  if (args.hashOnly) {
    const rc = await hashDryRun(args.hashOnly);
    process.exit(rc);
  }

  // bail immediately if knownHost can't be reached
  if (
    args.host &&
    args.port &&
    args.knownHost &&
    args.knownPort &&
    !(args.host == args.knownHost && args.port == args.knownPort)
  ) {
    if (!(await endpointIsResponsive(args.knownHost, args.knownPort))) {
      console.error(
        `${args.knownHost}:${args.knownPort} is not responsive. Exiting`
      );
      process.exit(-9);
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
    let userServiceNode = new UserService({ ...args });
    await userServiceNode.serve();
    await userServiceNode.joinCluster();
  } catch (err) {
    console.error(err);
    process.exit();
  }
}

main();
