import process from "process";
import minimist from "minimist";
import { UserService } from "./UserService";
import readline from "readline";

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

async function hashDryRun(sourceValue: string) {
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
 * --knownHost  - The host name of a node in the cluster
 * --knownPort  - The TCP Port of a node in the cluster
 */
async function main() {
  console.log("This process is your pid " + process.pid);
  const args = minimist(process.argv.slice(2), {
    string: ["host", "knownHost"],
    // @ts-ignore
    number: ["port", "knownPort", "id"]
  });

  if (args.hashOnly) {
    const rc = await hashDryRun(args.hashOnly);
    process.exit(rc);
  }

  // sanitize parameters corresponding to known node
  // + if no known host or port were provided, it is assumed that they are self's
  // + such as when starting a new chord; ie, joining itself
  let knownNodeHost = args.knownHost ? args.knownHost : args.host;
  let knownNodePort = args.knownPort ? args.knownPort : args.port;

  // protect against bad ID inputs
  if (args.id && args.id > 2 ** HASH_BIT_LENGTH - 1) {
    console.error(
      `Error. Bad ID {${args.id}} > 2^m-1 {${2 ** HASH_BIT_LENGTH -
        1}}. Terminating...\n`
    );
    return -13;
  }

  let userServiceNode = new UserService({ ...args });
  try {
    userServiceNode.serve();
    let knownNode = {
      id: null,
      host: knownNodeHost,
      port: knownNodePort
    };
    await userServiceNode.joinCluster(knownNode);
  } catch (err) {
    console.error(err);
    process.exit();
  }

  if (process.platform === "win32") {
    const rl = readline.createInterface({
      input: process.stdin,
      output: process.stdout
    });
    rl.on("SIGINT", () => {
      console.log("INT!");
      process.emit("SIGINT" as any);
    });
    rl.on("SIGTERM", () => {
      console.log("TERM!");
      process.emit("SIGTERM" as any);
    });
    process.on("SIGTERM", () => process.kill(process.pid, "SIGINT"));
  }

  // handle "ctrl + c" as a graceful exit
  process.on("SIGINT", async function() {
    console.log("\n\nUser issued ctrl+c");
    await userServiceNode.destructor();
    console.log(`Exiting process ${process.pid}`);
    process.exit();
  });

  process.on("SIGTERM", async function() {
    console.log("\n\nSIGTERM caught");
    await userServiceNode.destructor();
    console.log(`Exiting process ${process.pid}`);
    process.exit();
  });
}

main();
