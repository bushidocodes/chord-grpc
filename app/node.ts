import process from "process";
import minimist from "minimist";
import { UserService } from "./UserService.ts";
import readline from "readline";

import { computeIntegerHash, HASH_BIT_LENGTH, withTimeout } from "./utils.ts";

async function hashDryRun(sourceValue: string) {
  try {
    const integerHash = computeIntegerHash(sourceValue);
    console.log(`ID {${integerHash}} computed from hash of {${sourceValue}}`);
  } catch (err) {
    console.error(
      `Error computing hash of ${sourceValue}. Thus, terminating...\n`,
      err,
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
    number: ["port", "knownPort", "id"],
  } as minimist.Opts);

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
      `Error. Bad ID {${args.id}} > 2^m-1 {${
        2 ** HASH_BIT_LENGTH - 1
      }}. Terminating...\n`,
    );
    return -13;
  }

  let userServiceNode = new UserService({
    id: args.id,
    host: args.host,
    port: args.port,
  });
  try {
    userServiceNode.serve();
    let knownNode = {
      id: null,
      host: knownNodeHost,
      port: knownNodePort,
    };
    await userServiceNode.joinCluster(knownNode);
  } catch (err) {
    console.error(err);
    process.exit(1);
  }

  if (process.platform === "win32") {
    const rl = readline.createInterface({
      input: process.stdin,
      output: process.stdout,
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

  // Migrate keys on shutdown, but never hang forever: destructor() makes
  // deadline-less gRPC calls to peers, so if a successor/predecessor is
  // unreachable it would block past docker's 10s SIGKILL window. Bound it with
  // a timeout and force-exit regardless of outcome (issue #176).
  const SHUTDOWN_TIMEOUT_MS = 5000;
  const gracefulShutdown = async (signal: string) => {
    console.log(`\n\n${signal} caught`);
    try {
      await withTimeout(
        userServiceNode.destructor(),
        SHUTDOWN_TIMEOUT_MS,
        "Graceful shutdown timed out",
      );
    } catch (err) {
      console.error("Shutdown error:", err);
    }
    console.log(`Exiting process ${process.pid}`);
    process.exit(0);
  };

  // handle "ctrl + c" as a graceful exit
  process.on("SIGINT", () => gracefulShutdown("User issued ctrl+c (SIGINT)"));
  process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
}

main();
