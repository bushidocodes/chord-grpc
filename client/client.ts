import minimist from "minimist";
import { Client, type InsertArgs, type EditArgs } from "./common.ts";

const VALID_COMMANDS =
  "lookup, insert, edit, remove, bulkInsert, summary, health, fingerTable, predecessor, successor";

function main() {
  const args = minimist(process.argv.slice(2));
  const command = args._[0];

  if (!command) {
    console.error("Usage: node client.ts <command> [options]");
    console.error(`Valid commands: ${VALID_COMMANDS}`);
    process.exit(1);
  }

  const host: string = args.host || "localhost";
  const port: number = args.port || 8440;
  const client = new Client(host, port);

  switch (command) {
    case "lookup":
      client.lookup({ id: args.id });
      break;
    case "remove":
      client.remove({ id: args.id });
      break;
    case "bulkInsert":
      client.bulkInsert({ path: args.path });
      break;
    // minimist yields untyped args, so cast at this boundary; insert()/edit()
    // validate id and (for edit) the at-least-one-field rule at runtime.
    case "insert":
      client.insert(args as unknown as InsertArgs);
      break;
    case "edit":
      client.edit(args as unknown as EditArgs);
      break;
    case "summary":
      client.summary();
      break;
    case "health":
      client.health();
      break;
    case "fingerTable":
      client.fingerTable();
      break;
    case "predecessor":
      client.predecessor();
      break;
    case "successor":
      client.successor();
      break;
    default:
      console.error(`Unknown command: "${command}"`);
      console.error(`Valid commands: ${VALID_COMMANDS}`);
      process.exit(1);
  }
}

main();
