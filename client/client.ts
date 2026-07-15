import { parseArgs } from "node:util";
import { Client, type EditArgs, type InsertArgs } from "./common.ts";

const VALID_COMMANDS =
  "lookup, insert, edit, remove, bulkInsert, summary, health, fingerTable, predecessor, successor";

function main() {
  const { values, positionals } = parseArgs({
    args: process.argv.slice(2),
    options: {
      host: { type: "string" },
      port: { type: "string" },
      id: { type: "string" },
      path: { type: "string" },
      reputation: { type: "string" },
      creationDate: { type: "string" },
      displayName: { type: "string" },
      lastAccessDate: { type: "string" },
      websiteUrl: { type: "string" },
      location: { type: "string" },
      aboutMe: { type: "string" },
      views: { type: "string" },
      upVotes: { type: "string" },
      downVotes: { type: "string" },
      profileImageUrl: { type: "string" },
      accountId: { type: "string" },
    },
    allowPositionals: true,
  });
  const command = positionals[0];

  if (!command) {
    console.error("Usage: node client.ts <command> [options]");
    console.error(`Valid commands: ${VALID_COMMANDS}`);
    process.exit(1);
  }

  const host: string = values.host || "localhost";
  const port: number = values.port ? Number(values.port) : 8440;
  const client = new Client(host, port);

  // parseArgs yields string values for all options; coerce numerics at this
  // boundary. insert()/edit() validate id and (for edit) the at-least-one-field
  // rule at runtime.
  const num = (v: string | undefined) =>
    v !== undefined ? Number(v) : undefined;

  switch (command) {
    case "lookup":
      client.lookup({ id: num(values.id) });
      break;
    case "remove":
      client.remove({ id: num(values.id) });
      break;
    case "bulkInsert":
      client.bulkInsert({ path: values.path });
      break;
    case "insert":
      client.insert({
        ...values,
        id: Number(values.id),
        reputation: num(values.reputation),
        views: num(values.views),
        upVotes: num(values.upVotes),
        downVotes: num(values.downVotes),
        accountId: num(values.accountId),
      } as unknown as InsertArgs);
      break;
    case "edit":
      client.edit({
        ...values,
        id: Number(values.id),
        reputation: num(values.reputation),
        views: num(values.views),
        upVotes: num(values.upVotes),
        downVotes: num(values.downVotes),
        accountId: num(values.accountId),
      } as unknown as EditArgs);
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
