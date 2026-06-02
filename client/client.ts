import minimist from "minimist";
import os from "os";
import { Client } from "./common.ts";

function main() {
  if (process.argv.length >= 3) {
    const args = minimist(process.argv.slice(3));
    const host = args.host || os.hostname();
    const port = args.port || 8440;
    let client = new Client(host, port);

    const command = process.argv[2];

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
      case "insert":
        client.insert({ ...args, edit: false });
        break;
      case "edit":
        client.insert({ ...args, edit: true });
        break;
      case "summary":
        client.summary();
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
    }
  }
}

main();
