const minimist = require("minimist");
const os = require("os");
const { Client } = require("./common.js");

function main() {
  if (process.argv.length >= 3) {
    const args = minimist(process.argv.slice(3));
    const host = args.host || os.hostname();
    const port = args.port || 8440;
    client = new Client(host, port);

    const command = process.argv[2];

    switch (command) {
      case "lookup":
        client.lookup(args);
        break;
      case "remove":
        client.remove(args);
        break;
      case "insert":
        args["edit"] = false;
        client.insert(args);
        break;
      case "edit":
        args["edit"] = true;
        client.insert(args);
        break;
      case "summary":
        client.summary();
        break;
    }
  }
}

main();
