const minimist = require("minimist");
const { Client } = require("./common.js");

const DEFAULT_HOST_NAME = "localhost";
const DEFAULT_HOST_PORT = 1337;

const target = {
  host: DEFAULT_HOST_NAME,
  port: DEFAULT_HOST_PORT
};

function main() {
  if (process.argv.length >= 3) {
    const args = minimist(process.argv.slice(3));
    console.log("The command-line arguments were:\n", args);

    if (args.host) {
      target.host = args.host;
    }
    if (args.port) {
      target.port = args.port;
    }

    console.log(`Connecting to ${target.host}:${target.port}`);
    client = new Client(target.host, target.port);

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
