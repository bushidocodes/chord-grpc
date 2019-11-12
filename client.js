const minimist = require("minimist");
const path = require("path");
const caller = require("grpc-caller");
const PROTO_PATH = path.resolve(__dirname, "./protos/chord.proto");

const HOST = "127.0.0.1";
const DUMMY_REQUEST_OBJECT = { id: 99 };

const target = {
  ip: HOST,
  port: 1337
};

let client;

async function lookup({ _, ...rest }) {
  if (!rest.id) {
    console.log("lookup requires an ID");
    process.exit();
  }

  await client.lookup({ id: rest.id }, (err, user) => {
    if (err) {
      console.log("User not found");
      console.log(err);
    } else {
      console.log("User found: ", user);
    }
  });
}

async function remove({ _, ...rest }) {
  if (!rest.id) {
    console.log("remove requires an ID");
    process.exit();
  }
  console.log("Beginning client-side remove: ", rest.id);

  await client.remove({ id: rest.id }, (err, _) => {
    if (err) {
      console.log("User not deleted");
      console.log(err);
    } else {
      console.log("User deleted");
    }
  });
}

// I was originally thinking that the user service would assign the id, but this doesn't really seem possible...

async function insert({ _, ...rest }) {
  console.log(rest);
  if (!rest.id) {
    console.log("id is a mandatory field!");
    console.log("node client insert --id=42424242");

    console.log(
      "optional fields include reputation, creationDate, displayName, lastAccessDate, websiteUrl, location, aboutMe, views, upVotes, downVotes, profileImageUrl, accountId"
    );
    console.log(
      'node client insert --id=42424242 --displayName="Sean McBride" --reputation=3 --website="https://www.bushido.codes"'
    );
    process.exit();
  }
  console.log(rest);
  const user = {
    id: rest.id,
    reputation: rest.reputation || 0,
    creationDate: rest.creationDate || Date.now().toString(), // Not the right format, but whatever...
    displayName: rest.displayName || "",
    lastAccessDate: rest.lastAccessDate || "",
    websiteUrl: rest.websiteUrl || "",
    location: rest.location || "",
    aboutMe: rest.aboutMe || "",
    views: rest.views || 0,
    upVotes: rest.upVotes || 0,
    downVotes: rest.downVotes || 0,
    profileImageUrl: rest.profileImageUrl || 0,
    accountId: rest.accoutId || 0
  };
  console.log(user);
  await client.insert({ user, edit: rest.edit });
  if (rest.edit) {
    console.log("User editted successfully.");
  } else {
    console.log("User inserted successfully.");
  }
}

// Requests basic information about the target node
async function summary() {
  console.log("Client requesting summary:");
  try {
    const node = await client.summary({ id: 1 });
    console.log(
      `The node returned id: ${node.id}, ip: ${node.ip}, port: ${node.port}`
    );
  } catch (err) {
    console.error(err);
  }
}

class ChordCrawler {
  constructor(ip, port, stepInMS) {
    this.ip = ip;
    this.port = port;
    this.state = {};
    this.walk = new Set([]);
    setInterval(async () => {
      await this.crawl();
    }, stepInMS);
  }
  pruneUponCycle(connectionString) {
    // If we touch a node we've already touched, we've walked a complete cycle
    if (this.walk.has(connectionString)) {
      // We can now flush dangling nodes not encountered during the walk
      for (let storedConnectionString of Object.keys(this.state)) {
        if (!this.walk.has(storedConnectionString)) {
          delete this.state[storedConnectionString];
        }
      }
      // Clear to start a new walk
      this.walk.clear();
    } else {
      // Not yet a cycle, so add node to the walk
      this.walk.add(connectionString);
    }
  }
  updateSuccessor(connectionStringOfSourceNode, successorNode) {
    if (this.state[connectionStringOfSourceNode]) {
      this.state[connectionStringOfSourceNode].successor = successorNode;
    }
  }
  updateNode(node) {
    const connectionString = `${node.ip}:${node.port}`;
    this.state[connectionString] = {
      ...this.state[connectionString],
      ...node
    };
  }
  shuffleCurrentNode() {
    // If we have trouble reaching a node, just shuffle to any other node and walk from there
    const otherNodes = Object.values(this.state).filter(
      node =>
        (node.ip !== this.ip || node.port !== this.port) && this.ip && this.port
    );

    // Just return if we don't have any possible alternatives
    if (otherNodes.length == 0) {
      return;
    }

    const randomNode =
      otherNodes[Math.floor(Math.random() * otherNodes.length)];

    this.ip = randomNode.ip;
    this.port = randomNode.port;

    // And we have to invalidate the current walk to avoid accidental pruning
    this.walk.clear();
  }
  async crawl() {
    const connectionString = `${this.ip}:${this.port}`;
    console.log(`Connecting to ${connectionString}`);
    const client = caller(connectionString, PROTO_PATH, "Node");

    try {
      const successorNode = await client.getSuccessorRemoteHelper(
        DUMMY_REQUEST_OBJECT
      );
      this.pruneUponCycle(connectionString);
      this.updateSuccessor(connectionString, successorNode);
      this.updateNode(successorNode);
      this.ip = successorNode.ip;
      this.port = successorNode.port;
    } catch (err) {
      console.log("Error is : ", err);
      this.shuffleCurrentNode();
    }
  }
}

function main() {
  if (process.argv.length >= 3) {
    const args = minimist(process.argv.slice(3));

    console.log(args);

    if (args.ip) {
      target.ip = args.ip;
    }
    if (args.port) {
      target.port = args.port;
    }

    console.log(`Connecting to ${target.ip}:${target.port}`);

    client = caller(`localhost:${target.port}`, PROTO_PATH, "Node");

    const command = process.argv[2];

    switch (command) {
      case "lookup":
        lookup(args);
        break;
      case "remove":
        remove(args);
        break;
      case "insert":
        args["edit"] = false;
        insert(args);
        break;
      case "edit":
        args["edit"] = true;
        insert(args);
        break;
      case "summary":
        summary();
        break;
      case "crawl":
        let crawler = new ChordCrawler(target.ip, target.port, 3000);
        const express = require("express");
        const app = express();
        const port = args.webPort || 3000;
        app.use(express.static("public"));
        app.get("/data", (req, res) => res.json(crawler.state));
        app.listen(port, () =>
          console.log(`Example app listening on port ${port}!`)
        );
        break;
    }
  }
}

main();
