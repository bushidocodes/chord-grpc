const express = require("express");
const minimist = require("minimist");
const os = require("os");
const path = require("path");
const caller = require("grpc-caller");
const PROTO_PATH = path.resolve(__dirname, "../protos/chord.proto");
const PUBLIC_PATH = path.resolve(__dirname, "./public");

const DEFAULT_HOST_NAME = os.hostname();
const CRAWLER_INTERVAL_MS = 3000;
const DUMMY_REQUEST_OBJECT = { id: 99 };

class ChordCrawler {
  constructor(host, port, stepInMS) {
    this.host = host;
    this.port = port;
    this.client = caller(`${this.host}:${this.port}`, PROTO_PATH, "Node");
    this.state = {};
    this.walk = new Set([]);
    this.canAdvance = true; // Gate in case crawl execution is slower than quantum
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
    const connectionString = `${node.host}:${node.port}`;
    this.state[connectionString] = {
      ...this.state[connectionString],
      ...node
    };
  }
  shuffleCurrentNode() {
    // If we have trouble reaching a node, just shuffle to any other node and walk from there
    const otherNodes = Object.values(this.state).filter(
      node =>
        (node.host !== this.host || node.port !== this.port) &&
        this.host &&
        this.port
    );

    // Just return if we don't have any possible alternatives
    if (otherNodes.length == 0) {
      return;
    }

    const randomNode =
      otherNodes[Math.floor(Math.random() * otherNodes.length)];

    this.host = randomNode.host;
    this.port = randomNode.port;

    // And we have to invalidate the current walk to avoid accidental pruning
    this.walk.clear();
  }
  async crawl() {
    if (this.canAdvance) {
      this.canAdvance = false;
      const connectionString = `${this.host}:${this.port}`;
      console.log(`Connecting to ${connectionString}`);
      const client = caller(connectionString, PROTO_PATH, "Node");

      try {
        const successorNode = await client.getSuccessorRemoteHelper(
          DUMMY_REQUEST_OBJECT
        );
        this.pruneUponCycle(connectionString);
        this.updateSuccessor(connectionString, successorNode);
        this.updateNode(successorNode);
        this.host = successorNode.host;
        this.port = successorNode.port;
      } catch (err) {
        console.error("Error is : ", err);
        this.shuffleCurrentNode();
      } finally {
        this.canAdvance = true;
      }
    }
  }
}

function main() {
  if (process.argv.length >= 2) {
    const args = minimist(process.argv.slice(2));
    let crawler = new ChordCrawler(
      args.host || DEFAULT_HOST_NAME,
      args.port,
      args.interval || CRAWLER_INTERVAL_MS
    );

    const app = express();
    const port = args.webPort || DEFAULT_HOST_PORT;
    app.use(express.static(PUBLIC_PATH));
    app.get("/data", (req, res) => res.json(crawler.state));
    app.listen(port, () =>
      console.log(`Example app listening on port ${port}!`)
    );
  }
}

main();
