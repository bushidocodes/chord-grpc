import express from "express";
import minimist from "minimist";
import os from "os";
import path from "path";
import caller from "grpc-caller";

const PROTO_PATH = path.resolve(__dirname, "../protos/chord.proto");
const PUBLIC_PATH = path.resolve(__dirname, "./public");

const DEFAULT_HOST_NAME = os.hostname();
const CRAWLER_INTERVAL_MS = 3000;
const DUMMY_REQUEST_OBJECT = { id: 99 };

class ChordCrawler {
  host: string;
  port: number;
  client: any;
  state: object;
  walk: Set<any>;
  canAdvance: boolean;

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

  updateFingerTable(connectionStringOfSourceNode, fingerTable) {
    if (this.state[connectionStringOfSourceNode]) {
      this.state[connectionStringOfSourceNode].fingerTable = fingerTable;
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
        // Update Fingers
        let fingerTable = {};
        const thing = await client.getFingerTableEntries(DUMMY_REQUEST_OBJECT);
        thing.on("data", function({ index, node }) {
          fingerTable[index] = node;
        });
        thing.on("end", () =>
          this.updateFingerTable(connectionString, fingerTable)
        );

        // Update Successor
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
    const port = args.webPort || 1337;
    app.use(express.static(PUBLIC_PATH));
    app.get("/data", (req, res) => res.json(crawler.state));
    app.listen(port, () =>
      console.log(`Example app listening on port ${port}!`)
    );
  }
}

main();
