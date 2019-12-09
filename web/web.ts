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
  async advance() {
    const connectionString = `${this.host}:${this.port}`;
    this.walk.add(connectionString);
    this.state[connectionString].successor.host;
    // If the current node has a successor that we haven't yet crawled, advance to it
    if (
      this.state[connectionString] &&
      this.state[connectionString].successor &&
      this.state[connectionString].successor.host &&
      this.state[connectionString].successor.port &&
      !this.walk.has(
        `${this.state[connectionString].successor.host}:${this.state[connectionString].successor.port}`
      )
    ) {
      this.host = this.state[connectionString].successor.host;
      this.port = this.state[connectionString].successor.port;
    } else {
      let foundDangling = false;
      for (let storedConnectionString of Object.keys(this.state)) {
        if (!this.walk.has(storedConnectionString)) {
          this.host = this.state[storedConnectionString].host;
          this.port = this.state[storedConnectionString].port;
          foundDangling = true;
          break;
        }
      }
      if (!foundDangling) {
        // If we've visited all known nodes, clear the walk and try again
        this.walk.clear();
        this.shuffleCurrentNode();
      }
    }
  }

  updateSuccessor(connectionStringOfSourceNode, successorNode) {
    if (this.state[connectionStringOfSourceNode]) {
      this.state[connectionStringOfSourceNode].successor = successorNode;
    }
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
      this.client = caller(connectionString, PROTO_PATH, "Node");

      try {
        // Request ID to see if the node is even responsive
        const { id } = await this.client.getNodeIdRemoteHelper();

        // If it is, plumb out the object in state if anything is missing
        if (!this.state[connectionString]) {
          this.state[connectionString] = {};
          this.state[connectionString].host = this.host;
          this.state[connectionString].port = this.port;
        }
        if (!this.state[connectionString].fingerTable) {
          this.state[connectionString].fingerTable = {};
        }
        if (!this.state[connectionString].userIds) {
          this.state[connectionString].userIds = [];
        }
        if (!this.state[connectionString].id) {
          this.state[connectionString].id = id;
        }

        // Update Fingers
        const fingerTableStream = await this.client.getFingerTableEntries();
        this.state[connectionString].fingerTable = {};
        fingerTableStream.on("data", ({ index, node }) => {
          this.state[connectionString].fingerTable[index] = node;
        });

        // Update UserIds
        const userIdStream = await this.client.getUserIds();
        this.state[connectionString].userIds = [];
        userIdStream.on("data", idWithMetadata => {
          this.state[connectionString].userIds.push(idWithMetadata);
        });

        // Update Predecessor
        const predecessorNode = await this.client.getPredecessor();
        this.state[connectionString].predecessor = predecessorNode;

        // Update Successor
        const successorNode = await this.client.getSuccessorRemoteHelper();
        this.state[connectionString].successor = successorNode;

        // Advance to the successor or a known node in a partition
        this.advance();
      } catch (err) {
        if (err.code == 14) {
          // If you can't reach a node, delete it and select a random node to continue walk
          delete this.state[connectionString];
          this.shuffleCurrentNode();
        } else {
          console.log(err);
        }
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
