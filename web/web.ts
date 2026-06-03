import express from "express";
import minimist from "minimist";
import os from "os";
import path from "path";
import { connect } from "../app/utils.ts";
const PUBLIC_PATH = path.resolve(import.meta.dirname, "./public");

interface NodeSnapshot {
  host?: string;
  port?: number;
  id?: number;
  fingerTable?: Record<number, unknown>;
  userIds?: unknown[];
  predecessor?: unknown;
  successor?: { host?: string; port?: number };
}

type NetworkState = Record<string, NodeSnapshot>;

const DEFAULT_HOST_NAME = os.hostname();
const CRAWLER_INTERVAL_MS = 3000;

class ChordCrawler {
  #host: string;
  #port: number;
  #seedHost: string;
  #seedPort: number;
  #client: any;
  #state: NetworkState = {};
  #walk: Set<string> = new Set([]);
  #canAdvance: boolean = true;

  get state() {
    return this.#state;
  }

  constructor(host: string, port: number, stepInMS: number) {
    this.#host = host;
    this.#port = port;
    this.#seedHost = host;
    this.#seedPort = port;
    this.#client = connect({ host: this.#host, port: this.#port });
    setInterval(async () => {
      await this.#crawl();
    }, stepInMS);
  }

  #advance() {
    const connectionString = `${this.#host}:${this.#port}`;
    this.#walk.add(connectionString);
    // If the current node has a successor that we haven't yet crawled, advance to it
    if (
      this.#state[connectionString] &&
      this.#state[connectionString].successor &&
      this.#state[connectionString].successor.host &&
      this.#state[connectionString].successor.port &&
      !this.#walk.has(
        `${this.#state[connectionString].successor.host}:${this.#state[connectionString].successor.port}`,
      )
    ) {
      this.#host = this.#state[connectionString].successor.host;
      this.#port = this.#state[connectionString].successor.port;
    } else {
      let foundDangling = false;
      for (const storedConnectionString of Object.keys(this.#state)) {
        if (!this.#walk.has(storedConnectionString)) {
          this.#host = this.#state[storedConnectionString].host!;
          this.#port = this.#state[storedConnectionString].port!;
          foundDangling = true;
          break;
        }
      }
      if (!foundDangling) {
        // If we've visited all known nodes, clear the walk and try again
        this.#walk.clear();
        this.#shuffleCurrentNode();
      }
    }
  }

  #resetToSeed() {
    this.#host = this.#seedHost;
    this.#port = this.#seedPort;
    this.#walk.clear();
  }

  #shuffleCurrentNode() {
    // If we have trouble reaching a node, just shuffle to any other node and walk from there
    const otherNodes = Object.values(this.#state).filter(
      (node) =>
        (node.host !== this.#host || node.port !== this.#port) &&
        this.#host &&
        this.#port,
    );

    // Just return if we don't have any possible alternatives
    if (otherNodes.length == 0) {
      return;
    }

    const randomNode =
      otherNodes[Math.floor(Math.random() * otherNodes.length)];

    this.#host = randomNode.host!;
    this.#port = randomNode.port!;

    // And we have to invalidate the current walk to avoid accidental pruning
    this.#walk.clear();
  }

  async #crawl() {
    if (this.#canAdvance) {
      this.#canAdvance = false;
      const connectionString = `${this.#host}:${this.#port}`;
      console.log(`Connecting to ${connectionString}`);
      this.#client = connect({ host: this.#host, port: this.#port });

      try {
        // Request ID to see if the node is even responsive
        const { id } = await this.#client.getNodeIdRemoteHelper();

        // If it is, plumb out the object in state if anything is missing
        if (!this.#state[connectionString]) {
          this.#state[connectionString] = {};
          this.#state[connectionString].host = this.#host;
          this.#state[connectionString].port = this.#port;
        }
        if (!this.#state[connectionString].fingerTable) {
          this.#state[connectionString].fingerTable = {};
        }
        if (!this.#state[connectionString].userIds) {
          this.#state[connectionString].userIds = [];
        }
        if (!this.#state[connectionString].id) {
          this.#state[connectionString].id = id;
        }

        // Update Fingers
        const fingerTableStream = await this.#client.getFingerTableEntries();
        this.#state[connectionString].fingerTable = {};
        await new Promise<void>((resolve, reject) => {
          fingerTableStream.on(
            "data",
            ({ index, node }: { index: number; node: unknown }) => {
              this.#state[connectionString].fingerTable![index] = node;
            },
          );
          fingerTableStream.on("end", resolve);
          fingerTableStream.on("error", reject);
        });

        // Update UserIds
        const userIdStream = await this.#client.getUserIds();
        this.#state[connectionString].userIds = [];
        await new Promise<void>((resolve, reject) => {
          userIdStream.on("data", (idWithMetadata: any) => {
            this.#state[connectionString].userIds!.push(idWithMetadata);
          });
          userIdStream.on("end", resolve);
          userIdStream.on("error", reject);
        });

        // Update Predecessor
        const predecessorNode = await this.#client.getPredecessor();
        this.#state[connectionString].predecessor = predecessorNode;

        // Update Successor
        const successorNode = await this.#client.getSuccessorRemoteHelper();
        if (!successorNode.host || !successorNode.port) {
          console.error(
            `Node ${connectionString} returned invalid successor`,
            `(host=${successorNode.host}, port=${successorNode.port}) — pruning and resetting to seed`,
          );
          delete this.#state[connectionString];
          this.#resetToSeed();
          return;
        }
        this.#state[connectionString].successor = successorNode;

        // Advance to the successor or a known node in a partition
        this.#advance();
      } catch (err) {
        if ((err as { code?: number }).code == 14) {
          console.error(
            `Node ${connectionString} is unreachable — pruning and resetting to seed`,
          );
          delete this.#state[connectionString];
          this.#resetToSeed();
        } else {
          console.error(`Unexpected error crawling ${connectionString}:`, err);
        }
      } finally {
        this.#canAdvance = true;
      }
    }
  }
}

function main() {
  const args = minimist(process.argv.slice(2));
  const crawler = new ChordCrawler(
    args.host || DEFAULT_HOST_NAME,
    args.port || 8440,
    args.interval || CRAWLER_INTERVAL_MS,
  );

  const app = express();
  const port = args.webPort || 1337;
  app.use(express.static(PUBLIC_PATH));
  app.get("/data", (_, res) => res.json(crawler.state));
  app.listen(port, () => console.log(`Example app listening on port ${port}!`));
}

main();
