import path from "path";
import fs from "fs";
import { connect } from "../app/utils.ts";
import { connectHealth } from "../app/health.ts";

// Fields a caller may set on insert or change on edit. The server defaults any
// omitted field on insert and leaves it untouched on a partial edit.
interface EditableFields {
  reputation: number;
  creationDate: string;
  displayName: string;
  lastAccessDate: string;
  websiteUrl: string;
  location: string;
  aboutMe: string;
  views: number;
  upVotes: number;
  downVotes: number;
  profileImageUrl: string;
  accountId: number;
}

// Requires at least one key of T to be present (the rest stay optional).
type AtLeastOne<T> = {
  [K in keyof T]-?: Pick<T, K> & Partial<Omit<T, K>>;
}[keyof T];

// Insert needs an id; every user field is optional and defaulted server-side.
export type InsertArgs = { id: number } & Partial<EditableFields>;

// Edit needs an id plus at least one editable field to patch.
export type EditArgs = { id: number } & AtLeastOne<EditableFields>;

export class Client {
  host: string;
  port: number;
  client: any;
  healthClient: ReturnType<typeof connectHealth>;
  constructor(host: string, port: number) {
    this.host = host;
    this.port = port;
    this.client = connect({ host: this.host, port: this.port });
    this.healthClient = connectHealth({ host: this.host, port: this.port });
  }

  // Standard gRPC health check (grpc.health.v1.Health/Check). Exits non-zero on
  // a non-SERVING status or error so it doubles as a liveness probe.
  async health() {
    try {
      const { status } = await this.healthClient.check();
      console.log(`Health of ${this.host}:${this.port}: ${status}`);
      if (status !== "SERVING") process.exitCode = 1;
    } catch (err) {
      console.error(`Health check failed for ${this.host}:${this.port}`);
      console.error(err);
      process.exitCode = 1;
    }
  }

  // Identity (via getNodeIdRemoteHelper) plus liveness (via Health/Check). The
  // bespoke `summary` RPC that previously returned identity was removed in #97.
  async summary() {
    console.log("Client requesting summary:");
    try {
      const node = await this.client.getNodeIdRemoteHelper();
      let status = "UNKNOWN";
      try {
        ({ status } = await this.healthClient.check());
      } catch {
        status = "UNREACHABLE";
      }
      console.log(
        `The node returned id: ${node.id}, host: ${node.host}, port: ${node.port}, health: ${status}`,
      );
    } catch (err) {
      console.error(err);
    }
  }

  async fingerTable() {
    try {
      const node = await this.client.getNodeIdRemoteHelper();
      console.log(
        `Finger table for node ${node.id} (${node.host}:${node.port}):`,
      );
      const stream = this.client.getFingerTableEntries();
      let i = 0;
      await new Promise<void>((resolve, reject) => {
        stream.on("data", ({ index, node }: { index: number; node: any }) => {
          console.log(
            `  [${i++}]  start=${index}  successor=${node.id} (${node.host}:${node.port})`,
          );
        });
        stream.on("end", resolve);
        stream.on("error", reject);
      });
    } catch (err) {
      console.error(err);
    }
  }

  async predecessor() {
    try {
      const node = await this.client.getPredecessor();
      if (!node || (!node.id && !node.host && !node.port)) {
        console.log("No predecessor set");
      } else {
        console.log(
          `Predecessor: id=${node.id}, host=${node.host}, port=${node.port}`,
        );
      }
    } catch (err) {
      console.error(err);
    }
  }

  async successor() {
    try {
      const node = await this.client.getSuccessorRemoteHelper();
      if (!node || (!node.id && !node.host && !node.port)) {
        console.log("No successor set");
      } else {
        console.log(
          `Successor: id=${node.id}, host=${node.host}, port=${node.port}`,
        );
      }
    } catch (err) {
      console.error(err);
    }
  }

  async lookup(args: { id?: number }) {
    if (!args.id) {
      console.log("lookup requires an ID");
      process.exit();
    }

    await this.client.lookup({ id: args.id }, (err: any, user: any) => {
      if (err) {
        console.error(`User with userId ${args.id} not found`);
        console.error(err);
      } else {
        console.log("User found: ", user);
      }
    });
  }

  async insert(args: InsertArgs) {
    // The CLI dispatches untyped minimist args, so guard `id` at runtime even
    // though the type marks it required for programmatic callers.
    if (!args.id) {
      console.log("id is a mandatory field!");
      console.log("node client insert --id=42424242");
      console.log(
        "optional fields include reputation, creationDate, displayName, lastAccessDate, websiteUrl, location, aboutMe, views, upVotes, downVotes, profileImageUrl, accountId",
      );
      console.log(
        'node client insert --id=42424242 --displayName="Sean McBride" --reputation=3 --website="https://www.bushido.codes"',
      );
      process.exit();
    }

    const user = {
      id: args.id,
      reputation: args.reputation || 0,
      creationDate: args.creationDate || Date.now().toString(),
      displayName: args.displayName || "",
      lastAccessDate: args.lastAccessDate || "",
      websiteUrl: args.websiteUrl || "",
      location: args.location || "",
      aboutMe: args.aboutMe || "",
      views: args.views || 0,
      upVotes: args.upVotes || 0,
      downVotes: args.downVotes || 0,
      profileImageUrl: args.profileImageUrl || "",
      accountId: args.accountId || 0,
    };
    try {
      await this.client.insert({ user, edit: false });
      console.log("User inserted successfully");
    } catch (err) {
      const grpcErr = err as { code?: number };
      switch (grpcErr.code) {
        case 6:
          console.log("User already exists!");
          break;
        default:
          console.log("User insertion error:", err);
      }
    }
  }

  async edit(args: EditArgs) {
    // The CLI dispatches untyped minimist args, so re-check `id` and the
    // at-least-one-field rule at runtime; `EditArgs` enforces them for typed
    // callers. A no-field edit would otherwise overwrite the user with a
    // bare { id }, wiping every other field server-side.
    if (!args.id) {
      console.log("id is a mandatory field!");
      console.log(
        'node client edit --id=42424242 --displayName="Sean McBride"',
      );
      process.exit();
    }

    const editableFields = [
      "reputation",
      "creationDate",
      "displayName",
      "lastAccessDate",
      "websiteUrl",
      "location",
      "aboutMe",
      "views",
      "upVotes",
      "downVotes",
      "profileImageUrl",
      "accountId",
    ] as const;
    const paths = editableFields.filter((f) => args[f] !== undefined);
    if (paths.length === 0) {
      console.log(
        'edit requires at least one field to update (e.g. --displayName="Alice")',
      );
      process.exit();
    }
    const user: any = { id: args.id };
    for (const field of paths) {
      user[field] = args[field];
    }
    try {
      await this.client.insert({ user, edit: true, update_mask: { paths } });
      console.log("User edited successfully");
    } catch (err) {
      console.log("User edit error:", err);
    }
  }

  async bulkInsert(args: { path?: string }) {
    if (!args.path) {
      console.log("bulkInsert requires a path to a JSON file");
      process.exit();
    }
    try {
      const jsonPath = path.resolve(import.meta.dirname, "..", args.path);
      console.log(jsonPath);
      const rawData = await fs.promises.readFile(jsonPath, "utf8");
      const data = JSON.parse(rawData);
      const users: InsertArgs[] = Object.values(data);
      // Await each insert sequentially so every user lands before the client
      // exits — the old forEach fired the async inserts and returned, tearing
      // down the process first. Sequential (vs Promise.all) also avoids firing
      // tens of thousands of concurrent inserts at the cluster for users.json.
      for (const user of users) {
        await this.insert(user);
      }
    } catch (err) {
      console.error(err);
    }
  }

  async remove(args: { id?: number }) {
    if (!args.id) {
      console.log("remove requires an ID");
      process.exit();
    }
    console.log("Beginning client-side remove: ", args.id);

    await this.client.remove({ id: args.id }, (err: any, _: any) => {
      if (err) {
        console.error("User not deleted");
        console.error(err);
      } else {
        console.log("User deleted");
      }
    });
  }
}
