import fs from "fs";
import path from "path";
import { connectHealth } from "../app/health.ts";
import { connect } from "../app/utils.ts";

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

// True when the node is unreachable: gRPC UNAVAILABLE (14) or a raw
// ECONNREFUSED bubbling up. Used by bulkInsert to fail fast instead of
// re-attempting every record against a node that won't recover (issue #206).
export function isConnectionError(err: unknown): boolean {
  if ((err as { code?: number })?.code === 14) return true;
  const message = String((err as { message?: unknown })?.message ?? "");
  return /ECONNREFUSED|No connection established/i.test(message);
}

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
    // Guard `id` at runtime even though the type marks it required for
    // programmatic callers — the CLI passes parsed-but-untyped args here.
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

    try {
      await this.client.insert({ user: this.buildUser(args), edit: false });
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

  // Builds the wire User from CLI args, applying defaults for omitted fields.
  // Shared by insert() and bulkInsert().
  private buildUser(args: InsertArgs) {
    return {
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
  }

  async edit(args: EditArgs) {
    // Re-check `id` and the at-least-one-field rule at runtime — the CLI
    // passes parsed-but-untyped args here; `EditArgs` enforces them for typed
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

    let users: InsertArgs[];
    try {
      const jsonPath = path.resolve(import.meta.dirname, "..", args.path);
      const rawData = await fs.promises.readFile(jsonPath, "utf8");
      users = Object.values(JSON.parse(rawData));
    } catch (err) {
      console.error(`bulkInsert: could not read users from ${args.path}`);
      console.error(err);
      process.exitCode = 1;
      return;
    }

    // Insert sequentially (not Promise.all) so we don't fire tens of thousands
    // of concurrent inserts at the cluster and so the process doesn't exit
    // before they land. On the first connection failure, fail fast: a dead node
    // won't recover mid-loop, so attempting the remaining records would just
    // print one identical stack trace per record (issue #206).
    let inserted = 0;
    let duplicates = 0;
    for (const user of users) {
      if (!user.id) {
        console.warn("bulkInsert: skipping a record with no id");
        continue;
      }
      try {
        await this.client.insert({ user: this.buildUser(user), edit: false });
        inserted++;
      } catch (err) {
        if (isConnectionError(err)) {
          console.error(
            `bulkInsert: could not connect to node at ${this.host}:${this.port} — aborting after ${inserted} insert(s).`,
          );
          process.exitCode = 1;
          return;
        }
        const code = (err as { code?: number }).code;
        // ALREADY_EXISTS (6) is an expected per-record outcome, not a failure.
        if (code === 6) {
          duplicates++;
          continue;
        }
        console.error(
          `bulkInsert: failed to insert user ${user.id} (gRPC code ${code ?? "unknown"})`,
        );
      }
    }
    console.log(
      `bulkInsert: ${inserted} inserted, ${duplicates} already existed, ${users.length} total`,
    );
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
