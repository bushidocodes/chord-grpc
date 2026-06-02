import path from "path";
import fs from "fs";
import { connect } from "../app/utils.ts";

export class Client {
  host: string;
  port: number;
  client: any;
  constructor(host: string, port: number) {
    this.host = host;
    this.port = port;
    this.client = connect({ host: this.host, port: this.port });
  }

  async summary() {
    console.log("Client requesting summary:");
    try {
      const node = await this.client.summary({ id: 1 });
      console.log(
        `The node returned id: ${node.id}, host: ${node.host}, port: ${node.port}`,
      );
    } catch (err) {
      console.error(err);
    }
  }

  async fingerTable() {
    try {
      const summary = await this.client.summary({ id: 1 });
      console.log(
        `Finger table for node ${summary.id} (${summary.host}:${summary.port}):`,
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

  async lookup({ _, ...rest }) {
    if (!rest.id) {
      console.log("lookup requires an ID");
      process.exit();
    }

    await this.client.lookup({ id: rest.id }, (err: any, user: any) => {
      if (err) {
        console.error(`User with userId ${rest.id} not found`);
        console.error(err);
      } else {
        console.log("User found: ", user);
      }
    });
  }

  async insert({ _, ...rest }) {
    if (!rest.id) {
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

    if (rest.edit) {
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
      ];
      const paths = editableFields.filter((f) => rest[f] !== undefined);
      if (paths.length === 0) {
        console.log(
          'edit requires at least one field to update (e.g. --displayName="Alice")',
        );
        process.exit();
      }
      const user: any = { id: rest.id };
      for (const field of paths) {
        user[field] = rest[field];
      }
      try {
        await this.client.insert({ user, edit: true, update_mask: { paths } });
        console.log("User edited successfully");
      } catch (err) {
        console.log("User edit error:", err);
      }
      return;
    }

    const user = {
      id: rest.id,
      reputation: rest.reputation || 0,
      creationDate: rest.creationDate || Date.now().toString(),
      displayName: rest.displayName || "",
      lastAccessDate: rest.lastAccessDate || "",
      websiteUrl: rest.websiteUrl || "",
      location: rest.location || "",
      aboutMe: rest.aboutMe || "",
      views: rest.views || 0,
      upVotes: rest.upVotes || 0,
      downVotes: rest.downVotes || 0,
      profileImageUrl: rest.profileImageUrl || "",
      accountId: rest.accountId || 0,
    };
    try {
      await this.client.insert({ user, edit: false });
      console.log("User inserted successfully");
    } catch (err) {
      switch (err.code) {
        case 6:
          console.log("User already exists!");
          break;
        default:
          console.log("User insertion error:", err);
      }
    }
  }

  async bulkInsert({ _, ...rest }) {
    if (!rest.path) {
      console.log("bulkInsert requires a path to a JSON file");
      process.exit();
    }
    try {
      const jsonPath = path.resolve(import.meta.dirname, "..", rest.path);
      console.log(jsonPath);
      fs.readFile(jsonPath, "utf8", (err, rawData) => {
        const data = JSON.parse(rawData);
        const users: { [x: string]: any; _: any }[] = Object.values(data);
        users.forEach((user) => this.insert(user));
      });
    } catch (err) {
      console.error(err);
    }
  }

  async remove({ _, ...rest }) {
    if (!rest.id) {
      console.log("remove requires an ID");
      process.exit();
    }
    console.log("Beginning client-side remove: ", rest.id);

    await this.client.remove({ id: rest.id }, (err: any, _: any) => {
      if (err) {
        console.error("User not deleted");
        console.error(err);
      } else {
        console.log("User deleted");
      }
    });
  }
}
