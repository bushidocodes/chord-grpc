import caller from "grpc-caller";
import path from "path";

const PROTO_PATH = path.resolve(__dirname, "../protos/chord.proto");

export class Client {
  host: string;
  port: number;
  client: any;
  constructor(host, port) {
    this.host = host;
    this.port = port;
    this.client = caller(`${this.host}:${this.port}`, PROTO_PATH, "Node");
  }

  async summary() {
    console.log("Client requesting summary:");
    try {
      const node = await this.client.summary({ id: 1 });
      console.log(
        `The node returned id: ${node.id}, host: ${node.host}, port: ${node.port}`
      );
    } catch (err) {
      console.error(err);
    }
  }

  async migrateKeys() {
    console.log("Client testing migrate keys:");
    try {
      await this.client.migrateKeysBeforeLeavingRemoteHelper();
      console.log("Keys migrated successfully!");
    } catch (err) {
      console.error(err);
    }
  }

  async lookup({ _, ...rest }) {
    if (!rest.id) {
      console.log("lookup requires an ID");
      process.exit();
    }

    await this.client.lookup({ id: rest.id }, (err, user) => {
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
        "optional fields include reputation, creationDate, displayName, lastAccessDate, websiteUrl, location, aboutMe, views, upVotes, downVotes, profileImageUrl, accountId"
      );
      console.log(
        'node client insert --id=42424242 --displayName="Sean McBride" --reputation=3 --website="https://www.bushido.codes"'
      );
      process.exit();
    }
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
    try {
      const _ = await this.client.insert({ user, edit: rest.edit });
      if (rest.edit) {
        console.log("User edited successfully");
      } else {
        console.log("User inserted successfully");
      }
    } catch (err) {
      if (rest.edit) {
        console.log(err);
        console.log("User edit error:", err);
      } else {
        switch (err.code) {
          case 6:
            console.log("User already exists!");
            break;
          default:
            console.log("User insertion error:", err);
        }
      }
    }
  }

  async remove({ _, ...rest }) {
    if (!rest.id) {
      console.log("remove requires an ID");
      process.exit();
    }
    console.log("Beginning client-side remove: ", rest.id);

    await this.client.remove({ id: rest.id }, (err, _) => {
      if (err) {
        console.error("User not deleted");
        console.error(err);
      } else {
        console.log("User deleted");
      }
    });
  }
}
