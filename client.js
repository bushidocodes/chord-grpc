const grpc = require("grpc");
const protoLoader = require("@grpc/proto-loader");
const minimist = require("minimist");
const path = require("path");
const caller = require("grpc-caller");
const PROTO_PATH = path.resolve(__dirname, "./protos/chord.proto");

const packageDefinition = protoLoader.loadSync(
  `${__dirname}/protos/chord.proto`,
  {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true
  }
);

const target = {
  ip: `localhost`,
  port: 1337
};

const chord = grpc.loadPackageDefinition(packageDefinition).chord;
let client;

// Used by Crawl to accumulate data about the Chord ring
const bigBucketOfData = {};
const ring = new Set([]);
let lastNode;
const NULL_USER = { id: null };

async function lookup({ _, ...rest }) {
  if (!rest.id) {
    console.log("fetch required an ID");
    process.exit();
  }
  console.log("Beginning client-side lookup: ", rest.id);
  const lookedUpUser = await client.lookup({ id: rest.id });
  console.log(lookedUpUser);
  if (!lookedUpUser) {
    console.log("Finished client-side lookup, found nothing");
  } else {
    console.log("Finished client-side lookup: ", lookedUpUser);
  }
}

function remove({ _, ...rest }) {
  if (!rest.id) {
    console.log("remove required an ID");
    process.exit();
  }
  console.log("Beginning client-side remove: ", rest.id);
  try {
    client.remove({ id: rest.id });
    console.log("Remove successful, finishing");
  } catch (err) {
    conosle.log("Remove failed, err: ", err);
  }
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

//Requests basic information about the target node
function summary() {
  console.log("Client requesting summary:");
  client.summary({ id: 1 }, (err, node) => {
    if (err) {
      console.log(err);
      console.log(node);
    } else {
      //console.log(node);
      console.log(
        `The node returned id: ${node.id}, ip: ${node.ip}, port: ${node.port}`
      );
    }
  });
}

//Requests basic information about the target node
async function crawl() {
  client = new chord.Node(
    `${lastNode.ip}:${lastNode.port}`,
    grpc.credentials.createInsecure()
  );
  // The argument is total garbage
  client.getSuccessor_remotehelper({ id: 99 }, (err, node) => {
    if (err) {
      console.log(err);
      let nodeToDelete = lastNode.id;
      // Remove the node from the bucket and select a random node
      for (elem in Object.keys(bigBucketOfData)) {
        if (
          elem &&
          bigBucketOfData[elem] &&
          bigBucketOfData[elem].successor &&
          bigBucketOfData[elem].successor.id &&
          bigBucketOfData[elem].successor.id !== lastNode.id
        ) {
          lastNode = bigBucketOfData[elem].successor;
          break;
        }
      }
      delete bigBucketOfData[nodeToDelete];
    } else {
      if (bigBucketOfData[lastNode.id]) {
        bigBucketOfData[lastNode.id].successor = node;
      }

      // If we've walked the logical ring and we didn't touch a node, delete it
      if (ring.has(node.id)) {
        for (elem of Object.keys(bigBucketOfData)) {
          console.log(bigBucketOfData[elem]);
          if (!ring.has(bigBucketOfData[elem].id)) {
            delete bigBucketOfData[elem];
          }
        }
        ring.clear();
      }
      ring.add(node.id);
      bigBucketOfData[node.id] = { ...bigBucketOfData[node.id], ...node };
      lastNode = node;
    }
  });
}

function main() {
  if (process.argv.length >= 3) {
    const args = minimist(process.argv.slice(3));

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
        lastNode = { id: target.id, ip: target.ip, port: target.port };
        setInterval(async () => {
          await crawl();
        }, 3000);
        const express = require("express");
        const app = express();
        const port = args.webPort || 3000;
        app.use(express.static("public"));
        app.get("/data", (req, res) => res.json(bigBucketOfData));
        app.listen(port, () =>
          console.log(`Example app listening on port ${port}!`)
        );
        break;
    }
  }
}

main();
