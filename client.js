const grpc = require("grpc");
const protoLoader = require("@grpc/proto-loader");
const minimist = require("minimist");
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

function fetch({ _: args }) {
  if (!args[0]) {
    console.log("fetch required an ID");
    process.exit();
  }
  id = parseInt(process.argv[3], 10);
  client.fetch({ id }, (err, user) => {
    if (err) {
      console.log(err);
    } else {
      console.log(user);
    }
  });
}

// I was originally thinking that the user service would assign the id, but this doesn't really seem possible...

function insert({ _, ...rest }) {
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
  client.insert(user, (err, user) => {
    if (err) {
      console.log(err);
    } else {
      console.log(user);
    }
  });
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

    const command = process.argv[2];

    switch (command) {
      case "fetch":
        client = new chord.Node(
          `${target.ip}:${target.port}`,
          grpc.credentials.createInsecure()
        );
        fetch(args);
        break;
      case "insert":
        client = new chord.Node(
          `${target.ip}:${target.port}`,
          grpc.credentials.createInsecure()
        );
        insert(args);
        break;
      case "summary":
        client = new chord.Node(
          `${target.ip}:${target.port}`,
          grpc.credentials.createInsecure()
        );
        summary();
        break;
      case "crawl":
        client = new chord.Node(
          `${target.ip}:${target.port}`,
          grpc.credentials.createInsecure()
        );
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
