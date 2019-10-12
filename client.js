const grpc = require('grpc');
const protoLoader = require('@grpc/proto-loader');
const minimist = require('minimist')
const packageDefinition = protoLoader.loadSync(
  `${__dirname}/protos/chord.proto`,
  {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true
  });

const chord = grpc.loadPackageDefinition(packageDefinition).chord;
const client = new chord.Node('localhost:50053',
  grpc.credentials.createInsecure());

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

    console.log("optional fields include reputation, creationDate, displayName, lastAccessDate, websiteUrl, location, aboutMe, views, upVotes, downVotes, profileImageUrl, accountId");
    console.log('node client insert --id=42424242 --displayName="Sean McBride" --reputation=3 --website="https://www.bushido.codes"');
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
    accountId: rest.accoutId || 0,
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

// Returns information about a particular node
function summary() {
  client.summary({}, (err, summary) => {
    if (err) {
      console.log(err);
    } else {
      console.log(summary);
    }
  });
}

// Returns summary of multiple nodes that they will get it from each other
function chordInformation() {
  client.chordInformation({}, (err, summary) => {
    if (err) {
      console.log(err);
    } else {
      console.log(summary);
    }
  });
}

function main() {
  if (process.argv.length >= 3) {
    const args = minimist(process.argv.slice(3));
    const command = process.argv[2];
    switch (command) {
      case "fetch":
        fetch(args);
        break;
      case "insert":
        insert(args);
        break;
      case "summary":
        summary();
        break;
      case "chordInformation":
        chordInformation();
        break;
    }
  }
}

main();
