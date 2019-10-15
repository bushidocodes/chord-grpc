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

const target = {
  ip: `localhost`,
  port: 1337
}

const chord = grpc.loadPackageDefinition(packageDefinition).chord;
let client;

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
        client = new chord.Node(`${target.ip}:${target.port}`, grpc.credentials.createInsecure());
        fetch(args);
        break;
      case "insert":
        client = new chord.Node(`${target.ip}:${target.port}`, grpc.credentials.createInsecure());
        insert(args);
        break;
    }
  }
}

main();
