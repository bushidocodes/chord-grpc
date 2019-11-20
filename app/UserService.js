const os = require("os");
const path = require("path");
const grpc = require("grpc");
const protoLoader = require("@grpc/proto-loader");

const { ChordNode } = require("./ChordNode.js");
const {
  connect,
  DEBUGGING_LOCAL,
  handleGRPCErrors,
  NULL_NODE
} = require("./utils.js");

const PROTO_PATH = path.resolve(__dirname, "../protos/chord.proto");
const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true
});
const chord = grpc.loadPackageDefinition(packageDefinition).chord;

class UserService extends ChordNode {
  constructor({
    id,
    host = os.hostname(),
    port = 1337,
    knownId,
    knownHost = host,
    knownPort = port
  }) {
    super({ id, host, port, knownId, knownHost, knownPort });
    this.userMap = {};
  }

  // Starts the gRPC Server
  serve() {
    const server = new grpc.Server();
    server.addService(chord.Node.service, {
      summary: this.summary.bind(this),
      fetch: this.fetch.bind(this),
      remove: this.remove.bind(this),
      removeUserRemoteHelper: this.removeUserRemoteHelper.bind(this),
      insert: this.insert.bind(this),
      insertUserRemoteHelper: this.insertUserRemoteHelper.bind(this),
      lookup: this.lookup.bind(this),
      lookupUserRemoteHelper: this.lookupUserRemoteHelper.bind(this),
      findSuccessorRemoteHelper: this.findSuccessorRemoteHelper.bind(this),
      getSuccessorRemoteHelper: this.getSuccessorRemoteHelper.bind(this),
      getPredecessor: this.getPredecessor.bind(this),
      setPredecessor: this.setPredecessor.bind(this),
      closestPrecedingFingerRemoteHelper: this.closestPrecedingFingerRemoteHelper.bind(
        this
      ),
      updateFingerTable: this.updateFingerTable.bind(this),
      notify: this.notify.bind(this)
    });

    // We assume that binding to 0.0.0.0 indeed makes us accessible at this.host
    console.log(`Serving on ${this.host}:${this.port}`);
    server.bind(
      `0.0.0.0:${this.port}`,
      grpc.ServerCredentials.createInsecure()
    );
    server.start();
  }

  // gRPC handler that returns a user locally from this node
  fetch(message, callback) {
    const {
      request: { id }
    } = message;
    console.log(`Requested User ${id}`);
    if (!this.userMap[id]) {
      callback({ code: 5 }, null); // NOT_FOUND error
    } else {
      callback(null, this.userMap[id]);
    }
  }

  // Removes a User from local state
  removeUser(id) {
    if (this.userMap[id]) {
      delete this.userMap[id];
      console.log("removeUser: user removed");
      return null;
    } else {
      console.log("removeUser, user DNE");
      return { code: 5 };
    }
  }

  // gRPC Handler to allow other nodes to remove users from our local state
  async removeUserRemoteHelper(message, callback) {
    if (DEBUGGING_LOCAL) console.log("removeUserRemoteHelper: ", message);
    const err = removeUser(message.request.id);
    callback(err, {});
  }

  // Removes a User regardless of location in cluster
  async remove(message, callback) {
    const userId = message.request.id;
    let successor = NULL_NODE;
    console.log("remove: Attempting to remove user ", userId);

    try {
      successor = await this.findSuccessor(userId, this.encapsulateSelf());
    } catch (err) {
      successor = NULL_NODE;
      console.error("remove: findSuccessor failed with ", err);
    }

    if (this.iAmMyOwnSuccessor()) {
      if (DEBUGGING_LOCAL) console.log("remove: remove user from local node");
      const err = this.removeUser(userId);
      callback(err, {});
    } else {
      try {
        if (DEBUGGING_LOCAL)
          console.log("remove: remove user from remote node");
        const successorClient = connect(successor);
        await successorClient.removeUserRemoteHelper(
          { id: userId },
          (err, _) => {
            callback(err, {});
          }
        );
      } catch (err) {
        handleGRPCErrors(
          "remove",
          "removeUserRemoteHelper",
          successor.host,
          successor.port,
          err
        );
        callback(err, null);
      }
    }
  }

  // Insert User in local state
  insertUser(userEdit) {
    if (DEBUGGING_LOCAL) console.log("insertUser: ", userEdit);
    const user = userEdit.user;
    const edit = userEdit.edit;
    if (this.userMap[user.id] && !edit) {
      console.log(`insertUser: ${user.id} already exits`);
      return { code: 6 };
    } else {
      this.userMap[user.id] = user;
      if (edit) {
        console.log(`insertUser: Edited User ${user.id}`);
      } else {
        console.log(`insertUser: Inserted User ${user.id}`);
      }
      return null;
    }
  }

  // gRPC Handler to allow other nodes to insert users into our local state
  async insertUserRemoteHelper(message, callback) {
    if (DEBUGGING_LOCAL) console.log("insertUserRemoteHelper: ", message);
    const err = this.insertUser(message.request);
    callback(err, {});
  }

  // Inserts a User regardless of location in cluster
  async insert(message, callback) {
    const userEdit = message.request;
    const user = userEdit.user;
    const lookupKey = user.id;
    let successor = NULL_NODE;

    console.log(`insert: Attempting to insert user`, user.id);
    if (DEBUGGING_LOCAL) console.log(user);
    try {
      successor = await this.findSuccessor(lookupKey, this.encapsulateSelf());
    } catch (err) {
      successor = NULL_NODE;
      console.error("insert: findSuccessor failed with ", err);
    }

    if (this.iAmMyOwnSuccessor()) {
      if (DEBUGGING_LOCAL) console.log("insert: insert user to local node");
      const err = this.insertUser(userEdit);
      callback(err, {});
    } else {
      try {
        console.log("insert: insert user to remote node", lookupKey);
        const successorClient = connect(successor);
        await successorClient.insertUserRemoteHelper(userEdit, (err, _) => {
          console.log("insert finishing");
          callback(err, {});
        });
      } catch (err) {
        handleGRPCErrors(
          "insert",
          "insertUser",
          successor.host,
          successor.port,
          err
        );
        callback(err, null);
      }
    }
  }

  // gRPC handler that returns a user locally from this node
  fetch(message, callback) {
    const {
      request: { id }
    } = message;
    console.log(`Requested User ${id}`);
    if (!this.userMap[id]) {
      callback({ code: 5 }, null); // NOT_FOUND error
    } else {
      callback(null, this.userMap[id]);
    }
  }

  lookupUser(userId) {
    if (this.userMap[userId]) {
      const user = this.userMap[userId];
      if (DEBUGGING_LOCAL) console.log(`User found ${user.id}`);
      return { err: null, user };
    } else {
      if (DEBUGGING_LOCAL) console.log(`User with user ID ${userId} not found`);
      return { err: { code: 5 }, user: null };
    }
  }

  async lookupUserRemoteHelper(message, callback) {
    console.log("beginning lookupUserRemoteHelper: ", message.request.id);
    const { err, user } = lookupUser(message.request.id);
    console.log("finishing lookupUserRemoteHelper: ", user);
    callback(err, user);
  }
  async lookup(message, callback) {
    const userId = message.request.id;
    console.log(`lookup: Looking up user ${userId}`);
    const lookupKey = userId;
    let successor = NULL_NODE;

    try {
      successor = await this.findSuccessor(lookupKey, this.encapsulateSelf());
    } catch (err) {
      successor = NULL_NODE;
      console.error("lookup: findSuccessor failed with ", err);
    }

    if (this.iAmMyOwnSuccessor()) {
      if (DEBUGGING_LOCAL) console.log("lookup: lookup user to local node");
      const { err, user } = this.lookupUser(userId);
      if (DEBUGGING_LOCAL)
        console.log(
          "lookup: finished Server-side lookup, returning: ",
          err,
          user
        );
      callback(err, user);
    } else {
      try {
        console.log("In lookup: lookup user to remote node");
        const successorClient = connect(successor);
        const user = await successorClient.lookupUserRemoteHelper({
          id: userId
        });
        callback(null, user);
      } catch (err) {
        handleGRPCErrors(
          "lookup",
          "lookupUserRemotehelper",
          successor.host,
          successor.port,
          err
        );
        callback(err, null);
      }
    }
  }
}

module.exports = {
  UserService
};
