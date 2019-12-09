import os from "os";
import path from "path";
import grpc from "grpc";
import { loadSync } from "@grpc/proto-loader";
import { cloneDeep } from "lodash";

import { ChordNode } from "./ChordNode";
import {
  connect,
  DEBUGGING_LOCAL,
  handleGRPCErrors,
  isInModuloRange,
  NULL_NODE,
  computeIntegerHash
} from "./utils";

const packageDefinition = loadSync(
  path.resolve(__dirname, "../protos/chord.proto"),
  {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true
  }
);
const chord = grpc.loadPackageDefinition(packageDefinition).chord;

interface Metadata {
  primaryHash: number;
  secondaryHash: number;
  isPrimaryHash: boolean;
}

interface User {
  id: number;
  reputation: number;
  creationDate: string;
  displayName: string;
  lastAccessData: string;
  websiteUrl: string;
  location: string;
  aboutMe: string;
  views: number;
  upVotes: number;
  downVotes: number;
  profileImageUrl: string;
  accountId: number;
  metadata: Metadata;
}

export class UserService extends ChordNode {
  userMap: { [key: string]: User };

  constructor({ id, host = os.hostname(), port = 1337 }) {
    super({ id, host, port });
    this.userMap = {};
  }

  // Starts the gRPC Server
  serve() {
    const server = new grpc.Server();
    // @ts-ignore
    server.addService(chord.Node.service, {
      summary: this.summary.bind(this),
      fetch: this.fetch.bind(this),
      remove: this.remove.bind(this),
      removeUserRemoteHelper: this.removeUserRemoteHelper.bind(this),
      insert: this.insert.bind(this),
      insertUserRemoteHelper: this.insertUserRemoteHelper.bind(this),
      lookup: this.lookup.bind(this),
      lookupUserRemoteHelper: this.lookupUserRemoteHelper.bind(this),
      migrateUsersToPredecessorRemoteHelper: this.migrateUsersToPredecessorRemoteHelper.bind(
        this
      ),
      getNodeIdRemoteHelper: this.getNodeIdRemoteHelper.bind(this),
      findSuccessorRemoteHelper: this.findSuccessorRemoteHelper.bind(this),
      getSuccessorRemoteHelper: this.getSuccessorRemoteHelper.bind(this),
      setSuccessor: this.setSuccessor.bind(this),
      getPredecessor: this.getPredecessor.bind(this),
      setPredecessor: this.setPredecessor.bind(this),
      getUserIds: this.getUserIds.bind(this),
      getFingerTableEntries: this.getFingerTableEntries.bind(this),
      closestPrecedingFingerRemoteHelper: this.closestPrecedingFingerRemoteHelper.bind(
        this
      ),
      updateFingerTable: this.updateFingerTable.bind(this),
      notify: this.notify.bind(this),
      destructor: this.destructor.bind(this)
    });

    // We assume that binding to 0.0.0.0 indeed makes us accessible at this.host
    console.log(`Serving on ${this.host}:${this.port}`);
    server.bind(
      `0.0.0.0:${this.port}`,
      grpc.ServerCredentials.createInsecure()
    );
    server.start();
  }

  // Streams a List of User IDs stored by the Node
  getUserIds(call) {
    const users = Object.values(this.userMap);
    users.forEach(user => {
      call.write({
        id: user.id,
        metadata: user.metadata
      });
    });
    call.end();
  }

  // Removes a User from local state matching the hash
  removeUser(hashedUserId) {
    if (this.userMap[hashedUserId]) {
      delete this.userMap[hashedUserId];
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
    const err = this.removeUser(message.request.id);
    callback(err, {});
  }

  // Removes a User regardless of location in cluster
  async remove(message, callback) {
    const userId = message.request.id;
    const err1 = await this.removeWithHash(userId, true);
    const err2 = await this.removeWithHash(userId, false);

    if (err1 && err2) callback(err1, {});
    else callback(null, {});
  }

  async removeWithHash(userId: number, isPrimaryHash: boolean) {
    let successor = NULL_NODE;
    let lookupKey: number = null;
    let errorString: string = null;
    console.log("remove: Attempting to remove user ", userId);

    //compute primary user ID from hash
    if (userId && userId !== null) {
      lookupKey = isPrimaryHash
        ? await this.computeUserIdHashPrimary(userId)
        : await this.computeUserIdHashSecondary(userId);
    } else {
      errorString = `insert: error computing hash of ${userId}.`;
      if (DEBUGGING_LOCAL) {
        console.log(errorString);
      }
      throw new RangeError(errorString);
    }

    try {
      successor = await this.findSuccessor(lookupKey, this.encapsulateSelf());
    } catch (err) {
      successor = NULL_NODE;
      console.error("remove: findSuccessor failed with ", err);
    }

    if (this.iAmTheNode(successor)) {
      if (DEBUGGING_LOCAL) console.log("remove: remove user from local node");
      const err = this.removeUser(lookupKey);
      return err;
    } else {
      try {
        if (DEBUGGING_LOCAL)
          console.log("remove: remove user from remote node");
        const successorClient = connect(successor);
        await successorClient.removeUserRemoteHelper(
          { id: lookupKey },
          (err, _) => {
            return err;
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
        return err;
      }
    }
  }

  // Insert User in local state
  insertUser(userEdit) {
    // We need to clone deep because objects are copy by reference
    const clonedUserEdit = cloneDeep(userEdit);
    const key = clonedUserEdit.user.metadata.isPrimaryHash
      ? clonedUserEdit.user.metadata.primaryHash
      : clonedUserEdit.user.metadata.secondaryHash;

    if (DEBUGGING_LOCAL) console.log("insertUser: ", clonedUserEdit);
    const { user, edit } = clonedUserEdit;

    if (this.userMap[key] && !edit) {
      console.log(`insertUser: user already exits at hash ${key}`);
      return { code: 6 };
    } else {
      this.userMap[key] = user;
      if (edit) {
        console.log(`insertUser: Edited User ${user.id} at hash ${key}`);
      } else {
        console.log(`insertUser: Inserted User ${user.id} at hash ${key}`);
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
    // User and isEdit flag
    const userEdit = message.request;

    // Add Metadata
    userEdit.user.metadata = {};
    userEdit.user.metadata.primaryHash = await this.computeUserIdHashPrimary(
      userEdit.user.id
    );
    userEdit.user.metadata.secondaryHash = await this.computeUserIdHashSecondary(
      userEdit.user.id
    );

    // Execute Insert or Edit at primary and secondary hash
    const err1 = await this.insertWithHash(userEdit, true);
    const err2 = await this.insertWithHash(userEdit, false);

    if (err1 && err2) callback(err1, {});
    else callback(null, {});
  }

  async insertWithHash(userEdit: any, isPrimaryHash: boolean) {
    const user = userEdit.user;
    userEdit.user.metadata.isPrimaryHash = isPrimaryHash;
    let lookupKey: number = isPrimaryHash
      ? userEdit.user.metadata.primaryHash
      : userEdit.user.metadata.secondaryHash;
    let successor = NULL_NODE;

    console.log(`insert: Attempting to insert user ${user.id} at ${lookupKey}`);
    if (DEBUGGING_LOCAL) console.log(user);
    try {
      successor = await this.findSuccessor(lookupKey, this.encapsulateSelf());
    } catch (err) {
      successor = NULL_NODE;
      console.error("insert: findSuccessor failed with ", err);
    }

    if (this.iAmTheNode(successor)) {
      if (DEBUGGING_LOCAL) console.log("insert: insert user to local node");
      const err = this.insertUser(userEdit);
      return err;
    } else {
      try {
        console.log("insert: insert user to remote node", lookupKey);
        const successorClient = connect(successor);
        await successorClient.insertUserRemoteHelper(userEdit, (err, _) => {
          console.log("insert finishing");
          return err;
        });
      } catch (err) {
        handleGRPCErrors(
          "insert",
          "insertUser",
          successor.host,
          successor.port,
          err
        );
        return err;
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

  // Look up user by hash
  lookupUser(hashedUserId) {
    if (this.userMap[hashedUserId]) {
      const user = this.userMap[hashedUserId];
      if (DEBUGGING_LOCAL)
        console.log(`User ${user.id} found at ${hashedUserId}`);
      return { err: null, user };
    } else {
      if (DEBUGGING_LOCAL) console.log(`User not found at ${hashedUserId}`);
      return { err: { code: 5 }, user: null };
    }
  }

  async lookupUserRemoteHelper(message, callback) {
    if (DEBUGGING_LOCAL)
      console.log("beginning lookupUserRemoteHelper: ", message.request.id);
    const { err, user } = this.lookupUser(message.request.id);
    if (DEBUGGING_LOCAL)
      console.log("finishing lookupUserRemoteHelper: ", user);
    callback(err, user);
  }

  async lookup(message, callback) {
    const userId = message.request.id;
    console.log(`lookup: Looking up user ${userId}`);

    // Try Primary Hash
    let userErrorResponse = await this.lookupWithHash(userId, true);
    if (userErrorResponse.err) {
      // Try Secondary Hash in case of failure
      userErrorResponse = await this.lookupWithHash(userId, false);
    }
    callback(userErrorResponse.err, userErrorResponse.user);
  }

  async lookupWithHash(userId: number, isPrimaryHash: boolean) {
    let lookupKey: number = null;
    let errorString: string = null;
    let successor = NULL_NODE;

    //compute primary user ID from hash
    if (userId && userId !== null) {
      lookupKey = isPrimaryHash
        ? await this.computeUserIdHashPrimary(userId)
        : await this.computeUserIdHashSecondary(userId);
    } else {
      errorString = `insert: error computing hash of ${userId}.`;
      if (DEBUGGING_LOCAL) {
        console.log(errorString);
      }
      throw new RangeError(errorString);
    }

    try {
      successor = await this.findSuccessor(lookupKey, this.encapsulateSelf());
    } catch (err) {
      successor = NULL_NODE;
      console.error("lookup: findSuccessor failed with ", err);
    }

    if (this.iAmTheNode(successor)) {
      if (DEBUGGING_LOCAL) console.log("lookup: lookup user to local node");
      const { err, user } = this.lookupUser(lookupKey);
      if (DEBUGGING_LOCAL)
        console.log(
          "lookup: finished Server-side lookup, returning: ",
          err,
          user
        );
      return { err, user };
    } else {
      try {
        console.log("In lookup: lookup user to remote node");
        const successorClient = connect(successor);
        const user = await successorClient.lookupUserRemoteHelper({
          id: lookupKey
        });
        return { err: null, user };
      } catch (err) {
        handleGRPCErrors(
          "lookup",
          "lookupUserRemotehelper",
          successor.host,
          successor.port,
          err
        );
        return { err, user: null };
      }
    }
  }

  async migrateKeysBeforeDeparture() {
    try {
      await this.migrateUsersToSuccessor();
      return true;
    } catch (error) {
      handleGRPCErrors(
        "migrateKeysAfterJoining",
        "migrateUsersToNewPredecessor",
        this.predecessor.host,
        this.predecessor.port,
        error
      );
      return false;
    }
  }

  async migrateKeysAfterJoining() {
    if (this.iAmMyOwnSuccessor()) return;

    const successorClient = connect(this.fingerTable[0].successor);
    try {
      await successorClient.migrateUsersToPredecessorRemoteHelper();
    } catch (error) {
      handleGRPCErrors(
        "migrateKeysAfterJoining",
        "migrateUsersToNewPredecessor",
        this.predecessor.host,
        this.predecessor.port,
        error
      );
    }
  }

  migrateUsersToPredecessor() {
    if (this.userMapIsEmpty()) return null;

    const client = connect(this.predecessor);

    Object.keys(this.userMap)
      .filter(hashedKey =>
        isInModuloRange(
          parseInt(hashedKey, 10),
          this.id,
          false,
          this.predecessor.id,
          true
        )
      )
      .forEach(hashedKey => {
        try {
          client.insertUserRemoteHelper({
            user: this.userMap[hashedKey],
            edit: false
          });
          this.removeUser(hashedKey);
        } catch (error) {
          handleGRPCErrors(
            "migrateUsersToPredecessor",
            "insertUserRemoteHelper",
            this.predecessor.host,
            this.predecessor.port,
            error
          );
        }
      });
    return null;
  }

  migrateUsersToSuccessor() {
    if (this.userMapIsEmpty()) return null;

    const client = connect(this.fingerTable[0].successor);

    Object.keys(this.userMap).forEach(hashedKey => {
      try {
        client.insertUserRemoteHelper({
          user: this.userMap[hashedKey],
          edit: false
        });
        this.removeUser(hashedKey);
      } catch (error) {
        handleGRPCErrors(
          "migrateUsersToSuccessor",
          "insertUserRemoteHelper",
          this.predecessor.host,
          this.predecessor.port,
          error
        );
      }
    });
    return null;
  }

  migrateUsersToPredecessorRemoteHelper(_, callback) {
    callback(this.migrateUsersToPredecessor(), {});
  }

  // Checks if the local this.userMap is an empty object
  userMapIsEmpty() {
    return (
      Object.entries(this.userMap).length === 0 &&
      this.userMap.constructor === Object
    );
  }

  async computeUserIdHashPrimary(userId: number): Promise<number> {
    const highOrderBits = true;
    let userIdString: string = userId.toString().toLowerCase();
    let hashedUserId: number = await computeIntegerHash(
      userIdString,
      highOrderBits
    );
    return hashedUserId;
  }

  async computeUserIdHashSecondary(userId: number): Promise<number> {
    const highOrderBits = false;
    let userIdString: string = userId.toString().toLowerCase();
    let hashedUserId: number = await computeIntegerHash(
      userIdString,
      highOrderBits
    );
    return hashedUserId;
  }
}
