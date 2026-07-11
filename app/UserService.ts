import os from "os";
import path from "path";
import * as grpc from "@grpc/grpc-js";
import { loadSync } from "@grpc/proto-loader";

import { ChordNode } from "./ChordNode.ts";
import { HealthImplementation } from "./health.ts";
import {
  connect,
  handleGRPCErrors,
  isInModuloRange,
  loadTlsCredentials,
  NULL_NODE,
  computeIntegerHash,
} from "./utils.ts";

const packageDefinition = loadSync(
  path.resolve(import.meta.dirname, "../protos/chord.proto"),
  {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
  },
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
  userMap: { [key: string]: User[] };

  constructor({
    id,
    host = os.hostname(),
    port = 1337,
  }: {
    id?: number;
    host?: string;
    port?: number;
  }) {
    super({ id, host, port });
    this.userMap = {};
  }

  // Starts the gRPC Server
  serve() {
    const server = new grpc.Server();
    // Kept on the instance so destructor() can drain in-flight RPCs before
    // the process exits (issue #243).
    this.server = server;
    server.addService((chord as any).Node.service, {
      fetch: this.fetch.bind(this),
      remove: this.remove.bind(this),
      removeUserRemoteHelper: this.removeUserRemoteHelper.bind(this),
      insert: this.insert.bind(this),
      insertUserRemoteHelper: this.insertUserRemoteHelper.bind(this),
      lookup: this.lookup.bind(this),
      lookupUserRemoteHelper: this.lookupUserRemoteHelper.bind(this),
      migrateUsersToPredecessorRemoteHelper:
        this.migrateUsersToPredecessorRemoteHelper.bind(this),
      bulkInsertUsersRemoteHelper: this.bulkInsertUsersRemoteHelper.bind(this),
      getNodeIdRemoteHelper: this.getNodeIdRemoteHelper.bind(this),
      findSuccessorRemoteHelper: this.findSuccessorRemoteHelper.bind(this),
      getSuccessorRemoteHelper: this.getSuccessorRemoteHelper.bind(this),
      setSuccessor: this.setSuccessor.bind(this),
      getPredecessor: this.getPredecessor.bind(this),
      setPredecessor: this.setPredecessor.bind(this),
      getUserIds: this.getUserIds.bind(this),
      getFingerTableEntries: this.getFingerTableEntries.bind(this),
      closestPrecedingFingerRemoteHelper:
        this.closestPrecedingFingerRemoteHelper.bind(this),
      notify: this.notify.bind(this),
      destructor: this.destructor.bind(this),
    });

    // Standard gRPC Health Checking Protocol (issue #97). Advertises SERVING
    // for the whole server so grpc_health_probe / k8s probes work out of the
    // box; destructor() flips it to NOT_SERVING on graceful shutdown.
    this.health = new HealthImplementation();
    server.addService(this.health.service, this.health.handlers);

    const tls = loadTlsCredentials();
    const serverCredentials = tls
      ? grpc.ServerCredentials.createSsl(
          null,
          [{ private_key: tls.key, cert_chain: tls.cert }],
          false,
        )
      : grpc.ServerCredentials.createInsecure();

    // We assume that binding to 0.0.0.0 indeed makes us accessible at this.host
    this.logger.info(
      `Serving on ${this.host}:${this.port} (${tls ? "TLS" : "insecure"})`,
    );
    server.bindAsync(`0.0.0.0:${this.port}`, serverCredentials, (err) => {
      if (err) {
        this.logger.error({ err }, `Failed to bind server: ${err.message}`);
        process.exit(1);
      }
    });
  }

  // Streams a List of User IDs stored by the Node
  getUserIds(call: {
    write: (arg0: { id: number; metadata: Metadata }) => void;
    end: () => void;
  }) {
    const users = Object.values(this.userMap).flat();
    users.forEach((user) => {
      call.write({
        id: user.id,
        metadata: user.metadata,
      });
    });
    call.end();
  }

  // Removes a User from local state matching both the hash bucket and the user ID
  removeUser(hashedUserId: string | number, userId: number) {
    const bucket = this.userMap[hashedUserId];
    if (!bucket) {
      this.logger.warn(
        `removeUser: user ${userId} not found at hash ${hashedUserId}`,
      );
      return { code: 5 };
    }
    const newBucket = bucket.filter((u) => u.id !== userId);
    if (newBucket.length === bucket.length) {
      this.logger.warn(
        `removeUser: user ${userId} not found at hash ${hashedUserId}`,
      );
      return { code: 5 };
    }
    if (newBucket.length === 0) {
      delete this.userMap[hashedUserId];
    } else {
      this.userMap[hashedUserId] = newBucket;
    }
    this.logger.info(
      `removeUser: user ${userId} removed at hash ${hashedUserId}`,
    );
    return null;
  }

  // gRPC Handler to allow other nodes to remove users from our local state
  async removeUserRemoteHelper(
    message: { request: { id: any; userId: number } },
    callback: (call: { code: number } | null, arg1: {}) => void,
  ) {
    this.logger.debug({ message }, "removeUserRemoteHelper");
    const err = this.removeUser(message.request.id, message.request.userId);
    callback(err, {});
  }

  // Removes a User regardless of location in cluster.
  //
  // The user is stored at two hash locations (primary + secondary). A remove
  // that deletes only one copy is a correctness bug, not just degraded
  // redundancy: lookup() falls back to the secondary hash, so the surviving
  // copy resurrects the "deleted" user. A failed leg is therefore retried
  // once, and a still half-applied remove is reported as an error (#238).
  // NOT_FOUND on a leg is not a failure — that hash location holds no copy,
  // which is the desired end state of a remove.
  async remove(
    message: { request: { id: any } },
    callback: (call: any, arg1: {}) => void,
  ) {
    const userId = message.request.id;
    const isNotFound = (err: any) =>
      Boolean(err) && err.code === grpc.status.NOT_FOUND;
    const isFailure = (err: any) => Boolean(err) && !isNotFound(err);

    let err1 = await this.removeWithHash(userId, true);
    let err2 = await this.removeWithHash(userId, false);

    // Exactly one leg failed: this attempt half-applied the remove. Retry the
    // failed leg once before reporting divergence.
    if (isFailure(err1) && !isFailure(err2)) {
      this.logger.warn(
        { err: err1, userId },
        `remove: primary-hash replica failed for user ${userId}; retrying`,
      );
      err1 = await this.removeWithHash(userId, true);
    } else if (isFailure(err2) && !isFailure(err1)) {
      this.logger.warn(
        { err: err2, userId },
        `remove: secondary-hash replica failed for user ${userId}; retrying`,
      );
      err2 = await this.removeWithHash(userId, false);
    }

    if (isFailure(err1) || isFailure(err2)) {
      const failedReplica = isFailure(err1) ? "primary" : "secondary";
      this.logger.warn(
        { err: isFailure(err1) ? err1 : err2, userId, failedReplica },
        `remove: user ${userId} was removed from one hash location but the ${failedReplica} replica still failed — replicas have diverged`,
      );
      callback(isFailure(err1) ? err1 : err2, {});
    } else if (isNotFound(err1) && isNotFound(err2)) {
      // The user existed at neither hash location.
      callback(err1, {});
    } else {
      // Every remaining combination leaves no surviving copy.
      callback(null, {});
    }
  }

  async removeWithHash(userId: number, isPrimaryHash: boolean) {
    let successor = NULL_NODE;
    let lookupKey: number | null = null;
    let errorString: string | null = null;
    this.logger.info(`remove: Attempting to remove user ${userId}`);

    //compute primary user ID from hash
    if (userId && userId !== null) {
      lookupKey = isPrimaryHash
        ? this.computeUserIdHashPrimary(userId)
        : this.computeUserIdHashSecondary(userId);
    } else {
      errorString = `remove: error computing hash of ${userId}.`;
      this.logger.error(errorString);
      throw new RangeError(errorString);
    }

    try {
      successor = await this.findSuccessor(lookupKey, this.encapsulateSelf());
    } catch (err) {
      successor = NULL_NODE;
      this.logger.error({ err }, "remove: findSuccessor failed");
    }

    if (this.iAmTheNode(successor)) {
      this.logger.debug("remove: removing user from local node");
      const err = this.removeUser(lookupKey, userId);
      return err;
    } else {
      try {
        this.logger.debug("remove: removing user from remote node");
        const successorClient = connect(successor);
        // Promise form: promisifyClient rejects with the remote gRPC error so a
        // remote remove failure surfaces to the caller instead of being
        // swallowed by an inline callback. See #181.
        await successorClient.removeUserRemoteHelper({ id: lookupKey, userId });
      } catch (err) {
        handleGRPCErrors(
          this.logger,
          "remove",
          "removeUserRemoteHelper",
          successor.host,
          successor.port,
          err,
        );
        return err;
      }
    }
  }

  // Insert User in local state
  insertUser(userEdit: any) {
    // We need to clone deep because objects are copy by reference
    const clonedUserEdit = structuredClone(userEdit);
    const key = clonedUserEdit.user.metadata.isPrimaryHash
      ? clonedUserEdit.user.metadata.primaryHash
      : clonedUserEdit.user.metadata.secondaryHash;

    this.logger.debug({ clonedUserEdit }, "insertUser");
    const { user, edit, update_mask } = clonedUserEdit;
    const paths: string[] = update_mask?.paths ?? [];

    const bucket = this.userMap[key] ?? [];
    const existingIndex = bucket.findIndex((u) => u.id === user.id);
    const exists = existingIndex !== -1;

    if (exists && !edit) {
      this.logger.warn(
        `insertUser: user ${user.id} already exists at hash ${key}`,
      );
      return { code: 6 };
    }

    if (edit && paths.length > 0) {
      // Partial update: merge only the fields named in the mask
      if (!exists) {
        this.logger.warn(
          `insertUser: user ${user.id} not found at hash ${key} for partial edit`,
        );
        return { code: 5 };
      }
      for (const field of paths) {
        (bucket[existingIndex] as any)[field] = user[field];
      }
      this.userMap[key] = bucket;
      this.logger.info(
        `insertUser: Partially edited User ${user.id} at hash ${key} (fields: ${paths.join(", ")})`,
      );
    } else {
      if (exists) {
        bucket[existingIndex] = user;
      } else {
        bucket.push(user);
      }
      this.userMap[key] = bucket;
      this.logger.info(
        `insertUser: ${edit ? "Edited" : "Inserted"} User ${user.id} at hash ${key}`,
      );
    }
    return null;
  }

  // gRPC Handler to allow other nodes to insert users into our local state
  async insertUserRemoteHelper(
    message: { request: any },
    callback: (call: { code: number } | null, arg1: {}) => void,
  ) {
    this.logger.debug({ message }, "insertUserRemoteHelper");
    const err = this.insertUser(message.request);
    callback(err, {});
  }

  // Client-streaming gRPC handler: receives a stream of User messages and inserts each locally
  bulkInsertUsersRemoteHelper(
    call: any,
    callback: (err: any, response: {}) => void,
  ) {
    call.on("data", (user: User) => {
      const err = this.insertUser({ user, edit: false });
      if (err) {
        this.logger.warn(
          { err, userId: user.id },
          "bulkInsertUsersRemoteHelper: insertUser failed",
        );
      }
    });
    call.on("end", () => callback(null, {}));
    call.on("error", (err: any) => callback(err, {}));
  }

  // Inserts a User regardless of location in cluster
  async insert(
    message: { request: any },
    callback: (call: any, arg1: {}) => void,
  ) {
    // User and isEdit flag
    const userEdit = message.request;

    // Add Metadata
    userEdit.user.metadata = {};
    userEdit.user.metadata.primaryHash = this.computeUserIdHashPrimary(
      userEdit.user.id,
    );
    userEdit.user.metadata.secondaryHash = this.computeUserIdHashSecondary(
      userEdit.user.id,
    );

    // Execute Insert or Edit at primary and secondary hash
    const err1 = await this.insertWithHash(userEdit, true);
    const err2 = await this.insertWithHash(userEdit, false);

    if (err1 && err2) {
      callback(err1, {});
    } else if (err1 || err2) {
      // Exactly one replica write failed: the two hash locations have
      // diverged. Since the RPC response is Empty, a gRPC error with details
      // is the only channel to report the degraded write honestly instead of
      // claiming full success (#238).
      const failedReplica = err1 ? "primary" : "secondary";
      const storedReplica = err1 ? "secondary" : "primary";
      this.logger.warn(
        { err: err1 ?? err2, userId: userEdit.user.id, failedReplica },
        `insert: degraded write for user ${userEdit.user.id}: ${failedReplica} replica failed; stored only at the ${storedReplica} hash location`,
      );
      callback(
        {
          code: grpc.status.INTERNAL,
          details: `degraded write: the ${failedReplica} replica failed; user ${userEdit.user.id} is stored only at the ${storedReplica} hash location`,
        },
        {},
      );
    } else {
      callback(null, {});
    }
  }

  async insertWithHash(userEdit: any, isPrimaryHash: boolean) {
    const user = userEdit.user;
    userEdit.user.metadata.isPrimaryHash = isPrimaryHash;
    let lookupKey: number = isPrimaryHash
      ? userEdit.user.metadata.primaryHash
      : userEdit.user.metadata.secondaryHash;
    let successor = NULL_NODE;

    this.logger.info(
      { userId: user.id, lookupKey },
      `insert: Attempting to insert user ${user.id} at ${lookupKey}`,
    );
    try {
      successor = await this.findSuccessor(lookupKey, this.encapsulateSelf());
    } catch (err) {
      successor = NULL_NODE;
      this.logger.error({ err }, "insert: findSuccessor failed");
    }

    if (this.iAmTheNode(successor)) {
      this.logger.debug("insert: inserting user to local node");
      const err = this.insertUser(userEdit);
      return err;
    } else {
      try {
        this.logger.debug(
          { lookupKey },
          "insert: inserting user to remote node",
        );
        const successorClient = connect(successor);
        // Promise form: promisifyClient rejects with the remote gRPC error
        // (e.g. code 6 ALREADY_EXISTS) so duplicate inserts surface to the
        // caller instead of being swallowed by an inline callback. See #181.
        await successorClient.insertUserRemoteHelper(userEdit);
      } catch (err) {
        handleGRPCErrors(
          this.logger,
          "insert",
          "insertUser",
          successor.host,
          successor.port,
          err,
        );
        return err;
      }
    }
  }

  // gRPC handler that returns a user locally from this node (hash in id, user ID in userId)
  fetch(
    message: { request: { id: any; userId: number } },
    callback: (call: { code: number } | null, arg1: User | null) => void,
  ) {
    const { id, userId } = message.request;
    this.logger.info(`fetch: Requested User ${userId} at hash ${id}`);
    const { err, user } = this.lookupUser(id, userId);
    callback(err, user);
  }

  // Look up user by hash bucket and user ID (supports chained collision buckets)
  lookupUser(hashedUserId: number, userId: number) {
    const bucket = this.userMap[hashedUserId];
    if (bucket) {
      const user = bucket.find((u) => u.id === userId);
      if (user) {
        this.logger.debug(
          `lookupUser: User ${userId} found at ${hashedUserId}`,
        );
        return { err: null, user };
      }
    }
    this.logger.debug(
      `lookupUser: User ${userId} not found at ${hashedUserId}`,
    );
    return { err: { code: 5 }, user: null };
  }

  async lookupUserRemoteHelper(
    message: { request: { id: any; userId: number } },
    callback: (call: any, arg1: any) => void,
  ) {
    this.logger.debug(
      { id: message.request.id },
      "beginning lookupUserRemoteHelper",
    );
    const { err, user } = this.lookupUser(
      message.request.id,
      message.request.userId,
    );
    this.logger.debug({ user }, "finishing lookupUserRemoteHelper");
    callback(err, user);
  }

  async lookup(
    message: { request: { id: number } },
    callback: (call: any, arg1: any) => void,
  ) {
    const userId = message.request.id;
    this.logger.info(`lookup: Looking up user ${userId}`);

    // Try Primary Hash
    let userErrorResponse = await this.lookupWithHash(userId, true);
    if (userErrorResponse.err) {
      // Try Secondary Hash in case of failure
      userErrorResponse = await this.lookupWithHash(userId, false);
    }
    callback(userErrorResponse.err, userErrorResponse.user);
  }

  async lookupWithHash(userId: number, isPrimaryHash: boolean) {
    let lookupKey: number | null = null;
    let errorString: string | null = null;
    let successor = NULL_NODE;

    //compute primary user ID from hash
    if (userId && userId !== null) {
      lookupKey = isPrimaryHash
        ? this.computeUserIdHashPrimary(userId)
        : this.computeUserIdHashSecondary(userId);
    } else {
      errorString = `lookup: error computing hash of ${userId}.`;
      this.logger.error(errorString);
      throw new RangeError(errorString);
    }

    try {
      successor = await this.findSuccessor(lookupKey, this.encapsulateSelf());
    } catch (err) {
      successor = NULL_NODE;
      this.logger.error({ err }, "lookup: findSuccessor failed");
    }

    if (this.iAmTheNode(successor)) {
      this.logger.debug("lookup: looking up user on local node");
      const { err, user } = this.lookupUser(lookupKey, userId);
      this.logger.debug({ err, user }, "lookup: finished server-side lookup");
      return { err, user };
    } else {
      try {
        this.logger.debug("lookup: looking up user on remote node");
        const successorClient = connect(successor);
        const user = await successorClient.lookupUserRemoteHelper({
          id: lookupKey,
          userId,
        });
        return { err: null, user };
      } catch (err) {
        handleGRPCErrors(
          this.logger,
          "lookup",
          "lookupUserRemoteHelper",
          successor.host,
          successor.port,
          err,
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
        this.logger,
        "migrateKeysBeforeDeparture",
        "migrateUsersToSuccessor",
        this.predecessor.host,
        this.predecessor.port,
        error,
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
        this.logger,
        "migrateKeysAfterJoining",
        "migrateUsersToPredecessorRemoteHelper",
        this.predecessor.host,
        this.predecessor.port,
        error,
      );
    }
  }

  async migrateUsersToPredecessor() {
    if (this.userMapIsEmpty()) return;

    const keysToMigrate = Object.keys(this.userMap).filter((hashedKey) =>
      isInModuloRange(
        parseInt(hashedKey, 10),
        this.id,
        false,
        this.predecessor.id,
        true,
      ),
    );

    if (keysToMigrate.length === 0) return;

    const client = connect(this.predecessor);

    await new Promise<void>((resolve, reject) => {
      const call = client.bulkInsertUsersRemoteHelper((err: any) => {
        if (err) reject(err);
        else resolve();
      });
      for (const hashedKey of keysToMigrate) {
        for (const user of this.userMap[hashedKey] ?? []) {
          call.write(user);
        }
      }
      call.end();
    });

    for (const hashedKey of keysToMigrate) {
      delete this.userMap[hashedKey];
    }
  }

  async migrateUsersToSuccessor() {
    if (this.userMapIsEmpty()) return;

    const client = connect(this.fingerTable[0].successor);

    await new Promise<void>((resolve, reject) => {
      const call = client.bulkInsertUsersRemoteHelper((err: any) => {
        if (err) reject(err);
        else resolve();
      });
      for (const hashedKey of Object.keys(this.userMap)) {
        for (const user of this.userMap[hashedKey] ?? []) {
          call.write(user);
        }
      }
      call.end();
    });

    this.userMap = {};
  }

  async migrateUsersToPredecessorRemoteHelper(
    _: any,
    callback: (err: any, response: {}) => void,
  ) {
    try {
      await this.migrateUsersToPredecessor();
      callback(null, {});
    } catch (error) {
      callback(error, {});
    }
  }

  // Checks if the local this.userMap is an empty object
  userMapIsEmpty() {
    return (
      Object.entries(this.userMap).length === 0 &&
      this.userMap.constructor === Object
    );
  }

  computeUserIdHashPrimary(userId: number): number {
    const highOrderBits = true;
    let userIdString: string = userId.toString().toLowerCase();
    let hashedUserId: number = computeIntegerHash(userIdString, highOrderBits);
    return hashedUserId;
  }

  computeUserIdHashSecondary(userId: number): number {
    const highOrderBits = false;
    let userIdString: string = userId.toString().toLowerCase();
    let hashedUserId: number = computeIntegerHash(userIdString, highOrderBits);
    return hashedUserId;
  }
}
