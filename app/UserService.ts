import * as grpc from "@grpc/grpc-js";
import os from "os";

import { ChordNode } from "./ChordNode.ts";
import { buildNodeServiceHandlers } from "./grpcServer.ts";
import { GrpcTransport } from "./grpcTransport.ts";
import { HealthImplementation } from "./health.ts";
import { chordProto } from "./proto.ts";
import type { PeerTransport, UserTransport } from "./transport.ts";
import {
  computeIntegerHash,
  createLogger,
  isInModuloRange,
  loadTlsCredentials,
  type Node,
} from "./utils.ts";

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

// Outcomes of the replicated operations, shaped like gRPC status objects so
// the server adapter can pass them straight to the RPC callback.
type UserOpError = { code: number; details?: string } | Error | unknown;

export class UserService extends ChordNode {
  userMap: { [key: string]: User[] };
  userTransport: UserTransport;

  constructor({
    id,
    host = os.hostname(),
    port = 1337,
    transport,
  }: {
    id?: number;
    host?: string;
    port?: number;
    // Injectable for tests (e.g. an in-memory ring); defaults to gRPC.
    transport?: PeerTransport & UserTransport;
  }) {
    const resolvedTransport =
      transport ?? new GrpcTransport(createLogger(host, port));
    super({ id, host, port, transport: resolvedTransport });
    this.userTransport = resolvedTransport;
    this.userMap = {};
  }

  // Starts the gRPC Server (the inbound half of the transport adapter; the
  // handlers themselves live in app/grpcServer.ts, #233)
  serve() {
    const server = new grpc.Server();
    // Kept on the instance so destructor() can drain in-flight RPCs before
    // the process exits (issue #243).
    this.server = server;
    server.addService(chordProto.Node.service, buildNodeServiceHandlers(this));

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

  // Removes a User regardless of location in cluster.
  //
  // The user is stored at two hash locations (primary + secondary). A remove
  // that deletes only one copy is a correctness bug, not just degraded
  // redundancy: lookup falls back to the secondary hash, so the surviving
  // copy resurrects the "deleted" user. A failed leg is therefore retried
  // once, and a still half-applied remove is reported as an error (#238).
  // NOT_FOUND on a leg is not a failure — that hash location holds no copy,
  // which is the desired end state of a remove.
  async removeReplicated(userId: number): Promise<UserOpError | null> {
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
      return isFailure(err1) ? err1 : err2;
    } else if (isNotFound(err1) && isNotFound(err2)) {
      // The user existed at neither hash location.
      return err1;
    }
    // Every remaining combination leaves no surviving copy.
    return null;
  }

  async removeWithHash(
    userId: number,
    isPrimaryHash: boolean,
  ): Promise<UserOpError | null> {
    let successor: Node;
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
      successor = await this.findSuccessor(lookupKey);
    } catch (err) {
      // Routing failed: report this leg as failed instead of proceeding with
      // a sentinel value (#236); removeReplicated combines the legs (#238).
      this.logger.error({ err }, "remove: findSuccessor failed");
      return err;
    }

    if (this.iAmTheNode(successor)) {
      this.logger.debug("remove: removing user from local node");
      return this.removeUser(lookupKey, userId);
    }
    try {
      this.logger.debug("remove: removing user from remote node");
      // The transport rethrows the remote's original error so a remote
      // remove failure (e.g. NOT_FOUND) surfaces to the caller. See #181.
      await this.userTransport.removeUser(successor, lookupKey, userId);
      return null;
    } catch (err) {
      return err;
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

  // Inserts a User regardless of location in cluster (both replicas)
  async insertReplicated(userEdit: any): Promise<UserOpError | null> {
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
      return err1;
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
      return {
        code: grpc.status.INTERNAL,
        details: `degraded write: the ${failedReplica} replica failed; user ${userEdit.user.id} is stored only at the ${storedReplica} hash location`,
      };
    }
    return null;
  }

  async insertWithHash(
    userEdit: any,
    isPrimaryHash: boolean,
  ): Promise<UserOpError | null> {
    const user = userEdit.user;
    userEdit.user.metadata.isPrimaryHash = isPrimaryHash;
    const lookupKey: number = isPrimaryHash
      ? userEdit.user.metadata.primaryHash
      : userEdit.user.metadata.secondaryHash;
    let successor: Node;

    this.logger.info(
      { userId: user.id, lookupKey },
      `insert: Attempting to insert user ${user.id} at ${lookupKey}`,
    );
    try {
      successor = await this.findSuccessor(lookupKey);
    } catch (err) {
      // Routing failed: report this leg as failed instead of proceeding with
      // a sentinel value (#236); insertReplicated combines the legs (#238).
      this.logger.error({ err }, "insert: findSuccessor failed");
      return err;
    }

    if (this.iAmTheNode(successor)) {
      this.logger.debug("insert: inserting user to local node");
      return this.insertUser(userEdit);
    }
    try {
      this.logger.debug({ lookupKey }, "insert: inserting user to remote node");
      // The transport rethrows the remote's original error (e.g. code 6
      // ALREADY_EXISTS) so duplicate inserts surface to the caller. See #181.
      await this.userTransport.insertUser(successor, userEdit);
      return null;
    } catch (err) {
      return err;
    }
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

  // Looks up a User regardless of location in cluster, trying the primary
  // hash location first and falling back to the secondary replica.
  async lookupReplicated(
    userId: number,
  ): Promise<{ err: UserOpError | null; user: unknown }> {
    this.logger.info(`lookup: Looking up user ${userId}`);

    // Try Primary Hash
    let userErrorResponse = await this.lookupWithHash(userId, true);
    if (userErrorResponse.err) {
      // Try Secondary Hash in case of failure
      userErrorResponse = await this.lookupWithHash(userId, false);
    }
    return userErrorResponse;
  }

  async lookupWithHash(
    userId: number,
    isPrimaryHash: boolean,
  ): Promise<{ err: UserOpError | null; user: unknown }> {
    let lookupKey: number | null = null;
    let errorString: string | null = null;
    let successor: Node;

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
      successor = await this.findSuccessor(lookupKey);
    } catch (err) {
      // Routing failed: fail this hash location so lookupReplicated can fall
      // back to the other one, instead of proceeding with a sentinel (#236).
      this.logger.error({ err }, "lookup: findSuccessor failed");
      return { err, user: null };
    }

    if (this.iAmTheNode(successor)) {
      this.logger.debug("lookup: looking up user on local node");
      const { err, user } = this.lookupUser(lookupKey, userId);
      this.logger.debug({ err, user }, "lookup: finished server-side lookup");
      return { err, user };
    }
    try {
      this.logger.debug("lookup: looking up user on remote node");
      const user = await this.userTransport.lookupUser(
        successor,
        lookupKey,
        userId,
      );
      return { err: null, user };
    } catch (err) {
      return { err, user: null };
    }
  }

  async migrateKeysBeforeDeparture() {
    try {
      await this.migrateUsersToSuccessor();
      return true;
    } catch (error) {
      this.logger.error(
        { err: error },
        "migrateKeysBeforeDeparture: migrateUsersToSuccessor failed",
      );
      return false;
    }
  }

  async migrateKeysAfterJoining() {
    if (this.iAmMyOwnSuccessor()) return;

    try {
      await this.userTransport.requestMigrationToPredecessor(
        this.fingerTable[0].successor,
      );
    } catch (error) {
      this.logger.error(
        { err: error },
        "migrateKeysAfterJoining: requestMigrationToPredecessor failed",
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

    const users = keysToMigrate.flatMap(
      (hashedKey) => this.userMap[hashedKey] ?? [],
    );
    await this.userTransport.bulkInsertUsers(this.predecessor, users);

    for (const hashedKey of keysToMigrate) {
      delete this.userMap[hashedKey];
    }
  }

  async migrateUsersToSuccessor() {
    if (this.userMapIsEmpty()) return;

    const users = Object.values(this.userMap).flat();
    await this.userTransport.bulkInsertUsers(
      this.fingerTable[0].successor,
      users,
    );

    this.userMap = {};
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
    const userIdString: string = userId.toString().toLowerCase();
    const hashedUserId: number = computeIntegerHash(
      userIdString,
      highOrderBits,
    );
    return hashedUserId;
  }

  computeUserIdHashSecondary(userId: number): number {
    const highOrderBits = false;
    const userIdString: string = userId.toString().toLowerCase();
    const hashedUserId: number = computeIntegerHash(
      userIdString,
      highOrderBits,
    );
    return hashedUserId;
  }
}
