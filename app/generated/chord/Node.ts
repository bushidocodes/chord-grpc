// Original file: protos/chord.proto

import type * as grpc from '@grpc/grpc-js'
import type { MethodDefinition } from '@grpc/proto-loader'
import type { Empty as _google_protobuf_Empty, Empty__Output as _google_protobuf_Empty__Output } from '../google/protobuf/Empty.ts';
import type { FingerTableEntry as _chord_FingerTableEntry, FingerTableEntry__Output as _chord_FingerTableEntry__Output } from '../chord/FingerTableEntry.ts';
import type { NodeAddress as _chord_NodeAddress, NodeAddress__Output as _chord_NodeAddress__Output } from '../chord/NodeAddress.ts';
import type { RemoteId as _chord_RemoteId, RemoteId__Output as _chord_RemoteId__Output } from '../chord/RemoteId.ts';
import type { User as _chord_User, User__Output as _chord_User__Output } from '../chord/User.ts';
import type { UserEdit as _chord_UserEdit, UserEdit__Output as _chord_UserEdit__Output } from '../chord/UserEdit.ts';
import type { UserId as _chord_UserId, UserId__Output as _chord_UserId__Output } from '../chord/UserId.ts';
import type { UserIdWithMetadata as _chord_UserIdWithMetadata, UserIdWithMetadata__Output as _chord_UserIdWithMetadata__Output } from '../chord/UserIdWithMetadata.ts';

export interface NodeClient extends grpc.Client {
  bulkInsertUsersRemoteHelper(metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_google_protobuf_Empty__Output>): grpc.ClientWritableStream<_chord_User>;
  bulkInsertUsersRemoteHelper(metadata: grpc.Metadata, callback: grpc.requestCallback<_google_protobuf_Empty__Output>): grpc.ClientWritableStream<_chord_User>;
  bulkInsertUsersRemoteHelper(options: grpc.CallOptions, callback: grpc.requestCallback<_google_protobuf_Empty__Output>): grpc.ClientWritableStream<_chord_User>;
  bulkInsertUsersRemoteHelper(callback: grpc.requestCallback<_google_protobuf_Empty__Output>): grpc.ClientWritableStream<_chord_User>;
  
  closestPrecedingFingerRemoteHelper(argument: _chord_RemoteId, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_chord_NodeAddress__Output>): grpc.ClientUnaryCall;
  closestPrecedingFingerRemoteHelper(argument: _chord_RemoteId, metadata: grpc.Metadata, callback: grpc.requestCallback<_chord_NodeAddress__Output>): grpc.ClientUnaryCall;
  closestPrecedingFingerRemoteHelper(argument: _chord_RemoteId, options: grpc.CallOptions, callback: grpc.requestCallback<_chord_NodeAddress__Output>): grpc.ClientUnaryCall;
  closestPrecedingFingerRemoteHelper(argument: _chord_RemoteId, callback: grpc.requestCallback<_chord_NodeAddress__Output>): grpc.ClientUnaryCall;
  
  fetch(argument: _chord_UserId, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_chord_User__Output>): grpc.ClientUnaryCall;
  fetch(argument: _chord_UserId, metadata: grpc.Metadata, callback: grpc.requestCallback<_chord_User__Output>): grpc.ClientUnaryCall;
  fetch(argument: _chord_UserId, options: grpc.CallOptions, callback: grpc.requestCallback<_chord_User__Output>): grpc.ClientUnaryCall;
  fetch(argument: _chord_UserId, callback: grpc.requestCallback<_chord_User__Output>): grpc.ClientUnaryCall;
  
  findSuccessorRemoteHelper(argument: _chord_RemoteId, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_chord_NodeAddress__Output>): grpc.ClientUnaryCall;
  findSuccessorRemoteHelper(argument: _chord_RemoteId, metadata: grpc.Metadata, callback: grpc.requestCallback<_chord_NodeAddress__Output>): grpc.ClientUnaryCall;
  findSuccessorRemoteHelper(argument: _chord_RemoteId, options: grpc.CallOptions, callback: grpc.requestCallback<_chord_NodeAddress__Output>): grpc.ClientUnaryCall;
  findSuccessorRemoteHelper(argument: _chord_RemoteId, callback: grpc.requestCallback<_chord_NodeAddress__Output>): grpc.ClientUnaryCall;
  
  getFingerTableEntries(argument: _chord_NodeAddress, metadata: grpc.Metadata, options?: grpc.CallOptions): grpc.ClientReadableStream<_chord_FingerTableEntry__Output>;
  getFingerTableEntries(argument: _chord_NodeAddress, options?: grpc.CallOptions): grpc.ClientReadableStream<_chord_FingerTableEntry__Output>;
  
  getNodeIdRemoteHelper(argument: _chord_NodeAddress, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_chord_NodeAddress__Output>): grpc.ClientUnaryCall;
  getNodeIdRemoteHelper(argument: _chord_NodeAddress, metadata: grpc.Metadata, callback: grpc.requestCallback<_chord_NodeAddress__Output>): grpc.ClientUnaryCall;
  getNodeIdRemoteHelper(argument: _chord_NodeAddress, options: grpc.CallOptions, callback: grpc.requestCallback<_chord_NodeAddress__Output>): grpc.ClientUnaryCall;
  getNodeIdRemoteHelper(argument: _chord_NodeAddress, callback: grpc.requestCallback<_chord_NodeAddress__Output>): grpc.ClientUnaryCall;
  
  getPredecessor(argument: _google_protobuf_Empty, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_chord_NodeAddress__Output>): grpc.ClientUnaryCall;
  getPredecessor(argument: _google_protobuf_Empty, metadata: grpc.Metadata, callback: grpc.requestCallback<_chord_NodeAddress__Output>): grpc.ClientUnaryCall;
  getPredecessor(argument: _google_protobuf_Empty, options: grpc.CallOptions, callback: grpc.requestCallback<_chord_NodeAddress__Output>): grpc.ClientUnaryCall;
  getPredecessor(argument: _google_protobuf_Empty, callback: grpc.requestCallback<_chord_NodeAddress__Output>): grpc.ClientUnaryCall;
  
  getSuccessorRemoteHelper(argument: _google_protobuf_Empty, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_chord_NodeAddress__Output>): grpc.ClientUnaryCall;
  getSuccessorRemoteHelper(argument: _google_protobuf_Empty, metadata: grpc.Metadata, callback: grpc.requestCallback<_chord_NodeAddress__Output>): grpc.ClientUnaryCall;
  getSuccessorRemoteHelper(argument: _google_protobuf_Empty, options: grpc.CallOptions, callback: grpc.requestCallback<_chord_NodeAddress__Output>): grpc.ClientUnaryCall;
  getSuccessorRemoteHelper(argument: _google_protobuf_Empty, callback: grpc.requestCallback<_chord_NodeAddress__Output>): grpc.ClientUnaryCall;
  
  getUserIds(argument: _chord_NodeAddress, metadata: grpc.Metadata, options?: grpc.CallOptions): grpc.ClientReadableStream<_chord_UserIdWithMetadata__Output>;
  getUserIds(argument: _chord_NodeAddress, options?: grpc.CallOptions): grpc.ClientReadableStream<_chord_UserIdWithMetadata__Output>;
  
  insert(argument: _chord_UserEdit, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_google_protobuf_Empty__Output>): grpc.ClientUnaryCall;
  insert(argument: _chord_UserEdit, metadata: grpc.Metadata, callback: grpc.requestCallback<_google_protobuf_Empty__Output>): grpc.ClientUnaryCall;
  insert(argument: _chord_UserEdit, options: grpc.CallOptions, callback: grpc.requestCallback<_google_protobuf_Empty__Output>): grpc.ClientUnaryCall;
  insert(argument: _chord_UserEdit, callback: grpc.requestCallback<_google_protobuf_Empty__Output>): grpc.ClientUnaryCall;
  
  insertUserRemoteHelper(argument: _chord_UserEdit, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_google_protobuf_Empty__Output>): grpc.ClientUnaryCall;
  insertUserRemoteHelper(argument: _chord_UserEdit, metadata: grpc.Metadata, callback: grpc.requestCallback<_google_protobuf_Empty__Output>): grpc.ClientUnaryCall;
  insertUserRemoteHelper(argument: _chord_UserEdit, options: grpc.CallOptions, callback: grpc.requestCallback<_google_protobuf_Empty__Output>): grpc.ClientUnaryCall;
  insertUserRemoteHelper(argument: _chord_UserEdit, callback: grpc.requestCallback<_google_protobuf_Empty__Output>): grpc.ClientUnaryCall;
  
  lookup(argument: _chord_UserId, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_chord_User__Output>): grpc.ClientUnaryCall;
  lookup(argument: _chord_UserId, metadata: grpc.Metadata, callback: grpc.requestCallback<_chord_User__Output>): grpc.ClientUnaryCall;
  lookup(argument: _chord_UserId, options: grpc.CallOptions, callback: grpc.requestCallback<_chord_User__Output>): grpc.ClientUnaryCall;
  lookup(argument: _chord_UserId, callback: grpc.requestCallback<_chord_User__Output>): grpc.ClientUnaryCall;
  
  lookupUserRemoteHelper(argument: _chord_UserId, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_chord_User__Output>): grpc.ClientUnaryCall;
  lookupUserRemoteHelper(argument: _chord_UserId, metadata: grpc.Metadata, callback: grpc.requestCallback<_chord_User__Output>): grpc.ClientUnaryCall;
  lookupUserRemoteHelper(argument: _chord_UserId, options: grpc.CallOptions, callback: grpc.requestCallback<_chord_User__Output>): grpc.ClientUnaryCall;
  lookupUserRemoteHelper(argument: _chord_UserId, callback: grpc.requestCallback<_chord_User__Output>): grpc.ClientUnaryCall;
  
  migrateUsersToPredecessorRemoteHelper(argument: _google_protobuf_Empty, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_google_protobuf_Empty__Output>): grpc.ClientUnaryCall;
  migrateUsersToPredecessorRemoteHelper(argument: _google_protobuf_Empty, metadata: grpc.Metadata, callback: grpc.requestCallback<_google_protobuf_Empty__Output>): grpc.ClientUnaryCall;
  migrateUsersToPredecessorRemoteHelper(argument: _google_protobuf_Empty, options: grpc.CallOptions, callback: grpc.requestCallback<_google_protobuf_Empty__Output>): grpc.ClientUnaryCall;
  migrateUsersToPredecessorRemoteHelper(argument: _google_protobuf_Empty, callback: grpc.requestCallback<_google_protobuf_Empty__Output>): grpc.ClientUnaryCall;
  
  notify(argument: _chord_NodeAddress, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_google_protobuf_Empty__Output>): grpc.ClientUnaryCall;
  notify(argument: _chord_NodeAddress, metadata: grpc.Metadata, callback: grpc.requestCallback<_google_protobuf_Empty__Output>): grpc.ClientUnaryCall;
  notify(argument: _chord_NodeAddress, options: grpc.CallOptions, callback: grpc.requestCallback<_google_protobuf_Empty__Output>): grpc.ClientUnaryCall;
  notify(argument: _chord_NodeAddress, callback: grpc.requestCallback<_google_protobuf_Empty__Output>): grpc.ClientUnaryCall;
  
  remove(argument: _chord_UserId, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_google_protobuf_Empty__Output>): grpc.ClientUnaryCall;
  remove(argument: _chord_UserId, metadata: grpc.Metadata, callback: grpc.requestCallback<_google_protobuf_Empty__Output>): grpc.ClientUnaryCall;
  remove(argument: _chord_UserId, options: grpc.CallOptions, callback: grpc.requestCallback<_google_protobuf_Empty__Output>): grpc.ClientUnaryCall;
  remove(argument: _chord_UserId, callback: grpc.requestCallback<_google_protobuf_Empty__Output>): grpc.ClientUnaryCall;
  
  removeUserRemoteHelper(argument: _chord_UserId, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_google_protobuf_Empty__Output>): grpc.ClientUnaryCall;
  removeUserRemoteHelper(argument: _chord_UserId, metadata: grpc.Metadata, callback: grpc.requestCallback<_google_protobuf_Empty__Output>): grpc.ClientUnaryCall;
  removeUserRemoteHelper(argument: _chord_UserId, options: grpc.CallOptions, callback: grpc.requestCallback<_google_protobuf_Empty__Output>): grpc.ClientUnaryCall;
  removeUserRemoteHelper(argument: _chord_UserId, callback: grpc.requestCallback<_google_protobuf_Empty__Output>): grpc.ClientUnaryCall;
  
  setPredecessor(argument: _chord_NodeAddress, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_google_protobuf_Empty__Output>): grpc.ClientUnaryCall;
  setPredecessor(argument: _chord_NodeAddress, metadata: grpc.Metadata, callback: grpc.requestCallback<_google_protobuf_Empty__Output>): grpc.ClientUnaryCall;
  setPredecessor(argument: _chord_NodeAddress, options: grpc.CallOptions, callback: grpc.requestCallback<_google_protobuf_Empty__Output>): grpc.ClientUnaryCall;
  setPredecessor(argument: _chord_NodeAddress, callback: grpc.requestCallback<_google_protobuf_Empty__Output>): grpc.ClientUnaryCall;
  
  setSuccessor(argument: _chord_NodeAddress, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_google_protobuf_Empty__Output>): grpc.ClientUnaryCall;
  setSuccessor(argument: _chord_NodeAddress, metadata: grpc.Metadata, callback: grpc.requestCallback<_google_protobuf_Empty__Output>): grpc.ClientUnaryCall;
  setSuccessor(argument: _chord_NodeAddress, options: grpc.CallOptions, callback: grpc.requestCallback<_google_protobuf_Empty__Output>): grpc.ClientUnaryCall;
  setSuccessor(argument: _chord_NodeAddress, callback: grpc.requestCallback<_google_protobuf_Empty__Output>): grpc.ClientUnaryCall;
  
}

export interface NodeHandlers extends grpc.UntypedServiceImplementation {
  bulkInsertUsersRemoteHelper: grpc.handleClientStreamingCall<_chord_User__Output, _google_protobuf_Empty>;
  
  closestPrecedingFingerRemoteHelper: grpc.handleUnaryCall<_chord_RemoteId__Output, _chord_NodeAddress>;
  
  fetch: grpc.handleUnaryCall<_chord_UserId__Output, _chord_User>;
  
  findSuccessorRemoteHelper: grpc.handleUnaryCall<_chord_RemoteId__Output, _chord_NodeAddress>;
  
  getFingerTableEntries: grpc.handleServerStreamingCall<_chord_NodeAddress__Output, _chord_FingerTableEntry>;
  
  getNodeIdRemoteHelper: grpc.handleUnaryCall<_chord_NodeAddress__Output, _chord_NodeAddress>;
  
  getPredecessor: grpc.handleUnaryCall<_google_protobuf_Empty__Output, _chord_NodeAddress>;
  
  getSuccessorRemoteHelper: grpc.handleUnaryCall<_google_protobuf_Empty__Output, _chord_NodeAddress>;
  
  getUserIds: grpc.handleServerStreamingCall<_chord_NodeAddress__Output, _chord_UserIdWithMetadata>;
  
  insert: grpc.handleUnaryCall<_chord_UserEdit__Output, _google_protobuf_Empty>;
  
  insertUserRemoteHelper: grpc.handleUnaryCall<_chord_UserEdit__Output, _google_protobuf_Empty>;
  
  lookup: grpc.handleUnaryCall<_chord_UserId__Output, _chord_User>;
  
  lookupUserRemoteHelper: grpc.handleUnaryCall<_chord_UserId__Output, _chord_User>;
  
  migrateUsersToPredecessorRemoteHelper: grpc.handleUnaryCall<_google_protobuf_Empty__Output, _google_protobuf_Empty>;
  
  notify: grpc.handleUnaryCall<_chord_NodeAddress__Output, _google_protobuf_Empty>;
  
  remove: grpc.handleUnaryCall<_chord_UserId__Output, _google_protobuf_Empty>;
  
  removeUserRemoteHelper: grpc.handleUnaryCall<_chord_UserId__Output, _google_protobuf_Empty>;
  
  setPredecessor: grpc.handleUnaryCall<_chord_NodeAddress__Output, _google_protobuf_Empty>;
  
  setSuccessor: grpc.handleUnaryCall<_chord_NodeAddress__Output, _google_protobuf_Empty>;
  
}

export interface NodeDefinition extends grpc.ServiceDefinition {
  bulkInsertUsersRemoteHelper: MethodDefinition<_chord_User, _google_protobuf_Empty, _chord_User__Output, _google_protobuf_Empty__Output>
  closestPrecedingFingerRemoteHelper: MethodDefinition<_chord_RemoteId, _chord_NodeAddress, _chord_RemoteId__Output, _chord_NodeAddress__Output>
  fetch: MethodDefinition<_chord_UserId, _chord_User, _chord_UserId__Output, _chord_User__Output>
  findSuccessorRemoteHelper: MethodDefinition<_chord_RemoteId, _chord_NodeAddress, _chord_RemoteId__Output, _chord_NodeAddress__Output>
  getFingerTableEntries: MethodDefinition<_chord_NodeAddress, _chord_FingerTableEntry, _chord_NodeAddress__Output, _chord_FingerTableEntry__Output>
  getNodeIdRemoteHelper: MethodDefinition<_chord_NodeAddress, _chord_NodeAddress, _chord_NodeAddress__Output, _chord_NodeAddress__Output>
  getPredecessor: MethodDefinition<_google_protobuf_Empty, _chord_NodeAddress, _google_protobuf_Empty__Output, _chord_NodeAddress__Output>
  getSuccessorRemoteHelper: MethodDefinition<_google_protobuf_Empty, _chord_NodeAddress, _google_protobuf_Empty__Output, _chord_NodeAddress__Output>
  getUserIds: MethodDefinition<_chord_NodeAddress, _chord_UserIdWithMetadata, _chord_NodeAddress__Output, _chord_UserIdWithMetadata__Output>
  insert: MethodDefinition<_chord_UserEdit, _google_protobuf_Empty, _chord_UserEdit__Output, _google_protobuf_Empty__Output>
  insertUserRemoteHelper: MethodDefinition<_chord_UserEdit, _google_protobuf_Empty, _chord_UserEdit__Output, _google_protobuf_Empty__Output>
  lookup: MethodDefinition<_chord_UserId, _chord_User, _chord_UserId__Output, _chord_User__Output>
  lookupUserRemoteHelper: MethodDefinition<_chord_UserId, _chord_User, _chord_UserId__Output, _chord_User__Output>
  migrateUsersToPredecessorRemoteHelper: MethodDefinition<_google_protobuf_Empty, _google_protobuf_Empty, _google_protobuf_Empty__Output, _google_protobuf_Empty__Output>
  notify: MethodDefinition<_chord_NodeAddress, _google_protobuf_Empty, _chord_NodeAddress__Output, _google_protobuf_Empty__Output>
  remove: MethodDefinition<_chord_UserId, _google_protobuf_Empty, _chord_UserId__Output, _google_protobuf_Empty__Output>
  removeUserRemoteHelper: MethodDefinition<_chord_UserId, _google_protobuf_Empty, _chord_UserId__Output, _google_protobuf_Empty__Output>
  setPredecessor: MethodDefinition<_chord_NodeAddress, _google_protobuf_Empty, _chord_NodeAddress__Output, _google_protobuf_Empty__Output>
  setSuccessor: MethodDefinition<_chord_NodeAddress, _google_protobuf_Empty, _chord_NodeAddress__Output, _google_protobuf_Empty__Output>
}
