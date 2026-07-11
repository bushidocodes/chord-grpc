import type * as grpc from '@grpc/grpc-js';
import type { MessageTypeDefinition } from '@grpc/proto-loader';

import type { FingerTableEntry as _chord_FingerTableEntry, FingerTableEntry__Output as _chord_FingerTableEntry__Output } from './chord/FingerTableEntry.ts';
import type { NodeClient as _chord_NodeClient, NodeDefinition as _chord_NodeDefinition } from './chord/Node.ts';
import type { NodeAddress as _chord_NodeAddress, NodeAddress__Output as _chord_NodeAddress__Output } from './chord/NodeAddress.ts';
import type { RemoteId as _chord_RemoteId, RemoteId__Output as _chord_RemoteId__Output } from './chord/RemoteId.ts';
import type { User as _chord_User, User__Output as _chord_User__Output } from './chord/User.ts';
import type { UserEdit as _chord_UserEdit, UserEdit__Output as _chord_UserEdit__Output } from './chord/UserEdit.ts';
import type { UserId as _chord_UserId, UserId__Output as _chord_UserId__Output } from './chord/UserId.ts';
import type { UserIdWithMetadata as _chord_UserIdWithMetadata, UserIdWithMetadata__Output as _chord_UserIdWithMetadata__Output } from './chord/UserIdWithMetadata.ts';
import type { Empty as _google_protobuf_Empty, Empty__Output as _google_protobuf_Empty__Output } from './google/protobuf/Empty.ts';
import type { FieldMask as _google_protobuf_FieldMask, FieldMask__Output as _google_protobuf_FieldMask__Output } from './google/protobuf/FieldMask.ts';

type SubtypeConstructor<Constructor extends new (...args: any) => any, Subtype> = {
  new(...args: ConstructorParameters<Constructor>): Subtype;
};

export interface ProtoGrpcType {
  chord: {
    FingerTableEntry: MessageTypeDefinition<_chord_FingerTableEntry, _chord_FingerTableEntry__Output>
    Node: SubtypeConstructor<typeof grpc.Client, _chord_NodeClient> & { service: _chord_NodeDefinition }
    NodeAddress: MessageTypeDefinition<_chord_NodeAddress, _chord_NodeAddress__Output>
    RemoteId: MessageTypeDefinition<_chord_RemoteId, _chord_RemoteId__Output>
    User: MessageTypeDefinition<_chord_User, _chord_User__Output>
    UserEdit: MessageTypeDefinition<_chord_UserEdit, _chord_UserEdit__Output>
    UserId: MessageTypeDefinition<_chord_UserId, _chord_UserId__Output>
    UserIdWithMetadata: MessageTypeDefinition<_chord_UserIdWithMetadata, _chord_UserIdWithMetadata__Output>
  }
  google: {
    protobuf: {
      Empty: MessageTypeDefinition<_google_protobuf_Empty, _google_protobuf_Empty__Output>
      FieldMask: MessageTypeDefinition<_google_protobuf_FieldMask, _google_protobuf_FieldMask__Output>
    }
  }
}

