// Original file: protos/chord.proto

import type { User as _chord_User, User__Output as _chord_User__Output } from '../chord/User.ts';
import type { FieldMask as _google_protobuf_FieldMask, FieldMask__Output as _google_protobuf_FieldMask__Output } from '../google/protobuf/FieldMask.ts';

export interface UserEdit {
  'user'?: (_chord_User | null);
  'edit'?: (boolean);
  'update_mask'?: (_google_protobuf_FieldMask | null);
}

export interface UserEdit__Output {
  'user': (_chord_User__Output | null);
  'edit': (boolean);
  'update_mask': (_google_protobuf_FieldMask__Output | null);
}
