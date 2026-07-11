// Original file: protos/chord.proto

import type { NodeAddress as _chord_NodeAddress, NodeAddress__Output as _chord_NodeAddress__Output } from '../chord/NodeAddress.ts';

export interface RemoteId {
  'id'?: (number);
  'node'?: (_chord_NodeAddress | null);
}

export interface RemoteId__Output {
  'id': (number);
  'node': (_chord_NodeAddress__Output | null);
}
