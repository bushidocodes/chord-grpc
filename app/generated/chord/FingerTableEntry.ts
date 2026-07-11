// Original file: protos/chord.proto

import type { NodeAddress as _chord_NodeAddress, NodeAddress__Output as _chord_NodeAddress__Output } from '../chord/NodeAddress.ts';

export interface FingerTableEntry {
  'node'?: (_chord_NodeAddress | null);
  'index'?: (number);
}

export interface FingerTableEntry__Output {
  'node': (_chord_NodeAddress__Output | null);
  'index': (number);
}
