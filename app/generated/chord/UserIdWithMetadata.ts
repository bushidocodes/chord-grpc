// Original file: protos/chord.proto


export interface _chord_UserIdWithMetadata_UserMetadata {
  'primaryHash'?: (number);
  'secondaryHash'?: (number);
  'isPrimaryHash'?: (boolean);
}

export interface _chord_UserIdWithMetadata_UserMetadata__Output {
  'primaryHash': (number);
  'secondaryHash': (number);
  'isPrimaryHash': (boolean);
}

export interface UserIdWithMetadata {
  'id'?: (number);
  'metadata'?: (_chord_UserIdWithMetadata_UserMetadata | null);
}

export interface UserIdWithMetadata__Output {
  'id': (number);
  'metadata': (_chord_UserIdWithMetadata_UserMetadata__Output | null);
}
