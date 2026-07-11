// Original file: protos/chord.proto


export interface _chord_User_UserMetadata {
  'primaryHash'?: (number);
  'secondaryHash'?: (number);
  'isPrimaryHash'?: (boolean);
}

export interface _chord_User_UserMetadata__Output {
  'primaryHash': (number);
  'secondaryHash': (number);
  'isPrimaryHash': (boolean);
}

export interface User {
  'id'?: (number);
  'reputation'?: (number);
  'creationDate'?: (string);
  'displayName'?: (string);
  'lastAccessDate'?: (string);
  'websiteUrl'?: (string);
  'location'?: (string);
  'aboutMe'?: (string);
  'views'?: (number);
  'upVotes'?: (number);
  'downVotes'?: (number);
  'profileImageUrl'?: (string);
  'accountId'?: (number);
  'metadata'?: (_chord_User_UserMetadata | null);
}

export interface User__Output {
  'id': (number);
  'reputation': (number);
  'creationDate': (string);
  'displayName': (string);
  'lastAccessDate': (string);
  'websiteUrl': (string);
  'location': (string);
  'aboutMe': (string);
  'views': (number);
  'upVotes': (number);
  'downVotes': (number);
  'profileImageUrl': (string);
  'accountId': (number);
  'metadata': (_chord_User_UserMetadata__Output | null);
}
