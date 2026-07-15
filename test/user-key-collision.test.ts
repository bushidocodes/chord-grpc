import assert from "node:assert/strict";
import { test } from "node:test";
import { UserService } from "../app/UserService.ts";

// User IDs 35097 and 40686 share the same primary hash bucket.
//
//   sha1("35097") = c7b2880d adf7afce 4ca1c94a 10e2f19a 7ede89c1
//   sha1("40686") = c7b2880d 1ba1373d aaa6d9ba fd64f5c3 b42d5e96
//                  ^^^^^^^^ — first 4 bytes identical → same 32-bit primary hash
//
// Before the fix, insertUser checked only whether the bucket was occupied:
//   if (this.userMap[key] && !edit) return { code: 6 };
// User 35097 fills bucket 3350366221, so user 40686 is rejected as "already
// exists" even though it is a completely different user.
const COLLISION_HASH = 3350366221;
const USER_A = 35097;
const USER_B = 40686;

function makeUserEdit(id: number) {
  return {
    user: {
      id,
      reputation: 0,
      creationDate: "",
      displayName: `User ${id}`,
      lastAccessDate: "",
      websiteUrl: "",
      location: "",
      aboutMe: "",
      views: 0,
      upVotes: 0,
      downVotes: 0,
      profileImageUrl: "",
      accountId: 0,
      metadata: {
        primaryHash: COLLISION_HASH,
        secondaryHash: 0,
        isPrimaryHash: true,
      },
    },
    edit: false,
    update_mask: { paths: [] },
  };
}

test("inserting a colliding user does not reject it as a duplicate", () => {
  const service = new UserService({ id: 1, host: "localhost", port: 9999 });

  const errA = service.insertUser(makeUserEdit(USER_A));
  assert.equal(errA, null, "first insert should succeed");

  const errB = service.insertUser(makeUserEdit(USER_B));
  assert.equal(
    errB,
    null,
    `user ${USER_B} shares hash ${COLLISION_HASH} with user ${USER_A} but is a distinct user — insert must not return code 6`,
  );
});

test("both colliding users can be independently retrieved", () => {
  const service = new UserService({ id: 1, host: "localhost", port: 9999 });

  service.insertUser(makeUserEdit(USER_A));
  service.insertUser(makeUserEdit(USER_B));

  const { err: errA, user: userA } = service.lookupUser(COLLISION_HASH, USER_A);
  assert.equal(errA, null, `user ${USER_A} should be found`);
  assert.equal(userA?.id, USER_A);

  const { err: errB, user: userB } = service.lookupUser(COLLISION_HASH, USER_B);
  assert.equal(errB, null, `user ${USER_B} should be found`);
  assert.equal(userB?.id, USER_B);
});
