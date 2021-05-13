import fs from "fs";

import { parseString } from "xml2js";

const COUNT_OF_USERS_IN_TINY_USERS = 100;

interface XMLGeneratedPerson {
  Id: string;
  Reputation: string;
  CreationDate: string;
  DisplayName: string;
  LastAccessDate: string;
  WebsiteUrl: string;
  Location: string;
  AboutMe: string;
  Views: string;
  UpVotes: string;
  DownVotes: string;
  ProfileImageUrl: string;
  AccountId: string;
}

// Quick and dirty script to convert the StackOverflow data from XML to JSON
fs.readFile(`${__dirname}/users.xml`, (_err, data) => {
  parseString(data, (_err, rawData) => {
    const result = {};
    const tinyResult = {};
    rawData.users.row
      .map((person: { [x: string]: any }) => person["$"])
      .map((person: XMLGeneratedPerson) => ({
        id: parseInt(person.Id, 10),
        reputation: parseInt(person.Reputation, 10),
        creationDate: person.CreationDate,
        displayName: person.DisplayName,
        lastAccessDate: person.LastAccessDate,
        websiteUrl: person.WebsiteUrl,
        location: person.Location,
        aboutMe: person.AboutMe,
        views: parseInt(person.Views, 10),
        upVotes: parseInt(person.UpVotes, 10),
        downVotes: parseInt(person.DownVotes, 10),
        profileImageUrl: person.ProfileImageUrl,
        accountId: parseInt(person.AccountId, 10)
      }))
      .forEach((person: { id: string | number }, idx: number) => {
        if (idx < COUNT_OF_USERS_IN_TINY_USERS) {
          tinyResult[person.id] = person;
        }
        result[person.id] = person;
      });
    fs.writeFileSync(`${__dirname}/users.json`, JSON.stringify(result));
    fs.writeFileSync(`${__dirname}/tinyUsers.json`, JSON.stringify(tinyResult));
  });
});
