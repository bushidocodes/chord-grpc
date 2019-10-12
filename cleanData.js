const fs = require('fs');
const parseString = require('xml2js').parseString;

const COUNT_OF_USERS_IN_TINY_USERS = 100;

// Quick and dirty script to convert the StackOverflow data from XML to JSON
fs.readFile(`${__dirname}/data/users.xml`, (err, data) => {
    parseString(data, (err, rawData) => {
        
        const result = {};
        const tinyResult = {};
        rawData.users.row
        .map(person => person['$'])
        .map(person => ({
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
            accountId: parseInt(person.AccountId, 10),
        }))
        .forEach((person, idx) => {
            if (idx < COUNT_OF_USERS_IN_TINY_USERS){
                tinyResult[person.id] = person;
            }
            result[person.id] = person;
        })
        fs.writeFileSync(`${__dirname}/data/users.json`, JSON.stringify(result));
        fs.writeFileSync(`${__dirname}/data/tinyUsers.json`, JSON.stringify(tinyResult));
    });
});
