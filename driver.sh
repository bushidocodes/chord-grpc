#!/bin/sh

npm run server -- --ip localhost --port 8440 --id 0 --targetIp localhost --targetPort 8440 --targetId 0

npm run client -- crawl --ip localhost --port 8440 --webPort 1337

npm run server -- --ip localhost --port 8441 --id 6 --targetIp localhost --targetPort 8440 --targetId 0
npm run server -- --ip localhost --port 8442 --id 1 --targetIp localhost --targetPort 8440 --targetId 0
npm run server -- --ip localhost --port 8443 --id 2 --targetIp localhost --targetPort 8440 --targetId 0
npm run server -- --ip localhost --port 8445 --id 3 --targetIp localhost --targetPort 8440 --targetId 0
npm run server -- --ip localhost --port 8444 --id 7 --targetIp localhost --targetPort 8440 --targetId 0