#!/bin/sh
node node --ip localhost --port 8448 --id 0 --targetIp localhost --targetPort 8448 --targetId 0
node node --ip localhost --port 8449 --id 1 --targetIp localhost --targetPort 8448 --targetId 0
node node --ip localhost --port 8450 --id 2 --targetIp localhost --targetPort 8448 --targetId 0
node node --ip localhost --port 8451 --id 3 --targetIp localhost --targetPort 8448 --targetId 0