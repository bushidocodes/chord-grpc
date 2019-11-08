# Manual without Docker

npm run devServer -- --ip localhost --port 8440 --id 0 --targetIp localhost --targetPort 8440 --targetId 0
npm run devClient -- crawl --ip 0.0.0.0 --port 8440 --webPort 1337

npm run devServer -- --ip localhost --port 8441 --id 6 --targetIp localhost --targetPort 8440 --targetId 0
npm run devServer -- --ip localhost --port 8442 --id 1 --targetIp localhost --targetPort 8440 --targetId 0
npm run devServer -- --ip localhost --port 8443 --id 2 --targetIp localhost --targetPort 8440 --targetId 0
npm run devServer -- --ip localhost --port 8445 --id 3 --targetIp localhost --targetPort 8440 --targetId 0
npm run devServer -- --ip localhost --port 8444 --id 7 --targetIp localhost --targetPort 8440 --targetId 0

# Manual with Docker

I can't get the containers to communicate with each other. No idea about what IP should be used

docker build -t bushidocodes/chord -f ./NodeDockerfile .
docker build -t bushidocodes/chordweb -f ./ClientDockerfile .

docker run -p 8440:1337 -it --init bushidocodes/chord --id 0 --targetIp host.docker.internal --targetPort 8440 --targetId 0
docker run -p 8441:1337 -it --init bushidocodes/chord --id 6 --targetIp host.docker.internal --targetPort 8440 --targetId 0
docker run -p 1337:1337 -it --init bushidocodes/chordweb crawl --ip host.docker.internal --port 8440 --webPort 1337
