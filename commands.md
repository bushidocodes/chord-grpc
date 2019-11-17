# Manual without Docker

npm run devServer -- --host localhost --port 8440 --knownHost localhost --knownPort 8440
npm run devClient -- crawl --host localhost --port 8440 --webPort 1337

npm run devServer -- --host localhost --port 8441 --knownHost localhost --knownPort 8440
npm run devServer -- --host localhost --port 8444 --knownHost localhost --knownPort 8440
npm run devServer -- --host localhost --port 8446 --knownHost localhost --knownPort 8440
npm run devServer -- --host localhost --port 8448 --knownHost localhost --knownPort 8440
npm run devServer -- --host localhost --port 8450 --knownHost localhost --knownPort 8440

<!-- Alvaro: npm run devClient worked for me, but it does not ends so I can't run the following command -->

node client insert --host localhost --port 8440
node client insert --host localhost --port 8440 --displayName "Alvaro is cool" --reputation 99 --aboutMe "I'm so cool I need no description"
node client lookup --host localhost --port 8440
node client remove --host localhost --port 8440

# Manual with Docker

I can't get the containers to communicate with each other. No idea about what IP or host name should be used

docker build -t bushidocodes/chord -f ./NodeDockerfile .
docker build -t bushidocodes/chordweb -f ./ClientDockerfile .

docker run -p 8440:1337 -it --init bushidocodes/chord --knownHost host.docker.internal --knownPort 8440
docker run -p 8441:1337 -it --init bushidocodes/chord --knownHost host.docker.internal --knownPort 8440
docker run -p 1337:1337 -it --init bushidocodes/chordweb crawl --host host.docker.internal --port 8440 --webPort 1337
