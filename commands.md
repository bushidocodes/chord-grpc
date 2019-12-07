# Manual without Docker

npm run devServer -- --port 8440
npm run devWeb -- --port 8440 --webPort 1337

npm run devServer -- --port 8441 --knownPort 8440
npm run devServer -- --port 8444 --knownPort 8440
npm run devServer -- --port 8446 --knownPort 8440
npm run devServer -- --port 8448 --knownPort 8440
npm run devServer -- --port 8450 --knownPort 8440

npm run client -- insert --port 8440 --id 2
npm run client -- lookup --port 8440 --id 2
npm run client -- insert --port 8440 --id 5 --displayName "Alvaro is cool" --reputation 99 --aboutMe "I'm so cool I need no description"
npm run client -- lookup --port 8440 --id 5
npm run client -- edit --port 8440 --id 5 --displayName "Alvaro is cool" --reputation 1 --aboutMe "I'm not cool"
npm run client -- lookup --port 8440 --id 5
npm run client -- remove --port 8440 --id 2
npm run client -- remove --port 8440 --id 5

# Manual with Docker

I can't get the containers to communicate with each other. No idea about what IP or host name should be used

docker build -t bushidocodes/chord -f ./NodeDockerfile .
docker build -t bushidocodes/chordweb -f ./ClientDockerfile .

docker run -p 8440:1337 -it --init bushidocodes/chord --knownHost host.docker.internal --knownPort 8440
docker run -p 8441:1337 -it --init bushidocodes/chord --knownHost host.docker.internal --knownPort 8440
docker run -p 1337:1337 -it --init bushidocodes/chordweb crawl --host host.docker.internal --port 8440 --webPort 1337

# Automatic with Docker

docker-compose up --scale node_secondary=5 --build -d

When running, you can auto-scale the nodes up as down using the following:
docker-compose up --scale node_secondary=10 -d

And if on Linux:
google-chrome -incognito --password-store=basic --new-window http://localhost:1337 &

When complete, cleanup with
docker-compose down
s
