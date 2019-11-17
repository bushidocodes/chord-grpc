# Chord (Node.js and gRPC)

This project is an implementation of a p2p distributed hash table using the Chord algorithm by Ion Stoica, Robert Morris, David Karger, Kaashoek, and Hari Balakrishnan. It uses Node.js to implement the nodes, and gRPC as the method of inter-node communiation.

The client script:

- runs a crawler that walks the Chord successor chain to build an in-memory representaiton of the state of the overlay network
- serves a simple web UI that visualizes the overlay network

In the future, the project will implement a "Stack Exchange Computer Science User Service" on top of this DHT, complete with a simple web app to demonstrate transparency and real-work use of a DHT. It will also enhance the admin pain to add controls to dynamically add and remove nodes from the Chord.

## To Run

We assume that you have Node.js installed. You can confirm this with `node -v`

```
git clone git@github.com:bushidocodes/chord-grpc.git
cd chord-grpc
npm install
```

Then run start the first node:

```sh
npm run devServer -- --host localhost --port 8440 --knownHost localhost --knownPort 8440
```

Then start the client:

```sh
npm run devClient -- crawl --host localhost --port 8440 --webPort 1337
```

Then open localhost:1337 in a browser

Then run the following commands one at a time in separate tabs:

```sh
npm run devServer -- --host localhost --port 8441 --knownHost localhost --knownPort 8440
npm run devServer -- --host localhost --port 8444 --knownHost localhost --knownPort 8440
npm run devServer -- --host localhost --port 8446 --knownHost localhost --knownPort 8440
npm run devServer -- --host localhost --port 8448 --knownHost localhost --knownPort 8440
npm run devServer -- --host localhost --port 8450 --knownHost localhost --knownPort 8440
```

## License

The Stack Exchange Network data used in this licensed was released under the [cc-by-sa 4.0 license](https://creativecommons.org/licenses/by-sa/4.0/). It was downloaded from [archive.org](https://archive.org/details/stackexchange) as XML data, and subsequently converted to JSON. The derived Users.json file is thus also released under the [cc-by-sa 4.0 license](https://creativecommons.org/licenses/by-sa/4.0/) with identical conditions.

The remainder of the application logic is licensed under under the terms of the MIT license.
