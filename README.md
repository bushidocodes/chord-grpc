# Chord (Node.js and gRPC)

[View Project Walkthrough and Demo on YouTube](https://www.youtube.com/watch?v=x2AixjsanyE)

This project is an implementation of a p2p distributed hash table using the Chord algorithm by Ion Stoica, Robert Morris, David Karger, Kaashoek, and Hari Balakrishnan. It uses Node.js to implement the nodes, and gRPC as the method of inter-node communiation.

The client script:

- runs a crawler that walks the Chord successor chain to build an in-memory representaiton of the state of the overlay network
- serves a simple web UI that visualizes the overlay network

In the future, the project will implement a "Stack Exchange Computer Science User Service" on top of this DHT, complete with a simple web app to demonstrate transparency and real-work use of a DHT. It will also enhance the admin pain to add controls to dynamically add and remove nodes from the Chord.

## To Run

We assume that you have Node.js, Docker, and Docker-Compose installed. You can confirm this with `node -v`, `docker -v`, and `docker-compose -v`

```
git clone git@github.com:bushidocodes/chord-grpc.git
cd chord-grpc
npm install
npm run gen-certs   # generate developer-local TLS certs (optional, see below)
docker-compose up --scale node_secondary=5 -d
```

Then open localhost:1337 in a browser

### TLS certificates

Inter-node gRPC traffic is encrypted with TLS when certificates are present in
`certs/`. These are **developer-local and are not committed to the repo** — run
`npm run gen-certs` (a thin wrapper around `scripts/gen-certs.sh`, which needs
OpenSSL 3.x) to generate a fresh CA and server certificate. If `certs/` is empty
the cluster still runs, but over **insecure** transport — fine for local
experimentation, not for anything exposed. Regenerate certs on every machine
that builds or runs the cluster.

Then run the following command in a separate tab to seed the sample StackOverflow data:

```
npm run client -- bulkInsert --path ./data/tinyUsers.json
```

You can then use the Data API as documented in `commands.md`

You can also scale out the Chord cluster and see the data migrate:

```
docker-compose up --scale node_secondary=8 -d
```

When you are complete, be sure to stop the chord:

```
docker-compose down
```

## License

The Stack Exchange Network data used in this licensed was released under the [cc-by-sa 4.0 license](https://creativecommons.org/licenses/by-sa/4.0/). It was downloaded from [archive.org](https://archive.org/details/stackexchange) as XML data, and subsequently converted to JSON. The derived Users.json file is thus also released under the [cc-by-sa 4.0 license](https://creativecommons.org/licenses/by-sa/4.0/) with identical conditions.

The remainder of the application logic is licensed under under the terms of the MIT license.
