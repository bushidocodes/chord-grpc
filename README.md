# Chord (Node.js and gRPC)

[View Project Walkthrough and Demo on YouTube](https://www.youtube.com/watch?v=x2AixjsanyE)

This project is an implementation of a p2p distributed hash table using the Chord algorithm by Ion Stoica, Robert Morris, David Karger, Kaashoek, and Hari Balakrishnan. It uses Node.js to implement the nodes, and gRPC as the method of inter-node communiation.

The client script:

- runs a crawler that walks the Chord successor chain to build an in-memory representaiton of the state of the overlay network
- serves a simple web UI that visualizes the overlay network

In the future, the project will implement a "Stack Exchange Computer Science User Service" on top of this DHT, complete with a simple web app to demonstrate transparency and real-work use of a DHT. It will also enhance the admin pain to add controls to dynamically add and remove nodes from the Chord.

## To Run

We assume that you have Node.js, Docker, and Docker-Compose installed. You can confirm this with `node -v`, `docker -v`, and `docker-compose -v`.

This project uses **pnpm** (declared via `"packageManager"` in `package.json`, with a `pnpm-lock.yaml` lockfile; CI and the Docker images use it too). Install it with Corepack, which ships with Node — `corepack enable` — or directly with `npm install -g pnpm`. Using `npm install` here bypasses the lockfile and can resolve different dependency versions.

```
git clone git@github.com:bushidocodes/chord-grpc.git
cd chord-grpc
pnpm install
pnpm run gen-certs   # generate developer-local TLS certs (optional, see below)
docker-compose up --scale node_secondary=5 -d
```

Then open localhost:1337 in a browser

### TLS certificates

Inter-node gRPC traffic is encrypted with TLS when certificates are present in
`certs/`. These are **developer-local and are not committed to the repo** — run
`pnpm run gen-certs` (a thin wrapper around `scripts/gen-certs.sh`, which needs
OpenSSL 3.x) to generate a fresh CA and server certificate. If `certs/` is empty
the cluster still runs, but over **insecure** transport — fine for local
experimentation, not for anything exposed. Regenerate certs on every machine
that builds or runs the cluster.

Then run the following command in a separate tab to seed the sample StackOverflow data:

```
pnpm run client -- bulkInsert --path ./data/tinyUsers.json
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

## Configuration

Operational tunables are centralized in `app/config.ts` and read from
environment variables, with the defaults shown below (see also `LOG_LEVEL`
for logging and `GRPC_CERTS_DIR` for TLS):

| Variable                              | Default      | Purpose                                       |
| ------------------------------------- | ------------ | --------------------------------------------- |
| `CHORD_HASH_BIT_LENGTH`               | `32`         | Hash space size in bits (max 32)              |
| `CHORD_IS_FIBONACCI`                  | `false`      | Use Fibonacci finger table spacing            |
| `CHORD_FIBONACCI_ALPHA`               | `0.7`        | Fibonacci finger table pruning factor (0–1)   |
| `CHORD_STABILIZE_INTERVAL_MS`         | `1000`       | Cadence of the stabilize maintenance loop     |
| `CHORD_FIX_FINGERS_INTERVAL_MS`       | `3000`       | Cadence of the fixFingers maintenance loop    |
| `CHORD_CHECK_PREDECESSOR_INTERVAL_MS` | `1000`       | Cadence of the checkPredecessor loop          |
| `CHORD_RPC_DEADLINE_MS`               | `3000`       | Deadline for outbound unary RPCs              |
| `CHORD_STREAM_DEADLINE_MS`            | `30000`      | Deadline for outbound streaming RPCs          |
| `CHORD_SHUTDOWN_TIMEOUT_MS`           | `5000`       | Bound on graceful shutdown                    |
| `CHORD_DRAIN_TIMEOUT_MS`              | `2000`       | Bound on draining in-flight RPCs at shutdown  |
| `CHORD_CRAWLER_INTERVAL_MS`           | `3000`       | Web crawler step interval (`--interval` wins) |
| `CHORD_TLS_TARGET_NAME`               | `chord-node` | TLS target name override; must match cert SAN |

Values are validated at startup; a bad value fails fast rather than
misbehaving at runtime.

## License

The Stack Exchange Network data used in this licensed was released under the [cc-by-sa 4.0 license](https://creativecommons.org/licenses/by-sa/4.0/). It was downloaded from [archive.org](https://archive.org/details/stackexchange) as XML data, and subsequently converted to JSON. The derived Users.json file is thus also released under the [cc-by-sa 4.0 license](https://creativecommons.org/licenses/by-sa/4.0/) with identical conditions.

The remainder of the application logic is licensed under under the terms of the MIT license.
