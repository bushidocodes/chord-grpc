# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Chord-gRPC is a peer-to-peer distributed hash table (DHT) implementing the Chord algorithm. Built with TypeScript 5 / Node.js (tested on 14–24) and `@grpc/grpc-js` for inter-node communication. Includes a web UI for network visualization and a CLI client for data operations. The application layer implements a "Stack Exchange User Service" storing Stack Overflow user profiles. Docker containers use Node 24.

## Commands

### Running with Docker (preferred)

```bash
npm install
npm run gen-certs                                  # generate local TLS certs (see TLS note below)
docker-compose up --scale node_secondary=5 -d    # Start cluster
docker-compose up --scale node_secondary=8 -d     # Scale out
docker-compose down                                # Stop cluster
```

**TLS:** The `certs/` directory is developer-local and gitignored (#185) — never
commit keys. Run `npm run gen-certs` (`scripts/gen-certs.sh`, needs OpenSSL 3.x)
to create a fresh CA + server cert. When `certs/ca.crt` is absent,
`loadTlsCredentials()` in `app/utils.ts` returns `null` and nodes/clients fall
back to insecure transport, so the cluster still runs without certs.

### Running without Docker

```bash
# Start primary node
npm run devServer -- --port 8440

# Start web UI (connects to primary)
npm run devWeb -- --port 8440 --webPort 1337

# Start additional nodes (join via knownPort)
npm run devServer -- --port 8441 --knownPort 8440

# Debug with inspector
npm run debugPrimary    # port 8440, inspect on 9229
npm run debugSecondary  # port 8441, inspect on 9230
```

### Client operations

```bash
npm run client -- insert --port 8440 --id 2
npm run client -- lookup --port 8440 --id 2
npm run client -- edit --port 8440 --id 5 --displayName "Name" --reputation 99
npm run client -- remove --port 8440 --id 5
npm run client -- bulkInsert --path ./data/tinyUsers.json
npm run client -- summary --port 8440   # node identity + health status
npm run client -- health --port 8440    # standard gRPC health check (exits non-zero if not SERVING)

# Ring topology inspection (useful for debugging ring formation / stale routing)
npm run client -- fingerTable --port 8440   # print all finger table entries
npm run client -- predecessor --port 8440   # print predecessor node
npm run client -- successor --port 8440     # print successor node
```

### Logging

Nodes use [pino](https://github.com/pinojs/pino) for structured JSON logging. Set `LOG_LEVEL` to control verbosity:

```bash
LOG_LEVEL=debug npm run devServer -- --port 8440   # verbose Chord protocol tracing
LOG_LEVEL=info  npm run devServer -- --port 8440   # default: operations only
LOG_LEVEL=warn  npm run devServer -- --port 8440   # warnings and errors only
```

For human-readable output during development, pipe through `pino-pretty`:

```bash
LOG_LEVEL=debug npm run devServer -- --port 8440 | npx pino-pretty
```

### No test suite exists

The `npm test` script is a placeholder that exits with an error.

## Architecture

### Three main components

1. **Chord Nodes** (`app/`) — Each node is a gRPC server running the Chord protocol. `ChordNode.ts` implements the core algorithm (finger tables, successor/predecessor management, stabilization). `UserService.ts` extends ChordNode with application-level user data storage using dual-hashing for replication.

2. **Web Crawler/UI** (`web/`) — Express server that implements a "Chord Crawler" algorithm, walking the successor chain via gRPC streaming to build an in-memory representation of the overlay network. Serves a D3.js visualization at `/data`.

3. **CLI Client** (`client/`) — Standalone gRPC client for CRUD operations on user data.

### Key design decisions

- **32-bit hash space**: SHA-1 truncated to `HASH_BIT_LENGTH=32` bits (configurable in `app/utils.ts`)
- **Dual-hashing replication**: Each user stored at two nodes (primary + secondary hash) for fault tolerance
- **Synchronous hashing**: SHA-1 of short strings runs on the main thread via Node's `crypto` (`sha1()` in `app/utils.ts`)
- **Fibonacci finger tables**: Optional optimization via `FIBONACCI_ALPHA` constant
- **Circular key space**: `isInModuloRange()` in `utils.ts` handles wraparound arithmetic

### Protocol definition

`protos/chord.proto` defines 18 RPC methods split between library-level (Chord protocol: findSuccessor, stabilize, notify, etc.) and application-level (User CRUD: fetch, insert, lookup, remove, migrate). Node liveness is served separately by the standard gRPC Health Checking Protocol in `protos/health.proto` (`grpc.health.v1.Health/Check` + `Watch`, implemented in `app/health.ts`), which interoperates with `grpc_health_probe`, Kubernetes gRPC probes, etc. Node identity (id/host/port) is available via `getNodeIdRemoteHelper`.

### Entry points

- `app/node.ts` — Node process entry (parses --host, --port, --knownHost, --knownPort, --id)
- `web/web.ts` — Web server entry
- `client/client.ts` — CLI client entry

## Code Style

- Prettier enforced via Husky pre-commit hook (`pretty-quick --staged`)
- 2-space indentation, LF line endings (`.editorconfig`)
- TypeScript with ESM modules, run natively via Node 24's built-in type stripping (no ts-node)
