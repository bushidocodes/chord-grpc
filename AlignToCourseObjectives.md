Architecture

- Peer-to-Peer DHT
- Internally structures to support reuse as a library

Processes

- Our system is mostly multiprocess
- Each process is sandboxed in a container
- Each process uses Node.js multi-threading to offload SHA-1 hashing

Communication

- Our system uses gRPC and Protobuff for communication within the cluster and with the web app and the CLI client
- gRPC uses both unary calls and streaming

Network

- Our system depends on an orchestration system for networking
- We have implemented this for Docker Compose and Azure

Naming

- Our nodes and data keys use a consistent configurable namespace
- This includes the local keys within the nodes

Synchronization

- Our nodes run maintenance functions on set intervals

Consistency / Replication

- Our data is inserted into two locations in our namespace.
- Insert / Update is synchronous
- Lookup attempts the primary hash and then falls back to the secondary hash

File System

- As implemented, we effectively have implemented a distributed flat storage object storage bucket similar to AWS S3

Fault Tolerance

- Our nodes are able to recover the Chord logical ring for planned and unplanned outages
- Our data storage is fault tolerant based on having two redundant copies stored

Creativity

- We are using the Fibonacci Sequence to make our finger tables more efficient
- "Chord Crawler" Algorithm
