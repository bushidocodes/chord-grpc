
To Do 
1. Three Different Nodes just like in Paper
2. Create 3 nodes with IDs 0, 1 and 3 following the paper information
3. Create their finger tables with the IDs of the nodes they are connecting too and the address information
4. Once we have this, we can work on creating the look up algorithm in this pre-setup chord circle 

localhost:1337 (1337 - 1337 = 0)
localhost:1338 (1338 - 1337 = 1)
localhost:1340 (1340 - 1337 = 3)
Dummy Hash Function:  ([Port or User ID] - 1337)%8

Finger Table: 
    - Start: node.id + 2^ith entry  (0, 1, 2, 4, ) woudl be an example
    - succ: successor, owner of the data, for each of the start values 
    - interval: they keys that share the same successor

node 0: (k, 0], where k is the greatest node ID
node 1: (0, 1]
node 2: (1, 2]
node 4: (2, 4]
...
node k: (second greatest node ID, k]

High Level Functionality Requirements:
Add a Node (and rebalance files)
Remove a Node (and rebalance files)
Add Data - hash Data.name to key, ID = succ(key), store Data at ID 
Remove Data - hash Data.name to key, ID = succ(key), remove Data from ID
Find/Return Data - hash Data.name to key, ID = succ(key), retrieve Data from ID

// Returns the IP:Port 

{
    1: 1,
    2, 3,
    4, 0
}

1. Add find_successor(bucket_id)=>port to chord.proto (Sean)
2. Implement predecessor, successor, and implement (Hard-coded) Finger Table in server.js (Tyler)
3. Implement find_successor(bucket_id)=>port in node0.js (Alvaro)
4. Add find_predecessor(bucket_id)=>port to chord.proto (Sean)
5. Fix Sean's Problems(Alvaro)
6. Fix findSuccessor(Tyler)
7. Add findPredecessor as a gRPC proto (Naim)
8. Implement closestPrecedingFinger(Naim)


getPredecessor
setPredecessor
getSuccessor