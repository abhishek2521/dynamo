In this project I have implemented Amazon Dynamo style Replicated key value storage.There are three main pieces that have been  implemented : 1) Partitioning, 2) Replication, and 3) Failure handling.
Following are some of its important features

1 .Membership
Just as the original Dynamo, every node can know every other node. This means that each node knows all other nodes in the system and also knows exactly which partition belongs to which node; any node can forward a request to the correct node without using a ring-based routing.

2. Request routing
Unlike Chord, each Dynamo node knows all other nodes in the system and also knows exactly which partition belongs to which node.
Under no failures, all requests are directly forwarded to the coordinator, and the coordinator is in charge of serving read/write operations.


3. Quorum replication
I have implemented a quorum-based replication used by Dynamo.The replication degree N is 3. This means that given a key, the key’s coordinator as well as the 2 successor nodes in the Dynamo ring stores the key.Both the reader quorum size R and the writer quorum size W is 2.The coordinator for a get/put request always contacts other two nodes and get the votes.
For write operations, all objects have been versioned in order to distinguish stale copies from the most recent copy.
For read operations, if the readers in the reader quorum have different versions of the same object, the coordinator should pick the most recent version and return it.

4. Failure handling
When a coordinator for a request fails and it does not respond to the request, its successor is contacted next for the request.
