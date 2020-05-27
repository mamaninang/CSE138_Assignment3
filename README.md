# CSE138_Assignment3

A REST API that consists of two main endpoints namely /key-value-store, which receives requests directly from the client, and /key-value-store-view, which only receives requests sent from other replicas.

/key-value-store receives GET, PUT or DELETE requests from the client in order to access data, add an entry or modify an entry in the key-value store.
/key-value-store-view mainly receives requests from the /key-value-store endpoint and returns the most current view of the active replicas

## Causal Consistency

For this project, my team and I decided to use vector clocks as our form of causal metadata to ensure that each replica delivers requests in a causally consistent manner. Each PUT or DELETE request contains a causal metadata field with the vector clock as its value. If this field is empty, it means that the request is not causally dependent on another request and may be processed right away. If the causal metadata included has been previously generated, the request is processed immediately as well. Otherwise, the request gets placed in a queue which is later to be processed when the causal metadata needed is finally generated.

## Replication and Fault Tolerance

In order to provide replication, each replica that receives a write operation (PUT or DELETE) from a client broadcasts that write operation to the other running replicas. This subsequently also creates fault tolerance as each replica will have the same key-value store. If a replica goes down, the client is still able to receive the most updated values from the other replicas. Once that replica is able to function and connect to the subnet again, it is able to acquire the latest version of the key-value store by querying the other replicas for theirs.

## Replica Detection Mechanism

In the /key-value-store-view, a broadcast method is called whenever a GET request is received. This broadcast method is responsible for pinging all the possible replicas to create an updated view of all active replicas. If a response is received, the replica either remains or gets added to the current view. If no response is received within a given period of time, the replica is deleted from the view.
