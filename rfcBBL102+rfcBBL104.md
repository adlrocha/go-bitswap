# RFC|BB|L102 + RFC|BB|L104: TTL BITSWAP WITH WANT INSPECTION
### Implementation details.
* WANT messages include a TTL to enable the use of relay sessions so nodes can search for blocks on
behalf of other nodes (RFC|BB|L102).
* Nodes also inspect WANT messages from other peers to populate their peer-block registry.
The peer-block registry is used to directly send WANT-BLOCKs to nodes that have recently
show interest in a CID as he potentially has it already, and to prevent from having
to broadcast every connected peer (RFC|BB|L104)
* In this merged implementation, the relay session selects the nodes to which it will forward
the relayed WANT messages using the peer-block registry and the relay registry. The relay registry
is used to prevent from sending a request for a CID to peers we know don't have it or that have 
an active relay session. From the peer-block registry we choose a few candidates that have asked
for the CID before and forward the relayed request to them. The rest of the peers until we
reach the degree of the relay session are selected randomly from the session.

### Concerns
* Now there are more WANT messages traversing the network. This is a double edge sword, becuase
the fact that we receive WANT messages from requesters a few hops apart, allows us to populate
the peer-block registry with more CIDs. However, some of these CIDs may come from relayed
requests for which the node may not have ended up getting the block, thereby missing the peer
actually storing the block. This effect may be minimized by increasing the number of candidates
used from the peer-block registry for the optimistic WANT-BLOCK and the relay broadcast.

### Potential improvements
* In `broadcastRelayWants` from the `peerwantmanager.go` we can send a WANT-BLOCK instead of a 
WANT-HAVE to the candidates peers found in the PBR. We have to also bear in mind that we are
choosing the same number of candidates selected by direct session, and we may want to increase
the number of peers from the PBR to use in the broadcast.

* We currently choose randomly the peers for the relayBroadcast, and we use the same subset
for every CID. We could choose a random subset for each CID, or use a probabilistic
approach in which we use information from the session (latency, presence information) to
make smarter requests.