# RFC|BB|L102: TTL WANT MESSAGES
### Implementation details.
<!-- Add references to code once performed the unit tests -->
* An additional TTL field has been added to Bitswap WANT entries in Bitswap messages to
enable the forwarding of exchange requests to peers TTL+1 hops away.
* Bitswap is set with a defualt TTL of 1, so corresponding messages will only be forwarded
to nodes two hops away.
* Sessions now include a TTL parameter to determine how far their WANT messages can go. Sessions started within the peer (because the peer wants a block) are considered `direct`. 
* Whenever a WANT message with enough TTLs are received from another peer, the requested CIDs are added to a `relaySession`. Nodes keep a single `relaySession` for all the CIDs that are requesting on behalf of other peers. The `relaySession` includes a degree to limit the amount of WANT messages broadcasted for other peers. Apart from this, the `relaySession` follows the same logic as a standard session.

* All the logic around relay sessions is done in `engine.go`, `session.go`, `peerwantmanager.go`:
    - Whenever a peer receives a WANT message from which it doesn't have the block and its TTL is not zero, it sends a DONT_HAVE right away, and it tells the relay session to start a discovery for those WANT messages with TTL-1.
    - Whenever a new block or HAVE messages are received in an intermediate node for an active relay session, these messages are forwarded to the source (the initial requester). This action updates the DONT_HAVE status of the intermediate node so it is again included in the session. 
        - _We need to be careful, in the current implementation blocks from relay sessions are stored in the datastore for convenience, but they should be removed once all the interested relay sessions for the block are closed and they have been successfully forwarded to avoid peers storing content they didn't explicitly requested._
    - When receiving a HAVE the relay session will automatically send the WANT-BLOCK to the corresponding peers, we have identified the interest from every peer (including direct ones) so when a peer receives a block for an relay file it will automatically forward it to the source (there is no need to forward interest for WANT-BLOCKS because this is automatically managed withing the relay sessions). relay sessions work in the same as direct sessions in this first implementation.


# Notes
* There is deduplication in the transmission of want messages for sessions. We shouldn't have two want messages for the same CID. This is the broker deduplication we mentioned. Note that the PeerManager ensures that we don't sent duplicate want-haves / want-blocks to a peer, and that want-blocks take precedence over want-haves.

# Next steps
* Consider the implementation of a registry interface so that apart from the relay registry
we can use other registries such as the peer-block registry to perform decisions over relay
sessions and the degree.