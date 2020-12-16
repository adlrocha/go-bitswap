package relaysession

import (
	"context"
	"sync"

	cid "github.com/ipfs/go-cid"
	exchange "github.com/ipfs/go-ipfs-exchange-interface"
	logging "github.com/ipfs/go-log"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

var log = logging.Logger("relaysession")

// TODO: Consider using a map/Set here for convenience.
type relayInfo struct {
	p   peer.ID
	ttl int32
}

// RelayRegistry keeps info about status of relaySession.
type RelayRegistry struct {
	r      map[cid.Cid]map[peer.ID][]*relayInfo // CID - source -  interestedPeers
	Degree int32
	lk     sync.RWMutex
}

// RelaySession to search content on behalf of other peers.
type RelaySession struct {
	Session  exchange.Fetcher
	Registry *RelayRegistry
	// We could add here the PeerBlockRegistry to start making decisions
	// also with the information tracked from want messages.
	// PeerBlockRegistry
}

// NewRelaySession constructor.
func NewRelaySession(degree int32) *RelaySession {
	return &RelaySession{
		Session: nil,
		Registry: &RelayRegistry{
			r:      make(map[cid.Cid]map[peer.ID][]*relayInfo, 0),
			Degree: degree,
		},
	}
}

func removeFromRelayInfo(ri []*relayInfo, p peer.ID) []*relayInfo {
	res := make([]*relayInfo, 0)
	for _, i := range ri {
		if i.p != p {
			res = append(res, i)
		}
	}
	return res
}

// RemoveInterest removes interest for a CID from a peer from the registry.
// This is done when the block for a CID is received. It is forwarded to
// interested peers and we must forget about it.
func (rs *RelaySession) RemoveInterest(c cid.Cid, p peer.ID) {
	rs.Registry.lk.Lock()
	defer rs.Registry.lk.Unlock()

	// For each source looking for the cid.
	for s, v := range rs.Registry.r[c] {
		// Remove peer for source
		rs.Registry.r[c][s] = removeFromRelayInfo(v, p)
		// If the relayInfo list is empty remove the source.
		if len(rs.Registry.r[c][s]) == 0 {
			delete(rs.Registry.r[c], s)
		}
	}
	// If no sources for the CID remove.
	if len(rs.Registry.r[c]) == 0 {
		delete(rs.Registry.r, c)
	}
}

// BlockSent removes interest for a CID from a peer from the registry.
// This is done when the block for a CID is received. It is forwarded to
// interested peers and we just forget about it.
func (rs *RelaySession) BlockSent(c cid.Cid) {
	rs.Registry.lk.Lock()
	defer rs.Registry.lk.Unlock()

	delete(rs.Registry.r, c)
}

// UpdateSession updates the relayRegistry with information about what is being
// asked through the relay session
func (rs *RelaySession) UpdateSession(ctx context.Context, self peer.ID, kt *keyTracker) {
	log.Debugf("Entering update session")

	rs.Registry.lk.Lock()
	defer rs.Registry.lk.Unlock()
	sessionBlks := make([]cid.Cid, 0)

	for _, t := range kt.T {
		// If CID not active and I am not the source.
		log.Debugf("self %v: Updating session for key %v, with source %v", self, t.Key, t.Source)
		if rs.Registry.r[t.Key] == nil && self != t.Source {
			// if rs.Registry.r[t.Key] == nil {
			// We need to start a new search because the CID is not active.
			sessionBlks = append(sessionBlks, t.Key)
			// Add to the registry
			rs.Registry.r[t.Key] = make(map[peer.ID][]*relayInfo, 0)
		}

		// If CID active we are already looking for it in a relaySession.
		// Do not include self as interested, it means that it has been forwarded.
		if t.Source != self {
			rs.Registry.r[t.Key][t.Source] =
				append(rs.Registry.r[t.Key][t.Source], &relayInfo{kt.Peer, t.TTL})
		}
	}
	log.Debugf("Started new block search for keys: %v", sessionBlks)
	rs.Session.GetBlocks(ctx, sessionBlks)
}

func (rr *RelayRegistry) hasSource(c cid.Cid, p peer.ID) int32 {
	for _, i := range rr.r[c][p] {
		// if source is interested
		if p == i.p {
			return i.ttl
		}
	}
	return -1
}

// InterestedPeers returns peers looking for a cid. If the source is between
// the interested peers, I forward block presence and block info only to him.
// If not, we send to all interested because we don't know the right path to follow.
// *NOTE*: Instead of selecting all interested peers we could use a degree and prioritize
// the ones that got to us with a higher TTL (it potentially means that they
// are nearer the source). This minimizes the amplification effect.
func (rs *RelaySession) InterestedPeers(c cid.Cid) map[peer.ID]int32 {
	rs.Registry.lk.RLock()
	defer rs.Registry.lk.RUnlock()
	// Create brand new map to avoid data races.
	res := make(map[peer.ID]int32)

	// For each source looking for the cid.
	for s, v := range rs.Registry.r[c] {
		// If for that CID the source is interested
		if t := rs.Registry.hasSource(c, s); t > 0 {
			// Add just that peer to interested peers.
			res[s] = t
		} else {
			// Check peers that are interested.
			for _, i := range v {
				// If peer not seen yet
				if _, ok := res[i.p]; !ok {
					res[i.p] = i.ttl
				} else { // If seen assign the lower ttl.
					if res[i.p] > i.ttl {
						res[i.p] = i.ttl
					}
				}
			}
		}
	}

	return res
}

// GetTTL returns the ttl to perform a WANT-HAVE broadcast.
func (rr *RelayRegistry) GetTTL(c cid.Cid) int32 {
	rr.lk.RLock()
	defer rr.lk.RUnlock()
	// TODO: We return the TTL for any of the peers requesting it. We could
	// perform other logic to select the right TTL to send here but as how are sessions
	// currently implemented the broadcast considers the TTL for all the CIDs being broadcasted.
	for _, a := range rr.r[c] {
		for _, i := range a {
			return i.ttl
		}
	}

	return 0
}

// GetTTL returns the ttl to perform a WANT-HAVE broadcast.
func (rr *RelayRegistry) GetSource(c cid.Cid) peer.ID {
	rr.lk.RLock()
	defer rr.lk.RUnlock()
	// TODO: We currently support sending only one source per entry. The right
	// way to do it would be to use the latest source send for the CID. Here I want
	// to test how adding a source field could avoid the amplification of messages
	// we generate in vanilla TTL so I am not going to approach that for now, sorry!
	for p := range rr.r[c] {
		return p
	}

	return ""
}

// func (rs *RelaySession)
type itemTrack struct {
	Key    cid.Cid
	TTL    int32
	Source peer.ID
}
type keyTracker struct {
	Peer peer.ID
	T    []*itemTrack
}

func (kt *keyTracker) UpdateTracker(c cid.Cid, ttl int32, source peer.ID) {
	kt.T = append(kt.T, &itemTrack{c, ttl, source})
}

// NewKeyTracker data structure to track CIDs that need to be addded to relaySession.
func NewKeyTracker(p peer.ID) *keyTracker {
	return &keyTracker{
		T:    make([]*itemTrack, 0),
		Peer: p,
	}
}
