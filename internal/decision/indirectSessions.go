package decision

import (
	"sync"

	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

type keyTracker struct {
	t map[int32]map[int32][]cid.Cid //TTL-Priority-keys
}

func (kt *keyTracker) updateTracker(ttl int32, priority int32, c cid.Cid) {
	if kt.t[ttl][priority] == nil {
		kt.t[ttl] = make(map[int32][]cid.Cid)
	}
	kt.t[ttl][priority] = append(kt.t[ttl][priority], c)
}

func newKeyTracker() *keyTracker {
	return &keyTracker{
		t: make(map[int32]map[int32][]cid.Cid),
	}
}

type indirectInfo struct {
	peer peer.ID
	cids map[cid.Cid]int32 //CID-priority
	ttl  int32
}

type sessionRegistry struct {
	ledger map[uint64]*indirectInfo
	lk     sync.RWMutex
}

// NewSessionRegistry return a sessionID tracker
func newSessionRegistry() *sessionRegistry {
	return &sessionRegistry{
		ledger: make(map[uint64]*indirectInfo, 0),
	}
}

// Returns the peers of indirect sessions interested in a peer
// with their corresponding priority
func (s *sessionRegistry) interestedPeers(c cid.Cid) []peer.ID {
	s.lk.RLock()
	defer s.lk.RUnlock()
	res := make([]peer.ID, 0)
	for _, sess := range s.ledger {
		// If has cid
		if _, ok := sess.cids[c]; ok {
			res = append(res, sess.peer)
		}
	}
	return res
}

func (s *sessionRegistry) len() int {
	return len(s.ledger)
}

// Returns the peers of indirect sessions interested in a peer
// with their corresponding priority
func (s *sessionRegistry) interestedSessions(c cid.Cid) map[uint64]*indirectInfo {
	s.lk.RLock()
	defer s.lk.RUnlock()
	res := make(map[uint64]*indirectInfo, 0)

	// If the session includes the CID return it
	for id, info := range s.ledger {
		if _, ok := info.cids[c]; ok {
			res[id] = info
		}
	}
	return res
}

func (s *sessionRegistry) has(sessionID uint64) bool {
	_, found := s.ledger[sessionID]
	return found
}

func (s *sessionRegistry) notActive(peer peer.ID, keys []cid.Cid) []cid.Cid {
	res := make([]cid.Cid, 0)
	seen := false
	//For each key if it is already in a session for that peer don't include it.
	for _, k := range keys {
		for _, sess := range s.ledger {
			if sess.peer == peer {
				seen = true
				if _, ok := sess.cids[k]; !ok {
					seen = true
					res = append(res, k)
				}
			}
		}
	}
	if !seen {
		return keys
	}
	return res
}

func (s *sessionRegistry) removeKey(k cid.Cid) {
	s.lk.Lock()
	defer s.lk.Unlock()
	for sid, sess := range s.ledger {
		if _, ok := sess.cids[k]; ok {
			delete(sess.cids, k)
		}

		// Delete session if no keys pending in session
		if len(sess.cids) == 0 {
			delete(s.ledger, sid)
		}
	}
}

func (s *sessionRegistry) add(sessionID uint64, peer peer.ID,
	keys []cid.Cid, ttl int32, priority int32) {
	cs := make(map[cid.Cid]int32, 0)

	for _, k := range keys {
		cs[k] = priority
	}

	s.lk.Lock()
	defer s.lk.Unlock()
	s.ledger[sessionID] = &indirectInfo{
		peer,
		cs,
		ttl,
	}
}

func (s *sessionRegistry) remove(sessionID uint64) {
	s.lk.Lock()
	defer s.lk.Unlock()
	delete(s.ledger, sessionID)
}
