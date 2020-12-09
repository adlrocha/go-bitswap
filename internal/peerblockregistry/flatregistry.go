package peerblockregistry

import (
	"sync"

	"github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-peer"
)

type FlatRegistry struct {
	CidList map[cid.Cid][]peer.ID
	pbrLk   sync.RWMutex
}

func NewFlatRegistry() PeerBlockRegistry {
	return &FlatRegistry{
		CidList: make(map[cid.Cid][]peer.ID),
	}
}

// GetCandidates to send the WANT-BLOCK to, as we've received
// a request for the CID from them
func (fr *FlatRegistry) GetCandidates(cid cid.Cid) []peer.ID {
	fr.pbrLk.RLock()
	defer fr.pbrLk.RUnlock()

	entries := fr.CidList[cid]
	if len(entries) > numberWantBlocks {
		entries = entries[:numberWantBlocks]
	}
	// candidates := []peer.ID{}
	// for _, e := range entries {
	// 	candidates = append(candidates, e.PeerID)
	// }

	return entries
}

// UpdateRegistry updates the registry with information of the wantlist
func (fr *FlatRegistry) UpdateRegistry(p peer.ID, cid cid.Cid, priority int32) error {
	var err error

	fr.pbrLk.Lock()
	defer fr.pbrLk.Unlock()

	fr.CidList[cid], err = addEntry(fr.CidList[cid], p, maxEntires)
	if err != nil {
		return err
	}
	return nil
}

// Clear cleans the registry.
func (fr *FlatRegistry) Clear() {
	fr.pbrLk.Lock()
	defer fr.pbrLk.Unlock()
	*fr = FlatRegistry{
		CidList: make(map[cid.Cid][]peer.ID),
	}
}

// GarbageCollect cleans outdated entries.
func (fr *FlatRegistry) GarbageCollect() {
	// TODO:; Periodically cleans the registry
	// to remove old and outdated entries
}
