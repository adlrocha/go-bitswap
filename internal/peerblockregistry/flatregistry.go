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

// GarbageCollect cleans outdated entries.
func (fr *FlatRegistry) GarbageCollect() {
	// TODO:; Do we need a garbage collector if we limit the size of
	// the registry? One of the problems may be that no want messages
	// have been received for a long time for CID and you end up sending
	// WANT-BLOCK to a extremely updated blockEntry.
}
