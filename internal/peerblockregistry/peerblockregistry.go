package peerblockregistry

import (
	"fmt"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-peer"
)

const (
	numberWantBlocks = 3   // Number of peers to send want blcoks to
	gcPeriod         = 300 // Garbage collection period for the peerblockregistry
	maxEntires       = 10  // Max number of entries per CID in Registry
)

type PeerBlockRegistry interface {
	GetCandidates(cid cid.Cid) []peer.ID
	UpdateRegistry(peer peer.ID, cid cid.Cid, priority int32) error
	GarbageCollect()
}

type FlatRegistry struct {
	CidList map[cid.Cid][]*PeerBlockEntry
	pbrLk   sync.RWMutex
}

// PeerBlockEntry information that want to be tracked when inspecting
// Wantlists.
type PeerBlockEntry struct {
	PeerID   peer.ID
	Seen     time.Time
	Priority int32
	// Here we can add additional information
	// to make decisions.
}

func NewFlatPeerBlock() PeerBlockRegistry {
	return &FlatRegistry{
		CidList: make(map[cid.Cid][]*PeerBlockEntry),
	}
}

// GetCandidates to send the WANT-BLOCK to, as we've received
// a request for the CID from them
func (fr *FlatRegistry) GetCandidates(cid cid.Cid) []peer.ID {
	entries := fr.CidList[cid]
	if len(entries) > numberWantBlocks {
		entries = entries[:numberWantBlocks]
	}
	candidates := []peer.ID{}
	for _, e := range entries {
		candidates = append(candidates, e.PeerID)
	}

	return candidates
}

// UpdateRegistry updates the registry with information of the wantlist
func (fr *FlatRegistry) UpdateRegistry(peer peer.ID, cid cid.Cid, priority int32) error {
	fr.pbrLk.Lock()
	defer fr.pbrLk.Unlock()
	var err error

	for i, entry := range fr.CidList[cid] {
		if entry.PeerID == peer {
			// Remove previous entry of the peer in registry
			fr.CidList[cid], err = removeItem(fr.CidList[cid], i)
			if err != nil {
				return err
			}
		}
	}
	// Add entry to the beginning
	fr.CidList[cid] = append([]*PeerBlockEntry{
		{
			PeerID:   peer,
			Seen:     time.Now(),
			Priority: priority,
		},
	}, fr.CidList[cid]...)
	// If size of the CID registry over maximum permitted, remove last item
	if len(fr.CidList[cid]) > maxEntires {
		fr.CidList[cid], err = removeItem(fr.CidList[cid], len(fr.CidList[cid])-1)
		if err != nil {
			return err
		}
	}
	return nil
}

func removeItem(s []*PeerBlockEntry, index int) ([]*PeerBlockEntry, error) {
	if index >= len(s) || index < 0 {
		return nil, fmt.Errorf("Wrong index used")
	}
	return append(s[:index], s[index+1:]...), nil
}

// GarbageCollect cleans outdated entries.
func (fr *FlatRegistry) GarbageCollect() {
	// TODO:; Do we need a garbage collector if we limit the size of
	// the registry? One of the problems may be that no want messages
	// have been received for a long time for CID and you end up sending
	// WANT-BLOCK to a extremely updated blockEntry.
}
