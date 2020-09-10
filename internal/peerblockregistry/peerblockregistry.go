package peerblockregistry

import (
	"fmt"

	"github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-peer"
)

const (
	numberWantBlocks = 3   // Number of peers to send want blcoks to
	gcPeriod         = 300 // Garbage collection period for the peerblockregistry
	maxEntires       = 10  // Max number of entries per CID in Registry
)

// PeerBlockRegistry implements the table with information about content flowing around.
type PeerBlockRegistry interface {
	GetCandidates(cid cid.Cid) []peer.ID
	UpdateRegistry(peer peer.ID, cid cid.Cid, priority int32) error
	GarbageCollect()
}

// // PeerBlockEntry information that want to be tracked when inspecting
// // Wantlists.
// // TODO: Currently we don't use all this information so entries will be peerIDs.
// type PeerBlockEntry struct {
// 	PeerID   peer.ID
// 	Seen     time.Time
// 	Priority int32
// 	// Here we can add additional information
// 	// to make decisions.
// }

func removeItem(s []peer.ID, index int) ([]peer.ID, error) {
	if index >= len(s) || index < 0 {
		return nil, fmt.Errorf("Wrong index used")
	}
	return append(s[:index], s[index+1:]...), nil
}

func addEntry(peerList []peer.ID, p peer.ID, maxItems int) ([]peer.ID, error) {
	var err error
	for i, entry := range peerList {
		if entry == p {
			// Remove previous entry of the peer in registry
			peerList, err = removeItem(peerList, i)
			if err != nil {
				return nil, err
			}
		}
	}
	// Add entry to the beginning
	peerList = append([]peer.ID{p}, peerList...)
	// If size of the CID registry over maximum permitted, remove last item
	if len(peerList) > maxItems {
		peerList, err = removeItem(peerList, len(peerList)-1)
		if err != nil {
			return nil, err
		}
	}

	return peerList, nil
}
