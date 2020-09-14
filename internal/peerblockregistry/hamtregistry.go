package peerblockregistry

import (
	"encoding/binary"
	"sync"

	"github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-peer"
	"github.com/lleo/go-hamt"
	"github.com/lleo/go-hamt/key"
	"github.com/lleo/go-hamt/stringkey"
)

type HAMTRegistry struct {
	CidHAMT hamt.Hamt
	pbrLk   sync.RWMutex
}

type cidKey struct {
	cid cid.Cid
}

func NewHAMTRegistry() PeerBlockRegistry {
	return &HAMTRegistry{
		CidHAMT: hamt.NewHamt64(),
	}
}

// Implementation of a CidKey compatible with HAMT.
func (c *cidKey) String() string {
	return c.cid.String()
}

func (c *cidKey) Equals(k key.Key) bool {
	return c.cid.String() == k.String()
}

func (c *cidKey) Hash30() uint32 {
	return binary.LittleEndian.Uint32(c.cid.Bytes())
}

func (c *cidKey) Hash60() uint64 {
	return binary.LittleEndian.Uint64(c.cid.Bytes())
}

// GetCandidates to send the WANT-BLOCK to, as we've received
// a request for the CID from them
func (hr *HAMTRegistry) GetCandidates(cid cid.Cid) []peer.ID {
	key := stringkey.New(cid.String())
	// key := &cidKey{cid: cid}

	peers, res := hr.CidHAMT.Get(key)

	if !res {
		return []peer.ID{}
	}
	if len(peers.([]peer.ID)) > numberWantBlocks {
		peers = peers.([]peer.ID)[:numberWantBlocks]
	}
	return peers.([]peer.ID)
}

// UpdateRegistry updates the registry with information of the wantlist
func (hr *HAMTRegistry) UpdateRegistry(p peer.ID, cid cid.Cid, priority int32) error {
	var err error

	hr.pbrLk.Lock()
	defer hr.pbrLk.Unlock()

	key := stringkey.New(cid.String())
	// key := &cidKey{cid: cid}

	// Get list of peers in key
	peers, res := hr.CidHAMT.Del(key)
	peerList := []peer.ID{}
	if res {
		peerList = peers.([]peer.ID)

	}

	// Update with the incoming peer
	peerList, err = addEntry(peerList, p, maxEntires)
	if err != nil {
		return err
	}
	// Update HAMT
	res = hr.CidHAMT.Put(key, peerList)

	// To test:
	peers, res = hr.CidHAMT.Get(key)
	return nil
}

// Clear cleans the registry.
func (hr *HAMTRegistry) Clear() {
	*hr = HAMTRegistry{
		CidHAMT: hamt.NewHamt64(),
	}
}

// GarbageCollect cleans outdated entries.
func (hr *HAMTRegistry) GarbageCollect() {
	// TODO:; Periodically cleans the registry
	// to remove old and outdated entries
}
