package peerblockregistry

import (
	"testing"

	"github.com/ipfs/go-bitswap/internal/testutil"
)

func TestFlatPBR(t *testing.T) {
	pbr := NewFlatPeerBlock()
	cids := testutil.GenerateCids(3)
	peers := testutil.GeneratePeers(2)

	// UpdateRegistry with CIDs
	pbr.UpdateRegistry(peers[0], cids[0], 0)
	pbr.UpdateRegistry(peers[1], cids[0], 0)
	pbr.UpdateRegistry(peers[1], cids[1], 0)

	// Test right number of candidates
	candidates0 := pbr.GetCandidates(cids[0])
	candidates1 := pbr.GetCandidates(cids[1])
	emptyCandidates := pbr.GetCandidates(cids[2])

	if len(emptyCandidates) != 0 {
		t.Fatal("Registry shouldn't have candidates")
	}
	if len(candidates0) != 2 || len(candidates1) != 1 {
		t.Fatalf("Wrong number of candidates for %s", cids[0])
	}
	// Test right order.
	if candidates0[0] != peers[1] || candidates0[1] != peers[0] {
		t.Fatalf("Wrong order of peers for %s", cids[0])
	}
	// Test entries not duplicated
	pbr.UpdateRegistry(peers[0], cids[0], 0)
	candidates0 = pbr.GetCandidates(cids[0])
	if len(candidates0) != 2 {
		t.Fatalf("Wrong number of candidates for %s. Shouold be 2 got %d", cids[0], len(candidates0))
	}
	// Test that the maximum number is allowed.
	peers = testutil.GeneratePeers(12)
	for _, p := range peers {
		pbr.UpdateRegistry(p, cids[2], 0)
	}
	candidates := pbr.GetCandidates(cids[2])
	if len(candidates) != numberWantBlocks {
		t.Fatalf("Wrong number of numberWantBlocks being retrieved. Got %d and max %d",
			len(candidates), numberWantBlocks)
	}
	if len(pbr.(*FlatRegistry).CidList[cids[2]]) > maxEntires {
		t.Fatalf("Wrong number of maxEntries in FlatRegistry")
	}
	if candidates[0] != peers[len(peers)-1] {
		t.Fatalf("Peers being added in wrong order")
	}

	// Test that updates are correct when the number is high.
}
