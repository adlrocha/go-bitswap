package peerblockregistry

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/ipfs/go-bitswap/internal/testutil"
	peer "github.com/libp2p/go-libp2p-peer"
	"github.com/lleo/go-hamt/stringkey"
)

func TestFlatPBR(t *testing.T) {
	pbr := NewFlatRegistry()
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

	// Test Clear Registry.
	pbr.Clear()
	if len(pbr.(*FlatRegistry).CidList) != 0 {
		t.Fatalf("The registry wasn't cleared successfully")
	}
}

func TestHAMTPBR(t *testing.T) {
	pbr := NewHAMTRegistry()
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
		fmt.Println("Candidates0: ", candidates0)
		fmt.Println("Candidates1: ", candidates1)
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
		t.Fatalf("Wrong number of candidates for %s. Should be 2 got %d", cids[0], len(candidates0))
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

	// key := &cidKey{cids[2]}
	key := stringkey.New(cids[2].String())
	entries, _ := pbr.(*HAMTRegistry).CidHAMT.Get(key)
	if len(entries.([]peer.ID)) > maxEntires {
		t.Fatalf("Wrong number of maxEntries in FlatRegistry")
	}

	if candidates[0] != peers[len(peers)-1] {
		t.Fatalf("Peers being added in wrong order")
	}

	// Test Clear Registry.
	pbr.Clear()
	if !pbr.(*HAMTRegistry).CidHAMT.IsEmpty() {
		t.Fatalf("The registry wasn't cleared successfully")
	}
}

func TestBenchmarkFlatRegistry(t *testing.T) {
	pbr := NewFlatRegistry()
	cids := testutil.GenerateCids(1000000)
	peers := testutil.GeneratePeers(1000000)

	// UpdateRegistry with CIDs
	for i := 0; i < 1000000; i++ {
		pbr.UpdateRegistry(peers[i], cids[i], int32(i))
	}

	for i := 0; i < 1000000; i++ {
		pbr.GetCandidates(cids[i])
	}
	PrintMemUsage()
	t.Fatal()
}

func TestBenchmarkHAMTRegistry(t *testing.T) {
	pbr := NewHAMTRegistry()
	cids := testutil.GenerateCids(1000000)
	peers := testutil.GeneratePeers(1000000)

	// UpdateRegistry with CIDs
	for i := 0; i < 1000000; i++ {
		pbr.UpdateRegistry(peers[i], cids[i], int32(i))
	}

	for i := 0; i < 1000000; i++ {
		pbr.GetCandidates(cids[i])
	}
	PrintMemUsage()
	t.Fatal()
}

// PrintMemUsage outputs the current, total and OS memory being used. As well as the number
// of garage collection cycles completed.
func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
