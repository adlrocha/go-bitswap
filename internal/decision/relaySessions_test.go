package decision

import (
	"testing"

	"github.com/ipfs/go-bitswap/internal/testutil"
)

func TestKeyTracker(t *testing.T) {
	kt := newKeyTracker()
	if len(kt.t) != 0 {
		t.Fatal("Tracker not initialized successfully")
	}
	var i int32 = 0
	cids := testutil.GenerateCids(3)
	for _, c := range cids {
		kt.updateTracker(i, i+1, c)
		if len(kt.t[i][i+1]) != 1 || kt.t[i][i+1][0] != c {
			t.Fatal("Wrong individual CID update")
		}
		i++
	}
	if len(kt.t) != 3 || len(kt.t[1][2]) != 1 {
		t.Fatal("TTLs and priorities not updated successfully")
	}
}
func TestIndirectSessions(t *testing.T) {
	peers := testutil.GeneratePeers(2)
	keys := testutil.GenerateCids(5)
	sr := newSessionRegistry()
	sr.add(1, peers[0], keys[:3], 1, 1)
	sr.add(2, peers[1], keys[2:], 2, 2)
	if sr.len() != 2 {
		t.Fatal("Not every session was added")
	}
	if !sr.has(1) && sr.has(3) {
		t.Fatal("Ids for sessions added unsuccessfully")
	}

	// Test interested sessions
	interested := sr.interestedSessions(keys[2])
	if len(interested) != 2 {
		t.Fatal("Wrong number of interested sessions")
	}
	c := testutil.GenerateCids(1)[0]

	interested = sr.interestedSessions(c)
	if len(interested) != 0 {
		t.Fatal("No sessions should be interested")
	}

	// Test interested peers
	interestedP := sr.interestedPeers(keys[2])
	if len(interestedP) != 2 {
		t.Fatal("Wrong number of interested sessions")
	}
	// Test not active
	if len(sr.notActive(peers[1], keys[2:])) != 0 {
		t.Fatal("All cids should be active in sessions")
	}
	if len(sr.notActive(peers[1], keys)) != len(keys)-len(keys[2:]) {
		t.Fatal("Inactive keys computed wrong!")
	}
	// Test remove key
	sr.removeKey(keys[2])
	if len(sr.interestedSessions(keys[2])) != 0 {
		t.Fatalf("They key was not removed in every interested session")
	}

	// Test remove session
	sr.remove(1)
	if sr.has(1) {
		t.Fatalf("The session was not removed successfully")
	}

	// Remove session by deleting every key
	for _, k := range keys {
		sr.removeKey(k)
	}
	if sr.len() != 0 || sr.has(2) {
		t.Fatalf("All keys removed so every session should have been removed by now")
	}

}

func TestNotActive(t *testing.T) {
	peers := testutil.GeneratePeers(2)
	keys := testutil.GenerateCids(5)
	sr := newSessionRegistry()
	k := sr.notActive(peers[0], keys)
	if len(k) != len(keys) {
		t.Fatal("Wrong number of active keys", k)
	}
}
