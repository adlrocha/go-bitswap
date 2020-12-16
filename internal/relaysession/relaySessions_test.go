package relaysession

import (
	"context"
	"testing"

	"github.com/ipfs/go-bitswap/internal/notifications"
	"github.com/ipfs/go-bitswap/internal/testutil"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

type fakeSession struct {
	ks         []cid.Cid
	wantBlocks []cid.Cid
	wantHaves  []cid.Cid
	id         uint64
	pm         *fakeSesPeerManager
	notif      notifications.PubSub
	ttl        int32
	relay      bool
}

func (*fakeSession) GetBlock(context.Context, cid.Cid) (blocks.Block, error) {
	return nil, nil
}
func (*fakeSession) GetBlocks(context.Context, []cid.Cid) (<-chan blocks.Block, error) {
	return nil, nil
}
func (fs *fakeSession) ID() uint64 {
	return fs.id
}
func (fs *fakeSession) ReceiveFrom(p peer.ID, ks []cid.Cid, wantBlocks []cid.Cid, wantHaves []cid.Cid) {
	fs.ks = append(fs.ks, ks...)
	fs.wantBlocks = append(fs.wantBlocks, wantBlocks...)
	fs.wantHaves = append(fs.wantHaves, wantHaves...)
}
func (fs *fakeSession) Shutdown() {
}

type fakeSesPeerManager struct {
}

func TestKeyTracker(t *testing.T) {
	peers := testutil.GeneratePeers(2)
	p := peers[0]
	s := peers[1]
	kt := NewKeyTracker(p)
	if len(kt.T) != 0 || kt.Peer != p {
		t.Fatal("Tracker not initialized successfully")
	}

	var i int32 = 0
	cids := testutil.GenerateCids(3)
	for _, c := range cids {
		kt.UpdateTracker(c, i, s)
		if kt.T[i].TTL != i || kt.T[i].Key != c || kt.T[i].Source != s {
			t.Fatalf("Wrong individual CID update Got: %s, %d - Expected: %s, %d",
				kt.T[i].Key, kt.T[i].TTL, c, i)
		}
		i++
	}
	if len(kt.T) != len(cids) {
		t.Fatal("Number of updates not successful")
	}
}
func TestRelaySessions(t *testing.T) {
	peers := testutil.GeneratePeers(4)
	s := peers[2]
	self := peers[3]
	keys := testutil.GenerateCids(5)
	rs := NewRelaySession(10)
	if rs.Registry.Degree != 10 || rs.Session != nil {
		t.Fatal("Error initializing relaySession")
	}

	// Initialize keytrackers
	kt1 := NewKeyTracker(peers[0])
	kt2 := NewKeyTracker(peers[1])
	for _, c := range keys[:3] {
		kt1.UpdateTracker(c, 1, s)
	}
	for _, c := range keys[2:] {
		kt2.UpdateTracker(c, 2, s)
	}

	// Start new relaySession
	rs.Session = &fakeSession{}

	rs.UpdateSession(context.Background(), self, kt1)
	if len(rs.InterestedPeers(keys[0])) != 1 &&
		len(rs.InterestedPeers(keys[len(keys)-1])) != 0 {
		t.Fatal("First session update failed")
	}

	rs.UpdateSession(context.Background(), self, kt2)
	if len(rs.InterestedPeers(keys[2])) != 2 &&
		len(rs.InterestedPeers(keys[len(keys)])) != 1 {
		t.Fatal("Second session update failed")
	}

	if rs.Registry.GetTTL(keys[0]) != 1 ||
		rs.Registry.GetTTL(keys[len(keys)-1]) != 2 {
		t.Fatal("Get TTL failed")
	}

	if a := rs.Registry.GetSource(keys[0]); a != s {
		t.Fatal("Wrong source retrieved", a, s)
	}

	rs.RemoveInterest(keys[0], peers[0])
	rs.RemoveInterest(keys[0], peers[0])
	rs.RemoveInterest(keys[2], peers[0])
	ps := rs.InterestedPeers(keys[2])
	if len(ps) != 1 && ps[peers[1]] != 0 {
		t.Fatalf("Interest for peer not removed successfully")
	}
	_, found := rs.Registry.r[keys[0]]
	if found {
		t.Fatal("Interest CIDs not cleaned correctly")
	}
	_, found = rs.Registry.r[keys[2]]
	if !found {
		t.Fatal("Something went wrong removing peers")
	}

	rs.BlockSent(keys[2])
	ps = rs.InterestedPeers(keys[2])
	if len(ps) != 0 {
		t.Fatal("CID for block sent not cleared")
	}

}

func TestUpdateSelf(t *testing.T) {
	peers := testutil.GeneratePeers(4)
	self := peers[3]
	keys := testutil.GenerateCids(5)
	rs := NewRelaySession(10)
	if rs.Registry.Degree != 10 || rs.Session != nil {
		t.Fatal("Error initializing relaySession")
	}

	// Initialize keytracker
	kt1 := NewKeyTracker(peers[0])
	for _, c := range keys {
		// The source of WANTs is self
		kt1.UpdateTracker(c, 1, self)
	}

	// Start new relaySession
	rs.Session = &fakeSession{}

	rs.UpdateSession(context.Background(), self, kt1)
	if len(rs.InterestedPeers(keys[0])) != 0 {
		t.Fatal("Added interested and started relaySession for WANT where I am the source")
	}

}
