package bitswap_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	bitswap "github.com/ipfs/go-bitswap"
	testinstance "github.com/ipfs/go-bitswap/testinstance"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	blocksutil "github.com/ipfs/go-ipfs-blocksutil"
	delay "github.com/ipfs/go-ipfs-delay"
	logging "github.com/ipfs/go-log"
	"golang.org/x/sync/errgroup"

	bsnet "github.com/ipfs/go-bitswap/network"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/delayed"
	ds_sync "github.com/ipfs/go-datastore/sync"
	nilrouting "github.com/ipfs/go-ipfs-routing/none"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/host"
)

func TestRelaySessionTTL1(t *testing.T) {
	logging.SetLogLevel("engine", "DEBUG")
	// If you change the TTL to zero the test should timeout because
	// there is no direct connection between peers.
	var ttl int32 = 1

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	vnet := getVirtualNetwork()
	// Start instances with high provider delay to disable ProvideSearch
	ig := testinstance.NewTestInstanceGenerator(vnet, nil, []bitswap.Option{
		bitswap.ProviderSearchDelay(1000 * time.Second),
		bitswap.RebroadcastDelay(delay.Fixed(1000 * time.Second)),
		bitswap.SetTTL(ttl),
	})
	defer ig.Close()
	bgen := blocksutil.NewBlockGenerator()

	inst := ig.Instances(3)
	peerA := inst[0]
	peerB := inst[1]
	peerC := inst[2]

	// Force A to be connected to B and B to C. C will request blocks to A through C
	// Break connection
	err := peerA.Adapter.DisconnectFrom(ctx, peerC.Peer)
	if err != nil {
		t.Fatal(err)
	}
	// // Try uncommenting this to see what happens when A is isolated. There
	// is not provider search and it times out.
	// err = peerA.Adapter.DisconnectFrom(ctx, peerB.Peer)
	// if err != nil {
	// 	t.Fatal(err)
	// }

	// Provide 5 blocks on Peer A
	blks := bgen.Blocks(5)
	var cids []cid.Cid
	for _, blk := range blks {
		cids = append(cids, blk.Cid())
	}

	keys := make([]cid.Cid, 0)
	for _, block := range blks {
		keys = append(keys, block.Cid())
		if err := peerA.Exchange.HasBlock(block); err != nil {
			t.Fatal(err)
		}
	}

	// Request all blocks with Peer C
	ch, err := peerC.Exchange.GetBlocks(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}

	// Should get first 5 blocks
	var got []blocks.Block
	for i := 0; i < 5; i++ {
		b := <-ch
		got = append(got, b)
	}

	if err := assertBlockLists(got, blks); err != nil {
		t.Fatal(err)
	}
	fmt.Println(peerB.Exchange.Stat())
}

func TestRelaySessionTTL2(t *testing.T) {
	var ttl int32 = 2

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	vnet := getVirtualNetwork()
	// Start instances with high provider delay to disable ProvideSearch
	ig := testinstance.NewTestInstanceGenerator(vnet, nil, []bitswap.Option{
		bitswap.ProviderSearchDelay(1000 * time.Second),
		bitswap.RebroadcastDelay(delay.Fixed(1000 * time.Second)),
		bitswap.SetTTL(ttl),
	})
	defer ig.Close()
	bgen := blocksutil.NewBlockGenerator()

	inst := ig.Instances(4)
	peerA := inst[0]
	peerB := inst[1]
	peerC := inst[2]
	peerD := inst[3]

	t.Logf("%s -- %s -- %s -- %s", peerA.Peer, peerB.Peer, peerC.Peer, peerD.Peer)

	// Force A to be connected to B and B to C and C to D. D will request blocks to A
	// through B and C
	// Break connection
	err := peerA.Adapter.DisconnectFrom(ctx, peerC.Peer)
	if err != nil {
		t.Fatal(err)
	}
	err = peerA.Adapter.DisconnectFrom(ctx, peerD.Peer)
	if err != nil {
		t.Fatal(err)
	}
	err = peerB.Adapter.DisconnectFrom(ctx, peerD.Peer)
	if err != nil {
		t.Fatal(err)
	}
	err = peerC.Adapter.DisconnectFrom(ctx, peerA.Peer)
	if err != nil {
		t.Fatal(err)
	}
	err = peerD.Adapter.DisconnectFrom(ctx, peerA.Peer)
	if err != nil {
		t.Fatal(err)
	}
	err = peerD.Adapter.DisconnectFrom(ctx, peerB.Peer)
	if err != nil {
		t.Fatal(err)
	}

	// Provide 5 blocks on Peer A
	blks := bgen.Blocks(5)
	var cids []cid.Cid
	for _, blk := range blks {
		cids = append(cids, blk.Cid())
	}

	keys := make([]cid.Cid, 0)
	for _, block := range blks {
		keys = append(keys, block.Cid())
		if err := peerA.Exchange.HasBlock(block); err != nil {
			t.Fatal(err)
		}
	}

	// Request all blocks with Peer C
	ch, err := peerD.Exchange.GetBlocks(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}

	// Should get first 5 blocks
	var got []blocks.Block
	for i := 0; i < 5; i++ {
		b := <-ch
		got = append(got, b)
	}

	if err := assertBlockLists(got, blks); err != nil {
		t.Fatal(err)
	}
	// t.Fatal()
}

func CreateBlockstore(ctx context.Context, bstoreDelay time.Duration) (blockstore.Blockstore, error) {
	bsdelay := delay.Fixed(bstoreDelay)
	dstore := ds_sync.MutexWrap(delayed.New(ds.NewMapDatastore(), bsdelay))
	return blockstore.CachedBlockstore(ctx,
		blockstore.NewBlockstore(ds_sync.MutexWrap(dstore)),
		blockstore.DefaultCacheOpts())
}

// GenerateBlocksOfSize generates a series of blocks of the given byte size
func GenerateBlocksOfSize(n int, size int64) []blocks.Block {
	generatedBlocks := make([]blocks.Block, 0, n)
	for i := 0; i < n; i++ {
		// rand.Read never errors
		buf := make([]byte, size)
		rand.Read(buf)
		b := blocks.NewBlock(buf)
		generatedBlocks = append(generatedBlocks, b)

	}
	return generatedBlocks
}

func CreateBitswapNode(ctx context.Context, h host.Host, opts []bitswap.Option) (*bitswap.Bitswap, blockstore.Blockstore, error) {
	bstore, err := CreateBlockstore(ctx, 4000)
	if err != nil {
		return nil, nil, err
	}
	routing, err := nilrouting.ConstructNilRouting(ctx, nil, nil, nil)
	if err != nil {
		return nil, nil, err
	}
	net := bsnet.NewFromIpfsHost(h, routing)
	return bitswap.New(ctx, net, bstore, opts...).(*bitswap.Bitswap), bstore, nil
}

func TestRelayRealNet(t *testing.T) {
	logging.SetLogLevel("engine", "DEBUG")
	// logging.SetLogLevel("bitswap", "DEBUG")
	var ttl int32 = 2
	bsOpts := []bitswap.Option{
		bitswap.SetTTL(ttl),
	}

	ctx := context.Background()
	h1, _ := libp2p.New(ctx)
	h2, _ := libp2p.New(ctx)
	h3, _ := libp2p.New(ctx)
	h4, _ := libp2p.New(ctx)

	node1, _, _ := CreateBitswapNode(ctx, h1, bsOpts)
	_, _, _ = CreateBitswapNode(ctx, h2, bsOpts)
	_, _, _ = CreateBitswapNode(ctx, h3, bsOpts)
	node4, _, _ := CreateBitswapNode(ctx, h4, bsOpts)

	// Connect peers
	if err := h1.Connect(ctx, *host.InfoFromHost(h2)); err != nil {
		t.Fatalf("Error dialing peers")
	}
	if err := h2.Connect(ctx, *host.InfoFromHost(h3)); err != nil {
		t.Fatalf("Error dialing peers")
	}
	if err := h3.Connect(ctx, *host.InfoFromHost(h4)); err != nil {
		t.Fatalf("Error dialing peers")
	}

	// bg := blocksutil.NewBlockGenerator()
	// blks := bg.Blocks(1)
	blks := GenerateBlocksOfSize(1, 12345)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	if err := node1.HasBlock(blks[0]); err != nil {
		t.Fatal(err)
	}
	// Second peer broadcasts want for block CID
	// (Received by first and third peers)
	_, err := node4.GetBlock(ctx, blks[0].Cid())
	if err != nil {
		t.Fatal(err)
	}

	h1.Close()
	h2.Close()
	h3.Close()
	h4.Close()
}

func SetupConnections(ctx context.Context, self host.Host, others []host.Host) error {
	for _, h := range others {
		if err := self.Connect(ctx, *host.InfoFromHost(h)); err != nil {
			return err
		}
	}
	return nil
}

func TestComplexTopology(t *testing.T) {
	logging.SetLogLevel("engine", "DEBUG")
	logging.SetLogLevel("bitswap", "DEBUG")

	var ttl int32 = 1
	bsOpts := []bitswap.Option{
		bitswap.SetTTL(ttl),
	}
	ctx := context.Background()
	seed1, _ := libp2p.New(ctx)
	seed2, _ := libp2p.New(ctx)
	passive1, _ := libp2p.New(ctx)
	passive2, _ := libp2p.New(ctx)
	leech1, _ := libp2p.New(ctx)

	t.Logf("Seed1: %s", seed1.ID())
	t.Logf("Seed2: %s", seed2.ID())
	t.Logf("Passive1: %s", passive1.ID())
	t.Logf("Passive2: %s", passive2.ID())
	t.Logf("Leech1: %s", leech1.ID())

	seedNode1, _, _ := CreateBitswapNode(ctx, seed1, bsOpts)
	seedNode2, _, _ := CreateBitswapNode(ctx, seed2, bsOpts)
	_, _, _ = CreateBitswapNode(ctx, passive1, bsOpts)
	_, _, _ = CreateBitswapNode(ctx, passive2, bsOpts)
	leechNode1, _, _ := CreateBitswapNode(ctx, leech1, bsOpts)

	// bg := blocksutil.NewBlockGenerator()
	// blks := bg.Blocks(1)
	blks := GenerateBlocksOfSize(6, 123456)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	keys := make([]cid.Cid, 0)
	for _, b := range blks {
		keys = append(keys, b.Cid())
		// Seeder nodes have the block
		if err := seedNode1.HasBlock(b); err != nil {
			t.Fatal(err)
		}
		if err := seedNode2.HasBlock(b); err != nil {
			t.Fatal(err)
		}
	}

	// Connect peers. Seed can only connect to seeds and passive.
	// And leeches can only connect to leechers and passive so no seed-leech connection possible.
	if err := SetupConnections(ctx, seed1, []host.Host{seed2, passive1, passive2}); err != nil {
		t.Fatal("Error dialing peers seed1", err)
	}
	if err := SetupConnections(ctx, seed2, []host.Host{seed1, passive1, passive2}); err != nil {
		t.Fatal("Error dialing peers seed2", err)
	}
	if err := SetupConnections(ctx, leech1, []host.Host{passive1, passive2}); err != nil {
		t.Fatal("Error dialing peers leech1", err)
	}
	if err := SetupConnections(ctx, passive1, []host.Host{passive2}); err != nil {
		t.Fatal("Error dialing peers passive1", err)
	}

	ch, err := leechNode1.GetBlocks(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}

	var got []blocks.Block
	for i := 0; i < len(blks); i++ {
		b := <-ch
		got = append(got, b)
		t.Log("::::: BLOCK", b.Cid())
	}

	if err := assertBlockLists(got, blks); err != nil {
		t.Fatal(err)
	}

	// Closing peers
	for _, h := range []host.Host{seed1, seed2, passive1, passive2, leech1} {
		h.Close()
	}
	// t.Fatal()
}

func ClearBlockstore(ctx context.Context, bstore blockstore.Blockstore) error {
	ks, err := bstore.AllKeysChan(ctx)
	if err != nil {
		return err
	}
	g := errgroup.Group{}
	for k := range ks {
		c := k
		g.Go(func() error {
			return bstore.DeleteBlock(c)
		})
	}
	return g.Wait()
}

func TestComplexTopologyAndWaves(t *testing.T) {
	// logging.SetLogLevel("engine", "DEBUG")
	// logging.SetLogLevel("bitswap", "DEBUG")

	var ttl int32 = 2
	bsOpts := []bitswap.Option{
		bitswap.SetTTL(ttl),
	}
	ctx := context.Background()
	seed1, _ := libp2p.New(ctx)
	seed2, _ := libp2p.New(ctx)
	passive1, _ := libp2p.New(ctx)
	passive2, _ := libp2p.New(ctx)
	leech1, _ := libp2p.New(ctx)

	t.Logf("Seed1: %s", seed1.ID())
	t.Logf("Seed2: %s", seed2.ID())
	t.Logf("Passive1: %s", passive1.ID())
	t.Logf("Passive2: %s", passive2.ID())
	t.Logf("Leech1: %s", leech1.ID())

	seedNode1, _, _ := CreateBitswapNode(ctx, seed1, bsOpts)
	seedNode2, _, _ := CreateBitswapNode(ctx, seed2, bsOpts)
	_, bstorePassive1, _ := CreateBitswapNode(ctx, passive1, bsOpts)
	_, bstorePassive2, _ := CreateBitswapNode(ctx, passive2, bsOpts)
	leechNode1, bstoreLeech1, _ := CreateBitswapNode(ctx, leech1, bsOpts)

	// bg := blocksutil.NewBlockGenerator()
	// blks := bg.Blocks(1)
	blks := GenerateBlocksOfSize(6, 123456)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	keys := make([]cid.Cid, 0)
	for _, b := range blks {
		keys = append(keys, b.Cid())
		// Seeder nodes have the block
		if err := seedNode1.HasBlock(b); err != nil {
			t.Fatal(err)
		}
		if err := seedNode2.HasBlock(b); err != nil {
			t.Fatal(err)
		}
	}

	// Connect peers. Seed can only connect to seeds and passive.
	// And leeches can only connect to leechers and passive so no seed-leech connection possible.
	if err := SetupConnections(ctx, seed1, []host.Host{seed2, passive1, passive2}); err != nil {
		t.Fatal("Error dialing peers seed1", err)
	}
	if err := SetupConnections(ctx, seed2, []host.Host{seed1, passive1, passive2}); err != nil {
		t.Fatal("Error dialing peers seed2", err)
	}
	if err := SetupConnections(ctx, leech1, []host.Host{passive1, passive2}); err != nil {
		t.Fatal("Error dialing peers leech1", err)
	}
	if err := SetupConnections(ctx, passive1, []host.Host{passive2}); err != nil {
		t.Fatal("Error dialing peers passive1", err)
	}

	t.Log("First wave")
	ch, err := leechNode1.GetBlocks(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}

	var got []blocks.Block
	for i := 0; i < len(blks); i++ {
		b := <-ch
		got = append(got, b)
		t.Log("::::: BLOCK", b.Cid())
	}

	if err := assertBlockLists(got, blks); err != nil {
		t.Fatal(err)
	}

	// Clean datastore for leech and passive peers
	ClearBlockstore(ctx, bstoreLeech1)
	ClearBlockstore(ctx, bstorePassive1)
	ClearBlockstore(ctx, bstorePassive2)

	got = make([]blocks.Block, 0)
	t.Log("Second wave")
	ch, err = leechNode1.GetBlocks(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < len(blks); i++ {
		b := <-ch
		got = append(got, b)
		t.Log("::::: BLOCK", b.Cid())
	}

	if err := assertBlockLists(got, blks); err != nil {
		t.Fatal(err)
	}

	// Closing peers
	for _, h := range []host.Host{seed1, seed2, passive1, passive2, leech1} {
		h.Close()
	}
	// t.Fatal()
}
