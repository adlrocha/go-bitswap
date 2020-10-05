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

	bsnet "github.com/ipfs/go-bitswap/network"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/delayed"
	ds_sync "github.com/ipfs/go-datastore/sync"
	nilrouting "github.com/ipfs/go-ipfs-routing/none"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/host"
)

func TestIndirectSessionTTL1(t *testing.T) {
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

func TestIndirectSessionTTL2(t *testing.T) {
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
	t.Fatal()
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

func CreateBitswapNode(ctx context.Context, h host.Host, opts []bitswap.Option) (*bitswap.Bitswap, error) {
	bstore, err := CreateBlockstore(ctx, 4000)
	if err != nil {
		return nil, err
	}
	routing, err := nilrouting.ConstructNilRouting(ctx, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	net := bsnet.NewFromIpfsHost(h, routing)
	return bitswap.New(ctx, net, bstore, opts...).(*bitswap.Bitswap), nil
}

func TestIndirectRealNet(t *testing.T) {
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

	node1, _ := CreateBitswapNode(ctx, h1, bsOpts)
	_, _ = CreateBitswapNode(ctx, h2, bsOpts)
	_, _ = CreateBitswapNode(ctx, h3, bsOpts)
	node4, _ := CreateBitswapNode(ctx, h4, bsOpts)

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
	blk, err := node4.GetBlock(ctx, blks[0].Cid())
	if err != nil {
		t.Fatal(err)
	}

	h1.Close()
	h2.Close()
	h3.Close()
	h4.Close()
}
