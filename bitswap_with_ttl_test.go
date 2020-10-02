package bitswap_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	bitswap "github.com/ipfs/go-bitswap"
	testinstance "github.com/ipfs/go-bitswap/testinstance"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	blocksutil "github.com/ipfs/go-ipfs-blocksutil"
	delay "github.com/ipfs/go-ipfs-delay"
	logging "github.com/ipfs/go-log"
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
	logging.SetLogLevel("engine", "DEBUG")
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
}
