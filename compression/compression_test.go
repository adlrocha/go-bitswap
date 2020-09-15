package compression

import (
	"compress/gzip"
	"testing"

	blocks "github.com/ipfs/go-block-format"
)

var compressionStrategy = "blocks"

func TestGzip(t *testing.T) {
	// Initialize GZip compressor
	c := NewGzipCompressor(compressionStrategy)
	if c.(*Gzip).opts != gzip.BestCompression || c.Strategy() != compressionStrategy {
		t.Fatalf("GZip was not implemented")
	}

	// Compress random string
	s := []byte("This is a test")
	comp := c.Compress(s)
	uncomp := c.Uncompress(comp)
	if string(uncomp) != string(s) && len(uncomp) == len(s) {
		t.Fatalf("Gzip compression and uncompression unsuccessful: %s, %s", string(s), string(uncomp))
	}
}

func TestCompressBlocks(t *testing.T) {

	// Compress an uncompress list of blocks
	c := NewGzipCompressor(compressionStrategy)
	var err error

	blks := []blocks.Block{blocks.NewBlock([]byte("block1")),
		blocks.NewBlock([]byte("block2")),
	}
	compBlks := make([]blocks.Block, len(blks))
	for i, b := range blks {
		compData := c.Compress(b.RawData())
		compBlks[i], err = blocks.NewBlockWithCid(compData, b.Cid())
		if err != nil {
			t.Fatal(err)
		}
	}

	unblks := c.(*Gzip).UncompressBlocks(compBlks)
	for i, b := range unblks {
		if string(blks[i].RawData()) != string(b.RawData()) {
			t.Fatalf("Uncompression of blocks failed")

		}
	}
}
