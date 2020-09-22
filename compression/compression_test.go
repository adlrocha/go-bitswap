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

	blks := []blocks.Block{blocks.NewBlock([]byte("block1")),
		blocks.NewBlock([]byte("block2")),
		blocks.NewBlock([]byte("block3")),
		blocks.NewBlock([]byte("foo")),
		blocks.NewBlock([]byte("barxx")),
	}

	compBlks := c.CompressBlocks(blks)
	unblks := c.UncompressBlocks(compBlks)
	for i, b := range unblks {
		if string(blks[i].RawData()) != string(b.RawData()) ||
			len(blks[i].RawData()) != len(b.RawData()) {
			t.Fatalf("Uncompression of blocks failed")

		}
	}
}
