package compression

import (
	"compress/gzip"
	"crypto/rand"
	"fmt"
	"testing"

	blocks "github.com/ipfs/go-block-format"
)

var compressionStrategy = "blocks"

func TestGzip(t *testing.T) {
	// Initialize GZip compressor
	c := GzipCompressor(compressionStrategy)
	if CompressionPool == nil {
		t.Fatalf("CompressionPool not initialized succesfully")
	}
	if c.(*Gzip).opts != gzip.BestCompression || c.Strategy() != compressionStrategy {
		t.Fatalf("GZip was not implemented")
	}

	// Compress random bytes
	blks := GenerateBlocksOfSize(1, 1234567)
	s := blks[0].RawData()

	comp := c.Compress(s)
	uncomp := c.Uncompress(comp)
	if string(uncomp) != string(s) && len(uncomp) == len(s) {
		fmt.Println(uncomp, s)
		t.Fatalf("Gzip compression and uncompress unsuccessful: %d, %d", len(s), len(uncomp))
	}
}

func TestCompressBlocks(t *testing.T) {

	// Compress and uncompress list of blocks
	c := GzipCompressor(compressionStrategy)

	blks := GenerateBlocksOfSize(5, 1234567)

	compBlks := c.CompressBlocks(blks)
	unblks := c.UncompressBlocks(compBlks)
	for i, b := range unblks {
		if string(blks[i].RawData()) != string(b.RawData()) ||
			len(blks[i].RawData()) != len(b.RawData()) {
			t.Fatalf("Uncompression of blocks failed")

		}
	}
}

// GenerateBlocksOfSize to generate larger blocks.
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
