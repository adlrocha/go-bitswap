package compression

import (
	"bytes"
	"compress/gzip"
	"fmt"

	blocks "github.com/ipfs/go-block-format"
)

// Compressor implements a comperssor interface.
type Compressor interface {
	Type() string
	Compress([]byte) []byte
	Uncompress([]byte) []byte
	UncompressBlocks([]blocks.Block) []blocks.Block
	Strategy() string
}

// Gzip compressor
type Gzip struct {
	opts                int
	compressionStrategy string
}

// NewGzipCompressor intialize a new GZip compressor.
func NewGzipCompressor(compressionStrategy string) Compressor {
	return &Gzip{
		// Be use BestCompression by default. But this is prepared
		// to configure compressor with other options such as:
		// gzip.BestSpeed.
		// Best trade-off results gzip.DefaultCompression
		opts:                gzip.BestCompression,
		compressionStrategy: compressionStrategy,
	}
}

// Strategy returns the bitswap compression strategy to be used.
func (g *Gzip) Strategy() string {
	return g.compressionStrategy
}

// Type returns the
func (g *Gzip) Type() string {
	return "gzip"
}

// Compress bytes with GZip
func (g *Gzip) Compress(in []byte) []byte {
	var out bytes.Buffer
	// If we don't use the default level for gzip we should get
	// this error to detect if gzip compressor was configured with
	// a wrong level.
	w, _ := gzip.NewWriterLevel(&out, g.opts)
	w.Write(in)
	w.Close()
	return out.Bytes()
}

// Uncompress recovers original content,
func (g *Gzip) Uncompress(in []byte) []byte {
	inBuf := bytes.NewBuffer(in)
	// Initialize with enough space to fully uncompress.
	out := make([]byte, 2*inBuf.Len())
	r, err := gzip.NewReader(inBuf)
	if err != nil {
		fmt.Println(err)
	}
	count, _ := r.Read(out)
	// Remove trailing zeroes
	return out[:count]
}

// UncompressBlocks uncompresses a list of blocks.
func (g *Gzip) UncompressBlocks(blks []blocks.Block) []blocks.Block {
	for i, b := range blks {
		uncompressedData := g.Uncompress(b.RawData())
		blk, err := blocks.NewBlockWithCid(uncompressedData, b.Cid())
		// Do not assign compressed data if the node fails.
		// TODO: This is potentially dangerous and nemust be fixed
		// with a compression flag as it can lead to some blocks being
		// compressed and other not, and not being able to identify this.
		if err == nil {
			blks[i] = blk
		}
	}
	return blks
}
