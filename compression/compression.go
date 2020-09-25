package compression

import (
	"bytes"
	"compress/gzip"
	"io"
	"sync"

	logging "github.com/ipfs/go-log"

	blocks "github.com/ipfs/go-block-format"
)

var log = logging.Logger("bitswap")

// CompressionPool stores the compression buffers
var CompressionPool *sync.Pool

// Compressor implements a comperssor interface.
type Compressor interface {
	Type() string
	Compress([]byte) []byte
	Uncompress([]byte) []byte
	UncompressBlocks([]blocks.Block) []blocks.Block
	CompressBlocks([]blocks.Block) []blocks.Block
	Strategy() string
}

// Gzip compressor
type Gzip struct {
	opts                int
	compressionStrategy string
}

// GzipCompressor intialize a new GZip compressor.
func GzipCompressor(compressionStrategy string) Compressor {
	// We use BestCompression by default. But this is prepared
	// to configure compressor with other options such as:
	// gzip.BestSpeed.
	// gzip.BestCompression
	// Best trade-off results gzip.DefaultCompression
	compressionLevel := gzip.BestCompression

	// Initialize the pool if it hasn't been initialized.
	if CompressionPool == nil {
		CompressionPool = &sync.Pool{
			New: func() interface{} {
				// NewWriterLevel only returns error on a bad level, we are guaranteeing
				// that this will be a valid level so it is okay to ignore the returned
				// error.
				w, _ := gzip.NewWriterLevel(nil, compressionLevel)
				return w
			},
		}
	}
	// Return compressor descriptor.
	return &Gzip{
		opts:                compressionLevel,
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
	w := CompressionPool.Get().(*gzip.Writer)
	// If we don't use the default level for gzip we should get
	// this error to detect if gzip compressor was configured with
	// a wrong level.
	// w, _ := gzip.NewWriterLevel(&out, g.opts)
	w.Reset(&out)
	w.Write(in)
	w.Close()
	CompressionPool.Put(w)
	return out.Bytes()
}

// Uncompress recovers original content,
func (g *Gzip) Uncompress(in []byte) []byte {
	inBuf := bytes.NewBuffer(in)
	// Initialize with enough space to fully uncompress.
	out := make([]byte, 2*inBuf.Len())
	r, err := gzip.NewReader(inBuf)
	if err != nil {
		log.Debugf("[ERROR] Error uncompressing data!! %w", err)
	}

	// Careful! If we don't read the full reader the compression
	// reader optimizes to be fast and only sends the first chunk
	// of 32768 compressed.
	count, err := io.ReadFull(r, out)
	if err != nil {
		log.Debugf("[ERROR] Error uncompressing data!! %w", err)
	}
	r.Close()

	// Remove trailing zeroes
	return out[:count]
}

// UncompressBlocks uncompresses a list of blocks.
func (g *Gzip) UncompressBlocks(blks []blocks.Block) []blocks.Block {
	for i, b := range blks {
		uncompressedData := g.Uncompress(b.RawData())
		blk, _ := blocks.NewBlockWithCid(uncompressedData, b.Cid())
		blks[i] = blk
	}
	return blks
}

// CompressBlocks uncompresses a list of blocks.
func (g *Gzip) CompressBlocks(blks []blocks.Block) []blocks.Block {
	for i, b := range blks {
		compressedData := g.Compress(b.RawData())
		blk, _ := blocks.NewBlockWithCid(compressedData, b.Cid())
		blks[i] = blk

	}
	return blks
}
