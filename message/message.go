package message

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	pb "github.com/ipfs/go-bitswap/message/pb"
	"github.com/ipfs/go-bitswap/wantlist"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log"
	pool "github.com/libp2p/go-buffer-pool"
	msgio "github.com/libp2p/go-msgio"

	"github.com/ipfs/go-bitswap/compression"
	u "github.com/ipfs/go-ipfs-util"
	"github.com/libp2p/go-libp2p-core/network"
)

var log = logging.Logger("bitswap")

// BitSwapMessage is the basic interface for interacting building, encoding,
// and decoding messages sent on the BitSwap protocol.
type BitSwapMessage interface {
	// Wantlist returns a slice of unique keys that represent data wanted by
	// the sender.
	Wantlist() []Entry

	// Blocks returns a slice of unique blocks.
	Blocks() []blocks.Block
	// BlockPresences returns the list of HAVE / DONT_HAVE in the message
	BlockPresences() []BlockPresence
	// Haves returns the Cids for each HAVE
	Haves() []cid.Cid
	// DontHaves returns the Cids for each DONT_HAVE
	DontHaves() []cid.Cid
	// PendingBytes returns the number of outstanding bytes of data that the
	// engine has yet to send to the client (because they didn't fit in this
	// message)
	PendingBytes() int32

	// AddEntry adds an entry to the Wantlist.
	AddEntry(key cid.Cid, priority int32, wantType pb.Message_Wantlist_WantType, sendDontHave bool) int

	// Cancel adds a CANCEL for the given CID to the message
	// Returns the size of the CANCEL entry in the protobuf
	Cancel(key cid.Cid) int

	// Remove removes any entries for the given CID. Useful when the want
	// status for the CID changes when preparing a message.
	Remove(key cid.Cid)

	// Empty indicates whether the message has any information
	Empty() bool
	// Size returns the size of the message in bytes
	Size() int

	// A full wantlist is an authoritative copy, a 'non-full' wantlist is a patch-set
	Full() bool

	// AddBlock adds a block to the message
	AddBlock(blocks.Block)
	// AddBlockPresence adds a HAVE / DONT_HAVE for the given Cid to the message
	AddBlockPresence(cid.Cid, pb.Message_BlockPresenceType)
	// AddHave adds a HAVE for the given Cid to the message
	AddHave(cid.Cid)
	// AddDontHave adds a DONT_HAVE for the given Cid to the message
	AddDontHave(cid.Cid)
	// SetPendingBytes sets the number of bytes of data that are yet to be sent
	// to the client (because they didn't fit in this message)
	SetPendingBytes(int32)
	Exportable

	Loggable() map[string]interface{}

	// Reset the values in the message back to defaults, so it can be reused
	Reset(bool)

	// Clone the message fields
	Clone() BitSwapMessage

	// Sets compression to message
	SetCompression(pb.Message_CompressionType)
	HasCompression() pb.Message_CompressionType
}

// Exportable is an interface for structures than can be
// encoded in a bitswap protobuf.
type Exportable interface {
	// Note that older Bitswap versions use a different wire format, so we need
	// to convert the message to the appropriate format depending on which
	// version of the protocol the remote peer supports.
	ToProtoV0() *pb.Message
	ToProtoV1() *pb.Message
	ToNetV0(w io.Writer) (int, error)
	ToNetV1(w io.Writer) (int, error)
}

// BlockPresence represents a HAVE / DONT_HAVE for a given Cid
type BlockPresence struct {
	Cid  cid.Cid
	Type pb.Message_BlockPresenceType
}

// Entry is a wantlist entry in a Bitswap message, with flags indicating
// - whether message is a cancel
// - whether requester wants a DONT_HAVE message
// - whether requester wants a HAVE message (instead of the block)
type Entry struct {
	wantlist.Entry
	Cancel       bool
	SendDontHave bool
}

// Get the size of the entry on the wire
func (e *Entry) Size() int {
	epb := e.ToPB()
	return epb.Size()
}

// Get the entry in protobuf form
func (e *Entry) ToPB() pb.Message_Wantlist_Entry {
	return pb.Message_Wantlist_Entry{
		Block:        pb.Cid{Cid: e.Cid},
		Priority:     int32(e.Priority),
		Cancel:       e.Cancel,
		WantType:     e.WantType,
		SendDontHave: e.SendDontHave,
	}
}

var MaxEntrySize = maxEntrySize()

func maxEntrySize() int {
	var maxInt32 int32 = (1 << 31) - 1

	c := cid.NewCidV0(u.Hash([]byte("cid")))
	e := Entry{
		Entry: wantlist.Entry{
			Cid:      c,
			Priority: maxInt32,
			WantType: pb.Message_Wantlist_Have,
		},
		SendDontHave: true, // true takes up more space than false
		Cancel:       true,
	}
	return e.Size()
}

type impl struct {
	full           bool
	wantlist       map[cid.Cid]*Entry
	blocks         map[cid.Cid]blocks.Block
	blockPresences map[cid.Cid]pb.Message_BlockPresenceType
	pendingBytes   int32
	compression    pb.Message_CompressionType
}

// New returns a new, empty bitswap message
func New(full bool) BitSwapMessage {
	return newMsg(full)
}

func newMsg(full bool) *impl {
	return &impl{
		full:           full,
		wantlist:       make(map[cid.Cid]*Entry),
		blocks:         make(map[cid.Cid]blocks.Block),
		blockPresences: make(map[cid.Cid]pb.Message_BlockPresenceType),
	}
}

// Setcompression sets compression in message.
func (m *impl) SetCompression(compression pb.Message_CompressionType) {
	m.compression = compression
}

// HasCompression checks if message has compression.
func (m *impl) HasCompression() pb.Message_CompressionType {
	return m.compression
}

// Clone the message fields
func (m *impl) Clone() BitSwapMessage {
	msg := newMsg(m.full)
	for k := range m.wantlist {
		msg.wantlist[k] = m.wantlist[k]
	}
	for k := range m.blocks {
		msg.blocks[k] = m.blocks[k]
	}
	for k := range m.blockPresences {
		msg.blockPresences[k] = m.blockPresences[k]
	}
	msg.pendingBytes = m.pendingBytes
	msg.compression = m.compression
	return msg
}

// Reset the values in the message back to defaults, so it can be reused
func (m *impl) Reset(full bool) {
	m.full = full
	for k := range m.wantlist {
		delete(m.wantlist, k)
	}
	for k := range m.blocks {
		delete(m.blocks, k)
	}
	for k := range m.blockPresences {
		delete(m.blockPresences, k)
	}
	m.pendingBytes = 0
	m.compression = pb.Message_None
}

var errCidMissing = errors.New("missing cid")

func newMessageFromProto(pbm pb.Message) (BitSwapMessage, error) {
	log.Debugf("[protobuf] Size of full message received: %d", pbm.Size())
	// Check if the message is fully compressed and uncompress.
	if pbm.Compression == pb.Message_Gzip {
		unpbm, err := uncompressMsg(&pbm)
		if err != nil {
			return nil, fmt.Errorf("Error uncompressing message: %w", err)
		}
		unpbm.Compression = pbm.Compression
		pbm = *unpbm
	}

	m := newMsg(pbm.Wantlist.Full)
	// Set the compression that was used for the message. Used to get Size()
	m.compression = pbm.Compression
	for _, e := range pbm.Wantlist.Entries {
		if !e.Block.Cid.Defined() {
			return nil, errCidMissing
		}
		m.addEntry(e.Block.Cid, e.Priority, e.Cancel, e.WantType, e.SendDontHave)
	}

	// deprecated
	for _, d := range pbm.Blocks {
		// In V0 only RawData is sent in blocks so we uncompress rawData and
		// regenerate the CID with newBlock.
		if pbm.Compression == pb.Message_BlockCompression {

			// Initialize compressor.
			compressor := getCompressor(pb.Message_BlockCompression)
			//Uncompress compressed payload.
			d = compressor.Uncompress(d)
		}
		// CIDv0, sha256, protobuf only
		b := blocks.NewBlock(d)
		m.AddBlock(b)
	}
	//

	for _, b := range pbm.GetPayload() {
		pref, err := cid.PrefixFromBytes(b.GetPrefix())
		if err != nil {
			return nil, err
		}

		var c cid.Cid
		var blk *blocks.BasicBlock

		// If blocks compressed we need to regenerate CID from
		if pbm.Compression == pb.Message_BlockCompression {
			// Initialize compressor.
			compressor := getCompressor(pb.Message_BlockCompression)
			// Uncompress compressed payload.
			d := compressor.Uncompress(b.GetData())
			// Generate CID from uncompressed data.
			c, err = pref.Sum(d)
			if err != nil {
				return nil, err
			}
			// Add the block already uncompressed for processing.
			blk, err = blocks.NewBlockWithCid(d, c)
			if err != nil {
				return nil, err
			}
			log.Debugf("[protobuf] Receiving block in message uncompressed: CID=%v; len=%v", blk.Cid(), len(blk.RawData()))
		} else {
			c, err = pref.Sum(b.GetData())
			if err != nil {
				return nil, err
			}
			blk, err = blocks.NewBlockWithCid(b.GetData(), c)
			if err != nil {
				return nil, err
			}

		}

		m.AddBlock(blk)
	}

	for _, bi := range pbm.GetBlockPresences() {
		if !bi.Cid.Cid.Defined() {
			return nil, errCidMissing
		}
		m.AddBlockPresence(bi.Cid.Cid, bi.Type)
	}

	m.pendingBytes = pbm.PendingBytes

	return m, nil
}

func (m *impl) Full() bool {
	return m.full
}

func (m *impl) Empty() bool {
	return len(m.blocks) == 0 && len(m.wantlist) == 0 && len(m.blockPresences) == 0
}

func (m *impl) Wantlist() []Entry {
	out := make([]Entry, 0, len(m.wantlist))
	for _, e := range m.wantlist {
		out = append(out, *e)
	}
	return out
}

func (m *impl) Blocks() []blocks.Block {
	bs := make([]blocks.Block, 0, len(m.blocks))
	for _, block := range m.blocks {
		bs = append(bs, block)
	}
	return bs
}

func (m *impl) BlockPresences() []BlockPresence {
	bps := make([]BlockPresence, 0, len(m.blockPresences))
	for c, t := range m.blockPresences {
		bps = append(bps, BlockPresence{c, t})
	}
	return bps
}

func (m *impl) Haves() []cid.Cid {
	return m.getBlockPresenceByType(pb.Message_Have)
}

func (m *impl) DontHaves() []cid.Cid {
	return m.getBlockPresenceByType(pb.Message_DontHave)
}

func (m *impl) getBlockPresenceByType(t pb.Message_BlockPresenceType) []cid.Cid {
	cids := make([]cid.Cid, 0, len(m.blockPresences))
	for c, bpt := range m.blockPresences {
		if bpt == t {
			cids = append(cids, c)
		}
	}
	return cids
}

func (m *impl) PendingBytes() int32 {
	return m.pendingBytes
}

func (m *impl) SetPendingBytes(pendingBytes int32) {
	m.pendingBytes = pendingBytes
}

func (m *impl) Remove(k cid.Cid) {
	delete(m.wantlist, k)
}

func (m *impl) Cancel(k cid.Cid) int {
	return m.addEntry(k, 0, true, pb.Message_Wantlist_Block, false)
}

func (m *impl) AddEntry(k cid.Cid, priority int32, wantType pb.Message_Wantlist_WantType, sendDontHave bool) int {
	return m.addEntry(k, priority, false, wantType, sendDontHave)
}

func (m *impl) addEntry(c cid.Cid, priority int32, cancel bool, wantType pb.Message_Wantlist_WantType, sendDontHave bool) int {
	e, exists := m.wantlist[c]
	if exists {
		// Only change priority if want is of the same type
		if e.WantType == wantType {
			e.Priority = priority
		}
		// Only change from "dont cancel" to "do cancel"
		if cancel {
			e.Cancel = cancel
		}
		// Only change from "dont send" to "do send" DONT_HAVE
		if sendDontHave {
			e.SendDontHave = sendDontHave
		}
		// want-block overrides existing want-have
		if wantType == pb.Message_Wantlist_Block && e.WantType == pb.Message_Wantlist_Have {
			e.WantType = wantType
		}
		m.wantlist[c] = e
		return 0
	}

	e = &Entry{
		Entry: wantlist.Entry{
			Cid:      c,
			Priority: priority,
			WantType: wantType,
		},
		SendDontHave: sendDontHave,
		Cancel:       cancel,
	}
	m.wantlist[c] = e

	return e.Size()
}

func (m *impl) AddBlock(b blocks.Block) {
	delete(m.blockPresences, b.Cid())
	m.blocks[b.Cid()] = b
}

func (m *impl) AddBlockPresence(c cid.Cid, t pb.Message_BlockPresenceType) {
	if _, ok := m.blocks[c]; ok {
		return
	}
	m.blockPresences[c] = t
}

func (m *impl) AddHave(c cid.Cid) {
	m.AddBlockPresence(c, pb.Message_Have)
}

func (m *impl) AddDontHave(c cid.Cid) {
	m.AddBlockPresence(c, pb.Message_DontHave)
}

func (m *impl) Size() int {
	size := 0

	// TODO: This should be removed as it adds
	// a significant overhead to compress every time.
	// TODO: Update to account for the compression of blocks.
	if m.compression != pb.Message_None && m.compression != pb.Message_BlockCompression {
		pbm := m.ToProtoV1()
		return pbm.Size()
	}

	for _, block := range m.blocks {
		size += len(block.RawData())
	}
	for c := range m.blockPresences {
		size += BlockPresenceSize(c)
	}
	for _, e := range m.wantlist {
		size += e.Size()
	}

	return size
}

func BlockPresenceSize(c cid.Cid) int {
	return (&pb.Message_BlockPresence{
		Cid:  pb.Cid{Cid: c},
		Type: pb.Message_Have,
	}).Size()
}

// FromNet generates a new BitswapMessage from incoming data on an io.Reader.
func FromNet(r io.Reader) (BitSwapMessage, error) {
	reader := msgio.NewVarintReaderSize(r, network.MessageSizeMax)
	return FromMsgReader(reader)
}

// FromPBReader generates a new Bitswap message from a gogo-protobuf reader
func FromMsgReader(r msgio.Reader) (BitSwapMessage, error) {
	msg, err := r.ReadMsg()
	if err != nil {
		return nil, err
	}

	var pb pb.Message
	err = pb.Unmarshal(msg)
	r.ReleaseMsg(msg)
	if err != nil {
		return nil, err
	}

	return newMessageFromProto(pb)
}

func getCompressor(compressionType pb.Message_CompressionType) compression.Compressor {
	var compressor compression.Compressor

	// For now we always use Gzip until we have other compression algorithms.
	// This needs to be enhanced.
	switch compressionType {
	case pb.Message_Gzip:
		compressor = compression.GzipCompressor("full")
	case pb.Message_BlockCompression:
		compressor = compression.GzipCompressor("blocks")
	default:
		compressor = compression.GzipCompressor("full")
	}

	return compressor
}

func compressMsg(in *pb.Message, compressionType pb.Message_CompressionType) *pb.Message {

	// Initialize compressor.
	compressor := getCompressor(compressionType)

	marshIn, err := in.Marshal()
	if err != nil {
		log.Debugf("Error marshalling compressed message: %s", err)
	}

	// Generate compressed payload.
	payload := compressor.Compress(marshIn)
	pbm := new(pb.Message)

	// Set compression in message
	pbm.CompressedPayload = payload
	pbm.Compression = compressionType
	return pbm
}

func uncompressMsg(in *pb.Message) (*pb.Message, error) {
	// Initialize compressor.
	compressor := getCompressor(in.Compression)

	//Uncompress compressed payload.
	uncompPayload := compressor.Uncompress(in.CompressedPayload)
	// Unmarshal into pb message
	var pbm pb.Message
	err := pbm.Unmarshal(uncompPayload)
	if err != nil {
		return nil, fmt.Errorf("Error unmarshalling compressed message_ %w", err)
	}

	return &pbm, nil

}

func (m *impl) ifBlockCompressionCompress(blks []blocks.Block) []blocks.Block {
	if m.compression == pb.Message_BlockCompression {
		compressor := getCompressor(pb.Message_BlockCompression)
		blks = compressor.CompressBlocks(blks)
	}
	return blks
}

func (m *impl) ToProtoV0() *pb.Message {
	pbm := new(pb.Message)
	pbm.Wantlist.Entries = make([]pb.Message_Wantlist_Entry, 0, len(m.wantlist))
	for _, e := range m.wantlist {
		pbm.Wantlist.Entries = append(pbm.Wantlist.Entries, e.ToPB())
	}
	pbm.Wantlist.Full = m.full

	blocks := m.Blocks()
	blocks = m.ifBlockCompressionCompress(blocks)

	pbm.Blocks = make([][]byte, 0, len(blocks))
	for _, b := range blocks {
		pbm.Blocks = append(pbm.Blocks, b.RawData())
	}

	// If compression enabled, compress the full message and create new compressed message.
	// Compression in Payload and that's it.
	if m.compression == pb.Message_BlockCompression {
		pbm.Compression = pb.Message_BlockCompression
	} else if m.compression != pb.Message_None {
		pbm = compressMsg(pbm, m.compression)
	}

	return pbm
}

func (m *impl) ToProtoV1() *pb.Message {
	pbm := new(pb.Message)
	pbm.Wantlist.Entries = make([]pb.Message_Wantlist_Entry, 0, len(m.wantlist))
	for _, e := range m.wantlist {
		pbm.Wantlist.Entries = append(pbm.Wantlist.Entries, e.ToPB())
	}
	pbm.Wantlist.Full = m.full

	blocks := m.Blocks()
	blocks = m.ifBlockCompressionCompress(blocks)
	pbm.Payload = make([]pb.Message_Block, 0, len(blocks))
	for _, b := range blocks {
		pbm.Payload = append(pbm.Payload, pb.Message_Block{
			Data:   b.RawData(),              // Compessed data
			Prefix: b.Cid().Prefix().Bytes(), // CID uncompressed
		})
		log.Debugf("[protobuf] Sending block in message compressed: CID=%v; len=%v", b.Cid(), len(b.RawData()))
	}

	pbm.BlockPresences = make([]pb.Message_BlockPresence, 0, len(m.blockPresences))
	for c, t := range m.blockPresences {
		pbm.BlockPresences = append(pbm.BlockPresences, pb.Message_BlockPresence{
			Cid:  pb.Cid{Cid: c},
			Type: t,
		})
	}

	pbm.PendingBytes = m.PendingBytes()

	// If compression enabled, compress the full message and create new compressed message.
	// Compression in Payload and that's it.
	if m.compression == pb.Message_BlockCompression {
		pbm.Compression = pb.Message_BlockCompression
	} else if m.compression != pb.Message_None {
		pbm = compressMsg(pbm, m.compression)
	}

	log.Debugf("[protobuf] Size of full message sent: %d", pbm.Size())
	return pbm
}

func (m *impl) ToNetV0(w io.Writer) (int, error) {
	return write(w, m.ToProtoV0())
}

func (m *impl) ToNetV1(w io.Writer) (int, error) {
	return write(w, m.ToProtoV1())
}

func write(w io.Writer, m *pb.Message) (int, error) {
	size := m.Size()

	buf := pool.Get(size + binary.MaxVarintLen64)
	defer pool.Put(buf)

	n := binary.PutUvarint(buf, uint64(size))

	written, err := m.MarshalTo(buf[n:])
	if err != nil {
		return written, err
	}
	n += written

	written, err = w.Write(buf[:n])
	return written, err
}

func (m *impl) Loggable() map[string]interface{} {
	blocks := make([]string, 0, len(m.blocks))
	for _, v := range m.blocks {
		blocks = append(blocks, v.Cid().String())
	}
	return map[string]interface{}{
		"blocks": blocks,
		"wants":  m.Wantlist(),
	}
}
