// Package decision implements the decision engine for the bitswap service.
package decision

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/google/uuid"

	rs "github.com/ipfs/go-bitswap/internal/relaysession"
	bssm "github.com/ipfs/go-bitswap/internal/sessionmanager"
	bsmsg "github.com/ipfs/go-bitswap/message"
	pb "github.com/ipfs/go-bitswap/message/pb"
	wl "github.com/ipfs/go-bitswap/wantlist"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	delay "github.com/ipfs/go-ipfs-delay"
	logging "github.com/ipfs/go-log"
	"github.com/ipfs/go-peertaskqueue"
	"github.com/ipfs/go-peertaskqueue/peertask"
	process "github.com/jbenet/goprocess"
	peer "github.com/libp2p/go-libp2p-core/peer"

	pbr "github.com/ipfs/go-bitswap/internal/peerblockregistry"
)

// TODO consider taking responsibility for other types of requests. For
// example, there could be a |cancelQueue| for all of the cancellation
// messages that need to go out. There could also be a |wantlistQueue| for
// the local peer's wantlists. Alternatively, these could all be bundled
// into a single, intelligent global queue that efficiently
// batches/combines and takes all of these into consideration.
//
// Right now, messages go onto the network for four reasons:
// 1. an initial `sendwantlist` message to a provider of the first key in a
//    request
// 2. a periodic full sweep of `sendwantlist` messages to all providers
// 3. upon receipt of blocks, a `cancel` message to all peers
// 4. draining the priority queue of `blockrequests` from peers
//
// Presently, only `blockrequests` are handled by the decision engine.
// However, there is an opportunity to give it more responsibility! If the
// decision engine is given responsibility for all of the others, it can
// intelligently decide how to combine requests efficiently.
//
// Some examples of what would be possible:
//
// * when sending out the wantlists, include `cancel` requests
// * when handling `blockrequests`, include `sendwantlist` and `cancel` as
//   appropriate
// * when handling `cancel`, if we recently received a wanted block from a
//   peer, include a partial wantlist that contains a few other high priority
//   blocks
//
// In a sense, if we treat the decision engine as a black box, it could do
// whatever it sees fit to produce desired outcomes (get wanted keys
// quickly, maintain good relationships with peers, etc).

var log = logging.Logger("engine")

const (
	// outboxChanBuffer must be 0 to prevent stale messages from being sent
	outboxChanBuffer = 0
	// targetMessageSize is the ideal size of the batched payload. We try to
	// pop this much data off the request queue, but it may be a little more
	// or less depending on what's in the queue.
	targetMessageSize = 16 * 1024
	// tagFormat is the tag given to peers associated an engine
	tagFormat = "bs-engine-%s-%s"

	// queuedTagWeight is the default weight for peers that have work queued
	// on their behalf.
	queuedTagWeight = 10

	// maxBlockSizeReplaceHasWithBlock is the maximum size of the block in
	// bytes up to which we will replace a want-have with a want-block
	maxBlockSizeReplaceHasWithBlock = 1024

	// Number of concurrent workers that pull tasks off the request queue
	taskWorkerCount = 8

	// Number of concurrent workers that process requests to the blockstore
	blockstoreWorkerCount = 128

	// Defualt ProvSearchDelay relay sessions
	defaultProvSearchDelay = time.Second

	// Priority used for all blocks in relay sessions
	defualtPriorityRelaySessions = math.MaxInt32 - 5

	// TODO: Add as configuration from the bitswap constructor.
	// Degree session for the relay.
	defaultRelayDegree = 10
)

// Envelope contains a message for a Peer.
type Envelope struct {
	// Peer is the intended recipient.
	Peer peer.ID

	// Message is the payload.
	Message bsmsg.BitSwapMessage

	// A callback to notify the decision queue that the task is complete
	Sent func()
}

// PeerTagger covers the methods on the connection manager used by the decision
// engine to tag peers
type PeerTagger interface {
	TagPeer(peer.ID, string, int)
	UntagPeer(p peer.ID, tag string)
}

// Assigns a specific score to a peer
type ScorePeerFunc func(peer.ID, int)

// ScoreLedger is an external ledger dealing with peer scores.
type ScoreLedger interface {
	// Returns aggregated data communication with a given peer.
	GetReceipt(p peer.ID) *Receipt
	// Increments the sent counter for the given peer.
	AddToSentBytes(p peer.ID, n int)
	// Increments the received counter for the given peer.
	AddToReceivedBytes(p peer.ID, n int)
	// PeerConnected should be called when a new peer connects,
	// meaning the ledger should open accounting.
	PeerConnected(p peer.ID)
	// PeerDisconnected should be called when a peer disconnects to
	// clean up the accounting.
	PeerDisconnected(p peer.ID)
	// Starts the ledger sampling process.
	Start(scorePeer ScorePeerFunc)
	// Stops the sampling process.
	Stop()
}

// Engine manages sending requested blocks to peers.
type Engine struct {
	// peerRequestQueue is a priority queue of requests received from peers.
	// Requests are popped from the queue, packaged up, and placed in the
	// outbox.
	peerRequestQueue *peertaskqueue.PeerTaskQueue

	// FIXME it's a bit odd for the client and the worker to both share memory
	// (both modify the peerRequestQueue) and also to communicate over the
	// workSignal channel. consider sending requests over the channel and
	// allowing the worker to have exclusive access to the peerRequestQueue. In
	// that case, no lock would be required.
	workSignal chan struct{}

	// outbox contains outgoing messages to peers. This is owned by the
	// taskWorker goroutine
	outbox chan (<-chan *Envelope)

	bsm *blockstoreManager
	sm  *bssm.SessionManager

	peerTagger PeerTagger

	tagQueued, tagUseful string

	lock sync.RWMutex // protects the fields immediatly below

	// ledgerMap lists block-related Ledgers by their Partner key.
	ledgerMap map[peer.ID]*ledger

	// an external ledger dealing with peer scores
	scoreLedger ScoreLedger

	ticker *time.Ticker

	taskWorkerLock  sync.Mutex
	taskWorkerCount int

	// maxBlockSizeReplaceHasWithBlock is the maximum size of the block in
	// bytes up to which we will replace a want-have with a want-block
	maxBlockSizeReplaceHasWithBlock int

	sendDontHaves bool

	pbrEnabled bool

	self peer.ID

	// RelaySession to manage relay requests.
	relaySession *rs.RelaySession
	// data structure used to track inspected want messages.
	peerBlockRegistry pbr.PeerBlockRegistry
}

// NewEngine creates a new block sending engine for the given block store
func NewEngine(ctx context.Context, bs bstore.Blockstore, peerTagger PeerTagger, self peer.ID, sm *bssm.SessionManager, peerBlockRegistry pbr.PeerBlockRegistry) *Engine {
	return newEngine(ctx, bs, peerTagger, self, maxBlockSizeReplaceHasWithBlock, nil, sm, peerBlockRegistry)
}

// This constructor is used by the tests
func newEngine(ctx context.Context, bs bstore.Blockstore, peerTagger PeerTagger, self peer.ID,
	maxReplaceSize int, scoreLedger ScoreLedger, sm *bssm.SessionManager, peerBlockRegistry pbr.PeerBlockRegistry) *Engine {

	e := &Engine{
		ledgerMap:                       make(map[peer.ID]*ledger),
		scoreLedger:                     scoreLedger,
		bsm:                             newBlockstoreManager(ctx, bs, blockstoreWorkerCount),
		peerTagger:                      peerTagger,
		outbox:                          make(chan (<-chan *Envelope), outboxChanBuffer),
		workSignal:                      make(chan struct{}, 1),
		ticker:                          time.NewTicker(time.Millisecond * 100),
		maxBlockSizeReplaceHasWithBlock: maxReplaceSize,
		taskWorkerCount:                 taskWorkerCount,
		sendDontHaves:                   true,
		self:                            self,
		sm:                              sm,
		relaySession:                    rs.NewRelaySession(defaultRelayDegree),
		peerBlockRegistry:               peerBlockRegistry,
		pbrEnabled:                      true,
	}
	e.tagQueued = fmt.Sprintf(tagFormat, "queued", uuid.New().String())
	e.tagUseful = fmt.Sprintf(tagFormat, "useful", uuid.New().String())
	e.peerRequestQueue = peertaskqueue.New(
		peertaskqueue.OnPeerAddedHook(e.onPeerAdded),
		peertaskqueue.OnPeerRemovedHook(e.onPeerRemoved),
		peertaskqueue.TaskMerger(newTaskMerger()),
		peertaskqueue.IgnoreFreezing(true))
	return e
}

// SetSendDontHaves indicates what to do when the engine receives a want-block
// for a block that is not in the blockstore. Either
// - Send a DONT_HAVE message
// - Simply don't respond
// Older versions of Bitswap did not respond, so this allows us to simulate
// those older versions for testing.
func (e *Engine) SetSendDontHaves(send bool) {
	e.sendDontHaves = send
}

// Sets the scoreLedger to the given implementation. Should be called
// before StartWorkers().
func (e *Engine) UseScoreLedger(scoreLedger ScoreLedger) {
	e.scoreLedger = scoreLedger
}

// Starts the score ledger. Before start the function checks and,
// if it is unset, initializes the scoreLedger with the default
// implementation.
func (e *Engine) startScoreLedger(px process.Process) {
	if e.scoreLedger == nil {
		e.scoreLedger = NewDefaultScoreLedger()
	}
	e.scoreLedger.Start(func(p peer.ID, score int) {
		if score == 0 {
			e.peerTagger.UntagPeer(p, e.tagUseful)
		} else {
			e.peerTagger.TagPeer(p, e.tagUseful, score)
		}
	})
	px.Go(func(ppx process.Process) {
		<-ppx.Closing()
		e.scoreLedger.Stop()
	})
}

// SetPBR sets the use of peer-block registry. This data structure tracks
// the want messages exchange by neighbors and use this information to send
// a WANT-BLOCK with the WANT-HAVEs when starting a session.
func (e *Engine) SetPBR(pbrEnabled bool) {
	e.pbrEnabled = pbrEnabled
}

// Start up workers to handle requests from other nodes for the data on this node
func (e *Engine) StartWorkers(ctx context.Context, px process.Process) {
	// Start up blockstore manager
	e.bsm.start(px)
	e.startScoreLedger(px)

	for i := 0; i < e.taskWorkerCount; i++ {
		px.Go(func(px process.Process) {
			e.taskWorker(ctx)
		})
	}
}

func (e *Engine) onPeerAdded(p peer.ID) {
	e.peerTagger.TagPeer(p, e.tagQueued, queuedTagWeight)
}

func (e *Engine) onPeerRemoved(p peer.ID) {
	e.peerTagger.UntagPeer(p, e.tagQueued)
}

// WantlistForPeer returns the list of keys that the given peer has asked for
func (e *Engine) WantlistForPeer(p peer.ID) []wl.Entry {
	partner := e.findOrCreate(p)

	partner.lk.Lock()
	entries := partner.wantList.Entries()
	partner.lk.Unlock()

	wl.SortEntries(entries)

	return entries
}

// LedgerForPeer returns aggregated data communication with a given peer.
func (e *Engine) LedgerForPeer(p peer.ID) *Receipt {
	return e.scoreLedger.GetReceipt(p)
}

// Each taskWorker pulls items off the request queue up to the maximum size
// and adds them to an envelope that is passed off to the bitswap workers,
// which send the message to the network.
func (e *Engine) taskWorker(ctx context.Context) {
	defer e.taskWorkerExit()
	for {
		oneTimeUse := make(chan *Envelope, 1) // buffer to prevent blocking
		select {
		case <-ctx.Done():
			return
		case e.outbox <- oneTimeUse:
		}
		// receiver is ready for an outoing envelope. let's prepare one. first,
		// we must acquire a task from the PQ...
		envelope, err := e.nextEnvelope(ctx)
		if err != nil {
			close(oneTimeUse)
			return // ctx cancelled
		}
		oneTimeUse <- envelope // buffered. won't block
		close(oneTimeUse)
	}
}

// taskWorkerExit handles cleanup of task workers
func (e *Engine) taskWorkerExit() {
	e.taskWorkerLock.Lock()
	defer e.taskWorkerLock.Unlock()

	e.taskWorkerCount--
	if e.taskWorkerCount == 0 {
		close(e.outbox)
	}
}

// nextEnvelope runs in the taskWorker goroutine. Returns an error if the
// context is cancelled before the next Envelope can be created.
func (e *Engine) nextEnvelope(ctx context.Context) (*Envelope, error) {
	for {
		// Pop some tasks off the request queue
		p, nextTasks, pendingBytes := e.peerRequestQueue.PopTasks(targetMessageSize)
		for len(nextTasks) == 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-e.workSignal:
				p, nextTasks, pendingBytes = e.peerRequestQueue.PopTasks(targetMessageSize)
			case <-e.ticker.C:
				// When a task is cancelled, the queue may be "frozen" for a
				// period of time. We periodically "thaw" the queue to make
				// sure it doesn't get stuck in a frozen state.
				e.peerRequestQueue.ThawRound()
				p, nextTasks, pendingBytes = e.peerRequestQueue.PopTasks(targetMessageSize)
			}
		}

		// Create a new message
		msg := bsmsg.New(false)

		log.Debugw("Bitswap process tasks", "local", e.self, "taskCount", len(nextTasks))

		// Amount of data in the request queue still waiting to be popped
		msg.SetPendingBytes(int32(pendingBytes))

		// Split out want-blocks, want-haves and DONT_HAVEs
		blockCids := make([]cid.Cid, 0, len(nextTasks))
		blockTasks := make(map[cid.Cid]*taskData, len(nextTasks))
		for _, t := range nextTasks {
			c := t.Topic.(cid.Cid)
			td := t.Data.(*taskData)
			if td.HaveBlock {
				if td.IsWantBlock {
					blockCids = append(blockCids, c)
					blockTasks[c] = td
				} else {
					// Add HAVES to the message
					msg.AddHave(c)
				}
			} else {
				// Add DONT_HAVEs to the message
				msg.AddDontHave(c)
			}
		}

		// Fetch blocks from datastore
		blks, err := e.bsm.getBlocks(ctx, blockCids)
		if err != nil {
			// we're dropping the envelope but that's not an issue in practice.
			return nil, err
		}

		// Remove blocks being sent from relay sessions.
		// for _, b := range blks {
		// 	log.Debugf("local: %v Removing key from relay session as it's being sent.")

		// 	// TODO: Here we should remove all blocks from relay sessions
		// 	// to avoid having data we haven't requested explicitly. I am going to address
		// 	// this in the future, because I could remove blocks that belong exclusively
		// 	// to active relay sessions, but I don't know if a direct session
		// 	// requested it before and I am "garbage collecting" useful information
		// 	// requested by the peer. We could think of a garbage collection strategy
		// 	// for "useless" blocks. All of this needs some additional thought which
		// 	// adds no additional value to our prototype. Let's have it a though in the
		// 	// future.

		// 	// // Retrieve interested sessions for the block.
		// 	// sess := e.sim.InterestedSessions([]cid.Cid{b.Cid()}, []cid.Cid{}, []cid.Cid{})
		// 	// // Check if only relay sessions interested in block.
		// 	// if !e.OnlyRelaySessions(sess) {
		// 	// 	// Remove from datastore.
		// 	// 	e.bsm.bs.DeleteBlock(b.Cid())
		// 	// }
		// }

		for c, t := range blockTasks {
			blk := blks[c]
			// If the block was not found (it has been removed)
			if blk == nil {
				// If the client requested DONT_HAVE, add DONT_HAVE to the message
				if t.SendDontHave {
					msg.AddDontHave(c)
				}
			} else {
				// Add the block to the message
				// log.Debugf("  make evlp %s->%s block: %s (%d bytes)", e.self, p, c, len(blk.RawData()))
				msg.AddBlock(blk)
			}
		}

		// If there's nothing in the message, bail out
		if msg.Empty() {
			e.peerRequestQueue.TasksDone(p, nextTasks...)
			continue
		}

		log.Debugw("Bitswap engine -> msg", "local", e.self, "to", p, "blockCount", len(msg.Blocks()), "presenceCount", len(msg.BlockPresences()), "size", msg.Size())
		return &Envelope{
			Peer:    p,
			Message: msg,
			Sent: func() {
				// Once the message has been sent, signal the request queue so
				// it can be cleared from the queue
				e.peerRequestQueue.TasksDone(p, nextTasks...)

				// Signal the worker to check for more work
				e.signalNewWork()
			},
		}, nil
	}
}

// Outbox returns a channel of one-time use Envelope channels.
func (e *Engine) Outbox() <-chan (<-chan *Envelope) {
	return e.outbox
}

// Peers returns a slice of Peers with whom the local node has active sessions.
func (e *Engine) Peers() []peer.ID {
	e.lock.RLock()
	defer e.lock.RUnlock()

	response := make([]peer.ID, 0, len(e.ledgerMap))

	for _, ledger := range e.ledgerMap {
		response = append(response, ledger.Partner)
	}
	return response
}

// MessageReceived is called when a message is received from a remote peer.
// For each item in the wantlist, add a want-have or want-block entry to the
// request queue (this is later popped off by the workerTasks)
func (e *Engine) MessageReceived(ctx context.Context, p peer.ID, m bsmsg.BitSwapMessage) {
	entries := m.Wantlist()

	if len(entries) > 0 {
		log.Debugw("Bitswap engine <- msg", "local", e.self, "from", p, "entryCount", len(entries))
		for _, et := range entries {
			if !et.Cancel {
				if et.WantType == pb.Message_Wantlist_Have {
					log.Debugw("Bitswap engine <- want-have", "local", e.self, "from", p, "cid", et.Cid, "ttl", et.TTL)
				} else {
					log.Debugw("Bitswap engine <- want-block", "local", e.self, "from", p, "cid", et.Cid, "ttl", et.TTL)
				}
			}
		}
	}

	if m.Empty() {
		log.Infof("received empty message from %s", p)
	}

	newWorkExists := false
	defer func() {
		if newWorkExists {
			e.signalNewWork()
		}
	}()

	// Get block sizes
	wants, cancels := e.splitWantsCancels(entries)
	wantKs := cid.NewSet()
	for _, entry := range wants {
		wantKs.Add(entry.Cid)
	}
	blockSizes, err := e.bsm.getBlockSizes(ctx, wantKs.Keys())
	if err != nil {
		log.Info("aborting message processing", err)
		return
	}

	// Get the ledger for the peer
	l := e.findOrCreate(p)
	l.lk.Lock()
	defer l.lk.Unlock()

	// If the peer sent a full wantlist, replace the ledger's wantlist
	if m.Full() {
		l.wantList = wl.New()
	}

	var activeEntries []peertask.Task

	// For cancels seen
	for _, entry := range cancels {
		// If there is an active relay session
		if e.relaySession.Session != nil {
			log.Debugf("local: %v Removing key from received cancels in relay session registry", e.self)
			e.relaySession.RemoveInterest(entry.Cid, p)
		}

		// Remove cancelled blocks from the queue
		log.Debugw("Bitswap engine <- cancel", "local", e.self, "from", p, "cid", entry.Cid)
		if l.CancelWant(entry.Cid) {
			e.peerRequestQueue.Remove(entry.Cid, p)
		}
	}

	// Track keys of blocks not found in peer with enough TTL to add to the
	// relay session.
	relayKs := rs.NewKeyTracker(p)

	// For each want-have / want-block
	for _, entry := range wants {
		c := entry.Cid
		blockSize, found := blockSizes[entry.Cid]

		// Add each want-have / want-block to the ledger
		l.Wants(c, entry.Priority, entry.WantType, entry.TTL)

		// If enabled, Update peerblockregistry here.
		if e.pbrEnabled {
			e.peerBlockRegistry.UpdateRegistry(p, c, entry.Priority)
		}

		// If the block was not found
		if !found {
			log.Debugw("Bitswap engine: block not found", "local", e.self, "from", p, "cid", entry.Cid, "sendDontHave", entry.SendDontHave)

			if entry.TTL > 0 {
				// If we still have TTL add CIDs to start an relay session for that TTL.
				log.Debugf("Updating tracker to start a new relay session for %s, %d, %d", entry.Cid, entry.TTL, entry.Priority)
				// NOTE: In a previous implementation, the entry.Priority was being introduced
				// in the tracker and this was creating the generation of sessions with
				// individual blocks that were inefficient and ended up not finding all
				// the blocks of the relay session. Because of this, we are using a
				// fixed priority for every block of the relay session. If this ends
				// up cuasing problems we can set relay sessions with blocks with
				// different priorities, but this requires additonal implementation that
				// I am not going to approach in this first implementation.
				// relayKs.updateTracker(entry.TTL, entry.Priority, entry.Cid)

				relayKs.UpdateTracker(entry.Cid, entry.TTL)
			}
			// Only add the task to the queue if the requester wants a DONT_HAVE
			//
			// We always send a DON'T HAVE right away even if we end up triggering
			// and relaySession to minimize duplicates. If the relay session
			// ends up finding the block it will send a HAVE message and update
			// the requested accordingly. DON'T HAVEs are updatable through new
			// BlockPresence information.
			if e.sendDontHaves && entry.SendDontHave {
				newWorkExists = true
				isWantBlock := false
				if entry.WantType == pb.Message_Wantlist_Block {
					isWantBlock = true
				}

				activeEntries = append(activeEntries, peertask.Task{
					Topic:    c,
					Priority: int(entry.Priority),
					Work:     bsmsg.BlockPresenceSize(c),
					Data: &taskData{
						BlockSize:    0,
						HaveBlock:    false,
						IsWantBlock:  isWantBlock,
						SendDontHave: entry.SendDontHave,
					},
				})
			}
		} else {
			// The block was found, add it to the queue
			newWorkExists = true

			isWantBlock := e.sendAsBlock(entry.WantType, blockSize)

			log.Debugw("Bitswap engine: block found", "local", e.self, "from", p, "cid", entry.Cid, "isWantBlock", isWantBlock)

			// entrySize is the amount of space the entry takes up in the
			// message we send to the recipient. If we're sending a block, the
			// entrySize is the size of the block. Otherwise it's the size of
			// a block presence entry.
			entrySize := blockSize
			if !isWantBlock {
				entrySize = bsmsg.BlockPresenceSize(c)
			}
			activeEntries = append(activeEntries, peertask.Task{
				Topic:    c,
				Priority: int(entry.Priority),
				Work:     entrySize,
				Data: &taskData{
					BlockSize:    blockSize,
					HaveBlock:    true,
					IsWantBlock:  isWantBlock,
					SendDontHave: entry.SendDontHave,
				},
			})
		}
	}

	// If there are relayKs
	if len(relayKs.T) > 0 {
		// if the relaySession hasn't been started then start it.
		if e.relaySession.Session == nil {
			// We initially start the relay session with TTL=0, each WANT message should
			// be prepared with the right TTL.
			log.Debugf("local: %v Starting relaySession %v, %v", e.self, p)
			// TODO:Add this as a private attribute of relaySession so that
			// can't be unintentionally modified.
			e.relaySession.Session = e.sm.StartRelaySession(ctx,
				defaultProvSearchDelay,
				delay.Fixed(time.Minute), 0,
				e.relaySession.Registry)
		}
		// Update relaySession with new keys. This function start new GetBlocks
		// if there is no active search for any of the keys.
		e.relaySession.UpdateSession(ctx, relayKs)
	}

	// Push entries onto the request queue
	if len(activeEntries) > 0 {
		e.peerRequestQueue.PushTasks(p, activeEntries...)
	}
}

// Split the want-have / want-block entries from the cancel entries
func (e *Engine) splitWantsCancels(es []bsmsg.Entry) ([]bsmsg.Entry, []bsmsg.Entry) {
	wants := make([]bsmsg.Entry, 0, len(es))
	cancels := make([]bsmsg.Entry, 0, len(es))
	for _, et := range es {
		if et.Cancel {
			cancels = append(cancels, et)
		} else {
			wants = append(wants, et)
		}
	}
	return wants, cancels
}

// ReceiveFrom is called when new blocks are received and added to the block
// store, meaning there may be peers who want those blocks, so we should send
// the blocks to them.
//
// This function also updates the receive side of the ledger.
func (e *Engine) ReceiveFrom(from peer.ID, blks []blocks.Block, haves []cid.Cid) {
	if len(blks) == 0 {
		return
	}

	if from != "" {
		l := e.findOrCreate(from)
		l.lk.Lock()

		// Record how many bytes were received in the ledger
		// TODO: Should we account for relay blocks that will be forwarded also? Because we are doing so.
		for _, blk := range blks {
			log.Debugw("Bitswap engine <- block", "local", e.self, "from", from, "cid", blk.Cid(), "size", len(blk.RawData()))
			e.scoreLedger.AddToReceivedBytes(l.Partner, len(blk.RawData()))
		}

		l.lk.Unlock()
	}

	// Check each peer to see if it wants one of the blocks we received
	work := false
	e.lock.RLock()

	blockSizes := make(map[cid.Cid]int, len(blks))
	for _, blk := range blks {
		// Get the size of each block
		blockSizes[blk.Cid()] = len(blk.RawData())

		// Forward every block received to peers from relay sessions
		// interested in them.
		interestedPeers := e.relaySession.InterestedPeers(blk.Cid())

		for ip := range interestedPeers {
			log.Debugf("local: %v Sending block to relay session: %v", e.self, blk.Cid())
			// Remove interest of peers for which blocks are being sent.
			e.relaySession.RemoveInterest(blk.Cid(), ip)
			work = true
			e.peerRequestQueue.PushTasks(ip, peertask.Task{
				Topic:    blk.Cid(),
				Priority: defualtPriorityRelaySessions,
				Work:     blockSizes[blk.Cid()],
				Data: &taskData{
					BlockSize:    blockSizes[blk.Cid()],
					HaveBlock:    true, // I have the block
					IsWantBlock:  true, // I want to forward the actual block
					SendDontHave: false,
				},
			})
		}
		// The blocks will be removed once they are sent in nextEnvelope to ensure
		// that they are removed from datastore when they are no longer needed.
		// Check notes in that piece of code.
	}

	// Send blocks of to peers that don't belong to relay sessions.
	for _, l := range e.ledgerMap {
		l.lk.RLock()

		for _, b := range blks {
			k := b.Cid()

			if entry, ok := l.WantListContains(k); ok {
				work = true

				blockSize := blockSizes[k]
				isWantBlock := e.sendAsBlock(entry.WantType, blockSize)

				entrySize := blockSize
				if !isWantBlock {
					entrySize = bsmsg.BlockPresenceSize(k)
				}

				e.peerRequestQueue.PushTasks(l.Partner, peertask.Task{
					Topic:    entry.Cid,
					Priority: int(entry.Priority),
					Work:     entrySize,
					Data: &taskData{
						BlockSize:    blockSize,
						HaveBlock:    true,
						IsWantBlock:  isWantBlock,
						SendDontHave: false,
					},
				})
			}
		}
		l.lk.RUnlock()
	}

	// Forward every have message of relay sessions to its source.
	for _, h := range haves {
		work = true
		interestedPeers := e.relaySession.InterestedPeers(h)
		// Entrysize of block presence
		entrySize := bsmsg.BlockPresenceSize(h)
		for ip := range interestedPeers {
			log.Debug("Forwarding HAVE message to source %v", ip)
			e.peerRequestQueue.PushTasks(ip, peertask.Task{
				Topic:    h,
				Priority: defualtPriorityRelaySessions, // We could also track the right priority in relaySession
				Work:     entrySize,
				Data: &taskData{
					// BlockSize:    0, // Not sending a block, forwarding a have mesage. So don't have size.
					HaveBlock:    true,
					IsWantBlock:  false, // I am forwarding a HAVE
					SendDontHave: false,
				},
			})
		}
	}
	e.lock.RUnlock()

	if work {
		e.signalNewWork()
	}
}

// TODO add contents of m.WantList() to my local wantlist? NB: could introduce
// race conditions where I send a message, but MessageSent gets handled after
// MessageReceived. The information in the local wantlist could become
// inconsistent. Would need to ensure that Sends and acknowledgement of the
// send happen atomically

// MessageSent is called when a message has successfully been sent out, to record
// changes.
func (e *Engine) MessageSent(p peer.ID, m bsmsg.BitSwapMessage) {
	l := e.findOrCreate(p)
	l.lk.Lock()
	defer l.lk.Unlock()

	// Remove sent blocks from the want list for the peer
	for _, block := range m.Blocks() {
		e.scoreLedger.AddToSentBytes(l.Partner, len(block.RawData()))
		l.wantList.RemoveType(block.Cid(), pb.Message_Wantlist_Block)
	}

	// Remove sent block presences from the want list for the peer
	for _, bp := range m.BlockPresences() {
		// Don't record sent data. We reserve that for data blocks.
		if bp.Type == pb.Message_Have {
			l.wantList.RemoveType(bp.Cid, pb.Message_Wantlist_Have)
		}
	}
}

// PeerConnected is called when a new peer connects, meaning we should start
// sending blocks.
func (e *Engine) PeerConnected(p peer.ID) {
	e.lock.Lock()
	defer e.lock.Unlock()

	_, ok := e.ledgerMap[p]
	if !ok {
		e.ledgerMap[p] = newLedger(p)
	}

	e.scoreLedger.PeerConnected(p)
}

// PeerDisconnected is called when a peer disconnects.
func (e *Engine) PeerDisconnected(p peer.ID) {
	e.lock.Lock()
	defer e.lock.Unlock()

	delete(e.ledgerMap, p)

	e.scoreLedger.PeerDisconnected(p)
}

// If the want is a want-have, and it's below a certain size, send the full
// block (instead of sending a HAVE)
func (e *Engine) sendAsBlock(wantType pb.Message_Wantlist_WantType, blockSize int) bool {
	isWantBlock := wantType == pb.Message_Wantlist_Block
	return isWantBlock || blockSize <= e.maxBlockSizeReplaceHasWithBlock
}

func (e *Engine) numBytesSentTo(p peer.ID) uint64 {
	return e.LedgerForPeer(p).Sent
}

func (e *Engine) numBytesReceivedFrom(p peer.ID) uint64 {
	return e.LedgerForPeer(p).Recv
}

// ledger lazily instantiates a ledger
func (e *Engine) findOrCreate(p peer.ID) *ledger {
	// Take a read lock (as it's less expensive) to check if we have a ledger
	// for the peer
	e.lock.RLock()
	l, ok := e.ledgerMap[p]
	e.lock.RUnlock()
	if ok {
		return l
	}

	// There's no ledger, so take a write lock, then check again and create the
	// ledger if necessary
	e.lock.Lock()
	defer e.lock.Unlock()
	l, ok = e.ledgerMap[p]
	if !ok {
		l = newLedger(p)
		e.ledgerMap[p] = l
	}
	return l
}

func (e *Engine) signalNewWork() {
	// Signal task generation to restart (if stopped!)
	select {
	case e.workSignal <- struct{}{}:
	default:
	}
}
