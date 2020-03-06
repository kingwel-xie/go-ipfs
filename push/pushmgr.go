package pushmanager

import (
	"context"
	blocks "github.com/ipfs/go-block-format"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	format "github.com/ipfs/go-ipld-format"
	dag "github.com/ipfs/go-merkledag"
	unixfile "github.com/ipfs/go-unixfs/file"

	"io"
	"sync"
	"time"

	cid "github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log"
	"github.com/libp2p/go-libp2p-core/helpers"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	"github.com/libp2p/go-libp2p-core/routing"
	msgio "github.com/libp2p/go-msgio"
	ma "github.com/multiformats/go-multiaddr"
)

const protocolId = "/pushmgr/1.0.0"

var log = logging.Logger("pushmgr")

var sendMessageTimeout = time.Minute * 10

// Peer Instance
type peerInstance struct {
	ref int  	// reference count
	credit int	// credits of this peer
	peer.ID  
}

//
// The PushManager
type PushManager struct {
	host      	host.Host
	routing 	routing.ContentRouting
	dag			format.DAGService
	blockstore	blockstore.Blockstore
	wantlist	map[cid.Cid]struct{}

	protocolId 	protocol.ID

	ctx context.Context

	lock sync.Mutex // protects the connection map below
	connMap map[peer.ID]*peerInstance

	// ledgerMap lists Ledgers by their Partner key.
	ledgerMap map[peer.ID]*ledger

	addrMu sync.Mutex

	mq chan pushMessage
}

// NewPushManager returns a PushManager supported by underlying IPFS host.
func NewPushManager(parent context.Context, host host.Host, rt routing.Routing, dag format.DAGService, bs blockstore.Blockstore) *PushManager {
	pm := &PushManager{
		host:    host,
		routing:	rt,
		dag:	dag,
		blockstore:		bs,
		wantlist:	make(map[cid.Cid]struct{}),
		protocolId:       protocolId,
		connMap:	make(map[peer.ID]*peerInstance, 100),
		ledgerMap:	make(map[peer.ID]*ledger, 100),
		ctx:	parent,
		mq:	make(chan pushMessage),
	}

	host.SetStreamHandler(protocolId, pm.handleNewStream)

	host.Network().Notify((*netNotifiee)(pm))

	go pm.run()

	return pm
}

// Implement PushInterface
func (pm *PushManager) Push(ctx context.Context, c cid.Cid) error {
	return pm.PushMany(ctx, []cid.Cid{c})
}

func (pm *PushManager) PushAll(pushlist []cid.Cid, allowlist []cid.Cid, bs []blocks.Block) {
	msg := newMsg()

	for _, e := range pushlist {
		msg.AddEntry(e, 1)
	}

	for _, e := range allowlist {
		msg.addAllowEntry(e, 1)
	}

	for _, b := range bs {
		msg.AddBlock(b)
	}

	go pm.pushToPeer(pm.ctx, msg)
}

// Implement PushInterface
func (pm *PushManager) PushMany(ctx context.Context, pushlist []cid.Cid) error {
	entries := make([]Entry, 0, len(pushlist))
	for _, c := range pushlist {
		log.Debug("push ", c)
		entries = append(entries, Entry{
			Cid:      c,
			Priority: 0,
		})
	}

	select {
	case pm.mq <- &pushSet{entries: entries, from: 0}:
	case <-pm.ctx.Done():
	case <-ctx.Done():
	}

	return nil
}

func (pm *PushManager) run() {
	// NOTE: Do not open any streams or connections from anywhere in this
	// event loop. Really, just don't do anything likely to block.
	for {
		select {
		case message := <-pm.mq:
			message.handle(pm)
		case <-pm.ctx.Done():
			return
		}
	}
}

type pushMessage interface {
	handle(pm *PushManager)
}

type pushSet struct {
	entries []Entry
	targets []peer.ID
	from    uint64
}

func (ps *pushSet) handle(pm *PushManager) {
	msg := newMsg()
	// add changes to our wantlist
	for _, e := range ps.entries {
		log.Debug("Push ", e.Cid)

		msg.AddEntry(e.Cid, e.Priority)
	}

	go pm.pushToPeer(pm.ctx, msg)
}

func (pm *PushManager) pushToPeer(ctx context.Context, msg PushMessage) {

	//pm.getMostWantedPeer()
	if len(pm.connMap) == 0 {
		log.Warn("Peer connection unavailable")
		return
	}

	var pid peer.ID
	for k,_ := range pm.connMap {
		pid = k
		break
	}

	pm.SendMessage(pm.ctx, pid, msg)
}

func (pm *PushManager) msgToStream(ctx context.Context, s network.Stream, msg PushMessage) error {
	deadline := time.Now().Add(sendMessageTimeout)
	if dl, ok := ctx.Deadline(); ok {
		deadline = dl
	}

	if err := s.SetWriteDeadline(deadline); err != nil {
		log.Warn("error setting deadline: %s", err)
	}

	if err := msg.Send(s); err != nil {
		log.Debugf("error: %s", err)
		return err
	}

	if err := s.SetWriteDeadline(time.Time{}); err != nil {
		log.Warn("error resetting deadline: %s", err)
	}
	return nil
}

func (pm *PushManager) SendMessage(ctx context.Context,	p peer.ID, outgoing PushMessage) error {
	s, err := pm.host.NewStream(ctx, p, pm.protocolId)
	if err != nil {
		return err
	}

	if err = pm.msgToStream(ctx, s, outgoing); err != nil {
		_ = s.Reset()
		return err
	}
	//atomic.AddUint64(&bsnet.stats.MessagesSent, 1)

	// TODO(https://github.com/libp2p/go-libp2p-net/issues/28): Avoid this goroutine.
	//nolint
	go helpers.AwaitEOF(s)
	return s.Close()

}

// Provide provides the key to the network
func (pm *PushManager) Provide(ctx context.Context, k cid.Cid) error {
	return pm.routing.Provide(ctx, k, true)
}

// ledger lazily instantiates a ledger
func (pm *PushManager) findOrCreateLedger(p peer.ID) *ledger {
	pm.lock.Lock()
	defer pm.lock.Unlock()
	l, ok := pm.ledgerMap[p]
	if !ok {
		l = newLedger(p)
		pm.ledgerMap[p] = l
	}
	return l
}

// handleNewStream receives a new stream from the network.
func (pm *PushManager) handleNewStream(s network.Stream) {
	defer s.Close()
/*
	if bsnet.receiver == nil {
		_ = s.Reset()
		return
	}*/

	reader := msgio.NewVarintReaderSize(s, network.MessageSizeMax)
	for {
		received, err := FromMsgReader(reader)
		if err != nil {
			if err != io.EOF {
				_ = s.Reset()
				log.Debugf("pushManager net handleNewStream from %s error: %s", s.Conn().RemotePeer(), err)
			}
			return
		}

		p := s.Conn().RemotePeer()
		ctx := context.Background()
		log.Debugf("pushmanager net handleNewStream from %s", s.Conn().RemotePeer())
		pm.ReceiveMessage(ctx, p, received)
		//atomic.AddUint64(&bsnet.stats.MessagesRecvd, 1)
	}
}

func (pm *PushManager) ResolveCid(Cid cid.Cid) error {
	ctx, cancel := context.WithCancel(pm.ctx)
	defer cancel()

	dagRO := dag.NewReadOnlyDagService(dag.NewSession(ctx, pm.dag))

	node, err := dagRO.Get(ctx, Cid)
	if err != nil {
		return err
	}

	file, err := unixfile.NewUnixfsFile(ctx, dagRO, node)

	size, err := file.Size()
	if err != nil {
		return err
	}
	log.Debug("file size", size)

	return nil
}

func (pm *PushManager) ReceiveMessage(ctx context.Context, p peer.ID, incoming PushMessage) {
	pushlist := incoming.Pushlist()
	wants := make([]cid.Cid, 0, len(pushlist))
	for _, l := range pushlist {
		log.Debug("Received publist req ", l.Cid)

		has, err := pm.blockstore.Has(l.Cid)
		if err != nil {
			log.Debug(err)
			break
		}

		if has == false {
			log.Debug("allow publist ", l.Cid)
			pm.wantlist[l.Cid] = struct{}{}
			wants = append(wants, l.Cid)
		}
	}

	l := pm.findOrCreateLedger(p)
	allowlist := incoming.Allowlist()
	sBlocks := make([]blocks.Block, 0, len(allowlist))
	for _, allow := range allowlist {
		log.Debug("Received allow ", allow.Cid)

		b, err := pm.blockstore.Get(allow.Cid)
		if err != nil {
			continue
		}
		log.Debug("push block  ", allow.Cid)
		sBlocks = append(sBlocks, b)
		// update ledger
		l.SentBytes(len(b.RawData()))
	}

	if len(wants) > 0  || len(sBlocks) > 0 {
		pm.PushAll(nil, wants, sBlocks)
	}

	rblocks := make([]blocks.Block, 0, len(incoming.Blocks()))
	for _, block := range incoming.Blocks() {
		log.Debug("Received block  ", block.Cid)
		_, ok := pm.wantlist[block.Cid()]
		if ok {
			delete(pm.wantlist, block.Cid())
			rblocks = append(rblocks, block)

			// update ledger
			l.ReceivedBytes(len(block.RawData()))

			go pm.Provide(ctx, block.Cid())
		}
	}
	if len(rblocks) > 0 {
		pm.blockstore.PutMany(rblocks)
	}
}

func (pm *PushManager) PeerConnected(remotePeer peer.ID) {
	log.Debug("Peer connected", remotePeer)
	pm.lock.Lock()
	defer pm.lock.Unlock()

	c, ok := pm.connMap[remotePeer]
	if !ok {
		pm.connMap[remotePeer] = &peerInstance{
			ref:    1,
			credit: 0,
			ID:     remotePeer,
		}
	} else {
		c.ref ++
	}

	l, ok := pm.ledgerMap[remotePeer]
	if !ok {
		l = newLedger(remotePeer)
		pm.ledgerMap[remotePeer] = l
	}
	l.lk.Lock()
	defer l.lk.Unlock()
	l.ref++
}

func (pm *PushManager) PeerDisconnected(remotePeer peer.ID) {
	log.Debug("Peer disconnected", remotePeer)
	pm.lock.Lock()
	defer pm.lock.Unlock()

	c, ok := pm.connMap[remotePeer]
	if ok {
		c.ref--
		if c.ref <= 0 {
			delete(pm.connMap, remotePeer)
		}
	}
}

/*
func (bsnet *impl) ConnectionManager() connmgr.ConnManager {
	return bsnet.host.ConnManager()
}

func (bsnet *impl) Stats() Stats {
	return Stats{
		MessagesRecvd: atomic.LoadUint64(&bsnet.stats.MessagesRecvd),
		MessagesSent:  atomic.LoadUint64(&bsnet.stats.MessagesSent),
	}
}
*/
type netNotifiee PushManager

func (nn *netNotifiee) Manager() *PushManager {
	return (*PushManager)(nn)
}

func (nn *netNotifiee) Connected(n network.Network, v network.Conn) {
	nn.Manager().PeerConnected(v.RemotePeer())
}

func (nn *netNotifiee) Disconnected(n network.Network, v network.Conn) {
	nn.Manager().PeerDisconnected(v.RemotePeer())
}

func (nn *netNotifiee) OpenedStream(n network.Network, v network.Stream) {}
func (nn *netNotifiee) ClosedStream(n network.Network, v network.Stream) {}
func (nn *netNotifiee) Listen(n network.Network, a ma.Multiaddr)         {}
func (nn *netNotifiee) ListenClose(n network.Network, a ma.Multiaddr)    {}
