package pushmanager

import (
	"context"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	files "github.com/ipfs/go-ipfs-files"
	format "github.com/ipfs/go-ipld-format"
	dag "github.com/ipfs/go-merkledag"
	unixfile "github.com/ipfs/go-unixfs/file"
	"io/ioutil"
	"sort"

	"errors"
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
	dag			format.DAGService
	blockstore	blockstore.Blockstore

	protocolId 	protocol.ID

	ctx context.Context

	lock sync.Mutex // protects the connection map below
	connMap map[peer.ID]*peerInstance

	mq chan pushMessage
}

// NewPushManager returns a PushManager supported by underlying IPFS host.
func NewPushManager(parent context.Context, host host.Host, dag format.DAGService, bs blockstore.Blockstore) *PushManager {
	pm := &PushManager{
		host:    host,
		dag:	dag,
		blockstore:		bs,
		protocolId:       protocolId,
		connMap:	make(map[peer.ID]*peerInstance, 100),
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
	return pm.PushMany(ctx, []cid.Cid{c}, false)
}

// Implement PushInterface
func (pm *PushManager) PushMany(ctx context.Context, pushlist []cid.Cid, accepted bool) error {
	entries := make([]Entry, 0, len(pushlist))
	for _, c := range pushlist {
		log.Debug("push ", c)
		entries = append(entries, Entry{
			Cid:c,
			Priority:0,
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

type peerData struct {
	latency	time.Duration
	pid 	peer.ID
}

type peersData []peerData

func (pd peersData) Len() int {
	return len(pd)
}

func (pd peersData) Less(i, j int) bool {
	return pd[i].latency < pd[j].latency
}

func (pd peersData) Swap(i, j int) {
	pd[i], pd[j] = pd[j], pd[i]
}

func (pm *PushManager) pushToPeer(ctx context.Context, msg PushMessage) {
	//pm.getMostWantedPeer()
	if len(pm.connMap) == 0 {
		log.Warn("Peer connection unavailable")
		return
	}

	peers := make(peersData, 0)
	for pid, _ := range pm.connMap {
		latency := pm.host.Peerstore().LatencyEWMA(pid)
		peers = append(peers, peerData{
			latency: latency,
			pid:     pid,
		})
	}

	sort.Sort(peers)

	for _, pd := range peers {
		accepted, err := pm.SendAndRecvMsg(ctx, pd.pid, msg)
		if accepted || err != nil {
			return
		}
	}
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

func (pm *PushManager) SendAndRecvMsg(ctx context.Context,	p peer.ID, outgoing PushMessage) (bool, error) {
	s, err := pm.host.NewStream(ctx, p, pm.protocolId)
	if err != nil {
		return false, err
	}

	if err = pm.msgToStream(ctx, s, outgoing); err != nil {
		_ = s.Reset()
		return false, err
	}

	accepted, err := Recv(s)
	if err != nil {
		return false, err
	}
	log.Debugf("peer %s accept %t\n", p.Pretty(), accepted)
	//atomic.AddUint64(&bsnet.stats.MessagesSent, 1)

	// TODO(https://github.com/libp2p/go-libp2p-net/issues/28): Avoid this goroutine.
	//nolint
	go helpers.AwaitEOF(s)
	return accepted, s.Close()

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
		pm.ReceiveMessage(ctx, p, s, received)
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
	if err != nil {
		return err
	}
	defer file.Close()

	size, err := file.Size()
	if err != nil {
		return err
	}
	log.Debug("file size: ", size)

	r := files.ToFile(file)
	if r == nil {
		return errors.New("file is not regular")
	}

	ioutil.ReadAll(r)

	return nil
}

func (pm *PushManager) ReceiveMessage(ctx context.Context, p peer.ID, s network.Stream, incoming PushMessage) {
	pushlist := incoming.Pushlist()
	wants := make([]cid.Cid, 0, len(pushlist))
	for _, l := range pushlist {
		log.Debug("Received publist ", l.Cid)

		has, err := pm.blockstore.Has(l.Cid)
		if err != nil {
			log.Debug(err)
			break
		}

		if has == false {
			log.Debug("accept publist ", l.Cid)
			wants = append(wants, l.Cid)
			go pm.ResolveCid(l.Cid)
		} else {
			log.Debug("Don't accept, block is existed in blockstore")
		}
	}

	accepted := len(wants) > 0
	pm.msgToStream(ctx, s, resp{accepted:accepted})
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
