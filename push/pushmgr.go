package pushmanager

import (
	"context"
	files "github.com/ipfs/go-ipfs-files"
	format "github.com/ipfs/go-ipld-format"
	dag "github.com/ipfs/go-merkledag"
	unixfile "github.com/ipfs/go-unixfs/file"
	"github.com/pkg/errors"
	//"fmt"
	//"github.com/libp2p/go-libp2p-core/event"
	"io"
	"sync"
	//"sync/atomic"
	"time"

	cid "github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log"
	"github.com/libp2p/go-libp2p-core/helpers"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	//peerstore "github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/libp2p/go-libp2p-core/protocol"
	//"github.com/libp2p/go-libp2p-core/routing"
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
	protocolId 	protocol.ID

	ctx context.Context

	lock sync.Mutex // protects the connection map below
	connMap map[peer.ID]*peerInstance

	addrMu sync.Mutex

	mq chan pushMessage
}

// NewPushManager returns a PushManager supported by underlying IPFS host.
func NewPushManager(parent context.Context, host host.Host, dag format.DAGService) *PushManager {
	pm := &PushManager{
		host:    host,
		dag:	dag,
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

	log.Debug("Push", c)

	entries := make([]cid.Cid, 0, 10)
	entries = append(entries, c)

	select {
	case pm.mq <- &pushSet{targets: entries, from: 0}:
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
	targets []cid.Cid
	from    uint64
}

func (ps *pushSet) handle(pm *PushManager) {
	msg := newMsg()
	// add changes to our wantlist
	for _, t := range ps.targets {
		log.Debug("Push", t)

		msg.AddEntry(t, 1)
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

	if err := msg.ToNetV1(s); err != nil {
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

/*// FindProvidersAsync returns a channel of providers for the given key.
func (bsnet *impl) FindProvidersAsync(ctx context.Context, k cid.Cid, max int) <-chan peer.ID {
	out := make(chan peer.ID, max)
	go func() {
		defer close(out)
		providers := bsnet.routing.FindProvidersAsync(ctx, k, max)
		for info := range providers {
			if info.ID == bsnet.host.ID() {
				continue // ignore self as provider
			}
			bsnet.host.Peerstore().AddAddrs(info.ID, info.Addrs, peerstore.TempAddrTTL)
			select {
			case <-ctx.Done():
				return
			case out <- info.ID:
			}
		}
	}()
	return out
}
*/
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
				log.Debugf("bitswap net handleNewStream from %s error: %s", s.Conn().RemotePeer(), err)
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

	r := files.ToFile(file)
	if r == nil {
		return errors.New("file is not regular")
	}

	var buffer [1024000]byte
	for {
		n, e := r.Read(buffer[:])
		log.Debug("read", n, err)
		if e != nil {
			break
		}
	}
	r.Close()

	return nil
}

func (pm *PushManager) ReceiveMessage(ctx context.Context, p peer.ID, incoming PushMessage) {
	log.Debug("Received", incoming)
	for _, l := range incoming.Pushlist() {
		log.Debug("Received", l.Cid)

		pm.ResolveCid(l.Cid)
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
