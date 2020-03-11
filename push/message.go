package pushmanager

import (
	"encoding/binary"
	"fmt"
	pb "github.com/ipfs/go-ipfs/push/pb"
	"io"

	cid "github.com/ipfs/go-cid"
	pool "github.com/libp2p/go-buffer-pool"
	msgio "github.com/libp2p/go-msgio"

	"github.com/libp2p/go-libp2p-core/network"
)

// PushMessage is the basic interface for interacting building, encoding,
// and decoding messages sent on the PushManager.
type PushMessage interface {
	// Pushlist returns a slice of unique keys that represent data wanted by
	// the sender.
	Pushlist() []Entry

	Send(w io.Writer) error

	Loggable() map[string]interface{}
}

type impl struct {
	pushlist map[cid.Cid]*Entry
}

// New returns a new, empty bitswap message
func New() PushMessage {
	return newMsg()
}

func newMsg() *impl {
	return &impl{
		pushlist: make(map[cid.Cid]*Entry),
	}
}

type Entry struct {
	Cid      cid.Cid
	Priority int
}

func newMessageFromProto(pbm pb.Message) (PushMessage, error) {
	m := newMsg()
	for _, e := range pbm.Pushlist {
		c, err := cid.Cast([]byte(e.Block))
		if err != nil {
			return nil, fmt.Errorf("incorrectly formatted cid in wantlist: %s", err)
		}
		m.addEntry(c, int(e.Priority))
	}

	return m, nil
}

func (m *impl) Pushlist() []Entry {
	out := make([]Entry, 0, len(m.pushlist))
	for _, e := range m.pushlist {
		out = append(out, *e)
	}
	return out
}

func (m *impl) AddEntry(k cid.Cid, priority int) {
	m.addEntry(k, priority)
}

func (m *impl) addEntry(c cid.Cid, priority int) {
	e, exists := m.pushlist[c]
	if exists {
		e.Priority = priority
	} else {
		m.pushlist[c] = &Entry{
			Cid:      c,
			Priority: priority,
		}
	}
}

// FromNet generates a new BitswapMessage from incoming data on an io.Reader.
func FromNet(r io.Reader) (PushMessage, error) {
	reader := msgio.NewVarintReaderSize(r, network.MessageSizeMax)
	return FromMsgReader(reader)
}

// FromPBReader generates a new Bitswap message from a gogo-protobuf reader
func FromMsgReader(r msgio.Reader) (PushMessage, error) {
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

func (m *impl) encode() *pb.Message {
	pbm := new(pb.Message)
	pbm.Pushlist = make([]pb.Message_Entry, 0, len(m.pushlist))
	for _, e := range m.pushlist {
		pbm.Pushlist = append(pbm.Pushlist, pb.Message_Entry{
			Block:    e.Cid.Bytes(),
			Priority: int32(e.Priority),
		})
	}
	return pbm
}

func (m *impl) Send(w io.Writer) error {
	return write(w, m.encode())
}

func write(w io.Writer, m *pb.Message) error {
	size := m.Size()

	buf := pool.Get(size + binary.MaxVarintLen64)
	defer pool.Put(buf)

	n := binary.PutUvarint(buf, uint64(size))

	written, err := m.MarshalTo(buf[n:])
	if err != nil {
		return err
	}
	n += written

	_, err = w.Write(buf[:n])
	return err
}

func (m *impl) Loggable() map[string]interface{} {
	return map[string]interface{}{
		"push":  m.Pushlist(),
	}
}

type resp struct {
	accepted bool
}

func (r resp) Accept() bool {
	return r.accepted
}

func (r resp) Pushlist() []Entry {
	return nil
}

func (r *resp) encode() *pb.RespMsg {
	return &pb.RespMsg{Accepted: r.accepted}
}

func (r *resp) write(w io.Writer, m *pb.RespMsg) error {
	size := m.Size()

	buf := pool.Get(size + binary.MaxVarintLen64)
	defer pool.Put(buf)

	n := binary.PutUvarint(buf, uint64(size))

	written, err := m.MarshalTo(buf[n:])
	if err != nil {
		return err
	}
	n += written

	_, err = w.Write(buf[:n])
	return err
}

func (r resp) Send(w io.Writer) error {
	return r.write(w, r.encode())
}

func (r *resp) RecvRsp(s network.Stream) error {
	reader := msgio.NewVarintReaderSize(s, network.MessageSizeMax)
	msg, err := reader.ReadMsg()
	if err != nil {
		if err != io.EOF {
			_ = s.Reset()
			log.Debugf("pushManager net handleNewStream from %s error: %s", s.Conn().RemotePeer(), err)
		}
		return err
	}

	var pb pb.RespMsg
	err = pb.Unmarshal(msg)
	if err != nil {
		return err
	}
	reader.ReleaseMsg(msg)

	r.accepted = pb.Accepted

	return nil
}

func (r resp) Loggable() map[string]interface{} {
	return map[string]interface{}{
		"accepted": r.accepted,
	}
}
