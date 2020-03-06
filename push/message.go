package pushmanager

import (
	"encoding/binary"
	"fmt"
	blocks "github.com/ipfs/go-block-format"
	"io"

	pb "github.com/ipfs/go-ipfs/push/pb"

	cid "github.com/ipfs/go-cid"
	pool "github.com/libp2p/go-buffer-pool"
	msgio "github.com/libp2p/go-msgio"

	"github.com/libp2p/go-libp2p-core/network"
)

// BitSwapMessage is the basic interface for interacting building, encoding,
// and decoding messages sent on the BitSwap protocol.
type PushMessage interface {
	// Wantlist returns a slice of unique keys that represent data wanted by
	// the sender.
	Pushlist() []Entry

	Allowlist() []Entry

	// Blocks returns a slice of unique blocks.
	Blocks() []blocks.Block

	Send(w io.Writer) error

	Loggable() map[string]interface{}
}

type impl struct {
	pushlist map[cid.Cid]*Entry
	allowlist map[cid.Cid]*Entry
	blocks   map[cid.Cid]blocks.Block
}

// New returns a new, empty bitswap message
func New() PushMessage {
	return newMsg()
}

func newMsg() *impl {
	return &impl{
		pushlist: make(map[cid.Cid]*Entry),
		allowlist: make(map[cid.Cid]*Entry),
		blocks: make(map[cid.Cid]blocks.Block),
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

	for _, e := range pbm.Allowlist {
		c, err := cid.Cast([]byte(e.Block))
		if err != nil {
			return nil, fmt.Errorf("incorrectly formatted cid in wantlist: %s", err)
		}
		m.addAllowEntry(c, int(e.Priority))
	}

	for _, b := range pbm.GetPayload() {
		pref, err := cid.PrefixFromBytes(b.GetPrefix())
		if err != nil {
			return nil, err
		}

		c, err := pref.Sum(b.GetData())
		if err != nil {
			return nil, err
		}

		blk, err := blocks.NewBlockWithCid(b.GetData(), c)
		if err != nil {
			return nil, err
		}

		m.AddBlock(blk)
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

func (m *impl) Allowlist() []Entry {
	out := make([]Entry, 0, len(m.allowlist))
	for _, e := range m.allowlist {
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

func (m *impl) addAllowEntry(c cid.Cid, priority int) {
	e, exists := m.allowlist[c]
	if exists {
		e.Priority = priority
	} else {
		m.allowlist[c] = &Entry{
			Cid:      c,
			Priority: priority,
		}
	}
}

func (m *impl) Blocks() []blocks.Block {
	bs := make([]blocks.Block, 0, len(m.blocks))
	for _, block := range m.blocks {
		bs = append(bs, block)
	}
	return bs
}

func (m *impl) AddBlock(b blocks.Block) {
	m.blocks[b.Cid()] = b
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

	pbm.Allowlist = make([]pb.Message_Entry, 0, len(m.allowlist))
	for _, e := range m.allowlist {
		pbm.Allowlist = append(pbm.Allowlist, pb.Message_Entry{
			Block:    e.Cid.Bytes(),
			Priority: int32(e.Priority),
		})
	}

	blocks := m.Blocks()
	pbm.Payload = make([]pb.Message_Block, 0, len(blocks))
	for _, b := range blocks {
		pbm.Payload = append(pbm.Payload, pb.Message_Block{
			Data:   b.RawData(),
			Prefix: b.Cid().Prefix().Bytes(),
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
