package pushmanager

import (
	"context"
	"github.com/ipfs/go-cid"
)

// BitSwapNetwork provides network connectivity for BitSwap sessions.
type PushInterface interface {

	// Push CID to a peer.
	Push(context.Context, cid.Cid) error

	// SetDelegate registers the Reciver to handle messages received from the
	// network.
	//SetDelegate(Receiver)

	//ConnectTo(context.Context, peer.ID) error
}

