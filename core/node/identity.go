package node

import (
	"fmt"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipfs/go-ipfs/core/node/helpers"
	pushmanager "github.com/ipfs/go-ipfs/push"
	format "github.com/ipfs/go-ipld-format"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/routing"
	"go.uber.org/fx"

	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
)

func PeerID(id peer.ID) func() peer.ID {
	return func() peer.ID {
		return id
	}
}

// PrivateKey loads the private key from config
func PrivateKey(sk crypto.PrivKey) func(id peer.ID) (crypto.PrivKey, error) {
	return func(id peer.ID) (crypto.PrivKey, error) {
		id2, err := peer.IDFromPrivateKey(sk)
		if err != nil {
			return nil, err
		}

		if id2 != id {
			return nil, fmt.Errorf("private key in config does not match id: %s != %s", id, id2)
		}
		return sk, nil
	}
}

func PushInterface(mctx helpers.MetricsCtx, lc fx.Lifecycle, host host.Host, rt routing.Routing, dag format.DAGService, bs blockstore.GCBlockstore) pushmanager.PushInterface {
	return pushmanager.NewPushManager(helpers.LifecycleCtx(mctx, lc), host, rt, dag, bs)
}

func PushInterface2(host host.Host) func() (pushmanager.PushInterface, error) {
	return func() (pushmanager.PushInterface, error) {
		//pm := pushmanager.NewPushManager(host)
		return nil, nil
	}
}
