package mbs

import (
    "github.com/mengwanguc/go-ds-motr/mio"
    "github.com/mengwanguc/go-ds-motr/mds"
    "github.com/filecoin-project/lotus/blockstore"
    ipfsBlockstore "github.com/ipfs/go-ipfs-blockstore"
)


// NewBlockstore returns a default Blockstore implementation
// using the provided datastore.Batching backend.
func NewMotrBlockstore(conf mio.Config, indexID string) (blockstore.Blockstore, error) {
    motrds, err := mds.Open(conf, indexID)
//    return blockstore.FromDatastore(motrds), err
    return blockstore.Adapt(ipfsBlockstore.NewBlockstore(motrds)), err
}



