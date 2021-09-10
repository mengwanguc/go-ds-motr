package mbs

import (
    "io"
    "testing"
    blocks "github.com/ipfs/go-block-format"
    "context"
    "fmt"

    "github.com/mengwanguc/go-ds-motr/mio"
    "github.com/filecoin-project/lotus/blockstore"
    "github.com/ipfs/go-cid"
    u "github.com/ipfs/go-ipfs-util"
    "github.com/stretchr/testify/require"
)


// returns blockstore
func newBS(t *testing.T) (blockstore.Blockstore) {
    config := mio.Config{
        LocalEP:    "172.31.36.67@tcp:12345:33:1000",
        HaxEP:      "172.31.36.67@tcp:12345:34:1",
        Profile:    "0x7000000000000001:0",
        ProcFid:    "0x7200000000000001:64",
        TraceOn:    false,
        Verbose:    false,
        ThreadsN:   1,
    }
    indexID := "0x7800000000000001:123456701"
    motrbs, err := NewMotrBlockstore(config, indexID)
    if err != nil {
        t.Fatal("Failed to open index.. error: ", err)
    }
    return motrbs

}


func TestSuite(t *testing.T) {
    motrbs := newBS(t)

    t.Run("GetWhenKeyNotPresent", func(t *testing.T) {
        testGetWhenKeyNotPresent(t, motrbs)
    })

    t.Run("GetWhenKeyIsNil", func(t *testing.T) {
        testGetWhenKeyIsNil(t, motrbs)
    })
    t.Run("testPutThenGetBlock", func(t *testing.T) {
        testPutThenGetBlock(t, motrbs)
    })
    t.Run("testHas", func(t *testing.T) {
        testHas(t, motrbs)
    })
    t.Run("testCidv0v1", func(t *testing.T) {
        testCidv0v1(t, motrbs)
    })
    t.Run("testPutThenGetSizeBlock", func(t *testing.T) {
        testPutThenGetSizeBlock(t, motrbs)
    })
    t.Run("testAllKeysSimple", func(t *testing.T) {
        testAllKeysSimple(t, motrbs)
    })
    t.Run("testAllKeysRespectsContext", func(t *testing.T) {
        testAllKeysRespectsContext(t, motrbs)
    })
    t.Run("testDoubleClose", func(t *testing.T) {
        testDoubleClose(t, motrbs)
    })
    t.Run("testPutMany", func(t *testing.T) {
        testPutMany(t, motrbs)
    })
    t.Run("testDelete", func(t *testing.T) {
        testDelete(t, motrbs)
    })

}


func testGetWhenKeyNotPresent(t *testing.T, bs blockstore.Blockstore) {
	if c, ok := bs.(io.Closer); ok {
		defer func() { require.NoError(t, c.Close()) }()
	}

	c := cid.NewCidV0(u.Hash([]byte("stuff")))
	bl, err := bs.Get(c)
	require.Nil(t, bl)
	require.Equal(t, blockstore.ErrNotFound, err)
}



func testGetWhenKeyIsNil(t *testing.T, bs blockstore.Blockstore) {
	if c, ok := bs.(io.Closer); ok {
		defer func() { require.NoError(t, c.Close()) }()
	}

	_, err := bs.Get(cid.Undef)
	require.Equal(t, blockstore.ErrNotFound, err)
}


func testPutThenGetBlock(t *testing.T, bs blockstore.Blockstore) {
	if c, ok := bs.(io.Closer); ok {
		defer func() { require.NoError(t, c.Close()) }()
	}

	orig := blocks.NewBlock([]byte("some data"))

	err := bs.Put(orig)
	require.NoError(t, err)

	fetched, err := bs.Get(orig.Cid())
	require.NoError(t, err)
	require.Equal(t, orig.RawData(), fetched.RawData())
}

func testHas(t *testing.T, bs blockstore.Blockstore) {
	if c, ok := bs.(io.Closer); ok {
		defer func() { require.NoError(t, c.Close()) }()
	}

	orig := blocks.NewBlock([]byte("some data"))

	err := bs.Put(orig)
	require.NoError(t, err)

	ok, err := bs.Has(orig.Cid())
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = bs.Has(blocks.NewBlock([]byte("another thing")).Cid())
	require.NoError(t, err)
	require.False(t, ok)
}

func testCidv0v1(t *testing.T, bs blockstore.Blockstore) {
	if c, ok := bs.(io.Closer); ok {
		defer func() { require.NoError(t, c.Close()) }()
	}

	orig := blocks.NewBlock([]byte("some data"))

	err := bs.Put(orig)
	require.NoError(t, err)

	fetched, err := bs.Get(cid.NewCidV1(cid.DagProtobuf, orig.Cid().Hash()))
	require.NoError(t, err)
	require.Equal(t, orig.RawData(), fetched.RawData())
}

func testPutThenGetSizeBlock(t *testing.T, bs blockstore.Blockstore) {
	if c, ok := bs.(io.Closer); ok {
		defer func() { require.NoError(t, c.Close()) }()
	}

	block := blocks.NewBlock([]byte("some data"))
	missingBlock := blocks.NewBlock([]byte("missingBlock"))
//	emptyBlock := blocks.NewBlock([]byte{})

	err := bs.Put(block)
	require.NoError(t, err)

	blockSize, err := bs.GetSize(block.Cid())
	require.NoError(t, err)
	require.Len(t, block.RawData(), blockSize)

/*	err = bs.Put(emptyBlock)
	require.NoError(t, err)

	emptySize, err := bs.GetSize(emptyBlock.Cid())
	require.NoError(t, err)
	require.Zero(t, emptySize)
*/

	missingSize, err := bs.GetSize(missingBlock.Cid())
	require.Equal(t, blockstore.ErrNotFound, err)
	require.Equal(t, -1, missingSize)
}

func testAllKeysSimple(t *testing.T, bs blockstore.Blockstore) {
	if c, ok := bs.(io.Closer); ok {
		defer func() { require.NoError(t, c.Close()) }()
	}

	cleanBlockstore(bs)

	keys := insertBlocks(t, bs, 100)

	ctx := context.Background()
	ch, err := bs.AllKeysChan(ctx)
	require.NoError(t, err)
	actual := collect(ch)

	require.ElementsMatch(t, keys, actual)
}


func cleanBlockstore(bs blockstore.Blockstore) {
	ctx := context.Background()
	ch, _ := bs.AllKeysChan(ctx)
	actual := collect(ch)
	for _,c := range actual {
//		fmt.Println(c)
		bs.DeleteBlock(c)
	}
}



func testAllKeysRespectsContext(t *testing.T, bs blockstore.Blockstore) {
	if c, ok := bs.(io.Closer); ok {
		defer func() { require.NoError(t, c.Close()) }()
	}

	cleanBlockstore(bs)

	_ = insertBlocks(t, bs, 100)

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := bs.AllKeysChan(ctx)
	require.NoError(t, err)

	// consume 2, then cancel context.
	v, ok := <-ch
	require.NotEqual(t, cid.Undef, v)
	require.True(t, ok)

	v, ok = <-ch
	require.NotEqual(t, cid.Undef, v)
	require.True(t, ok)

	cancel()
	// pull one value out to avoid race
	_, _ = <-ch

	v, ok = <-ch
	require.Equal(t, cid.Undef, v)
	require.False(t, ok)
}

func testDoubleClose(t *testing.T, bs blockstore.Blockstore) {
	c, ok := bs.(io.Closer)
	if !ok {
		t.SkipNow()
	}
	require.NoError(t, c.Close())
	require.NoError(t, c.Close())
}


func testPutMany(t *testing.T, bs blockstore.Blockstore) {
	if c, ok := bs.(io.Closer); ok {
		defer func() { require.NoError(t, c.Close()) }()
	}

	cleanBlockstore(bs)

	blks := []blocks.Block{
		blocks.NewBlock([]byte("foo1")),
		blocks.NewBlock([]byte("foo2")),
		blocks.NewBlock([]byte("foo3")),
	}
	err := bs.PutMany(blks)
	require.NoError(t, err)

	for _, blk := range blks {
		fetched, err := bs.Get(blk.Cid())
		require.NoError(t, err)
		require.Equal(t, blk.RawData(), fetched.RawData())

		ok, err := bs.Has(blk.Cid())
		require.NoError(t, err)
		require.True(t, ok)
	}

	ch, err := bs.AllKeysChan(context.Background())
	require.NoError(t, err)

	cids := collect(ch)
	require.Len(t, cids, 3)
}

func testDelete(t *testing.T, bs blockstore.Blockstore) {
	if c, ok := bs.(io.Closer); ok {
		defer func() { require.NoError(t, c.Close()) }()
	}

	cleanBlockstore(bs)

	blks := []blocks.Block{
		blocks.NewBlock([]byte("foo1")),
		blocks.NewBlock([]byte("foo2")),
		blocks.NewBlock([]byte("foo3")),
	}
	err := bs.PutMany(blks)
	require.NoError(t, err)

	err = bs.DeleteBlock(blks[1].Cid())
	require.NoError(t, err)

	ch, err := bs.AllKeysChan(context.Background())
	require.NoError(t, err)

	cids := collect(ch)
	require.Len(t, cids, 2)
	require.ElementsMatch(t, cids, []cid.Cid{
		cid.NewCidV1(cid.Raw, blks[0].Cid().Hash()),
		cid.NewCidV1(cid.Raw, blks[2].Cid().Hash()),
	})

	has, err := bs.Has(blks[1].Cid())
	require.NoError(t, err)
	require.False(t, has)

}

func insertBlocks(t *testing.T, bs blockstore.BasicBlockstore, count int) []cid.Cid {
	keys := make([]cid.Cid, count)
	for i := 0; i < count; i++ {
		block := blocks.NewBlock([]byte(fmt.Sprintf("some data %d", i)))
		err := bs.Put(block)
		require.NoError(t, err)
		// NewBlock assigns a CIDv0; we convert it to CIDv1 because that's what
		// the store returns.
		keys[i] = cid.NewCidV1(cid.Raw, block.Multihash())
	}
	return keys
}

func collect(ch <-chan cid.Cid) []cid.Cid {
	var keys []cid.Cid
	for k := range ch {
		keys = append(keys, k)
	}
	return keys
}
