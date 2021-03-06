//motr datastore
package mds

import (
//    "os"
    "fmt"
//    "flag"
    "log"
    "strings"
    "github.com/mengwanguc/go-ds-motr/mio"
    ds "github.com/ipfs/go-datastore"
    dsq "github.com/ipfs/go-datastore/query"    
)


type MotrDS struct {
    Config     mio.Config
    IndexID    string
    Mkv        *mio.Mkv
}

func Open(conf mio.Config, indexID string) (*MotrDS, error) {
    fmt.Println("-- menglog mio.InitWithConfig(conf) , indexID:", indexID)
    mio.InitWithConfig(conf)
    var mkv mio.Mkv
    createFlag := true
    fmt.Println("-- menglog mkv.Open(indexID, createFlag)")
    if err := mkv.Open(indexID, createFlag); err != nil {
        log.Fatalf("failed to open index %v: %v", indexID, err)
        return nil, err
    }
//    defer mkv.Close()


    return &MotrDS {
        Config:    conf,
        IndexID:   indexID,
        Mkv:       &mkv,
    }, nil
}


func (mds *MotrDS) Put(k ds.Key, value []byte) error {
    err := mds.Mkv.Put([]byte(k.String()), value, true)
    fmt.Println("-- menglog MotrDS ", mds.IndexID, " Put key: ", k, " value: ", string(value), "error:", err)
    return err
}

func (mds *MotrDS) Get(k ds.Key) ([]byte, error) {
    val, err := mds.Mkv.Get([]byte(k.String()))
    if len(val) == 0 {
        if strings.HasSuffix(err.Error(), "-2") == true {
            fmt.Println("-- menglog MotrDS ", mds.IndexID, " Get key: ", k, "error:", ds.ErrNotFound)
            return val, ds.ErrNotFound
        }
    }
    fmt.Println("-- menglog MotrDS ", mds.IndexID, " Get key: ", k, "error:", err)
    return val, err
}


func (mds *MotrDS) GetSize(k ds.Key) (int, error) {
    val, err := mds.Mkv.Get([]byte(k.String()))
    if err != nil {
        if strings.HasSuffix(err.Error(), "-2") == true {
            if len(val) == 0 {
                fmt.Println("-- menglog MotrDS ", mds.IndexID, " GetSize key: ", k, " return", -1, " error:", ds.ErrNotFound)
                return -1, ds.ErrNotFound
            }
        }
        fmt.Println("-- menglog MotrDS ", mds.IndexID, " GetSize key: ", k, " return", -1, " error:", err)
        return -1, err
    }
    fmt.Println("-- menglog MotrDS ", mds.IndexID, " GetSize key: ", k, " return", len(val), " error:", err)
    return len(val), nil
}

func (mds *MotrDS) Delete(k ds.Key) error {
    fmt.Println("-- menglog MotrDS ", mds.IndexID, " Delete key: ", k)
    err :=  mds.Mkv.Delete([]byte(k.String()))
    if err != nil && strings.HasSuffix(err.Error(), "-2") == true {
        fmt.Println("-- menglog MotrDS ", mds.IndexID, " Delete key: ", k, "error: ", nil)
        return nil
    }
    fmt.Println("-- menglog MotrDS ", mds.IndexID, " Delete key: ", k, "error: ", nil)
    return err
}




func (mds *MotrDS) Has(k ds.Key) (bool, error) {
    val, err :=  mds.Mkv.Get([]byte(k.String()))
    if len(val) == 0  {
        if strings.HasSuffix(err.Error(), "-2") == true {
            fmt.Println("-- menglog MotrDS ", mds.IndexID, " Has key: ", k, " return", false, " error:", nil)
            return false, nil
        }
        fmt.Println("-- menglog MotrDS ", mds.IndexID, " Has key: ", k, " return", false, " error:", err)
        return false, err
    }
    fmt.Println("-- menglog MotrDS ", mds.IndexID, " Has key: ", k, " return", true, " error:", err)
    return true, err
}


func (mds *MotrDS) Sync(prefix ds.Key) error {
	return nil
}

func (mds *MotrDS) Query(q dsq.Query) (dsq.Results, error) {
    fmt.Println("-- menglog MotrDS ", mds.IndexID, " Query: ", q)
    var k, val []byte
    var err error = nil

    results := make(chan dsq.Result)


    go func() {
        k = []byte{0}
        k, val, err = mds.Mkv.Next(append(k, 0))
        fmt.Println(k, string(val))
        for err == nil {
            var result dsq.Result
            result.Entry.Key = string(k)
            result.Entry.Value = val
            if q.ReturnsSizes {
                result.Entry.Size = len(val)
            }

            results <- result
            k, val, err = mds.Mkv.Next(append(k, 0))
        }
        close(results)
    } ()

    r := dsq.ResultsWithChan(q, results)
    r = dsq.NaiveQueryApply(q, r)

    return r, nil
}

func (mds *MotrDS) Close() error {
    mds.Mkv.Close()
    return nil
}


type motrDSBatch struct {
    puts     map[ds.Key][]byte
    deletes  map[ds.Key]struct{}
    mds      *MotrDS

}

type batchOp struct {
    value     []byte
    isDelete  bool
}

func (mds *MotrDS) Batch() (ds.Batch, error) {
    return &motrDSBatch{
        puts:    make(map[ds.Key][]byte),
        deletes: make(map[ds.Key]struct{}),
        mds:     mds,
    }, nil
}

func (bt *motrDSBatch) Put(key ds.Key, val []byte) error {
    bt.puts[key] = val
    return nil
}

func (bt *motrDSBatch) Delete(key ds.Key) error {
    bt.deletes[key] = struct{}{}
    return nil
}

func (bt *motrDSBatch) Commit() error {
    for k, val := range bt.puts {
        if err := bt.mds.Put(k, val); err != nil {
            return err
        }
    }

    for k, _ := range bt.deletes {
        if err := bt.mds.Delete(k); err != nil {
            return err
        }
    }

    return nil
}

