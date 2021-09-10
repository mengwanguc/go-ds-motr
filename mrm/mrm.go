
package main

import (
    "fmt"
    mio "github.com/mengwanguc/go-ds-motr/mio"
    mds "github.com/mengwanguc/go-ds-motr/mds"
    ds "github.com/ipfs/go-datastore"
    query "github.com/ipfs/go-datastore/query"
)


func main() {
    clearIndex("0x7800000000000001:123456701")
    clearIndex("0x7800000000000001:123456702")
    clearIndex("0x7800000000000001:123456703")
    clearIndex("0x7800000000000001:123456704")
}

func clearIndex(indexID string) {
    config := mio.Config{
        LocalEP:    "172.31.36.67@tcp:12345:33:1000",
        HaxEP:      "172.31.36.67@tcp:12345:34:1",
        Profile:    "0x7000000000000001:0",
        ProcFid:    "0x7200000000000001:64",
        TraceOn:    false,
        Verbose:    false,
        ThreadsN:   1,
    }
    motrds, err := mds.Open(config, indexID)
    if err != nil {
        fmt.Println("Failed to open index.. error: ", err)
    }

    r, err := motrds.Query(query.Query{Prefix:""})
    all, err := r.Rest()
    for _, e := range all {
        fmt.Println("-------------")
        motrds.Delete(ds.NewKey(e.Key))
        fmt.Println("key: ", e.Key)
    }

    motrds.Mkv.Close()
}
