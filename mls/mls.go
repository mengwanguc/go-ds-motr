
package main

import (
    "fmt"
    mio "github.com/mengwanguc/go-ds-motr/mio"
    mds "github.com/mengwanguc/go-ds-motr/mds"
//    ds "github.com/ipfs/go-datastore"
    query "github.com/ipfs/go-datastore/query"
)



func main() {
    config := mio.Config{
        LocalEP:    "10.230.242.162@tcp:12345:4:1",
        HaxEP:      "10.230.242.162@tcp:12345:1:1",
        Profile:    "0x7000000000000001:0x3d",
        ProcFid:    "0x7200000000000001:0x17",
        TraceOn:    false,
        Verbose:    false,
        ThreadsN:   1,
    }
    indexID := "0x7800000000000001:123456702"
    motrds, err := mds.Open(config, indexID)
    if err != nil {
        fmt.Println("Failed to open index.. error: ", err)
    }

    r, err := motrds.Query(query.Query{Prefix:""})
    all, err := r.Rest()
    for _, e := range all {
        fmt.Println("-------------")
//y        motrds.Delete(ds.NewKey(e.Key))
        fmt.Println(e.Key, string(e.Value))
    }

//    r, err := motrds.Get(" /CIQENVCICS44LLYUDQ5KVN6ALXC6QRHK2X4R6EUFRMBB5OSFO2FUYDQ ")
//    fmt.Println(r, err)
    motrds.Mkv.Close()
}
