package processing

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/TilliboyF/1brc/go/hashtable"
)

func ResultProzessor(stream <-chan []hashtable.Entry) bytes.Buffer {
	allData := hashtable.NewLHashTable()

	//aggregating
	for res := range stream {
		for _, entry := range res {
			if entry.Key != nil {
				allData.Put2(entry.Key, entry.Value)
			}
		}
	}

	// sorting
	keys := allData.Keys()
	sort.Sort(ByteSlices(keys))

	//creating result
	var output bytes.Buffer

	fmt.Fprint(&output, "{")
	for i, k := range keys {
		if i > 0 {
			fmt.Fprint(&output, ",")
		}
		fmt.Fprintf(&output, "%s=%s", string(k), allData.MustGet(k).String())
	}
	fmt.Fprint(&output, "}\n")

	return output

}
