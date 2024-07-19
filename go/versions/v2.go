package versions

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/TilliboyF/1brc/go/data"
	"github.com/TilliboyF/1brc/go/hashtable"
)

var (
	v2Data         hashtable.HashTable
	v2ResultStream chan []hashtable.Entry
)

// setup
func init() {
	v2Data = hashtable.NewLHashTable()
	v2ResultStream = make(chan []hashtable.Entry, 10)
}

func V2() {

	start := time.Now()

	go v2ReadInData()

	for res := range v2ResultStream {
		for _, entry := range res {
			if entry.Key != nil {
				v2Data.Put2(entry.Key, entry.Value)
			}
		}
	}

	v2SortData()

	elapsed := time.Since(start)
	log.Printf("1brc took %s", elapsed)

}

func v2ReadInData() {

	fmt.Println("reading in lines...")

	numCPU := runtime.NumCPU() - 1

	var wg sync.WaitGroup
	chunkStream := make(chan []byte, 10)

	for i := 0; i < numCPU; i++ {
		wg.Add(1)
		go func() {
			for chunk := range chunkStream {
				v2ProcessChunk(chunk, v2ResultStream)
			}
			wg.Done()
		}()
	}

	file, err := os.Open("measurements.txt")
	if nil != err {
		panic(err)
	}
	defer file.Close()

	buf := make([]byte, 1024*1024*32)
	leftover := make([]byte, 0)
	sep := []byte{'\n'}

	for {
		n, err := file.Read(buf)
		if nil != err {
			if errors.Is(err, io.EOF) {
				break
			} else {
				panic(err)
			}
		}
		buf = buf[:n]
		lastNewLine := bytes.LastIndex(buf, sep)

		toSend := make([]byte, n)
		copy(toSend, buf[:lastNewLine+1])

		toSend = append(leftover, toSend...)
		leftover = make([]byte, len(buf[lastNewLine+1:]))
		copy(leftover, buf[lastNewLine+1:])

		chunkStream <- toSend
	}

	fmt.Println("Processed all lines...")

	close(chunkStream)
	wg.Wait()
	fmt.Println("All chunks processed...")
	close(v2ResultStream)
}

type ByteSlices [][]byte

func (b ByteSlices) Len() int {
	return len(b)
}

func (b ByteSlices) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

func (b ByteSlices) Less(i, j int) bool {
	return bytes.Compare(b[i], b[j]) < 0
}

func v2SortData() {
	keys := v2Data.Keys()
	sort.Sort(ByteSlices(keys))

	var output bytes.Buffer

	fmt.Fprint(&output, "{")
	for i, k := range keys {
		if i > 0 {
			fmt.Fprint(&output, ",")
		}
		fmt.Fprintf(&output, "%s=%s", string(k), v2Data.MustGet(k).String())
	}
	fmt.Fprint(&output, "}\n")
}

func v2ProcessChunk(chunk []byte, stream chan<- []hashtable.Entry) {

	var numOfBuckets uint32 = 1 << 16
	entrys := make([]hashtable.Entry, numOfBuckets)

	var city []byte

	cityStartIndex := 0
	tempStartIndex := 0
	index := 0

	chunksize := len(chunk)

	for index < chunksize {
		if chunk[index] == ';' { // city ends
			city = chunk[cityStartIndex:index]
			tempStartIndex = index + 1
		} else if chunk[index] == '\n' {
			temp := bytesToInt32(chunk[tempStartIndex:index])
			idx := hashtable.Hash2(city) % numOfBuckets

			for {
				if entrys[idx].Key == nil {
					entrys[idx] = hashtable.Entry{
						Key:   city,
						Value: data.NewMeasurement(temp),
					}
					break
				}
				if bytes.Equal(entrys[idx].Key, city) {
					entrys[idx].Value.AddVal(temp)
					break
				}
				idx++
				if idx >= uint32(numOfBuckets) {
					idx = 0
				}
			}

			cityStartIndex = index + 1
		}
		index++
	}

	stream <- entrys
}
