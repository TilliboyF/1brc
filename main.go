package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/TilliboyF/1brc/data"
	"github.com/TilliboyF/1brc/hashtable"
)

var (
	allData      hashtable.HashTable
	resultStream chan hashtable.HashTable
)

// setup
func init() {
	allData = hashtable.NewSimpleHashTable()
	resultStream = make(chan hashtable.HashTable)
}

func main() {

	start := time.Now()

	go readInData()

	for res := range resultStream {
		for entry := range res.Iter() {
			if m, ok := allData.Get(entry.Key); ok {
				m.Amount += entry.Value.Amount
				m.Sum += entry.Value.Sum
				if entry.Value.Max > m.Max {
					m.Max = entry.Value.Max
				}
				if entry.Value.Min < m.Min {
					m.Min = entry.Value.Min
				}
			} else {
				allData.Put(entry.Key, entry.Value)
			}
		}
	}

	sortData()

	elapsed := time.Since(start)
	log.Printf("1brc took %s", elapsed)

}

func readInData() {

	fmt.Println("reading in lines...")

	var wg sync.WaitGroup
	chunkStream := make(chan []byte, 10)

	numCPU := runtime.NumCPU()

	for i := 0; i < numCPU-1; i++ {
		wg.Add(1)
		go func() {
			for chunk := range chunkStream {
				processChunk(chunk, resultStream)
			}
			wg.Done()
		}()
	}

	file, err := os.Open("measurements.txt")
	if nil != err {
		panic(err)
	}
	defer file.Close()

	buf := make([]byte, 1024*1024*64)
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
	close(resultStream)
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

func sortData() {
	fmt.Println("Sorting...")
	keys := allData.Keys()

	sort.Sort(ByteSlices(keys))

	fmt.Println("Creating Result...")

	var stringbuilder strings.Builder

	stringbuilder.WriteString("{\n")
	for _, k := range keys {
		stringbuilder.Write(k)
		stringbuilder.WriteString("=")
		stringbuilder.WriteString(allData.MustGet(k).String())
		stringbuilder.WriteString("\n")
	}
	stringbuilder.WriteString("}")
	writeResults(stringbuilder.String())
	fmt.Println("Done...")
}

func writeResults(result string) {
	f, err := os.Create("result.txt")
	check(err)

	defer f.Close()

	f.WriteString(result)
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func bytesToInt32(tempInBytes []byte) int32 {
	negativ := false
	index := 0
	if tempInBytes[index] == '-' {
		index++
		negativ = true
	}

	// to convert a byte int to an actual int it's need to subtract 0 from them'
	// 0-9 in Ascii/unicode is 48-57
	temp := int32(tempInBytes[index] - '0')
	index++
	if tempInBytes[index] != '.' {
		temp = temp*10 + int32(tempInBytes[index]-'0')
		index++
	}
	index++
	temp = temp*10 + int32(tempInBytes[index]-'0')

	if negativ {
		temp = -temp
	}

	return temp

}

func processChunk(chunk []byte, stream chan<- hashtable.HashTable) {

	result := hashtable.NewSimpleHashTable()
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
			if m, ok := result.Get(city); ok {
				m.AddVal(temp)
			} else {
				result.Put(city, data.NewMeasurement(temp))
			}
			cityStartIndex = index + 1
		}
		index++
	}

	stream <- result
}
