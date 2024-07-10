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
	allData      *hashtable.HashTable
	resultStream chan *hashtable.HashTable
)

func Djb2(key string) uint64 {
	// Initial prime value
	hash := uint64(1099511628211)
	for _, c := range key {
		char_code := uint64(c)

		// (hash<<5) means hash*(2^5)
		hash = ((hash << 5) + hash) + char_code
	}
	return hash
}

// setup
func init() {
	data = NewHashTable(1000000, 200, Djb2)
	resultStream = make(chan *HashTable, 20)
}

func main() {

	start := time.Now()

	go readInData()

	for res := range resultStream {
		for city, vals := range res {
			if m, ok := data[city]; ok {
				m.Amount += vals.Amount
				m.Sum += vals.Sum
				if vals.Max > m.Max {
					m.Max = vals.Max
				}
				if vals.Min < m.Min {
					m.Min = vals.Min
				}
			} else {
				data[city] = vals
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
	chunkStream := make(chan []byte, 11)

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

func sortData() {
	fmt.Println("Sorting...")
	keys := []string{}
	for k := range data { // how to fix that????
		keys = append(keys, k)
	}
	sort.Strings(keys)

	fmt.Println("Creating Result...")

	var stringbuilder strings.Builder

	stringbuilder.WriteString("{\n")
	for _, k := range keys {
		stringbuilder.WriteString(k)
		stringbuilder.WriteString("=")
		stringbuilder.WriteString(data.Get(k).String())
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

func processChunk(chunk []byte, stream chan<- *HashTable) {

	result := NewHashTable(1000000, 80, Djb2)
	var city string

	cityStartIndex := 0
	tempStartIndex := 0
	index := 0

	chunksize := len(chunk)

	for index < chunksize {
		if chunk[index] == ';' { // city ends
			city = string(chunk[cityStartIndex:index])
			tempStartIndex = index + 1
		} else if chunk[index] == '\n' {
			temp := bytesToInt32(chunk[tempStartIndex:index])
			if m := result.Get(city); m != nil {
				m.addVal(temp)
			} else {
				result.Set(city, NewMeasurement(temp))
			}
			cityStartIndex = index + 1
		}
		index++
	}

	stream <- result
}
