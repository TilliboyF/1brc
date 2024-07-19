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
	"strings"
	"sync"
	"time"

	"github.com/TilliboyF/1brc/go/data"
	"github.com/TilliboyF/1brc/go/hashtable"
)

var (
	allData      map[string]*data.Measurement
	resultStream chan map[string]*data.Measurement
)

// setup
func init() {
	allData = make(map[string]*data.Measurement)
	resultStream = make(chan map[string]*data.Measurement, 10)
}

func V1() {

	start := time.Now()

	go readInData()

	for res := range resultStream {
		for key, value := range res {
			if val, ok := allData[key]; ok {
				val.AddMeasurement(value)
			} else {
				allData[key] = value
			}
		}
	}

	sortData()

	elapsed := time.Since(start)
	log.Printf("1brc took %s", elapsed)

}

func readInData() {

	fmt.Println("reading in lines...")

	numCPU := runtime.NumCPU() - 1

	var wg sync.WaitGroup
	chunkStream := make(chan []byte, 10)

	for i := 0; i < numCPU; i++ {
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
	var keys []string
	for key, _ := range allData {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	fmt.Println("Creating Result...")

	var stringbuilder strings.Builder

	stringbuilder.WriteString("{\n")
	for _, k := range keys {
		stringbuilder.WriteString(k)
		stringbuilder.WriteString("=")
		stringbuilder.WriteString(allData[k].String())
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

func processChunk(chunk []byte, stream chan<- map[string]*data.Measurement) {

	result := hashtable.NewLHashTable()
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
			result.Put(city, temp)
			cityStartIndex = index + 1
		}
		index++
	}

	res := make(map[string]*data.Measurement)
	for _, entry := range result.Entrys {
		if entry.Key != nil {
			res[string(entry.Key)] = entry.Value
		}
	}

	stream <- res
}
