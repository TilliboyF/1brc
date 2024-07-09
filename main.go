package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
)

var (
	data         map[string]*Measurement
	resultStream chan map[string]*Measurement
)

// setup
func init() {
	data = make(map[string]*Measurement)
	resultStream = make(chan map[string]*Measurement, 10)
}

func main() {
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

func sortData() {
	fmt.Println("Sorting...")
	keys := []string{}
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	fmt.Println("Creating Result...")

	var stringbuilder strings.Builder

	stringbuilder.WriteString("{\n")
	for _, k := range keys {
		stringbuilder.WriteString(data[k].String())
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

func stringToInt64(val string) int64 {
	input := val[:len(val)-2] + val[len(val)-1:]
	res, _ := strconv.ParseInt(input, 10, 64)
	return res
}

func processChunk(chunk []byte, stream chan<- map[string]*Measurement) {

	result := make(map[string]*Measurement)
	var builder strings.Builder
	var city string

	for _, c := range chunk {
		if c == ';' { // case city
			city = builder.String()
			builder.Reset()
		} else if c == '\n' { // case temperature
			tempString := builder.String()
			temp := stringToInt64(tempString)
			if m, ok := result[city]; ok {
				m.addVal(temp)
			} else {
				result[city] = NewMeasurement(city, temp)
			}
			builder.Reset()
		} else { // case in between
			builder.WriteByte(c)
		}
	}

	stream <- result
}
