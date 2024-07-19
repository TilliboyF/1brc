package processing

import (
	"bytes"
	"os"

	"github.com/TilliboyF/1brc/go/data"
	"github.com/TilliboyF/1brc/go/hashtable"
)

func ChunkProzessor(filename string, chunk Chunk, stream chan<- []hashtable.Entry) {
	file, err := os.Open(filename)
	defer file.Close()
	if nil != err {
		panic(err)
	}

	buf := make([]byte, chunk.to-chunk.from)

	file.ReadAt(buf, chunk.from)

	var numOfBuckets uint32 = 1 << 16
	entrys := make([]hashtable.Entry, numOfBuckets)

	var city []byte

	var cityStartIndex int64 = 0
	var tempStartIndex int64 = 0
	var index int64 = 0

	chunksize := chunk.to - chunk.from
	for index < chunksize {
		if buf[index] == ';' { // city ends
			city = buf[cityStartIndex:index]
			tempStartIndex = index + 1
		} else if buf[index] == '\n' {
			temp := BytesToInt32(buf[tempStartIndex:index])
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

func ChunkProzessor2(filename string, chunk Chunk, stream chan<- []hashtable.Entry) {
	file, err := os.Open(filename)
	defer file.Close()
	if nil != err {
		panic(err)
	}

	buf := make([]byte, chunk.to-chunk.from)

	file.ReadAt(buf, chunk.from)

	var numOfBuckets uint32 = 1 << 16
	entrys := make([]hashtable.Entry, numOfBuckets)

	var city []byte

	var cityStartIndex int64 = 0
	var tempStartIndex int64 = 0
	var index int64 = 0

	chunksize := chunk.to - chunk.from
	for index < chunksize {
		if buf[index] == ';' { // city ends
			city = buf[cityStartIndex:index]
			tempStartIndex = index + 1
		} else if buf[index] == '\n' {
			temp := BytesToInt32(buf[tempStartIndex:index])
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
