package hashtable

import (
	"bytes"
	"hash/fnv"

	"github.com/TilliboyF/1brc/data"
)

type Entry struct {
	Key   []byte
	Value *data.Measurement
}

type SimpleHashTable struct {
	buckets map[uint32][]*Entry
}

func NewSimpleHashTable() *SimpleHashTable {
	return &SimpleHashTable{
		buckets: make(map[uint32][]*Entry),
	}
}

func _hash(key []byte) uint32 {
	h := fnv.New32a()
	h.Write(key)
	return h.Sum32()
}

func (ht *SimpleHashTable) Put(key []byte, value *data.Measurement) {
	hash := _hash(key)
	bucket := ht.buckets[hash]

	for _, entry := range bucket {
		if bytes.Equal(entry.Key, key) {
			entry.Value = value
			return
		}
	}

	ht.buckets[hash] = append(ht.buckets[hash], &Entry{Key: key, Value: value})
}

func (ht *SimpleHashTable) Get(key []byte) (*data.Measurement, bool) {
	hash := _hash(key)
	bucket, ok := ht.buckets[hash]
	if !ok {
		return nil, false
	}
	for _, entry := range bucket {
		if bytes.Equal(entry.Key, key) {
			return entry.Value, true
		}
	}
	return nil, false
}

func (ht *SimpleHashTable) MustGet(key []byte) *data.Measurement {
	hash := _hash(key)
	bucket, ok := ht.buckets[hash]
	if !ok {
		panic("MustGet didn't find value...")
	}
	for _, entry := range bucket {
		if bytes.Equal(entry.Key, key) {
			return entry.Value
		}
	}
	panic("MustGet didn't find value...")
}

func (ht *SimpleHashTable) Keys() [][]byte {
	var keys [][]byte
	for _, bucket := range ht.buckets {
		for _, entry := range bucket {
			keys = append(keys, entry.Key)
		}
	}
	return keys
}

func (ht *SimpleHashTable) Iter() <-chan *Entry {
	ch := make(chan *Entry)
	go func() {
		for _, bucket := range ht.buckets {
			for _, entry := range bucket {
				ch <- entry
			}
		}
		close(ch)
	}()
	return ch
}