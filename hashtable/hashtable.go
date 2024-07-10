package hashtable

import (
	"hash/fnv"

	"github.com/TilliboyF/1brc/data"
)

type HashTable interface {
	Put(key []byte, value *data.Measurement)
	Get(key []byte) (*data.Measurement, bool)
	MustGet(key []byte) *data.Measurement
	Keys() [][]byte
	Iter() <-chan *Entry
}

type Entry struct {
	Key   []byte
	Value *data.Measurement
	Next  *Entry
}

func _hash(key []byte) uint32 {
	h := fnv.New32a()
	h.Write(key)
	return h.Sum32()
}
