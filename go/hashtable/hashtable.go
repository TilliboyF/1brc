package hashtable

import (
	"hash/fnv"

	"github.com/TilliboyF/1brc/go/data"
)

type HashTable interface {
	Put(key []byte, value int32)
	Put2(key []byte, value *data.Measurement)
	Get(key []byte) (*data.Measurement, bool)
	MustGet(key []byte) *data.Measurement
	Keys() [][]byte
	Iter() <-chan Entry
}

type Entry struct {
	Key   []byte
	Value *data.Measurement
}

func _hash(key []byte) uint32 {
	h := fnv.New32a()
	h.Write(key)
	return h.Sum32()
}

const (
	offset32 = 2166136261
	prime32  = 16777619
)

func Hash2(key []byte) uint32 {
	hash := uint32(offset32)
	for _, b := range key {
		hash *= prime32
		hash ^= uint32(b)
	}
	return hash
}
