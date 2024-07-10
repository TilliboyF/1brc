package hashtable

import (
	"github.com/TilliboyF/1brc/data"
)

type HashTable interface {
	Put(key []byte, value *data.Measurement)
	Get(key []byte) (*data.Measurement, bool)
	MustGet(key []byte) *data.Measurement
	Keys() [][]byte
	Iter() <-chan *Entry
}
