package hashtable

import (
	"bytes"

	"github.com/TilliboyF/1brc/go/data"
)

type LHashTable struct {
	Entrys     []Entry
	NumBuckets int
}

func NewLHashTable() *LHashTable {
	return &LHashTable{
		Entrys:     make([]Entry, 1<<17),
		NumBuckets: 1 << 17,
	}
}

func (ht *LHashTable) index(key []byte) uint32 {
	return Hash2(key) & uint32(ht.NumBuckets-1)
}

func (ht *LHashTable) Put(key []byte, value int32) {
	idx := ht.index(key)
	for {
		if ht.Entrys[idx].Key == nil {
			ht.Entrys[idx] = Entry{
				Key:   key,
				Value: data.NewMeasurement(value),
			}
			break
		}
		if bytes.Equal(ht.Entrys[idx].Key, key) {
			ht.Entrys[idx].Value.AddVal(value)
			break
		}
		idx++
		if idx >= uint32(ht.NumBuckets) {
			idx = 0
		}
	}
}

func (ht *LHashTable) Put2(key []byte, value *data.Measurement) {
	idx := ht.index(key)
	for {
		if ht.Entrys[idx].Key == nil {
			ht.Entrys[idx] = Entry{
				Key:   key,
				Value: value,
			}
			break
		}
		if bytes.Equal(ht.Entrys[idx].Key, key) {
			ht.Entrys[idx].Value.AddMeasurement(value)
			break
		}
		idx++
		if idx >= uint32(ht.NumBuckets) {
			idx = 0
		}
	}
}

func (ht *LHashTable) Get(key []byte) (*data.Measurement, bool) {
	idx := ht.index(key)
	for {
		if ht.Entrys[idx].Key == nil {
			return nil, false
		}
		if bytes.Equal(ht.Entrys[idx].Key, key) {
			return ht.Entrys[idx].Value, true
		}
		idx++
		if idx >= uint32(ht.NumBuckets) {
			idx = 0
		}
	}
}

func (ht *LHashTable) MustGet(key []byte) *data.Measurement {
	value, ok := ht.Get(key)
	if !ok {
		panic("Couldn't find key")
	}
	return value

}

func (ht *LHashTable) Iter() <-chan Entry {
	ch := make(chan Entry)
	go func() {
		for _, entry := range ht.Entrys {
			if entry.Key != nil {
				ch <- entry
			}
		}
		close(ch)
	}()
	return ch
}

func (ht *LHashTable) Keys() [][]byte {
	var keys [][]byte
	for _, entry := range ht.Entrys {
		if entry.Key != nil {
			keys = append(keys, entry.Key)
		}
	}
	return keys
}
