package hashtable

import (
	"bytes"

	"github.com/TilliboyF/1brc/data"
)

type CustomHashTable struct {
	buckets []*Entry
	size    int
}

func NewCustomHashTable(capa int) *CustomHashTable {
	return &CustomHashTable{
		buckets: make([]*Entry, capa),
	}
}

func (ht *CustomHashTable) index(key []byte) uint32 {
	return _hash(key) & uint32(len(ht.buckets)-1)
}

func (ht *CustomHashTable) Put(key []byte, value *data.Measurement) {
	idx := ht.index(key)
	entry := ht.buckets[idx]

	if entry == nil {
		ht.buckets[idx] = &Entry{
			Key:   key,
			Value: value,
		}
		ht.size++
		return
	}

	for {
		if bytes.Equal(entry.Key, key) {
			entry.Value = value
			return
		}
		if entry.Next == nil {
			entry.Next = &Entry{
				Key:   key,
				Value: value,
			}
			ht.size++
			return
		}
		entry = entry.Next
	}
}

func (ht *CustomHashTable) Get(key []byte) (*data.Measurement, bool) {
	idx := ht.index(key)
	entry := ht.buckets[idx]

	for entry != nil {
		if bytes.Equal(entry.Key, key) {
			return entry.Value, true
		}
		entry = entry.Next
	}
	return nil, false
}

func (ht *CustomHashTable) MustGet(key []byte) *data.Measurement {
	value, exists := ht.Get(key)
	if !exists {
		panic("Key not found")
	}
	return value
}

func (ht *CustomHashTable) Iter() <-chan *Entry {
	ch := make(chan *Entry)

	go func() {
		for _, bucket := range ht.buckets {
			for entry := bucket; entry != nil; entry = entry.Next {
				ch <- entry
			}
		}
		close(ch)
	}()

	return ch
}

func (ht *CustomHashTable) Keys() [][]byte {
	var keys [][]byte
	for _, bucket := range ht.buckets {
		for entry := bucket; entry != nil; entry = entry.Next {
			keys = append(keys, entry.Key)
		}
	}
	return keys
}
