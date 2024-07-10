package hashtable

import "github.com/TilliboyF/1brc/data"

type KeyVal struct {
	key string
	m   *data.Measurement
}

type CustomHashTable struct {
	BucketSize int32
	FilledSize int32
	Bucket     [][]KeyVal
	LoadFactor int32
	HashFunc   func(string) uint32
}

func NewHashTable(BucketSize int32, LoadFactor int32, HashFunc func(string) uint32) *CustomHashTable {
	return &CustomHashTable{
		BucketSize: BucketSize,
		Bucket:     make([][]KeyVal, BucketSize),
		FilledSize: 0,
		LoadFactor: LoadFactor,
		HashFunc:   HashFunc,
	}
}

func (ht *CustomHashTable) _hash(key string) int32 {
	h1 := int32(ht.HashFunc(key) % uint32(ht.BucketSize))

	h2 := int32(unknownHash(key) % uint32(ht.BucketSize))

	var i int32 = 0
	for len(ht.Bucket[h1]) > 0 && ht.Bucket[h1][0].key != key {
		h1 = (h1 + i*h2 + (i*i*i-i)/6) % ht.BucketSize
		if i == ht.BucketSize {
			break
		}
		i++
	}
	return h1
}

func (ht *CustomHashTable) Set(key string, value *data.Measurement) {
	load := ht.FilledSize * 100 / ht.BucketSize

	if load >= ht.LoadFactor {
		ht.BucketSize = ht.BucketSize * 2
		temp := ht.Bucket
		ht.Bucket = make([][]KeyVal, ht.BucketSize)

		for _, v := range temp {
			for _, w := range v {
				hash := ht._hash(w.key)
				ht.Bucket[hash] = append(ht.Bucket[hash], KeyVal{key: w.key, m: w.m})
			}
		}
	}

	hash := ht._hash(key)

	if len(ht.Bucket[hash]) > 0 {
		for i, v := range ht.Bucket[hash] {
			if v.key == key {
				ht.Bucket[hash][i].m = value
				return
			}
		}
	}

	if len(ht.Bucket[hash]) == 0 {
		ht.FilledSize++
	}
	ht.Bucket[hash] = append(ht.Bucket[hash], KeyVal{key: key, m: value})

}

func (ht *CustomHashTable) Get(key string) *data.Measurement {
	hash := ht._hash(key)
	if len(ht.Bucket[hash]) > 0 {
		for _, v := range ht.Bucket[hash] {
			if v.key == key {
				return v.m
			}
		}
	}
	return nil
}

func unknownHash(key string) uint32 {
	hash := uint32(0)
	mul := uint32(1)
	for i, c := range key {
		char_code := uint32(c)
		if i%4 == 0 {
			mul = 1
		} else {
			mul *= 256
		}
		hash += char_code * mul
	}
	return hash
}
