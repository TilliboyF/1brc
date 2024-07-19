package processing

import "bytes"

type ByteSlices [][]byte

func (b ByteSlices) Len() int {
	return len(b)
}

func (b ByteSlices) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

func (b ByteSlices) Less(i, j int) bool {
	return bytes.Compare(b[i], b[j]) < 0
}

func BytesToInt32(tempInBytes []byte) int32 {
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
