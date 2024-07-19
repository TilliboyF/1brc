package processing

import (
	"bytes"
	"os"
	"testing"
)

func TestFileSpliter(t *testing.T) {
	var splits int64 = 10
	var test_file = "../weather_stations.csv"
	ch := FileSpliter(test_file, splits)

	file, err := os.Open(test_file)
	defer file.Close()
	if nil != err {
		t.Error(err)
	}

	stats, _ := file.Stat()
	splitsize := (stats.Size() / splits) + 1

	buf := make([]byte, 1)
	var preTo int64
	index := 0

	for chunk := range ch {
		if index == 0 {
			if chunk.from != 0 {
				t.Error("First Split: split from not 0")
			}
			if (chunk.to > splitsize) || (chunk.to < splitsize-int64(50)) {
				t.Error("First Split: split to invalid range")
			}
			preTo = chunk.to
		} else {
			if chunk.from != preTo+1 {
				t.Error("From is not pre+1")
			}

			_, err := file.ReadAt(buf, chunk.to)
			if nil != err {
				t.Error(err)
			}
			if !bytes.Equal(buf, []byte{'\n'}) {
				t.Error("to index is not a line split!")
			}

			preTo = chunk.to
		}
		index++
	}
}
