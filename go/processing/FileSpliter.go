package processing

import (
	"bytes"
	"errors"
	"io"
	"os"
)

type Chunk struct {
	from, to int64
}

func FileSpliter(filename string, splits int64) chan Chunk {
	ch := make(chan Chunk, 10)

	go func() {
		file, err := os.Open(filename)
		defer file.Close()
		if nil != err {
			panic(err)
		}
		fileStat, err := file.Stat()
		if nil != err {
			panic(err)
		}
		fileSize := fileStat.Size()

		var offset int64
		const CHECK_LENGTH int64 = 50
		buf := make([]byte, CHECK_LENGTH)
		sep := []byte{'\n'}

		splitsize := (fileSize / splits) + 1

		for {
			if offset+splitsize >= fileSize {
				file.Seek(fileSize-CHECK_LENGTH, io.SeekStart)
				file.Read(buf)
				lastNewLine := bytes.LastIndex(buf, sep)
				ch <- Chunk{
					from: offset,
					to:   fileSize - CHECK_LENGTH + int64(lastNewLine),
				}
				break
			}

			file.Seek(offset+splitsize-CHECK_LENGTH, io.SeekStart)

			n, err := file.Read(buf) // buf contains offset-50 til offset
			if nil != err {
				if errors.Is(err, io.EOF) {
					break // can't actually happen, is checked upfront'
				}
				panic(err)
			}
			buf = buf[:n]
			lastNewLine := bytes.LastIndex(buf, sep)

			to := offset + splitsize + int64(lastNewLine) - CHECK_LENGTH

			ch <- Chunk{
				from: offset,
				to:   offset + splitsize + int64(lastNewLine) - CHECK_LENGTH,
			}
			offset = to + 1

		}

		close(ch)
	}()

	return ch
}
