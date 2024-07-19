package versions

import (
	"log"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/TilliboyF/1brc/go/hashtable"
	"github.com/TilliboyF/1brc/go/processing"
)

func V4() {

	start := time.Now()

	resultStream := make(chan []hashtable.Entry, 10)
	filename := "../data/measurements.txt"

	numCPU := runtime.NumCPU() - 1
	chunkStream := processing.FileSpliter(filename, int64(numCPU)*30)

	go func() {
		var wg sync.WaitGroup
		for i := 0; i < numCPU; i++ {
			wg.Add(1)
			go func() {
				for chunk := range chunkStream {
					processing.ChunkProzessor(filename, chunk, resultStream)
				}
				wg.Done()
			}()
		}
		wg.Wait()
		close(resultStream)
	}()

	buf := processing.ResultProzessor(resultStream)

	elapsed := time.Since(start)
	log.Printf("1brc took %s", elapsed)

	f, err := os.Create("results.txt")
	defer f.Close()
	if nil != err {
		panic(err)
	}
	f.Write(buf.Bytes())
	f.Sync()
}
