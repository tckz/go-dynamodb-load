package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/google/uuid"
)

var (
	optCount = flag.Int("count", 100, "number of records to generate")
	optSplit = flag.Int("split", 1, "number of files to split")
	optOut   = flag.String("out", "out/out-", "path/to/out/prefix-")
)

func main() {
	flag.Parse()

	wgOut := &sync.WaitGroup{}
	chLine := make(chan string, *optSplit*2)
	for i := 0; i < *optSplit; i++ {
		wgOut.Add(1)
		i := i
		go func() {
			defer wgOut.Done()

			fn := fmt.Sprintf("%s%03d", *optOut, i)
			if err := os.MkdirAll(filepath.Dir(fn), 0755); err != nil {
				panic(err)
			}
			f, err := os.Create(fn)
			if err != nil {
				panic(err)
			}
			defer f.Close()

			for line := range chLine {
				f.WriteString(line + "\n")
			}
		}()
	}

	for i := 0; i < *optCount; i++ {
		if i%10000 == 0 {
			log.Printf("%d recs", i)
		}
		content := "content-" + uuid.NewString()
		chLine <- strings.Join([]string{
			strconv.Itoa(i),
			content,
		}, "\t")
	}

	close(chLine)
	log.Printf("waiting for workers done")
	wgOut.Wait()

	log.Printf("total %d recs", *optCount)
}
