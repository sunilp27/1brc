package brc

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

var (
	numWorkers int
	chunkSize  int64
	fileName   string
)

func Optimised() {

	flag.Int64Var(&chunkSize, "n", 2*1024, "chunk size in KB")
	flag.StringVar(&fileName, "f", "measurements.txt", "file name")
	flag.Parse()

	chunkSize = chunkSize * 1024

	finalResults := make(map[string]*Stat)

	numWorkers = runtime.NumCPU()
	runtime.GOMAXPROCS(numWorkers)

	log.Printf("Number of workers/cpu is %d", numWorkers)

	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal("ERROR !!! file not found ", fileName)
		os.Exit(1)
	}
	defer file.Close()

	resultsChan := make(chan map[string]*Stat, numWorkers)

	fileInfo, _ := file.Stat()
	fileSize := fileInfo.Size()

	var wg sync.WaitGroup

	var aggWg sync.WaitGroup

	aggWg.Add(1)

	go func() {
		defer aggWg.Done()
		for result := range resultsChan {
			for station, stats := range result {
				finalStats, exists := finalResults[station]
				if !exists {
					finalResults[station] = stats
					continue
				} else {
					finalStats.min = min(finalStats.min, stats.min)
					finalStats.max = max(finalStats.max, stats.max)
					finalStats.mean = finalStats.mean + stats.mean
					finalStats.count = finalStats.count + stats.count
				}
			}
		}
	}()

	go func() {
		offset := int64(0) * 0
		for offset <= fileSize {

			data := make([]byte, chunkSize)
			_, err := file.ReadAt(data, offset)

			if err != nil && err != io.EOF {
				fmt.Println("Error reading file:", err)
				break
			}

			// set size of last chunk
			if offset+chunkSize > fileSize {
				data = data[:fileSize-offset]
			}

			offset = offset + chunkSize

			byt := make([]byte, 1)

			if offset < fileSize {
				for {
					_, err := file.ReadAt(byt, offset)
					if err == io.EOF {
						break
					}
					if byt[0] == '\n' {
						offset++
						break
					}
					data = append(data, byt[0])
					offset++
				}
			}

			wg.Add(1)
			go process(data, &wg, resultsChan)

		}

		wg.Wait()
		close(resultsChan)

	}()

	aggWg.Wait()
	print(finalResults)

}

func process(data []byte, wg *sync.WaitGroup, resultChan chan<- map[string]*Stat) {

	defer wg.Done()

	lines := strings.Split(string(data), "\n")
	mp := make(map[string]*Stat)

	for _, line := range lines {

		delimiter := strings.Index(line, ";")

		// ignore last line
		if delimiter < 0 {
			continue
		}

		city := line[0:delimiter]
		temp := line[delimiter+1:]

		t, err := strconv.ParseFloat(temp, 64)
		if err != nil {
			log.Printf("error in str conv")
			continue
		}

		if val, ok := mp[city]; !ok {
			mp[city] = &Stat{t, t, t, 1}
		} else {
			if val.max < t {
				val.max = t
			}
			if val.min > t {
				val.min = t
			}
			val.mean = (val.mean + t)
			val.count = val.count + 1
		}
	}
	resultChan <- mp
}
