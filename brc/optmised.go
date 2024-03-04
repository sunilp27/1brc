package brc

import (
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

const chunkSize = 2 * 1024 * 1024

var (
	numWorkers int
)

func Optimised() {

	finalResults := make(map[string]*Temp)

	numWorkers = runtime.NumCPU()
	runtime.GOMAXPROCS(numWorkers)

	log.Printf("Number of workers/cpu is %d", numWorkers)

	fileName := os.Args[1]
	file, err := os.Open(fileName)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	resultsChan := make(chan map[string]*Temp, numWorkers)

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

			offset = offset + chunkSize

			byt := make([]byte, 1)

			if offset < fileSize {
				for {
					_, err := file.ReadAt(byt, offset)
					char := string(byt[0])
					data = append(data, byt[0])
					if char == "\n" {
						break
					}
					if err == io.EOF {
						break
					}
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

func process(data []byte, wg *sync.WaitGroup, resultChan chan<- map[string]*Temp) {

	defer wg.Done()

	lines := strings.Split(string(data), "\n")
	mp := make(map[string]*Temp)

	for _, line := range lines {
		spilt := strings.Split(line, ";")
		if len(spilt) < 2 {
			continue
		}

		t, err := strconv.ParseFloat(spilt[1], 64)
		if err != nil {
			continue
		}

		if val, ok := mp[spilt[0]]; !ok {
			mp[spilt[0]] = &Temp{t, t, t, 1}
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
