package brc

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"sync"
)

var (
	numWorkers int
	chunkSize  int64
	fileName   string
)

func Optimised(measurementsPath string) {

	chunkSize = 32 * 1024 * 1024

	finalResults := make(map[string]*Stat, 1000)

	numWorkers = runtime.NumCPU()
	runtime.GOMAXPROCS(numWorkers)

	//log.Printf("Number of workers/cpu is %d", numWorkers)

	fileName = measurementsPath

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
	//print(finalResults)

}

func process(data []byte, wg *sync.WaitGroup, resultChan chan<- map[string]*Stat) {
	defer wg.Done()

	// Create a map to store city statistics
	mp := make(map[string]*Stat, 1000)

	// Variables to track the start of each line
	start := 0

	for i, b := range data {
		if b == '\n' {
			// Extract a single line
			line := data[start:i]
			start = i + 1 // Update start to the next character after '\n'

			// Find the delimiter ';'
			delimiter := bytes.IndexByte(line, ';')

			// Skip if no delimiter (e.g., empty or invalid line)
			if delimiter < 0 {
				continue
			}

			// Extract city and temperature fields
			city := line[:delimiter]
			temp := line[delimiter+1:]

			// Parse temperature using a custom function for performance
			t := ParseFloatFast(temp)

			// Update statistics for the city
			if val, ok := mp[string(city)]; !ok {
				// Initialize stats for a new city
				mp[string(city)] = &Stat{t, t, t, 1}
			} else {
				// Update existing stats
				if val.max < t {
					val.max = t
				}
				if val.min > t {
					val.min = t
				}
				val.mean += t
				val.count++
			}
		}
	}

	// Send the resulting map to the result channel
	resultChan <- mp
}

func ParseFloatFast(bs []byte) float64 {
	var intStartIdx int // is negative?
	if bs[0] == '-' {
		intStartIdx = 1
	}

	v := float64(bs[len(bs)-1]-'0') / 10 // single decimal digit
	place := 1.0
	for i := len(bs) - 3; i >= intStartIdx; i-- { // integer part
		v += float64(bs[i]-'0') * place
		place *= 10
	}

	if intStartIdx == 1 {
		v *= -1
	}
	return v
}
