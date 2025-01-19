package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime/pprof"
	"sunilpatil/1brc/brc"
	"time"
)

var (
	// others: "heap", "threadcreate", "block", "mutex"
	profileTypes = []string{"goroutine", "allocs"}
)

func main() {

	shouldProfile := true

	defaultMeasurementsPath := "measurements.txt"

	measurementsPath := defaultMeasurementsPath
	if len(os.Args) > 1 {
		measurementsPath = os.Args[1]
	}

	if shouldProfile {
		nowUnix := time.Now().Unix()
		os.MkdirAll(fmt.Sprintf("profiles/%d", nowUnix), 0755)
		for _, profileType := range profileTypes {
			file, _ := os.Create(fmt.Sprintf("profiles/%d/%s.%s.pprof",
				nowUnix, filepath.Base(measurementsPath), profileType))
			defer file.Close()
			defer pprof.Lookup(profileType).WriteTo(file, 0)
		}

		file, _ := os.Create(fmt.Sprintf("profiles/%d/%s.cpu.pprof",
			nowUnix, filepath.Base(measurementsPath)))
		defer file.Close()
		pprof.StartCPUProfile(file)
		defer pprof.StopCPUProfile()
	}

	brc.Optimised(measurementsPath)
}
