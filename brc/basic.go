package brc

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Temp struct {
	min   float64
	max   float64
	mean  float64
	count int
}

func BasicCalc() {
	start := time.Now()

	fileName := os.Args[1]
	file, err := os.Open(fileName)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	mp := make(map[string]*Temp)

	for str, _, err := reader.ReadLine(); err != io.EOF; str, _, err = reader.ReadLine() {
		spilt := strings.Split(string(str), ";")
		t, _ := strconv.ParseFloat(spilt[1], 64)
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

	print(mp)

	elapsed := time.Since(start)
	log.Printf("time took %s", elapsed)
}

func print(mp map[string]*Temp) {

	//sort keys

	keys := make([]string, 0, len(mp))

	for k := range mp {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	fmt.Print("{")
	for i, st := range keys {
		station := mp[st]
		fmt.Printf("%s=%.1f/%.1f/%.1f", st, station.min, station.mean/float64(station.count), station.max)
		if i < len(keys)-1 {
			fmt.Print(", ")
		}
	}

	fmt.Println("}")

}
