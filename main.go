package main

import (
	"log"
	"sunilpatil/1brc/brc"
	"time"
)

func main() {
	start := time.Now()
	brc.Optimised()
	//elh.Main()
	elapsed := time.Since(start)
	log.Printf("time took %s", elapsed)
}
