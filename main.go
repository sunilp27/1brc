package main

import (
	"log"
	"sunilpatil/1brc/brc"
	"time"
)

func main() {
	start := time.Now()
	brc.Optimised()
	//karan.Karan()
	elapsed := time.Since(start)
	log.Printf("time took %s", elapsed)
}
