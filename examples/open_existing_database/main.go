package main

import (
	"fmt"
	"math"
	"os"

	"github.com/jspc/jdb"
)

func main() {
	f, err := os.CreateTemp("", "")
	if err != nil {
		panic(err)
	}

	err = f.Close()
	if err != nil {
		panic(err)
	}

	// Effectively disable flushing to disk for the sake of
	// timeliness in this test
	jdb.FlushMaxSize = int(math.Inf(1))
	jdb.FlushMaxDuration = 1<<63 - 1

	database, err := jdb.New(f.Name())
	if err != nil {
		panic(err)
	}

	err = database.Insert(&jdb.Measurement{Name: "counters", Dimensions: map[string]float64{"Counter": 1234}})
	if err != nil {
		panic(err)
	}

	// Query database
	m, err := database.QueryAll("counters", nil)
	if err != nil {
		panic(err)
	}

	fmt.Printf("counters: %d\n", len(m))

	// Close database
	err = database.Close()
	if err != nil {
		panic(err)
	}

	// Reopen, reconcile for same data
	database, err = jdb.New(f.Name())
	if err != nil {
		panic(err)
	}

	m, err = database.QueryAll("counters", nil)
	if err != nil {
		panic(err)
	}

	fmt.Printf("counters: %d\n", len(m))
}
