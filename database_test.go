package jdb_test

import (
	"fmt"
	"math"
	"os"
	"time"

	"github.com/jspc/jdb"
)

func ExampleNew_create_database_and_query_index() {
	f, err := os.CreateTemp("", "")
	if err != nil {
		panic(err)
	}
	f.Close()

	// Effectively disable flushing to disk for the sake of
	// timeliness in this test
	jdb.FlushMaxSize = int(math.Inf(1))
	jdb.FlushMaxDuration = 1<<63 - 1

	database, err := jdb.New(f.Name())
	if err != nil {
		panic(err)
	}

	defer database.Close()

	t := time.Time{}
	for i := 0; i < 1000; i++ {
		t = t.Add(time.Minute)

		m := &jdb.Measurement{
			When: t,
			Name: "environmental_monitoring",
			Dimensions: map[string]float64{
				"Temperature": 19.23,
				"Humidity":    52.43234,
				"AQI":         1,
			},
			Labels: map[string]string{
				"sensor_version": "v1.0.1",
				"uptime":         "1h31m6s",
			},
			Indices: map[string]string{
				"location": "living room",
			},
		}

		err = m.Validate()
		if err != nil {
			panic(err)
		}

		err = database.Insert(m)
		if err != nil {
			panic(err)
		}
	}

	// Query an empty index
	measurements, err := database.QueryAllIndex("environmental_monitoring", "location", "bedroom")
	if err != nil {
		panic(err)
	}

	fmt.Printf("measurements where location == bedroom: %d\n", len(measurements))

	// Query an index with items
	measurements, err = database.QueryAllIndex("environmental_monitoring", "location", "living room")
	if err != nil {
		panic(err)
	}

	fmt.Printf("measurements where location == 'living room': %d\n", len(measurements))

	// output:
	// measurements where location == bedroom: 0
	// measurements where location == 'living room': 1000
}

func ExampleNew_create_close_reopen_database() {
	f, err := os.CreateTemp("", "")
	if err != nil {
		panic(err)
	}
	f.Close()

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
	m, err := database.QueryAll("counters")
	if err != nil {
		panic(err)
	}

	fmt.Printf("counters: %d\n", len(m))

	// Close database
	database.Close()

	// Reopen, reconcile for same data
	database, err = jdb.New(f.Name())
	if err != nil {
		panic(err)
	}

	m, err = database.QueryAll("counters")
	if err != nil {
		panic(err)
	}

	fmt.Printf("counters: %d\n", len(m))

	// output:
	// counters: 1
	// counters: 1
}
