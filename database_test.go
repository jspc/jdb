package jdb_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jspc/jdb"
)

func TestNew(t *testing.T) {
	for _, test := range []struct {
		name      string
		path      string
		expectErr bool
	}{
		{"Trying to read a file with no read permissions fails", "/root/whatever.db", true},
		{"Trying to open a readonly file fails", "testdata/ro.db", true},
		{"Trying to load a database from a garbage file fails", "testdata/garbage.db", true},
		{"Trying to load a database of valid base64, but not json, fails", "testdata/b64.db", true},

		{"Loading a database with data succedes", "testdata/valid.db", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := jdb.New(test.path)
			if test.expectErr == (err == nil) {
				t.Errorf("expected: %v, received %#v", test.expectErr, err)
			}
		})
	}
}

func TestJDB_Insert_with_small_buffer(t *testing.T) {
	f, err := os.CreateTemp("", "")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	jdb.FlushMaxSize = 10

	db, err := jdb.New(f.Name())
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	for i := 0; i < jdb.FlushMaxSize*5; i++ {
		err = db.Insert(&jdb.Measurement{
			Name: "wibbles",
			Dimensions: map[string]float64{
				"wobble_count": float64(i * 17),
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestJDB_Insert_with_short_duration(t *testing.T) {
	f, err := os.CreateTemp("", "")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	jdb.FlushMaxDuration = time.Millisecond

	db, err := jdb.New(f.Name())
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	for i := 0; i < jdb.FlushMaxSize*5; i++ {
		err = db.Insert(&jdb.Measurement{
			Name: "wibbles",
			Dimensions: map[string]float64{
				"wobble_count": float64(i * 17),
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestJDB_QueryAll(t *testing.T) {
	f, err := os.CreateTemp("", "")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	db, err := jdb.New(f.Name())
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	for i := 0; i < 10; i++ {
		err = db.Insert(&jdb.Measurement{
			Name: "wibbles",
			When: time.Now().Add(time.Hour * time.Duration(i)),
			Dimensions: map[string]float64{
				"wobble_count": float64(i * 17),
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	for _, test := range []struct {
		name        string
		searchName  string
		expectCount int
		expectErr   bool
	}{
		{"Empty measurement fails", "", 0, true},
		{"Missing/ unknown measurement fails", "zimzams", 0, true},
		{"Valid measurement returns correctly", "wibbles", 10, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			m, err := db.QueryAll(test.searchName)
			if test.expectErr == (err == nil) {
				t.Errorf("expected: %v, received %#v", test.expectErr, err)
			}

			rcvd := len(m)
			if test.expectCount != rcvd {
				t.Errorf("expected: %d, received %d", test.expectCount, rcvd)
			}
		})
	}
}

func TestJDB_QueryAllIndex(t *testing.T) {
	f, err := os.CreateTemp("", "")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	db, err := jdb.New(f.Name())
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	for i := 0; i < 10; i++ {
		err = db.Insert(&jdb.Measurement{
			Name: "wibbles",
			When: time.Now().Add(time.Hour * time.Duration(i)),
			Dimensions: map[string]float64{
				"wobble_count": float64(i * 17),
			},
			Indices: map[string]string{
				"wizzles": "plenty",
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	for _, test := range []struct {
		name           string
		searchName     string
		searchIndex    string
		searchIndexKey string
		expectCount    int
		expectErr      bool
	}{
		{"Empty measurement fails", "", "", "", 0, true},
		{"Missing/ unknown measurement fails", "zimzams", "", "", 0, true},
		{"Empty index fails", "wibbles", "", "", 0, true},
		{"Missing/ unknown index fails", "wibbles", "wazzles", "", 0, true},

		{"Valid measurement and index, no-such value, returns 0", "wibbles", "wizzles", "some", 0, false},
		{"Valid measurement and index returns correctly", "wibbles", "wizzles", "plenty", 10, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			m, err := db.QueryAllIndex(test.searchName, test.searchIndex, test.searchIndexKey)
			if test.expectErr == (err == nil) {
				t.Errorf("expected: %v, received %#v", test.expectErr, err)
			}

			rcvd := len(m)
			if test.expectCount != rcvd {
				t.Errorf("expected: %d, received %d", test.expectCount, rcvd)
			}
		})
	}
}

func ExampleNew_create_database_and_query_index() {
	f, err := os.CreateTemp("", "")
	if err != nil {
		panic(err)
	}
	f.Close()

	// Effectively disable flushing to disk for the sake of
	// timeliness in this test
	jdb.FlushMaxSize = 1_000_000
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
	jdb.FlushMaxSize = 1_000_000
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
