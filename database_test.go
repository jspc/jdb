package jdb_test

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"os"
	"slices"
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

func TestJDB_Insert(t *testing.T) {
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

	now := time.Now()
	m := &jdb.Measurement{
		When: now,
		Name: "test",
		Dimensions: map[string]float64{
			"xyz": 3232,
		},
		Indices: map[string]string{
			"test": "true",
		},
	}
	db.Insert(m)

	for _, test := range []struct {
		name      string
		m         *jdb.Measurement
		expectErr bool
	}{
		{"Inserting a duplicate measurement fails", m, true},
		{"Inserting a measurement with a reused time and index fails", &jdb.Measurement{When: now, Name: "test", Dimensions: map[string]float64{"abc": 4545}, Indices: map[string]string{"test": "true"}}, true},
		{"Inserting a measurement with duplicate field names fails, labels", &jdb.Measurement{When: time.Now(), Name: "test", Dimensions: map[string]float64{"abc": 4545}, Indices: map[string]string{"test": "true"}, Labels: map[string]string{"test": "also true"}}, true},
		{"Inserting a measurement with duplicate field names fails, indices", &jdb.Measurement{When: time.Now(), Name: "test", Dimensions: map[string]float64{"abc": 4545}, Indices: map[string]string{"test": "true", "abc": "four thousand, five hundred, and forty five"}, Labels: map[string]string{"test": "also true"}}, true},

		{"Inserting a measurement without any indices succedes, however inadvisable", &jdb.Measurement{When: now, Name: "test", Dimensions: map[string]float64{"counter": 100}}, false},

		// The following tests come from measurement_test.go - we duplicate them here
		// to ensure that validations are, in fact, being called.
		//
		// We keep them in the original location too because we want to ensure that
		// validate can be called separately- this allows us to fail fast in cases
		// where we're parsing Measurements from, say, an API
		{"Empty measurement name should fail", &jdb.Measurement{}, true},
		{"Empty dimensions should fail", &jdb.Measurement{Name: "My Measurement"}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			err = db.Insert(test.m)
			if test.expectErr == (err == nil) {
				t.Errorf("expected: %v, received %#v", test.expectErr, err)
			}
		})
	}
}

func TestJDB_Upsert(t *testing.T) {
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

	now := time.Now()
	m := &jdb.Measurement{
		When: now,
		Name: "test",
		Dimensions: map[string]float64{
			"xyz": 3232,
		},
		Indices: map[string]string{
			"test": "true",
		},
	}
	db.Insert(m)

	for _, test := range []struct {
		name      string
		m         *jdb.Measurement
		expectXyz float64
		expectErr bool
	}{
		{"Inserting a duplicate measurement succedes", m, 3232, false},
		{"Inserting a measurement of the same time and name, with different values, returns new values", &jdb.Measurement{When: now, Name: "test", Dimensions: map[string]float64{"xyz": 9999}, Indices: map[string]string{"test": "true"}}, 9999, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			err = db.Upsert(test.m)
			if test.expectErr == (err == nil) {
				t.Errorf("expected: %v, received %#v", test.expectErr, err)
			}

			m, err := db.QueryAll("test", &jdb.Options{Deduplicate: true})
			if err != nil {
				t.Error(err)
			}

			t.Logf("%#v", m)

			if len(m) != 1 {
				t.Fatalf("expected 1 measurement, received %d", len(m))
			}

			v := m[0].Dimensions["xyz"]
			if test.expectXyz != v {
				t.Errorf("expected %f, received %f", test.expectXyz, v)
			}
		})
	}
}

func TestJDB_Upsert_Complex(t *testing.T) {
	f, err := os.CreateTemp("", "")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	jdb.FlushMaxSize = 1_000_000
	jdb.FlushMaxDuration = 1<<63 - 1

	db, err := jdb.New(f.Name())
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	now := time.Now()
	runs := 20_000

	for i := 0; i < runs; i++ {
		if i%7 == 0 {
			err := db.Upsert(&jdb.Measurement{
				Name: "supplementary",
				When: time.Now(),
				Dimensions: map[string]float64{
					"idx": float64(i),
				},
			})
			if err != nil {
				t.Fatal(err)
			}
		}

		err := db.Upsert(&jdb.Measurement{
			Name: "upserts",
			When: now,
			Indices: map[string]string{
				"test_func": "TestJDB_Upsert_Complex",
			},
			Labels: map[string]string{
				"iteration": fmt.Sprintf("%d", i),
			},
			Dimensions: map[string]float64{
				"value": float64(i),
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Get data without deduping
	dupes, err := db.QueryAll("upserts", nil)
	if err != nil {
		t.Fatal(err)
	}

	if runs != len(dupes) {
		t.Errorf("expected %d, received %d", runs, len(dupes))
	}

	// Get data after deduping
	dedupes, err := db.QueryAll("upserts", &jdb.Options{Deduplicate: true})
	if err != nil {
		t.Fatal(err)
	}

	if len(dedupes) > 1 {
		t.Errorf("expected 1, received %d", len(dedupes))
	}

	v := dedupes[0].Dimensions["value"]
	if v != float64(runs-1) {
		t.Errorf("expected %f, received %f", float64(runs-1), v)
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
			When: time.Now().Add(0 - time.Hour*time.Duration(i)),
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
			When: time.Now().Add(0 - time.Hour*time.Duration(i)),
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
			m, err := db.QueryAll(test.searchName, nil)
			if test.expectErr == (err == nil) {
				t.Errorf("expected: %v, received %#v", test.expectErr, err)
			}

			rcvd := len(m)
			if test.expectCount != rcvd {
				t.Errorf("expected: %d, received %d", test.expectCount, rcvd)
			}

			t.Run("results are sorted", func(t *testing.T) {
				sorted := slices.IsSortedFunc(m, func(a, b *jdb.Measurement) int {
					return a.When.Compare(b.When)
				})

				if !sorted {
					t.Error("Results are not sorted")
				}
			})
		})
	}
}

func TestJDB_QueryAll_options(t *testing.T) {
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

	now := time.Now()
	for i := 0; i < 10; i++ {
		err = db.Insert(&jdb.Measurement{
			Name: "wibbles",
			When: now.Add(0 - time.Hour*time.Duration(i)),
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
		opts        *jdb.Options
		expectCount int
		expectErr   bool
	}{
		{"Nil options returns all values", "wibbles", nil, 10, false},
		{"Empty options returns all values", "wibbles", new(jdb.Options), 10, false},
		{"Setting To to now and leaving all else returns all values", "wibbles", &jdb.Options{To: now}, 10, false},
		{"Setting To to now and setting Duration to 24 hours returns all values", "wibbles", &jdb.Options{To: now, Since: time.Hour * 24}, 10, false},
		{"Setting Duration to 24 hours and leaving all else returns all values", "wibbles", &jdb.Options{Since: time.Hour * 24}, 10, false},
		{"Setting From to 2 hours ago returns three values", "wibbles", &jdb.Options{From: now.Add(0 - time.Hour*2)}, 3, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			m, err := db.QueryAll(test.searchName, test.opts)
			if test.expectErr == (err == nil) {
				t.Errorf("expected: %v, received %#v", test.expectErr, err)
			}

			rcvd := len(m)
			if test.expectCount != rcvd {
				t.Errorf("expected: %d, received %d", test.expectCount, rcvd)
			}

			t.Run("results are sorted", func(t *testing.T) {
				sorted := slices.IsSortedFunc(m, func(a, b *jdb.Measurement) int {
					return a.When.Compare(b.When)
				})

				if !sorted {
					t.Error("Results are not sorted")
				}
			})
		})
	}
}

func TestJDB_QueryAllCSV(t *testing.T) {
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
				"jiggle_tally": float64(8 ^ 17),
			},
			Indices: map[string]string{
				"enabled": "probably",
				"wibbler": "0xabadbabe",
			},
			Labels: map[string]string{
				"version": "v0.1.1",
				"uptime":  "1h32m11s",
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Add one more with slight different labels
	err = db.Insert(&jdb.Measurement{
		Name: "wibbles",
		When: time.Now().Add(time.Hour * 72),
		Dimensions: map[string]float64{
			"wobble_count": 6.1111111111113,
			"jiggle_tally": 1,
		},
		Indices: map[string]string{
			"wibbler": "0xcafebabe",
		},
		Labels: map[string]string{
			"uptime":   "1h32m11s",
			"operator": "Big Doug",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		name        string
		measurement string
		expectRows  int
		expectCols  int
		expectErr   bool
	}{
		{"Querying non-existent measurement should fail", "floops", 0, 0, true},
		{"Querying valid measurement should return union of all fields", "wibbles", 12, 9, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			b, err := db.QueryAllCSV(test.measurement, nil)
			if test.expectErr == (err == nil) {
				t.Errorf("expected: %v, received %#v", test.expectErr, err)
			}

			buf := bytes.NewBuffer(b)
			r := csv.NewReader(buf)

			records, err := r.ReadAll()
			if err != nil {
				t.Fatal(err)
			}

			if test.expectRows != len(records) {
				t.Errorf("expected %d records, received %d", test.expectRows, len(records))
			}

			if len(records) == 0 {
				if test.expectRows > 0 {
					t.Fatal("there should be some columns to count, but there arent'")
				}

				return
			}

			cols := records[0]
			if test.expectCols != len(cols) {
				t.Errorf("expected %d columns, received %d", test.expectCols, len(cols))
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
			m, err := db.QueryAllIndex(test.searchName, test.searchIndex, test.searchIndexKey, nil)
			if test.expectErr == (err == nil) {
				t.Errorf("expected: %v, received %#v", test.expectErr, err)
			}

			rcvd := len(m)
			if test.expectCount != rcvd {
				t.Errorf("expected: %d, received %d", test.expectCount, rcvd)
			}

			t.Run("results are sorted", func(t *testing.T) {
				sorted := slices.IsSortedFunc(m, func(a, b *jdb.Measurement) int {
					return a.When.Compare(b.When)
				})

				if !sorted {
					t.Error("Results are not sorted")
				}
			})
		})
	}
}

func TestJDB_QueryFields(t *testing.T) {
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

	for i := 0; i < jdb.FlushMaxSize*5; i++ {
		err = db.Insert(&jdb.Measurement{
			Name: "wibbles",
			When: time.Now().Add(0 - time.Hour*time.Duration(i)),
			Dimensions: map[string]float64{
				"wobble_count": float64(i * 17),
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	for _, test := range []struct {
		name         string
		measurement  string
		expectFields int
		expectErr    bool
	}{
		{"Querying an unknown measure should fail", "wet_hankies", 0, true},
		{"Querying an valid measure should succeed", "wibbles", 2, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			f, err := db.QueryFields(test.measurement)
			if test.expectErr == (err == nil) {
				t.Errorf("expected: %v, received %#v", test.expectErr, err)
			}

			if test.expectFields != len(f) {
				t.Errorf("expected %d fields, received %d", test.expectFields, len(f))
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
	measurements, err := database.QueryAllIndex("environmental_monitoring", "location", "bedroom", nil)
	if err != nil {
		panic(err)
	}

	fmt.Printf("measurements where location == bedroom: %d\n", len(measurements))

	// Query an index with items
	measurements, err = database.QueryAllIndex("environmental_monitoring", "location", "living room", new(jdb.Options))
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
	m, err := database.QueryAll("counters", nil)
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

	m, err = database.QueryAll("counters", nil)
	if err != nil {
		panic(err)
	}

	fmt.Printf("counters: %d\n", len(m))

	// output:
	// counters: 1
	// counters: 1
}
