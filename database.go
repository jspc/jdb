package jdb

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"maps"
	"os"
	"slices"
	"strconv"
	"sync"
	"time"
)

var (
	// Logger can be used to log database internal operations for various
	// info statements, or left as the default- which wont log anything
	Logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	// If the save buffer hits `FlushMaxSize` length then
	// flush to disk
	FlushMaxSize = 1_000

	// If the save buffer hasn't been flushed for `FlushMaxDuration` or
	// longer then flush to disk
	FlushMaxDuration = time.Hour

	// ErrNoSuchMeasurement returns when trying to retrieve a Measurement
	// that hasn't been indexed by this JDB instance
	ErrNoSuchMeasurement = errors.New("unknown measurement name")

	// ErrNoSuchIndex returns for calls to QueryAllIndex where the index in
	// question does not exist for the specified Measurement
	ErrNoSuchIndex = errors.New("unknown index")

	// ErrDuplicateMeasurement returns when trying to Insert a Measurement, where
	// there is already a Measurement with the same derived ID
	//
	// These IDs are derived in such a way that they have a Nanosecond precision
	// against a particular measurement + index name + index value and so receiving
	// this error is a problem, and may point toward reusing/ not correctly
	// setting the value of Measurement.When
	ErrDuplicateMeasurement = errors.New("measurement and index combination exist for this timestamp")
)

// JDB is an embeddable Schemaless Timeseries Database, queried in-memory, and
// with on-disc persistence.
//
// It is deliberately naive and is designed to be 'good-enough'. It wont solve
// all of your woes, it wont handle petabytes of scale, and it wont make your
// applications more enterprisey.
//
// It will, however, give you a reasonably quick way of storing timeseries, querying
// against an index or time range, and provide de-duplication gaurantees.
type JDB struct {
	f *os.File

	saveBuffer []*Measurement
	saveMutex  sync.Mutex
	lastSave   time.Time

	// ids is a mapping of derived IDs for a given measurement/ index pair
	// and is used to ensure a degree of deduplication.
	//
	// An id is derived as a base64 string, from a combination of a Measurement
	// name, the indices contained within, and the value of Measurement.When.UnixNano()
	//
	// This means one Measurement against a particular index can be created per
	// billionth of a second, which should be fine
	ids map[string]*Measurement

	// measurements are stored as per:
	//     measurements[measurement_name] = map[date + hour][]Measurement
	// which allows for quick selecting of data.
	//
	// We key the []Measurement slice against a date+hour string because writes
	// can come at any time, but we want to store them ordered by timestamp. Thus,
	// we want to store these `Measurement`s in reasonably small blocks so that
	// we don't need to sort the world just to slot a single Measurement in
	measurements map[string]map[string][]*Measurement

	// indices are stored as per:
	//    indices[measurement_name] = map[index_name]map[index_value]map[date + hour][]*Measurement
	// which allows for mutliple measurements to use the same index name
	// without clashing.
	indices map[string]map[string]map[string]map[string][]*Measurement

	// measurementFields is a mapping of Measurement.Name to a union of Dimension,
	// Index, and Label values.
	//
	// This is stored as per:
	//    measurement -> field -> type
	// because that allows us to, essentially, keep an additive set of fields without
	// needing to append and deduplicate slices which we'd need to for `map[string]measurementFields`
	measurementFields map[string]map[string]measurementFieldType
}

// New returns a JDB from a databse file on disk, creating the database file if it
// doesn't already exist.
//
// New returns errors in the following contexts:
//
//  1. Where the OS can't open a database file for writing
//  2. The file it has opened isn't valid for JDB
//
// This function outputs optional logs, which can be enabled by setting `jdb.Logger` to
// a valid `slog.Logger`
func New(file string) (j *JDB, err error) {
	Logger.Info("Creating new JDB instance from disk", "stage", "boot", "file", file)

	j = new(JDB)
	j.saveBuffer = make([]*Measurement, 0, FlushMaxSize)
	j.lastSave = time.Now()

	j.ids = make(map[string]*Measurement)
	j.measurements = make(map[string]map[string][]*Measurement)
	j.indices = make(map[string]map[string]map[string]map[string][]*Measurement)
	j.measurementFields = make(map[string]map[string]measurementFieldType)

	// #nosec: G302,G304
	j.f, err = os.OpenFile(file, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0640)
	if err != nil {
		return
	}

	// For line in file, decode, add to the correct fields in JDB
	measurementCount := 0

	scanner := bufio.NewScanner(j.f)
	for scanner.Scan() {
		line := scanner.Bytes()

		m := new(Measurement)

		// Decode base64 to string
		dst := make([]byte, base64.StdEncoding.DecodedLen(len(line)))
		_, err = base64.StdEncoding.Decode(dst, line)
		if err != nil {
			return
		}

		// Parse string as json
		err = json.NewDecoder(bytes.NewBuffer(dst)).Decode(m)
		if err != nil {
			return
		}

		measurementCount++

		// We're using addMeasurement directly because we trust the data
		// flushed to disc, and so we don't care about the dedupe stuff we
		// do when we accept a Measurement on the public, export, [JDB.Insert]
		// api
		fields, _ := m.fields()
		j.addMeasurement(m, m.ids(), fields)
	}

	err = scanner.Err()
	if err != nil {
		return
	}

	// Sort the data we've just inserted
	//
	// QUERY: Why do we do this here, and not in `addMeasurement`? Especially
	// since we do the same thing in `Insert`?
	//
	// ANSWER: Because doing it for every Measurement we read from disk, especially,
	// on a big database, would be hugely expensive
	for _, times := range j.measurements {
		for _, measures := range times {
			slices.SortFunc(measures, func(a, b *Measurement) int {
				return a.When.Compare(b.When)
			})
		}
	}

	indexCount := 0
	for _, idx := range j.indices {
		for _, v := range idx {
			for _, measures := range v {
				for _, ts := range measures {
					indexCount++

					slices.SortFunc(ts, func(a, b *Measurement) int {
						return a.When.Compare(b.When)
					})
				}
			}
		}
	}

	Logger.Info("Measurements Loaded",
		"stage", "boot",
		"measurements", measurementCount,
		"groups", len(j.measurements),
		"indices", indexCount,
	)

	return
}

// Close a JDB, flushing contents to disk
func (j *JDB) Close() (err error) {
	j.saveMutex.Lock()
	defer j.saveMutex.Unlock()

	err = j.flush()
	if err != nil {
		return
	}

	return j.f.Close()
}

// Insert a Measurement into the database.
//
// Insert does this by performing a handful of tasks:
//
//  1. Insert will call m.Validate() to ensure the data is correct
//  2. Check whether we've already received this Measurement, erroring if so
//  3. Adding the Measurement to the underlying data structure(s)
//  4. Updating Measurement metadata (field names, indices, etc.)
//  5. Persisting to disk if the write buffer is full, or it's been some time since the last write
//
// Because we're using slices and maps under the hood without intermediate buffers, this
// call relies on mutexes that may be slow at times.
//
// The upshot of this is that calls to Insert are immediately consistent.
func (j *JDB) Insert(m *Measurement) (err error) {
	// Validate the measurement before doing anything else
	if err = m.Validate(); err != nil {
		return
	}

	// Insert one thing at a time, for goodness sake
	j.saveMutex.Lock()
	defer j.saveMutex.Unlock()

	// Grab Measurement IDs; if we have one that exists then
	// error out
	measurementIDs := m.ids()
	for _, id := range measurementIDs {
		if _, ok := j.ids[id]; ok {
			return ErrDuplicateMeasurement
		}
	}

	measurementFields, err := m.fields()
	if err != nil {
		return
	}

	j.addMeasurement(m, measurementIDs, measurementFields)

	j.saveBuffer = append(j.saveBuffer, m)

	// Ensure the new Measurement is placed in the right place(s)
	slices.SortFunc(j.measurements[m.Name][m.dts()], func(a, b *Measurement) int {
		return a.When.Compare(b.When)
	})

	for k, v := range m.Indices {
		slices.SortFunc(j.indices[m.Name][k][v][m.dts()], func(a, b *Measurement) int {
			return a.When.Compare(b.When)
		})
	}

	// If we've either got a full write buffer, or we haven't saved in a while,
	// then save now.
	//
	// Of course this might mean that some inserts are quite slow, but it is what it is
	if len(j.saveBuffer) >= FlushMaxSize || time.Now().After(j.lastSave.Add(FlushMaxDuration)) {
		err = j.flush()
		if err != nil {
			return
		}
	}

	return
}

// QueryAll queries for a Measurement name, returning all Measurements that fit.
//
// When opts is not nil, the specified time slicing options are used to
// return a subset of Measurements.
//
// For the purposes of time slicing, setting opts to nil has identical behaviour to
// setting it to empty, such as `&jdb.Options{}`, or `new(jdb.Options)`- though setting
// opts as nil saves a chunk of cycles and is, therefore, marginallty more efficient
func (j *JDB) QueryAll(name string, opts *Options) (m []*Measurement, err error) {
	measurement, ok := j.measurements[name]
	if !ok {
		err = ErrNoSuchMeasurement

		return
	}

	tmpM := make([][]*Measurement, 0)
	for _, shard := range measurement {
		switch opts {
		case nil:
			tmpM = append(tmpM, shard)

		default:
			v := opts.validMeasurements(shard)
			if len(v) > 0 {
				tmpM = append(tmpM, v)
			}
		}
	}

	// Here we're sorting the slice of measurement slices because, of course, a map
	// doesn't persist write order due to how elements are hashed
	//
	// At some point this has the potential to become quite inefficient; if a Query
	// returns a lot of matching shards then this extra sort becomes overhead.
	//
	// At this point we're going to want to look at the final map[string][]*Measurement
	// in the various data structures we keep. This will become some kind of shard
	// container that can travel through shards in order
	slices.SortFunc(tmpM, func(a, b []*Measurement) int {
		return a[0].When.Compare(b[0].When)
	})

	m = make([]*Measurement, 0)
	for _, t := range tmpM {
		m = append(m, t...)
	}

	return
}

// QueryAllCSV works identically to `QueryAll` (in fact it calls `QueryAll` under
// the hood), but returns Measurements as a []byte representation of the generated
// CSV.
//
// It can be quite expensive for large datasets.
//
// This function can be used to load data into other sources, such as jupyter, or
// a spreadsheet.
//
// When opts is not nil, the specified time slicing options are used to
// return a subset of Measurements.
//
// For the purposes of time slicing, setting opts to nil has identical behaviour to
// setting it to empty, such as `&jdb.Options{}`, or `new(jdb.Options)`- though setting
// opts as nil saves a chunk of cycles and is, therefore, marginallty more efficient
func (j *JDB) QueryAllCSV(name string, opts *Options) (b []byte, err error) {
	measurements, err := j.QueryAll(name, opts)
	if err != nil {
		return
	}

	buf := new(bytes.Buffer)
	w := csv.NewWriter(buf)

	fields := j.measurementFields[name]

	fieldNames := make([]string, 0, len(fields))
	for f := range fields {
		fieldNames = append(fieldNames, f)
	}

	// Let's make the output nice and deterministic
	slices.Sort(fieldNames)

	// Let's prepend with the important ones
	fieldNames = append([]string{"timestamp", "measure"}, fieldNames...)

	err = w.Write(fieldNames)
	if err != nil {
		return
	}

	for _, m := range measurements {
		line := make([]string, 0, len(fieldNames)+2)

		for _, f := range fieldNames {
			if f == "timestamp" {
				line = append(line, m.When.Format(time.RFC3339))

				continue
			}

			if f == "measure" {
				line = append(line, m.Name)

				continue
			}

			t := fields[f]

			switch t {
			case dimension:
				line = append(line, strconv.FormatFloat(m.Dimensions[f], 'g', -1, 64))

			case index:
				line = append(line, m.Indices[f])

			case label:
				line = append(line, m.Labels[f])
			}
		}

		err = w.Write(line)
		if err != nil {
			return
		}
	}

	w.Flush()

	return buf.Bytes(), err
}

// QueryAllIndex queries for a Measurement name, returning all Measurements with a specific Index value.
//
// When opts is not nil, the specified time slicing options are used to
// return a subset of Measurements.
//
// For the purposes of time slicing, setting opts to nil has identical behaviour to
// setting it to empty, such as `&jdb.Options{}`, or `new(jdb.Options)`- though setting
// opts as nil saves a chunk of cycles and is, therefore, marginallty more efficient
func (j *JDB) QueryAllIndex(name, index, indexValue string, opts *Options) (m []*Measurement, err error) {
	measurement, ok := j.indices[name]
	if !ok {
		err = ErrNoSuchMeasurement

		return
	}

	idx, ok := measurement[index]
	if !ok {
		err = ErrNoSuchIndex

		return
	}

	iv, ok := idx[indexValue]
	if !ok {
		return
	}

	tmpM := make([][]*Measurement, 0)
	for _, shard := range iv {
		switch opts {
		case nil:
			tmpM = append(tmpM, shard)

		default:
			v := opts.validMeasurements(shard)
			if len(v) > 0 {
				tmpM = append(tmpM, v)
			}
		}
	}

	slices.SortFunc(tmpM, func(a, b []*Measurement) int {
		return a[0].When.Compare(b[0].When)
	})

	m = make([]*Measurement, 0)
	for _, t := range tmpM {
		m = append(m, t...)
	}

	return
}

// QueryFields returns the fields set for a Measurement
func (j *JDB) QueryFields(measurement string) (fields []string, err error) {
	fm, ok := j.measurementFields[measurement]
	if !ok {
		return nil, ErrNoSuchMeasurement
	}

	fields = make([]string, 0, len(fm))
	for f := range fm {
		fields = append(fields, f)
	}

	return
}

// addMeasurement adds a Measurement to the underlying fields in JDB
func (j *JDB) addMeasurement(m *Measurement, ids []string, fields map[string]measurementFieldType) {
	if _, ok := j.measurements[m.Name]; !ok {
		j.measurements[m.Name] = make(map[string][]*Measurement)
	}

	dsStr := m.dts()
	if _, ok := j.measurements[m.Name][dsStr]; !ok {
		j.measurements[m.Name][dsStr] = make([]*Measurement, 0)
	}

	j.measurements[m.Name][dsStr] = append(j.measurements[m.Name][dsStr], m)

	if _, ok := j.indices[m.Name]; !ok {
		j.indices[m.Name] = make(map[string]map[string]map[string][]*Measurement)
	}

	for k, v := range m.Indices {
		if _, ok := j.indices[m.Name][k]; !ok {
			j.indices[m.Name][k] = make(map[string]map[string][]*Measurement)
		}

		if _, ok := j.indices[m.Name][k][v]; !ok {
			j.indices[m.Name][k][v] = make(map[string][]*Measurement, 0)
		}

		if _, ok := j.indices[m.Name][k][v][dsStr]; !ok {
			j.indices[m.Name][k][v][dsStr] = make([]*Measurement, 0)
		}

		j.indices[m.Name][k][v][dsStr] = append(j.indices[m.Name][k][v][dsStr], m)
	}

	// Update the IDs map
	for _, id := range ids {
		j.ids[id] = m
	}

	// Update measurement fields
	if _, ok := j.measurementFields[m.Name]; !ok {
		j.measurementFields[m.Name] = make(map[string]measurementFieldType)
	}

	maps.Copy(j.measurementFields[m.Name], fields)
}

func (j *JDB) flush() (err error) {
	Logger.Info("Flushing to disc", "buffer_length", len(j.saveBuffer))

	for _, m := range j.saveBuffer {
		buf := new(bytes.Buffer)
		err = json.NewEncoder(buf).Encode(*m)
		if err != nil {
			return
		}

		dst := make([]byte, base64.StdEncoding.EncodedLen(buf.Len()))
		base64.StdEncoding.Encode(dst, buf.Bytes())

		_, err = j.f.Write(dst)
		if err != nil {
			return
		}

		_, err = j.f.Write([]byte{'\n'})
		if err != nil {
			return
		}
	}

	j.saveBuffer = make([]*Measurement, 0, FlushMaxSize)
	j.lastSave = time.Now()

	return
}
