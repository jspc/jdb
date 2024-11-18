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
	//    indices[measurement_name] = map[index_name]map[index_value][]*Measurement
	// which allows for mutliple measurements to use the same index name
	// withoug clashing.
	indices map[string]map[string]map[string][]*Measurement

	// measurementFields is a mapping of Measurement.Name to a union of Dimension,
	// Index, and Label values.
	//
	// This is stored as per:
	//    measurement -> field -> type
	// because that allows us to, essentially, keep an additive set of fields without
	// needing to append and deduplicate slices which we'd need to for `map[string]measurementFields`
	measurementFields map[string]map[string]measurementFieldType
}

// New returns a JDB from a databse file on disk
func New(file string) (j *JDB, err error) {
	Logger.Info("Creating new JDB instance from disk", "stage", "boot", "file", file)

	j = new(JDB)
	j.saveBuffer = make([]*Measurement, 0, FlushMaxSize)
	j.lastSave = time.Now()

	j.ids = make(map[string]*Measurement)
	j.measurements = make(map[string]map[string][]*Measurement)
	j.indices = make(map[string]map[string]map[string][]*Measurement)
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
				indexCount++

				slices.SortFunc(measures, func(a, b *Measurement) int {
					return a.When.Compare(b.When)
				})
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
	slices.SortFunc(j.measurements[m.Name][m.DTS()], func(a, b *Measurement) int {
		return a.When.Compare(b.When)
	})

	for k, v := range m.Indices {
		slices.SortFunc(j.indices[m.Name][k][v], func(a, b *Measurement) int {
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

// QueryAll returns all measurements against a specific name
func (j *JDB) QueryAll(name string) (m []*Measurement, err error) {
	measurement, ok := j.measurements[name]
	if !ok {
		err = ErrNoSuchMeasurement

		return
	}

	tmpM := make([][]*Measurement, 0)
	for _, shard := range measurement {
		tmpM = append(tmpM, shard)
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

func (j *JDB) QueryAllCSV(name string) (b []byte, err error) {
	measurements, err := j.QueryAll(name)
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

// QueryAllIndex returns all measurements against a specific name, which has
// a specific index
func (j *JDB) QueryAllIndex(name, index, indexValue string) (m []*Measurement, err error) {
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

	return idx[indexValue], nil
}

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

	dsStr := m.DTS()
	if _, ok := j.measurements[m.Name][dsStr]; !ok {
		j.measurements[m.Name][dsStr] = make([]*Measurement, 0)
	}

	j.measurements[m.Name][dsStr] = append(j.measurements[m.Name][dsStr], m)

	if _, ok := j.indices[m.Name]; !ok {
		j.indices[m.Name] = make(map[string]map[string][]*Measurement)
	}

	for k, v := range m.Indices {
		if _, ok := j.indices[m.Name][k]; !ok {
			j.indices[m.Name][k] = make(map[string][]*Measurement)
		}

		if _, ok := j.indices[m.Name][k][v]; !ok {
			j.indices[m.Name][k][v] = make([]*Measurement, 0)
		}

		j.indices[m.Name][k][v] = append(j.indices[m.Name][k][v], m)
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
