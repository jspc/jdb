package jdb

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"slices"
	"time"
)

var (
	ErrEmptyName    = errors.New("measurement name must not be empty")
	ErrNoDimensions = errors.New("measurement has no dimensions")
	ErrFieldInUse   = errors.New("field names must be unique across dimensions, labels, and indices for a given Measurement name")
)

const (
	dtsFmt = "2006-01-02_15"

	// DefaultIndexName is used for Measurements where an Index
	// hasn't beed specified so we can still de-dupe it.
	DefaultIndexName = "_default_index"
)

// A Measurement represents a collection of values and metadata to store
// against a timestamp.
//
// It contains a timestamp, measurement name, some dimensions, some indices,
// and some labels.
//
// In our world, a Measurement Name might be analogous to a database name.
// A Measurement has one or more numerical Dimensions, some labels and some
// indices.
//
// The only differences between a label and an index is that an index is
// searchable and a label isn't. Because of this, an index takes up more
// memory space and so isn't always appropriate. If you're never going to
// need to search for a given string then it's best off using a label for the
// sake of resources and speed.
//
// Internally, Measurements are deduplicated by deriving a Measurement ID of
// the format:
//
//	id := name + \0x00 + indexName + \0x00 + indexValue + \0x00 + measurement_timestamp_in_nanoseconds + \0x00
//
// and then base64 encoded.
//
// This does mean there's the potential for collisions, should multiple Measurements
// have the same name, index, and timestamp (to the nanosecond); it's _unlikely_ to
// happen, but it's possible. With this in mind, indexing on a sensor ID, or
// something unique to the creator of a Measurement is always smart
type Measurement struct {
	When       time.Time          `json:"when"`
	Name       string             `json:"name"`
	Dimensions map[string]float64 `json:"dimensions"`
	Labels     map[string]string  `json:"labels"`
	Indices    map[string]string  `json:"indices"`
}

// Validate returns an error if:
//
//  1. The Measurement name is empty
//  2. The Measurement has no Dimensions
//
// If the Measurement has no indices, we create one called `_default_index`
// with the same value as the Measurement name. This exists purely to make
// deduplication easier and can be ignored by pretty much everything
//
// Without these three elements, a Measurement is functionally meaningless
func (m *Measurement) Validate() error {
	if len(m.Name) == 0 {
		return ErrEmptyName
	}

	if len(m.Dimensions) == 0 {
		return ErrNoDimensions
	}

	if len(m.Indices) == 0 {
		m.Indices = map[string]string{
			DefaultIndexName: m.Name,
		}
	}

	return nil
}

func (m Measurement) dts() string {
	return m.When.Format(dtsFmt)
}

func (m Measurement) ids() (ids []string) {
	ids = make([]string, 0, len(m.Indices))
	ns := m.When.UnixNano()

	nsBuf := make([]byte, binary.MaxVarintLen64)
	_ = binary.PutVarint(nsBuf, ns)

	nulBytes := []byte{'\x00'}

	for iK, iV := range m.Indices {
		ids = append(ids, base64.StdEncoding.EncodeToString(slices.Concat(
			[]byte(m.Name),
			nulBytes,
			[]byte(iK),
			nulBytes,
			[]byte(iV),
			nulBytes,
			nsBuf,
			nulBytes,
		)))
	}

	return
}

func (m Measurement) fields() (f map[string]measurementFieldType, err error) {
	f = make(map[string]measurementFieldType)

	for k := range m.Dimensions {
		f[k] = dimension
	}

	for k := range m.Indices {
		if _, ok := f[k]; ok {
			err = ErrFieldInUse

			return
		}

		f[k] = index
	}

	for k := range m.Labels {
		if _, ok := f[k]; ok {
			err = ErrFieldInUse

			return
		}

		f[k] = label
	}

	return
}
