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
)

const dtsFmt = "2006-01-02_15"

type Measurement struct {
	When       time.Time          `json:"when"`
	Name       string             `json:"name"`
	Dimensions map[string]float64 `json:"dimensions"`
	Labels     map[string]string  `json:"labels"`
	Indices    map[string]string  `json:"indices"`
}

func (m Measurement) DTS() string {
	return m.When.Format(dtsFmt)
}

func (m Measurement) Validate() error {
	if len(m.Name) == 0 {
		return ErrEmptyName
	}

	if len(m.Dimensions) == 0 {
		return ErrNoDimensions
	}

	return nil
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
