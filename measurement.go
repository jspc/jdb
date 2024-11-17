package jdb

import (
	"errors"
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
