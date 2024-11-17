package jdb_test

import (
	"testing"
	"time"

	"github.com/jspc/jdb"
)

func TestMeasurement_DTS(t *testing.T) {
	ts := time.Unix(1731874198, 0)

	for _, test := range []struct {
		name   string
		when   time.Time
		expect string
	}{
		{"empty/ zero timestamp", time.Time{}, "0001-01-01_00"},
		{"arbitrary timestamp", ts, "2024-11-17_20"},
	} {
		t.Run(test.name, func(t *testing.T) {
			rcvd := jdb.Measurement{When: test.when}.DTS()

			if test.expect != rcvd {
				t.Errorf("expected %q, received %q", test.expect, rcvd)
			}
		})
	}
}

func TestMeasurement_Validate(t *testing.T) {
	for _, test := range []struct {
		name      string
		m         jdb.Measurement
		expectErr bool
	}{
		{"Empty measurement name should fail", jdb.Measurement{}, true},
		{"Empty dimensions should fail", jdb.Measurement{Name: "My Measurement"}, true},
		{"When specified fields are set, validation succedes", jdb.Measurement{Name: "My Measurement", Dimensions: map[string]float64{"counter": 100}}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := test.m.Validate()

			if test.expectErr == (err == nil) {
				t.Errorf("expected: %v, received %#v", test.expectErr, err)
			}
		})
	}
}
