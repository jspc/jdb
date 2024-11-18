package jdb_test

import (
	"testing"

	"github.com/jspc/jdb"
)

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
