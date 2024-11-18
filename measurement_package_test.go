package jdb

import (
	"testing"
	"time"
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
			rcvd := Measurement{When: test.when}.dts()

			if test.expect != rcvd {
				t.Errorf("expected %q, received %q", test.expect, rcvd)
			}
		})
	}
}
