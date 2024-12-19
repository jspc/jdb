package jdb

import (
	"time"
)

// Options can be passed to Query* functions on Database
// and allow for slicing Measurements based on timestamps
// according to the rules below
type Options struct {
	// From defines the earliest timestamp to return Measurements
	// for. It is inclusive, which is to say that if the time is set
	// to `14:45:00 30th April 2024` and there is a record with that
	// precise timestamp, then that record will be included.
	//
	// This field is ignored if `Since` is set. If this field is unset
	// and To is set then From implies "All data from the start of time"
	From time.Time `json:"from" form:"from"`

	// To defines the latest timestamp to return Measurements for.
	// Similarly to From, if this field is empty and From is set, then
	// the implication is "All records from `From` to the end".
	//
	// If both this field and Since are set, then JDB returns the last
	// `Since` duration _to_ To
	To time.Time `json:"to" form:"to"`

	// Since returns Measurements created within the Duration covered by
	// this field. If `To` is unset, then Since returns up until the
	// current time
	Since time.Duration `json:"since" form:"since"`

	// Deduplicate measurements, when you know there's going to be upserted
	// data in your database.
	//
	// This is (potentially!) a very expensive operation, depending on the
	// amount of data being returned. Because of this, we default this to
	// false, because upserting into JDB isn't necessarily the correct
	// way to use it.
	//
	// Set this to true if using `Upsert`, rather than `Insert`, where you
	// know that you're likely to have reused the same measure+timestamp+index
	// combination, and you don't want to have to deduplicate yourself
	Deduplicate bool `json:"deduplicate" form:"deduplicate"`
}

func (o Options) mRange() (from, to time.Time) {
	now := time.Now()

	if o.Since > 0 {
		if o.To.IsZero() {
			return now.Add(0 - o.Since), now
		}

		return o.To.Add(0 - o.Since), o.To
	}

	to = o.To
	if o.To.IsZero() {
		to = now
	}

	return o.From, to
}

// validMeasurements iterates through a shard and returns the measurements
// that sit within the range defined in these options
func (o Options) validMeasurements(shard []*Measurement) (out []*Measurement) {
	// Because shards are pre-sorted, we can be clever and rule out a shard
	// without even needing to iterate through it if:
	//  1. The first element is after o.To; or
	//  2. The last element is before o.From
	if len(shard) == 0 {
		return nil
	}

	from, to := o.mRange()
	if shard[0].When.After(to) || shard[len(shard)-1].When.Before(from) {
		return nil
	}

	// The maximum this slice can be is the length of the input shards,
	// so pre-allocate now, rather than continually trying to grow the
	// slice as we go
	out = make([]*Measurement, 0, len(shard))
	for _, m := range shard {
		if (m.When == from || m.When.After(from)) && (m.When == to || m.When.Before(to)) {
			out = append(out, m)
		}
	}

	return
}
