# jdb - the naive timeseries database

jdb is an embeddable Schemaless Timeseries Database, queried in-memory, and with on-disc persistence.

It is deliberately naive and is designed to be 'good-enough'. It wont solve all of your woes, it wont handle petabytes of scale, and it wont make your applications more enterprisey.

It will, however, give you a reasonably quick way of storing timeseries, querying against an index or time range, and provide de-duplication guarantees.

In essence, jdb stores measurements in a series of nested `map`s, with extra `map`s acting as indices, and protgates writes with a mutex. All writes use the same mutex, making it decent and simple enough for relatively few measurement types, or where writes don't need to complete in jig time. And, in fact, that's kind of the point; jdb is designed to be aggresively immediately consistent and simple for me to maintain.

If the above sucks for your usecase then that's cool, too- just because I've written this doesn't mean you have to use this.

## Measurements

A measurement looks like:

```golang
type Measurement struct {
    When       time.Time          `json:"when"`
    Name       string             `json:"name"`
    Dimensions map[string]float64 `json:"dimensions"`
    Labels     map[string]string  `json:"labels"`
    Indices    map[string]string  `json:"indices"`
}
```

In this struct, the following fields have the following meaning:

* `When`: A `time.Time` representing when this measurement should be plotted against; you can do what you want. It's used to sort ingested data, meaning that writes can occur in any order.
* `Name`: We use `Name` to group measurements together. You could easily compare this with a database in another world
* `Dimensions`: The actual, numerical, things being measured. These are stored as `float64`s, but a `float` is easily coerced to/from more or less any numeric type, so you do you babe
* `Labels`: Optional metadata for a measurement. These aren't searchable or orderable and so only really cost whatever space they take up
* `Indices`: An index can be used to lookup measurements matching specific criteria and, thus, take up more space in memory for that to happen. Think about cardinality when sussing out what `Indices` and what `Labels` to uuse

An example, in JSON, of a measurement from one of my own environmental sensors is:

```json
{
    "when": "2024-11-22T11:46:44.599303882Z",
    "name": "environment",
    "dimensions": {
      "aqi": 3,
      "co2": 806,
      "humidity": 34.83123779296875,
      "temperature": 19.743728637695312,
      "tvoc": 315
    },
    "labels": {
      "device_id": "RP2040",
      "internal_temperature": "28074",
      "uptime": "74482980"
    },
    "indices": {
      "device": "kitchen"
    }
}
```

(Side note: better go and open a kitchen window, or make sure I haven't left my lunch cooking)

## Querying

jdb will either return all matching data, or allows for time slicing with the optional argument `*jdb.Options`:

```golang
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
}
```

jdb provides two major interfaces for querying data:

### `QueryAll(name string, opts *jdb.Options)`

Return measurements for a given name (so, in the example above, `environment`), optionally using the time slice.

### `QueryAllIndex(name, index, indexValue string, opts *jdb.Options)`

Returns measurements for a given name, _and where a specific index value matches_. For the above json example, you might query `QueryAllIndex("environment", "device", "kitchen", nil)` to grab every measurement from the `kitchen` device.
