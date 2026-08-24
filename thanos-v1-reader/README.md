# Thanos v1 reader experiment

A minimal DataFusion-backed [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html)
server. It is a starting point for connecting a query client to data served by a future Thanos
reader implementation.

## Run

```bash
cargo run
```

The server listens on `127.0.0.1:50051` by default. Set `FLIGHT_LISTEN_ADDR` to override it:

```bash
FLIGHT_LISTEN_ADDR=0.0.0.0:50051 cargo run
```

## Query flow

This scaffold uses the base Arrow Flight protocol, not Flight SQL. Send UTF-8 SQL in
`FlightDescriptor.cmd` to `GetFlightInfo`; the response contains an endpoint whose ticket is the
same SQL. Send that ticket to `DoGet` to receive Arrow record batches.

At startup, the server registers an in-memory `metrics` table:

```sql
SELECT timestamp_ms, metric, value FROM metrics WHERE metric = 'up'
```

The `get_flight_info` and `do_get` handlers are implemented. Authentication, uploads, actions,
query polling, and `get_schema` deliberately return `UNIMPLEMENTED` so they remain clear
extension points.

## Check

```bash
cargo check
```
This package implements [custom table provider](https://datafusion.apache.org/library-user-guide/custom-table-providers.html) for thanos storage [format](https://thanos.io/tip/thanos/storage.md/)

It starts by defining the objstore, which can be s3:// or file://. Underlying access pattern is using [OpenDAL](https://docs.rs/opendal/latest/opendal/) library which handles fetching, concurrency, retry, caching, etc. 

Any thanos block reader is using OpenDAL wrapper, and async via tokio. Tokio is used within datafusion, thus it's important for interoperability here.