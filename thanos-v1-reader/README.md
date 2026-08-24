# Thanos v1 reader experiment

This package implements [custom table provider](https://datafusion.apache.org/library-user-guide/custom-table-providers.html) for thanos storage [format](https://thanos.io/tip/thanos/storage.md/)

It starts by defining the objstore, which can be s3:// or file://. Underlying access pattern is using [OpenDAL](https://docs.rs/opendal/latest/opendal/) library which handles fetching, concurrency, retry, caching, etc. 

Any thanos block reader is using OpenDAL wrapper, and async via tokio. Tokio is used within datafusion, thus it's important for interoperability here.

From the custom table provider goal is implementing thanos-store [API](https://github.com/thanos-io/thanos/blob/main/pkg/store/storepb/rpc.proto). Thus this rust impl could serve thanos-v1 data format...but also this code can be extended to other storage formats (e.g. [thanos-parquet-gateway](https://github.com/thanos-io/thanos-parquet-gateway) ) 

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

This scaffold implements Flight SQL statement queries. A client sends `CommandStatementQuery`
through `GetFlightInfo`; the server plans it with DataFusion and returns a Flight SQL statement
ticket. Supplying that ticket to `DoGet` streams the Arrow record batches.

At startup, the server registers an in-memory `metrics` table:

```sql
SELECT timestamp_ms, metric, value FROM metrics WHERE metric = 'up'
```

## Flight SQL CLI

Install the Arrow Flight SQL client matching this server's Arrow Flight version:

```bash
cargo install arrow-flight --version 58.4.0 \
  --features "cli,flight-sql,tls-ring" \
  --bin flight_sql_client
```

With the server running, issue a query:

```bash
flight_sql_client --host 127.0.0.1 --port 50051 \
  statement-query "SELECT 1"
```

The service currently supports statement queries. Authentication, prepared statements, metadata,
uploads, actions, query polling, and `get_schema` remain unimplemented extension points.

## Check

```bash
cargo check
```
