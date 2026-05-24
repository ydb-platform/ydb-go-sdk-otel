# ydb-go-sdk-otel

OpenTelemetry adapter for [ydb-go-sdk](https://github.com/ydb-platform/ydb-go-sdk): traces (spans), metrics and logs from YDB driver events.

The adapter does **not** configure OpenTelemetry exporters by itself. You set up `TracerProvider`, `MeterProvider` and `LoggerProvider` in your application (or rely on auto-instrumentation / OTel SDK defaults), then pass the corresponding instruments into `ydb.Open`.

## What you get

| Adapter option | YDB SDK signal | OpenTelemetry signal |
|---|---|---|
| `WithTracer` | driver / table / query / … events | spans and traces |
| `WithMetrics` | pool sizes, latencies, counters, … | metrics via OTLP |
| `WithLogger` | SDK log events (resolver, retries, …) | log records via OTLP |

All three options are independent: enable only what you need.

## Where to get tracer, meter and logger

OpenTelemetry instruments are obtained from **global providers** after you configure exporters (OTLP, stdout, etc.):

```go
import (
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/log/global"
)

// Call once at application startup, before ydb.Open:
//   otel.SetTracerProvider(...)
//   otel.SetMeterProvider(...)
//   global.SetLoggerProvider(...)

tracer := otel.Tracer("my-service")       // go.opentelemetry.io/otel/trace.Tracer
meter := otel.Meter("my-service")         // go.opentelemetry.io/otel/metric.Meter
logger := global.Logger("my-service")     // go.opentelemetry.io/otel/log.Logger
```

Use the same instrumentation scope name (for example `"my-service"`) across tracer, meter and logger so signals correlate in the backend.

See [OpenTelemetry Go documentation](https://opentelemetry.io/docs/languages/go/) and package docs:

- traces: [go.opentelemetry.io/otel/sdk/trace](https://pkg.go.dev/go.opentelemetry.io/otel/sdk/trace)
- metrics: [go.opentelemetry.io/otel/sdk/metric](https://pkg.go.dev/go.opentelemetry.io/otel/sdk/metric)
- logs: [go.opentelemetry.io/otel/sdk/log](https://pkg.go.dev/go.opentelemetry.io/otel/sdk/log)

For OTLP export to a collector set `OTEL_EXPORTER_OTLP_ENDPOINT` (for example `localhost:4318` for HTTP) and use OTLP exporters from `go.opentelemetry.io/otel/exporters/otlp/...`.

## Quick start

```go
package main

import (
    "context"
    "os"

    "github.com/ydb-platform/ydb-go-sdk/v3"
    "github.com/ydb-platform/ydb-go-sdk/v3/trace"
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/log/global"

    ydbOtel "github.com/ydb-platform/ydb-go-sdk-otel"
)

func main() {
    ctx := context.Background()

    // 1. Configure OTel providers and exporters (TracerProvider, MeterProvider, LoggerProvider).
    //    See internal/cmd/native/query/main.go for a trace-only OTLP/HTTP example.

    tracer := otel.Tracer("my-service")
    meter := otel.Meter("my-service")
    logger := global.Logger("my-service")

    // 2. Open YDB driver with adapter options.
    db, err := ydb.Open(ctx, os.Getenv("YDB_CONNECTION_STRING"),
        ydbOtel.WithTracer(tracer, ydbOtel.WithDetailer(trace.DetailsAll)),
        ydbOtel.WithMetrics(meter, ydbOtel.WithDetailer(trace.DetailsAll)),
        ydbOtel.WithLogger(logger, ydbOtel.WithDetailer(trace.DetailsAll)),
    )
    if err != nil {
        panic(err)
    }
    defer func() { _ = db.Close(ctx) }()

    // work with db
}
```

More examples: [example_test.go](./example_test.go) (also shown on [pkg.go.dev](https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk-otel)).

## Options

### Common

`WithDetailer(trace.Detailer)` — controls which SDK events are reported. Works with `WithTracer`, `WithMetrics` and `WithLogger`:

```go
ydbOtel.WithDetailer(trace.DetailsAll)
```

### Traces

```go
ydbOtel.WithTracer(tracer, opts...)
```

### Metrics

```go
ydbOtel.WithMetrics(meter, opts...)
```

Additional metrics options:

- `WithNamespace(prefix)` — metric name prefix
- `WithSeparator(sep)` — scope separator (default `_`)
- `WithTimerBuckets(buckets)` — histogram buckets for timers

### Logs

```go
ydbOtel.WithLogger(logger, opts...)
```

If `logger` is `nil`, the adapter uses `global.Logger("ydb-go-sdk")`.

Additional log options:

- `WithLogQuery()` — log SQL/YQL query text

When trace ID is available in context, the adapter adds `otel-trace-id` to log fields for correlation with spans.

## Local development

Start YDB and an OTLP-compatible backend (Jaeger accepts OTLP on port 4318):

```bash
docker compose -f internal/cmd/configs/docker-compose.yml up -d
export YDB_CONNECTION_STRING="grpc://localhost:2136/local"
export OTEL_EXPORTER_OTLP_ENDPOINT="localhost:4318"
```

Run the sample application:

```bash
go run ./internal/cmd/native/query
```

Open Jaeger UI at http://localhost:16686.

## Related adapters

- [ydb-go-sdk-prometheus](https://github.com/ydb-platform/ydb-go-sdk-prometheus) — Prometheus metrics
- [ydb-go-sdk-zap](https://github.com/ydb-platform/ydb-go-sdk-zap) — structured logs via zap
