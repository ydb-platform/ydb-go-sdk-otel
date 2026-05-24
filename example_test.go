package ydb_test

import (
	"context"
	"os"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/log/global"
	otelLog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/metric"

	ydbOtel "github.com/ydb-platform/ydb-go-sdk-otel"
)

func ExampleWithTraces() {
	// Configure global TracerProvider before opening the driver.
	// See go.opentelemetry.io/otel/sdk/trace and OTLP trace exporters.
	tracer := otel.Tracer("my-service")

	db, err := ydb.Open(
		context.Background(),
		os.Getenv("YDB_CONNECTION_STRING"),
		ydbOtel.WithTraces(
			ydbOtel.WithTracer(tracer),
			ydbOtel.WithDetails(trace.DetailsAll),
		),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close(context.Background())
	}()

	_ = db
}

func ExampleWithMetrics() {
	// Configure global MeterProvider before opening the driver.
	// See go.opentelemetry.io/otel/sdk/metric and OTLP metric exporters.
	var customMeter metric.Meter // optional, defaults to otel.Meter("ydb-go-sdk")

	db, err := ydb.Open(
		context.Background(),
		os.Getenv("YDB_CONNECTION_STRING"),
		ydbOtel.WithMetrics(
			ydbOtel.WithMeter(customMeter),
			ydbOtel.WithMetricsDetailer(trace.DetailsAll),
		),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close(context.Background())
	}()

	_ = db
}

func ExampleWithLogger() {
	// Configure global LoggerProvider before opening the driver.
	// See go.opentelemetry.io/otel/sdk/log and OTLP log exporters.
	var customLogger otelLog.Logger // optional, defaults to global.Logger("ydb-go-sdk")

	db, err := ydb.Open(
		context.Background(),
		os.Getenv("YDB_CONNECTION_STRING"),
		ydbOtel.WithLogger(
			ydbOtel.WithLogLogger(customLogger),
			ydbOtel.WithLogDetailer(trace.DetailsAll),
		),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close(context.Background())
	}()

	_ = db
}

func Example_openTelemetry() {
	// Typical setup: traces, metrics and logs via global OpenTelemetry providers.
	_ = global.Logger("my-service")
	_ = otel.Tracer("my-service")
	_ = otel.Meter("my-service")

	db, err := ydb.Open(
		context.Background(),
		os.Getenv("YDB_CONNECTION_STRING"),
		ydbOtel.WithTraces(
			ydbOtel.WithTracer(otel.Tracer("my-service")),
			ydbOtel.WithDetailer(trace.DetailsAll),
		),
		ydbOtel.WithMetrics(
			ydbOtel.WithMeter(otel.Meter("my-service")),
			ydbOtel.WithMetricsDetailer(trace.DetailsAll),
		),
		ydbOtel.WithLogger(
			ydbOtel.WithLogLogger(global.Logger("my-service")),
			ydbOtel.WithLogDetailer(trace.DetailsAll),
		),
		ydbOtel.WithLogTraces(
			ydbOtel.WithLogLogger(global.Logger("my-service")),
			ydbOtel.WithLogDetailer(trace.DetailsAll),
		),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close(context.Background())
	}()

	_ = db
}
