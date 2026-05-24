package ydb_test

import (
	"context"
	"os"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"go.opentelemetry.io/otel"
	otelLog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/global"

	ydbOtel "github.com/ydb-platform/ydb-go-sdk-otel"
)

func ExampleWithTraces() {
	// Configure global TracerProvider before opening the driver.
	// See go.opentelemetry.io/otel/sdk/trace and OTLP trace exporters.
	tracer := otel.Tracer("my-service")
	connectionString := os.Getenv("YDB_CONNECTION_STRING")
	if connectionString == "" {
		return
	}

	db, err := ydb.Open(
		context.Background(),
		connectionString,
		ydbOtel.WithTracer(tracer, ydbOtel.WithDetailer(trace.DetailsAll)),
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
	meter := otel.Meter("my-service")
	connectionString := os.Getenv("YDB_CONNECTION_STRING")
	if connectionString == "" {
		return
	}

	db, err := ydb.Open(
		context.Background(),
		connectionString,
		ydbOtel.WithMetrics(meter, ydbOtel.WithDetailer(trace.DetailsAll)),
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
	connectionString := os.Getenv("YDB_CONNECTION_STRING")
	if connectionString == "" {
		return
	}

	db, err := ydb.Open(
		context.Background(),
		connectionString,
		ydbOtel.WithLogger(customLogger, ydbOtel.WithDetailer(trace.DetailsAll)),
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
	detailer := trace.DetailsAll
	logger := global.Logger("my-service")
	tracer := otel.Tracer("my-service")
	meter := otel.Meter("my-service")
	connectionString := os.Getenv("YDB_CONNECTION_STRING")
	if connectionString == "" {
		return
	}

	db, err := ydb.Open(
		context.Background(),
		connectionString,
		ydbOtel.WithTracer(tracer, ydbOtel.WithDetailer(detailer)),
		ydbOtel.WithMetrics(meter, ydbOtel.WithDetailer(detailer)),
		ydbOtel.WithLogger(logger, ydbOtel.WithDetailer(detailer)),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close(context.Background())
	}()

	_ = db
}
