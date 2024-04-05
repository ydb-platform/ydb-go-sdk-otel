package main

import (
	"context"
	"database/sql"
	"flag"
	"io"
	"log"
	"os"
	"path"
	"time"

	ydbOtel "github.com/ydb-platform/ydb-go-sdk-otel"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	otelTrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var (
	stopAfter    = flag.Duration("stop-after", 0, "define -stop-after=1m for limit time of benchmark")
	collectorURL = os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
)

func init() {
	log.SetOutput(io.Discard)
}

func initTracer() func(context.Context) error {
	exporter, err := otlptrace.New(
		context.Background(),
		otlptracehttp.NewClient(
			otlptracehttp.WithInsecure(),
			otlptracehttp.WithEndpoint(collectorURL),
		),
	)
	if err != nil {
		log.Fatalf("Failed to create exporter: %v", err)
	}

	resources, err := resource.New(
		context.Background(),
		resource.WithAttributes(
			attribute.String("service.name", "main"),
		),
	)
	if err != nil {
		log.Fatalf("Could not set resources: %v", err)
	}

	otel.SetTracerProvider(
		otelTrace.NewTracerProvider(
			otelTrace.WithSampler(otelTrace.AlwaysSample()),
			otelTrace.WithBatcher(exporter),
			otelTrace.WithResource(resources),
		),
	)
	return exporter.Shutdown
}

func main() {
	cleanup := initTracer()
	defer func() {
		_ = cleanup(context.Background())
	}()

	var (
		tracer = otel.Tracer("main")
		ctx    context.Context
		cancel context.CancelFunc
	)
	if *stopAfter == 0 {
		ctx, cancel = context.WithCancel(context.Background())
	} else {
		ctx, cancel = context.WithTimeout(context.Background(), *stopAfter)
	}
	defer cancel()

	defer func() {
		time.Sleep(15 * time.Second)
	}()

	ctx, span := tracer.Start(ctx, "main")
	defer span.End()

	nativeDriver, err := ydb.Open(ctx, os.Getenv("YDB_CONNECTION_STRING"),
		ydb.WithDiscoveryInterval(5*time.Second),
		ydb.WithBalancer(balancers.PreferLocalDC(balancers.RandomChoice())),
		ydbOtel.WithTraces(
			ydbOtel.WithTracer(tracer),
			ydbOtel.WithDetails(trace.DetailsAll),
		),
	)
	if err != nil {
		panic(err)
	}
	defer func() { _ = nativeDriver.Close(ctx) }()

	connector, err := ydb.Connector(nativeDriver,
		ydb.WithTablePathPrefix(path.Join(nativeDriver.Name(), "database/sql")),
		ydb.WithAutoDeclare(),
	)
	if err != nil {
		panic(err)
	}

	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()

	db.SetMaxOpenConns(50)
	db.SetMaxIdleConns(50)
	db.SetConnMaxIdleTime(time.Second)
	db.SetConnMaxLifetime(time.Minute)

	err = prepareSchema(ctx, db)
	if err != nil {
		panic(err)
	}

	err = fillTablesWithData(ctx, db)
	if err != nil {
		panic(err)
	}

	err = selectDefault(ctx, db)
	if err != nil {
		log.Println(err)
	}

	err = selectScan(ctx, db)
	if err != nil {
		log.Println(err)
	}

	err = selectTx(ctx, db)
	if err != nil {
		log.Println(err)
	}
}
