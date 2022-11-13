package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"os"
	"path"
	"sync"
	"time"

	jaegerPropogator "go.opentelemetry.io/contrib/propagators/jaeger"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.12.0"

	ydbOtel "github.com/ydb-platform/ydb-go-sdk-otel"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

const (
	tracerURL   = "http://localhost:14268/api/traces"
	serviceName = "ydb-go-sdk-otel"
	prefix      = "ydb-go-sdk-otel/series"
)

func init() {
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 500
}

func tracerProvider(url string) (*tracesdk.TracerProvider, error) {
	exp, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(url)))
	if err != nil {
		return nil, err
	}

	otel.SetTextMapPropagator(jaegerPropogator.Jaeger{})

	tp := tracesdk.NewTracerProvider(
		// Always be sure to batch in production.
		tracesdk.WithBatcher(exp),
		// Record information about this application in a Resource.
		tracesdk.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(serviceName),
		)),
		tracesdk.WithSampler(tracesdk.AlwaysSample()),
	)

	otel.SetTracerProvider(tp)

	return tp, nil
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tp, err := tracerProvider(tracerURL)
	if err != nil {
		panic(err)
	}
	defer func(ctx context.Context) {
		// Do not make the application hang when it is shutdown.
		ctx, cancel = context.WithTimeout(ctx, time.Second*5)
		defer cancel()
		if err = tp.Shutdown(ctx); err != nil {
			panic(err)
		}
	}(ctx)

	tr := tp.Tracer(serviceName)

	ctx, span := tr.Start(ctx, "main")
	defer span.End()

	nativeDriver, err := ydb.Open(ctx, os.Getenv("YDB_CONNECTION_STRING"),
		ydb.WithDiscoveryInterval(5*time.Second),
		ydbOtel.WithTraces(trace.DetailsAll),
	)
	if err != nil {
		panic(err)
	}
	defer func() { _ = nativeDriver.Close(ctx) }()

	connector, err := ydb.Connector(nativeDriver)
	if err != nil {
		panic(err)
	}

	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()

	cc, err := ydb.Unwrap(db)
	if err != nil {
		panic(err)
	}

	prefix := path.Join(cc.Name(), prefix)

	err = sugar.RemoveRecursive(ctx, cc, prefix)
	if err != nil {
		panic(err)
	}

	err = prepareSchema(ctx, db, prefix)
	if err != nil {
		panic(err)
	}

	err = fillTablesWithData(ctx, db, prefix)
	if err != nil {
		panic(err)
	}

	wg := sync.WaitGroup{}

	for i := 0; i < 10; i++ {
		wg.Add(3)
		go func() {
			defer wg.Done()
			for {
				err = fillTablesWithData(ctx, db, prefix)
				if err != nil {
					log.Fatalf("fill tables with data error: %v", err)
				}
			}
		}()
		go func() {
			defer wg.Done()
			for {
				err = selectDefault(ctx, db, prefix)
				if err != nil {
					log.Fatal(err)
				}
			}
		}()
		go func() {
			defer wg.Done()
			for {
				err = selectScan(ctx, db, prefix)
				if err != nil {
					log.Fatal(err)
				}
			}
		}()
	}
	wg.Wait()
}
