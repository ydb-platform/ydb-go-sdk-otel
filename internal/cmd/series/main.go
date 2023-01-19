package main

import (
	"context"
	"database/sql"
	"flag"
	"io"
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

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"

	ydbOtel "github.com/ydb-platform/ydb-go-sdk-otel"
)

const (
	tracerURL   = "http://localhost:14268/api/traces"
	serviceName = "ydb-go-sdk-otel"
)

var (
	stopAfter = flag.Duration("stop-after", 0, "define -stop-after=1m for limit time of benchmark")
	prefix    = flag.String("prefix", "ydb-go-sdk-otel/series", "prefix path for tables")
)

func init() {
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 500
	log.SetOutput(io.Discard)
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
	flag.Parse()

	var (
		ctx    context.Context
		cancel context.CancelFunc
	)
	if *stopAfter == 0 {
		ctx, cancel = context.WithCancel(context.Background())
	} else {
		ctx, cancel = context.WithTimeout(context.Background(), *stopAfter)
	}
	defer cancel()

	tp, err := tracerProvider(tracerURL)
	if err != nil {
		panic(err)
	}
	defer func(ctx context.Context) {
		// Do not make the application hang when it is shutdown.
		ctx, cancel = context.WithTimeout(ctx, time.Second*5)
		defer cancel()
		_ = tp.Shutdown(ctx)
	}(ctx)

	tracer := tp.Tracer(serviceName)

	ctx, span := tracer.Start(ctx, "main")
	defer span.End()

	nativeDriver, err := ydb.Open(ctx, os.Getenv("YDB_CONNECTION_STRING"),
		ydb.WithDiscoveryInterval(5*time.Second),
		ydbOtel.WithTraces(ydbOtel.WithTracer(tracer)),
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

	prefix := path.Join(cc.Name(), *prefix)

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

	for i := 0; i < 100; i++ {
		wg.Add(3)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					err = fillTablesWithData(ctx, db, prefix)
					if err != nil {
						log.Printf("fill tables with data error: %v\n", err)
					}
				}
			}
		}()
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					err = selectDefault(ctx, db, prefix)
					if err != nil {
						log.Println(err)
					}
				}
			}
		}()
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					err = selectScan(ctx, db, prefix)
					if err != nil {
						log.Println(err)
					}
				}
			}
		}()
	}
	wg.Wait()
}
