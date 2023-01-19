package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
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
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"

	ydbOtel "github.com/ydb-platform/ydb-go-sdk-otel"
)

const (
	tracerURL   = "http://localhost:14268/api/traces"
	serviceName = "ydb-go-sdk"
)

var (
	stopAfter = flag.Duration("stop-after", 0, "define -stop-after=1m for limit time of benchmark")
	prefix    = flag.String("prefix", "ydb-go-sdk-otel/bench", "prefix for tables")
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

	creds := ydb.WithAnonymousCredentials()
	if token, has := os.LookupEnv("YDB_ACCESS_TOKEN_CREDENTIALS"); has {
		creds = ydb.WithAccessTokenCredentials(token)
	}

	db, err := ydb.Open(
		ctx,
		os.Getenv("YDB_CONNECTION_STRING"),
		ydb.WithDialTimeout(5*time.Second),
		ydb.WithBalancer(balancers.RandomChoice()),
		creds,
		ydb.WithSessionPoolSizeLimit(300),
		ydb.WithSessionPoolIdleThreshold(time.Second*5),
		ydbOtel.WithTraces(ydbOtel.WithTracer(tracer)),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close(ctx)
	}()

	err = prepareSchema(ctx, db.Table(), path.Join(db.Name(), *prefix), "series")
	if err != nil {
		panic(err)
	}

	wg := &sync.WaitGroup{}
	for i := 0; i < 300; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Duration(rand.Int63n(int64(time.Second)))): //nolint:gosec
					switch rand.Int63n(3) { //nolint:gosec
					case 0:
						err = upsertData(ctx,
							db.Table(),
							path.Join(db.Name(), *prefix),
							"series",
							rand.Int63n(3000), //nolint:gosec
						)
						if err != nil {
							log.Println(err)
						}
					case 1:
						_, err = scanDefault(
							ctx,
							db.Table(),
							path.Join(db.Name(), *prefix),
							rand.Int63n(1000), //nolint:gosec
						)
						if err != nil {
							log.Println(err)
						}
					case 2:
						_, err = scanSelect(
							ctx,
							db.Table(),
							path.Join(db.Name(), *prefix),
							rand.Int63n(25000), //nolint:gosec
						)
						if err != nil {
							log.Println(err)
						}
					}
				}
			}
		}()
	}
	wg.Wait()
}

func prepareSchema(ctx context.Context, c table.Client, prefix, tableName string) (err error) {
	_ = c.Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			return s.DropTable(ctx, path.Join(prefix, tableName))
		},
		table.WithIdempotent(),
	)
	return c.Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			return s.CreateTable(ctx, path.Join(prefix, tableName),
				options.WithColumn("series_id", types.TypeUint64),
				options.WithColumn("title", types.Optional(types.TypeText)),
				options.WithColumn("series_info", types.Optional(types.TypeText)),
				options.WithColumn("release_date", types.Optional(types.TypeDate)),
				options.WithColumn("comment", types.Optional(types.TypeText)),
				options.WithPrimaryKeyColumn("series_id"),
				options.WithPartitioningSettings(
					options.WithMinPartitionsCount(10),
					options.WithMaxPartitionsCount(50),
					options.WithPartitioningByLoad(options.FeatureEnabled),
					options.WithPartitioningBySize(options.FeatureEnabled),
					options.WithPartitionSizeMb(2048),
				),
			)
		},
		table.WithIdempotent(),
	)
}

func upsertData(ctx context.Context, c table.Client, prefix, tableName string, rowsLen int64) (err error) {
	batchSize := int64(1000)
	for shift := int64(0); shift < rowsLen; shift += batchSize {
		rows := make([]types.Value, 0, batchSize)
		for i := int64(0); i < batchSize; i++ {
			rows = append(rows, types.StructValue(
				types.StructFieldValue("series_id", types.Uint64Value(uint64(i+shift+3))),
				types.StructFieldValue("title", types.UTF8Value(fmt.Sprintf("series No. %d title", i+shift+3))),
				types.StructFieldValue("series_info", types.UTF8Value(fmt.Sprintf("series No. %d info", i+shift+3))),
				types.StructFieldValue("release_date", types.DateValueFromTime(time.Now())),
				types.StructFieldValue("comment", types.UTF8Value(fmt.Sprintf("series No. %d comment", i+shift+3))),
			))
		}
		err = c.Do(ctx,
			func(ctx context.Context, session table.Session) (err error) {
				return session.BulkUpsert(
					ctx,
					path.Join(prefix, tableName),
					types.ListValue(rows...),
				)
			},
			table.WithIdempotent(),
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func scanSelect(ctx context.Context, c table.Client, prefix string, limit int64) (count uint64, err error) {
	query := fmt.Sprintf(`
		PRAGMA TablePathPrefix("%s");
		SELECT
			series_id,
			title,
			release_date
		FROM series LIMIT %d;`,
		prefix,
		limit,
	)
	err = c.Do(ctx,
		func(ctx context.Context, s table.Session) error {
			var res result.StreamResult
			count = 0
			res, err = s.StreamExecuteScanQuery(ctx, query, table.NewQueryParameters())
			if err != nil {
				return err
			}
			defer func() {
				_ = res.Close()
			}()
			var (
				id    *uint64
				title *string
				date  *time.Time
			)
			log.Printf("> scan_select:\n")
			for res.NextResultSet(ctx) {
				for res.NextRow() {
					count++
					err = res.ScanNamed(
						named.Optional("series_id", &id),
						named.Optional("title", &title),
						named.Optional("release_date", &date),
					)
					if err != nil {
						return err
					}
					log.Printf(
						"  > %d %s %s\n",
						*id, *title, *date,
					)
				}
			}
			return res.Err()
		},
		table.WithIdempotent(),
	)
	return count, err
}

func scanDefault(ctx context.Context, c table.Client, prefix string, limit int64) (count uint64, err error) {
	var (
		query = fmt.Sprintf(`
		PRAGMA TablePathPrefix("%s");
		SELECT
			series_id,
			title,
			release_date
		FROM series LIMIT %d;`,
			prefix,
			limit,
		)
		txControl = table.DefaultTxControl()
	)
	err = c.Do(ctx,
		func(ctx context.Context, s table.Session) error {
			var res result.StreamResult
			count = 0
			_, res, err = s.Execute(ctx, txControl, query, table.NewQueryParameters())
			if err != nil {
				return err
			}
			defer func() {
				_ = res.Close()
			}()
			var (
				id    *uint64
				title *string
				date  *time.Time
			)
			log.Printf("> scan_default:\n")
			for res.NextResultSet(ctx) {
				for res.NextRow() {
					count++
					err = res.ScanNamed(
						named.Optional("series_id", &id),
						named.Optional("title", &title),
						named.Optional("release_date", &date),
					)
					if err != nil {
						return err
					}
					log.Printf(
						"  > %d %s %s\n",
						*id, *title, *date,
					)
				}
			}
			return res.Err()
		},
		table.WithIdempotent(),
	)
	return count, err
}
