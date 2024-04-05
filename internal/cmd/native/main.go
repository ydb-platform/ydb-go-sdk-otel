package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
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
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
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

	db, err := ydb.Open(ctx, os.Getenv("YDB_CONNECTION_STRING"),
		ydbOtel.WithTraces(
			ydbOtel.WithTracer(tracer),
			ydbOtel.WithDetails(trace.DetailsAll),
		),
	)
	if err != nil {
		panic(err)
	}
	defer func() { _ = db.Close(ctx) }()

	err = prepareSchema(ctx, db.Table(), path.Join(db.Name(), "/"), "series")
	if err != nil {
		panic(err)
	}

	err = upsertData(ctx,
		db.Table(),
		path.Join(db.Name(), "/"),
		"series",
		rand.Int63n(3000), //nolint:gosec
	)
	if err != nil {
		log.Println(err)
	}

	_, err = scanDefault(
		ctx,
		db.Table(),
		path.Join(db.Name(), "/"),
		rand.Int63n(1000), //nolint:gosec
	)
	if err != nil {
		log.Println(err)
	}

	_, err = scanSelect(
		ctx,
		db.Table(),
		path.Join(db.Name(), "/"),
		rand.Int63n(25000), //nolint:gosec
	)
	if err != nil {
		log.Println(err)
	}

	_, err = selectTx(
		ctx,
		db.Table(),
		path.Join(db.Name(), "/"),
		rand.Int63n(25000), //nolint:gosec
	)
	if err != nil {
		log.Println(err)
	}

	time.Sleep(time.Minute)
}

func prepareSchema(ctx context.Context, c table.Client, prefix, tableName string) (err error) {
	_ = c.Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			return s.DropTable(ctx, path.Join(prefix, tableName))
		},
		table.WithIdempotent(),
		table.WithLabel("prepareSchema => drop table"),
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
		table.WithLabel("prepareSchema => create table"),
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
			table.WithLabel("upsertData"),
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
		table.WithLabel("scanSelect"),
	)
	return count, err
}

func selectTx(ctx context.Context, c table.Client, prefix string, limit int64) (count uint64, err error) {
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
	err = c.DoTx(ctx,
		func(ctx context.Context, tx table.TransactionActor) error {
			var res result.StreamResult
			count = 0
			res, err = tx.Execute(ctx, query, table.NewQueryParameters())
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
		table.WithLabel("selectTx"),
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
		table.WithLabel("scanDefault"),
	)
	return count, err
}
