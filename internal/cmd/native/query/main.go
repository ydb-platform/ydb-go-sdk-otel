package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"path"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	otelTrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	ydbOtel "github.com/ydb-platform/ydb-go-sdk-otel"
)

var (
	stopAfter    = flag.Duration("stop-after", 0, "define -stop-after=1m for limit time of benchmark")
	collectorURL = os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
)

func init() {
	//log.SetOutput(io.Discard)
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
		ydbOtel.WithTraces(),
		ydb.WithDiscoveryInterval(time.Second),
	)
	if err != nil {
		panic(err)
	}
	defer func() { _ = db.Close(ctx) }()

	err = prepareSchema(ctx, tracer, db.Query(), path.Join(db.Name(), "/"), "series")
	if err != nil {
		panic(err)
	}

	err = upsertData(ctx, tracer, db.Query(), path.Join(db.Name(), "/"), "series",
		rand.Int63n(3000), //nolint:gosec
	)
	if err != nil {
		log.Println(err)
	}

	_, err = selectSession(ctx, tracer, db.Query(), path.Join(db.Name(), "/"),
		rand.Int63n(1000), //nolint:gosec
	)
	if err != nil {
		log.Println(err)
	}

	_, err = selectTx(ctx, tracer, db.Query(), path.Join(db.Name(), "/"),
		rand.Int63n(25000), //nolint:gosec
	)
	if err != nil {
		log.Println(err)
	}

	time.Sleep(time.Minute)
}

func prepareSchema(ctx context.Context, tracer trace.Tracer, c query.Client, prefix, tableName string) (err error) {
	ctx, span := tracer.Start(ctx, "prepareSchema")
	defer span.End()

	_ = c.Exec(ctx, fmt.Sprintf("DROP TABLE `%s`", path.Join(prefix, tableName)),
		query.WithIdempotent(),
		query.WithLabel("prepareSchema => drop table"),
	)
	return c.Exec(ctx,
		fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s (
				series_id Uint64,
				title Text,
				series_info Text,
				release_date Date,
				comment Text,
				
				PRIMARY KEY(series_id)
			)
		`, "`"+path.Join(prefix, "series")+"`"),
		query.WithTxControl(query.NoTx()),
		query.WithIdempotent(),
		query.WithLabel("prepareSchema => create table"),
	)
}

func upsertData(ctx context.Context, tracer trace.Tracer, c query.Client, prefix, tableName string, rowsLen int64) (err error) {
	ctx, span := tracer.Start(ctx, "upsertData")
	defer span.End()

	q := fmt.Sprintf(`
		DECLARE $values AS List<Struct<
			series_id: Uint64,
			title: Text,
			series_info: Text,
			release_date: Date,
			comment: Text>>;

		REPLACE INTO %s
		SELECT
			series_id,
			title,
			series_info,
			release_date,
			comment
		FROM AS_TABLE($values);
	`, "`"+path.Join(prefix, "series")+"`")
	batchSize := int64(1000)
	for shift := int64(0); shift < rowsLen; shift += batchSize {
		rows := make([]types.Value, 0, batchSize)
		for i := int64(0); i < batchSize; i++ {
			rows = append(rows, types.StructValue(
				types.StructFieldValue("series_id", types.Uint64Value(uint64(i+shift+3))),
				types.StructFieldValue("title", types.TextValue(fmt.Sprintf("series No. %d title", i+shift+3))),
				types.StructFieldValue("series_info", types.TextValue(fmt.Sprintf("series No. %d info", i+shift+3))),
				types.StructFieldValue("release_date", types.DateValueFromTime(time.Now())),
				types.StructFieldValue("comment", types.TextValue(fmt.Sprintf("series No. %d comment", i+shift+3))),
			))
		}
		err = c.Exec(ctx, q,
			query.WithParameters(
				ydb.ParamsBuilder().Param("$values").BeginList().AddItems(rows...).EndList().Build(),
			),
			query.WithIdempotent(),
			query.WithLabel("upsertData"),
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func selectTx(ctx context.Context, tracer trace.Tracer, c query.Client, prefix string, limit int64) (count uint64, err error) {
	ctx, span := tracer.Start(ctx, "selectTx")
	defer span.End()

	q := fmt.Sprintf(`
		SELECT
			series_id,
			title,
			release_date
		FROM %s LIMIT %d;`,
		"`"+path.Join(prefix, "series")+"`",
		limit,
	)
	err = c.DoTx(ctx,
		func(ctx context.Context, tx query.TxActor) error {
			count = 0
			r, err := tx.Query(ctx, q)
			if err != nil {
				return err
			}
			defer func() {
				_ = r.Close(ctx)
			}()
			var (
				id    uint64
				title *string
				date  *time.Time
			)
			log.Printf("> selectTx:\n")
			for {
				rs, err := r.NextResultSet(ctx)
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}

					return err
				}
				for {
					row, err := rs.NextRow(ctx)
					if err != nil {
						if errors.Is(err, io.EOF) {
							break
						}

						return err
					}
					count++
					err = row.ScanNamed(
						query.Named("series_id", &id),
						query.Named("title", &title),
						query.Named("release_date", &date),
					)
					if err != nil {
						return err
					}
					log.Printf(
						"  > %d %s %s\n",
						id, *title, *date,
					)
				}
			}
			return nil
		},
		query.WithIdempotent(),
		query.WithLabel("selectTx"),
	)
	return count, err
}

func selectSession(ctx context.Context, tracer trace.Tracer, c query.Client, prefix string, limit int64) (count uint64, err error) {
	ctx, span := tracer.Start(ctx, "selectSession")
	defer span.End()

	q := fmt.Sprintf(`
		SELECT
			series_id,
			title,
			release_date
		FROM %s LIMIT %d;`,
		"`"+path.Join(prefix, "series")+"`",
		limit,
	)
	err = c.Do(ctx,
		func(ctx context.Context, s query.Session) error {
			count = 0
			r, err := s.Query(ctx, q)
			if err != nil {
				return err
			}
			defer func() {
				_ = r.Close(ctx)
			}()
			var (
				id    uint64
				title *string
				date  *time.Time
			)
			log.Printf("> selectSession:\n")
			for {
				rs, err := r.NextResultSet(ctx)
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}

					return err
				}
				for {
					row, err := rs.NextRow(ctx)
					if err != nil {
						if errors.Is(err, io.EOF) {
							break
						}

						return err
					}
					count++
					err = row.ScanNamed(
						query.Named("series_id", &id),
						query.Named("title", &title),
						query.Named("release_date", &date),
					)
					if err != nil {
						return err
					}
					log.Printf(
						"  > %d %s %s\n",
						id, *title, *date,
					)
				}
			}
			return nil
		},
		query.WithIdempotent(),
		query.WithLabel("scan"),
	)
	return count, err
}
