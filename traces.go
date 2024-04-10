package ydb

import (
	"go.opentelemetry.io/otel"
	otelTrace "go.opentelemetry.io/otel/trace"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type Detailer interface {
	Details() trace.Details
}

type config struct {
	tracer   otelTrace.Tracer
	detailer Detailer
}

type Option func(c *config)

func WithTracer(tracer otelTrace.Tracer) Option {
	return func(c *config) {
		c.tracer = tracer
	}
}

func WithDetails(d trace.Details) Option {
	return func(c *config) {
		c.detailer = d
	}
}

func WithDetailer(d Detailer) Option {
	return func(c *config) {
		c.detailer = d
	}
}

func makeConfig(opts ...Option) *config {
	c := &config{
		detailer: trace.DetailsAll,
	}
	for _, opt := range opts {
		opt(c)
	}
	if c.tracer == nil {
		c.tracer = otel.Tracer(tracerID)
	}
	return c
}

func WithTraces(opts ...Option) ydb.Option {
	c := makeConfig(opts...)
	return ydb.MergeOptions(
		ydb.WithTraceDriver(driver(c)),
		ydb.WithTraceTable(table(c)),
		ydb.WithTraceScripting(scripting(c)),
		ydb.WithTraceScheme(scheme(c)),
		ydb.WithTraceCoordination(coordination(c)),
		ydb.WithTraceRatelimiter(ratelimiter(c)),
		ydb.WithTraceDiscovery(discovery(c)),
		ydb.WithTraceDatabaseSQL(databaseSQL(c)),
		ydb.WithTraceRetry(retry(c)),
	)
}
