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

func WithTraces(opts ...Option) ydb.Option {
	cfg := &config{
		tracer:   otel.Tracer(tracerID),
		detailer: trace.DetailsAll,
	}
	for _, opt := range opts {
		opt(cfg)
	}
	return ydb.MergeOptions(
		ydb.WithTraceDriver(Driver(cfg)),
		ydb.WithTraceTable(Table(cfg)),
		ydb.WithTraceScripting(Scripting(cfg)),
		ydb.WithTraceScheme(Scheme(cfg)),
		ydb.WithTraceCoordination(Coordination(cfg)),
		ydb.WithTraceRatelimiter(Ratelimiter(cfg)),
		ydb.WithTraceDiscovery(Discovery(cfg)),
		ydb.WithTraceDatabaseSQL(DatabaseSQL(cfg)),
	)
}
