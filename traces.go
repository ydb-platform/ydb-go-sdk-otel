package ydb

import (
	"go.opentelemetry.io/otel"
	otelTrace "go.opentelemetry.io/otel/trace"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func WithTraces(tracer otelTrace.Tracer, details trace.Details) ydb.Option {
	if tracer == nil {
		tracer = otel.Tracer(tracerID)
	}
	return ydb.MergeOptions(
		ydb.WithTraceDriver(Driver(tracer, details)),
		ydb.WithTraceTable(Table(tracer, details)),
		ydb.WithTraceScripting(Scripting(tracer, details)),
		ydb.WithTraceScheme(Scheme(tracer, details)),
		ydb.WithTraceCoordination(Coordination(tracer, details)),
		ydb.WithTraceRatelimiter(Ratelimiter(tracer, details)),
		ydb.WithTraceDiscovery(Discovery(tracer, details)),
		ydb.WithTraceDatabaseSQL(DatabaseSQL(tracer, details)),
	)
}
