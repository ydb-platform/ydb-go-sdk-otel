package ydb

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	otelTrace "go.opentelemetry.io/otel/trace"
)

func Ratelimiter(tracer otelTrace.Tracer, details trace.Details) (t trace.Ratelimiter) {
	return t
}
