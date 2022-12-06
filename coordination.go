package ydb

import (
	"go.opentelemetry.io/otel"
	otelTrace "go.opentelemetry.io/otel/trace"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func Coordination(tracer otelTrace.Tracer, details trace.Details) (t trace.Coordination) {
	if tracer == nil {
		tracer = otel.Tracer(tracerID)
	}
	return t
}
