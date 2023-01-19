package ydb

import (
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelTrace "go.opentelemetry.io/otel/trace"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func Retry(tracer otelTrace.Tracer, d Detailer) (t trace.Retry) {
	if tracer == nil {
		tracer = otel.Tracer(tracerID)
	}
	t.OnRetry = func(info trace.RetryLoopStartInfo) func(trace.RetryLoopIntermediateInfo) func(trace.RetryLoopDoneInfo) {
		if d.Details()&trace.RetryEvents != 0 {
			start := startSpan(
				tracer,
				info.Context,
				"ydb_retry",
				attribute.Bool("idempotent", info.Idempotent),
			)
			if info.NestedCall {
				start.RecordError(fmt.Errorf("nested call"))
			}
			return func(info trace.RetryLoopIntermediateInfo) func(trace.RetryLoopDoneInfo) {
				intermediate(start, info.Error)
				return func(info trace.RetryLoopDoneInfo) {
					finish(start,
						info.Error,
						attribute.Int("attempts", info.Attempts),
					)
				}
			}
		}
		return nil
	}
	return t
}
