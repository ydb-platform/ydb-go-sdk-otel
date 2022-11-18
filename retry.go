package ydb

import (
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	otelTrace "go.opentelemetry.io/otel/trace"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func Retry(tracer otelTrace.Tracer, details trace.Details) (t trace.Retry) {
	if details&trace.RetryEvents != 0 {
		t.OnRetry = func(info trace.RetryLoopStartInfo) func(trace.RetryLoopIntermediateInfo) func(trace.RetryLoopDoneInfo) {
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
	}
	return t
}
