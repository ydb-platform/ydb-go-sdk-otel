package ydb

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelTrace "go.opentelemetry.io/otel/trace"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"

	"github.com/ydb-platform/ydb-go-sdk-otel/internal/safe"
)

func Scripting(tracer otelTrace.Tracer, details trace.Details) (t trace.Scripting) {
	if tracer == nil {
		tracer = otel.Tracer(tracerID)
	}
	if details&trace.ScriptingEvents != 0 {
		t.OnExecute = func(info trace.ScriptingExecuteStartInfo) func(trace.ScriptingExecuteDoneInfo) {
			start := startSpan(
				tracer,
				info.Context,
				"ydb_scripting_execute",
				attribute.String("query", info.Query),
				attribute.String("params", safe.Stringer(info.Parameters)),
			)
			return func(info trace.ScriptingExecuteDoneInfo) {
				if info.Error == nil {
					finish(
						start,
						safe.Err(info.Result),
					)
				} else {
					finish(
						start,
						info.Error,
					)
				}
			}
		}
		t.OnStreamExecute = func(
			info trace.ScriptingStreamExecuteStartInfo,
		) func(
			trace.ScriptingStreamExecuteIntermediateInfo,
		) func(
			trace.ScriptingStreamExecuteDoneInfo,
		) {
			start := startSpan(
				tracer,
				info.Context,
				"ydb_scripting_stream_execute",
				attribute.String("query", info.Query),
				attribute.String("params", safe.Stringer(info.Parameters)),
			)
			return func(
				info trace.ScriptingStreamExecuteIntermediateInfo,
			) func(
				trace.ScriptingStreamExecuteDoneInfo,
			) {
				intermediate(start, info.Error)
				return func(info trace.ScriptingStreamExecuteDoneInfo) {
					finish(start, info.Error)
				}
			}
		}
		t.OnExplain = func(info trace.ScriptingExplainStartInfo) func(trace.ScriptingExplainDoneInfo) {
			start := startSpan(
				tracer,
				info.Context,
				"ydb_scripting_explain",
				attribute.String("query", info.Query),
			)
			return func(info trace.ScriptingExplainDoneInfo) {
				finish(start, info.Error)
			}
		}
		t.OnClose = func(info trace.ScriptingCloseStartInfo) func(trace.ScriptingCloseDoneInfo) {
			start := startSpan(
				tracer,
				info.Context,
				"ydb_scripting_close",
			)
			return func(info trace.ScriptingCloseDoneInfo) {
				finish(start, info.Error)
			}
		}
	}
	return t
}
