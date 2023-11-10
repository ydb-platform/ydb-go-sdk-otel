package ydb

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/ydb-platform/ydb-go-sdk-otel/internal/safe"
)

func scripting(cfg *config) (t trace.Scripting) {
	t.OnExecute = func(info trace.ScriptingExecuteStartInfo) func(trace.ScriptingExecuteDoneInfo) {
		if cfg.detailer.Details()&trace.ScriptingEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
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
		return nil
	}
	t.OnStreamExecute = func(
		info trace.ScriptingStreamExecuteStartInfo,
	) func(
		trace.ScriptingStreamExecuteIntermediateInfo,
	) func(
		trace.ScriptingStreamExecuteDoneInfo,
	) {
		if cfg.detailer.Details()&trace.ScriptingEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
				attribute.String("query", info.Query),
				attribute.String("params", safe.Stringer(info.Parameters)),
			)
			return func(
				info trace.ScriptingStreamExecuteIntermediateInfo,
			) func(
				trace.ScriptingStreamExecuteDoneInfo,
			) {
				if info.Error != nil {
					start.RecordError(info.Error)
				}
				return func(info trace.ScriptingStreamExecuteDoneInfo) {
					if info.Error != nil {
						start.RecordError(info.Error)
						start.SetStatus(codes.Error, info.Error.Error())
					}
					start.End()
				}
			}
		}
		return nil
	}
	t.OnExplain = func(info trace.ScriptingExplainStartInfo) func(trace.ScriptingExplainDoneInfo) {
		if cfg.detailer.Details()&trace.ScriptingEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
				attribute.String("query", info.Query),
			)
			return func(info trace.ScriptingExplainDoneInfo) {
				finish(start, info.Error)
			}
		}
		return nil
	}
	t.OnClose = func(info trace.ScriptingCloseStartInfo) func(trace.ScriptingCloseDoneInfo) {
		if cfg.detailer.Details()&trace.ScriptingEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
			)
			return func(info trace.ScriptingCloseDoneInfo) {
				finish(start, info.Error)
			}
		}
		return nil
	}
	return t
}
