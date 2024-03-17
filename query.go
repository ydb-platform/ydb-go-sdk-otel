package ydb

import (
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"go.opentelemetry.io/otel/attribute"

	"github.com/ydb-platform/ydb-go-sdk-otel/internal/safe"
)

func query(cfg *config) trace.Query {
	nodeID := func(session interface{ NodeID() int64 }) int64 {
		if session != nil {
			return session.NodeID()
		}
		return 0
	}
	return trace.Query{
		OnNew: func(info trace.QueryNewStartInfo) func(info trace.QueryNewDoneInfo) {
			if cfg.detailer.Details()&trace.QueryEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
			)
			return func(info trace.QueryNewDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnClose: func(info trace.QueryCloseStartInfo) func(info trace.QueryCloseDoneInfo) {
			if cfg.detailer.Details()&trace.QueryEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
			)
			return func(info trace.QueryCloseDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnPoolNew: func(info trace.QueryPoolNewStartInfo) func(trace.QueryPoolNewDoneInfo) {
			if cfg.detailer.Details()&trace.QueryPoolEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
			)
			return func(info trace.QueryPoolNewDoneInfo) {
				finish(
					start,
					info.Error,
					attribute.Int("MinSize", info.MinSize),
					attribute.Int("MaxSize", info.MaxSize),
					attribute.Int("ProducersCount", info.ProducersCount),
				)
			}
		},
		OnPoolClose: func(info trace.QueryPoolCloseStartInfo) func(trace.QueryPoolCloseDoneInfo) {
			if cfg.detailer.Details()&trace.QueryPoolEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
			)
			return func(info trace.QueryPoolCloseDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnPoolProduce: func(info trace.QueryPoolProduceStartInfo) func(trace.QueryPoolProduceDoneInfo) {
			if cfg.detailer.Details()&trace.QueryPoolEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
				attribute.Int("Concurrency", info.Concurrency),
			)
			return func(info trace.QueryPoolProduceDoneInfo) {
				start.End()
			}
		},
		OnPoolTry: func(info trace.QueryPoolTryStartInfo) func(trace.QueryPoolTryDoneInfo) {
			if cfg.detailer.Details()&trace.QueryPoolEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
			)
			return func(info trace.QueryPoolTryDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnPoolWith: func(info trace.QueryPoolWithStartInfo) func(trace.QueryPoolWithDoneInfo) {
			if cfg.detailer.Details()&trace.QueryPoolEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
			)
			return func(info trace.QueryPoolWithDoneInfo) {
				finish(
					start,
					info.Error,
					attribute.Int("Attempts", info.Attempts),
				)
			}
		},
		OnPoolPut: func(info trace.QueryPoolPutStartInfo) func(trace.QueryPoolPutDoneInfo) {
			if cfg.detailer.Details()&trace.QueryPoolEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
			)
			return func(info trace.QueryPoolPutDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnPoolGet: func(info trace.QueryPoolGetStartInfo) func(trace.QueryPoolGetDoneInfo) {
			if cfg.detailer.Details()&trace.QueryPoolEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
			)
			return func(info trace.QueryPoolGetDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnPoolSpawn: func(info trace.QueryPoolSpawnStartInfo) func(trace.QueryPoolSpawnDoneInfo) {
			if cfg.detailer.Details()&trace.QueryPoolEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
			)
			return func(info trace.QueryPoolSpawnDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnPoolWant: func(info trace.QueryPoolWantStartInfo) func(trace.QueryPoolWantDoneInfo) {
			if cfg.detailer.Details()&trace.QueryPoolEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
			)
			return func(info trace.QueryPoolWantDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnDo: func(info trace.QueryDoStartInfo) func(trace.QueryDoDoneInfo) {
			if cfg.detailer.Details()&trace.QueryEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
			)
			return func(info trace.QueryDoDoneInfo) {
				finish(
					start,
					info.Error,
					attribute.Int("Attempts", info.Attempts),
				)
			}
		},
		OnDoTx: func(info trace.QueryDoTxStartInfo) func(trace.QueryDoTxDoneInfo) {
			if cfg.detailer.Details()&trace.QueryEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
			)
			return func(info trace.QueryDoTxDoneInfo) {
				finish(
					start,
					info.Error,
					attribute.Int("Attempts", info.Attempts),
				)
			}
		},
		OnSessionCreate: func(info trace.QuerySessionCreateStartInfo) func(info trace.QuerySessionCreateDoneInfo) {
			if cfg.detailer.Details()&trace.QuerySessionEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
			)
			return func(info trace.QuerySessionCreateDoneInfo) {
				finish(
					start,
					info.Error,
					attribute.String("SessionID", safe.ID(info.Session)),
					attribute.String("SessionStatus", safe.Status(info.Session)),
					attribute.Int64("NodeID", nodeID(info.Session)),
				)
			}
		},
		OnSessionAttach: func(info trace.QuerySessionAttachStartInfo) func(info trace.QuerySessionAttachDoneInfo) {
			if cfg.detailer.Details()&trace.QuerySessionEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
			)

			return func(info trace.QuerySessionAttachDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnSessionDelete: func(info trace.QuerySessionDeleteStartInfo) func(info trace.QuerySessionDeleteDoneInfo) {
			if cfg.detailer.Details()&trace.QuerySessionEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
			)

			return func(info trace.QuerySessionDeleteDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
		OnSessionExecute: func(info trace.QuerySessionExecuteStartInfo) func(info trace.QuerySessionExecuteDoneInfo) {
			if cfg.detailer.Details()&trace.QuerySessionEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
				attribute.String("Query", strings.TrimSpace(info.Query)),
			)

			return func(info trace.QuerySessionExecuteDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		},
	}
}
