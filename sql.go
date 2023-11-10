package ydb

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"go.opentelemetry.io/otel/attribute"
	otelTrace "go.opentelemetry.io/otel/trace"

	"github.com/ydb-platform/ydb-go-sdk-otel/internal/safe"
)

type ctxStmtCallKey struct{}

func markStmtCall(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxStmtCallKey{}, true)
}

func isStmtCall(ctx context.Context) bool {
	if txStmt, has := ctx.Value(ctxStmtCallKey{}).(bool); has {
		return txStmt
	}
	return false
}

// databaseSQL makes trace.DatabaseSQL with logging events from details
func databaseSQL(c *config) (t trace.DatabaseSQL) {
	childSpanWithReplaceCtx := func(
		ctx *context.Context,
		operationName string,
		fields ...attribute.KeyValue,
	) (s otelTrace.Span) {
		return childSpanWithReplaceCtx(c.tracer, ctx, operationName, fields...)
	}
	t.OnConnectorConnect = func(
		info trace.DatabaseSQLConnectorConnectStartInfo,
	) func(
		trace.DatabaseSQLConnectorConnectDoneInfo,
	) {
		if c.detailer.Details()&trace.DatabaseSQLConnectorEvents != 0 {
			start := childSpanWithReplaceCtx(info.Context,
				info.Call.FunctionID(),
			)
			return func(info trace.DatabaseSQLConnectorConnectDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}
		return nil
	}
	t.OnConnPing = func(info trace.DatabaseSQLConnPingStartInfo) func(trace.DatabaseSQLConnPingDoneInfo) {
		if c.detailer.Details()&trace.DatabaseSQLConnEvents != 0 {
			start := childSpanWithReplaceCtx(
				info.Context,
				info.Call.FunctionID(),
			)
			return func(info trace.DatabaseSQLConnPingDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}
		return nil
	}
	t.OnConnPrepare = func(info trace.DatabaseSQLConnPrepareStartInfo) func(trace.DatabaseSQLConnPrepareDoneInfo) {
		if c.detailer.Details()&trace.DatabaseSQLConnEvents != 0 {
			start := childSpanWithReplaceCtx(
				info.Context,
				info.Call.FunctionID(),
				attribute.String("query", info.Query),
			)
			return func(info trace.DatabaseSQLConnPrepareDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}
		return nil
	}
	t.OnConnExec = func(info trace.DatabaseSQLConnExecStartInfo) func(trace.DatabaseSQLConnExecDoneInfo) {
		if c.detailer.Details()&trace.DatabaseSQLConnEvents != 0 {
			start := childSpanWithReplaceCtx(
				info.Context,
				info.Call.FunctionID(),
				attribute.String("query", info.Query),
				attribute.String("query_mode", info.Mode),
				attribute.Bool("idempotent", info.Idempotent),
			)
			return func(info trace.DatabaseSQLConnExecDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}
		return nil
	}
	t.OnConnQuery = func(info trace.DatabaseSQLConnQueryStartInfo) func(trace.DatabaseSQLConnQueryDoneInfo) {
		if c.detailer.Details()&trace.DatabaseSQLConnEvents != 0 {
			start := childSpanWithReplaceCtx(
				info.Context,
				info.Call.FunctionID(),
				attribute.String("query", info.Query),
				attribute.String("query_mode", info.Mode),
				attribute.Bool("idempotent", info.Idempotent),
			)
			return func(info trace.DatabaseSQLConnQueryDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}
		return nil
	}
	t.OnConnIsTableExists = func(info trace.DatabaseSQLConnIsTableExistsStartInfo) func(
		trace.DatabaseSQLConnIsTableExistsDoneInfo,
	) {
		if c.detailer.Details()&trace.DatabaseSQLConnEvents != 0 {
			start := childSpanWithReplaceCtx(
				info.Context,
				info.Call.FunctionID(),
				attribute.String("table_name", info.TableName),
			)
			return func(info trace.DatabaseSQLConnIsTableExistsDoneInfo) {
				finish(
					start,
					info.Error,
					attribute.Bool("exists", info.Exists),
				)
			}
		}
		return nil
	}
	t.OnConnBegin = func(info trace.DatabaseSQLConnBeginStartInfo) func(trace.DatabaseSQLConnBeginDoneInfo) {
		if c.detailer.Details()&trace.DatabaseSQLTxEvents != 0 {
			start := childSpanWithReplaceCtx(
				info.Context,
				info.Call.FunctionID(),
			)
			return func(info trace.DatabaseSQLConnBeginDoneInfo) {
				finish(
					start,
					info.Error,
					attribute.String("transaction_id", safe.ID(info.Tx)),
				)
			}
		}
		return nil
	}
	t.OnTxRollback = func(info trace.DatabaseSQLTxRollbackStartInfo) func(trace.DatabaseSQLTxRollbackDoneInfo) {
		if c.detailer.Details()&trace.DatabaseSQLTxEvents != 0 {
			start := childSpanWithReplaceCtx(
				info.Context,
				info.Call.FunctionID(),
				attribute.String("transaction_id", safe.ID(info.Tx)),
			)
			return func(info trace.DatabaseSQLTxRollbackDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}
		return nil
	}
	t.OnTxCommit = func(info trace.DatabaseSQLTxCommitStartInfo) func(trace.DatabaseSQLTxCommitDoneInfo) {
		if c.detailer.Details()&trace.DatabaseSQLTxEvents != 0 {
			start := childSpanWithReplaceCtx(
				info.Context,
				info.Call.FunctionID(),
				attribute.String("transaction_id", safe.ID(info.Tx)),
			)
			return func(info trace.DatabaseSQLTxCommitDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}
		return nil
	}
	t.OnTxExec = func(info trace.DatabaseSQLTxExecStartInfo) func(trace.DatabaseSQLTxExecDoneInfo) {
		if c.detailer.Details()&trace.DatabaseSQLTxEvents != 0 {
			if !isStmtCall(*info.Context) {
				*info.Context = otelTrace.ContextWithSpan(*info.Context, otelTrace.SpanFromContext(info.TxContext))
			}
			start := childSpanWithReplaceCtx(
				info.Context,
				info.Call.FunctionID(),
				attribute.String("query", info.Query),
				attribute.String("transaction_id", safe.ID(info.Tx)),
			)
			return func(info trace.DatabaseSQLTxExecDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}
		return nil
	}
	t.OnTxQuery = func(info trace.DatabaseSQLTxQueryStartInfo) func(trace.DatabaseSQLTxQueryDoneInfo) {
		if c.detailer.Details()&trace.DatabaseSQLTxEvents != 0 {
			if !isStmtCall(*info.Context) {
				*info.Context = otelTrace.ContextWithSpan(*info.Context, otelTrace.SpanFromContext(info.TxContext))
			}
			start := childSpanWithReplaceCtx(
				info.Context,
				info.Call.FunctionID(),
				attribute.String("query", info.Query),
				attribute.String("transaction_id", safe.ID(info.Tx)),
			)
			return func(info trace.DatabaseSQLTxQueryDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}
		return nil
	}
	t.OnTxPrepare = func(info trace.DatabaseSQLTxPrepareStartInfo) func(trace.DatabaseSQLTxPrepareDoneInfo) {
		if c.detailer.Details()&trace.DatabaseSQLTxEvents != 0 {
			*info.Context = otelTrace.ContextWithSpan(*info.Context, otelTrace.SpanFromContext(*info.TxContext))
			start := childSpanWithReplaceCtx(
				info.Context,
				info.Call.FunctionID(),
				attribute.String("query", info.Query),
				attribute.String("transaction_id", safe.ID(info.Tx)),
			)
			return func(info trace.DatabaseSQLTxPrepareDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}
		return nil
	}
	t.OnStmtExec = func(info trace.DatabaseSQLStmtExecStartInfo) func(trace.DatabaseSQLStmtExecDoneInfo) {
		if c.detailer.Details()&trace.DatabaseSQLStmtEvents != 0 {
			*info.Context = markStmtCall(
				otelTrace.ContextWithSpan(*info.Context, otelTrace.SpanFromContext(info.StmtContext)),
			)
			start := childSpanWithReplaceCtx(
				info.Context,
				info.Call.FunctionID(),
				attribute.String("query", info.Query),
			)
			return func(info trace.DatabaseSQLStmtExecDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}
		return nil
	}
	t.OnStmtQuery = func(info trace.DatabaseSQLStmtQueryStartInfo) func(trace.DatabaseSQLStmtQueryDoneInfo) {
		if c.detailer.Details()&trace.DatabaseSQLStmtEvents != 0 {
			*info.Context = markStmtCall(
				otelTrace.ContextWithSpan(*info.Context, otelTrace.SpanFromContext(info.StmtContext)),
			)
			start := childSpanWithReplaceCtx(
				info.Context,
				info.Call.FunctionID(),
				attribute.String("query", info.Query),
			)
			return func(info trace.DatabaseSQLStmtQueryDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}
		return nil
	}
	return t
}
