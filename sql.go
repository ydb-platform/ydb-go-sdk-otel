package ydb

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"go.opentelemetry.io/otel/attribute"
	otelTrace "go.opentelemetry.io/otel/trace"

	"github.com/ydb-platform/ydb-go-sdk-otel/internal/safe"
)

// DatabaseSQL makes trace.DatabaseSQL with logging events from details
func DatabaseSQL(tracer otelTrace.Tracer, details trace.Details) (t trace.DatabaseSQL) {
	if details&trace.DatabaseSQLEvents == 0 {
		return
	}
	prefix := "ydb_database_sql"
	if details&trace.DatabaseSQLConnectorEvents != 0 {
		//nolint:govet
		prefix := prefix + "_connector"
		t.OnConnectorConnect = func(
			info trace.DatabaseSQLConnectorConnectStartInfo,
		) func(
			trace.DatabaseSQLConnectorConnectDoneInfo,
		) {
			start := startSpan(
				tracer,
				info.Context,
				prefix+"_connect",
			)
			return func(info trace.DatabaseSQLConnectorConnectDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}
	}
	if details&trace.DatabaseSQLConnEvents != 0 {
		//nolint:govet
		prefix := prefix + "_conn"
		t.OnConnPing = func(info trace.DatabaseSQLConnPingStartInfo) func(trace.DatabaseSQLConnPingDoneInfo) {
			start := startSpan(
				tracer,
				info.Context,
				prefix+"_ping",
			)
			return func(info trace.DatabaseSQLConnPingDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}
		t.OnConnPrepare = func(info trace.DatabaseSQLConnPrepareStartInfo) func(trace.DatabaseSQLConnPrepareDoneInfo) {
			start := startSpan(
				tracer,
				info.Context,
				prefix+"_prepare",
				attribute.String("query", info.Query),
			)
			return func(info trace.DatabaseSQLConnPrepareDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}
		t.OnConnExec = func(info trace.DatabaseSQLConnExecStartInfo) func(trace.DatabaseSQLConnExecDoneInfo) {
			start := startSpan(
				tracer,
				info.Context,
				prefix+"_exec",
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
		t.OnConnQuery = func(info trace.DatabaseSQLConnQueryStartInfo) func(trace.DatabaseSQLConnQueryDoneInfo) {
			start := startSpan(
				tracer,
				info.Context,
				prefix+"_query",
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
	}
	if details&trace.DatabaseSQLConnEvents != 0 {
		//nolint:govet
		prefix := prefix + "_tx"
		t.OnConnBegin = func(info trace.DatabaseSQLConnBeginStartInfo) func(trace.DatabaseSQLConnBeginDoneInfo) {
			start := startSpan(
				tracer,
				info.Context,
				prefix+"_begin",
			)
			return func(info trace.DatabaseSQLConnBeginDoneInfo) {
				finish(
					start,
					info.Error,
					attribute.String("transaction_id", safe.ID(info.Tx)),
				)
			}
		}
		t.OnTxRollback = func(info trace.DatabaseSQLTxRollbackStartInfo) func(trace.DatabaseSQLTxRollbackDoneInfo) {
			start := startSpan(
				tracer,
				info.Context,
				prefix+"_rollback",
				attribute.String("transaction_id", safe.ID(info.Tx)),
			)
			return func(info trace.DatabaseSQLTxRollbackDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}
		t.OnTxCommit = func(info trace.DatabaseSQLTxCommitStartInfo) func(trace.DatabaseSQLTxCommitDoneInfo) {
			start := startSpan(
				tracer,
				info.Context,
				prefix+"_commit",
				attribute.String("transaction_id", safe.ID(info.Tx)),
			)
			return func(info trace.DatabaseSQLTxCommitDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}
		t.OnTxExec = func(info trace.DatabaseSQLTxExecStartInfo) func(trace.DatabaseSQLTxExecDoneInfo) {
			start := followSpan(
				tracer,
				otelTrace.SpanFromContext(info.TxContext).SpanContext(),
				info.Context,
				prefix+"_exec",
				attribute.String("query", info.Query),
				attribute.String("transaction_id", safe.ID(info.Tx)),
				attribute.Bool("idempotent", info.Idempotent),
			)
			return func(info trace.DatabaseSQLTxExecDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}
		t.OnTxQuery = func(info trace.DatabaseSQLTxQueryStartInfo) func(trace.DatabaseSQLTxQueryDoneInfo) {
			start := followSpan(
				tracer,
				otelTrace.SpanFromContext(info.TxContext).SpanContext(),
				info.Context,
				prefix+"_query",
				attribute.String("query", info.Query),
				attribute.String("transaction_id", safe.ID(info.Tx)),
				attribute.Bool("idempotent", info.Idempotent),
			)
			return func(info trace.DatabaseSQLTxQueryDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}
	}
	if details&trace.DatabaseSQLStmtEvents != 0 {
		//nolint:govet
		prefix := prefix + "_stmt"
		t.OnStmtExec = func(info trace.DatabaseSQLStmtExecStartInfo) func(trace.DatabaseSQLStmtExecDoneInfo) {
			start := startSpan(
				tracer,
				info.Context,
				prefix+"_exec",
				attribute.String("query", info.Query),
			)
			return func(info trace.DatabaseSQLStmtExecDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}
		t.OnStmtQuery = func(info trace.DatabaseSQLStmtQueryStartInfo) func(trace.DatabaseSQLStmtQueryDoneInfo) {
			start := startSpan(
				tracer,
				info.Context,
				prefix+"_query",
				attribute.String("query", info.Query),
			)
			return func(info trace.DatabaseSQLStmtQueryDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}
	}
	return t
}
