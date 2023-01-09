package ydb

import (
	"go.opentelemetry.io/otel/attribute"
	otelTrace "go.opentelemetry.io/otel/trace"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"

	"github.com/ydb-platform/ydb-go-sdk-otel/internal/safe"
)

// DatabaseSQL makes trace.DatabaseSQL with logging events from details
func DatabaseSQL(cfg *config) (t trace.DatabaseSQL) {
	prefix := "ydb_database_sql"
	t.OnConnectorConnect = func(
		info trace.DatabaseSQLConnectorConnectStartInfo,
	) func(
		trace.DatabaseSQLConnectorConnectDoneInfo,
	) {
		if cfg.detailer.Details()&trace.DatabaseSQLConnectorEvents != 0 {
			start := startSpan(
				cfg.tracer,
				info.Context,
				prefix+"_connector_connect",
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
		if cfg.detailer.Details()&trace.DatabaseSQLConnEvents != 0 {
			start := startSpan(
				cfg.tracer,
				info.Context,
				prefix+"_conn_ping",
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
		if cfg.detailer.Details()&trace.DatabaseSQLConnEvents != 0 {
			start := startSpan(
				cfg.tracer,
				info.Context,
				prefix+"_conn_prepare",
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
		if cfg.detailer.Details()&trace.DatabaseSQLConnEvents != 0 {
			start := startSpan(
				cfg.tracer,
				info.Context,
				prefix+"_conn_exec",
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
		if cfg.detailer.Details()&trace.DatabaseSQLConnEvents != 0 {
			start := startSpan(
				cfg.tracer,
				info.Context,
				prefix+"_conn_query",
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
	t.OnConnBegin = func(info trace.DatabaseSQLConnBeginStartInfo) func(trace.DatabaseSQLConnBeginDoneInfo) {
		if cfg.detailer.Details()&trace.DatabaseSQLConnEvents != 0 {
			start := startSpan(
				cfg.tracer,
				info.Context,
				prefix+"_tx_begin",
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
		if cfg.detailer.Details()&trace.DatabaseSQLConnEvents != 0 {
			start := startSpan(
				cfg.tracer,
				info.Context,
				prefix+"_tx_rollback",
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
		if cfg.detailer.Details()&trace.DatabaseSQLConnEvents != 0 {
			start := startSpan(
				cfg.tracer,
				info.Context,
				prefix+"_tx_commit",
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
		if cfg.detailer.Details()&trace.DatabaseSQLConnEvents != 0 {
			start := followSpan(
				cfg.tracer,
				otelTrace.SpanFromContext(info.TxContext).SpanContext(),
				info.Context,
				prefix+"_tx_exec",
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
		return nil
	}
	t.OnTxQuery = func(info trace.DatabaseSQLTxQueryStartInfo) func(trace.DatabaseSQLTxQueryDoneInfo) {
		if cfg.detailer.Details()&trace.DatabaseSQLConnEvents != 0 {
			start := followSpan(
				cfg.tracer,
				otelTrace.SpanFromContext(info.TxContext).SpanContext(),
				info.Context,
				prefix+"_tx_query",
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
		return nil
	}
	t.OnStmtExec = func(info trace.DatabaseSQLStmtExecStartInfo) func(trace.DatabaseSQLStmtExecDoneInfo) {
		if cfg.detailer.Details()&trace.DatabaseSQLStmtEvents != 0 {
			start := startSpan(
				cfg.tracer,
				info.Context,
				prefix+"_stmt_exec",
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
		if cfg.detailer.Details()&trace.DatabaseSQLStmtEvents != 0 {
			start := startSpan(
				cfg.tracer,
				info.Context,
				prefix+"_stmt_query",
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
