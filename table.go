package ydb

import (
	"fmt"
	"go.opentelemetry.io/otel"

	"go.opentelemetry.io/otel/attribute"
	otelTrace "go.opentelemetry.io/otel/trace"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"

	"github.com/ydb-platform/ydb-go-sdk-otel/internal/safe"
)

// Table makes table.ClientTrace with solomon metrics publishing
func Table(tracer otelTrace.Tracer, details trace.Details) (t trace.Table) {
	if tracer == nil {
		tracer = otel.Tracer(tracerID)
	}
	if details&trace.TableEvents != 0 {
		t.OnCreateSession = func(
			info trace.TableCreateSessionStartInfo,
		) func(
			info trace.TableCreateSessionIntermediateInfo,
		) func(
			trace.TableCreateSessionDoneInfo,
		) {
			start := startSpan(
				tracer,
				info.Context,
				"ydb_table_create_session",
			)
			return func(info trace.TableCreateSessionIntermediateInfo) func(trace.TableCreateSessionDoneInfo) {
				intermediate(start, info.Error)
				return func(info trace.TableCreateSessionDoneInfo) {
					finish(start,
						info.Error,
						attribute.Int("attempts", info.Attempts),
					)
				}
			}
		}
		t.OnDo = func(
			info trace.TableDoStartInfo,
		) func(
			info trace.TableDoIntermediateInfo,
		) func(
			trace.TableDoDoneInfo,
		) {
			start := startSpan(
				tracer,
				info.Context,
				"ydb_table_do",
				attribute.Bool("idempotent", info.Idempotent),
			)
			if info.NestedCall {
				start.RecordError(fmt.Errorf("nested call"))
			}
			return func(info trace.TableDoIntermediateInfo) func(trace.TableDoDoneInfo) {
				intermediate(start, info.Error)
				return func(info trace.TableDoDoneInfo) {
					finish(start,
						info.Error,
						attribute.Int("attempts", info.Attempts),
					)
				}
			}
		}
		t.OnDoTx = func(
			info trace.TableDoTxStartInfo,
		) func(
			info trace.TableDoTxIntermediateInfo,
		) func(
			trace.TableDoTxDoneInfo,
		) {
			start := startSpan(
				tracer,
				info.Context,
				"ydb_table_do_tx",
				attribute.Bool("idempotent", info.Idempotent),
			)
			if info.NestedCall {
				start.RecordError(fmt.Errorf("nested call"))
			}
			return func(info trace.TableDoTxIntermediateInfo) func(trace.TableDoTxDoneInfo) {
				intermediate(start, info.Error)
				return func(info trace.TableDoTxDoneInfo) {
					finish(start,
						info.Error,
						attribute.Int("attempts", info.Attempts),
					)
				}
			}
		}
	}
	//nolint:nestif
	if details&trace.TableSessionEvents != 0 {
		if details&trace.TableSessionLifeCycleEvents != 0 {
			t.OnSessionNew = func(info trace.TableSessionNewStartInfo) func(trace.TableSessionNewDoneInfo) {
				start := startSpan(
					tracer,
					info.Context,
					"ydb_table_session_new",
				)
				return func(info trace.TableSessionNewDoneInfo) {
					finish(
						start,
						info.Error,
						attribute.String("status", safe.Status(info.Session)),
						attribute.String("node_id", nodeID(safe.ID(info.Session))),
						attribute.String("session_id", safe.ID(info.Session)),
					)
				}
			}
			t.OnSessionDelete = func(info trace.TableSessionDeleteStartInfo) func(trace.TableSessionDeleteDoneInfo) {
				start := startSpan(
					tracer,
					info.Context,
					"ydb_table_session_delete",
					attribute.String("node_id", nodeID(safe.ID(info.Session))),
					attribute.String("session_id", safe.ID(info.Session)),
				)
				return func(info trace.TableSessionDeleteDoneInfo) {
					finish(start, info.Error)
				}
			}
			t.OnSessionKeepAlive = func(info trace.TableKeepAliveStartInfo) func(trace.TableKeepAliveDoneInfo) {
				start := startSpan(
					tracer,
					info.Context,
					"ydb_table_session_keep_alive",
					attribute.String("node_id", nodeID(safe.ID(info.Session))),
					attribute.String("session_id", safe.ID(info.Session)),
				)
				return func(info trace.TableKeepAliveDoneInfo) {
					finish(start, info.Error)
				}
			}
		}
		if details&trace.TableSessionQueryEvents != 0 {
			if details&trace.TableSessionQueryInvokeEvents != 0 {
				t.OnSessionQueryPrepare = func(
					info trace.TablePrepareDataQueryStartInfo,
				) func(
					trace.TablePrepareDataQueryDoneInfo,
				) {
					start := startSpan(
						tracer,
						info.Context,
						"ydb_table_session_query_prepare",
						attribute.String("query", info.Query),
						attribute.String("node_id", nodeID(safe.ID(info.Session))),
						attribute.String("session_id", safe.ID(info.Session)),
					)
					return func(info trace.TablePrepareDataQueryDoneInfo) {
						finish(
							start,
							info.Error,
							attribute.String("result", safe.Stringer(info.Result)),
						)
					}
				}
				t.OnSessionQueryExecute = func(
					info trace.TableExecuteDataQueryStartInfo,
				) func(
					trace.TableExecuteDataQueryDoneInfo,
				) {
					start := startSpan(
						tracer,
						info.Context,
						"ydb_table_session_query_execute",
						attribute.String("node_id", nodeID(safe.ID(info.Session))),
						attribute.String("session_id", safe.ID(info.Session)),
						attribute.String("query", safe.Stringer(info.Query)),
						attribute.String("params", safe.Stringer(info.Parameters)),
						attribute.Bool("keep_in_cache", info.KeepInCache),
					)
					return func(info trace.TableExecuteDataQueryDoneInfo) {
						if info.Error == nil {
							finish(
								start,
								safe.Err(info.Result),
								attribute.Bool("prepared", info.Prepared),
								attribute.String("transaction_id", safe.ID(info.Tx)),
							)
						} else {
							finish(
								start,
								info.Error,
							)
						}
					}
				}
			}
			if details&trace.TableSessionQueryStreamEvents != 0 {
				t.OnSessionQueryStreamExecute = func(
					info trace.TableSessionQueryStreamExecuteStartInfo,
				) func(
					intermediateInfo trace.TableSessionQueryStreamExecuteIntermediateInfo,
				) func(
					trace.TableSessionQueryStreamExecuteDoneInfo,
				) {
					start := startSpan(
						tracer,
						info.Context,
						"ydb_table_session_query_stream_execute",
						attribute.String("query", safe.Stringer(info.Query)),
						attribute.String("params", safe.Stringer(info.Parameters)),
						attribute.String("node_id", nodeID(safe.ID(info.Session))),
						attribute.String("session_id", safe.ID(info.Session)),
					)
					return func(
						info trace.TableSessionQueryStreamExecuteIntermediateInfo,
					) func(
						trace.TableSessionQueryStreamExecuteDoneInfo,
					) {
						intermediate(start, info.Error)
						return func(info trace.TableSessionQueryStreamExecuteDoneInfo) {
							finish(start, info.Error)
						}
					}
				}
				t.OnSessionQueryStreamRead = func(
					info trace.TableSessionQueryStreamReadStartInfo,
				) func(
					trace.TableSessionQueryStreamReadIntermediateInfo,
				) func(
					trace.TableSessionQueryStreamReadDoneInfo,
				) {
					start := startSpan(
						tracer,
						info.Context,
						"ydb_table_session_query_stream_read",
						attribute.String("node_id", nodeID(safe.ID(info.Session))),
						attribute.String("session_id", safe.ID(info.Session)),
					)
					return func(
						info trace.TableSessionQueryStreamReadIntermediateInfo,
					) func(
						trace.TableSessionQueryStreamReadDoneInfo,
					) {
						intermediate(start, info.Error)
						return func(info trace.TableSessionQueryStreamReadDoneInfo) {
							finish(start, info.Error)
						}
					}
				}
			}
		}
		if details&trace.TableSessionTransactionEvents != 0 {
			t.OnSessionTransactionBegin = func(
				info trace.TableSessionTransactionBeginStartInfo,
			) func(
				trace.TableSessionTransactionBeginDoneInfo,
			) {
				start := startSpan(
					tracer,
					info.Context,
					"ydb_table_session_tx_begin",
					attribute.String("node_id", nodeID(safe.ID(info.Session))),
					attribute.String("session_id", safe.ID(info.Session)),
				)
				return func(info trace.TableSessionTransactionBeginDoneInfo) {
					finish(
						start,
						info.Error,
						attribute.String("transaction_id", safe.ID(info.Tx)),
					)
				}
			}
			t.OnSessionTransactionCommit = func(
				info trace.TableSessionTransactionCommitStartInfo,
			) func(
				trace.TableSessionTransactionCommitDoneInfo,
			) {
				start := startSpan(
					tracer,
					info.Context,
					"ydb_table_session_tx_commit",
					attribute.String("node_id", nodeID(safe.ID(info.Session))),
					attribute.String("session_id", safe.ID(info.Session)),
					attribute.String("transaction_id", safe.ID(info.Tx)),
				)
				return func(info trace.TableSessionTransactionCommitDoneInfo) {
					finish(start, info.Error)
				}
			}
			t.OnSessionTransactionRollback = func(
				info trace.TableSessionTransactionRollbackStartInfo,
			) func(
				trace.TableSessionTransactionRollbackDoneInfo,
			) {
				start := startSpan(
					tracer,
					info.Context,
					"ydb_table_session_tx_rollback",
					attribute.String("node_id", nodeID(safe.ID(info.Session))),
					attribute.String("session_id", safe.ID(info.Session)),
					attribute.String("transaction_id", safe.ID(info.Tx)),
				)
				return func(info trace.TableSessionTransactionRollbackDoneInfo) {
					finish(start, info.Error)
				}
			}
			t.OnSessionTransactionExecute = func(
				info trace.TableTransactionExecuteStartInfo,
			) func(
				trace.TableTransactionExecuteDoneInfo,
			) {
				start := startSpan(
					tracer,
					info.Context,
					"ydb_table_session_tx_execute",
					attribute.String("node_id", nodeID(safe.ID(info.Session))),
					attribute.String("session_id", safe.ID(info.Session)),
					attribute.String("transaction_id", safe.ID(info.Tx)),
					attribute.String("query", safe.Stringer(info.Query)),
					attribute.String("params", safe.Stringer(info.Parameters)),
				)
				return func(info trace.TableTransactionExecuteDoneInfo) {
					finish(start, info.Error)
				}
			}
			t.OnSessionTransactionExecuteStatement = func(
				info trace.TableTransactionExecuteStatementStartInfo,
			) func(
				info trace.TableTransactionExecuteStatementDoneInfo,
			) {
				start := startSpan(
					tracer,
					info.Context,
					"ydb_table_session_tx_execute_statement",
					attribute.String("node_id", nodeID(safe.ID(info.Session))),
					attribute.String("session_id", safe.ID(info.Session)),
					attribute.String("transaction_id", safe.ID(info.Tx)),
					attribute.String("query", safe.Stringer(info.StatementQuery)),
					attribute.String("params", safe.Stringer(info.Parameters)),
				)
				return func(info trace.TableTransactionExecuteStatementDoneInfo) {
					finish(start, info.Error)
				}
			}
		}
	}
	if details&trace.TablePoolEvents != 0 {
		if details&trace.TablePoolLifeCycleEvents != 0 {
			t.OnInit = func(info trace.TableInitStartInfo) func(trace.TableInitDoneInfo) {
				start := startSpan(
					tracer,
					info.Context,
					"ydb_table_pool_init",
				)
				return func(info trace.TableInitDoneInfo) {
					finish(
						start,
						nil,
						attribute.Int("limit", info.Limit),
					)
				}
			}
			t.OnClose = func(info trace.TableCloseStartInfo) func(trace.TableCloseDoneInfo) {
				start := startSpan(
					tracer,
					info.Context,
					"ydb_table_pool_close",
				)
				return func(info trace.TableCloseDoneInfo) {
					finish(start, info.Error)
				}
			}
		}
		if details&trace.TablePoolAPIEvents != 0 {
			t.OnPoolPut = func(info trace.TablePoolPutStartInfo) func(trace.TablePoolPutDoneInfo) {
				start := startSpan(
					tracer,
					info.Context,
					"ydb_table_pool_put",
					attribute.String("node_id", nodeID(safe.ID(info.Session))),
					attribute.String("session_id", safe.ID(info.Session)),
				)
				return func(info trace.TablePoolPutDoneInfo) {
					finish(start, info.Error)
				}
			}
			t.OnPoolGet = func(info trace.TablePoolGetStartInfo) func(trace.TablePoolGetDoneInfo) {
				start := startSpan(
					tracer,
					info.Context,
					"ydb_table_pool_get",
				)
				return func(info trace.TablePoolGetDoneInfo) {
					finish(
						start,
						info.Error,
						attribute.Int("attempts", info.Attempts),
						attribute.String("status", safe.Status(info.Session)),
						attribute.String("node_id", nodeID(safe.ID(info.Session))),
						attribute.String("session_id", safe.ID(info.Session)),
					)
				}
			}
			t.OnPoolWait = func(info trace.TablePoolWaitStartInfo) func(trace.TablePoolWaitDoneInfo) {
				start := startSpan(
					tracer,
					info.Context,
					"ydb_table_pool_wait",
				)
				return func(info trace.TablePoolWaitDoneInfo) {
					finish(
						start,
						info.Error,
						attribute.String("status", safe.Status(info.Session)),
						attribute.String("node_id", nodeID(safe.ID(info.Session))),
						attribute.String("session_id", safe.ID(info.Session)),
					)
				}
			}
		}
	}
	return t
}
