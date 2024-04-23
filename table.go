package ydb

import (
	"fmt"
	"net/url"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/ydb-platform/ydb-go-sdk-otel/internal/safe"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// table makes table.ClientTrace with solomon metrics publishing
func table(cfg *config) (t trace.Table) { //nolint:gocyclo
	nodeID := func(sessionID string) string {
		u, err := url.Parse(sessionID)
		if err != nil {
			return ""
		}

		return u.Query().Get("node_id")
	}
	t.OnCreateSession = func(info trace.TableCreateSessionStartInfo) func(trace.TableCreateSessionDoneInfo) {
		if cfg.detailer.Details()&trace.TableEvents != 0 {
			fieldsStore := fieldsStoreFromContext(info.Context)
			*info.Context = withFunctionID(*info.Context, info.Call.FunctionID())
			return func(info trace.TableCreateSessionDoneInfo) {
				if info.Error == nil {
					fieldsStore.fields = append(fieldsStore.fields,
						attribute.String("session_id", safe.ID(info.Session)),
						attribute.String("session_status", safe.Status(info.Session)),
						attribute.String("node_id", nodeID(safe.ID(info.Session))),
					)
				}
			}
		}
		return nil
	}
	t.OnDo = func(info trace.TableDoStartInfo) func(trace.TableDoDoneInfo) {
		if cfg.detailer.Details()&trace.TableEvents != 0 {
			*info.Context = noTraceRetry(*info.Context)
			operationName := info.Label
			if operationName == "" {
				operationName = info.Call.FunctionID()
			}
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				operationName,
				attribute.Bool("idempotent", info.Idempotent),
			)
			if info.NestedCall {
				start.RecordError(fmt.Errorf("nested call"))
			}
			return func(info trace.TableDoDoneInfo) {
				start.SetAttributes(
					attribute.Int("attempts", info.Attempts),
				)
				if info.Error != nil {
					start.RecordError(info.Error)
					start.SetStatus(codes.Error, info.Error.Error())
				}
				start.End()
			}
		}
		return nil
	}
	t.OnDoTx = func(info trace.TableDoTxStartInfo) func(trace.TableDoTxDoneInfo) {
		if cfg.detailer.Details()&trace.TableEvents != 0 {
			*info.Context = noTraceRetry(*info.Context)
			operationName := info.Label
			if operationName == "" {
				operationName = info.Call.FunctionID()
			}
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				operationName,
				attribute.Bool("idempotent", info.Idempotent),
			)
			if info.NestedCall {
				start.RecordError(fmt.Errorf("nested call"))
			}
			return func(info trace.TableDoTxDoneInfo) {
				start.SetAttributes(
					attribute.Int("attempts", info.Attempts),
				)
				if info.Error != nil {
					start.RecordError(info.Error)
					start.SetStatus(codes.Error, info.Error.Error())
				}
				start.End()
			}
		}
		return nil
	}
	t.OnSessionNew = func(info trace.TableSessionNewStartInfo) func(trace.TableSessionNewDoneInfo) {
		if cfg.detailer.Details()&trace.TableSessionLifeCycleEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
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
		return nil
	}
	t.OnSessionDelete = func(info trace.TableSessionDeleteStartInfo) func(trace.TableSessionDeleteDoneInfo) {
		if cfg.detailer.Details()&trace.TableSessionLifeCycleEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
				attribute.String("node_id", nodeID(safe.ID(info.Session))),
				attribute.String("session_id", safe.ID(info.Session)),
			)
			return func(info trace.TableSessionDeleteDoneInfo) {
				finish(start, info.Error)
			}
		}
		return nil
	}
	t.OnSessionKeepAlive = func(info trace.TableKeepAliveStartInfo) func(trace.TableKeepAliveDoneInfo) {
		if cfg.detailer.Details()&trace.TableSessionLifeCycleEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
				attribute.String("node_id", nodeID(safe.ID(info.Session))),
				attribute.String("session_id", safe.ID(info.Session)),
			)
			return func(info trace.TableKeepAliveDoneInfo) {
				finish(start, info.Error)
			}
		}
		return nil
	}
	t.OnSessionBulkUpsert = func(info trace.TableBulkUpsertStartInfo) func(trace.TableBulkUpsertDoneInfo) {
		if cfg.detailer.Details()&trace.TableSessionQueryEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
				attribute.String("node_id", nodeID(safe.ID(info.Session))),
				attribute.String("session_id", safe.ID(info.Session)),
			)
			return func(info trace.TableBulkUpsertDoneInfo) {
				finish(start, info.Error)
			}
		}
		return nil
	}
	t.OnSessionQueryPrepare = func(
		info trace.TablePrepareDataQueryStartInfo,
	) func(
		trace.TablePrepareDataQueryDoneInfo,
	) {
		if cfg.detailer.Details()&trace.TableSessionQueryInvokeEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
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
		return nil
	}
	t.OnSessionQueryExecute = func(
		info trace.TableExecuteDataQueryStartInfo,
	) func(
		trace.TableExecuteDataQueryDoneInfo,
	) {
		if cfg.detailer.Details()&trace.TableSessionQueryInvokeEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
				attribute.String("node_id", nodeID(safe.ID(info.Session))),
				attribute.String("session_id", safe.ID(info.Session)),
				attribute.String("query", safe.Stringer(info.Query)),
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
		return nil
	}
	t.OnSessionQueryStreamExecute = func(
		info trace.TableSessionQueryStreamExecuteStartInfo,
	) func(
		trace.TableSessionQueryStreamExecuteDoneInfo,
	) {
		if cfg.detailer.Details()&trace.TableSessionQueryStreamEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
				attribute.String("query", safe.Stringer(info.Query)),
				attribute.String("node_id", nodeID(safe.ID(info.Session))),
				attribute.String("session_id", safe.ID(info.Session)),
			)
			return func(info trace.TableSessionQueryStreamExecuteDoneInfo) {
				if info.Error != nil {
					start.RecordError(info.Error)
					start.SetStatus(codes.Error, info.Error.Error())
				}
				start.End()
			}
		}
		return nil
	}
	t.OnSessionQueryStreamRead = func(
		info trace.TableSessionQueryStreamReadStartInfo,
	) func(
		trace.TableSessionQueryStreamReadDoneInfo,
	) {
		if cfg.detailer.Details()&trace.TableSessionQueryStreamEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
				attribute.String("node_id", nodeID(safe.ID(info.Session))),
				attribute.String("session_id", safe.ID(info.Session)),
			)
			return func(info trace.TableSessionQueryStreamReadDoneInfo) {
				if info.Error != nil {
					start.RecordError(info.Error)
					start.SetStatus(codes.Error, info.Error.Error())
				}
				start.End()
			}
		}
		return nil
	}
	t.OnTxBegin = func(info trace.TableTxBeginStartInfo) func(trace.TableTxBeginDoneInfo) {
		if cfg.detailer.Details()&trace.TableSessionTransactionEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
				attribute.String("node_id", nodeID(safe.ID(info.Session))),
				attribute.String("session_id", safe.ID(info.Session)),
			)
			return func(info trace.TableTxBeginDoneInfo) {
				finish(
					start,
					info.Error,
					attribute.String("transaction_id", safe.ID(info.Tx)),
				)
			}
		}
		return nil
	}
	t.OnTxCommit = func(info trace.TableTxCommitStartInfo) func(trace.TableTxCommitDoneInfo) {
		if cfg.detailer.Details()&trace.TableSessionTransactionEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
				attribute.String("node_id", nodeID(safe.ID(info.Session))),
				attribute.String("session_id", safe.ID(info.Session)),
				attribute.String("transaction_id", safe.ID(info.Tx)),
			)
			return func(info trace.TableTxCommitDoneInfo) {
				finish(start, info.Error)
			}
		}
		return nil
	}
	t.OnTxRollback = func(info trace.TableTxRollbackStartInfo) func(trace.TableTxRollbackDoneInfo) {
		if cfg.detailer.Details()&trace.TableSessionTransactionEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
				attribute.String("node_id", nodeID(safe.ID(info.Session))),
				attribute.String("session_id", safe.ID(info.Session)),
				attribute.String("transaction_id", safe.ID(info.Tx)),
			)
			return func(info trace.TableTxRollbackDoneInfo) {
				finish(start, info.Error)
			}
		}
		return nil
	}
	t.OnTxExecute = func(info trace.TableTransactionExecuteStartInfo) func(trace.TableTransactionExecuteDoneInfo) {
		if cfg.detailer.Details()&trace.TableSessionTransactionEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
				attribute.String("node_id", nodeID(safe.ID(info.Session))),
				attribute.String("session_id", safe.ID(info.Session)),
				attribute.String("transaction_id", safe.ID(info.Tx)),
				attribute.String("query", safe.Stringer(info.Query)),
			)
			return func(info trace.TableTransactionExecuteDoneInfo) {
				finish(start, info.Error)
			}
		}
		return nil
	}
	t.OnTxExecuteStatement = func(
		info trace.TableTransactionExecuteStatementStartInfo,
	) func(
		info trace.TableTransactionExecuteStatementDoneInfo,
	) {
		if cfg.detailer.Details()&trace.TableSessionTransactionEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
				attribute.String("node_id", nodeID(safe.ID(info.Session))),
				attribute.String("session_id", safe.ID(info.Session)),
				attribute.String("transaction_id", safe.ID(info.Tx)),
				attribute.String("query", safe.Stringer(info.StatementQuery)),
			)
			return func(info trace.TableTransactionExecuteStatementDoneInfo) {
				finish(start, info.Error)
			}
		}
		return nil
	}
	t.OnInit = func(info trace.TableInitStartInfo) func(trace.TableInitDoneInfo) {
		if cfg.detailer.Details()&trace.TableEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
			)
			return func(info trace.TableInitDoneInfo) {
				finish(
					start,
					nil,
					attribute.Int("limit", info.Limit),
				)
			}
		}
		return nil
	}
	t.OnClose = func(info trace.TableCloseStartInfo) func(trace.TableCloseDoneInfo) {
		if cfg.detailer.Details()&trace.TableEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
			)
			return func(info trace.TableCloseDoneInfo) {
				finish(start, info.Error)
			}
		}
		return nil
	}
	t.OnPoolPut = func(info trace.TablePoolPutStartInfo) func(trace.TablePoolPutDoneInfo) {
		if cfg.detailer.Details()&trace.TablePoolAPIEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
				attribute.String("node_id", nodeID(safe.ID(info.Session))),
				attribute.String("session_id", safe.ID(info.Session)),
			)
			return func(info trace.TablePoolPutDoneInfo) {
				finish(start, info.Error)
			}
		}
		return nil
	}
	t.OnPoolGet = func(info trace.TablePoolGetStartInfo) func(trace.TablePoolGetDoneInfo) {
		if cfg.detailer.Details()&trace.TablePoolAPIEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
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
		return nil
	}
	t.OnPoolWait = func(info trace.TablePoolWaitStartInfo) func(trace.TablePoolWaitDoneInfo) {
		if cfg.detailer.Details()&trace.TablePoolAPIEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
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
		return nil
	}
	return t
}
