package ydb

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

type (
	ctxRetryFunctionIDKey struct{}
	ctxRetryFieldsKey     struct{}
	ctxNoTraceRetryKey    struct{}
	fieldsStore           struct {
		fields []attribute.KeyValue
	}
)

func withFunctionID(ctx context.Context, functionID string) context.Context {
	return context.WithValue(ctx, ctxRetryFunctionIDKey{}, functionID)
}

func functionID(ctx context.Context) string {
	if functionID, has := ctx.Value(ctxRetryFunctionIDKey{}).(string); has {
		return functionID
	}
	return ""
}

func noTraceRetry(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxNoTraceRetryKey{}, true)
}

func isTraceRetry(ctx context.Context) bool {
	if noTrace, has := ctx.Value(ctxNoTraceRetryKey{}).(bool); has {
		return !noTrace
	}
	return true
}

func fieldsStoreFromContext(ctx *context.Context) *fieldsStore {
	if store, has := (*ctx).Value(ctxRetryFieldsKey{}).(*fieldsStore); has {
		return store
	}
	store := &fieldsStore{}
	*ctx = context.WithValue(*ctx, ctxRetryFieldsKey{}, store)
	return store
}

func fieldsFromStore(ctx context.Context) []attribute.KeyValue {
	if holder, has := ctx.Value(ctxRetryFieldsKey{}).(*fieldsStore); has {
		return holder.fields
	}
	return nil
}

func retry(cfg *config) (t trace.Retry) {
	t.OnRetry = func(info trace.RetryLoopStartInfo) func(trace.RetryLoopIntermediateInfo) func(trace.RetryLoopDoneInfo) {
		if cfg.detailer.Details()&trace.RetryEvents != 0 && isTraceRetry(*info.Context) { //nolint:nestif
			operationName := info.Label
			if operationName == "" {
				operationName = info.Call.FunctionID()
			}
			if functionID := functionID(*info.Context); functionID != "" {
				operationName = functionID
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
			ctx := *info.Context
			return func(info trace.RetryLoopIntermediateInfo) func(trace.RetryLoopDoneInfo) {
				if info.Error != nil {
					start.RecordError(info.Error)
				}
				return func(info trace.RetryLoopDoneInfo) {
					start.SetAttributes(
						attribute.Int("attempts", info.Attempts),
						attribute.Bool(errorAttribute, info.Error != nil),
					)
					if fields := fieldsFromStore(ctx); len(fields) > 0 {
						start.SetAttributes(fields...)
					}
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
	return t
}
