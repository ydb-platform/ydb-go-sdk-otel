package ydb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	otelLog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/logtest"
	otelTrace "go.opentelemetry.io/otel/trace"
)

func TestTraceCorrelationAttributeFromContext(t *testing.T) {
	traceID, err := otelTrace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	require.NoError(t, err)
	spanID, err := otelTrace.SpanIDFromHex("0102030405060708")
	require.NoError(t, err)

	ctx := otelTrace.ContextWithSpanContext(context.Background(), otelTrace.NewSpanContext(otelTrace.SpanContextConfig{
		TraceID: traceID,
		SpanID:  spanID,
	}))

	attr, ok := traceCorrelationAttribute(ctx, nil)
	require.True(t, ok)
	require.Equal(t, traceIDLogField, attr.Key)
	require.Equal(t, traceID.String(), attr.Value.AsString())
}

func TestTraceCorrelationAttributeSkipsDuplicateField(t *testing.T) {
	traceID, err := otelTrace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	require.NoError(t, err)
	spanID, err := otelTrace.SpanIDFromHex("0102030405060708")
	require.NoError(t, err)

	ctx := otelTrace.ContextWithSpanContext(context.Background(), otelTrace.NewSpanContext(otelTrace.SpanContextConfig{
		TraceID: traceID,
		SpanID:  spanID,
	}))

	_, ok := traceCorrelationAttribute(ctx, []log.Field{
		log.String(traceIDLogField, "already-set"),
	})
	require.False(t, ok)
}

func TestLogAdapterAddsTraceIDFromContext(t *testing.T) {
	traceID, err := otelTrace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	require.NoError(t, err)
	spanID, err := otelTrace.SpanIDFromHex("0102030405060708")
	require.NoError(t, err)

	recorder := logtest.NewRecorder()
	adapter := &logAdapter{logger: recorder.Logger("test")}

	ctx := otelTrace.ContextWithSpanContext(context.Background(), otelTrace.NewSpanContext(otelTrace.SpanContextConfig{
		TraceID: traceID,
		SpanID:  spanID,
	}))

	adapter.Log(ctx, "hello")

	scopes := recorder.Result()
	require.Len(t, scopes, 1)
	require.Len(t, scopes[0].Records, 1)

	var found bool

	scopes[0].Records[0].WalkAttributes(func(kv otelLog.KeyValue) bool {
		if kv.Key == traceIDLogField && kv.Value.AsString() == traceID.String() {
			found = true
		}

		return true
	})
	require.True(t, found)
}
