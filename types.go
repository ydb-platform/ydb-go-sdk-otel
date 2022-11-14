package ydb

import (
	"context"
	"net/url"
	"sync/atomic"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

const (
	errorAttribute = "error"
	tracerID       = "ydb-go-sdk"
	version        = "v" + ydb.Version
)

func logError(s trace.Span, err error, fields ...attribute.KeyValue) {
	s.RecordError(err, trace.WithAttributes(append(fields, attribute.Bool(errorAttribute, true))...))
	m := retry.Check(err)
	s.SetAttributes(
		attribute.Bool(errorAttribute+".delete_session", m.MustDeleteSession()),
		attribute.Bool(errorAttribute+".must_retry", m.MustRetry(false)),
		attribute.Bool(errorAttribute+".must_retry_idempotent", m.MustRetry(true)),
	)
}

func finish(s trace.Span, err error, fields ...attribute.KeyValue) {
	if err != nil {
		logError(s, err, fields...)
	} else {
		s.SetAttributes(fields...)
	}
	s.End()
}

//nolint:unparam
func intermediate(s trace.Span, err error, fields ...attribute.KeyValue) {
	if err != nil {
		logError(s, err, fields...)
	} else {
		s.SetAttributes(fields...)
	}
}

type counter struct {
	span    trace.Span
	counter int64
	name    string
}

func (s *counter) add(delta int64) {
	atomic.AddInt64(&s.counter, delta)
	s.span.SetAttributes(
		attribute.Int64(s.name, atomic.LoadInt64(&s.counter)),
	)
}

func startSpanWithCounter(
	tracer trace.Tracer,
	ctx *context.Context,
	operationName string,
	counterName string,
	fields ...attribute.KeyValue,
) (c *counter) {
	fields = append(fields, attribute.String("ydb.driver.sensor", operationName+"_"+counterName))
	return &counter{
		span:    startSpan(tracer, ctx, operationName, fields...),
		counter: 0,
		name:    counterName,
	}
}

func startSpan(
	tracer trace.Tracer,
	ctx *context.Context,
	operationName string,
	fields ...attribute.KeyValue,
) (s trace.Span) {
	fields = append(fields, attribute.String("ydb-go-sdk", version))
	*ctx, s = tracer.Start(
		*ctx,
		operationName,
		trace.WithAttributes(fields...),
	)
	*ctx = ydb.WithTraceID(*ctx, s.SpanContext().TraceID().String())
	return s
}

func followSpan(
	tracer trace.Tracer,
	related trace.SpanContext,
	ctx *context.Context,
	operationName string,
	fields ...attribute.KeyValue,
) (s trace.Span) {
	fields = append(fields, attribute.String("ydb-go-sdk", version))
	*ctx, s = tracer.Start(
		trace.ContextWithRemoteSpanContext(*ctx, related),
		operationName,
		trace.WithAttributes(fields...),
	)
	return s
}

func nodeID(sessionID string) string {
	u, err := url.Parse(sessionID)
	if err != nil {
		return ""
	}
	return u.Query().Get("node_id")
}
