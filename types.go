package tracing

import (
	"context"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"net/url"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const errorAttribute = "error"

func logError(s trace.Span, err error, fields ...attribute.KeyValue) {
	s.RecordError(err, append(fields, attribute.Bool(errorAttribute, true)))
	m := retry.Check(err)
	if v := s.BaggageItem("idempotent"); v != "" {
		s.SetAttributes(attribute.String(errorAttribute+".retryable", m.MustRetry(v == "true")))
	}
	s.SetAttributes(attribute.String(errorAttribute+".delete_session", m.MustDeleteSession()))
}

func finish(s trace.Span, err error, fields ...attribute.KeyValue) {
	if err != nil {
		logError(s, err, fields...)
	} else {
		s.SetAttributes(fields...)
	}
	s.End()
}

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

func startSpanWithCounter(ctx *context.Context, operationName string, counterName string, fields ...attribute.KeyValue) (c *counter) {
	defer func() {
		c.span.SetAttributes(attribute.String("ydb.driver.sensor", operationName+"_"+counterName))
	}()
	return &counter{
		span:    startSpan(ctx, operationName, fields...),
		counter: 0,
		name:    counterName,
	}
}

func startSpan(ctx *context.Context, operationName string, fields ...attribute.KeyValue) (s trace.Span) {
	if ctx != nil {
		var childCtx context.Context
		s, childCtx = trace.StartSpanFromContext(*ctx, operationName)
		*ctx = childCtx
	} else {
		s = trace.StartSpan(operationName)
	}
	s.SetAttributes(append(fields, attribute.String("ydb-go-sdk", "v"+ydb.Version)))
	return s
}

func followSpan(related trace.SpanContext, ctx *context.Context, operationName string, fields ...attribute.KeyValue) (s trace.Span) {
	if ctx != nil {
		var childCtx context.Context
		s, childCtx = trace.StartSpanFromContext(*ctx, operationName, trace.FollowsFrom(related))
		*ctx = childCtx
	} else {
		s = trace.StartSpan(operationName)
	}
	s.SetAttributes(append(fields, attribute.String("ydb-go-sdk", "v"+ydb.Version)))
	return s
}

func nodeID(sessionID string) string {
	u, err := url.Parse(sessionID)
	if err != nil {
		return ""
	}
	return u.Query().Get("node_id")
}
