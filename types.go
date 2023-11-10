package ydb

import (
	"context"
	"errors"
	"net/url"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	ydbRetry "github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

const (
	errorAttribute = "error"
	tracerID       = "ydb-go-sdk"
	version        = "v" + ydb.Version
)

func logError(s trace.Span, err error, fields ...attribute.KeyValue) {
	s.RecordError(err, trace.WithAttributes(append(fields, attribute.Bool(errorAttribute, true))...))
	s.SetStatus(codes.Error, err.Error())
	m := ydbRetry.Check(err)
	s.SetAttributes(
		attribute.Bool(errorAttribute+".delete_session", m.MustDeleteSession()),
		attribute.Bool(errorAttribute+".must_retry", m.MustRetry(false)),
		attribute.Bool(errorAttribute+".must_retry_idempotent", m.MustRetry(true)),
	)
	var ydbErr ydb.Error
	if errors.As(err, &ydbErr) {
		s.SetAttributes(
			attribute.Int(errorAttribute+".ydb.code", int(ydbErr.Code())),
			attribute.String(errorAttribute+".ydb.name", ydbErr.Name()),
		)
	}
}

func finish(s trace.Span, err error, fields ...attribute.KeyValue) {
	if err != nil {
		logError(s, err, fields...)
	} else if len(fields) > 0 {
		s.SetAttributes(fields...)
	}
	s.End()
}

func childSpanWithReplaceCtx(
	tracer trace.Tracer,
	ctx *context.Context,
	operationName string,
	fields ...attribute.KeyValue,
) (s trace.Span) {
	fields = append(fields, attribute.String("ydb-go-sdk", version))
	*ctx, s = childSpan(tracer, *ctx, operationName, fields...)
	return s
}

func childSpan(
	tracer trace.Tracer,
	ctx context.Context, //nolint:revive
	operationName string,
	fields ...attribute.KeyValue,
) (context.Context, trace.Span) {
	fields = append(fields, attribute.String("ydb-go-sdk", version))
	return tracer.Start(ctx,
		operationName,
		trace.WithAttributes(fields...),
	)
}

func nodeID(sessionID string) string {
	u, err := url.Parse(sessionID)
	if err != nil {
		return ""
	}
	return u.Query().Get("node_id")
}
