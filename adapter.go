package ydb

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/spans"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"go.opentelemetry.io/otel"
	otelTrace "go.opentelemetry.io/otel/trace"
)

const tracerID = "ydb-go-sdk"

var _ spans.Adapter = (*adapter)(nil)

type adapter struct {
	tracer   otelTrace.Tracer
	detailer trace.Detailer
}

func (cfg *adapter) Details() trace.Details {
	return cfg.detailer.Details()
}

func (cfg *adapter) SpanFromContext(ctx context.Context) spans.Span {
	return &span{
		span: otelTrace.SpanFromContext(ctx),
	}
}

func (cfg *adapter) Start(ctx context.Context, operationName string, fields ...spans.KeyValue) (
	context.Context, spans.Span,
) {
	childCtx, s := cfg.tracer.Start(ctx, operationName, //nolint:spancheck
		otelTrace.WithAttributes(fieldsToAttributes(fields)...),
	)

	return childCtx, &span{ //nolint:spancheck
		span: s,
	}
}

func WithTraces(opts ...Option) ydb.Option {
	cfg := &adapter{
		detailer: trace.DetailsAll,
	}
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.tracer == nil {
		cfg.tracer = otel.Tracer(tracerID)
	}

	return spans.WithTraces(cfg)
}
