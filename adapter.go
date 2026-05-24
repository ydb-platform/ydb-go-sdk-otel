package ydb

import (
	"context"
	"slices"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xslices"
	"github.com/ydb-platform/ydb-go-sdk/v3/spans"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	otelTrace "go.opentelemetry.io/otel/trace"
)

const traceIDLogField = "otel-trace-id"

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

	if spanCtx := s.SpanContext(); spanCtx.IsValid() {
		logFields := log.FieldsFromContext(childCtx)
		if !slices.Contains(xslices.Transform(logFields, func(field log.Field) string {
			return field.Key()
		}), traceIDLogField) {
			childCtx = log.WithFields(childCtx, log.String(traceIDLogField, spanCtx.TraceID().String()))
		}
	}

	return childCtx, &span{ //nolint:spancheck
		span: s,
	}
}

func WithTraces(tracer otelTrace.Tracer, opts ...tracesOption) ydb.Option {
	cfg := &adapter{
		tracer:   tracer,
		detailer: trace.DetailsAll,
	}
	for _, opt := range opts {
		opt.applyTracesOption(cfg)
	}

	return spans.WithTraces(cfg)
}
