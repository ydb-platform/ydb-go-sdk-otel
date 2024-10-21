package ydb

import (
	"go.opentelemetry.io/otel/codes"
	otelTrace "go.opentelemetry.io/otel/trace"

	"github.com/ydb-platform/ydb-go-sdk/v3/spans"
)

var _ spans.Span = (*span)(nil)

type span struct {
	span otelTrace.Span
}

func (s *span) Log(msg string, fields ...spans.KeyValue) {
	s.span.AddEvent(msg, otelTrace.WithAttributes(fieldsToAttributes(fields)...))
}

func (s *span) Warn(err error, fields ...spans.KeyValue) {
	s.span.RecordError(err, otelTrace.WithAttributes(fieldsToAttributes(fields)...))
}

func (s *span) Error(err error, fields ...spans.KeyValue) {
	s.span.RecordError(err, otelTrace.WithAttributes(fieldsToAttributes(fields)...))
	s.span.SetStatus(codes.Error, err.Error())
}

func (s *span) TraceID() (string, bool) {
	traceID := s.span.SpanContext().TraceID()

	return traceID.String(), traceID.IsValid()
}

func (s *span) Link(link spans.Span, fields ...spans.KeyValue) {
	s.span.AddLink(otelTrace.Link{
		SpanContext: link.(*span).span.SpanContext(),
		Attributes:  fieldsToAttributes(fields),
	})
}

func (s *span) End(fields ...spans.KeyValue) {
	s.span.SetAttributes(fieldsToAttributes(fields)...)
	s.span.End()
}
