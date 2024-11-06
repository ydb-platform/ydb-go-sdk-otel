package ydb

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/spans"
	"go.opentelemetry.io/otel/codes"
	otelTrace "go.opentelemetry.io/otel/trace"
)

var (
	_ spans.Span = (*span)(nil)
	_ spans.Span = nopSpan{}
)

type (
	span struct {
		span otelTrace.Span
	}
	nopSpan struct{}
)

func (nopSpan) TraceID() (_ string, valid bool) {
	return "", false
}

func (nopSpan) Link(link spans.Span, attributes ...spans.KeyValue) {}

func (nopSpan) Log(msg string, attributes ...spans.KeyValue) {}

func (nopSpan) Warn(err error, attributes ...spans.KeyValue) {}

func (nopSpan) Error(err error, attributes ...spans.KeyValue) {}

func (nopSpan) End(attributes ...spans.KeyValue) {}

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
		SpanContext: link.(*span).span.SpanContext(), //nolint:forcetypeassert
		Attributes:  fieldsToAttributes(fields),
	})
}

func (s *span) End(fields ...spans.KeyValue) {
	s.span.SetAttributes(fieldsToAttributes(fields)...)
	s.span.End()
}
