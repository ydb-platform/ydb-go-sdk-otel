package ydb

import (
	otelTrace "go.opentelemetry.io/otel/trace"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type Option func(c *adapter)

func WithTracer(tracer otelTrace.Tracer) Option {
	return func(c *adapter) {
		c.tracer = tracer
	}
}

func WithDetails(d trace.Details) Option {
	return func(c *adapter) {
		c.detailer = d
	}
}

func WithDetailer(d trace.Detailer) Option {
	return func(c *adapter) {
		c.detailer = d
	}
}
