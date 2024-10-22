package ydb

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/spans"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"go.opentelemetry.io/otel"
)

func Retry(opts ...Option) trace.Retry {
	cfg := &adapter{
		detailer: trace.DetailsAll,
	}
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.tracer == nil {
		cfg.tracer = otel.Tracer(tracerID)
	}

	return spans.Retry(cfg)
}
