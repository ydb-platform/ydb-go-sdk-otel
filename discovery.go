package ydb

import (
	"go.opentelemetry.io/otel/attribute"
	otelTrace "go.opentelemetry.io/otel/trace"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func Discovery(tracer otelTrace.Tracer, details trace.Details) (t trace.Discovery) {
	if details&trace.DiscoveryEvents != 0 {
		t.OnDiscover = func(info trace.DiscoveryDiscoverStartInfo) func(discovery trace.DiscoveryDiscoverDoneInfo) {
			start := startSpan(
				tracer,
				info.Context,
				"ydb_discovery",
				attribute.String("address", info.Address),
				attribute.String("database", info.Database),
			)
			return func(info trace.DiscoveryDiscoverDoneInfo) {
				endpoints := make([]string, len(info.Endpoints))
				for i, e := range info.Endpoints {
					endpoints[i] = e.String()
				}
				finish(
					start,
					info.Error,
					attribute.StringSlice("endpoints", endpoints),
				)
			}
		}
	}
	return t
}
