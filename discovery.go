package ydb

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"go.opentelemetry.io/otel/attribute"
)

func Discovery(cfg *config) (t trace.Discovery) {
	t.OnDiscover = func(info trace.DiscoveryDiscoverStartInfo) func(discovery trace.DiscoveryDiscoverDoneInfo) {
		if cfg.detailer.Details()&trace.DiscoveryEvents != 0 {
			start := startSpan(
				cfg.tracer,
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
		return nil
	}
	return t
}
