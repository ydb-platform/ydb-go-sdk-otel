package ydb

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
)

func TestTracerFromUsesGlobalWhenNil(t *testing.T) {
	require.NotNil(t, tracerFrom(nil))
}

func TestMeterFromUsesGlobalWhenNil(t *testing.T) {
	require.NotNil(t, meterFrom(nil))
}

func TestLoggerFromUsesGlobalWhenNil(t *testing.T) {
	require.NotNil(t, loggerFrom(nil))
}

func TestMetricsConfigFromOptsNilMeter(t *testing.T) {
	cfg, ok := metricsConfigFromOpts(nil).(*metricsConfig)
	require.True(t, ok)
	require.NotNil(t, cfg.meter)

	vec := cfg.CounterVec("requests", "status")
	require.NotNil(t, vec)
	vec.With(map[string]string{"status": "ok"}).Inc()
}

func TestMetricsConfigFromOptsCustomMeter(t *testing.T) {
	custom := noop.NewMeterProvider().Meter("custom")
	cfg, ok := metricsConfigFromOpts(custom).(*metricsConfig)
	require.True(t, ok)
	require.Equal(t, custom, cfg.meter)
}
