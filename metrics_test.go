package ydb

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
)

func TestCounterVecDifferentLabelNamesNotCachedTogether(t *testing.T) {
	cfg, ok := metricsConfigFromOpts(noop.NewMeterProvider().Meter("test")).(*metricsConfig)
	require.True(t, ok)

	vecA := cfg.CounterVec("requests", "status")
	vecB := cfg.CounterVec("requests", "method")

	require.NotSame(t, vecA, vecB)
}

func TestGaugeVecWithDifferentExtraLabelsNotCachedTogether(t *testing.T) {
	cfg, ok := metricsConfigFromOpts(noop.NewMeterProvider().Meter("test")).(*metricsConfig)
	require.True(t, ok)

	vec, ok := cfg.GaugeVec("sessions", "node_id").(*gaugeVec)
	require.True(t, ok)

	gaugeA := vec.With(map[string]string{
		"node_id": "1",
		"az":      "a",
	})
	gaugeB := vec.With(map[string]string{
		"node_id": "1",
		"az":      "b",
	})

	require.NotSame(t, gaugeA, gaugeB)
}

func TestLabelsCacheKeyIncludesExtraLabels(t *testing.T) {
	keyA := labelsCacheKey(map[string]string{
		"node_id": "1",
		"az":      "a",
	}, []string{"node_id"})
	keyB := labelsCacheKey(map[string]string{
		"node_id": "1",
		"az":      "b",
	}, []string{"node_id"})

	require.NotEqual(t, keyA, keyB)
}
