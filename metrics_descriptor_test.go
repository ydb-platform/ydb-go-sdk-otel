package ydb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestGaugeExportsSeparateSeriesForExtraLabels(t *testing.T) {
	meter, reader := manualMeter(t)
	cfg := metricsConfigFromOpts(meter)
	vec := cfg.GaugeVec("sessions", "node_id")
	vec.With(map[string]string{"node_id": "1", "az": "a"}).Set(10)
	vec.With(map[string]string{"node_id": "1", "az": "b"}).Set(20)

	instrument := collectMetric(t, reader)
	sum, ok := instrument.Data.(metricdata.Sum[float64])
	require.True(t, ok)
	require.Len(t, sum.DataPoints, 2)
	values := make(map[attribute.Distinct]float64, len(sum.DataPoints))
	for _, point := range sum.DataPoints {
		values[point.Attributes.Equivalent()] = point.Value
	}
	first := attribute.NewSet(attribute.String("node_id", "1"), attribute.String("az", "a"))
	second := attribute.NewSet(attribute.String("node_id", "1"), attribute.String("az", "b"))
	require.Equal(t, map[attribute.Distinct]float64{first.Equivalent(): 10, second.Equivalent(): 20}, values)
}

func TestDescriptorGaugeSetSharesSeriesAcrossSchemas(t *testing.T) {
	meter, reader := manualMeter(t)
	cfg, ok := metricsConfigFromOpts(meter).(*metricsConfig)
	require.True(t, ok)
	labels := map[string]string{"a": "x"}
	cfg.GaugeVecWithDescriptor("balance", "By", "a", "b").With(labels).Set(10)
	cfg.GaugeVecWithDescriptor("balance", "By", "a").With(labels).Set(20)

	instrument := collectMetric(t, reader)
	sum, ok := instrument.Data.(metricdata.Sum[float64])
	require.True(t, ok)
	require.Len(t, sum.DataPoints, 1)
	require.Equal(t, float64(20), sum.DataPoints[0].Value)
	require.Equal(t, attribute.NewSet(attribute.String("a", "x")), sum.DataPoints[0].Attributes)
}

func TestDescriptorCounterKeepsFirstUnitAcrossScopes(t *testing.T) {
	meter, reader := manualMeter(t)
	cfg, ok := metricsConfigFromOpts(meter).(*metricsConfig)
	require.True(t, ok)
	scoped, ok := cfg.WithSystem("topic").(*metricsConfig)
	require.True(t, ok)
	cfg.CounterVecWithDescriptor("received", "By", "a").With(nil).Inc()
	scoped.CounterVecWithDescriptor("received", "{message}", "b").With(nil).Inc()

	instrument := collectMetric(t, reader)
	require.Equal(t, "By", instrument.Unit)
	sum, ok := instrument.Data.(metricdata.Sum[int64])
	require.True(t, ok)
	require.Len(t, sum.DataPoints, 1)
	require.Equal(t, int64(2), sum.DataPoints[0].Value)
}

func TestDescriptorAndLegacyGaugeKeepFirstUnitAndBinding(t *testing.T) {
	meter, reader := manualMeter(t)
	cfg, ok := metricsConfigFromOpts(meter, WithNamespace("ydb"), WithSeparator(".")).(*metricsConfig)
	require.True(t, ok)
	scoped, ok := cfg.WithSystem("topic").(*metricsConfig)
	require.True(t, ok)
	descriptor := cfg.GaugeVecWithDescriptor("ydb.topic.balance", "By", "a")
	legacy := scoped.GaugeVec("balance", "a")
	descriptor.With(map[string]string{"a": ""}).Set(10)
	legacy.With(nil).Set(20)
	descriptor.With(nil).Set(30)

	instrument := collectMetric(t, reader)
	require.Equal(t, "By", instrument.Unit)
	sum, ok := instrument.Data.(metricdata.Sum[float64])
	require.True(t, ok)
	require.Len(t, sum.DataPoints, 2)
	values := make(map[attribute.Distinct]float64, len(sum.DataPoints))
	for _, point := range sum.DataPoints {
		values[point.Attributes.Equivalent()] = point.Value
	}
	empty := attribute.NewSet()
	explicit := attribute.NewSet(attribute.String("a", ""))
	require.Equal(t, map[attribute.Distinct]float64{empty.Equivalent(): 30, explicit.Equivalent(): 20}, values)
}

func TestDescriptorBindingAllocationParity(t *testing.T) {
	for _, binding := range metricOperations(t) {
		if binding.name != "CounterWith" && binding.name != "GaugeWith" {
			continue
		}

		t.Run(binding.name, func(t *testing.T) {
			legacy := testing.AllocsPerRun(100, binding.legacy)
			descriptor := testing.AllocsPerRun(100, binding.descriptor)
			require.LessOrEqual(t, descriptor, legacy)
		})
	}
}

func TestBoundMetricRecordingAllocationBudget(t *testing.T) {
	for _, operation := range metricOperations(t) {
		if operation.name == "CounterWith" || operation.name == "GaugeWith" {
			continue
		}

		t.Run(operation.name, func(t *testing.T) {
			// The SDK can allocate once per measurement even with a bound option.
			require.LessOrEqual(t, testing.AllocsPerRun(100, operation.legacy), float64(1), "legacy")
			require.LessOrEqual(t, testing.AllocsPerRun(100, operation.descriptor), float64(1), "descriptor")
		})
	}
}

func BenchmarkMetricOperations(b *testing.B) {
	for _, operation := range metricOperations(b) {
		b.Run("Legacy/"+operation.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				operation.legacy()
			}
		})
		b.Run("Descriptor/"+operation.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				operation.descriptor()
			}
		})
	}
}

type metricOperation struct {
	name       string
	legacy     func()
	descriptor func()
}

func metricOperations(t testing.TB) []metricOperation {
	t.Helper()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(sdkmetric.NewManualReader()))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
	cfg, ok := metricsConfigFromOpts(provider.Meter("test")).(*metricsConfig)
	require.True(t, ok)
	names := []string{"database", "endpoint", "reader.name", "topic", "consumer"}
	labels := map[string]string{
		"database": "/db", "endpoint": "host", "reader.name": "reader", "topic": "topic", "consumer": "c",
	}
	legacyCounterVec := cfg.CounterVec("legacy.requests", names...)
	descriptorCounterVec := cfg.CounterVecWithDescriptor("descriptor.requests", "{message}", names...)
	legacyGaugeVec := cfg.GaugeVec("legacy.balance", names...)
	descriptorGaugeVec := cfg.GaugeVecWithDescriptor("descriptor.balance", "By", names...)
	legacyCounter, descriptorCounter := legacyCounterVec.With(labels), descriptorCounterVec.With(labels)
	legacyAdder, ok := legacyCounter.(interface{ Add(delta int64) })
	require.True(t, ok)
	descriptorAdder, ok := descriptorCounter.(interface{ Add(delta int64) })
	require.True(t, ok)
	legacyGauge, descriptorGauge := legacyGaugeVec.With(labels), descriptorGaugeVec.With(labels)

	return []metricOperation{
		{"CounterWith", func() { legacyCounterVec.With(labels) }, func() { descriptorCounterVec.With(labels) }},
		{"GaugeWith", func() { legacyGaugeVec.With(labels) }, func() { descriptorGaugeVec.With(labels) }},
		{"CounterAdd", func() { legacyAdder.Add(2) }, func() { descriptorAdder.Add(2) }},
		{"CounterInc", legacyCounter.Inc, descriptorCounter.Inc},
		{"GaugeAdd", func() { legacyGauge.Add(2) }, func() { descriptorGauge.Add(2) }},
	}
}
