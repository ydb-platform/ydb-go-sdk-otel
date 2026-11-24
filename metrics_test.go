package ydb

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	ydbmetrics "github.com/ydb-platform/ydb-go-sdk/v3/metrics"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
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

func TestDescriptorCounterUsesExactInt64Instrument(t *testing.T) {
	meter, reader := manualMeter(t)
	cfg := metricsConfigFromOpts(meter, WithNamespace("custom"), WithSeparator("_"))
	scoped := cfg.WithSystem("topic")
	descriptorConfig, ok := scoped.(interface {
		CounterVecWithDescriptor(name, unit string, labelNames ...string) ydbmetrics.CounterVec
	})
	require.True(t, ok)

	vec := descriptorConfig.CounterVecWithDescriptor(
		"ydb.topic.reader.received.bytes", "By", "endpoint", "database", "consumer",
	)
	counter := vec.With(map[string]string{"endpoint": "node", "database": "/db"})
	adder, ok := counter.(interface{ Add(delta int64) })
	require.True(t, ok)
	largeDelta := (int64(1) << 53) + 1
	adder.Add(largeDelta)
	adder.Add(1 << 20)
	counter.Inc()

	instrument := collectMetric(t, reader)
	require.Equal(t, "ydb.topic.reader.received.bytes", instrument.Name)
	require.Equal(t, "By", instrument.Unit)
	sum, ok := instrument.Data.(metricdata.Sum[int64])
	require.True(t, ok)
	require.True(t, sum.IsMonotonic)
	require.Len(t, sum.DataPoints, 1)
	require.Equal(t, largeDelta+(1<<20)+1, sum.DataPoints[0].Value)
	require.Equal(t, attribute.NewSet(attribute.String("database", "/db"), attribute.String("endpoint", "node")),
		sum.DataPoints[0].Attributes)
}

func TestLegacyCounterUsesInt64Instrument(t *testing.T) {
	meter, reader := manualMeter(t)
	cfg := metricsConfigFromOpts(meter, WithNamespace("custom"), WithSeparator("."))
	scoped, ok := cfg.WithSystem("topic").(*metricsConfig)
	require.True(t, ok)

	vec := scoped.CounterVec("requests", "status")
	vec.With(map[string]string{"status": "ok"}).Inc()

	instrument := collectMetric(t, reader)
	require.Equal(t, "custom.topic.requests", instrument.Name)
	require.Empty(t, instrument.Unit)
	sum, ok := instrument.Data.(metricdata.Sum[int64])
	require.True(t, ok)
	require.Len(t, sum.DataPoints, 1)
	require.EqualValues(t, 1, sum.DataPoints[0].Value)
}

func TestDescriptorCacheSharesEquivalentInstruments(t *testing.T) {
	meter := &recordingMeter{}
	cfg, ok := metricsConfigFromOpts(meter).(*metricsConfig)
	require.True(t, ok)

	legacy := cfg.CounterVec("same")
	require.Same(t, legacy, cfg.CounterVec("same"))

	descriptorEmpty := cfg.CounterVecWithDescriptor("same", "")
	require.Same(t, descriptorEmpty, cfg.CounterVecWithDescriptor("same", ""))
	require.Same(t, legacy, descriptorEmpty)

	descriptorBy := cfg.CounterVecWithDescriptor("same", "By")
	require.NotSame(t, descriptorEmpty, descriptorBy)
	require.Equal(t, 2, meter.int64Counters)

	legacyGauge := cfg.GaugeVec("gauge")
	require.Same(t, legacyGauge, cfg.GaugeVec("gauge"))
	descriptorGaugeEmpty := cfg.GaugeVecWithDescriptor("gauge", "")
	require.Same(t, descriptorGaugeEmpty, cfg.GaugeVecWithDescriptor("gauge", ""))
	require.Same(t, legacyGauge, descriptorGaugeEmpty)
	labels := map[string]string{"label": "value"}
	legacyGauge.With(labels).Set(10)
	descriptorGaugeEmpty.With(labels).Set(20)
	require.Equal(t, []float64{10, 10}, meter.upDownCounters[0].adds)
	require.NotSame(t, descriptorGaugeEmpty, cfg.GaugeVecWithDescriptor("gauge", "By"))
}

func TestMetricFactoriesShareVectorDuringConcurrentCreation(t *testing.T) {
	for _, test := range []struct {
		name   string
		create func(*metricsConfig) any
	}{
		{name: "counter", create: func(c *metricsConfig) any { return c.CounterVec("metric") }},
		{name: "descriptor counter", create: func(c *metricsConfig) any {
			return c.CounterVecWithDescriptor("metric", "1")
		}},
		{name: "gauge", create: func(c *metricsConfig) any { return c.GaugeVec("metric") }},
		{name: "descriptor gauge", create: func(c *metricsConfig) any {
			return c.GaugeVecWithDescriptor("metric", "1")
		}},
		{name: "timer", create: func(c *metricsConfig) any { return c.TimerVec("metric") }},
		{name: "histogram", create: func(c *metricsConfig) any { return c.HistogramVec("metric", nil) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			entered := make(chan struct{}, 2)
			release := make(chan struct{})
			meter := &blockingMetricMeter{beforeCreate: func() {
				entered <- struct{}{}
				<-release
			}}
			cfg, ok := metricsConfigFromOpts(meter).(*metricsConfig)
			require.True(t, ok)
			created := make(chan any, 2)
			for range 2 {
				go func() { created <- test.create(cfg) }()
			}

			// Both calls must reach OTel before either instrument has been cached.
			for range 2 {
				select {
				case <-entered:
				case <-time.After(5 * time.Second):
					close(release)
					t.Fatal("instrument creation blocked another factory call")
				}
			}
			close(release)

			first := <-created
			require.Same(t, first, <-created)
			require.Same(t, first, test.create(cfg))
		})
	}
}

func TestDescriptorGaugeExportsCompensatedBalance(t *testing.T) {
	meter, reader := manualMeter(t)
	cfg := metricsConfigFromOpts(meter, WithNamespace("custom"), WithSeparator("_"))
	scoped := cfg.WithSystem("topic")
	descriptorConfig, ok := scoped.(interface {
		GaugeVecWithDescriptor(name, unit string, labelNames ...string) ydbmetrics.GaugeVec
	})
	require.True(t, ok)
	vec := descriptorConfig.GaugeVecWithDescriptor(
		"ydb.topic.reader.credit_balance_bytes", "By", "endpoint", "consumer",
	)

	vec.With(map[string]string{"endpoint": "node", "consumer": ""}).Add(3)
	vec.With(map[string]string{"endpoint": "node"}).Add(7)
	vec.With(map[string]string{"endpoint": "node"}).Add(-7)

	instrument := collectMetric(t, reader)
	require.Equal(t, "ydb.topic.reader.credit_balance_bytes", instrument.Name)
	require.Equal(t, "By", instrument.Unit)
	sum, ok := instrument.Data.(metricdata.Sum[float64])
	require.True(t, ok)
	require.False(t, sum.IsMonotonic)
	require.Len(t, sum.DataPoints, 2)
	values := make(map[attribute.Distinct]float64, len(sum.DataPoints))
	for _, point := range sum.DataPoints {
		values[point.Attributes.Equivalent()] = point.Value
	}
	omitted := attribute.NewSet(attribute.String("endpoint", "node"))
	explicit := attribute.NewSet(attribute.String("endpoint", "node"), attribute.String("consumer", ""))
	require.Equal(t, map[attribute.Distinct]float64{omitted.Equivalent(): 0, explicit.Equivalent(): 3}, values)
}

func manualMeter(t *testing.T) (metric.Meter, *sdkmetric.ManualReader) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })

	return provider.Meter("test"), reader
}

func collectMetric(t *testing.T, reader *sdkmetric.ManualReader) metricdata.Metrics {
	var data metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &data))
	require.Len(t, data.ScopeMetrics, 1)
	require.Len(t, data.ScopeMetrics[0].Metrics, 1)

	return data.ScopeMetrics[0].Metrics[0]
}

type recordingMeter struct {
	noop.Meter

	int64Counters  int
	upDownCounters []*recordingFloat64UpDownCounter
}

func (m *recordingMeter) Int64Counter(name string, opts ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	m.int64Counters++

	return m.Meter.Int64Counter(name, opts...)
}

func (m *recordingMeter) Float64UpDownCounter(
	_ string,
	_ ...metric.Float64UpDownCounterOption,
) (metric.Float64UpDownCounter, error) {
	counter := &recordingFloat64UpDownCounter{}
	m.upDownCounters = append(m.upDownCounters, counter)

	return counter, nil
}

type recordingFloat64UpDownCounter struct {
	noop.Float64UpDownCounter

	adds []float64
}

func (c *recordingFloat64UpDownCounter) Add(_ context.Context, value float64, _ ...metric.AddOption) {
	c.adds = append(c.adds, value)
}

type blockingMetricMeter struct {
	noop.Meter

	beforeCreate func()
}

func (m *blockingMetricMeter) Int64Counter(
	name string,
	opts ...metric.Int64CounterOption,
) (metric.Int64Counter, error) {
	m.beforeCreate()

	return m.Meter.Int64Counter(name, opts...)
}

func (m *blockingMetricMeter) Float64UpDownCounter(
	name string,
	opts ...metric.Float64UpDownCounterOption,
) (metric.Float64UpDownCounter, error) {
	m.beforeCreate()

	return m.Meter.Float64UpDownCounter(name, opts...)
}

func (m *blockingMetricMeter) Float64Histogram(
	name string,
	opts ...metric.Float64HistogramOption,
) (metric.Float64Histogram, error) {
	m.beforeCreate()

	return m.Meter.Float64Histogram(name, opts...)
}
