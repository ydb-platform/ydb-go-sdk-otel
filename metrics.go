package ydb

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/metrics"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const defaultMetricsSeparator = "_"

var defaultTimerBuckets = []float64{
	0.0005, 0.001, 0.0025, 0.005, 0.0075, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25,
	0.5, 0.75, 1, 2.5, 5, 7.5, 10, 30, 60,
}

var _ metrics.Config = (*metricsConfig)(nil)

type metricsConfig struct {
	meter        metric.Meter
	detailer     trace.Detailer
	namespace    string
	separator    string
	timerBuckets []float64

	m          sync.Mutex
	counters   *vectorCache[metrics.CounterVec]
	gauges     *vectorCache[metrics.GaugeVec]
	timers     map[metricInstrumentKey]metrics.TimerVec
	histograms map[metricInstrumentKey]metrics.HistogramVec
}

type metricInstrumentKey struct {
	name       string
	buckets    string
	labelNames string
}

func newMetricInstrumentKey(name, buckets string, labelNames []string) metricInstrumentKey {
	return metricInstrumentKey{
		name:       name,
		buckets:    buckets,
		labelNames: strings.Join(labelNames, "\xff"),
	}
}

// metricsConfigFromOpts returns metrics registry config for OpenTelemetry instruments.
func metricsConfigFromOpts(meter metric.Meter, opts ...metricsOption) metrics.Config {
	cfg := &metricsConfig{
		meter:        meterFrom(meter),
		detailer:     trace.DetailsAll,
		separator:    defaultMetricsSeparator,
		timerBuckets: defaultTimerBuckets,
		counters:     &vectorCache[metrics.CounterVec]{},
		gauges:       &vectorCache[metrics.GaugeVec]{},
		timers:       map[metricInstrumentKey]metrics.TimerVec{},
		histograms:   map[metricInstrumentKey]metrics.HistogramVec{},
	}
	for _, opt := range opts {
		opt.applyMetricsOption(cfg)
	}

	return cfg
}

// WithMetrics enables ydb-go-sdk metrics export via OpenTelemetry.
// If meter is nil, otel.Meter("ydb-go-sdk") is used.
func WithMetrics(meter metric.Meter, opts ...metricsOption) ydb.Option {
	return metrics.WithTraces(metricsConfigFromOpts(meter, opts...))
}

func (c *metricsConfig) Details() trace.Details {
	return c.detailer.Details()
}

func (c *metricsConfig) WithSystem(subsystem string) metrics.Config {
	return &metricsConfig{
		meter:        c.meter,
		detailer:     c.detailer,
		namespace:    c.join(c.namespace, subsystem),
		separator:    c.separator,
		timerBuckets: c.timerBuckets,
		counters:     c.counters,
		gauges:       c.gauges,
		timers:       map[metricInstrumentKey]metrics.TimerVec{},
		histograms:   map[metricInstrumentKey]metrics.HistogramVec{},
	}
}

func (c *metricsConfig) TimerVec(name string, labelNames ...string) metrics.TimerVec {
	instrumentName := c.instrumentName(name)
	key := newMetricInstrumentKey(instrumentName, fmt.Sprintf("%v", c.timerBuckets), labelNames)

	c.m.Lock()
	cached, ok := c.timers[key]
	c.m.Unlock()
	if ok {
		return cached
	}

	histogram, err := c.meter.Float64Histogram(
		instrumentName,
		metric.WithDescription("ydb-go-sdk timer"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(c.timerBuckets...),
	)
	if err != nil {
		panic(err)
	}

	c.m.Lock()
	defer c.m.Unlock()
	if cached, ok := c.timers[key]; ok {
		return cached
	}

	t := &timerVec{
		histogram:  histogram,
		labelNames: labelNames,
	}
	c.timers[key] = t

	return t
}

func (c *metricsConfig) HistogramVec(name string, buckets []float64, labelNames ...string) metrics.HistogramVec {
	instrumentName := c.instrumentName(name)

	histogramBuckets := append([]float64(nil), buckets...)
	key := newMetricInstrumentKey(instrumentName, fmt.Sprintf("%v", histogramBuckets), labelNames)

	c.m.Lock()
	cached, ok := c.histograms[key]
	c.m.Unlock()
	if ok {
		return cached
	}

	histogram, err := c.meter.Float64Histogram(
		instrumentName,
		metric.WithDescription("ydb-go-sdk histogram"),
		metric.WithExplicitBucketBoundaries(histogramBuckets...),
	)
	if err != nil {
		panic(err)
	}

	c.m.Lock()
	defer c.m.Unlock()
	if cached, ok := c.histograms[key]; ok {
		return cached
	}

	h := &histogramVec{
		histogram:  histogram,
		labelNames: labelNames,
	}
	c.histograms[key] = h

	return h
}

func (c *metricsConfig) join(a, b string) string {
	if a == "" {
		return b
	}

	if b == "" {
		return a
	}

	return strings.Join([]string{a, b}, c.separator)
}

func (c *metricsConfig) instrumentName(name string) string {
	return c.join(c.namespace, name)
}

type timerVec struct {
	histogram  metric.Float64Histogram
	labelNames []string
}

func (t *timerVec) With(labels map[string]string) metrics.Timer {
	return &timerMetric{
		histogram: t.histogram,
		attrs:     labelsToAttributes(labels, t.labelNames),
	}
}

type timerMetric struct {
	histogram metric.Float64Histogram
	attrs     []attribute.KeyValue
}

func (t *timerMetric) Record(value time.Duration) {
	t.histogram.Record(context.Background(), value.Seconds(), metric.WithAttributes(t.attrs...))
}

type histogramVec struct {
	histogram  metric.Float64Histogram
	labelNames []string
}

func (h *histogramVec) With(labels map[string]string) metrics.Histogram {
	return &histogramMetric{
		histogram: h.histogram,
		attrs:     labelsToAttributes(labels, h.labelNames),
	}
}

type histogramMetric struct {
	histogram metric.Float64Histogram
	attrs     []attribute.KeyValue
}

func (h *histogramMetric) Record(value float64) {
	h.histogram.Record(context.Background(), value, metric.WithAttributes(h.attrs...))
}

func labelsToAttributes(labels map[string]string, labelNames []string) []attribute.KeyValue {
	if len(labelNames) == 0 && len(labels) == 0 {
		return nil
	}

	attrs := make([]attribute.KeyValue, 0, max(len(labelNames), len(labels)))
	if len(labels) == 0 {
		for _, name := range labelNames {
			attrs = append(attrs, attribute.String(name, ""))
		}

		return attrs
	}

	for _, name := range labelNames {
		attrs = append(attrs, attribute.String(name, labels[name]))
	}

	for name, value := range labels {
		if !slices.Contains(labelNames, name) {
			attrs = append(attrs, attribute.String(name, value))
		}
	}

	return attrs
}
