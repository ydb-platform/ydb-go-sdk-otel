package ydb

import (
	"context"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/metrics"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

func (c *metricsConfig) CounterVec(name string, labelNames ...string) metrics.CounterVec {
	return c.counters.get(vectorSpec{name: c.instrumentName(name), labelNames: labelNames}, c.counterBinding)
}

func (c *metricsConfig) CounterVecWithDescriptor(name, unit string, _ ...string) metrics.CounterVec {
	return c.counters.get(vectorSpec{name: name, unit: unit}, c.counterBinding)
}

func (c *metricsConfig) counterBinding(name, unit string) func([]string) metrics.CounterVec {
	counter, err := c.meter.Int64Counter(name,
		metric.WithDescription("ydb-go-sdk counter"),
		metric.WithUnit(unit),
	)
	if err != nil {
		panic(err)
	}

	return func(labelNames []string) metrics.CounterVec {
		return &counterVec{counter: counter, labelNames: labelNames}
	}
}

func (c *metricsConfig) GaugeVec(name string, labelNames ...string) metrics.GaugeVec {
	return c.gauges.get(vectorSpec{name: c.instrumentName(name), labelNames: labelNames}, c.gaugeBinding)
}

func (c *metricsConfig) GaugeVecWithDescriptor(name, unit string, _ ...string) metrics.GaugeVec {
	return c.gauges.get(vectorSpec{name: name, unit: unit}, c.gaugeBinding)
}

func (c *metricsConfig) gaugeBinding(name, unit string) func([]string) metrics.GaugeVec {
	upDown, err := c.meter.Float64UpDownCounter(name,
		metric.WithDescription("ydb-go-sdk gauge"),
		metric.WithUnit(unit),
	)
	if err != nil {
		panic(err)
	}
	series := &gaugeSeries{upDown: upDown, metrics: make(map[attribute.Distinct]*gaugeMetric)}

	return func(labelNames []string) metrics.GaugeVec {
		return &gaugeVec{series: series, labelNames: labelNames}
	}
}

type counterVec struct {
	counter    metric.Int64Counter
	labelNames []string
}

func (c *counterVec) With(labels map[string]string) metrics.Counter {
	attrs := attribute.NewSet(labelsToAttributes(labels, c.labelNames)...)

	return &counterMetric{counter: c.counter, option: metric.WithAttributeSet(attrs)}
}

type counterMetric struct {
	counter metric.Int64Counter
	option  metric.AddOption
}

func (c *counterMetric) Inc() {
	c.Add(1)
}

func (c *counterMetric) Add(value int64) {
	c.counter.Add(context.Background(), value, c.option)
}

type gaugeVec struct {
	series     *gaugeSeries
	labelNames []string
}

func (g *gaugeVec) With(labels map[string]string) metrics.Gauge {
	return g.series.bind(attribute.NewSet(labelsToAttributes(labels, g.labelNames)...))
}

type gaugeSeries struct {
	mu      sync.Mutex
	upDown  metric.Float64UpDownCounter
	metrics map[attribute.Distinct]*gaugeMetric
}

func (s *gaugeSeries) bind(attrs attribute.Set) *gaugeMetric {
	key := attrs.Equivalent()
	s.mu.Lock()
	defer s.mu.Unlock()

	if gauge, ok := s.metrics[key]; ok {
		return gauge
	}
	gauge := &gaugeMetric{upDown: s.upDown, option: metric.WithAttributeSet(attrs)}
	s.metrics[key] = gauge

	return gauge
}

type gaugeMetric struct {
	mu     sync.Mutex
	last   float64
	hasVal bool

	upDown metric.Float64UpDownCounter
	option metric.AddOption
}

func (g *gaugeMetric) Add(delta float64) {
	g.mu.Lock()
	g.last += delta
	g.hasVal = true
	g.mu.Unlock()

	if delta == 0 {
		return
	}

	g.upDown.Add(context.Background(), delta, g.option)
}

func (g *gaugeMetric) Set(value float64) {
	g.mu.Lock()

	delta := value
	if g.hasVal {
		delta = value - g.last
	}

	g.last = value
	g.hasVal = true
	g.mu.Unlock()

	if delta == 0 {
		return
	}

	g.upDown.Add(context.Background(), delta, g.option)
}
