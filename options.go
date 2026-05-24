package ydb

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// TracesOption configures OpenTelemetry spans adapter.
type TracesOption interface {
	applyTracesOption(*adapter)
}

// MetricsOption configures OpenTelemetry metrics adapter.
type MetricsOption interface {
	applyMetricsOption(*metricsConfig)
}

// LoggerOption configures OpenTelemetry logging adapter.
type LoggerOption interface {
	applyLoggerOption(*loggerConfig)
}

// Option configures spans, metrics and logs adapters.
type Option interface {
	TracesOption
	MetricsOption
	LoggerOption
}

type detailerOption struct {
	detailer trace.Detailer
}

func (o detailerOption) applyTracesOption(c *adapter) {
	c.detailer = o.detailer
}

func (o detailerOption) applyMetricsOption(c *metricsConfig) {
	c.detailer = o.detailer
}

func (o detailerOption) applyLoggerOption(c *loggerConfig) {
	c.detailer = o.detailer
}

// WithDetailer sets detailer for spans, metrics and logs adapters.
func WithDetailer(detailer trace.Detailer) Option {
	return detailerOption{detailer: detailer}
}

type namespaceOption struct {
	namespace string
}

func (o namespaceOption) applyMetricsOption(c *metricsConfig) {
	c.namespace = o.namespace
}

// WithNamespace sets metrics namespace prefix.
func WithNamespace(namespace string) MetricsOption {
	return namespaceOption{namespace: namespace}
}

type separatorOption struct {
	separator string
}

func (o separatorOption) applyMetricsOption(c *metricsConfig) {
	c.separator = o.separator
}

// WithSeparator sets separator for metrics namespace scopes.
func WithSeparator(separator string) MetricsOption {
	return separatorOption{separator: separator}
}

type timerBucketsOption struct {
	timerBuckets []float64
}

func (o timerBucketsOption) applyMetricsOption(c *metricsConfig) {
	c.timerBuckets = o.timerBuckets
}

// WithTimerBuckets sets histogram buckets for timer metrics.
func WithTimerBuckets(timerBuckets []float64) MetricsOption {
	return timerBucketsOption{timerBuckets: timerBuckets}
}

type logQueryOption struct{}

func (logQueryOption) applyLoggerOption(c *loggerConfig) {
	c.logOpts = append(c.logOpts, log.WithLogQuery())
}

// WithLogQuery enables logging of queries.
func WithLogQuery() LoggerOption {
	return logQueryOption{}
}
