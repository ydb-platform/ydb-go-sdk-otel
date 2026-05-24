package ydb

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	otelLog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/global"
)

const loggerID = "ydb-go-sdk"

var _ log.Logger = (*logAdapter)(nil)

type logAdapter struct {
	logger otelLog.Logger
}

type loggerConfig struct {
	logger   otelLog.Logger
	detailer trace.Detailer
	logOpts  []log.Option
}

// LoggerOption configures OpenTelemetry logging for ydb-go-sdk.
type LoggerOption func(*loggerConfig)

func WithLogLogger(logger otelLog.Logger) LoggerOption {
	return func(c *loggerConfig) {
		c.logger = logger
	}
}

func WithLogDetails(details trace.Details) LoggerOption {
	return func(c *loggerConfig) {
		c.detailer = details
	}
}

func WithLogDetailer(detailer trace.Detailer) LoggerOption {
	return func(c *loggerConfig) {
		c.detailer = detailer
	}
}

func WithLogQuery() LoggerOption {
	return func(c *loggerConfig) {
		c.logOpts = append(c.logOpts, log.WithLogQuery())
	}
}

func loggerConfigFrom(opts ...LoggerOption) *loggerConfig {
	cfg := &loggerConfig{
		detailer: trace.DetailsAll,
	}
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.logger == nil {
		cfg.logger = global.Logger(loggerID)
	}

	return cfg
}

// WithLogger sets ydb-go-sdk logger that emits records to OpenTelemetry.
func WithLogger(opts ...LoggerOption) ydb.Option {
	cfg := loggerConfigFrom(opts...)

	return ydb.WithLogger(&logAdapter{logger: cfg.logger}, cfg.detailer, cfg.logOpts...)
}

// WithLogTraces enables ydb-go-sdk trace event logging via OpenTelemetry.
func WithLogTraces(opts ...LoggerOption) ydb.Option {
	cfg := loggerConfigFrom(opts...)
	a := &logAdapter{logger: cfg.logger}

	return ydb.MergeOptions(
		ydb.WithTraceDriver(log.Driver(a, cfg.detailer, cfg.logOpts...)),
		ydb.WithTraceTable(log.Table(a, cfg.detailer, cfg.logOpts...)),
		ydb.WithTraceQuery(log.Query(a, cfg.detailer, cfg.logOpts...)),
		ydb.WithTraceScripting(log.Scripting(a, cfg.detailer, cfg.logOpts...)),
		ydb.WithTraceScheme(log.Scheme(a, cfg.detailer, cfg.logOpts...)),
		ydb.WithTraceCoordination(log.Coordination(a, cfg.detailer, cfg.logOpts...)),
		ydb.WithTraceRatelimiter(log.Ratelimiter(a, cfg.detailer, cfg.logOpts...)),
		ydb.WithTraceDiscovery(log.Discovery(a, cfg.detailer, cfg.logOpts...)),
		ydb.WithTraceTopic(log.Topic(a, cfg.detailer, cfg.logOpts...)),
		ydb.WithTraceDatabaseSQL(log.DatabaseSQL(a, cfg.detailer, cfg.logOpts...)),
		ydb.WithTraceRetry(log.Retry(a, cfg.detailer, cfg.logOpts...)),
	)
}

func (a *logAdapter) Log(ctx context.Context, msg string, fields ...log.Field) {
	severity, severityText := severityFromLevel(log.LevelFromContext(ctx))

	record := otelLog.Record{}
	now := time.Now()
	record.SetTimestamp(now)
	record.SetObservedTimestamp(now)
	record.SetSeverity(severity)
	record.SetSeverityText(severityText)
	record.SetBody(otelLog.StringValue(msg))

	attrs := make([]otelLog.KeyValue, 0, len(fields)+1)
	if scope := strings.Join(log.NamesFromContext(ctx), "."); scope != "" {
		attrs = append(attrs, otelLog.String("scope", scope))
	}
	attrs = append(attrs, fieldsToLogAttributes(append(log.FieldsFromContext(ctx), fields...))...)
	record.AddAttributes(attrs...)

	a.logger.Emit(ctx, record)
}

func severityFromLevel(level log.Level) (otelLog.Severity, string) {
	switch level {
	case log.TRACE:
		return otelLog.SeverityTrace, "TRACE"
	case log.DEBUG:
		return otelLog.SeverityDebug, "DEBUG"
	case log.INFO:
		return otelLog.SeverityInfo, "INFO"
	case log.WARN:
		return otelLog.SeverityWarn, "WARN"
	case log.ERROR:
		return otelLog.SeverityError, "ERROR"
	case log.FATAL:
		return otelLog.SeverityFatal, "FATAL"
	default:
		return otelLog.SeverityUndefined, ""
	}
}

func fieldsToLogAttributes(fields []log.Field) []otelLog.KeyValue {
	attrs := make([]otelLog.KeyValue, len(fields))
	for i, field := range fields {
		attrs[i] = fieldToLogAttribute(field)
	}

	return attrs
}

func fieldToLogAttribute(field log.Field) otelLog.KeyValue {
	switch field.Type() {
	case log.IntType:
		return otelLog.Int(field.Key(), field.IntValue())
	case log.Int64Type:
		return otelLog.Int64(field.Key(), field.Int64Value())
	case log.StringType:
		return otelLog.String(field.Key(), field.StringValue())
	case log.BoolType:
		return otelLog.Bool(field.Key(), field.BoolValue())
	case log.DurationType:
		return otelLog.Int64(field.Key(), field.DurationValue().Nanoseconds())
	case log.StringsType:
		return otelLog.Slice(field.Key(), stringSliceValues(field.StringsValue())...)
	case log.ErrorType:
		err := field.ErrorValue()
		if err == nil {
			return otelLog.String(field.Key(), "")
		}

		return otelLog.String(field.Key(), err.Error())
	case log.StringerType:
		return otelLog.String(field.Key(), field.Stringer().String())
	default:
		return otelLog.String(field.Key(), fmt.Sprintf("%v", field.AnyValue()))
	}
}

func stringSliceValues(values []string) []otelLog.Value {
	result := make([]otelLog.Value, len(values))
	for i, value := range values {
		result[i] = otelLog.StringValue(value)
	}

	return result
}
