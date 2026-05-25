package ydb

import (
	"go.opentelemetry.io/otel"
	otelLog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/metric"
	otelTrace "go.opentelemetry.io/otel/trace"
)

// instrumentationName is the default OpenTelemetry instrumentation scope.
const instrumentationName = "ydb-go-sdk"

func tracerFrom(tracer otelTrace.Tracer) otelTrace.Tracer {
	if tracer != nil {
		return tracer
	}

	return otel.Tracer(instrumentationName)
}

func meterFrom(meter metric.Meter) metric.Meter {
	if meter != nil {
		return meter
	}

	return otel.Meter(instrumentationName)
}

func loggerFrom(logger otelLog.Logger) otelLog.Logger {
	if logger != nil {
		return logger
	}

	return global.Logger(instrumentationName)
}
