package ydb

import (
	"context"
	"errors"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/metrics"
	"go.opentelemetry.io/otel/metric"
)

var (
	errNilObservableGaugeCallback     = errors.New("observable gauge callback is nil")
	errNilObservableGaugeRegistration = errors.New("observable gauge registration is nil")
)

type observableGaugeVec struct {
	meter         metric.Meter
	gauge         metric.Float64ObservableGauge
	instrumentErr error
	labelNames    []string
}

func (g *observableGaugeVec) Register(callback metrics.ObservableGaugeCallback) (func() error, error) {
	if callback == nil {
		return nil, errNilObservableGaugeCallback
	}
	if g.instrumentErr != nil {
		return nil, g.instrumentErr
	}

	var callbackMu sync.RWMutex
	var registeredCallback metrics.ObservableGaugeCallback
	registeredCallback = callback
	otelCallback := func(ctx context.Context, observer metric.Observer) error {
		callbackMu.RLock()
		callback := registeredCallback
		callbackMu.RUnlock()
		if callback == nil {
			return nil
		}

		return callback(ctx, func(value float64, labels map[string]string) {
			attrs := labelsToAttributes(labels, g.labelNames)
			observer.ObserveFloat64(g.gauge, value, metric.WithAttributes(attrs...))
		})
	}

	otelRegistration, err := g.meter.RegisterCallback(otelCallback, g.gauge)
	if err != nil {
		callbackMu.Lock()
		registeredCallback = nil
		callbackMu.Unlock()
		if otelRegistration != nil {
			_ = otelRegistration.Unregister()
		}

		return nil, err
	}
	if otelRegistration == nil {
		callbackMu.Lock()
		registeredCallback = nil
		callbackMu.Unlock()

		return nil, errNilObservableGaugeRegistration
	}

	return func() error {
		callbackMu.Lock()
		registeredCallback = nil
		callbackMu.Unlock()

		return otelRegistration.Unregister()
	}, nil
}
