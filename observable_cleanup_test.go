package ydb

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

func TestObservableGaugeUnregisterDetachesRetainedCallback(t *testing.T) {
	unregisterErr := errors.New("unregister failed")
	registration := &observableCleanupRegistration{err: unregisterErr}
	meter := &observableCleanupMeter{registration: registration}
	vec := newObservableCleanupVec(t, meter)

	var calls atomic.Int32
	unregister, err := vec.Register(func(context.Context, func(float64, map[string]string)) error {
		calls.Add(1)

		return nil
	})
	require.NoError(t, err)
	require.NotNil(t, unregister)
	require.NotNil(t, meter.callback)
	require.NoError(t, meter.callback(context.Background(), noop.Observer{}))
	require.Equal(t, int32(1), calls.Load())

	require.ErrorIs(t, unregister(), unregisterErr)
	require.ErrorIs(t, unregister(), unregisterErr)
	require.NoError(t, meter.callback(context.Background(), noop.Observer{}))
	require.Equal(t, int32(1), calls.Load())
}

func TestObservableGaugeUnregisterConcurrentWithCollection(t *testing.T) {
	meter := &observableCleanupMeter{registration: &observableCleanupRegistration{}}
	vec := newObservableCleanupVec(t, meter)
	var calls atomic.Int32
	unregister, err := vec.Register(func(context.Context, func(float64, map[string]string)) error {
		calls.Add(1)

		return nil
	})
	require.NoError(t, err)

	const workers = 10
	results := make(chan error, 2*workers)
	for range workers {
		go func() { results <- meter.callback(context.Background(), noop.Observer{}) }()
		go func() { results <- unregister() }()
	}
	for range 2 * workers {
		require.NoError(t, <-results)
	}

	completedCalls := calls.Load()
	require.NoError(t, meter.callback(context.Background(), noop.Observer{}))
	require.Equal(t, completedCalls, calls.Load())
}

func TestObservableGaugeRegisterFailureDetachesRetainedCallback(t *testing.T) {
	registerErr := errors.New("register failed")
	cleanupErr := errors.New("cleanup failed")

	tests := []struct {
		name         string
		registration metric.Registration
		registerErr  error
		wantErr      error
	}{
		{
			name:         "registration returned",
			registration: &observableCleanupRegistration{err: cleanupErr},
			registerErr:  registerErr,
			wantErr:      registerErr,
		},
		{
			name:        "registration absent",
			registerErr: registerErr,
			wantErr:     registerErr,
		},
		{
			name:    "registration absent without error",
			wantErr: errNilObservableGaugeRegistration,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meter := &observableCleanupMeter{
				registration: tt.registration,
				registerErr:  tt.registerErr,
			}
			vec := newObservableCleanupVec(t, meter)

			var calls atomic.Int32
			unregister, err := vec.Register(func(context.Context, func(float64, map[string]string)) error {
				calls.Add(1)

				return nil
			})
			require.ErrorIs(t, err, tt.wantErr)
			require.Nil(t, unregister)
			require.NotNil(t, meter.callback)
			require.NoError(t, meter.callback(context.Background(), noop.Observer{}))
			require.Zero(t, calls.Load())

			if registration, ok := tt.registration.(*observableCleanupRegistration); ok {
				require.Equal(t, int32(1), registration.calls.Load())
			}
		})
	}
}

func newObservableCleanupVec(t *testing.T, meter *observableCleanupMeter) *observableGaugeVec {
	t.Helper()

	gauge, err := noop.NewMeterProvider().Meter("test").Float64ObservableGauge("gauge")
	require.NoError(t, err)

	return &observableGaugeVec{meter: meter, gauge: gauge}
}

type observableCleanupMeter struct {
	noop.Meter

	callback     metric.Callback
	registration metric.Registration
	registerErr  error
}

func (m *observableCleanupMeter) RegisterCallback(
	callback metric.Callback,
	_ ...metric.Observable,
) (metric.Registration, error) {
	m.callback = callback

	return m.registration, m.registerErr
}

type observableCleanupRegistration struct {
	noop.Registration

	err   error
	calls atomic.Int32
}

func (r *observableCleanupRegistration) Unregister() error {
	r.calls.Add(1)

	return r.err
}
