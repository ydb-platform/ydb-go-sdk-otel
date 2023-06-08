package ydb

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"go.opentelemetry.io/otel/attribute"

	"github.com/ydb-platform/ydb-go-sdk-otel/internal/safe"
)

// Driver makes Driver with publishing traces
func Driver(cfg *config) (t trace.Driver) {
	t.OnRepeaterWakeUp = func(info trace.DriverRepeaterWakeUpStartInfo) func(trace.DriverRepeaterWakeUpDoneInfo) {
		if cfg.detailer.Details()&trace.DriverRepeaterEvents != 0 {
			start := startSpan(
				cfg.tracer,
				info.Context,
				"ydb_repeater_wake_up",
				attribute.String("name", info.Name),
				attribute.String("event", info.Event),
			)
			return func(info trace.DriverRepeaterWakeUpDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}
		return nil
	}
	t.OnConnDial = func(info trace.DriverConnDialStartInfo) func(trace.DriverConnDialDoneInfo) {
		if cfg.detailer.Details()&trace.DriverConnEvents != 0 {
			start := startSpan(
				cfg.tracer,
				info.Context,
				"ydb_conn_take",
				attribute.String("address", safe.Address(info.Endpoint)),
			)
			return func(info trace.DriverConnDialDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}
		return nil
	}
	t.OnConnInvoke = func(info trace.DriverConnInvokeStartInfo) func(trace.DriverConnInvokeDoneInfo) {
		if cfg.detailer.Details()&trace.DriverConnEvents != 0 {
			start := startSpan(
				cfg.tracer,
				info.Context,
				"ydb_conn_invoke",
				attribute.String("address", safe.Address(info.Endpoint)),
				attribute.String("method", string(info.Method)),
			)
			return func(info trace.DriverConnInvokeDoneInfo) {
				issues := make([]string, len(info.Issues))
				for i, issue := range info.Issues {
					issues[i] = fmt.Sprintf("%+v", issue)
				}
				finish(
					start,
					info.Error,
					attribute.StringSlice("issues", issues),
					attribute.String("opID", info.OpID),
					attribute.String("state", safe.Stringer(info.State)),
				)
			}
		}
		return nil
	}
	t.OnConnNewStream = func(
		info trace.DriverConnNewStreamStartInfo,
	) func(
		trace.DriverConnNewStreamRecvInfo,
	) func(
		trace.DriverConnNewStreamDoneInfo,
	) {
		if cfg.detailer.Details()&trace.DriverConnEvents != 0 {
			start := startSpan(
				cfg.tracer,
				info.Context,
				"ydb_conn_new_stream",
				attribute.String("address", safe.Address(info.Endpoint)),
				attribute.String("method", string(info.Method)),
			)
			return func(info trace.DriverConnNewStreamRecvInfo) func(trace.DriverConnNewStreamDoneInfo) {
				intermediate(start, info.Error)
				return func(info trace.DriverConnNewStreamDoneInfo) {
					finish(
						start,
						info.Error,
						attribute.String("state", safe.Stringer(info.State)),
					)
				}
			}
		}
		return nil
	}
	t.OnConnPark = func(info trace.DriverConnParkStartInfo) func(trace.DriverConnParkDoneInfo) {
		if cfg.detailer.Details()&trace.DriverConnEvents != 0 {
			start := startSpan(
				cfg.tracer,
				info.Context,
				"ydb_conn_park",
				attribute.String("address", safe.Address(info.Endpoint)),
			)
			return func(info trace.DriverConnParkDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}
		return nil
	}
	t.OnConnClose = func(info trace.DriverConnCloseStartInfo) func(trace.DriverConnCloseDoneInfo) {
		if cfg.detailer.Details()&trace.DriverConnEvents != 0 {
			start := startSpan(
				cfg.tracer,
				info.Context,
				"ydb_conn_close",
				attribute.String("address", safe.Address(info.Endpoint)),
			)
			return func(info trace.DriverConnCloseDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}
		return nil
	}
	t.OnConnBan = func(info trace.DriverConnBanStartInfo) func(trace.DriverConnBanDoneInfo) {
		if cfg.detailer.Details()&trace.DriverConnEvents != 0 {
			start := startSpan(
				cfg.tracer,
				info.Context,
				"ydb_conn_ban",
				attribute.String("state", safe.Stringer(info.State)),
				attribute.String("cause", safe.Error(info.Cause)),
				attribute.String("address", safe.Address(info.Endpoint)),
				attribute.String("nodeID", safe.NodeID(info.Endpoint)),
			)
			return func(info trace.DriverConnBanDoneInfo) {
				finish(
					start,
					nil,
					attribute.String("state", safe.Stringer(info.State)),
				)
			}
		}
		return nil
	}
	t.OnConnAllow = func(info trace.DriverConnAllowStartInfo) func(trace.DriverConnAllowDoneInfo) {
		if cfg.detailer.Details()&trace.DriverConnEvents != 0 {
			start := startSpan(
				cfg.tracer,
				info.Context,
				"ydb_conn_allow",
				attribute.String("state", safe.Stringer(info.State)),
				attribute.String("address", safe.Address(info.Endpoint)),
				attribute.String("nodeID", safe.NodeID(info.Endpoint)),
			)
			return func(info trace.DriverConnAllowDoneInfo) {
				finish(
					start,
					nil,
					attribute.String("state", safe.Stringer(info.State)),
				)
			}
		}
		return nil
	}
	t.OnBalancerInit = func(info trace.DriverBalancerInitStartInfo) func(trace.DriverBalancerInitDoneInfo) {
		if cfg.detailer.Details()&trace.DriverBalancerEvents != 0 {
			start := startSpan(
				cfg.tracer,
				info.Context,
				"ydb_balancer_init",
			)
			return func(info trace.DriverBalancerInitDoneInfo) {
				finish(start, nil)
			}
		}
		return nil
	}
	t.OnBalancerClose = func(info trace.DriverBalancerCloseStartInfo) func(trace.DriverBalancerCloseDoneInfo) {
		if cfg.detailer.Details()&trace.DriverBalancerEvents != 0 {
			start := startSpan(
				cfg.tracer,
				info.Context,
				"ydb_balancer_close",
			)
			return func(info trace.DriverBalancerCloseDoneInfo) {
				finish(start, info.Error)
			}
		}
		return nil
	}
	t.OnBalancerUpdate = func(info trace.DriverBalancerUpdateStartInfo) func(trace.DriverBalancerUpdateDoneInfo) {
		if cfg.detailer.Details()&trace.DriverBalancerEvents != 0 {
			start := startSpan(
				cfg.tracer,
				info.Context,
				"ydb_balancer_update",
				attribute.Bool("need_local_dc", info.NeedLocalDC),
			)
			return func(info trace.DriverBalancerUpdateDoneInfo) {
				start.SetAttributes(attribute.String("local_dc", info.LocalDC))
				endpoints := make([]string, len(info.Endpoints))
				for i, e := range info.Endpoints {
					endpoints[i] = e.String()
				}
				finish(start, nil,
					attribute.StringSlice("endpoints", endpoints),
				)
			}
		}
		return nil
	}
	t.OnBalancerChooseEndpoint = func(
		info trace.DriverBalancerChooseEndpointStartInfo,
	) func(
		trace.DriverBalancerChooseEndpointDoneInfo,
	) {
		if cfg.detailer.Details()&trace.DriverBalancerEvents != 0 {
			start := startSpan(
				cfg.tracer,
				info.Context,
				"ydb_balancer_get",
			)
			return func(info trace.DriverBalancerChooseEndpointDoneInfo) {
				if info.Error == nil {
					start.SetAttributes(
						attribute.String("address", safe.Address(info.Endpoint)),
						attribute.String("nodeID", safe.NodeID(info.Endpoint)),
					)
				}
				finish(start, info.Error)
			}
		}
		return nil
	}
	t.OnGetCredentials = func(info trace.DriverGetCredentialsStartInfo) func(trace.DriverGetCredentialsDoneInfo) {
		if cfg.detailer.Details()&trace.DriverCredentialsEvents != 0 {
			start := startSpan(
				cfg.tracer,
				info.Context,
				"ydb_credentials_get",
			)
			return func(info trace.DriverGetCredentialsDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}
		return nil
	}
	ctx := context.Background()
	connectionsTotal := startSpanWithCounter(cfg.tracer, &ctx, "ydb_connections", "total")
	return *t.Compose(&trace.Driver{
		OnInit: func(info trace.DriverInitStartInfo) func(trace.DriverInitDoneInfo) {
			start := startSpan(
				cfg.tracer,
				info.Context,
				"ydb_driver_init",
				attribute.String("endpoint", info.Endpoint),
				attribute.String("database", info.Database),
				attribute.Bool("secure", info.Secure),
			)
			return func(info trace.DriverInitDoneInfo) {
				finish(start, info.Error)
			}
		},
		OnClose: func(info trace.DriverCloseStartInfo) func(trace.DriverCloseDoneInfo) {
			connectionsTotal.span.End()
			start := startSpan(
				cfg.tracer,
				info.Context,
				"ydb_driver_close",
			)
			return func(info trace.DriverCloseDoneInfo) {
				finish(start, info.Error)
			}
		},
	})
}
