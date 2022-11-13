package ydb_otel

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk-opentelemetry/internal/safe"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"go.opentelemetry.io/otel/attribute"
)

// Driver makes Driver with publishing traces
func Driver(details trace.Details) (t trace.Driver) {
	if details&trace.DriverNetEvents != 0 {
		t.OnNetDial = func(info trace.DriverNetDialStartInfo) func(trace.DriverNetDialDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_net_dial",
				attribute.String("address", info.Address),
			)
			return func(info trace.DriverNetDialDoneInfo) {
				finish(start, info.Error)
			}
		}
	}
	if details&trace.DriverRepeaterEvents != 0 {
		t.OnRepeaterWakeUp = func(info trace.DriverRepeaterWakeUpStartInfo) func(trace.DriverRepeaterWakeUpDoneInfo) {
			start := startSpan(
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
	}
	if details&trace.DriverConnEvents != 0 {
		t.OnConnTake = func(info trace.DriverConnTakeStartInfo) func(trace.DriverConnTakeDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_conn_take",
				attribute.String("address", safe.Address(info.Endpoint)),
			)
			return func(info trace.DriverConnTakeDoneInfo) {
				finish(
					start,
					info.Error,
				)
			}
		}
		t.OnConnInvoke = func(info trace.DriverConnInvokeStartInfo) func(trace.DriverConnInvokeDoneInfo) {
			start := startSpan(
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
		t.OnConnNewStream = func(info trace.DriverConnNewStreamStartInfo) func(trace.DriverConnNewStreamRecvInfo) func(trace.DriverConnNewStreamDoneInfo) {
			start := startSpan(
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
		t.OnConnPark = func(info trace.DriverConnParkStartInfo) func(trace.DriverConnParkDoneInfo) {
			start := startSpan(
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
		t.OnConnClose = func(info trace.DriverConnCloseStartInfo) func(trace.DriverConnCloseDoneInfo) {
			start := startSpan(
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
		t.OnConnBan = func(info trace.DriverConnBanStartInfo) func(trace.DriverConnBanDoneInfo) {
			start := startSpan(
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
		t.OnConnAllow = func(info trace.DriverConnAllowStartInfo) func(trace.DriverConnAllowDoneInfo) {
			start := startSpan(
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
	}
	if details&trace.DriverBalancerEvents != 0 {
		t.OnBalancerInit = func(info trace.DriverBalancerInitStartInfo) func(trace.DriverBalancerInitDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_balancer_init",
			)
			return func(info trace.DriverBalancerInitDoneInfo) {
				finish(start, nil)
			}
		}
		t.OnBalancerClose = func(info trace.DriverBalancerCloseStartInfo) func(trace.DriverBalancerCloseDoneInfo) {
			start := startSpan(
				info.Context,
				"ydb_balancer_close",
			)
			return func(info trace.DriverBalancerCloseDoneInfo) {
				finish(start, info.Error)
			}
		}
		t.OnBalancerUpdate = func(info trace.DriverBalancerUpdateStartInfo) func(trace.DriverBalancerUpdateDoneInfo) {
			start := startSpan(
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
				finish(start, info.Error,
					attribute.StringSlice("endpoints", endpoints),
				)
			}
		}
		t.OnBalancerChooseEndpoint = func(info trace.DriverBalancerChooseEndpointStartInfo) func(trace.DriverBalancerChooseEndpointDoneInfo) {
			start := startSpan(
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
	}
	if details&trace.DriverCredentialsEvents != 0 {
		t.OnGetCredentials = func(info trace.DriverGetCredentialsStartInfo) func(trace.DriverGetCredentialsDoneInfo) {
			start := startSpan(
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
	}
	ctx := context.Background()
	connectionsTotal := startSpanWithCounter(&ctx, "ydb_connections", "total")
	return t.Compose(trace.Driver{
		OnInit: func(info trace.DriverInitStartInfo) func(trace.DriverInitDoneInfo) {
			start := startSpan(
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
				info.Context,
				"ydb_driver_close",
			)
			return func(info trace.DriverCloseDoneInfo) {
				finish(start, info.Error)
			}
		},
		OnNetDial: func(info trace.DriverNetDialStartInfo) func(trace.DriverNetDialDoneInfo) {
			return func(info trace.DriverNetDialDoneInfo) {
				if info.Error == nil {
					connectionsTotal.add(1)
				}
			}
		},
		OnNetClose: func(info trace.DriverNetCloseStartInfo) func(trace.DriverNetCloseDoneInfo) {
			connectionsTotal.add(-1)
			return nil
		},
	})
}
