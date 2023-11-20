package ydb

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc32"

	"github.com/ydb-platform/ydb-go-sdk/v3/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	otelTrace "go.opentelemetry.io/otel/trace"

	"github.com/ydb-platform/ydb-go-sdk-otel/internal/safe"
)

// driver makes driver with publishing traces
func driver(cfg *config) (t trace.Driver) {
	withTraceID := func(ctx context.Context) context.Context {
		spanCtx := otelTrace.SpanContextFromContext(ctx)
		if spanCtx.HasTraceID() {
			flags := spanCtx.TraceFlags() & otelTrace.FlagsSampled
			traceParent := fmt.Sprintf("%.2x-%s-%s-%s",
				0,
				spanCtx.TraceID(),
				spanCtx.SpanID(),
				flags,
			)
			return meta.WithTraceID(ctx, traceParent)
		}
		return ctx
	}
	t.OnRepeaterWakeUp = func(info trace.DriverRepeaterWakeUpStartInfo) func(trace.DriverRepeaterWakeUpDoneInfo) {
		if cfg.detailer.Details()&trace.DriverRepeaterEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
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
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
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
		*info.Context = withTraceID(*info.Context)
		if cfg.detailer.Details()&trace.DriverConnEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
				attribute.String("address", safe.Address(info.Endpoint)),
				attribute.String("method", string(info.Method)),
			)
			return func(info trace.DriverConnInvokeDoneInfo) {
				if len(info.Issues) > 0 {
					issues := make([]string, len(info.Issues))
					for i, issue := range info.Issues {
						issues[i] = fmt.Sprintf("%+v", issue)
					}
					start.SetAttributes(
						attribute.StringSlice("issues", issues),
					)
				}
				finish(
					start,
					info.Error,
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
		*info.Context = withTraceID(*info.Context)
		if cfg.detailer.Details()&trace.DriverConnEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
				attribute.String("address", safe.Address(info.Endpoint)),
				attribute.String("method", string(info.Method)),
			)
			return func(info trace.DriverConnNewStreamRecvInfo) func(trace.DriverConnNewStreamDoneInfo) {
				if info.Error != nil {
					start.RecordError(info.Error)
				}
				return func(info trace.DriverConnNewStreamDoneInfo) {
					finish(start, info.Error,
						attribute.String("state", safe.Stringer(info.State)),
					)
				}
			}
		}
		return nil
	}
	t.OnConnPark = func(info trace.DriverConnParkStartInfo) func(trace.DriverConnParkDoneInfo) {
		if cfg.detailer.Details()&trace.DriverConnEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
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
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
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
			s := otelTrace.SpanFromContext(*info.Context)
			s.AddEvent(info.Call.FunctionID(),
				otelTrace.WithAttributes(
					attribute.String("cause", safe.Error(info.Cause)),
				),
			)
			return nil
		}
		return nil
	}
	t.OnConnStateChange = func(info trace.DriverConnStateChangeStartInfo) func(trace.DriverConnStateChangeDoneInfo) {
		if cfg.detailer.Details()&trace.DriverConnEvents != 0 {
			s := otelTrace.SpanFromContext(*info.Context)
			oldState := safe.Stringer(info.State)
			functionID := info.Call.FunctionID()
			return func(info trace.DriverConnStateChangeDoneInfo) {
				s.AddEvent(functionID, otelTrace.WithAttributes(
					attribute.String("old state", oldState),
					attribute.String("new state", safe.Stringer(info.State)),
				))
			}
		}
		return nil
	}
	t.OnConnAllow = func(info trace.DriverConnAllowStartInfo) func(trace.DriverConnAllowDoneInfo) {
		if cfg.detailer.Details()&trace.DriverConnEvents != 0 {
			s := otelTrace.SpanFromContext(*info.Context)
			s.AddEvent(info.Call.FunctionID())
			return nil
		}
		return nil
	}
	t.OnBalancerInit = func(info trace.DriverBalancerInitStartInfo) func(trace.DriverBalancerInitDoneInfo) {
		if cfg.detailer.Details()&trace.DriverBalancerEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
				attribute.String("name", info.Name),
			)
			return func(info trace.DriverBalancerInitDoneInfo) {
				finish(start, info.Error)
			}
		}
		return nil
	}
	t.OnBalancerClusterDiscoveryAttempt = func(info trace.DriverBalancerClusterDiscoveryAttemptStartInfo) func(
		trace.DriverBalancerClusterDiscoveryAttemptDoneInfo,
	) {
		if cfg.detailer.Details()&trace.DriverBalancerEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
			)
			return func(info trace.DriverBalancerClusterDiscoveryAttemptDoneInfo) {
				finish(start, info.Error)
			}
		}
		return nil
	}
	t.OnBalancerUpdate = func(info trace.DriverBalancerUpdateStartInfo) func(trace.DriverBalancerUpdateDoneInfo) {
		if cfg.detailer.Details()&trace.DriverBalancerEvents != 0 {
			s := otelTrace.SpanFromContext(*info.Context)
			s.SetAttributes(
				attribute.Bool("need_local_dc", info.NeedLocalDC),
			)
			return func(info trace.DriverBalancerUpdateDoneInfo) {
				var (
					endpoints = make([]string, len(info.Endpoints))
					added     = make([]string, len(info.Added))
					dropped   = make([]string, len(info.Dropped))
				)
				for i, e := range info.Endpoints {
					endpoints[i] = e.String()
				}
				for i, e := range info.Added {
					added[i] = e.String()
				}
				for i, e := range info.Dropped {
					dropped[i] = e.String()
				}
				s.SetAttributes(
					attribute.String("local_dc", info.LocalDC),
					attribute.StringSlice("endpoints", endpoints),
					attribute.StringSlice("added", added),
					attribute.StringSlice("dropped", dropped),
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
			parent := otelTrace.SpanFromContext(*info.Context)
			functionID := info.Call.FunctionID()
			return func(info trace.DriverBalancerChooseEndpointDoneInfo) {
				if info.Error != nil {
					parent.AddEvent(functionID)
					parent.RecordError(info.Error, otelTrace.WithAttributes(attribute.Bool(errorAttribute, true)))
					parent.SetStatus(codes.Error, info.Error.Error())
				} else {
					parent.AddEvent(functionID,
						otelTrace.WithAttributes(
							attribute.String("address", safe.Address(info.Endpoint)),
							attribute.String("nodeID", safe.NodeID(info.Endpoint)),
						),
					)
				}
			}
		}
		return nil
	}
	t.OnGetCredentials = func(info trace.DriverGetCredentialsStartInfo) func(trace.DriverGetCredentialsDoneInfo) {
		if cfg.detailer.Details()&trace.DriverCredentialsEvents != 0 { //nolint:nestif
			parent := otelTrace.SpanFromContext(*info.Context)
			functionID := info.Call.FunctionID()
			return func(info trace.DriverGetCredentialsDoneInfo) {
				if info.Error != nil {
					parent.AddEvent(functionID)
					parent.RecordError(info.Error, otelTrace.WithAttributes(attribute.Bool(errorAttribute, true)))
					parent.SetStatus(codes.Error, info.Error.Error())
				} else {
					var mask bytes.Buffer
					if len(info.Token) > 16 {
						mask.WriteString(info.Token[:4])
						mask.WriteString("****")
						mask.WriteString(info.Token[len(info.Token)-4:])
					} else {
						mask.WriteString("****")
					}
					mask.WriteString(fmt.Sprintf("(CRC-32c: %08X)", crc32.Checksum([]byte(info.Token), crc32.IEEETable)))
					parent.AddEvent(functionID,
						otelTrace.WithAttributes(
							attribute.String("token", mask.String()),
						),
					)
				}
			}
		}
		return nil
	}
	t.OnInit = func(info trace.DriverInitStartInfo) func(trace.DriverInitDoneInfo) {
		if cfg.detailer.Details()&trace.DriverEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
				attribute.String("endpoint", info.Endpoint),
				attribute.String("database", info.Database),
				attribute.Bool("secure", info.Secure),
			)
			return func(info trace.DriverInitDoneInfo) {
				finish(start, info.Error)
			}
		}
		return nil
	}
	t.OnClose = func(info trace.DriverCloseStartInfo) func(trace.DriverCloseDoneInfo) {
		if cfg.detailer.Details()&trace.DriverEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
			)
			return func(info trace.DriverCloseDoneInfo) {
				finish(start, info.Error)
			}
		}
		return nil
	}
	t.OnPoolNew = func(info trace.DriverConnPoolNewStartInfo) func(trace.DriverConnPoolNewDoneInfo) {
		if cfg.detailer.Details()&trace.DriverEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
			)
			return func(info trace.DriverConnPoolNewDoneInfo) {
				start.End()
			}
		}
		return nil
	}
	t.OnPoolRelease = func(info trace.DriverConnPoolReleaseStartInfo) func(trace.DriverConnPoolReleaseDoneInfo) {
		start := childSpanWithReplaceCtx(
			cfg.tracer,
			info.Context,
			info.Call.FunctionID(),
		)
		return func(info trace.DriverConnPoolReleaseDoneInfo) {
			finish(start, info.Error)
		}
	}
	return t
}
