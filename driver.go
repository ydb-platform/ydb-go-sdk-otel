package ydb

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc32"
	"sort"

	"github.com/ydb-platform/ydb-go-sdk-otel/internal/safe"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	otelTrace "go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type metadataCarrier metadata.MD

func (m metadataCarrier) Get(key string) string {
	panic("metadataCarrier is not for getting internal data")
}

func (m metadataCarrier) Set(key string, value string) {
	if values, has := m[key]; has {
		sort.Strings(values)
		if i := sort.SearchStrings(values, value); i == len(values) {
			values = append(values, value)
		}
		m[key] = values
	} else {
		m[key] = []string{value}
	}
}

func (m metadataCarrier) Keys() []string {
	panic("metadataCarrier is not for getting internal data")
}

// driver makes driver with publishing traces
func driver(cfg *config) trace.Driver {
	propagator := propagation.TraceContext{}
	withTraceID := func(ctx context.Context) context.Context {
		md, _ := metadata.FromOutgoingContext(ctx)
		propagator.Inject(ctx, metadataCarrier(md))
		return metadata.NewOutgoingContext(ctx, md)
	}
	return trace.Driver{
		OnRepeaterWakeUp: func(info trace.DriverRepeaterWakeUpStartInfo) func(trace.DriverRepeaterWakeUpDoneInfo) {
			if cfg.detailer.Details()&trace.DriverRepeaterEvents == 0 {
				return nil
			}
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
		},
		OnConnDial: func(info trace.DriverConnDialStartInfo) func(trace.DriverConnDialDoneInfo) {
			if cfg.detailer.Details()&trace.DriverConnEvents == 0 {
				return nil
			}
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
		},
		OnConnInvoke: func(info trace.DriverConnInvokeStartInfo) func(trace.DriverConnInvokeDoneInfo) {
			*info.Context = withTraceID(*info.Context)
			if cfg.detailer.Details()&trace.DriverConnEvents == 0 {
				return nil
			}
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
		},
		OnConnNewStream: func(info trace.DriverConnNewStreamStartInfo) func(trace.DriverConnNewStreamDoneInfo) {
			*info.Context = withTraceID(*info.Context)
			if cfg.detailer.Details()&trace.DriverConnEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
				attribute.String("address", safe.Address(info.Endpoint)),
				attribute.String("method", string(info.Method)),
			)
			return func(info trace.DriverConnNewStreamDoneInfo) {
				finish(start, info.Error,
					attribute.String("state", safe.Stringer(info.State)),
				)
			}
		},
		OnConnStreamRecvMsg: func(info trace.DriverConnStreamRecvMsgStartInfo) func(trace.DriverConnStreamRecvMsgDoneInfo) {
			*info.Context = withTraceID(*info.Context)
			if cfg.detailer.Details()&trace.DriverConnEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
			)
			return func(info trace.DriverConnStreamRecvMsgDoneInfo) {
				finish(start, info.Error)
			}
		},
		OnConnStreamSendMsg: func(info trace.DriverConnStreamSendMsgStartInfo) func(trace.DriverConnStreamSendMsgDoneInfo) {
			*info.Context = withTraceID(*info.Context)
			if cfg.detailer.Details()&trace.DriverConnEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
			)
			return func(info trace.DriverConnStreamSendMsgDoneInfo) {
				finish(start, info.Error)
			}
		},
		OnConnStreamCloseSend: func(info trace.DriverConnStreamCloseSendStartInfo) func(
			trace.DriverConnStreamCloseSendDoneInfo,
		) {
			*info.Context = withTraceID(*info.Context)
			if cfg.detailer.Details()&trace.DriverConnEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
			)
			return func(info trace.DriverConnStreamCloseSendDoneInfo) {
				finish(start, info.Error)
			}
		},
		OnConnPark: func(info trace.DriverConnParkStartInfo) func(trace.DriverConnParkDoneInfo) {
			if cfg.detailer.Details()&trace.DriverConnEvents == 0 {
				return nil
			}
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
		},
		OnConnClose: func(info trace.DriverConnCloseStartInfo) func(trace.DriverConnCloseDoneInfo) {
			if cfg.detailer.Details()&trace.DriverConnEvents == 0 {
				return nil
			}
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
		},
		OnConnBan: func(info trace.DriverConnBanStartInfo) func(trace.DriverConnBanDoneInfo) {
			if cfg.detailer.Details()&trace.DriverConnEvents == 0 {
				return nil
			}
			s := otelTrace.SpanFromContext(*info.Context)
			s.AddEvent(info.Call.FunctionID(),
				otelTrace.WithAttributes(
					attribute.String("cause", safe.Error(info.Cause)),
				),
			)
			return nil
		},
		OnConnStateChange: func(info trace.DriverConnStateChangeStartInfo) func(trace.DriverConnStateChangeDoneInfo) {
			if cfg.detailer.Details()&trace.DriverConnEvents == 0 {
				return nil
			}
			s := otelTrace.SpanFromContext(*info.Context)
			oldState := safe.Stringer(info.State)
			functionID := info.Call.FunctionID()
			return func(info trace.DriverConnStateChangeDoneInfo) {
				s.AddEvent(functionID, otelTrace.WithAttributes(
					attribute.String("old state", oldState),
					attribute.String("new state", safe.Stringer(info.State)),
				))
			}
		},
		OnBalancerInit: func(info trace.DriverBalancerInitStartInfo) func(trace.DriverBalancerInitDoneInfo) {
			if cfg.detailer.Details()&trace.DriverBalancerEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
				attribute.String("name", info.Name),
			)
			return func(info trace.DriverBalancerInitDoneInfo) {
				finish(start, info.Error)
			}
		},
		OnBalancerClusterDiscoveryAttempt: func(info trace.DriverBalancerClusterDiscoveryAttemptStartInfo) func(
			trace.DriverBalancerClusterDiscoveryAttemptDoneInfo,
		) {
			if cfg.detailer.Details()&trace.DriverBalancerEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
			)
			return func(info trace.DriverBalancerClusterDiscoveryAttemptDoneInfo) {
				finish(start, info.Error)
			}
		},
		OnBalancerUpdate: func(info trace.DriverBalancerUpdateStartInfo) func(trace.DriverBalancerUpdateDoneInfo) {
			if cfg.detailer.Details()&trace.DriverBalancerEvents == 0 {
				return nil
			}
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
		},
		OnBalancerChooseEndpoint: func(
			info trace.DriverBalancerChooseEndpointStartInfo,
		) func(
			trace.DriverBalancerChooseEndpointDoneInfo,
		) {
			if cfg.detailer.Details()&trace.DriverBalancerEvents == 0 {
				return nil
			}
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
		},
		OnGetCredentials: func(info trace.DriverGetCredentialsStartInfo) func(trace.DriverGetCredentialsDoneInfo) {
			if cfg.detailer.Details()&trace.DriverCredentialsEvents == 0 {
				return nil
			}
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
		},
		OnInit: func(info trace.DriverInitStartInfo) func(trace.DriverInitDoneInfo) {
			if cfg.detailer.Details()&trace.DriverEvents == 0 {
				return nil
			}
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
		},
		OnClose: func(info trace.DriverCloseStartInfo) func(trace.DriverCloseDoneInfo) {
			if cfg.detailer.Details()&trace.DriverEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
			)
			return func(info trace.DriverCloseDoneInfo) {
				finish(start, info.Error)
			}
		},
		OnPoolNew: func(info trace.DriverConnPoolNewStartInfo) func(trace.DriverConnPoolNewDoneInfo) {
			if cfg.detailer.Details()&trace.DriverEvents == 0 {
				return nil
			}
			start := childSpanWithReplaceCtx(
				cfg.tracer,
				info.Context,
				info.Call.FunctionID(),
			)
			return func(info trace.DriverConnPoolNewDoneInfo) {
				start.End()
			}
		},
	}
}
