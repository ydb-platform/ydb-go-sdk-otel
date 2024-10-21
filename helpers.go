package ydb

import (
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	
	"github.com/ydb-platform/ydb-go-sdk/v3/spans"
)

func fieldToAttribute(field spans.KeyValue) attribute.KeyValue {
	switch field.Type() {
	case spans.IntType:
		return attribute.Int(field.Key(), field.IntValue())
	case spans.Int64Type:
		return attribute.Int64(field.Key(), field.Int64Value())
	case spans.StringType:
		return attribute.String(field.Key(), field.StringValue())
	case spans.BoolType:
		return attribute.Bool(field.Key(), field.BoolValue())
	case spans.StringsType:
		return attribute.StringSlice(field.Key(), field.StringsValue())
	case spans.StringerType:
		return attribute.Stringer(field.Key(), field.Stringer())
	default:
		return attribute.String(field.Key(), fmt.Sprintf("%v", field.AnyValue()))
	}
}

func fieldsToAttributes(fields []spans.KeyValue) []attribute.KeyValue {
	attributes := make([]attribute.KeyValue, 0, len(fields))
	for _, kv := range fields {
		attributes = append(attributes, fieldToAttribute(kv))
	}

	return attributes
}
