package agenticexporter

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"math"
	"strconv"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

var errInvalidEnvelope = errors.New(string(rejectInvalidEnvelope))

type projectionInput struct {
	candidate         candidateType
	kind              recordKind
	context           spanContext
	resource          pcommon.Resource
	resourceSchemaURL string
	scope             pcommon.InstrumentationScope
	scopeSchemaURL    string
	span              ptrace.Span
	event             *ptrace.SpanEvent
	eventIndex        *int
}

type candidateRecord struct {
	SchemaVersion string            `json:"schema_version"`
	CandidateType candidateType     `json:"candidate_type"`
	RecordKind    recordKind        `json:"record_kind"`
	Timestamp     string            `json:"timestamp"`
	AgenticRunUID string            `json:"agenticrun_uid"`
	Phase         string            `json:"phase"`
	TraceID       string            `json:"trace_id"`
	SpanID        string            `json:"span_id"`
	ParentSpanID  *string           `json:"parent_span_id"`
	EventIndex    *int              `json:"event_index"`
	ServiceName   string            `json:"service_name"`
	Name          string            `json:"name"`
	OTel          otelEnvelope      `json:"otel"`
	Attributes    attributeEnvelope `json:"attributes"`
}

type otelEnvelope struct {
	Resource resourceMetadata `json:"resource"`
	Scope    scopeMetadata    `json:"scope"`
	Span     spanMetadata     `json:"span"`
	Event    *eventMetadata   `json:"event,omitempty"`
}

type resourceMetadata struct {
	SchemaURL              string `json:"schemaUrl"`
	DroppedAttributesCount uint32 `json:"droppedAttributesCount"`
}

type scopeMetadata struct {
	Name                   string `json:"name"`
	Version                string `json:"version"`
	SchemaURL              string `json:"schemaUrl"`
	DroppedAttributesCount uint32 `json:"droppedAttributesCount"`
}

type spanMetadata struct {
	StartTimeUnixNano      string         `json:"startTimeUnixNano"`
	EndTimeUnixNano        string         `json:"endTimeUnixNano"`
	Kind                   int32          `json:"kind"`
	Status                 statusMetadata `json:"status"`
	TraceState             string         `json:"traceState"`
	Flags                  uint32         `json:"flags"`
	Links                  []linkMetadata `json:"links"`
	DroppedAttributesCount uint32         `json:"droppedAttributesCount"`
	DroppedEventsCount     uint32         `json:"droppedEventsCount"`
	DroppedLinksCount      uint32         `json:"droppedLinksCount"`
}

type statusMetadata struct {
	Code    int32  `json:"code"`
	Message string `json:"message"`
}

type linkMetadata struct {
	TraceID                string     `json:"traceId"`
	SpanID                 string     `json:"spanId"`
	TraceState             string     `json:"traceState"`
	Flags                  uint32     `json:"flags"`
	Attributes             []keyValue `json:"attributes"`
	DroppedAttributesCount uint32     `json:"droppedAttributesCount"`
}

type eventMetadata struct {
	DroppedAttributesCount uint32 `json:"droppedAttributesCount"`
}

type attributeEnvelope struct {
	Resource map[string]anyValue  `json:"resource"`
	Scope    map[string]anyValue  `json:"scope"`
	Span     map[string]anyValue  `json:"span"`
	Event    *map[string]anyValue `json:"event,omitempty"`
}

type anyValue struct {
	StringValue *string      `json:"stringValue,omitempty"`
	BoolValue   *bool        `json:"boolValue,omitempty"`
	IntValue    *string      `json:"intValue,omitempty"`
	DoubleValue any          `json:"doubleValue,omitempty"`
	BytesValue  *string      `json:"bytesValue,omitempty"`
	ArrayValue  *arrayValue  `json:"arrayValue,omitempty"`
	KVListValue *kvListValue `json:"kvlistValue,omitempty"`
}

type arrayValue struct {
	Values []anyValue `json:"values"`
}

type kvListValue struct {
	Values []keyValue `json:"values"`
}

type keyValue struct {
	Key   string   `json:"key"`
	Value anyValue `json:"value"`
}

func project(input projectionInput) (result []byte, err error) {
	defer func() {
		if recover() != nil {
			result = nil
			err = errInvalidEnvelope
		}
	}()

	if !validProjectionInput(input) {
		return nil, errInvalidEnvelope
	}

	resourceAttributes, err := convertMap(input.resource.Attributes())
	if err != nil {
		return nil, errInvalidEnvelope
	}
	scopeAttributes, err := convertMap(input.scope.Attributes())
	if err != nil {
		return nil, errInvalidEnvelope
	}
	spanAttributes, err := convertMap(input.span.Attributes())
	if err != nil {
		return nil, errInvalidEnvelope
	}
	links, err := convertLinks(input.span.Links())
	if err != nil {
		return nil, errInvalidEnvelope
	}

	var parentSpanID *string
	if parent := input.span.ParentSpanID(); !parent.IsEmpty() {
		encoded := parent.String()
		parentSpanID = &encoded
	}

	record := candidateRecord{
		SchemaVersion: "1.0",
		CandidateType: input.candidate,
		RecordKind:    input.kind,
		Timestamp:     strconv.FormatUint(uint64(input.span.StartTimestamp()), 10),
		AgenticRunUID: input.context.agenticRunUID,
		Phase:         input.context.phase,
		TraceID:       input.span.TraceID().String(),
		SpanID:        input.span.SpanID().String(),
		ParentSpanID:  parentSpanID,
		EventIndex:    nil,
		ServiceName:   input.context.serviceName,
		Name:          input.span.Name(),
		OTel: otelEnvelope{
			Resource: resourceMetadata{
				SchemaURL:              input.resourceSchemaURL,
				DroppedAttributesCount: input.resource.DroppedAttributesCount(),
			},
			Scope: scopeMetadata{
				Name:                   input.scope.Name(),
				Version:                input.scope.Version(),
				SchemaURL:              input.scopeSchemaURL,
				DroppedAttributesCount: input.scope.DroppedAttributesCount(),
			},
			Span: spanMetadata{
				StartTimeUnixNano:      strconv.FormatUint(uint64(input.span.StartTimestamp()), 10),
				EndTimeUnixNano:        strconv.FormatUint(uint64(input.span.EndTimestamp()), 10),
				Kind:                   int32(input.span.Kind()),
				Status:                 statusMetadata{Code: int32(input.span.Status().Code()), Message: input.span.Status().Message()},
				TraceState:             input.span.TraceState().AsRaw(),
				Flags:                  uint32(input.span.Flags()),
				Links:                  links,
				DroppedAttributesCount: input.span.DroppedAttributesCount(),
				DroppedEventsCount:     input.span.DroppedEventsCount(),
				DroppedLinksCount:      input.span.DroppedLinksCount(),
			},
		},
		Attributes: attributeEnvelope{
			Resource: resourceAttributes,
			Scope:    scopeAttributes,
			Span:     spanAttributes,
		},
	}

	if input.kind == recordSpanEvent {
		eventAttributes, conversionErr := convertMap(input.event.Attributes())
		if conversionErr != nil {
			return nil, errInvalidEnvelope
		}
		record.Timestamp = strconv.FormatUint(uint64(input.event.Timestamp()), 10)
		record.EventIndex = input.eventIndex
		record.Name = input.event.Name()
		record.OTel.Event = &eventMetadata{DroppedAttributesCount: input.event.DroppedAttributesCount()}
		record.Attributes.Event = &eventAttributes
	}

	encoded, err := json.Marshal(record)
	if err != nil {
		return nil, errInvalidEnvelope
	}
	result = make([]byte, len(encoded)+1)
	copy(result, encoded)
	result[len(encoded)] = '\n'
	return result, nil
}

func validProjectionInput(input projectionInput) bool {
	if input.candidate != candidateAction && input.candidate != candidateTranscript {
		return false
	}
	if input.kind != recordSpan && input.kind != recordSpanEvent {
		return false
	}
	if input.context.serviceName == "" || input.context.agenticRunUID == "" || input.context.phase == "" {
		return false
	}
	if input.span.TraceID().IsEmpty() || input.span.SpanID().IsEmpty() {
		return false
	}
	if input.kind == recordSpan {
		return input.event == nil && input.eventIndex == nil
	}
	return input.event != nil && input.eventIndex != nil && *input.eventIndex >= 0
}

func convertLinks(links ptrace.SpanLinkSlice) ([]linkMetadata, error) {
	converted := make([]linkMetadata, links.Len())
	for i := range links.Len() {
		link := links.At(i)
		attributes, err := convertKeyValues(link.Attributes())
		if err != nil {
			return nil, err
		}
		converted[i] = linkMetadata{
			TraceID:                link.TraceID().String(),
			SpanID:                 link.SpanID().String(),
			TraceState:             link.TraceState().AsRaw(),
			Flags:                  uint32(link.Flags()),
			Attributes:             attributes,
			DroppedAttributesCount: link.DroppedAttributesCount(),
		}
	}
	return converted, nil
}

func convertMap(source pcommon.Map) (map[string]anyValue, error) {
	converted := make(map[string]anyValue, source.Len())
	var conversionErr error
	source.Range(func(key string, value pcommon.Value) bool {
		convertedValue, err := convertValue(value)
		if err != nil {
			conversionErr = err
			return false
		}
		converted[key] = convertedValue
		return true
	})
	if conversionErr != nil {
		return nil, conversionErr
	}
	return converted, nil
}

func convertKeyValues(source pcommon.Map) ([]keyValue, error) {
	values := make([]keyValue, 0, source.Len())
	var conversionErr error
	source.Range(func(key string, value pcommon.Value) bool {
		converted, err := convertValue(value)
		if err != nil {
			conversionErr = err
			return false
		}
		values = append(values, keyValue{Key: key, Value: converted})
		return true
	})
	if conversionErr != nil {
		return nil, conversionErr
	}
	return values, nil
}

func convertValue(value pcommon.Value) (anyValue, error) {
	switch value.Type() {
	case pcommon.ValueTypeEmpty:
		return anyValue{}, nil
	case pcommon.ValueTypeStr:
		v := value.Str()
		return anyValue{StringValue: &v}, nil
	case pcommon.ValueTypeBool:
		v := value.Bool()
		return anyValue{BoolValue: &v}, nil
	case pcommon.ValueTypeInt:
		v := strconv.FormatInt(value.Int(), 10)
		return anyValue{IntValue: &v}, nil
	case pcommon.ValueTypeDouble:
		v := value.Double()
		var encoded any = v
		switch {
		case math.IsNaN(v):
			encoded = "NaN"
		case math.IsInf(v, 1):
			encoded = "Infinity"
		case math.IsInf(v, -1):
			encoded = "-Infinity"
		}
		return anyValue{DoubleValue: encoded}, nil
	case pcommon.ValueTypeBytes:
		v := base64.StdEncoding.EncodeToString(value.Bytes().AsRaw())
		return anyValue{BytesValue: &v}, nil
	case pcommon.ValueTypeSlice:
		source := value.Slice()
		values := make([]anyValue, source.Len())
		for i := range source.Len() {
			converted, err := convertValue(source.At(i))
			if err != nil {
				return anyValue{}, err
			}
			values[i] = converted
		}
		return anyValue{ArrayValue: &arrayValue{Values: values}}, nil
	case pcommon.ValueTypeMap:
		values, err := convertKeyValues(value.Map())
		if err != nil {
			return anyValue{}, err
		}
		return anyValue{KVListValue: &kvListValue{Values: values}}, nil
	default:
		return anyValue{}, errInvalidEnvelope
	}
}
