package pebblestore

import (
	"bytes"
	"math"
	"strconv"
	"strings"

	"github.com/paegun/vitaledge/internal/graph/store/typedvalue"
)

var typedPropertyEnvelopePrefix = []byte{0xFF, 'T', 'V', 0x01}

func encodeStoredPropertyValue(raw []byte) []byte {
	if len(raw) == 0 || isTypedPropertyEnvelope(raw) {
		return append([]byte(nil), raw...)
	}
	text := strings.TrimSpace(string(raw))
	if text == "" {
		return append([]byte(nil), raw...)
	}
	if looksLikeTemporalConstructor(text) || parseStoredMapStringLooksLikeComplex(text) || parseStoredListStringLooksLikeComplex(text) {
		return append([]byte(nil), raw...)
	}
	switch {
	case text == "true" || text == "false":
		return appendTypedPropertyEnvelope(typedvalue.TypeBool, raw)
	case isCanonicalIntText(text):
		return appendTypedPropertyEnvelope(typedvalue.TypeInt64, raw)
	case isFloatText(text):
		return appendTypedPropertyEnvelope(typedvalue.TypeFloat64, raw)
	default:
		return appendTypedPropertyEnvelope(typedvalue.TypeString, raw)
	}
}

func decodeStoredPropertyValueBytes(raw []byte) []byte {
	if tag, payload, ok := splitTypedPropertyEnvelope(raw); ok {
		switch tag {
		case typedvalue.TypeBool, typedvalue.TypeInt64, typedvalue.TypeFloat64, typedvalue.TypeString, typedvalue.TypeBytes:
			return append([]byte(nil), payload...)
		default:
			return append([]byte(nil), payload...)
		}
	}
	return append([]byte(nil), raw...)
}

func storedPropertyIndexTypeSegment(raw []byte) string {
	if tag, _, ok := splitTypedPropertyEnvelope(raw); ok {
		switch tag {
		case typedvalue.TypeBool:
			return "boolean"
		case typedvalue.TypeInt64, typedvalue.TypeFloat64:
			return "numeric"
		case typedvalue.TypeString:
			return "string"
		case typedvalue.TypeBytes:
			return "bytes"
		}
	}
	return "raw"
}

func splitTypedPropertyEnvelope(raw []byte) (typedvalue.TypeTag, []byte, bool) {
	if !bytes.HasPrefix(raw, typedPropertyEnvelopePrefix) || len(raw) < len(typedPropertyEnvelopePrefix)+1 {
		return 0, nil, false
	}
	tag := typedvalue.TypeTag(raw[len(typedPropertyEnvelopePrefix)])
	payload := raw[len(typedPropertyEnvelopePrefix)+1:]
	return tag, payload, true
}

func appendTypedPropertyEnvelope(tag typedvalue.TypeTag, payload []byte) []byte {
	out := make([]byte, 0, len(typedPropertyEnvelopePrefix)+1+len(payload))
	out = append(out, typedPropertyEnvelopePrefix...)
	out = append(out, byte(tag))
	out = append(out, payload...)
	return out
}

func isTypedPropertyEnvelope(raw []byte) bool {
	_, _, ok := splitTypedPropertyEnvelope(raw)
	return ok
}

func isCanonicalIntText(raw string) bool {
	if raw == "" {
		return false
	}
	if raw[0] == '+' {
		return false
	}
	if raw[0] == '-' {
		if len(raw) == 1 {
			return false
		}
		raw = raw[1:]
	}
	if raw == "0" {
		return true
	}
	if raw[0] == '0' {
		return false
	}
	for i := 0; i < len(raw); i++ {
		if raw[i] < '0' || raw[i] > '9' {
			return false
		}
	}
	return true
}

func isFloatText(raw string) bool {
	if raw == "" {
		return false
	}
	if !strings.ContainsAny(raw, ".eE") {
		return false
	}
	value, err := strconv.ParseFloat(raw, 64)
	return err == nil && !math.IsNaN(value) && !math.IsInf(value, 0)
}

func parseStoredMapStringLooksLikeComplex(raw string) bool {
	_, ok := parseStoredMapString(raw)
	return ok
}

func parseStoredListStringLooksLikeComplex(raw string) bool {
	raw = strings.TrimSpace(raw)
	return strings.HasPrefix(raw, "[") && strings.HasSuffix(raw, "]")
}

func looksLikeTemporalConstructor(raw string) bool {
	raw = strings.TrimSpace(strings.ToLower(raw))
	for _, prefix := range []string{"date(", "localtime(", "local_time(", "time(", "zoned_time(", "localdatetime(", "local_datetime(", "datetime(", "zoned_datetime(", "duration("} {
		if strings.HasPrefix(raw, prefix) {
			return true
		}
	}
	return false
}
