package pebblestore

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/spaceqraft/vitaledge/internal/graph"
)

func vertexPropertyPartsFromKey(key []byte) (schema, property string, encodedValue []byte, ok bool) {
	parts := bytes.Split(key, []byte{'/'})
	if len(parts) < 6 {
		return "", "", nil, false
	}
	schema = string(parts[len(parts)-3])
	property = string(parts[len(parts)-2])
	valueHex := string(parts[len(parts)-1])
	if schema == "" || property == "" {
		return "", "", nil, false
	}
	decodedValue, err := hex.DecodeString(valueHex)
	if err != nil {
		return "", "", nil, false
	}
	return schema, property, decodedValue, true
}

func propertyValueKind(raw []byte) string {
	if _, ok := numericOrderedValueFromEncoded(raw); ok {
		return "numeric"
	}
	if _, ok := datetimeOrderedValueFromEncoded(raw); ok {
		return "datetime"
	}
	if _, ok := booleanOrderedValueFromEncoded(raw); ok {
		return "boolean"
	}
	return "categorical"
}

func validatePropertyEntry(entry *graph.PropertyIndexEntry) error {
	if entry == nil {
		return graph.NewError(graph.ErrKindInvalidInput, "property index entry is required", nil)
	}
	if entry.Tenant == "" || entry.Schema == "" || entry.Property == "" || entry.EntityID == "" || entry.EntityClass == "" {
		return graph.NewError(graph.ErrKindInvalidInput, "property index entry has missing required fields", nil)
	}
	return nil
}

func propertyIndexEntryFromKey(key, value []byte, tenant, schema, property string) (*graph.PropertyIndexEntry, error) {
	parts := bytes.Split(key, []byte{'/'})
	if len(parts) < 7 {
		return nil, graph.NewError(graph.ErrKindStorage, "malformed property index key", nil)
	}
	entityID := string(parts[len(parts)-1])
	encodedValue := parts[len(parts)-2]
	decodedValue, err := hex.DecodeString(string(encodedValue))
	if err != nil {
		return nil, graph.NewError(graph.ErrKindStorage, "decode property index value", err)
	}
	entityClass, edgeSrcID, edgeDstID, err := parsePropertyIndexPayload(value)
	if err != nil {
		return nil, err
	}
	return &graph.PropertyIndexEntry{
		Tenant:      tenant,
		Schema:      schema,
		Property:    property,
		Value:       decodeStoredPropertyValueBytes(decodedValue),
		EntityID:    entityID,
		EntityClass: entityClass,
		EdgeSrcID:   edgeSrcID,
		EdgeDstID:   edgeDstID,
	}, nil
}

func numericPropertyIndexEntryFromKey(key, payload []byte, tenant, schema, property string) (*graph.PropertyIndexEntry, error) {
	parts := bytes.Split(key, []byte{'/'})
	if len(parts) < 7 {
		return nil, graph.NewError(graph.ErrKindStorage, "malformed numeric property index key", nil)
	}
	entityID := string(parts[len(parts)-1])
	entityClass, edgeSrcID, edgeDstID, rawValue, err := parseNumericPropertyIndexPayload(payload)
	if err != nil {
		return nil, err
	}
	return &graph.PropertyIndexEntry{
		Tenant:      tenant,
		Schema:      schema,
		Property:    property,
		Value:       decodeStoredPropertyValueBytes(rawValue),
		EntityID:    entityID,
		EntityClass: entityClass,
		EdgeSrcID:   edgeSrcID,
		EdgeDstID:   edgeDstID,
	}, nil
}

func edgePropertyPartsFromKey(key []byte) (schema, property string, encodedValue []byte, ok bool) {
	parts := bytes.Split(key, []byte{'/'})
	if len(parts) < 6 {
		return "", "", nil, false
	}
	schema = string(parts[len(parts)-3])
	property = string(parts[len(parts)-2])
	valueHex := string(parts[len(parts)-1])
	if schema == "" || property == "" {
		return "", "", nil, false
	}
	decodedValue, err := hex.DecodeString(valueHex)
	if err != nil {
		return "", "", nil, false
	}
	return schema, property, decodedValue, true
}

func parseNumericPropertyValue(raw []byte) (float64, bool) {
	text := string(raw)
	if text == "" {
		return 0, false
	}
	numeric, err := strconv.ParseFloat(text, 64)
	if err != nil || math.IsNaN(numeric) {
		return 0, false
	}
	return numeric, true
}

func propertyIndexPayload(entry *graph.PropertyIndexEntry) []byte {
	if entry == nil {
		return nil
	}
	if entry.EdgeSrcID == "" && entry.EdgeDstID == "" {
		return []byte(entry.EntityClass)
	}
	payload := make([]byte, 0, len(entry.EntityClass)+len(entry.EdgeSrcID)+len(entry.EdgeDstID)+3)
	payload = append(payload, []byte(entry.EntityClass)...)
	payload = append(payload, 0)
	payload = append(payload, []byte(entry.EdgeSrcID)...)
	payload = append(payload, 0)
	payload = append(payload, []byte(entry.EdgeDstID)...)
	return payload
}

func numericPropertyIndexPayload(entry *graph.PropertyIndexEntry) []byte {
	if entry == nil {
		return nil
	}
	prefix := propertyIndexPayload(entry)
	payload := make([]byte, 0, len(prefix)+1+len(entry.Value))
	payload = append(payload, prefix...)
	payload = append(payload, 0)
	payload = append(payload, entry.Value...)
	return payload
}

func parsePropertyIndexPayload(payload []byte) (string, string, string, error) {
	if len(payload) == 0 {
		return "", "", "", graph.NewError(graph.ErrKindStorage, "malformed property index payload", nil)
	}
	parts := bytes.Split(payload, []byte{0})
	entityClass := ""
	edgeSrcID := ""
	edgeDstID := ""
	if len(parts) > 0 {
		entityClass = string(parts[0])
	}
	if len(parts) > 1 {
		edgeSrcID = string(parts[1])
	}
	if len(parts) > 2 {
		edgeDstID = string(parts[2])
	}
	if entityClass == "" {
		return "", "", "", graph.NewError(graph.ErrKindStorage, "property index payload missing entity class", nil)
	}
	return entityClass, edgeSrcID, edgeDstID, nil
}

func parseNumericPropertyIndexPayload(payload []byte) (string, string, string, []byte, error) {
	if len(payload) == 0 {
		return "", "", "", nil, graph.NewError(graph.ErrKindStorage, "malformed numeric property index payload", nil)
	}

	parts := bytes.Split(payload, []byte{0})
	if len(parts) >= 4 {
		entityClass := string(parts[0])
		edgeSrcID := string(parts[1])
		edgeDstID := string(parts[2])
		rawValue := append([]byte(nil), parts[3]...)
		if entityClass == "" {
			return "", "", "", nil, graph.NewError(graph.ErrKindStorage, "numeric property index payload missing entity class", nil)
		}
		return entityClass, edgeSrcID, edgeDstID, rawValue, nil
	}

	sep := bytes.IndexByte(payload, 0)
	if sep < 0 {
		return "", "", "", nil, graph.NewError(graph.ErrKindStorage, "malformed numeric property index payload", nil)
	}
	entityClass := string(payload[:sep])
	rawValue := append([]byte(nil), payload[sep+1:]...)
	if entityClass == "" {
		return "", "", "", nil, graph.NewError(graph.ErrKindStorage, "numeric property index payload missing entity class", nil)
	}
	return entityClass, "", "", rawValue, nil
}

func numericOrderedValueFromEncoded(raw []byte) ([]byte, bool) {
	raw = decodeStoredPropertyValueBytes(raw)
	text := string(raw)
	if text == "" {
		return nil, false
	}
	numeric, err := strconv.ParseFloat(text, 64)
	if err != nil || math.IsNaN(numeric) {
		return nil, false
	}
	return orderedFloat64Bytes(numeric), true
}

func booleanOrderedValueFromEncoded(raw []byte) ([]byte, bool) {
	raw = decodeStoredPropertyValueBytes(raw)
	text := string(raw)
	switch text {
	case "false":
		return orderedBoolBytes(false), true
	case "true":
		return orderedBoolBytes(true), true
	default:
		return nil, false
	}
}

func datetimeOrderedValueFromEncoded(raw []byte) ([]byte, bool) {
	raw = decodeStoredPropertyValueBytes(raw)
	temporal, ok := parseStoredMapString(string(raw))
	if !ok {
		return nil, false
	}
	if !strings.EqualFold(fmt.Sprint(temporal["__temporal_type"]), "datetime") {
		return nil, false
	}
	t, ok := storedTemporalDateTime(temporal)
	if !ok {
		return nil, false
	}
	return orderedTimeBytes(t.UTC()), true
}

func booleanPropertyIndexEntryFromKey(key, payload []byte, tenant, schema, property string) (*graph.PropertyIndexEntry, error) {
	return numericPropertyIndexEntryFromKey(key, payload, tenant, schema, property)
}

func datetimePropertyIndexEntryFromKey(key, payload []byte, tenant, schema, property string) (*graph.PropertyIndexEntry, error) {
	return numericPropertyIndexEntryFromKey(key, payload, tenant, schema, property)
}

func numericValueInRange(value, lower float64, lowerSet, lowerInclusive bool, upper float64, upperSet, upperInclusive bool) bool {
	if lowerSet {
		if lowerInclusive {
			if value < lower {
				return false
			}
		} else if value <= lower {
			return false
		}
	}
	if upperSet {
		if upperInclusive {
			if value > upper {
				return false
			}
		} else if value >= upper {
			return false
		}
	}
	return true
}

func orderedBoolBytes(v bool) []byte {
	if v {
		return []byte{1}
	}
	return []byte{0}
}

func orderedInt64Bytes(v int64) []byte {
	bits := uint64(v) ^ (uint64(1) << 63)
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, bits)
	return out
}

func orderedTimeBytes(t time.Time) []byte {
	return orderedInt64Bytes(t.UnixNano())
}

func parseStoredMapString(raw string) (map[string]any, bool) {
	if !strings.HasPrefix(raw, "map[") || !strings.HasSuffix(raw, "]") {
		return nil, false
	}
	body := raw[len("map[") : len(raw)-1]
	if body == "" {
		return map[string]any{}, true
	}
	out := map[string]any{}
	for _, part := range strings.Fields(body) {
		pair := strings.SplitN(part, ":", 2)
		if len(pair) != 2 {
			continue
		}
		out[pair[0]] = pair[1]
	}
	return out, true
}

func storedTemporalDateTime(src map[string]any) (time.Time, bool) {
	year, ok := storedMapInt(src, "year")
	if !ok {
		return time.Time{}, false
	}
	month, ok := storedMapInt(src, "month")
	if !ok {
		month = 1
	}
	day, ok := storedMapInt(src, "day")
	if !ok {
		day = 1
	}
	hour, _ := storedMapInt(src, "hour")
	minute, _ := storedMapInt(src, "minute")
	second, _ := storedMapInt(src, "second")
	nanosecond, _ := storedMapInt(src, "nanosecond")

	loc := time.UTC
	if tzRaw, ok := src["timezone"]; ok {
		tz := fmt.Sprint(tzRaw)
		if tz != "" {
			if offset, err := parseOffsetSeconds(tz); err == nil {
				loc = time.FixedZone("", offset)
			} else if loaded, err := time.LoadLocation(tz); err == nil {
				loc = loaded
			}
		}
	}

	if month < 1 || month > 12 || day < 1 || day > 31 {
		return time.Time{}, false
	}
	return time.Date(year, time.Month(month), day, hour, minute, second, nanosecond, loc), true
}

func storedMapInt(value map[string]any, key string) (int, bool) {
	raw, ok := value[key]
	if !ok {
		return 0, false
	}
	switch typed := raw.(type) {
	case string:
		parsed, err := strconv.Atoi(typed)
		if err != nil {
			return 0, false
		}
		return parsed, true
	case int:
		return typed, true
	case int64:
		return int(typed), true
	case float64:
		return int(typed), true
	default:
		return 0, false
	}
}

func parseOffsetSeconds(raw string) (int, error) {
	if raw == "" || strings.EqualFold(raw, "Z") {
		return 0, nil
	}
	if strings.HasPrefix(raw, "+") || strings.HasPrefix(raw, "-") {
		sign := 1
		if raw[0] == '-' {
			sign = -1
		}
		parts := strings.Split(strings.TrimPrefix(strings.TrimPrefix(raw, "+"), "-"), ":")
		if len(parts) != 2 && len(parts) != 3 {
			return 0, fmt.Errorf("invalid offset %q", raw)
		}
		hours, err := strconv.Atoi(parts[0])
		if err != nil {
			return 0, err
		}
		minutes, err := strconv.Atoi(parts[1])
		if err != nil {
			return 0, err
		}
		seconds := 0
		if len(parts) == 3 {
			seconds, err = strconv.Atoi(parts[2])
			if err != nil {
				return 0, err
			}
		}
		return sign * (hours*3600 + minutes*60 + seconds), nil
	}
	return 0, fmt.Errorf("invalid offset %q", raw)
}

func orderedFloat64Bytes(v float64) []byte {
	bits := math.Float64bits(v)
	if bits&(1<<63) != 0 {
		bits = ^bits
	} else {
		bits ^= 1 << 63
	}
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, bits)
	return out
}
