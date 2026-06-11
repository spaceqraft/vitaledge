package typedvalue

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// TypeTag identifies the encoded value domain.
type TypeTag uint8

const (
	TypeNull TypeTag = iota
	TypeBool
	TypeInt64
	TypeFloat64
	TypeString
	TypeBytes
)

var (
	ErrUnsupportedType = errors.New("typedvalue: unsupported type")
	ErrInvalidEncoding = errors.New("typedvalue: invalid encoding")
)

// Encode serializes a scalar value into a typed binary form.
func Encode(value any) (TypeTag, []byte, error) {
	switch typed := value.(type) {
	case nil:
		return TypeNull, nil, nil
	case bool:
		if typed {
			return TypeBool, []byte{1}, nil
		}
		return TypeBool, []byte{0}, nil
	case int:
		return encodeInt64(int64(typed))
	case int8:
		return encodeInt64(int64(typed))
	case int16:
		return encodeInt64(int64(typed))
	case int32:
		return encodeInt64(int64(typed))
	case int64:
		return encodeInt64(typed)
	case uint:
		if uint64(typed) > math.MaxInt64 {
			return 0, nil, fmt.Errorf("%w: uint out of int64 range", ErrUnsupportedType)
		}
		return encodeInt64(int64(typed))
	case uint8:
		return encodeInt64(int64(typed))
	case uint16:
		return encodeInt64(int64(typed))
	case uint32:
		return encodeInt64(int64(typed))
	case uint64:
		if typed > math.MaxInt64 {
			return 0, nil, fmt.Errorf("%w: uint64 out of int64 range", ErrUnsupportedType)
		}
		return encodeInt64(int64(typed))
	case float32:
		return encodeFloat64(float64(typed))
	case float64:
		return encodeFloat64(typed)
	case string:
		return TypeString, []byte(typed), nil
	case []byte:
		out := make([]byte, len(typed))
		copy(out, typed)
		return TypeBytes, out, nil
	case json.Number:
		if strings.ContainsAny(typed.String(), ".eE") {
			f, err := typed.Float64()
			if err != nil {
				return 0, nil, fmt.Errorf("%w: invalid json number float", ErrUnsupportedType)
			}
			return encodeFloat64(f)
		}
		i, err := typed.Int64()
		if err == nil {
			return encodeInt64(i)
		}
		f, err := typed.Float64()
		if err != nil {
			return 0, nil, fmt.Errorf("%w: invalid json number", ErrUnsupportedType)
		}
		return encodeFloat64(f)
	default:
		return 0, nil, fmt.Errorf("%w: %T", ErrUnsupportedType, value)
	}
}

// Decode reconstructs a scalar value from encoded bytes and a type tag.
func Decode(tag TypeTag, encoded []byte) (any, error) {
	switch tag {
	case TypeNull:
		if len(encoded) != 0 {
			return nil, fmt.Errorf("%w: null must have zero-length payload", ErrInvalidEncoding)
		}
		return nil, nil
	case TypeBool:
		if len(encoded) != 1 {
			return nil, fmt.Errorf("%w: bool payload length must be 1", ErrInvalidEncoding)
		}
		switch encoded[0] {
		case 0:
			return false, nil
		case 1:
			return true, nil
		default:
			return nil, fmt.Errorf("%w: bool payload must be 0 or 1", ErrInvalidEncoding)
		}
	case TypeInt64:
		if len(encoded) != 8 {
			return nil, fmt.Errorf("%w: int64 payload length must be 8", ErrInvalidEncoding)
		}
		u := binary.BigEndian.Uint64(encoded)
		i := int64(u ^ (uint64(1) << 63))
		return i, nil
	case TypeFloat64:
		if len(encoded) != 8 {
			return nil, fmt.Errorf("%w: float64 payload length must be 8", ErrInvalidEncoding)
		}
		u := binary.BigEndian.Uint64(encoded)
		if u&(uint64(1)<<63) != 0 {
			u ^= uint64(1) << 63
		} else {
			u = ^u
		}
		f := math.Float64frombits(u)
		if math.IsNaN(f) {
			return nil, fmt.Errorf("%w: nan is not supported", ErrInvalidEncoding)
		}
		return f, nil
	case TypeString:
		return string(encoded), nil
	case TypeBytes:
		out := make([]byte, len(encoded))
		copy(out, encoded)
		return out, nil
	default:
		return nil, fmt.Errorf("%w: unknown type tag %d", ErrInvalidEncoding, tag)
	}
}

// Compare compares two encoded scalar values. Returns -1, 0, 1.
func Compare(tagA TypeTag, encodedA []byte, tagB TypeTag, encodedB []byte) (int, error) {
	if tagA != tagB {
		switch {
		case tagA < tagB:
			return -1, nil
		case tagA > tagB:
			return 1, nil
		default:
			return 0, nil
		}
	}

	switch tagA {
	case TypeNull:
		if len(encodedA) != 0 || len(encodedB) != 0 {
			return 0, fmt.Errorf("%w: null must have zero-length payload", ErrInvalidEncoding)
		}
		return 0, nil
	case TypeBool:
		if len(encodedA) != 1 || len(encodedB) != 1 {
			return 0, fmt.Errorf("%w: bool payload length must be 1", ErrInvalidEncoding)
		}
		if encodedA[0] < encodedB[0] {
			return -1, nil
		}
		if encodedA[0] > encodedB[0] {
			return 1, nil
		}
		return 0, nil
	case TypeInt64, TypeFloat64, TypeString, TypeBytes:
		if tagA == TypeInt64 || tagA == TypeFloat64 {
			if len(encodedA) != 8 || len(encodedB) != 8 {
				return 0, fmt.Errorf("%w: numeric payload length must be 8", ErrInvalidEncoding)
			}
		}
		return normalizeBytesCompare(bytes.Compare(encodedA, encodedB)), nil
	default:
		return 0, fmt.Errorf("%w: unknown type tag %d", ErrInvalidEncoding, tagA)
	}
}

func encodeInt64(value int64) (TypeTag, []byte, error) {
	u := uint64(value) ^ (uint64(1) << 63)
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, u)
	return TypeInt64, buf, nil
}

func encodeFloat64(value float64) (TypeTag, []byte, error) {
	if math.IsNaN(value) {
		return 0, nil, fmt.Errorf("%w: nan is not supported", ErrUnsupportedType)
	}
	bits := math.Float64bits(value)
	if bits&(uint64(1)<<63) != 0 {
		bits = ^bits
	} else {
		bits ^= uint64(1) << 63
	}
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, bits)
	return TypeFloat64, buf, nil
}

func normalizeBytesCompare(v int) int {
	switch {
	case v < 0:
		return -1
	case v > 0:
		return 1
	default:
		return 0
	}
}

// MustEncodeInt64 is a small helper for tests/benchmarks that build expected keys.
func MustEncodeInt64(value int64) []byte {
	tag, encoded, err := Encode(value)
	if err != nil || tag != TypeInt64 {
		panic("typedvalue: unable to encode int64")
	}
	return encoded
}

// MustEncodeFloat64 is a small helper for tests/benchmarks that build expected keys.
func MustEncodeFloat64(value float64) []byte {
	tag, encoded, err := Encode(value)
	if err != nil || tag != TypeFloat64 {
		panic("typedvalue: unable to encode float64")
	}
	return encoded
}

// ParseTypeTag parses decimal strings to a TypeTag.
func ParseTypeTag(raw string) (TypeTag, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("%w: empty type tag", ErrInvalidEncoding)
	}
	n, err := strconv.ParseUint(raw, 10, 8)
	if err != nil {
		return 0, fmt.Errorf("%w: invalid type tag %q", ErrInvalidEncoding, raw)
	}
	return TypeTag(n), nil
}
