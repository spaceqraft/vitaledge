package typedvalue

import (
	"encoding/json"
	"errors"
	"math"
	"testing"
)

func TestEncodeDecodeRoundTripScalars(t *testing.T) {
	tests := []struct {
		name     string
		in       any
		wantTag  TypeTag
		assertFn func(t *testing.T, got any)
	}{
		{name: "nil", in: nil, wantTag: TypeNull, assertFn: func(t *testing.T, got any) {
			if got != nil {
				t.Fatalf("expected nil, got %#v", got)
			}
		}},
		{name: "bool true", in: true, wantTag: TypeBool, assertFn: func(t *testing.T, got any) {
			if got != true {
				t.Fatalf("expected true, got %#v", got)
			}
		}},
		{name: "bool false", in: false, wantTag: TypeBool, assertFn: func(t *testing.T, got any) {
			if got != false {
				t.Fatalf("expected false, got %#v", got)
			}
		}},
		{name: "int64", in: int64(-42), wantTag: TypeInt64, assertFn: func(t *testing.T, got any) {
			v, ok := got.(int64)
			if !ok || v != -42 {
				t.Fatalf("expected int64 -42, got %#v", got)
			}
		}},
		{name: "uint32", in: uint32(42), wantTag: TypeInt64, assertFn: func(t *testing.T, got any) {
			v, ok := got.(int64)
			if !ok || v != 42 {
				t.Fatalf("expected int64 42, got %#v", got)
			}
		}},
		{name: "float64", in: 12.5, wantTag: TypeFloat64, assertFn: func(t *testing.T, got any) {
			v, ok := got.(float64)
			if !ok || v != 12.5 {
				t.Fatalf("expected float64 12.5, got %#v", got)
			}
		}},
		{name: "string", in: "alice", wantTag: TypeString, assertFn: func(t *testing.T, got any) {
			v, ok := got.(string)
			if !ok || v != "alice" {
				t.Fatalf("expected string alice, got %#v", got)
			}
		}},
		{name: "bytes", in: []byte{1, 2, 3}, wantTag: TypeBytes, assertFn: func(t *testing.T, got any) {
			v, ok := got.([]byte)
			if !ok || len(v) != 3 || v[0] != 1 || v[1] != 2 || v[2] != 3 {
				t.Fatalf("expected []byte{1,2,3}, got %#v", got)
			}
		}},
		{name: "json number int", in: json.Number("123"), wantTag: TypeInt64, assertFn: func(t *testing.T, got any) {
			v, ok := got.(int64)
			if !ok || v != 123 {
				t.Fatalf("expected int64 123, got %#v", got)
			}
		}},
		{name: "json number float", in: json.Number("123.5"), wantTag: TypeFloat64, assertFn: func(t *testing.T, got any) {
			v, ok := got.(float64)
			if !ok || v != 123.5 {
				t.Fatalf("expected float64 123.5, got %#v", got)
			}
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tag, encoded, err := Encode(tc.in)
			if err != nil {
				t.Fatalf("encode failed: %v", err)
			}
			if tag != tc.wantTag {
				t.Fatalf("expected tag %d, got %d", tc.wantTag, tag)
			}

			decoded, err := Decode(tag, encoded)
			if err != nil {
				t.Fatalf("decode failed: %v", err)
			}
			tc.assertFn(t, decoded)
		})
	}
}

func TestEncodeDeterministic(t *testing.T) {
	values := []any{nil, true, int64(-123), 3.14159, "hello", []byte{9, 8, 7}}
	for _, value := range values {
		tag1, enc1, err := Encode(value)
		if err != nil {
			t.Fatalf("encode1 failed for %#v: %v", value, err)
		}
		tag2, enc2, err := Encode(value)
		if err != nil {
			t.Fatalf("encode2 failed for %#v: %v", value, err)
		}
		if tag1 != tag2 {
			t.Fatalf("tag mismatch for %#v: %d vs %d", value, tag1, tag2)
		}
		if len(enc1) != len(enc2) {
			t.Fatalf("length mismatch for %#v", value)
		}
		for i := range enc1 {
			if enc1[i] != enc2[i] {
				t.Fatalf("byte mismatch for %#v at %d", value, i)
			}
		}
	}
}

func TestCompareScalars(t *testing.T) {
	less, equal, greater := -1, 0, 1

	assertCompare := func(a any, b any, want int) {
		t.Helper()
		tagA, encA, err := Encode(a)
		if err != nil {
			t.Fatalf("encode(a) failed: %v", err)
		}
		tagB, encB, err := Encode(b)
		if err != nil {
			t.Fatalf("encode(b) failed: %v", err)
		}
		got, err := Compare(tagA, encA, tagB, encB)
		if err != nil {
			t.Fatalf("compare failed: %v", err)
		}
		if got != want {
			t.Fatalf("compare(%#v,%#v)=%d want %d", a, b, got, want)
		}
	}

	assertCompare(int64(1), int64(2), less)
	assertCompare(int64(2), int64(2), equal)
	assertCompare(int64(3), int64(2), greater)

	assertCompare(-2.5, -1.5, less)
	assertCompare(10.0, 10.0, equal)
	assertCompare(11.0, 10.0, greater)

	assertCompare("alice", "bob", less)
	assertCompare("bob", "bob", equal)
	assertCompare("cora", "bob", greater)
}

func TestEncodeRejectsNaN(t *testing.T) {
	_, _, err := Encode(math.NaN())
	if err == nil {
		t.Fatalf("expected error for NaN")
	}
	if !errors.Is(err, ErrUnsupportedType) {
		t.Fatalf("expected ErrUnsupportedType, got %v", err)
	}
}

func TestDecodeRejectsInvalidPayload(t *testing.T) {
	_, err := Decode(TypeBool, []byte{2})
	if err == nil {
		t.Fatalf("expected bool decode error")
	}
	if !errors.Is(err, ErrInvalidEncoding) {
		t.Fatalf("expected ErrInvalidEncoding, got %v", err)
	}
}

func TestEncodeRejectsUnsupported(t *testing.T) {
	type unsupported struct{ X int }
	_, _, err := Encode(unsupported{X: 1})
	if err == nil {
		t.Fatalf("expected unsupported type error")
	}
	if !errors.Is(err, ErrUnsupportedType) {
		t.Fatalf("expected ErrUnsupportedType, got %v", err)
	}
}
