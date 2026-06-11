package typedvalue

import (
	"math"
	"testing"
)

func FuzzEncodeDecodeInt64(f *testing.F) {
	f.Add(int64(0))
	f.Add(int64(-1))
	f.Add(int64(1))
	f.Add(int64(math.MaxInt64))
	f.Add(int64(math.MinInt64))

	f.Fuzz(func(t *testing.T, v int64) {
		tag, encoded, err := Encode(v)
		if err != nil {
			t.Fatalf("encode failed: %v", err)
		}
		if tag != TypeInt64 {
			t.Fatalf("expected TypeInt64, got %d", tag)
		}
		decoded, err := Decode(tag, encoded)
		if err != nil {
			t.Fatalf("decode failed: %v", err)
		}
		got, ok := decoded.(int64)
		if !ok {
			t.Fatalf("expected int64 decode, got %#v", decoded)
		}
		if got != v {
			t.Fatalf("round-trip mismatch: got %d want %d", got, v)
		}
	})
}

func FuzzEncodeDecodeFloat64(f *testing.F) {
	f.Add(0.0)
	f.Add(-1.0)
	f.Add(1.5)
	f.Add(math.SmallestNonzeroFloat64)
	f.Add(math.MaxFloat64)

	f.Fuzz(func(t *testing.T, v float64) {
		if math.IsNaN(v) {
			t.Skip()
		}
		tag, encoded, err := Encode(v)
		if err != nil {
			t.Fatalf("encode failed: %v", err)
		}
		if tag != TypeFloat64 {
			t.Fatalf("expected TypeFloat64, got %d", tag)
		}
		decoded, err := Decode(tag, encoded)
		if err != nil {
			t.Fatalf("decode failed: %v", err)
		}
		got, ok := decoded.(float64)
		if !ok {
			t.Fatalf("expected float64 decode, got %#v", decoded)
		}
		if got != v {
			t.Fatalf("round-trip mismatch: got %v want %v", got, v)
		}
	})
}

func FuzzEncodeDecodeString(f *testing.F) {
	f.Add("")
	f.Add("a")
	f.Add("alice")

	f.Fuzz(func(t *testing.T, v string) {
		tag, encoded, err := Encode(v)
		if err != nil {
			t.Fatalf("encode failed: %v", err)
		}
		if tag != TypeString {
			t.Fatalf("expected TypeString, got %d", tag)
		}
		decoded, err := Decode(tag, encoded)
		if err != nil {
			t.Fatalf("decode failed: %v", err)
		}
		got, ok := decoded.(string)
		if !ok {
			t.Fatalf("expected string decode, got %#v", decoded)
		}
		if got != v {
			t.Fatalf("round-trip mismatch: got %q want %q", got, v)
		}
	})
}
