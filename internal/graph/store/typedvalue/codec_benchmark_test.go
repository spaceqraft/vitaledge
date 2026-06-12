package typedvalue

import (
	"encoding/json"
	"testing"
)

var benchmarkDecodedValue any

type decodeBenchmarkCase struct {
	name       string
	value      any
	legacyJSON []byte
}

var decodeBenchmarkCases = []decodeBenchmarkCase{
	{name: "bool_true", value: true, legacyJSON: []byte("true")},
	{name: "int64", value: int64(123456789), legacyJSON: []byte("123456789")},
	{name: "float64", value: 123456.789, legacyJSON: []byte("123456.789")},
	{name: "string", value: "typed decode benchmark payload", legacyJSON: []byte(`"typed decode benchmark payload"`)},
}

func BenchmarkDecodeTypedCodecScalars(b *testing.B) {
	for _, tc := range decodeBenchmarkCases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			tag, encoded, err := Encode(tc.value)
			if err != nil {
				b.Fatalf("encode setup failed: %v", err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				decoded, decodeErr := Decode(tag, encoded)
				if decodeErr != nil {
					b.Fatalf("decode failed: %v", decodeErr)
				}
				benchmarkDecodedValue = decoded
			}
		})
	}
}

func BenchmarkDecodeLegacyJSONScalars(b *testing.B) {
	for _, tc := range decodeBenchmarkCases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var decoded any
				if err := json.Unmarshal(tc.legacyJSON, &decoded); err != nil {
					b.Fatalf("json unmarshal failed: %v", err)
				}
				benchmarkDecodedValue = decoded
			}
		})
	}
}
