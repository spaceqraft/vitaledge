package pebblestore

import (
	"fmt"
	"strconv"
	"strings"
)

func normalizedLabelsOrdered(labels []string) []string {
	out := make([]string, 0, len(labels))
	seen := make(map[string]struct{}, len(labels))
	for _, label := range labels {
		normalized := label
		if normalized == "" {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	if len(out) == 0 {
		return []string{"UNLABELED"}
	}
	return out
}

func labelSliceSet(labels []string) map[string]struct{} {
	out := make(map[string]struct{}, len(labels))
	for _, label := range labels {
		out[label] = struct{}{}
	}
	return out
}

func encodeVertexLabelOrder(order int, label string) []byte {
	return []byte(fmt.Sprintf("%09d:%s", order, label))
}

func decodeVertexLabelOrder(raw []byte, label string) int {
	text := string(raw)
	if text == "" {
		return 0
	}
	parts := strings.SplitN(text, ":", 2)
	if len(parts) != 2 {
		return 0
	}
	if parts[1] != label {
		return 0
	}
	parsed, err := strconv.Atoi(parts[0])
	if err != nil || parsed < 0 {
		return 0
	}
	return parsed
}
