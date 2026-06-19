package pebblestore

import (
	"bytes"
	"hash/fnv"
	"sort"
	"strconv"

	"github.com/spaceqraft/vitaledge/internal/graph"
)

func vertexIDFromKey(key []byte) string {
	parts := bytes.Split(key, []byte{'/'})
	if len(parts) < 3 {
		return ""
	}
	return string(parts[len(parts)-1])
}

func vertexPHash(vertex *graph.Vertex) []byte {
	h := fnv.New64a()
	if vertex != nil {
		_, _ = h.Write([]byte(vertex.Tenant))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(vertex.ID))
		_, _ = h.Write([]byte{0})
		labels := make([]string, 0, len(vertex.Labels))
		for _, label := range vertex.Labels {
			normalized := label
			if normalized == "" {
				continue
			}
			labels = append(labels, normalized)
		}
		sort.Strings(labels)
		for _, label := range labels {
			_, _ = h.Write([]byte(label))
			_, _ = h.Write([]byte{0})
		}
		if len(vertex.Properties) > 0 {
			keys := make([]string, 0, len(vertex.Properties))
			for key := range vertex.Properties {
				normalized := key
				if normalized == "" {
					continue
				}
				keys = append(keys, normalized)
			}
			sort.Strings(keys)
			for _, key := range keys {
				_, _ = h.Write([]byte(key))
				_, _ = h.Write([]byte{0})
				_, _ = h.Write(vertex.Properties[key])
				_, _ = h.Write([]byte{0})
			}
		}
	}
	return []byte(strconv.FormatUint(h.Sum64(), 16))
}
