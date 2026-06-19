package pebblestore

import (
	"bytes"
	"hash/fnv"
	"sort"
	"strconv"

	"github.com/spaceqraft/vitaledge/internal/graph"
)

type edgeMeta struct {
	edgeType string
	srcID    string
	dstID    string
}

func edgePHash(edge *graph.Edge) []byte {
	h := fnv.New64a()
	if edge != nil {
		_, _ = h.Write([]byte(edge.Tenant))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(edge.ID))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(edge.Type))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(edge.SrcID))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(edge.DstID))
		_, _ = h.Write([]byte{0})
		if len(edge.Properties) > 0 {
			keys := make([]string, 0, len(edge.Properties))
			for key := range edge.Properties {
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
				_, _ = h.Write(edge.Properties[key])
				_, _ = h.Write([]byte{0})
			}
		}
	}
	return []byte(strconv.FormatUint(h.Sum64(), 16))
}

func edgeIDFromAdjKey(key []byte) string {
	i := bytes.LastIndexByte(key, '/')
	if i < 0 || i >= len(key)-1 {
		return ""
	}
	return string(key[i+1:])
}

func outAdjSourceAndTypeFromKey(key []byte) (sourceID string, edgeType string, ok bool) {
	parts := bytes.Split(key, []byte{'/'})
	if len(parts) < 5 {
		return "", "", false
	}
	source := parts[len(parts)-3]
	typ := parts[len(parts)-2]
	if len(source) == 0 || len(typ) == 0 {
		return "", "", false
	}
	return string(source), string(typ), true
}

func edgeEndpointsPayload(srcID, dstID string) []byte {
	payload := make([]byte, 0, len(srcID)+len(dstID)+1)
	payload = append(payload, srcID...)
	payload = append(payload, 0)
	payload = append(payload, dstID...)
	return payload
}

func parseEdgeEndpointsPayload(payload []byte) (srcID, dstID string, ok bool) {
	parts := bytes.Split(payload, []byte{0})
	if len(parts) != 2 {
		return "", "", false
	}
	if len(parts[0]) == 0 || len(parts[1]) == 0 {
		return "", "", false
	}
	return string(parts[0]), string(parts[1]), true
}
