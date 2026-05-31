package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

const (
	minAutoTunedMaxWriteBatchBytes    = 16 * 1024 * 1024
	maxAutoTunedMaxWriteBatchBytes    = 256 * 1024 * 1024
	autoTuneMaxWriteBatchBytesDivisor = 128
)

var readHostMemoryTotalBytes = readHostMemoryTotalBytesFromProc

func resolveMaxWriteBatchBytes(requested int) (configured int, effective int, autoTuned bool, hostMemoryBytes uint64, err error) {
	if requested < 0 {
		return 0, 0, false, 0, fmt.Errorf("max write batch bytes must be >= 0")
	}

	configured = requested
	effective = requested
	if requested > 0 {
		return configured, effective, false, 0, nil
	}

	configured = defaultMaxWriteBatchBytes
	effective = configured
	hostMemoryBytes, err = readHostMemoryTotalBytes()
	if err != nil {
		return configured, effective, false, 0, nil
	}
	if hostMemoryBytes == 0 {
		return configured, effective, false, 0, nil
	}
	effective = autoTuneMaxWriteBatchBytes(hostMemoryBytes)
	autoTuned = effective != configured
	return configured, effective, autoTuned, hostMemoryBytes, nil
}

func autoTuneMaxWriteBatchBytes(hostMemoryBytes uint64) int {
	if hostMemoryBytes == 0 {
		return defaultMaxWriteBatchBytes
	}
	tuned := int(hostMemoryBytes / autoTuneMaxWriteBatchBytesDivisor)
	if tuned < minAutoTunedMaxWriteBatchBytes {
		tuned = minAutoTunedMaxWriteBatchBytes
	}
	if tuned > maxAutoTunedMaxWriteBatchBytes {
		tuned = maxAutoTunedMaxWriteBatchBytes
	}
	if tuned <= 0 {
		return defaultMaxWriteBatchBytes
	}
	return tuned
}

func readHostMemoryTotalBytesFromProc() (uint64, error) {
	buf, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(buf), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		if strings.TrimSuffix(fields[0], ":") != "MemTotal" {
			continue
		}
		kb, parseErr := strconv.ParseUint(fields[1], 10, 64)
		if parseErr != nil {
			return 0, parseErr
		}
		return kb * 1024, nil
	}
	return 0, fmt.Errorf("meminfo missing MemTotal")
}
