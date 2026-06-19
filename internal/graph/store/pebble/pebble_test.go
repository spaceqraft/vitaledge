package pebblestore

import (
	"os"
	"path/filepath"
	"testing"

	cpebble "github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

func TestPutGet(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	key := []byte("key")
	value := []byte("value")

	err := db.Set(key, value, cpebble.NoSync)
	require.NoError(t, err)

	got, closer, err := db.Get(key)
	require.NoError(t, err)
	require.Equal(t, value, got)
	closer.Close()
}

func TestDelete(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	key := []byte("key")
	value := []byte("value")

	err := db.Set(key, value, cpebble.NoSync)
	require.NoError(t, err)

	err = db.Delete(key, cpebble.NoSync)
	require.NoError(t, err)

	_, closer, err := db.Get(key)
	if closer != nil {
		closer.Close()
	}
	require.ErrorIs(t, err, cpebble.ErrNotFound)
}

func BenchmarkMixedWorkload(b *testing.B) {
	db, cleanup := openTestDB(b)
	defer cleanup()

	key := []byte("key")
	value := []byte("value")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := db.Set(key, value, cpebble.NoSync)
		if err != nil {
			b.Fatalf("failed to set key: %v", err)
		}

		_, closer, err := db.Get(key)
		if err != nil {
			b.Fatalf("failed to get key: %v", err)
		}
		closer.Close()

		err = db.Delete(key, cpebble.NoSync)
		if err != nil {
			b.Fatalf("failed to delete key: %v", err)
		}
	}
}

func openTestDB(t testing.TB) (*cpebble.DB, func()) {
	t.Helper()

	base := t.TempDir()
	path := base + "/ptestdb"
	if err := os.MkdirAll(filepath.Clean(path), 0o755); err != nil {
		t.Fatalf("failed to create test directory: %v", err)
	}

	pebbleOpts := &cpebble.Options{}
	ownedCache := cpebble.NewCache(1 << 20)
	pebbleOpts.Cache = ownedCache
	pebbleOpts.MemTableSize = uint64(1 << 19)
	pebbleOpts.MemTableStopWritesThreshold = 4

	db, err := cpebble.Open(path, pebbleOpts)
	require.NoError(t, err)

	return db, func() {
		db.Close()
		ownedCache.Unref()
		os.RemoveAll(path)
	}
}
