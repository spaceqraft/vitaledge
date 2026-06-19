package pebblestore

import (
	"context"
	"io"
	"time"

	cpebble "github.com/cockroachdb/pebble"

	"github.com/spaceqraft/vitaledge/internal/graph"
)

type Store struct {
	db                 *cpebble.DB
	metrics            Metrics
	maxWriteBatchBytes int
	ownedCache         *cpebble.Cache
}

type kvReader interface {
	Get(key []byte) ([]byte, io.Closer, error)
	NewIter(o *cpebble.IterOptions) (*cpebble.Iterator, error)
}

type kvWriter interface {
	Set(key []byte, value []byte, opts *cpebble.WriteOptions) error
	Delete(key []byte, opts *cpebble.WriteOptions) error
}

var _ graph.GraphStore = (*Store)(nil)

func Open(path string) (*Store, error) {
	return OpenWithOptions(path, StoreOptions{})
}

func OpenWithOptions(path string, opts StoreOptions) (*Store, error) {
	pebbleOpts := opts.PebbleOptions
	if pebbleOpts == nil {
		pebbleOpts = &cpebble.Options{}
	}
	var ownedCache *cpebble.Cache
	if opts.PebbleBlockCacheBytes > 0 && pebbleOpts.Cache == nil {
		ownedCache = cpebble.NewCache(opts.PebbleBlockCacheBytes)
		pebbleOpts.Cache = ownedCache
	}
	if opts.PebbleMemTableSizeBytes > 0 {
		pebbleOpts.MemTableSize = uint64(opts.PebbleMemTableSizeBytes)
	}
	if opts.PebbleMemTableStopWritesThreshold > 0 {
		pebbleOpts.MemTableStopWritesThreshold = opts.PebbleMemTableStopWritesThreshold
	}
	db, err := cpebble.Open(path, pebbleOpts)
	if err != nil {
		if ownedCache != nil {
			ownedCache.Unref()
		}
		return nil, graph.NewError(graph.ErrKindStorage, "open pebble db", err)
	}
	metrics := opts.Metrics
	if metrics == nil {
		metrics = defaultMetrics
	}
	maxWriteBatchBytes := opts.MaxWriteBatchBytes
	if maxWriteBatchBytes <= 0 {
		maxWriteBatchBytes = DefaultMaxWriteBatchBytes
	}
	store := &Store{db: db, metrics: metrics, maxWriteBatchBytes: maxWriteBatchBytes, ownedCache: ownedCache}
	if err := store.runMigrations(context.Background()); err != nil {
		_ = db.Close()
		if ownedCache != nil {
			ownedCache.Unref()
		}
		return nil, err
	}
	return store, nil
}

func (s *Store) BeginTx(ctx context.Context, opts graph.TxOptions) (graph.Tx, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	if s == nil || s.db == nil {
		return nil, graph.NewError(graph.ErrKindStorage, "graph store is closed", nil)
	}

	switch opts.Mode {
	case graph.TxReadOnly:
		snap := s.db.NewSnapshot()
		return &tx{store: s, mode: opts.Mode, reader: snap, snapshot: snap}, nil
	case graph.TxReadWrite:
		batch := s.db.NewIndexedBatch()
		return &tx{store: s, mode: opts.Mode, reader: batch, writer: batch, batch: batch, maxWriteBatchBytes: s.maxWriteBatchBytes}, nil
	default:
		return nil, graph.NewError(graph.ErrKindInvalidInput, "unsupported transaction mode", nil)
	}
}

func (s *Store) View(ctx context.Context, fn func(graph.Tx) error) error {
	started := time.Now()
	var txErr error
	defer func() {
		s.observeTx(graph.TxReadOnly, txErr, started)
	}()

	txn, err := s.BeginTx(ctx, graph.TxOptions{Mode: graph.TxReadOnly})
	if err != nil {
		txErr = err
		return txErr
	}
	defer func() {
		_ = txn.Rollback()
	}()

	if err := fn(txn); err != nil {
		txErr = err
		return txErr
	}
	txErr = txn.Commit()
	return txErr
}

func (s *Store) Update(ctx context.Context, fn func(graph.Tx) error) error {
	started := time.Now()
	var txErr error
	defer func() {
		s.observeTx(graph.TxReadWrite, txErr, started)
	}()

	txn, err := s.BeginTx(ctx, graph.TxOptions{Mode: graph.TxReadWrite})
	if err != nil {
		txErr = err
		return txErr
	}
	defer func() {
		_ = txn.Rollback()
	}()

	if err := fn(txn); err != nil {
		txErr = err
		return txErr
	}
	txErr = txn.Commit()
	return txErr
}

func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	var closeErr error
	if s.db != nil {
		if err := s.db.Close(); err != nil {
			closeErr = graph.NewError(graph.ErrKindStorage, "close pebble db", err)
		}
		s.db = nil
	}
	if s.ownedCache != nil {
		s.ownedCache.Unref()
		s.ownedCache = nil
	}
	return closeErr
}

func checkCtx(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return graph.NewError(graph.ErrKindTimeout, "context canceled", err)
	}
	return nil
}
