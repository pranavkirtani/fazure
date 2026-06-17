package common

import (
	"fmt"
	"os"
	"strconv"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
)

const (
	// defaultCacheSizeMB is the Pebble block-cache size. Pebble's own default is only 8 MB,
	// which thrashes under a large dataset and makes the emulator itself the bottleneck.
	defaultCacheSizeMB = 512
	// defaultMemTableSize raises Pebble's 4 MB default so bursts of writes (catalog ingestion)
	// flush less often.
	defaultMemTableSize = 64 << 20 // 64 MB
)

type Store struct {
	db *pebble.DB
}

// NewStore opens a Pebble DB tuned for the emulator's workload. Pebble's defaults are tiny
// (8 MB block cache, 4 MB memtable); under a large dataset that thrashes and makes the
// emulator the bottleneck rather than the system under test. We raise the block cache and
// memtable, and add a Bloom filter on every level so point lookups (Get, and the
// read-before-write on upserts) skip sstables that can't contain the key.
//
// The block-cache size is overridable via FAZURE_PEBBLE_CACHE_MB for large local runs.
func NewStore(datadir string) (*Store, error) {
	cacheMB := defaultCacheSizeMB
	if v := os.Getenv("FAZURE_PEBBLE_CACHE_MB"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cacheMB = n
		}
	}

	opts := &pebble.Options{
		CacheSize:    int64(cacheMB) << 20,
		MemTableSize: defaultMemTableSize,
	}
	// Levels is a fixed [NumLevels]LevelOptions array; set the Bloom filter on each level.
	for i := range opts.Levels {
		opts.Levels[i].FilterPolicy = bloom.FilterPolicy(10)
	}

	db, err := pebble.Open(datadir, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open pebble db: %w", err)
	}

	return &Store{
		db: db,
	}, nil
}

func (s *Store) DB() *pebble.DB { return s.db }

func (s *Store) Metrics() string { return s.db.Metrics().String() }

func (s *Store) Close() error { return s.db.Close() }
