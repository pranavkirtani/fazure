package tables

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/fgrzl/fazure/internal/common"
)

func closeIO(c io.Closer) error {
	if c == nil {
		return nil
	}
	return c.Close()
}

// Entity represents a table entity with metadata.
type Entity struct {
	PartitionKey string                 `json:"PartitionKey"`
	RowKey       string                 `json:"RowKey"`
	Timestamp    time.Time              `json:"Timestamp"`
	ETag         string                 `json:"ETag"`
	Properties   map[string]interface{} `json:"-"`
}

// MarshalJSON implements custom JSON marshaling (Azure-style flat object).
func (e *Entity) MarshalJSON() ([]byte, error) {
	m := map[string]interface{}{
		"PartitionKey": e.PartitionKey,
		"RowKey":       e.RowKey,
		"Timestamp":    e.Timestamp.Format(time.RFC3339Nano),
		"ETag":         e.ETag,
	}

	for k, v := range e.Properties {
		m[k] = v

		// Add type metadata for special types
		if val, ok := v.(time.Time); ok {
			m[k+"@odata.type"] = "Edm.DateTime"
			// Format as ISO 8601
			m[k] = val.Format(time.RFC3339Nano)
		}

		// Check for type hints in property name
		if typeHint, hasType := e.Properties[k+"@odata.type"].(string); hasType {
			m[k+"@odata.type"] = typeHint
		}
	}

	return json.Marshal(m)
}

// UnmarshalJSON implements custom JSON unmarshaling.
func (e *Entity) UnmarshalJSON(data []byte) error {
	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}

	if pk, ok := m["PartitionKey"].(string); ok {
		e.PartitionKey = pk
	}
	if rk, ok := m["RowKey"].(string); ok {
		e.RowKey = rk
	}
	if etag, ok := m["ETag"].(string); ok {
		e.ETag = etag
	}
	if ts, ok := m["Timestamp"].(string); ok {
		// Parse ISO 8601 / RFC3339 timestamp
		if parsed, err := time.Parse(time.RFC3339Nano, ts); err == nil {
			e.Timestamp = parsed.UTC()
		} else if parsed, err := time.Parse(time.RFC3339, ts); err == nil {
			e.Timestamp = parsed.UTC()
		}
	} else if ts, ok := m["Timestamp"].(float64); ok {
		// Fallback: Unix timestamp for backward compatibility
		e.Timestamp = time.Unix(int64(ts), 0).UTC()
	}

	e.Properties = make(map[string]interface{}, len(m))
	for k, v := range m {
		if k != "PartitionKey" && k != "RowKey" && k != "Timestamp" && k != "ETag" {
			// Preserve type metadata (properties ending with @odata.type)
			if strings.HasSuffix(k, "@odata.type") {
				e.Properties[k] = v
				continue
			}

			// Check if there's a type annotation for this property
			if typeHint, ok := m[k+"@odata.type"].(string); ok {
				switch typeHint {
				case "Edm.DateTime":
					// Try to parse as datetime
					if str, ok := v.(string); ok {
						if t, err := time.Parse(time.RFC3339Nano, str); err == nil {
							e.Properties[k] = t
							e.Properties[k+"@odata.type"] = typeHint
							continue
						} else if t, err := time.Parse(time.RFC3339, str); err == nil {
							e.Properties[k] = t
							e.Properties[k+"@odata.type"] = typeHint
							continue
						}
					}
				case "Edm.Guid", "Edm.Int64", "Edm.Binary":
					// Preserve type hint
					e.Properties[k+"@odata.type"] = typeHint
				}
			}

			e.Properties[k] = v
		}
	}

	return nil
}

// ---------------------------------------------------------------------------
// Pebble key encoding
// Layout: tables/data/<table>\x00<partitionKey>\x00<rowKey>
// This preserves table, PK, RK ordering and works well with prefix/range scans.
// ---------------------------------------------------------------------------

const (
	metaPrefix      = "tables/meta/"
	dataPrefix      = "tables/data/"
	keySep     byte = 0x00
)

// metaKey(tableName) => tables/meta/<table>
func metaKey(tableName string) []byte {
	return []byte(metaPrefix + tableName)
}

// filterReservedProperties removes system-managed properties from user input.
// Azure Table Storage ignores these properties when sent by clients.
func filterReservedProperties(properties map[string]interface{}) map[string]interface{} {
	filtered := make(map[string]interface{}, len(properties))
	for k, v := range properties {
		// Skip reserved properties
		if k == "Timestamp" || k == "ETag" || k == "odata.etag" || k == "@odata.etag" {
			continue
		}
		filtered[k] = v
	}
	return filtered
}

// dataKey(table, pk, rk) => tables/data/<table>\x00<pk>\x00<rk>
func dataKey(table, pk, rk string) []byte {
	n := len(dataPrefix) + len(table) + 1 + len(pk) + 1 + len(rk)
	b := make([]byte, 0, n)
	b = append(b, dataPrefix...)
	b = append(b, table...)
	b = append(b, keySep)
	b = append(b, pk...)
	b = append(b, keySep)
	b = append(b, rk...)
	return b
}

// tablePrefix(table) => prefix for all entities in a table
// tables/data/<table>\x00
func tablePrefix(table string) []byte {
	n := len(dataPrefix) + len(table) + 1
	b := make([]byte, 0, n)
	b = append(b, dataPrefix...)
	b = append(b, table...)
	b = append(b, keySep)
	return b
}

// partitionPrefix(table, pk) => prefix for all entities in a partition
// tables/data/<table>\x00<pk>\x00
func partitionPrefix(table, pk string) []byte {
	n := len(dataPrefix) + len(table) + 1 + len(pk) + 1
	b := make([]byte, 0, n)
	b = append(b, dataPrefix...)
	b = append(b, table...)
	b = append(b, keySep)
	b = append(b, pk...)
	b = append(b, keySep)
	return b
}

// upperBoundForPrefix returns an upper bound suitable for Pebble range scans.
// It increments the last non-0xff byte to create an exclusive upper bound for prefix scans.
func upperBoundForPrefix(prefix []byte) []byte {
	// Find the last byte that can be incremented
	end := len(prefix)
	for end > 0 && prefix[end-1] == 0xff {
		end--
	}

	// If all bytes are 0xff, no upper bound is needed
	if end == 0 {
		return nil
	}

	// Copy and increment the last non-0xff byte
	upper := make([]byte, end)
	copy(upper, prefix[:end])
	upper[end-1]++
	return upper
}

// ---------------------------------------------------------------------------
// TableStore
// ---------------------------------------------------------------------------

// TableStore manages tables and entities.
type TableStore struct {
	store   *common.Store
	log     *slog.Logger
	metrics *Metrics
}

// NewTableStore creates a new table store.
func NewTableStore(store *common.Store, logger *slog.Logger, metrics *Metrics) (*TableStore, error) {
	if logger == nil {
		logger = slog.Default()
	}

	return &TableStore{
		store:   store,
		log:     logger.With("component", "tables-store"),
		metrics: metrics,
	}, nil
}

// ListTables returns all table names.
func (ts *TableStore) ListTables(ctx context.Context) ([]map[string]string, error) {
	db := ts.store.DB()
	prefix := []byte(metaPrefix)
	upper := upperBoundForPrefix(prefix)

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upper,
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()

	var tables []map[string]string
	for ok := iter.First(); ok; ok = iter.Next() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		key := iter.Key()
		tableName := string(key[len(prefix):])
		tables = append(tables, map[string]string{
			"TableName": tableName,
		})
	}

	return tables, iter.Error()
}

func validateTableName(tableName string) error {
	if len(tableName) < 3 || len(tableName) > 63 {
		return ErrInvalidTableName
	}
	// Must start with a letter
	first := tableName[0]
	if (first < 'A' || first > 'Z') && (first < 'a' || first > 'z') {
		return ErrInvalidTableName
	}
	for i := 0; i < len(tableName); i++ {
		c := tableName[i]
		if (c < '0' || c > '9') && (c < 'A' || c > 'Z') && (c < 'a' || c > 'z') {
			return ErrInvalidTableName
		}
	}
	if strings.EqualFold(tableName, "Tables") {
		return ErrInvalidTableName
	}
	return nil
}

// CreateTable creates a new table.
func (ts *TableStore) CreateTable(ctx context.Context, tableName string) error {
	if err := validateTableName(tableName); err != nil {
		return err
	}
	db := ts.store.DB()
	key := metaKey(tableName)

	_, closer, err := db.Get(key)
	if err == nil {
		_ = closeIO(closer)
		return ErrTableExists
	}
	if !errors.Is(err, pebble.ErrNotFound) {
		return err
	}

	metadata := map[string]interface{}{
		"name":      tableName,
		"createdAt": time.Now().UTC(),
	}
	data, err := json.Marshal(metadata)
	if err != nil {
		return err
	}

	return db.Set(key, data, pebble.NoSync)
}

// DeleteTable deletes a table and all its entities.
func (ts *TableStore) DeleteTable(ctx context.Context, tableName string) error {
	db := ts.store.DB()
	mKey := metaKey(tableName)

	_, closer, err := db.Get(mKey)
	if errors.Is(err, pebble.ErrNotFound) {
		return ErrTableNotFound
	}
	if err != nil {
		return err
	}
	if err := closeIO(closer); err != nil {
		return err
	}

	if err := db.Delete(mKey, pebble.NoSync); err != nil {
		return err
	}

	prefix := tablePrefix(tableName)
	upper := upperBoundForPrefix(prefix)

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upper,
	})
	if err != nil {
		return err
	}
	defer func() { _ = iter.Close() }()

	batch := db.NewBatch()
	for ok := iter.First(); ok; ok = iter.Next() {
		select {
		case <-ctx.Done():
			_ = batch.Close()
			return ctx.Err()
		default:
		}
		if err := batch.Delete(iter.Key(), nil); err != nil {
			_ = batch.Close()
			return err
		}
	}

	if err := iter.Error(); err != nil {
		_ = batch.Close()
		return err
	}

	return batch.Commit(pebble.NoSync)
}

// ---------------------------------------------------------------------------
// Table
// ---------------------------------------------------------------------------

// Table represents a table handle.
type Table struct {
	store   *common.Store
	name    string
	log     *slog.Logger
	metrics *Metrics
}

// GetTable returns a table handle.
func (ts *TableStore) GetTable(ctx context.Context, tableName string) (*Table, error) {
	db := ts.store.DB()
	key := metaKey(tableName)

	_, closer, err := db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, ErrTableNotFound
	}
	if err != nil {
		return nil, err
	}
	if err := closeIO(closer); err != nil {
		return nil, err
	}

	logger := ts.log
	if logger == nil {
		logger = slog.Default()
	}

	return &Table{
		store:   ts.store,
		name:    tableName,
		log:     logger.With("table", tableName),
		metrics: ts.metrics,
	}, nil
}

// keyValidation helpers
//
// Matches Azure Table Storage key constraints: non-empty, at most 1 KiB, and free of the
// disallowed characters '/', '\', '#', '?' and control characters U+0000–U+001F and
// U+007F–U+009F. Iterates runes (not bytes) so multi-byte UTF-8 characters whose
// continuation bytes fall in the control ranges are not falsely rejected.
func validateKey(v string) error {
	if v == "" {
		return ErrInvalidEntity
	}
	if len(v) > 1024 {
		return ErrInvalidEntity
	}
	for _, r := range v {
		switch {
		case r == '/' || r == '\\' || r == '#' || r == '?':
			return ErrInvalidEntity
		case r <= 0x1F: // C0 control characters (includes tab, newline, carriage return)
			return ErrInvalidEntity
		case r >= 0x7F && r <= 0x9F: // DEL and C1 control characters
			return ErrInvalidEntity
		}
	}
	return nil
}

func validatePropertyName(n string) error {
	if len(n) > 255 {
		return ErrInvalidEntity
	}
	for i := 0; i < len(n); i++ {
		c := n[i]
		if c < 0x20 {
			return ErrInvalidEntity
		}
	}
	return nil
}

// utf16CodeUnitCount returns the size of s in UTF-16 code units, matching Azure Table Storage
// limits for Edm.String (64 KiB UTF-16 → at most 32 Ki code units).
func utf16CodeUnitCount(s string) int {
	n := 0
	for _, r := range s {
		if r > 0xffff {
			n += 2 // surrogate pair
		} else {
			n++
		}
	}
	return n
}

const maxPropertyUTF16Units = 32 * 1024
const maxPropertyBinaryBytes = 64 * 1024

func validatePropertyValue(name string, v interface{}, props map[string]interface{}) error {
	switch vv := v.(type) {
	case string:
		typeHint, _ := props[name+"@odata.type"].(string)
		if typeHint == "Edm.Binary" {
			decoded, err := base64.StdEncoding.DecodeString(vv)
			if err != nil {
				return ErrInvalidEntity
			}
			if len(decoded) > maxPropertyBinaryBytes {
				return ErrPropertyValueTooLarge
			}
			return nil
		}
		if utf16CodeUnitCount(vv) > maxPropertyUTF16Units {
			return ErrPropertyValueTooLarge
		}
	case []byte:
		if len(vv) > maxPropertyBinaryBytes {
			return ErrPropertyValueTooLarge
		}
	}
	return nil
}

func validateProperties(props map[string]interface{}) error {
	if props == nil {
		return nil
	}
	count := 0
	for k, v := range props {
		if k == "PartitionKey" || k == "RowKey" {
			continue
		}
		if strings.HasSuffix(k, "@odata.type") {
			// type annotations don't count as separate properties
			continue
		}
		count++
		if err := validatePropertyName(k); err != nil {
			return err
		}
		if err := validatePropertyValue(k, v, props); err != nil {
			return err
		}
	}
	if count > 255 {
		return ErrInvalidEntity
	}
	return nil
}

func validateEntitySize(b []byte) error {
	if len(b) > 1*1024*1024 {
		return ErrInvalidEntity
	}
	return nil
}

// InsertEntity inserts a new entity (fails if exists).
func (t *Table) InsertEntity(ctx context.Context, partitionKey, rowKey string, properties map[string]interface{}) (*Entity, error) {
	// validate keys
	if err := validateKey(partitionKey); err != nil {
		return nil, err
	}
	if err := validateKey(rowKey); err != nil {
		return nil, err
	}

	// validate properties
	if err := validateProperties(properties); err != nil {
		return nil, err
	}

	db := t.store.DB()
	key := dataKey(t.name, partitionKey, rowKey)

	_, closer, err := db.Get(key)
	if err == nil {
		_ = closeIO(closer)
		return nil, ErrEntityExists
	}
	if !errors.Is(err, pebble.ErrNotFound) {
		return nil, err
	}

	// Filter out reserved properties - these are system-managed
	filteredProps := filterReservedProperties(properties)

	entity := &Entity{
		PartitionKey: partitionKey,
		RowKey:       rowKey,
		Timestamp:    time.Now().UTC(),
		Properties:   filteredProps,
		ETag:         "", // Will be set after initial marshal
	}

	// Marshal once to compute ETag
	data, err := json.Marshal(entity)
	if err != nil {
		return nil, err
	}
	// Validate total entity size (before ETag is assigned)
	if err := validateEntitySize(data); err != nil {
		return nil, err
	}
	entity.ETag = common.GenerateETag(data)

	// Marshal again with ETag included
	finalData, err := json.Marshal(entity)
	if err != nil {
		return nil, err
	}

	if err := db.Set(key, finalData, pebble.NoSync); err != nil {
		return nil, err
	}

	return entity, nil
}

// GetEntity retrieves an entity.
func (t *Table) GetEntity(ctx context.Context, partitionKey, rowKey string) (*Entity, error) {
	db := t.store.DB()
	key := dataKey(t.name, partitionKey, rowKey)

	data, closer, err := db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, ErrEntityNotFound
	}
	if err != nil {
		return nil, err
	}

	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	if err := closeIO(closer); err != nil {
		return nil, err
	}

	var entity Entity
	if err := json.Unmarshal(dataCopy, &entity); err != nil {
		return nil, err
	}

	return &entity, nil
}

// UpdateEntity updates an entity (merge if merge=true, replace if merge=false).
func (t *Table) UpdateEntity(
	ctx context.Context,
	partitionKey, rowKey string,
	properties map[string]interface{},
	merge bool,
) (*Entity, error) {
	return t.UpdateEntityWithETag(ctx, partitionKey, rowKey, properties, merge, "")
}

// UpdateEntityWithETag updates an entity with optional ETag validation.
func (t *Table) UpdateEntityWithETag(
	ctx context.Context,
	partitionKey, rowKey string,
	properties map[string]interface{},
	merge bool,
	ifMatch string,
) (*Entity, error) {
	db := t.store.DB()
	key := dataKey(t.name, partitionKey, rowKey)

	data, closer, err := db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, ErrEntityNotFound
	}
	if err != nil {
		return nil, err
	}

	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	if err := closeIO(closer); err != nil {
		return nil, err
	}

	var entity Entity
	if err := json.Unmarshal(dataCopy, &entity); err != nil {
		return nil, err
	}

	if ifMatch != "" && ifMatch != "*" && entity.ETag != ifMatch {
		return nil, ErrPreconditionFailed
	}

	// Filter reserved properties
	filteredProps := filterReservedProperties(properties)

	if merge {
		if entity.Properties == nil {
			entity.Properties = make(map[string]interface{})
		}
		for k, v := range filteredProps {
			if k != "PartitionKey" && k != "RowKey" {
				entity.Properties[k] = v
			}
		}
	} else {
		entity.Properties = make(map[string]interface{}, len(filteredProps))
		for k, v := range filteredProps {
			if k != "PartitionKey" && k != "RowKey" {
				entity.Properties[k] = v
			}
		}
	}

	// Validate properties and entity size before persisting
	if err := validateProperties(entity.Properties); err != nil {
		return nil, err
	}
	entity.Timestamp = time.Now().UTC()
	entity.ETag = "" // Reset for consistent ETag computation

	// Marshal once to compute ETag
	data, err = json.Marshal(&entity)
	if err != nil {
		return nil, err
	}
	// Validate total entity size (before ETag is assigned)
	if err := validateEntitySize(data); err != nil {
		return nil, err
	}
	entity.ETag = common.GenerateETag(data)

	// Marshal again with ETag included
	finalData, err := json.Marshal(&entity)
	if err != nil {
		return nil, err
	}

	if err := db.Set(key, finalData, pebble.NoSync); err != nil {
		return nil, err
	}

	return &entity, nil
}

// UpsertEntity inserts or updates an entity.
func (t *Table) UpsertEntity(
	ctx context.Context,
	partitionKey, rowKey string,
	properties map[string]interface{},
	merge bool,
) (*Entity, error) {
	db := t.store.DB()
	key := dataKey(t.name, partitionKey, rowKey)

	data, closer, err := db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return t.InsertEntity(ctx, partitionKey, rowKey, properties)
	}
	if err != nil {
		return nil, err
	}

	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	if err := closeIO(closer); err != nil {
		return nil, err
	}

	var entity Entity
	if err := json.Unmarshal(dataCopy, &entity); err != nil {
		return nil, err
	}

	// Filter reserved properties
	filteredProps := filterReservedProperties(properties)

	if merge {
		if entity.Properties == nil {
			entity.Properties = make(map[string]interface{})
		}
		for k, v := range filteredProps {
			if k != "PartitionKey" && k != "RowKey" {
				entity.Properties[k] = v
			}
		}
	} else {
		entity.Properties = make(map[string]interface{}, len(filteredProps))
		for k, v := range filteredProps {
			if k != "PartitionKey" && k != "RowKey" {
				entity.Properties[k] = v
			}
		}
	}

	// Validate properties and entity size
	if err := validateProperties(entity.Properties); err != nil {
		return nil, err
	}
	entity.Timestamp = time.Now().UTC()
	entity.ETag = "" // Reset for consistent ETag computation

	// Marshal once to compute ETag
	marshaledData, err := json.Marshal(&entity)
	if err != nil {
		return nil, err
	}
	// Validate total entity size (before ETag is assigned)
	if err := validateEntitySize(marshaledData); err != nil {
		return nil, err
	}
	entity.ETag = common.GenerateETag(marshaledData)

	// Marshal again with ETag included
	finalData, err := json.Marshal(&entity)
	if err != nil {
		return nil, err
	}

	if err := db.Set(key, finalData, pebble.NoSync); err != nil {
		return nil, err
	}

	return &entity, nil
}

// DeleteEntity deletes an entity.
func (t *Table) DeleteEntity(ctx context.Context, partitionKey, rowKey string) error {
	return t.DeleteEntityWithETag(ctx, partitionKey, rowKey, "")
}

// DeleteEntityWithETag deletes an entity with optional ETag validation.
func (t *Table) DeleteEntityWithETag(ctx context.Context, partitionKey, rowKey, ifMatch string) error {
	db := t.store.DB()
	key := dataKey(t.name, partitionKey, rowKey)

	data, closer, err := db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return ErrEntityNotFound
	}
	if err != nil {
		return err
	}

	// If ETag validation is requested, check it
	if ifMatch != "" && ifMatch != "*" {
		dataCopy := make([]byte, len(data))
		copy(dataCopy, data)
		if err := closeIO(closer); err != nil {
			return err
		}

		var entity Entity
		if err := json.Unmarshal(dataCopy, &entity); err != nil {
			return err
		}

		if entity.ETag != ifMatch {
			return ErrPreconditionFailed
		}
	} else if err := closeIO(closer); err != nil {
		return err
	}

	return db.Delete(key, pebble.NoSync)
}

// ListEntities lists all entities in the table.
func (t *Table) ListEntities(ctx context.Context) ([]*Entity, error) {
	db := t.store.DB()
	prefix := tablePrefix(t.name)
	upper := upperBoundForPrefix(prefix)

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upper,
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()

	var entities []*Entity
	for ok := iter.First(); ok; ok = iter.Next() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		var entity Entity
		if err := json.Unmarshal(iter.Value(), &entity); err != nil {
			continue
		}
		entities = append(entities, &entity)
	}

	return entities, iter.Error()
}

// QueryEntities queries entities with filters and pagination.
//
// - filter: OData-like filter string, compiled once via CompileFilter and evaluated per row.
// - top: max number of results (0 => default 1000).
// - selectFields: currently ignored here (projection is done in handler).
// - nextPK/nextRK: continuation tokens (Azure-style).
func (t *Table) QueryEntities(
	ctx context.Context,
	filter string,
	top int,
	selectFields []string,
	nextPK, nextRK string,
) (entities []*Entity, contPK string, contRK string, err error) {
	start := time.Now()
	fullScan := false
	db := t.store.DB()
	scanned := 0
	pkHint := ""
	scanReason := ""

	defer func() {
		if t.metrics != nil {
			t.metrics.ObserveQuery(QueryMetric{
				Duration:     time.Since(start),
				Returned:     len(entities),
				Scanned:      scanned,
				FullScan:     fullScan,
				Err:          err != nil,
				FilterError:  errors.Is(err, ErrInvalidFilter),
				Continuation: contPK != "" || contRK != "",
			})
		}
	}()

	// Compile the filter once, up front. This validates the filter syntax (so invalid
	// filters are rejected even on empty tables) and, critically, avoids re-parsing the
	// filter string for every scanned row — the dominant CPU cost of a large scan.
	var compiledFilter *CompiledFilter
	if filter != "" {
		compiledFilter, err = CompileFilter(filter)
		if err != nil {
			return entities, contPK, contRK, err
		}
	}

	// Derive scan bounds (and whether the bounds fully cover the filter) from the compiled
	// filter. When it is a pure AND of PartitionKey/RowKey constraints we bound the iterator
	// to exactly that key range using Azure Table Storage semantics for eq/ge/gt/le/lt.
	var plan keyQueryPlan
	if compiledFilter != nil {
		plan = compiledFilter.keyQueryPlan()
	}

	var (
		lowerBound []byte
		upperBound []byte
	)

	if plan.hasPartition {
		pkHint = plan.partitionEqual
		prefix := partitionPrefix(t.name, plan.partitionEqual)
		lowerBound = prefix
		upperBound = upperBoundForPrefix(prefix)

		switch {
		case plan.hasRowExact:
			// Exact RowKey: narrow to the single entity. The upper bound appends the key
			// separator so it includes the row itself but excludes any longer successor.
			lowerBound = dataKey(t.name, plan.partitionEqual, plan.rowExact)
			upperBound = append(dataKey(t.name, plan.partitionEqual, plan.rowExact), keySep)
		default:
			if plan.rowLow.set {
				lk := dataKey(t.name, plan.partitionEqual, plan.rowLow.value)
				if !plan.rowLow.inclusive {
					// gt: exclude the bound value itself. No valid RowKey contains 0x00, so
					// value+0x00 is the smallest key strictly greater than the value.
					lk = append(lk, keySep)
				}
				lowerBound = lk
			}
			if plan.rowHigh.set {
				hk := dataKey(t.name, plan.partitionEqual, plan.rowHigh.value)
				if plan.rowHigh.inclusive {
					// le: include the bound value, exclude its successors (e.g. "B" but not "B1").
					hk = append(hk, keySep)
				}
				// lt: the exclusive upper bound is the value's key itself.
				upperBound = hk
			}
		}

		t.log.Debug("query bounded to partition",
			"partitionKey", plan.partitionEqual,
			"rowExact", plan.hasRowExact,
			"rowLow", plan.rowLow.set,
			"rowHigh", plan.rowHigh.set,
			"coversFilter", plan.coversFilter,
			"filter", filter,
			"top", top,
		)
	} else if plan.hasPartitionRange {
		// PartitionKey range: bound the scan to the span of partitions [low, high) rather than
		// scanning the whole table. RowKey constraints (if any) can't apply across partitions,
		// so they fall to per-row filtering.
		pkHint = "range"
		lowerBound = tablePrefix(t.name)
		upperBound = upperBoundForPrefix(tablePrefix(t.name))
		if plan.partitionLow.set {
			pp := partitionPrefix(t.name, plan.partitionLow.value)
			if plan.partitionLow.inclusive {
				lowerBound = pp // ge: start at the partition (and everything after it)
			} else {
				lowerBound = upperBoundForPrefix(pp) // gt: skip past the partition itself
			}
		}
		if plan.partitionHigh.set {
			pp := partitionPrefix(t.name, plan.partitionHigh.value)
			if plan.partitionHigh.inclusive {
				upperBound = upperBoundForPrefix(pp) // le: include the whole partition
			} else {
				upperBound = pp // lt: exclude the partition
			}
		}
		t.log.Debug("query bounded to partition range",
			"partitionLow", plan.partitionLow.value,
			"partitionHigh", plan.partitionHigh.value,
			"coversFilter", plan.coversFilter,
			"filter", filter,
			"top", top,
		)
	} else {
		// No single-partition constraint: scan the whole table and rely on per-row filtering.
		fullScan = true
		prefix := tablePrefix(t.name)
		lowerBound = prefix
		upperBound = upperBoundForPrefix(prefix)

		if filter == "" {
			scanReason = "no filter provided"
		} else {
			scanReason = "filter not partition-restricted"
		}
		t.log.Debug("query using table scan",
			"reason", scanReason,
			"filter", filter,
			"top", top,
			"continuation", nextPK != "" || nextRK != "",
		)
	}

	if nextPK != "" && nextRK != "" {
		lowerBound = dataKey(t.name, nextPK, nextRK)
	}

	iterStart := time.Now()
	iter, newIterErr := db.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if newIterErr != nil {
		err = newIterErr
		return entities, contPK, contRK, err
	}
	defer func() { _ = iter.Close() }()
	t.log.Debug("iterator created",
		"elapsed", time.Since(iterStart),
		"pkHint", pkHint,
		"fullScan", fullScan,
		"scanReason", scanReason,
	)

	limit := top
	if limit <= 0 {
		limit = 1000
	}

	// Decide whether per-row filter evaluation is needed. When the scan bounds already
	// represent the entire filter (a pure PartitionKey/RowKey constraint set), every row in
	// range matches, so per-row evaluation can be skipped entirely.
	applyFilter := compiledFilter != nil && !plan.coversFilter
	if compiledFilter != nil && plan.coversFilter {
		t.log.Debug("skipping per-row filter; key bounds fully cover filter",
			"partitionKey", plan.partitionEqual)
	}

	// matchEntity reads fields from the current scan entity (cur), so the compiled filter
	// can be evaluated without allocating a map and copying every property per row.
	var cur *Entity
	matchEntity := func(field string) (interface{}, bool) {
		switch field {
		case "PartitionKey":
			return cur.PartitionKey, true
		case "RowKey":
			return cur.RowKey, true
		}
		v, ok := cur.Properties[field]
		return v, ok
	}

	scanStart := time.Now()
	count := 0

	logCompletion := func(hasMore bool) {
		elapsed := time.Since(scanStart)
		total := time.Since(start)
		logger := t.log
		level := logger.Debug
		if elapsed >= slowQueryThreshold {
			level = logger.Warn
		}
		level("query complete",
			"returned", count,
			"scanned", scanned,
			"scanTime", elapsed,
			"totalTime", total,
			"hasMore", hasMore,
			"fullScan", fullScan,
			"pkHint", pkHint,
			"scanReason", scanReason,
			"slow", elapsed >= slowQueryThreshold,
		)
	}

	for ok := iter.First(); ok; ok = iter.Next() {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			return entities, contPK, contRK, err
		default:
		}

		scanned++

		var entity Entity
		if unmarshalErr := json.Unmarshal(iter.Value(), &entity); unmarshalErr != nil {
			continue
		}

		if nextPK != "" && nextRK != "" &&
			entity.PartitionKey == nextPK &&
			entity.RowKey == nextRK {
			continue
		}

		if applyFilter {
			cur = &entity
			match, matchErr := compiledFilter.MatchGetter(matchEntity)
			if matchErr != nil {
				if errors.Is(matchErr, ErrInvalidFilter) {
					t.log.Debug("invalid filter during evaluation", "filter", filter, "error", matchErr)
				}
				err = matchErr
				return entities, contPK, contRK, err
			}
			if !match {
				continue
			}
		}

		entities = append(entities, &entity)
		count++

		if count >= limit {
			hasMore := iter.Next()
			if hasMore {
				last := entities[len(entities)-1]
				contPK = last.PartitionKey
				contRK = last.RowKey
			}

			logCompletion(hasMore)
			return entities, contPK, contRK, err
		}
	}

	logCompletion(false)
	err = iter.Error()
	return entities, contPK, contRK, err
}
