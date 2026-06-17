package tables

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestQueryEntitiesMatchesBruteForceFilter is a conformance test for the compiled-filter
// fast path in QueryEntities (compile-once + the "PartitionKey eq" skip shortcut).
//
// For every filter it asserts that the result set returned by QueryEntities (paged, with
// key-range bounds and the per-row skip optimization) is identical to a brute-force
// reference: ListEntities() followed by evaluating MatchesFilter on every entity. Any
// divergence — e.g. the skip shortcut returning rows a filter would exclude, or loose
// key bounds leaking rows — fails here.
func TestQueryEntitiesMatchesBruteForceFilter(t *testing.T) {
	ts, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	const tableName = "conformancetable"
	require.NoError(t, ts.CreateTable(ctx, tableName))

	table, err := ts.GetTable(ctx, tableName)
	require.NoError(t, err)

	// Seed entities across a few partitions with zero-padded, lexicographically ordered
	// RowKeys (so RowKey ranges behave like the fixed-width keys mesh-core's merkle uses),
	// plus properties to exercise non-key predicates and string functions.
	partitions := []string{"p1", "p2", "p3"}
	const perPartition = 25
	for _, pk := range partitions {
		for i := 0; i < perPartition; i++ {
			rk := fmt.Sprintf("%04d", i)
			status := "active"
			if i%3 == 0 {
				status = "inactive"
			}
			props := map[string]interface{}{
				"Status": status,
				"Name":   fmt.Sprintf("%s-%04d", pk, i),
				"Seq":    int64(i),
			}
			_, insErr := table.InsertEntity(ctx, pk, rk, props)
			require.NoError(t, insErr)
		}
	}

	// Partition "pv" uses variable-length RowKeys where a value is a prefix of others
	// ("B" vs "B1"/"BA"). This is the case where loose prefix bounds leak: "RowKey le 'B'"
	// must return "B" but not "B1"/"BA". Because these filters are fully key-covered, the
	// fast path skips per-row evaluation and relies solely on the bounds, so any divergence
	// from brute-force MatchesFilter here is a bounds-tightness bug.
	for _, rk := range []string{"A", "B", "B1", "BA", "C"} {
		_, insErr := table.InsertEntity(ctx, "pv", rk, map[string]interface{}{"Name": "pv-" + rk})
		require.NoError(t, insErr)
	}

	filters := []string{
		"",                                       // no filter (full scan)
		"PartitionKey eq 'p2'",                   // exercises the skip shortcut
		"PartitionKey eq 'nope'",                 // empty partition
		"PartitionKey eq 'p2' and RowKey ge '0005' and RowKey le '0010'", // merkle-like range (inclusive)
		"PartitionKey eq 'p2' and RowKey gt '0005' and RowKey lt '0010'", // exclusive both ends
		"PartitionKey eq 'p2' and RowKey ge '0005' and RowKey lt '0008'", // mixed inclusivity
		"PartitionKey eq 'p1' and RowKey le '0009'",                      // upper bound only
		"PartitionKey eq 'p1' and RowKey ge '0020'",                      // lower bound only
		"PartitionKey eq 'p1' and RowKey eq '0007'",                      // point lookup
		"PartitionKey eq 'p2' and (RowKey eq '0003' or RowKey eq '0007' or RowKey eq '0011')", // batch-get shape (must stay partition-bounded)
		"PartitionKey eq 'p1' and Status eq 'active'",                    // compound, NOT covered by prefix
		"PartitionKey eq 'pv' and RowKey le 'B'",                         // loose-bound leak: excludes B1/BA
		"PartitionKey eq 'pv' and RowKey lt 'B'",                         // exclusive: excludes B itself
		"PartitionKey eq 'pv' and RowKey ge 'B'",                         // includes B/B1/BA/C
		"PartitionKey eq 'pv' and RowKey gt 'B'",                         // excludes B, includes B1/BA/C
		"PartitionKey ge 'p2'",                                          // partition range: lower only
		"PartitionKey lt 'p3'",                                          // partition range: upper only
		"PartitionKey ge 'p1' and PartitionKey lt 'p3'",                 // partition range: both, inclusive/exclusive
		"PartitionKey gt 'p1' and PartitionKey le 'p3'",                 // partition range: exclusive/inclusive
		"PartitionKey ge 'p2' and RowKey ge '0010'",                     // partition range + RowKey residual (per-row)
		"PartitionKey ge 'p1' and PartitionKey lt 'p3' and Status eq 'active'", // partition range + non-key residual
		"Status eq 'active'",                                             // non-key full scan
		"Status ne 'active'",                                             // ne
		"RowKey gt '0010'",                                               // key range, no partition
		"RowKey ge '0005' and RowKey le '0008'",                          // range across partitions
		"startswith(Name, 'p2-001')",                                     // string function
		"Status eq 'active' or PartitionKey eq 'p3'",                     // or
		"Seq ge 20",                                                      // numeric comparison
	}

	for _, filter := range filters {
		t.Run(fmt.Sprintf("filter=%q", filter), func(t *testing.T) {
			fast := drainQuery(t, table, filter)
			brute := bruteForce(t, table, filter)
			require.Equal(t, brute, fast, "fast path diverged from brute-force MatchesFilter for filter %q", filter)
		})
	}
}

// drainQuery pages through QueryEntities with a small page size to exercise continuation
// tokens, and returns the sorted set of "PK|RK" keys it yielded.
func drainQuery(t *testing.T, table *Table, filter string) []string {
	t.Helper()
	ctx := context.Background()

	const pageSize = 7
	var (
		keys           []string
		nextPK, nextRK string
		seen           = map[string]bool{}
	)
	for {
		page, cPK, cRK, err := table.QueryEntities(ctx, filter, pageSize, nil, nextPK, nextRK)
		require.NoError(t, err)
		for _, e := range page {
			key := e.PartitionKey + "|" + e.RowKey
			require.Falsef(t, seen[key], "entity %q returned more than once across pages", key)
			seen[key] = true
			keys = append(keys, key)
		}
		if cPK == "" && cRK == "" {
			break
		}
		nextPK, nextRK = cPK, cRK
	}
	sort.Strings(keys)
	return keys
}

// bruteForce computes the reference result: every entity in the table that MatchesFilter
// accepts, as a sorted set of "PK|RK" keys.
func bruteForce(t *testing.T, table *Table, filter string) []string {
	t.Helper()
	ctx := context.Background()

	all, err := table.ListEntities(ctx)
	require.NoError(t, err)

	var keys []string
	for _, e := range all {
		entityMap := map[string]interface{}{
			"PartitionKey": e.PartitionKey,
			"RowKey":       e.RowKey,
		}
		for k, v := range e.Properties {
			entityMap[k] = v
		}
		match, matchErr := MatchesFilter(filter, entityMap)
		require.NoError(t, matchErr)
		if match {
			keys = append(keys, e.PartitionKey+"|"+e.RowKey)
		}
	}
	sort.Strings(keys)
	return keys
}

// TestKeyQueryPlanBounding guards scan bounding (not just result correctness). The
// regression that motivated this: a batch-get filter "PartitionKey eq X and (RowKey eq a or
// RowKey eq b)" must stay bounded to partition X, NOT degrade to a full table scan. It also
// pins which shapes are fully covered (per-row filter skipped) vs only bounded.
func TestKeyQueryPlanBounding(t *testing.T) {
	cases := []struct {
		filter            string
		hasPartition      bool
		hasPartitionRange bool
		coversFilter      bool
	}{
		{"PartitionKey eq 'X'", true, false, true},
		{"PartitionKey eq 'X' and RowKey eq 'r'", true, false, true},
		{"PartitionKey eq 'X' and RowKey ge 'a' and RowKey le 'b'", true, false, true},
		// Batch-get: bounded to the partition, but the OR keeps it from being fully covered.
		{"PartitionKey eq 'X' and (RowKey eq 'a' or RowKey eq 'b')", true, false, false},
		// Partition + non-key predicate: bounded, not covered.
		{"PartitionKey eq 'X' and Status eq 'active'", true, false, false},
		// Partition ranges: bounded to a span of partitions; pure range is fully covered.
		{"PartitionKey ge 'X'", false, true, true},
		{"PartitionKey ge 'X' and PartitionKey lt 'Y'", false, true, true},
		{"PartitionKey gt 'X' and PartitionKey le 'Y'", false, true, true},
		// Partition range + RowKey: range-bounded, RowKey is per-row residual.
		{"PartitionKey ge 'X' and RowKey ge 'a'", false, true, false},
		// No single-partition constraint: cannot bound.
		{"Status eq 'active'", false, false, false},
		{"PartitionKey eq 'X' or PartitionKey eq 'Y'", false, false, false},
	}

	for _, tc := range cases {
		t.Run(tc.filter, func(t *testing.T) {
			compiled, err := CompileFilter(tc.filter)
			require.NoError(t, err)
			plan := compiled.keyQueryPlan()
			require.Equal(t, tc.hasPartition, plan.hasPartition, "hasPartition for %q", tc.filter)
			require.Equal(t, tc.hasPartitionRange, plan.hasPartitionRange, "hasPartitionRange for %q", tc.filter)
			require.Equal(t, tc.coversFilter, plan.coversFilter, "coversFilter for %q", tc.filter)
		})
	}
}
