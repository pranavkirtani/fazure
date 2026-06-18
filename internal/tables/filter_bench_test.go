package tables

import "testing"

// BenchmarkFilterPerRow quantifies the per-row cost difference between the old behavior
// (re-parsing the filter string for every entity, via MatchesFilter) and the new behavior
// (CompileFilter once, then evaluate the compiled form per entity). The merkle leaf scan
// uses exactly this shape of filter against many rows, so the ratio here is roughly the
// per-row CPU saved on a large partition scan.
func BenchmarkFilterPerRow(b *testing.B) {
	filter := "PartitionKey eq 'connector-a/accounts/0' and RowKey ge '00000000' and RowKey le '00010000'"
	entity := map[string]interface{}{
		"PartitionKey": "connector-a/accounts/0",
		"RowKey":       "00005000",
		"Name":         "account-5000",
		"Status":       "active",
	}

	b.Run("ReparseEachRow", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := MatchesFilter(filter, entity); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("CompiledOnce", func(b *testing.B) {
		compiled, err := CompileFilter(filter)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := compiled.Match(entity); err != nil {
				b.Fatal(err)
			}
		}
	})
}
