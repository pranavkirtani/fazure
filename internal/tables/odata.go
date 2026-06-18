package tables

import (
	"fmt"
	"strconv"
	"strings"
)

// ODataQuery represents parsed OData query parameters
type ODataQuery struct {
	Filter string
	Select []string
	Top    int
}

// ParseODataQuery parses OData query parameters
func ParseODataQuery(filterStr, selectStr string, top int) *ODataQuery {
	q := &ODataQuery{
		Filter: filterStr,
		Top:    top,
	}

	if selectStr != "" {
		q.Select = strings.Split(selectStr, ",")
		for i := range q.Select {
			q.Select[i] = strings.TrimSpace(q.Select[i])
		}
	}

	return q
}

// MatchesFilter checks if an entity matches the OData filter.
// Returns ErrInvalidFilter when the filter expression is unsupported or malformed,
// mirroring Azure Tables behavior of rejecting invalid $filter values.
//
// MatchesFilter compiles the filter and evaluates it once. To evaluate the same filter
// against many entities (e.g. a table or partition scan), compile it once with
// CompileFilter and reuse the *CompiledFilter — that avoids re-parsing the filter
// string for every row, which is the dominant cost of a large scan.
func MatchesFilter(filter string, entity map[string]interface{}) (bool, error) {
	compiled, err := CompileFilter(filter)
	if err != nil {
		return false, err
	}
	return compiled.Match(entity)
}

// CompiledFilter is a parsed OData $filter expression that can be evaluated against
// many entities without re-parsing the filter string each time.
type CompiledFilter struct {
	root filterNode
}

// filterNode is a node in the compiled filter tree. get returns the value for a field
// ("PartitionKey", "RowKey", or a property name) and whether that field exists.
type filterNode interface {
	match(get func(field string) (interface{}, bool)) (bool, error)
}

// CompileFilter parses an OData $filter string into a reusable CompiledFilter.
// An empty filter compiles to a matcher that accepts every entity. Syntax validation
// happens here, once, rather than per entity.
func CompileFilter(filter string) (*CompiledFilter, error) {
	root, err := compileFilterExpr(filter)
	if err != nil {
		return nil, err
	}
	return &CompiledFilter{root: root}, nil
}

// Match evaluates the compiled filter against an entity map.
func (c *CompiledFilter) Match(entity map[string]interface{}) (bool, error) {
	return c.MatchGetter(func(field string) (interface{}, bool) {
		v, ok := entity[field]
		return v, ok
	})
}

// MatchGetter evaluates the compiled filter using a field accessor, letting callers
// avoid materializing a map per entity (e.g. scanning Entity structs directly).
func (c *CompiledFilter) MatchGetter(get func(field string) (interface{}, bool)) (bool, error) {
	if c == nil || c.root == nil {
		return true, nil
	}
	return c.root.match(get)
}

// rowKeyBound is one endpoint of a RowKey range extracted from a filter.
type rowKeyBound struct {
	value     string
	inclusive bool
	set       bool
}

// keyQueryPlan describes the PartitionKey/RowKey constraints a compiled filter imposes,
// so a scan can be bounded to exactly the matching key range (matching Azure Table Storage
// semantics for eq/ge/gt/le/lt). When coversFilter is true, those constraints represent the
// ENTIRE filter, so per-row evaluation can be skipped — the bounded rows are precisely the
// matching rows.
type keyQueryPlan struct {
	// Single-partition constraint (PartitionKey eq). When set, RowKey constraints below can
	// further narrow the scan within that partition.
	partitionEqual string
	hasPartition   bool
	rowExact       string
	hasRowExact    bool
	rowLow         rowKeyBound
	rowHigh        rowKeyBound

	// Partition-range constraint (PartitionKey ge/gt/le/lt), used only when there is no
	// single-partition eq. Bounds the scan to a span of partitions. RowKey constraints can't
	// be applied across a partition range, so they fall to per-row filtering.
	hasPartitionRange bool
	partitionLow      rowKeyBound
	partitionHigh     rowKeyBound

	coversFilter bool
}

// keyQueryPlan analyzes the compiled filter for PartitionKey/RowKey constraints usable as
// scan bounds. Bounds are extracted from comparison leaves along the top-level AND spine —
// every such leaf is a necessary condition for all matching rows, so it is safe to bound on
// them even when the expression also contains non-comparison branches (e.g. an OR group, as
// in a batch-get "PartitionKey eq X and (RowKey eq r1 or RowKey eq r2 ...)"). Such residual
// branches mean the bounds are a superset, so coversFilter stays false and the per-row
// filter still runs — but the scan is still bounded to the partition instead of degrading to
// a full table scan.
func (c *CompiledFilter) keyQueryPlan() keyQueryPlan {
	plan := keyQueryPlan{}
	if c == nil || c.root == nil {
		// Empty filter matches everything; an unbounded scan covers it.
		plan.coversFilter = true
		return plan
	}

	leaves, hasResidual := collectAndLeaves(c.root)

	partitionEquals := 0
	rowExactCount := 0
	keyOnly := true
	for _, leaf := range leaves {
		switch leaf.field {
		case "PartitionKey":
			v, ok := leaf.right.(string)
			if !ok {
				keyOnly = false
				continue
			}
			switch leaf.op {
			case "eq":
				if plan.hasPartition && plan.partitionEqual != v {
					// Conflicting PartitionKey eq values: unsatisfiable. Don't bound to one
					// partition; let per-row evaluation return nothing.
					return keyQueryPlan{}
				}
				plan.partitionEqual = v
				plan.hasPartition = true
				partitionEquals++
			case "ge":
				applyRowLow(&plan.partitionLow, rowKeyBound{value: v, inclusive: true, set: true})
				plan.hasPartitionRange = true
			case "gt":
				applyRowLow(&plan.partitionLow, rowKeyBound{value: v, inclusive: false, set: true})
				plan.hasPartitionRange = true
			case "le":
				applyRowHigh(&plan.partitionHigh, rowKeyBound{value: v, inclusive: true, set: true})
				plan.hasPartitionRange = true
			case "lt":
				applyRowHigh(&plan.partitionHigh, rowKeyBound{value: v, inclusive: false, set: true})
				plan.hasPartitionRange = true
			default:
				keyOnly = false
			}
		case "RowKey":
			rv, ok := leaf.right.(string)
			if !ok {
				keyOnly = false
				continue
			}
			switch leaf.op {
			case "eq":
				if plan.hasRowExact && plan.rowExact != rv {
					rowExactCount++ // conflicting exact values; coverage disabled below
				}
				plan.rowExact = rv
				plan.hasRowExact = true
				rowExactCount++
			case "ge":
				applyRowLow(&plan.rowLow, rowKeyBound{value: rv, inclusive: true, set: true})
			case "gt":
				applyRowLow(&plan.rowLow, rowKeyBound{value: rv, inclusive: false, set: true})
			case "le":
				applyRowHigh(&plan.rowHigh, rowKeyBound{value: rv, inclusive: true, set: true})
			case "lt":
				applyRowHigh(&plan.rowHigh, rowKeyBound{value: rv, inclusive: false, set: true})
			default:
				keyOnly = false
			}
		default:
			keyOnly = false
		}
	}

	switch {
	case plan.hasPartition:
		// Single partition: the eq bound (optionally narrowed by RowKey) fully covers the
		// filter when nothing else is present. A RowKey eq combined with a range, or any
		// PartitionKey range alongside the eq, can't be expressed by the bounds alone.
		plan.coversFilter = keyOnly && !hasResidual && partitionEquals == 1 &&
			rowExactCount <= 1 && !plan.hasPartitionRange
		if plan.hasRowExact && (plan.rowLow.set || plan.rowHigh.set) {
			plan.coversFilter = false
		}
	case plan.hasPartitionRange:
		// Partition range: covered only when the partition-range comparisons are the entire
		// filter. RowKey constraints can't be applied across partitions, so their presence
		// (or any non-key residual) means per-row filtering must still run.
		plan.coversFilter = keyOnly && !hasResidual &&
			!plan.hasRowExact && !plan.rowLow.set && !plan.rowHigh.set
	}
	return plan
}

// applyRowLow keeps the more restrictive (larger / exclusive) lower bound.
func applyRowLow(cur *rowKeyBound, cand rowKeyBound) {
	if !cur.set || cand.value > cur.value ||
		(cand.value == cur.value && !cand.inclusive && cur.inclusive) {
		*cur = cand
	}
}

// applyRowHigh keeps the more restrictive (smaller / exclusive) upper bound.
func applyRowHigh(cur *rowKeyBound, cand rowKeyBound) {
	if !cur.set || cand.value < cur.value ||
		(cand.value == cur.value && !cand.inclusive && cur.inclusive) {
		*cur = cand
	}
}

// collectAndLeaves walks the top-level AND spine and returns the comparison leaves usable
// for scan bounds. hasResidual is true if any branch is something other than a comparison
// (an OR group, a string function, etc.). Such a branch can't be expressed as key bounds, so
// the bounds become a superset and per-row filtering must still run — but the comparison
// leaves along the spine are still necessary conditions for every match, so they remain safe
// to bound on (this is what keeps "PartitionKey eq X and (RowKey eq a or RowKey eq b)" a
// partition scan rather than a full table scan).
func collectAndLeaves(n filterNode) (leaves []*comparisonNode, hasResidual bool) {
	switch node := n.(type) {
	case *comparisonNode:
		return []*comparisonNode{node}, false
	case *andNode:
		l, lr := collectAndLeaves(node.left)
		r, rr := collectAndLeaves(node.right)
		return append(l, r...), lr || rr
	case alwaysTrueNode:
		return nil, false
	default:
		// OR groups, string functions, etc. — not expressible as key bounds.
		return nil, true
	}
}

// alwaysTrueNode matches every entity (empty filter).
type alwaysTrueNode struct{}

func (alwaysTrueNode) match(func(string) (interface{}, bool)) (bool, error) { return true, nil }

// andNode short-circuits: if the left side is false (or errors), the right side is not evaluated.
type andNode struct{ left, right filterNode }

func (n *andNode) match(get func(string) (interface{}, bool)) (bool, error) {
	l, err := n.left.match(get)
	if err != nil {
		return false, err
	}
	if !l {
		return false, nil
	}
	return n.right.match(get)
}

// orNode short-circuits: if the left side is true (or errors), the right side is not evaluated.
type orNode struct{ left, right filterNode }

func (n *orNode) match(get func(string) (interface{}, bool)) (bool, error) {
	l, err := n.left.match(get)
	if err != nil {
		return false, err
	}
	if l {
		return true, nil
	}
	return n.right.match(get)
}

// comparisonNode evaluates "<field> <op> <value>" with the right-hand value parsed once
// at compile time.
type comparisonNode struct {
	field string
	op    string
	right interface{}
	cmp   func(left, right interface{}) bool
}

func (n *comparisonNode) match(get func(string) (interface{}, bool)) (bool, error) {
	val, ok := get(n.field)
	if !ok {
		return false, nil
	}
	return n.cmp(val, n.right), nil
}

// startsWithNode evaluates "startswith(<property>, '<prefix>')".
type startsWithNode struct {
	property string
	prefix   string
}

func (n *startsWithNode) match(get func(string) (interface{}, bool)) (bool, error) {
	val, ok := get(n.property)
	if !ok {
		return false, nil
	}
	return strings.HasPrefix(fmt.Sprintf("%v", val), n.prefix), nil
}

// compileFilterExpr mirrors the precedence of the original recursive evaluator:
// fully-enclosing parentheses, then 'and', then 'or', then a simple comparison/function.
func compileFilterExpr(filter string) (filterNode, error) {
	if filter == "" {
		return alwaysTrueNode{}, nil
	}

	filter = strings.TrimSpace(filter)

	// Strip surrounding parentheses if they enclose the whole expression.
	if strings.HasPrefix(filter, "(") && strings.HasSuffix(filter, ")") {
		depth := 0
		balanced := true
		for i := 0; i < len(filter); i++ {
			c := filter[i]
			if c == '(' {
				depth++
			} else if c == ')' {
				depth--
				if depth == 0 && i != len(filter)-1 {
					balanced = false
					break
				}
			}
		}
		if balanced {
			return compileFilterExpr(strings.TrimSpace(filter[1 : len(filter)-1]))
		}
	}

	if andIndex := findOperatorIndex(filter, " and "); andIndex >= 0 {
		left, err := compileFilterExpr(strings.TrimSpace(filter[:andIndex]))
		if err != nil {
			return nil, err
		}
		right, err := compileFilterExpr(strings.TrimSpace(filter[andIndex+5:]))
		if err != nil {
			return nil, err
		}
		return &andNode{left: left, right: right}, nil
	}

	if orIndex := findOperatorIndex(filter, " or "); orIndex >= 0 {
		left, err := compileFilterExpr(strings.TrimSpace(filter[:orIndex]))
		if err != nil {
			return nil, err
		}
		right, err := compileFilterExpr(strings.TrimSpace(filter[orIndex+4:]))
		if err != nil {
			return nil, err
		}
		return &orNode{left: left, right: right}, nil
	}

	return compileSimpleFilter(filter)
}

// findOperatorIndex finds the index of an operator (case insensitive)
// It skips operators that are inside parentheses.
// The operator can be provided with or without surrounding spaces; this function
// tolerates either and ensures the operator is matched as a standalone token.
func findOperatorIndex(s, op string) int {
	lower := strings.ToLower(s)
	lowerOp := strings.ToLower(op)
	opLen := len(lowerOp)
	depth := 0
	for i := 0; i <= len(lower)-opLen; i++ {
		c := lower[i]
		switch c {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		}
		if depth == 0 && i+opLen <= len(lower) && lower[i:i+opLen] == lowerOp {
			// Determine surrounding character constraints.
			// If the operator string starts/ends with a space, skip checking that side
			beforeOK := true
			afterOK := true
			if !strings.HasPrefix(lowerOp, " ") {
				beforeOK = i == 0 || isSeparator(lower[i-1])
			}
			if !strings.HasSuffix(lowerOp, " ") {
				afterOK = i+opLen == len(lower) || isSeparator(lower[i+opLen])
			}
			if beforeOK && afterOK {
				return i
			}
		}
	}
	return -1
}

func isSeparator(c byte) bool {
	return c == ' ' || c == '(' || c == ')'
}

// compileSimpleFilter compiles a single comparison or supported function expression.
// Returns ErrInvalidFilter when no supported operator/function is found.
func compileSimpleFilter(filter string) (filterNode, error) {
	filter = strings.TrimSpace(filter)

	// Handle supported OData string functions, e.g. startswith(Name, 'Al').
	if node, ok, err := compileStringFunctionFilter(filter); ok {
		if err != nil {
			return nil, err
		}
		return node, nil
	}

	// Handle comparison operators: eq, ne, lt, le, gt, ge.
	operators := []struct {
		op   string
		eval func(left, right interface{}) bool
	}{
		{"eq", func(l, r interface{}) bool { return compareValues(l, r) == 0 }},
		{"ne", func(l, r interface{}) bool { return compareValues(l, r) != 0 }},
		{"lt", func(l, r interface{}) bool { return compareValues(l, r) < 0 }},
		{"le", func(l, r interface{}) bool { return compareValues(l, r) <= 0 }},
		{"gt", func(l, r interface{}) bool { return compareValues(l, r) > 0 }},
		{"ge", func(l, r interface{}) bool { return compareValues(l, r) >= 0 }},
	}

	for _, op := range operators {
		idx := findOperatorIndex(filter, " "+op.op+" ")
		if idx >= 0 {
			left := strings.TrimSpace(filter[:idx])
			rightStr := strings.TrimSpace(filter[idx+len(op.op)+2:])

			// Validate left and right are not empty
			if left == "" || rightStr == "" {
				return nil, fmt.Errorf("incomplete filter expression: %q: %w", filter, ErrInvalidFilter)
			}

			// Parse the right-side value once, at compile time.
			var right interface{}
			if strings.HasPrefix(rightStr, "'") && strings.HasSuffix(rightStr, "'") {
				// Quoted string - remove quotes
				right = rightStr[1 : len(rightStr)-1]
			} else {
				// Unquoted value - try to parse as number
				right = parseFilterValue(rightStr)
			}

			return &comparisonNode{field: left, op: op.op, right: right, cmp: op.eval}, nil
		}
	}

	return nil, fmt.Errorf("unsupported or invalid filter expression: %q: %w", filter, ErrInvalidFilter)
}

// compileStringFunctionFilter compiles supported OData string functions.
// ok reports whether the expression is a (possibly malformed) string function, mirroring
// the original evaluator's contract so the caller can distinguish "not a function" from
// "malformed function".
func compileStringFunctionFilter(filter string) (node filterNode, ok bool, err error) {
	lower := strings.ToLower(strings.TrimSpace(filter))
	if !strings.HasPrefix(lower, "startswith(") {
		return nil, false, nil
	}

	filter = strings.TrimSpace(filter)
	if !strings.HasSuffix(filter, ")") {
		return nil, true, fmt.Errorf("invalid startswith function: %q: %w", filter, ErrInvalidFilter)
	}

	argsStr := strings.TrimSpace(filter[len("startswith(") : len(filter)-1])
	args, parseErr := splitODataFunctionArgs(argsStr)
	if parseErr != nil {
		return nil, true, fmt.Errorf("invalid startswith function: %q: %w", filter, ErrInvalidFilter)
	}
	if len(args) != 2 {
		return nil, true, fmt.Errorf("invalid startswith arguments: %q: %w", filter, ErrInvalidFilter)
	}

	property := strings.TrimSpace(args[0])
	if property == "" {
		return nil, true, fmt.Errorf("invalid startswith property: %q: %w", filter, ErrInvalidFilter)
	}

	prefixLiteral := strings.TrimSpace(args[1])
	prefix, okLit := parseSingleQuotedString(prefixLiteral)
	if !okLit {
		return nil, true, fmt.Errorf("invalid startswith prefix: %q: %w", filter, ErrInvalidFilter)
	}

	return &startsWithNode{property: property, prefix: prefix}, true, nil
}

func splitODataFunctionArgs(s string) ([]string, error) {
	var args []string
	depth := 0
	inString := false
	start := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '\'':
			// OData string escaping uses doubled single quotes inside the literal.
			if inString {
				if i+1 < len(s) && s[i+1] == '\'' {
					i++
					continue
				}
				inString = false
			} else {
				inString = true
			}
		case '(':
			if !inString {
				depth++
			}
		case ')':
			if !inString {
				if depth == 0 {
					return nil, fmt.Errorf("unbalanced parentheses")
				}
				depth--
			}
		case ',':
			if !inString && depth == 0 {
				args = append(args, strings.TrimSpace(s[start:i]))
				start = i + 1
			}
		}
	}

	if inString || depth != 0 {
		return nil, fmt.Errorf("unterminated string or unbalanced parentheses")
	}

	args = append(args, strings.TrimSpace(s[start:]))
	return args, nil
}

func parseSingleQuotedString(s string) (string, bool) {
	if len(s) < 2 || !strings.HasPrefix(s, "'") || !strings.HasSuffix(s, "'") {
		return "", false
	}
	inner := s[1 : len(s)-1]
	// OData escapes single quotes by doubling them.
	inner = strings.ReplaceAll(inner, "''", "'")
	return inner, true
}

// compareValues compares two values, handling type conversion
func compareValues(left, right interface{}) int {
	// Try numeric comparison first
	leftNum, leftIsNum := toFloat64(left)
	rightNum, rightIsNum := toFloat64(right)

	if leftIsNum && rightIsNum {
		if leftNum < rightNum {
			return -1
		} else if leftNum > rightNum {
			return 1
		}
		return 0
	}

	// Fall back to string comparison
	leftStr := fmt.Sprintf("%v", left)
	rightStr := fmt.Sprintf("%v", right)

	if leftStr < rightStr {
		return -1
	} else if leftStr > rightStr {
		return 1
	}
	return 0
}

// toFloat64 attempts to convert a value to float64
func toFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	case int32:
		return float64(val), true
		// Removed string parsing case - strings should be compared as strings
		// not converted to numbers, as this causes issues with hex-encoded values
		// that contain 'e' and are incorrectly parsed as scientific notation
	}
	return 0, false
}

// parseFilterValue attempts to parse an unquoted filter value as a number.
// If parsing fails, returns the string as-is.
func parseFilterValue(s string) interface{} {
	// Try parsing as int64 first
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i
	}
	// Try parsing as float64
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	// Return as string if not a number
	return s
}

// SelectFields returns only selected fields from entity
func SelectFields(entity map[string]interface{}, fields []string) map[string]interface{} {
	if len(fields) == 0 {
		return entity
	}

	result := make(map[string]interface{})
	for _, field := range fields {
		if val, ok := entity[field]; ok {
			result[field] = val
		}
	}
	return result
}

// keyRangeHint holds extracted key filter information for PartitionKey or RowKey.
type keyRangeHint struct {
	Exact      string // Exact match value from "Key eq 'value'"
	RangeStart string // Start value from "Key ge 'value'" or "Key gt 'value'"
	RangeEnd   string // End value from "Key le 'value'" or "Key lt 'value'"
	UseRange   bool   // True if range operators were found
}

// PartitionKeyHint represents extracted partition key filter information.
type PartitionKeyHint = keyRangeHint

// extractPartitionKeyFromFilter extracts PartitionKey filter information from OData filter
// This allows optimizing queries by scanning only the relevant partition or partition range
func extractPartitionKeyFromFilter(filter string) string {
	hint := extractPartitionKeyHint(filter)
	if hint.Exact != "" {
		return hint.Exact
	}
	// For now, if we only have range queries, return the range start
	// This allows prefix optimization for range queries like "PartitionKey ge 'X' and PartitionKey le 'Y'"
	if hint.UseRange && hint.RangeStart != "" {
		return hint.RangeStart
	}
	return ""
}

// extractPartitionKeyHint extracts detailed PartitionKey filter information.
func extractPartitionKeyHint(filter string) PartitionKeyHint {
	return extractKeyRangeHint(filter, "partitionkey")
}

func extractKeyRangeHint(filter, keyPrefix string) keyRangeHint {
	hint := keyRangeHint{}

	if filter == "" {
		return hint
	}

	filter = strings.TrimSpace(filter)

	parts := strings.Split(strings.ToLower(filter), " and ")
	originalParts := splitPreservingCase(filter)

	for i, part := range parts {
		part = strings.TrimSpace(part)
		origPart := strings.TrimSpace(originalParts[i])

		if strings.HasPrefix(part, keyPrefix+" eq ") {
			idx := findOperatorIndex(origPart, " eq ")
			if idx >= 0 {
				value := strings.TrimSpace(origPart[idx+4:])
				if strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'") {
					hint.Exact = value[1 : len(value)-1]
					return hint
				}
			}
		}

		if strings.HasPrefix(part, keyPrefix+" ge ") || strings.HasPrefix(part, keyPrefix+" gt ") {
			var idx int
			if strings.HasPrefix(part, keyPrefix+" ge ") {
				idx = findOperatorIndex(origPart, " ge ")
			} else {
				idx = findOperatorIndex(origPart, " gt ")
			}
			if idx >= 0 {
				opLen := 4
				value := strings.TrimSpace(origPart[idx+opLen:])
				if strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'") {
					hint.RangeStart = value[1 : len(value)-1]
					hint.UseRange = true
				}
			}
		}

		if strings.HasPrefix(part, keyPrefix+" le ") || strings.HasPrefix(part, keyPrefix+" lt ") {
			var idx int
			if strings.HasPrefix(part, keyPrefix+" le ") {
				idx = findOperatorIndex(origPart, " le ")
			} else {
				idx = findOperatorIndex(origPart, " lt ")
			}
			if idx >= 0 {
				opLen := 4
				value := strings.TrimSpace(origPart[idx+opLen:])
				if strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'") {
					hint.RangeEnd = value[1 : len(value)-1]
					hint.UseRange = true
				}
			}
		}
	}

	return hint
}

// splitPreservingCase splits a string by " and " (case-insensitive) while preserving original case.
func splitPreservingCase(s string) []string {
	const sep = " and "
	lowerS := strings.ToLower(s)
	lowerSep := strings.ToLower(sep)

	var result []string
	start := 0
	for {
		idx := strings.Index(lowerS[start:], lowerSep)
		if idx == -1 {
			result = append(result, s[start:])
			break
		}
		result = append(result, s[start:start+idx])
		start = start + idx + len(sep)
	}
	return result
}
