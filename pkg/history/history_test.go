package history

import (
	"testing"
	"time"

	"github.com/example/metadata-exporter/pkg/collector"
	"github.com/example/metadata-exporter/pkg/config"
)

// sampleService is an unstructured Service-like object used across tests.
func sampleService() map[string]interface{} {
	return map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Service",
		"metadata": map[string]interface{}{
			"namespace":       "prod-web",
			"name":            "api",
			"uid":             "uid-123",
			"resourceVersion": "555",
			"labels":          map[string]interface{}{"app": "api", "tier": "frontend"},
		},
		"spec": map[string]interface{}{
			"clusterIP": "10.0.0.5",
			"selector":  map[string]interface{}{"app": "api", "role": "server"},
			"ports": []interface{}{
				map[string]interface{}{"port": int64(80), "name": "http"},
				map[string]interface{}{"port": int64(443), "name": "https"},
			},
		},
	}
}

func anchorLookup(obj map[string]interface{}) srcLookup {
	return func(src string) map[string]interface{} {
		switch src {
		case "", "anchor":
			return obj
		default:
			return nil
		}
	}
}

func mustCompile(t *testing.T, r config.HistoryResource) *CompiledResource {
	t.Helper()
	cr, err := Compile(r)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return cr
}

func TestColumnValue_Types(t *testing.T) {
	r := config.HistoryResource{
		Kind: "Service",
		Columns: []config.HistoryColumn{
			{Extract: config.Extract{Path: "spec.clusterIP"}, Name: "cluster_ip", Type: "String"},
			{Extract: config.Extract{Path: "spec.ports[*].port"}, Name: "ports", Type: "Array(String)"},
			{Extract: config.Extract{Path: "spec.selector"}, Name: "selector_kv", Type: "Array(String)", Encode: "kv"},
			{Extract: config.Extract{Path: "spec"}, Name: "spec_json", Type: "String", Encode: "json"},
			{Extract: config.Extract{Path: "spec.ports[0].port"}, Name: "first_port", Type: "Int64"},
			{Extract: config.Extract{Path: "missing.field"}, Name: "absent", Type: "String"},
			{Extract: config.Extract{Path: "missing.arr[*]"}, Name: "absent_arr", Type: "Array(String)"},
		},
	}
	cr := mustCompile(t, r)
	in := &Ingester{ev: collector.NewEvaluator()}
	lookup := anchorLookup(sampleService())

	get := func(name string) any {
		for i := range cr.Columns {
			if cr.Columns[i].Name == name {
				v, err := in.columnValue(&cr.Columns[i], lookup)
				if err != nil {
					t.Fatalf("columnValue(%s): %v", name, err)
				}
				return v
			}
		}
		t.Fatalf("column %s not found", name)
		return nil
	}

	if v := get("cluster_ip"); v != "10.0.0.5" {
		t.Errorf("cluster_ip = %v", v)
	}
	if v := get("ports").([]string); len(v) != 2 || v[0] != "80" || v[1] != "443" {
		t.Errorf("ports = %v", v)
	}
	// kv is sorted "k=v"
	kv := get("selector_kv").([]string)
	if len(kv) != 2 || kv[0] != "app=api" || kv[1] != "role=server" {
		t.Errorf("selector_kv = %v", kv)
	}
	if v := get("spec_json").(string); len(v) == 0 || v[0] != '{' {
		t.Errorf("spec_json not a JSON object: %v", v)
	}
	if v := get("first_port").(int64); v != 80 {
		t.Errorf("first_port = %v", v)
	}
	if v := get("absent"); v != "" {
		t.Errorf("absent String zero = %v", v)
	}
	if v := get("absent_arr").([]string); v == nil || len(v) != 0 {
		t.Errorf("absent Array(String) must be empty non-nil slice, got %#v", v)
	}
}

func TestColumnValue_OnMissingAndConstant(t *testing.T) {
	om := "unknown"
	cr := mustCompile(t, config.HistoryResource{
		Kind: "Service",
		Columns: []config.HistoryColumn{
			{Name: "team", Type: "String", Extract: config.Extract{Path: `metadata.labels["team"]`, OnMissing: &om}},
			{Name: "app", Type: "String", Extract: config.Extract{Path: `metadata.labels["app"]`, OnMissing: &om}},
			{Name: "source", Type: "String", Value: "metadata-exporter"},
			{Name: "missing_no_default", Type: "String", Extract: config.Extract{Path: "missing.field"}},
		},
	})
	in := &Ingester{ev: collector.NewEvaluator()}
	lookup := anchorLookup(sampleService())

	get := func(name string) any {
		for i := range cr.Columns {
			if cr.Columns[i].Name == name {
				v, err := in.columnValue(&cr.Columns[i], lookup)
				if err != nil {
					t.Fatalf("columnValue(%s): %v", name, err)
				}
				return v
			}
		}
		t.Fatalf("column %s not found", name)
		return nil
	}

	if v := get("team"); v != "unknown" {
		t.Errorf("team onMissing = %v, want unknown", v)
	}
	if v := get("app"); v != "api" {
		t.Errorf("app present = %v, want api", v)
	}
	if v := get("source"); v != "metadata-exporter" {
		t.Errorf("constant = %v", v)
	}
	if v := get("missing_no_default"); v != "" {
		t.Errorf("no onMissing zero = %v", v)
	}
}

func TestCompileAll_InjectsConstants(t *testing.T) {
	t.Setenv("CLUSTER_NAME", "prod-a")
	rs, err := CompileAll(config.History{
		Constants: []config.HistoryConstant{
			{Name: "cluster_name", Type: "String", ValueEnv: "CLUSTER_NAME"},
		},
		Resources: []config.HistoryResource{{
			Kind:  "Service",
			Table: "svc_versions",
			Columns: []config.HistoryColumn{
				{Extract: config.Extract{Path: "spec.clusterIP"}, Name: "cluster_ip", Type: "String"},
				{Name: "source", Type: "String", Value: "exporter"},
			},
		}},
	})
	if err != nil {
		t.Fatalf("CompileAll: %v", err)
	}
	if len(rs) != 1 || len(rs[0].Columns) != 3 {
		t.Fatalf("columns = %+v", rs[0].Columns)
	}
	if rs[0].Columns[0].Name != "cluster_name" || !rs[0].Columns[0].IsConstant || rs[0].Columns[0].Constant != "prod-a" {
		t.Fatalf("injected constant = %+v", rs[0].Columns[0])
	}
	if rs[0].Columns[1].Name != "cluster_ip" || rs[0].Columns[1].IsConstant {
		t.Fatalf("path column = %+v", rs[0].Columns[1])
	}
	if rs[0].Columns[2].Constant != "exporter" {
		t.Fatalf("per-resource constant = %+v", rs[0].Columns[2])
	}
}

// ScopeValue feeds store.Options.Scope: the resolved constant is the writer's
// identity in a multi-writer shared table.
func TestScopeValue(t *testing.T) {
	t.Setenv("CLUSTER_NAME", "cluster-a")
	rs, err := CompileAll(config.History{
		Constants: []config.HistoryConstant{
			{Name: "cluster", Type: "String", ValueEnv: "CLUSTER_NAME"},
		},
		Resources: []config.HistoryResource{{
			Kind:  "Service",
			Table: "svc_versions",
			Columns: []config.HistoryColumn{
				{Extract: config.Extract{Path: "spec.clusterIP"}, Name: "cluster_ip", Type: "String"},
			},
		}},
	})
	if err != nil {
		t.Fatalf("CompileAll: %v", err)
	}

	v, err := ScopeValue(rs, "cluster")
	if err != nil || v != "cluster-a" {
		t.Fatalf("ScopeValue = %q, %v; want cluster-a", v, err)
	}
	if v, err := ScopeValue(rs, ""); err != nil || v != "" {
		t.Fatalf("empty column must disable scoping, got %q, %v", v, err)
	}
	if _, err := ScopeValue(rs, "no_such"); err == nil {
		t.Fatal("unknown scope column must error")
	}
	// A path column (non-constant) is not a valid identity.
	if _, err := ScopeValue(rs, "cluster_ip"); err == nil {
		t.Fatal("non-constant scope column must error")
	}
}

func TestFilters_Ops(t *testing.T) {
	obj := sampleService()
	ev := collector.NewEvaluator()
	cases := []struct {
		name   string
		filter config.HistoryFilter
		want   bool
	}{
		{"equals hit", config.HistoryFilter{Extract: config.Extract{Path: "metadata.namespace"}, Op: "equals", Value: "prod-web"}, true},
		{"equals miss", config.HistoryFilter{Extract: config.Extract{Path: "metadata.namespace"}, Op: "equals", Value: "staging"}, false},
		{"notEquals", config.HistoryFilter{Extract: config.Extract{Path: "metadata.namespace"}, Op: "notEquals", Value: "staging"}, true},
		{"prefix", config.HistoryFilter{Extract: config.Extract{Path: "metadata.namespace"}, Op: "prefix", Value: "prod-"}, true},
		{"suffix", config.HistoryFilter{Extract: config.Extract{Path: "metadata.namespace"}, Op: "suffix", Value: "-web"}, true},
		{"contains", config.HistoryFilter{Extract: config.Extract{Path: "metadata.namespace"}, Op: "contains", Value: "od-w"}, true},
		{"exists", config.HistoryFilter{Extract: config.Extract{Path: "spec.clusterIP"}, Op: "exists"}, true},
		{"exists miss", config.HistoryFilter{Extract: config.Extract{Path: "spec.nope"}, Op: "exists"}, false},
		{"in hit", config.HistoryFilter{Extract: config.Extract{Path: `metadata.labels["app"]`}, Op: "in", Values: []string{"api", "web"}}, true},
		{"in miss", config.HistoryFilter{Extract: config.Extract{Path: `metadata.labels["app"]`}, Op: "in", Values: []string{"web"}}, false},
		{"regex hit", config.HistoryFilter{Extract: config.Extract{Path: "metadata.namespace"}, Op: "regex", Value: "^(prod|staging)-"}, true},
		{"regex miss", config.HistoryFilter{Extract: config.Extract{Path: "metadata.namespace"}, Op: "regex", Value: "^dev-"}, false},
		{"negate", config.HistoryFilter{Extract: config.Extract{Path: "metadata.namespace"}, Op: "equals", Value: "staging", Negate: true}, true},
		// array match-any: any port equals 443
		{"array in", config.HistoryFilter{Extract: config.Extract{Path: "spec.ports[*].port"}, Op: "in", Values: []string{"443"}}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cr := mustCompile(t, config.HistoryResource{
				Kind:    "Service",
				Columns: []config.HistoryColumn{{Extract: config.Extract{Path: "metadata.name"}, Name: "name", Type: "String"}},
				Filters: []config.HistoryFilter{tc.filter},
			})
			if got := cr.Passes(ev, anchorLookup(obj)); got != tc.want {
				t.Fatalf("Passes = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestFilters_AndSemanticsAndRegexOrdering(t *testing.T) {
	// Two filters: a cheap equals that FAILS and a regex. Compile must place the
	// regex last, and Passes must short-circuit on the failing cheap filter.
	cr := mustCompile(t, config.HistoryResource{
		Kind:    "Service",
		Columns: []config.HistoryColumn{{Extract: config.Extract{Path: "metadata.name"}, Name: "name", Type: "String"}},
		Filters: []config.HistoryFilter{
			{Extract: config.Extract{Path: "metadata.namespace"}, Op: "regex", Value: "^prod-"},
			{Extract: config.Extract{Path: "metadata.name"}, Op: "equals", Value: "does-not-match"},
		},
	})
	if cr.Filters[0].Leaves[0].isRegex {
		t.Fatal("regex filter must be ordered last, not first")
	}
	if cr.Filters[len(cr.Filters)-1].Leaves[0].Regex == nil {
		t.Fatal("last filter should be the regex")
	}
	if cr.Passes(collector.NewEvaluator(), anchorLookup(sampleService())) {
		t.Fatal("AND semantics: a failing filter must reject the object")
	}
}

func TestFilters_AnyOf(t *testing.T) {
	obj := sampleService() // namespace=prod-web, labels{app:api, tier:frontend}
	ev := collector.NewEvaluator()
	prefixProd := config.HistoryFilter{Extract: config.Extract{Path: "metadata.namespace"}, Op: "prefix", Value: "prod-"}
	tierCritical := config.HistoryFilter{Extract: config.Extract{Path: `metadata.labels["tier"]`}, Op: "equals", Value: "critical"}
	nsStaging := config.HistoryFilter{Extract: config.Extract{Path: "metadata.namespace"}, Op: "equals", Value: "staging"}

	cases := []struct {
		name    string
		filters []config.HistoryFilter
		want    bool
	}{
		{"first child passes", []config.HistoryFilter{{AnyOf: []config.HistoryFilter{prefixProd, tierCritical}}}, true},
		{"second child passes", []config.HistoryFilter{{AnyOf: []config.HistoryFilter{nsStaging, prefixProd}}}, true},
		{"all children fail", []config.HistoryFilter{{AnyOf: []config.HistoryFilter{nsStaging, tierCritical}}}, false},
		{"anyOf AND failing leaf", []config.HistoryFilter{
			{AnyOf: []config.HistoryFilter{prefixProd, tierCritical}},
			nsStaging,
		}, false},
		{"anyOf AND passing leaf", []config.HistoryFilter{
			{AnyOf: []config.HistoryFilter{nsStaging, prefixProd}},
			{Extract: config.Extract{Path: "metadata.name"}, Op: "equals", Value: "api"},
		}, true},
		{"negated child inside anyOf", []config.HistoryFilter{
			{AnyOf: []config.HistoryFilter{
				nsStaging,
				{Extract: config.Extract{Path: `metadata.labels["tier"]`}, Op: "equals", Value: "critical", Negate: true},
			}},
		}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cr := mustCompile(t, config.HistoryResource{
				Kind:    "Service",
				Columns: []config.HistoryColumn{{Extract: config.Extract{Path: "metadata.name"}, Name: "name", Type: "String"}},
				Filters: tc.filters,
			})
			if got := cr.Passes(ev, anchorLookup(obj)); got != tc.want {
				t.Fatalf("Passes = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestFilters_AnyOfRegexOrdering(t *testing.T) {
	// A regex-bearing anyOf group and a plain equals leaf: the plain group must
	// come first, the regex group last, and within the group the regex leaf last.
	cr := mustCompile(t, config.HistoryResource{
		Kind:    "Service",
		Columns: []config.HistoryColumn{{Extract: config.Extract{Path: "metadata.name"}, Name: "name", Type: "String"}},
		Filters: []config.HistoryFilter{
			{AnyOf: []config.HistoryFilter{
				{Extract: config.Extract{Path: "metadata.namespace"}, Op: "regex", Value: "^prod-"},
				{Extract: config.Extract{Path: "metadata.name"}, Op: "equals", Value: "api"},
			}},
			{Extract: config.Extract{Path: "metadata.name"}, Op: "equals", Value: "api"},
		},
	})
	if len(cr.Filters) != 2 {
		t.Fatalf("groups = %d, want 2", len(cr.Filters))
	}
	if cr.Filters[0].hasRegex || len(cr.Filters[0].Leaves) != 1 {
		t.Fatalf("plain leaf group must be first, got %+v", cr.Filters[0])
	}
	g := cr.Filters[1]
	if !g.hasRegex || len(g.Leaves) != 2 {
		t.Fatalf("regex-bearing group must be last, got %+v", g)
	}
	if g.Leaves[0].isRegex || !g.Leaves[len(g.Leaves)-1].isRegex {
		t.Fatal("within a group the regex leaf must be ordered last")
	}
	if !cr.Passes(collector.NewEvaluator(), anchorLookup(sampleService())) {
		t.Fatal("object should pass both groups")
	}
}

func TestColumnHash(t *testing.T) {
	base := map[string]any{
		"cluster_ip": "10.0.0.1",
		"ports":      []string{"80", "443"},
		"replicas":   int64(3),
	}
	h1, err := ColumnHash(base)
	if err != nil {
		t.Fatal(err)
	}

	// Key order is irrelevant — equal values hash equal.
	same := map[string]any{
		"replicas":   int64(3),
		"ports":      []string{"80", "443"},
		"cluster_ip": "10.0.0.1",
	}
	if h2, _ := ColumnHash(same); h1 != h2 {
		t.Fatalf("hash changed for equal values: %s vs %s", h1, h2)
	}

	// A declared-column change flips the hash.
	changed := map[string]any{
		"cluster_ip": "10.0.0.2",
		"ports":      []string{"80", "443"},
		"replicas":   int64(3),
	}
	if h3, _ := ColumnHash(changed); h1 == h3 {
		t.Fatal("hash did not change on a declared-column change")
	}

	// Type tags prevent cross-type collisions: string "3" != int64(3).
	hStr, _ := ColumnHash(map[string]any{"replicas": "3"})
	hInt, _ := ColumnHash(map[string]any{"replicas": int64(3)})
	if hStr == hInt {
		t.Fatal("string and int column values must not collide")
	}

	// Slice-element boundaries matter: ["a","b"] != ["ab"].
	hAB, _ := ColumnHash(map[string]any{"x": []string{"a", "b"}})
	hCat, _ := ColumnHash(map[string]any{"x": []string{"ab"}})
	if hAB == hCat {
		t.Fatal("array element boundaries must be preserved in the hash")
	}

	// Time hashes at millisecond precision (matches DateTime64(3) readback), so
	// a sub-millisecond difference — which ClickHouse truncates away — must not
	// change the hash.
	base2 := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	hA, _ := ColumnHash(map[string]any{"t": base2})
	hB, _ := ColumnHash(map[string]any{"t": base2.Add(500 * time.Microsecond)})
	if hA != hB {
		t.Fatal("sub-millisecond time difference must not change the hash")
	}
}

func TestTableSchema_Mapping(t *testing.T) {
	cr := mustCompile(t, config.HistoryResource{
		Kind:  "Service",
		Table: "svc_versions",
		Columns: []config.HistoryColumn{
			{Extract: config.Extract{Path: "spec.clusterIP"}, Name: "cluster_ip", Type: "String"},
			{Extract: config.Extract{Path: "spec.selector"}, Name: "selector_kv", Type: "Array(String)", Encode: "kv", Index: "bloom_filter"},
		},
	})
	ts := cr.TableSchema()
	if ts.Table != "svc_versions" || len(ts.Columns) != 2 {
		t.Fatalf("unexpected schema: %+v", ts)
	}
	if ts.Columns[1].Index != "bloom_filter" {
		t.Fatalf("index not carried: %+v", ts.Columns[1])
	}
}
