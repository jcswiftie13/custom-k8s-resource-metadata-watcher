package history

import (
	"testing"

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
	if cr.Filters[0].isRegex {
		t.Fatal("regex filter must be ordered last, not first")
	}
	if cr.Filters[len(cr.Filters)-1].Regex == nil {
		t.Fatal("last filter should be the regex")
	}
	if cr.Passes(collector.NewEvaluator(), anchorLookup(sampleService())) {
		t.Fatal("AND semantics: a failing filter must reject the object")
	}
}

func TestSpecHash_StableAndVolatileInsensitive(t *testing.T) {
	a := sampleService()
	b := sampleService()
	// change only volatile fields
	b["metadata"].(map[string]interface{})["resourceVersion"] = "999"
	b["metadata"].(map[string]interface{})["generation"] = int64(7)

	ha, err := SpecHash(a)
	if err != nil {
		t.Fatal(err)
	}
	hb, err := SpecHash(b)
	if err != nil {
		t.Fatal(err)
	}
	if ha != hb {
		t.Fatalf("hash changed on volatile-only diff: %s vs %s", ha, hb)
	}

	// a real spec change must change the hash
	c := sampleService()
	c["spec"].(map[string]interface{})["clusterIP"] = "10.0.0.9"
	hc, _ := SpecHash(c)
	if ha == hc {
		t.Fatal("hash did not change on spec change")
	}

	// SpecHash must not mutate the input (shared informer object)
	if _, ok := a["metadata"].(map[string]interface{})["resourceVersion"]; !ok {
		t.Fatal("SpecHash mutated the input object's metadata")
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
