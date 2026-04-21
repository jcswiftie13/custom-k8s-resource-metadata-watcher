package sink

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestPrometheusSink_RegisterAndUpsert(t *testing.T) {
	reg := prometheus.NewRegistry()
	s := NewPrometheusSink(reg)
	if err := s.RegisterRule(RuleSchema{
		Name:   "custom_pod_info",
		Help:   "test",
		Labels: []string{"namespace", "pod"},
	}); err != nil {
		t.Fatal(err)
	}
	s.Upsert("custom_pod_info", "ns/p", map[string]string{"namespace": "ns", "pod": "p"})

	want := `
# HELP custom_pod_info test
# TYPE custom_pod_info gauge
custom_pod_info{namespace="ns",pod="p"} 1
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "custom_pod_info"); err != nil {
		t.Fatal(err)
	}
}

func TestPrometheusSink_ReplaceForAnchorShrinks(t *testing.T) {
	reg := prometheus.NewRegistry()
	s := NewPrometheusSink(reg)
	_ = s.RegisterRule(RuleSchema{
		Name:   "m",
		Help:   "h",
		Labels: []string{"pod", "container"},
	})

	// First reconcile: two containers.
	s.ReplaceForAnchor("m", "ns/p", map[string]map[string]string{
		"ns/p|a": {"pod": "p", "container": "a"},
		"ns/p|b": {"pod": "p", "container": "b"},
	})

	// Second reconcile: only one container remains.
	s.ReplaceForAnchor("m", "ns/p", map[string]map[string]string{
		"ns/p|a": {"pod": "p", "container": "a"},
	})

	want := `
# HELP m h
# TYPE m gauge
m{container="a",pod="p"} 1
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "m"); err != nil {
		t.Fatal(err)
	}
}

func TestPrometheusSink_ReplaceForAnchorClearsOnNil(t *testing.T) {
	reg := prometheus.NewRegistry()
	s := NewPrometheusSink(reg)
	_ = s.RegisterRule(RuleSchema{
		Name:   "m",
		Help:   "h",
		Labels: []string{"pod"},
	})
	s.ReplaceForAnchor("m", "ns/p", map[string]map[string]string{
		"ns/p": {"pod": "p"},
	})
	s.ReplaceForAnchor("m", "ns/p", nil)

	want := ``
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "m"); err != nil {
		t.Fatal(err)
	}
}

func TestPrometheusSink_Delete(t *testing.T) {
	reg := prometheus.NewRegistry()
	s := NewPrometheusSink(reg)
	_ = s.RegisterRule(RuleSchema{
		Name:   "m",
		Help:   "h",
		Labels: []string{"pod"},
	})
	s.Upsert("m", "k", map[string]string{"pod": "p"})
	s.Delete("m", "k")
	if err := testutil.GatherAndCompare(reg, strings.NewReader(""), "m"); err != nil {
		t.Fatal(err)
	}
}

func TestPrometheusSink_DuplicateRegisterFails(t *testing.T) {
	reg := prometheus.NewRegistry()
	s := NewPrometheusSink(reg)
	schema := RuleSchema{Name: "dup", Help: "h", Labels: []string{"a"}}
	if err := s.RegisterRule(schema); err != nil {
		t.Fatal(err)
	}
	if err := s.RegisterRule(schema); err == nil {
		t.Fatal("expected duplicate registration error")
	}
}
