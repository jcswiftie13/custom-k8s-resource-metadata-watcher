package gwresolve

import "testing"

func TestResolveMostSpecific(t *testing.T) {
	r := New([]Gateway{
		{Name: "gw-000", Hosts: []string{"*.gw000.example.com"}},
		{Name: "gw-001", Hosts: []string{"*.gw001.example.com"}},
		{Name: "gw-broad-all", Hosts: []string{"*.example.com"}},
		{Name: "gw-exact", Hosts: []string{"exact.host.example.com"}},
	})

	cases := []struct {
		host   string
		wantGW string
		wantOK bool
	}{
		// matches both *.gw000.example.com and *.example.com -> most specific wins
		{"svc07.gw000.example.com", "gw-000", true},
		{"svc99.gw001.example.com", "gw-001", true},
		// only matches the broad wildcard
		{"direct-3.example.com", "gw-broad-all", true},
		// exact host beats the broad wildcard that also matches it
		{"exact.host.example.com", "gw-exact", true},
		// no gateway serves this
		{"nope-1.unknown.invalid", "", false},
	}
	for _, c := range cases {
		gw, ok := r.Resolve(c.host)
		if ok != c.wantOK || gw != c.wantGW {
			t.Errorf("Resolve(%q) = (%q,%v), want (%q,%v)", c.host, gw, ok, c.wantGW, c.wantOK)
		}
	}
}
