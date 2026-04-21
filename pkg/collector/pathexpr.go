package collector

import (
	"fmt"
	"strings"
	"unicode"
)

// pathSegment represents a single step of a parsed path expression.
type pathSegment struct {
	// name is set for map accesses ("foo", "kind", "argocd.argoproj.io/tracking-id").
	name string

	// kind is one of: "field", "index", "wildcard".
	kind string

	// index is used when kind == "index".
	index int
}

// parsedPath is a compiled representation of a user-supplied path expression
// such as `metadata.annotations["argocd.argoproj.io/tracking-id"]` or
// `spec.containers[*].image`.
type parsedPath struct {
	segments []pathSegment
}

// parsePath compiles a path expression. It is intentionally small: it only
// supports the syntax we need and documents exactly what users may write.
//
// Grammar (EBNF-ish):
//
//	path       := (field | subscript) ("." field | subscript)*
//	field      := ident           # e.g. metadata, namespace, kind
//	subscript  := "[" (integer | "*" | quoted-string) "]"
//	ident      := [A-Za-z_][A-Za-z0-9_]*
//	quoted     := '"' any-char* '"' | "'" any-char* "'"
func parsePath(expr string) (*parsedPath, error) {
	p := strings.TrimSpace(expr)
	// Allow a leading `{...}` wrap for friendliness with kubectl JSONPath users.
	if strings.HasPrefix(p, "{") && strings.HasSuffix(p, "}") {
		p = strings.TrimSpace(p[1 : len(p)-1])
	}
	p = strings.TrimPrefix(p, ".")
	p = strings.TrimPrefix(p, "$.")

	if p == "" {
		return nil, fmt.Errorf("empty path")
	}

	pp := &parsedPath{}
	i := 0
	expectFieldOrSubscript := true
	for i < len(p) {
		ch := p[i]
		switch {
		case ch == '.':
			if expectFieldOrSubscript {
				return nil, fmt.Errorf("unexpected '.' in path %q at position %d", expr, i)
			}
			expectFieldOrSubscript = true
			i++
		case ch == '[':
			expectFieldOrSubscript = false
			end := strings.IndexByte(p[i:], ']')
			if end < 0 {
				return nil, fmt.Errorf("unterminated [ in path %q", expr)
			}
			end += i
			token := strings.TrimSpace(p[i+1 : end])
			seg, err := parseSubscript(token, expr)
			if err != nil {
				return nil, err
			}
			pp.segments = append(pp.segments, seg)
			i = end + 1
		case isIdentStart(rune(ch)):
			expectFieldOrSubscript = false
			start := i
			for i < len(p) && isIdentPart(rune(p[i])) {
				i++
			}
			pp.segments = append(pp.segments, pathSegment{kind: "field", name: p[start:i]})
		default:
			return nil, fmt.Errorf("unexpected character %q at position %d in path %q", ch, i, expr)
		}
	}
	if len(pp.segments) == 0 {
		return nil, fmt.Errorf("empty path %q", expr)
	}
	return pp, nil
}

func parseSubscript(token, full string) (pathSegment, error) {
	if token == "*" {
		return pathSegment{kind: "wildcard"}, nil
	}
	if len(token) >= 2 && (token[0] == '"' || token[0] == '\'') && token[len(token)-1] == token[0] {
		return pathSegment{kind: "field", name: token[1 : len(token)-1]}, nil
	}
	var n int
	if _, err := fmt.Sscanf(token, "%d", &n); err == nil {
		return pathSegment{kind: "index", index: n}, nil
	}
	return pathSegment{}, fmt.Errorf("invalid subscript %q in path %q (expected number, *, or quoted string)", token, full)
}

func isIdentStart(r rune) bool {
	return r == '_' || unicode.IsLetter(r)
}

func isIdentPart(r rune) bool {
	return r == '_' || r == '-' || unicode.IsLetter(r) || unicode.IsDigit(r)
}

// evaluate walks a parsedPath over unstructured input. It returns a slice of
// raw values (maps, slices or scalars). Missing keys or type mismatches yield
// an empty slice, not an error.
func (pp *parsedPath) evaluate(input interface{}) []interface{} {
	frontier := []interface{}{input}
	for _, seg := range pp.segments {
		next := make([]interface{}, 0, len(frontier))
		for _, cur := range frontier {
			next = append(next, applySegment(cur, seg)...)
		}
		frontier = next
		if len(frontier) == 0 {
			return nil
		}
	}
	return frontier
}

func applySegment(cur interface{}, seg pathSegment) []interface{} {
	switch seg.kind {
	case "field":
		if m, ok := cur.(map[string]interface{}); ok {
			if v, has := m[seg.name]; has && v != nil {
				return []interface{}{v}
			}
		}
		return nil
	case "index":
		if s, ok := cur.([]interface{}); ok {
			if seg.index >= 0 && seg.index < len(s) {
				return []interface{}{s[seg.index]}
			}
		}
		return nil
	case "wildcard":
		if s, ok := cur.([]interface{}); ok {
			return append([]interface{}{}, s...)
		}
		if m, ok := cur.(map[string]interface{}); ok {
			out := make([]interface{}, 0, len(m))
			for _, v := range m {
				out = append(out, v)
			}
			return out
		}
		return nil
	}
	return nil
}
