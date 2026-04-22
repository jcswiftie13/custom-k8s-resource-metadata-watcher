package collector

import (
	"fmt"
	"sort"

	"github.com/example/metadata-exporter/pkg/config"
)

// compileFlatten expands every config.FlattenExtract in rule into a list
// of synthetic CompiledLabel entries and returns them. The caller is
// expected to merge the result into cr.Labels and then reorder cr.Labels
// / cr.LabelOrder canonically (see reorderLabels).
//
// The path for each generated label is formed by parsing FlattenExtract.Path
// once and appending a "field" segment equal to the user-supplied key. This
// sidesteps any quoting concerns with keys that contain '.', '/', '-' and
// similar characters, which the path grammar otherwise expects inside a
// quoted subscript.
func compileFlatten(rule *config.Rule) ([]CompiledLabel, error) {
	if len(rule.Flatten) == 0 {
		return nil, nil
	}
	var out []CompiledLabel
	for fi, f := range rule.Flatten {
		base, err := parsePath(f.Path)
		if err != nil {
			return nil, fmt.Errorf("rule %q flatten[%d] path %q: %w", rule.Name, fi, f.Path, err)
		}
		src := f.EffectiveSource()
		resolvedSrc := rule.ResolveRelation(src)
		onMissing := f.OnMissingValue()
		for _, key := range f.Keys {
			name := f.NamePrefix + config.SanitizeLabelName(key)
			segs := make([]pathSegment, 0, len(base.segments)+1)
			segs = append(segs, base.segments...)
			segs = append(segs, pathSegment{kind: "field", name: key})
			out = append(out, CompiledLabel{
				Name: name,
				Primary: CompiledExtract{
					Source: resolvedSrc,
					RawSrc: src,
					Path:   &parsedPath{segments: segs},
				},
				OnMissing: onMissing,
			})
		}
	}
	return out, nil
}

// reorderLabels sorts the combined label set by name so both cr.LabelOrder
// and cr.Labels stay aligned. The sort is stable w.r.t. name because names
// are unique (config validation rejects duplicates).
func reorderLabels(labels []CompiledLabel) ([]string, []CompiledLabel) {
	names := make([]string, 0, len(labels))
	byName := make(map[string]CompiledLabel, len(labels))
	for _, cl := range labels {
		names = append(names, cl.Name)
		byName[cl.Name] = cl
	}
	sort.Strings(names)
	sorted := make([]CompiledLabel, 0, len(labels))
	for _, n := range names {
		sorted = append(sorted, byName[n])
	}
	return names, sorted
}
