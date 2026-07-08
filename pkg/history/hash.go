package history

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// volatileMetadataKeys are metadata fields that change without a meaningful
// spec/status change; they are excluded from the dedup hash so resync events
// re-emitting an unchanged object are dropped.
var volatileMetadataKeys = map[string]struct{}{
	"resourceVersion": {},
	"managedFields":   {},
	"generation":      {},
}

// SpecHash returns a stable content hash of the object with volatile metadata
// stripped. status is intentionally retained (e.g. a Service's
// status.loadBalancer IP change must open a new version). json.Marshal of a
// map[string]interface{} sorts keys, so the result is deterministic.
func SpecHash(obj map[string]interface{}) (string, error) {
	b, err := json.Marshal(pruneVolatile(obj))
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// pruneVolatile returns a shallow copy of obj whose metadata submap omits
// volatile keys. The input object (shared with the informer cache) is never
// mutated.
func pruneVolatile(obj map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(obj))
	for k, v := range obj {
		if k == "metadata" {
			if md, ok := v.(map[string]interface{}); ok {
				nmd := make(map[string]interface{}, len(md))
				for mk, mv := range md {
					if _, skip := volatileMetadataKeys[mk]; skip {
						continue
					}
					nmd[mk] = mv
				}
				out[k] = nmd
				continue
			}
		}
		out[k] = v
	}
	return out
}
