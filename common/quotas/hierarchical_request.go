package quotas

import (
	"strings"
)

type (
	// HierarchicalRequest identifies a single quota check against a
	// HierarchicalRateLimiter. Path addresses a leaf bucket through the
	// tree: element 0 selects a child of the implicit root, and so on.
	// Paths deeper than the configured tree depth clamp to the deepest
	// level's configuration.
	HierarchicalRequest struct {
		Path   []string
		Tokens int64
	}
)

// Key renders the request path in a stable joined form suitable for use as
// a map key or metric tag value. It is not used for bucket identity; the
// limiter keys nodes per level so path separators in names cannot produce
// collisions.
func (r HierarchicalRequest) Key() string {
	return strings.Join(r.Path, "/")
}

// Depth returns the path length; a zero-depth request targets the root.
func (r HierarchicalRequest) Depth() int {
	return len(r.Path)
}
