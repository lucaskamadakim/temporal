package quotas

import (
	"sync"
	"time"

	"go.temporal.io/server/common/clock"
)

var _ MultiHierarchicalRateLimiter = (*multiHierarchicalRateLimiterImpl)(nil)

type (
	// MultiHierarchicalRateLimiter manages one HierarchicalRateLimiter
	// tree per named group (e.g. per shard or per cell) under a shared
	// configuration. Groups are created lazily and may be pruned when
	// idle, so callers need not track group lifecycle.
	MultiHierarchicalRateLimiter interface {
		// Limiter returns the limiter tree for group, creating it on
		// first use.
		Limiter(group string) HierarchicalRateLimiter
		// Allow checks a request within group's tree.
		Allow(group string, request HierarchicalRequest) bool
		// Reserve takes a cancellable reservation within group's tree.
		Reserve(group string, request HierarchicalRequest) HierarchicalReservation
		// Update reconfigures every existing tree and all future groups.
		Update(config HierarchicalLimiterConfig) error
		// RemoveGroup drops a group's tree. Returns false if absent.
		RemoveGroup(group string) bool
		// Groups lists live group names.
		Groups() []string
		// PruneIdleGroups removes whole groups untouched for longer than
		// cutoff. Returns the number removed.
		PruneIdleGroups(cutoff time.Duration) int
	}

	limiterGroup struct {
		limiter  *hierarchicalRateLimiterImpl
		lastUsed time.Time
	}

	multiHierarchicalRateLimiterImpl struct {
		sync.Mutex
		config     HierarchicalLimiterConfig
		timeSource clock.TimeSource
		observers  HierarchicalRateLimiterMetrics
		groups     map[string]*limiterGroup
	}
)

// NewMultiHierarchicalRateLimiter builds a group-aware limiter. Observers
// are shared across groups; tag by group at the call site if needed.
func NewMultiHierarchicalRateLimiter(
	config HierarchicalLimiterConfig,
	timeSource clock.TimeSource,
	observers HierarchicalRateLimiterMetrics,
) (*multiHierarchicalRateLimiterImpl, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &multiHierarchicalRateLimiterImpl{
		config:     config,
		timeSource: timeSource,
		observers:  observers,
		groups:     make(map[string]*limiterGroup),
	}, nil
}

func (m *multiHierarchicalRateLimiterImpl) Limiter(group string) HierarchicalRateLimiter {
	m.Lock()
	defer m.Unlock()
	return m.limiterLocked(group)
}

func (m *multiHierarchicalRateLimiterImpl) limiterLocked(group string) *hierarchicalRateLimiterImpl {
	g := m.groups[group]
	if g == nil {
		limiter, err := NewHierarchicalRateLimiter(m.config, m.timeSource, m.observers)
		if err != nil {
			// Config was validated at construction and on every Update.
			return nil
		}
		g = &limiterGroup{limiter: limiter}
		m.groups[group] = g
	}
	g.lastUsed = m.timeSource.Now()
	return g.limiter
}

func (m *multiHierarchicalRateLimiterImpl) Allow(group string, request HierarchicalRequest) bool {
	l := m.Limiter(group)
	if l == nil {
		return false
	}
	return l.Allow(request)
}

func (m *multiHierarchicalRateLimiterImpl) Reserve(group string, request HierarchicalRequest) HierarchicalReservation {
	l := m.Limiter(group)
	if l == nil {
		return newFailedReservation(0)
	}
	return l.Reserve(request)
}

func (m *multiHierarchicalRateLimiterImpl) Update(config HierarchicalLimiterConfig) error {
	if err := config.Validate(); err != nil {
		return err
	}
	m.Lock()
	defer m.Unlock()
	m.config = config
	for _, g := range m.groups {
		if err := g.limiter.Update(config); err != nil {
			return err
		}
	}
	return nil
}

func (m *multiHierarchicalRateLimiterImpl) RemoveGroup(group string) bool {
	m.Lock()
	defer m.Unlock()
	if _, ok := m.groups[group]; !ok {
		return false
	}
	delete(m.groups, group)
	return true
}

func (m *multiHierarchicalRateLimiterImpl) Groups() []string {
	m.Lock()
	defer m.Unlock()
	groups := make([]string, 0, len(m.groups))
	for name := range m.groups {
		groups = append(groups, name)
	}
	return groups
}

func (m *multiHierarchicalRateLimiterImpl) PruneIdleGroups(cutoff time.Duration) int {
	m.Lock()
	defer m.Unlock()
	now := m.timeSource.Now()
	removed := 0
	for name, g := range m.groups {
		if now.Sub(g.lastUsed) > cutoff {
			delete(m.groups, name)
			removed++
		}
	}
	return removed
}
