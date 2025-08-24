// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package alertsgenconnector // import "github.com/open-telemetry/opentelemetry-collector-contrib/connector/alertsgenconnector"

import "sync"

// inmemStore is a simple in-memory key->int64 counter store.
// It's provided as a fallback/reference implementation for ephemeral runs.
type inmemStore struct {
	mu sync.RWMutex
	m  map[string]int64
}

func newInmemStore() *inmemStore {
	return &inmemStore{m: make(map[string]int64)}
}

func (s *inmemStore) IncCounter(key string, n int64) {
	s.mu.Lock()
	s.m[key] += n
	s.mu.Unlock()
}

func (s *inmemStore) GetCounter(key string) int64 {
	s.mu.RLock()
	v := s.m[key]
	s.mu.RUnlock()
	return v
}

func (s *inmemStore) Close() error { return nil }
