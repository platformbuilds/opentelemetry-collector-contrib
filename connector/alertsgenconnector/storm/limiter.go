// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package storm // import "github.com/open-telemetry/opentelemetry-collector-contrib/connector/alertsgenconnector/storm"

import (
	"sync"
	"time"
)

// Limiter applies coarse global throttles for alert storms.
type Limiter struct {
	MaxTransitionsPerMinute int
	MaxEventsPerInterval    int
	Interval                time.Duration

	mu          sync.Mutex
	windowEnd   time.Time
	transitions int
}

func New(maxTPM, maxPerInterval int, interval time.Duration) *Limiter {
	return &Limiter{
		MaxTransitionsPerMinute: maxTPM,
		MaxEventsPerInterval:    maxPerInterval,
		Interval:                interval,
	}
}

// Allow returns how many events can be processed (may be less than n).
func (l *Limiter) Allow(n int) (allow int, dropped int) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	if l.windowEnd.IsZero() || now.After(l.windowEnd) {
		l.windowEnd = now.Add(l.Interval)
		l.transitions = 0
	}

	allow = n
	if l.MaxTransitionsPerMinute > 0 {
		if l.transitions+n > l.MaxTransitionsPerMinute {
			allow = l.MaxTransitionsPerMinute - l.transitions
			if allow < 0 {
				allow = 0
			}
		}
		l.transitions += allow
	}
	if l.MaxEventsPerInterval > 0 && allow > l.MaxEventsPerInterval {
		allow = l.MaxEventsPerInterval
	}
	if allow < n {
		dropped = n - allow
	}
	return
}
