// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package alertsgenconnector

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/alertsgenconnector/cardinality"
	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/alertsgenconnector/dedup"
	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/alertsgenconnector/storm"
)

// ---- Dedup Tests ----

func TestFingerprintDeduper(t *testing.T) {
	t.Run("basic_dedup", func(t *testing.T) {
		d := dedup.New(100 * time.Millisecond)

		fp1 := uint64(12345)
		fp2 := uint64(67890)

		// First time should not be seen
		assert.False(t, d.Seen(fp1))
		assert.False(t, d.Seen(fp2))

		// Second time should be seen
		assert.True(t, d.Seen(fp1))
		assert.True(t, d.Seen(fp2))

		// After TTL expires, should not be seen
		time.Sleep(150 * time.Millisecond)
		assert.False(t, d.Seen(fp1))
		assert.False(t, d.Seen(fp2))
	})

	t.Run("ttl_expiry", func(t *testing.T) {
		d := dedup.New(50 * time.Millisecond)

		fp := uint64(11111)

		assert.False(t, d.Seen(fp))
		assert.True(t, d.Seen(fp))

		// Wait half TTL
		time.Sleep(25 * time.Millisecond)
		assert.True(t, d.Seen(fp))

		// Wait past TTL
		time.Sleep(30 * time.Millisecond)
		assert.False(t, d.Seen(fp))
	})

	t.Run("gc_cleanup", func(t *testing.T) {
		d := dedup.New(50 * time.Millisecond)

		// Add multiple fingerprints
		for i := uint64(1); i <= 10; i++ {
			d.Seen(i)
		}

		// Wait for TTL to expire
		time.Sleep(60 * time.Millisecond)

		// Trigger GC with a new fingerprint
		d.Seen(99999)

		// Old entries should be cleaned up
		for i := uint64(1); i <= 10; i++ {
			assert.False(t, d.Seen(i))
		}
	})

	t.Run("concurrent_access", func(t *testing.T) {
		d := dedup.New(100 * time.Millisecond)
		var wg sync.WaitGroup

		// Concurrent writes
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(fp uint64) {
				defer wg.Done()
				d.Seen(fp)
			}(uint64(i))
		}

		wg.Wait()

		// Concurrent reads
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(fp uint64) {
				defer wg.Done()
				assert.True(t, d.Seen(fp))
			}(uint64(i))
		}

		wg.Wait()
	})
}

// ---- Storm Limiter Tests ----

func TestStormLimiter(t *testing.T) {
	t.Run("transitions_limit", func(t *testing.T) {
		l := storm.New(10, 0, 1*time.Second)

		// Should allow up to 10 transitions
		for i := 0; i < 10; i++ {
			allow, dropped := l.Allow(1)
			assert.Equal(t, 1, allow)
			assert.Equal(t, 0, dropped)
		}

		// 11th should be dropped
		allow, dropped := l.Allow(1)
		assert.Equal(t, 0, allow)
		assert.Equal(t, 1, dropped)
	})

	t.Run("events_per_interval", func(t *testing.T) {
		l := storm.New(0, 5, 100*time.Millisecond)

		// Should allow up to 5 events
		allow, dropped := l.Allow(5)
		assert.Equal(t, 5, allow)
		assert.Equal(t, 0, dropped)

		// 6th should be limited
		allow, dropped = l.Allow(1)
		assert.Equal(t, 0, allow)
		assert.Equal(t, 1, dropped)

		// After interval, should allow again
		time.Sleep(150 * time.Millisecond)
		allow, dropped = l.Allow(3)
		assert.Equal(t, 3, allow)
		assert.Equal(t, 0, dropped)
	})

	t.Run("batch_allow", func(t *testing.T) {
		l := storm.New(100, 10, 1*time.Second)

		// Request batch of 5
		allow, dropped := l.Allow(5)
		assert.Equal(t, 5, allow)
		assert.Equal(t, 0, dropped)

		// Request batch of 10 (only 5 left in interval)
		allow, dropped = l.Allow(10)
		assert.Equal(t, 5, allow)
		assert.Equal(t, 5, dropped)
	})

	t.Run("both_limits", func(t *testing.T) {
		l := storm.New(20, 5, 100*time.Millisecond)

		// First interval
		allow, dropped := l.Allow(5)
		assert.Equal(t, 5, allow)
		assert.Equal(t, 0, dropped)

		// Wait for new interval
		time.Sleep(150 * time.Millisecond)

		// Second interval (10 transitions total)
		allow, dropped = l.Allow(5)
		assert.Equal(t, 5, allow)
		assert.Equal(t, 0, dropped)

		// Wait for new interval
		time.Sleep(150 * time.Millisecond)

		// Third interval (15 transitions total)
		allow, dropped = l.Allow(5)
		assert.Equal(t, 5, allow)
		assert.Equal(t, 0, dropped)

		// Wait for new interval
		time.Sleep(150 * time.Millisecond)

		// Fourth interval (would be 20 transitions, hits limit)
		allow, dropped = l.Allow(5)
		assert.Equal(t, 5, allow)
		assert.Equal(t, 0, dropped)

		// Fifth interval (exceeds transitions limit)
		allow, dropped = l.Allow(5)
		assert.Equal(t, 0, allow)
		assert.Equal(t, 5, dropped)
	})

	t.Run("no_limits", func(t *testing.T) {
		l := storm.New(0, 0, 1*time.Second)

		// Should allow everything
		allow, dropped := l.Allow(1000)
		assert.Equal(t, 1000, allow)
		assert.Equal(t, 0, dropped)
	})

	t.Run("concurrent_allow", func(t *testing.T) {
		l := storm.New(1000, 100, 1*time.Second)
		var wg sync.WaitGroup
		var totalAllowed, totalDropped int
		var mu sync.Mutex

		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				allow, dropped := l.Allow(15)
				mu.Lock()
				totalAllowed += allow
				totalDropped += dropped
				mu.Unlock()
			}()
		}

		wg.Wait()

		// Total requested: 150
		// Limit per interval: 100
		assert.Equal(t, 100, totalAllowed)
		assert.Equal(t, 50, totalDropped)
	})
}

// ---- Cardinality Control Tests ----

func TestCardinalityControl(t *testing.T) {
	t.Run("max_labels", func(t *testing.T) {
		c := &cardinality.Control{
			MaxLabels: 3,
		}

		labels := map[string]string{
			"a": "1",
			"b": "2",
			"c": "3",
			"d": "4",
			"e": "5",
		}

		result := c.Enforce(labels)
		assert.LessOrEqual(t, len(result), 3)
	})

	t.Run("max_value_length", func(t *testing.T) {
		c := &cardinality.Control{
			MaxValLen: 10,
		}

		labels := map[string]string{
			"short": "abc",
			"long":  "this is a very long value that exceeds the limit",
		}

		result := c.Enforce(labels)
		assert.Equal(t, "abc", result["short"])
		assert.Equal(t, "this is a ", result["long"])
	})

	t.Run("hash_long_values", func(t *testing.T) {
		c := &cardinality.Control{
			MaxValLen:     10,
			HashIfExceeds: 8,
		}

		labels := map[string]string{
			"key": "this is a very long value",
		}

		result := c.Enforce(labels)
		// Should be hashed to 8 chars
		assert.Equal(t, 8, len(result["key"]))
		assert.NotEqual(t, "this is a", result["key"])
	})

	t.Run("blocklist", func(t *testing.T) {
		c := &cardinality.Control{
			Block: map[string]struct{}{
				"pod_ip":       {},
				"container_id": {},
			},
		}

		labels := map[string]string{
			"service":      "api",
			"pod_ip":       "10.0.0.1",
			"container_id": "abc123",
			"namespace":    "default",
		}

		result := c.Enforce(labels)
		assert.Equal(t, 2, len(result))
		assert.Equal(t, "api", result["service"])
		assert.Equal(t, "default", result["namespace"])
		_, hasPodIP := result["pod_ip"]
		assert.False(t, hasPodIP)
		_, hasContainerID := result["container_id"]
		assert.False(t, hasContainerID)
	})

	t.Run("allowlist_preservation", func(t *testing.T) {
		c := &cardinality.Control{
			MaxLabels: 2,
			Allow: map[string]struct{}{
				"important": {},
			},
		}

		labels := map[string]string{
			"important": "keep",
			"optional1": "drop1",
			"optional2": "drop2",
			"optional3": "drop3",
		}

		result := c.Enforce(labels)
		assert.LessOrEqual(t, len(result), 2)
		assert.Equal(t, "keep", result["important"])
	})

	t.Run("max_total_size", func(t *testing.T) {
		c := &cardinality.Control{
			MaxTotalSize: 50,
			Allow: map[string]struct{}{
				"keep": {},
			},
		}

		labels := map[string]string{
			"keep":  "important",
			"long1": "verylongvalue1",
			"long2": "verylongvalue2",
			"long3": "verylongvalue3",
		}

		result := c.Enforce(labels)

		// Calculate total size
		totalSize := 0
		for k, v := range result {
			totalSize += len(k) + len(v)
		}
		assert.LessOrEqual(t, totalSize, 50)

		// Allowed labels should be kept
		assert.Equal(t, "important", result["keep"])
	})

	t.Run("nil_control", func(t *testing.T) {
		var c *cardinality.Control

		labels := map[string]string{
			"a": "1",
			"b": "2",
		}

		result := c.Enforce(labels)
		assert.Equal(t, labels, result)
	})

	t.Run("empty_labels", func(t *testing.T) {
		c := &cardinality.Control{
			MaxLabels: 5,
			MaxValLen: 10,
		}

		labels := map[string]string{}
		result := c.Enforce(labels)
		assert.Equal(t, 0, len(result))
	})

	t.Run("combined_controls", func(t *testing.T) {
		c := &cardinality.Control{
			MaxLabels:    5,
			MaxValLen:    20,
			MaxTotalSize: 100,
			Block: map[string]struct{}{
				"blocked": {},
			},
			Allow: map[string]struct{}{
				"important": {},
			},
		}

		labels := map[string]string{
			"important": "keep_this_value",
			"blocked":   "remove_this",
			"normal1":   "value1",
			"normal2":   "value2",
			"normal3":   "value3",
			"normal4":   "value4",
			"normal5":   "value5",
			"normal6":   "value6", // Should be dropped due to MaxLabels
		}

		result := c.Enforce(labels)

		// Blocked should be removed
		_, hasBlocked := result["blocked"]
		assert.False(t, hasBlocked)

		// Important should be kept
		assert.Equal(t, "keep_this_value", result["important"])

		// Total should not exceed MaxLabels
		assert.LessOrEqual(t, len(result), 5)
	})

	t.Run("deterministic_dropping", func(t *testing.T) {
		c := &cardinality.Control{
			MaxLabels: 3,
		}

		labels := map[string]string{
			"z": "26",
			"y": "25",
			"x": "24",
			"w": "23",
			"v": "22",
		}

		// Should drop labels consistently (alphabetically last)
		result1 := c.Enforce(labels)
		result2 := c.Enforce(labels)

		assert.Equal(t, result1, result2)
		assert.Equal(t, 3, len(result1))
	})
}
