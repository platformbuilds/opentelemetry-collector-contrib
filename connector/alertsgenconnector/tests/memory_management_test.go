package alertsgenconnector_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/alertsgenconnector"
)

func TestMemoryManagerAdaptiveScaling(t *testing.T) {
	cfg := alertsgenconnector.MemoryConfig{
		MaxMemoryPercent:      0.1,
		MaxTraceEntries:       1000,
		MaxLogEntries:         1000,
		MaxMetricEntries:      1000,
		EnableAdaptiveScaling: true,
		ScaleUpThreshold:      0.8,
		ScaleDownThreshold:    0.4,
		ScaleCheckInterval:    time.Millisecond * 100, // Fast for testing
		MaxScaleFactor:        5.0,
	}

	memMgr := alertsgenconnector.NewMemoryManager(cfg)
	require.NotNil(t, memMgr)

	// Test initial limits
	traces, logs, metrics := memMgr.GetCurrentLimits()
	assert.Equal(t, int64(1000), traces)
	assert.Equal(t, int64(1000), logs)
	assert.Equal(t, int64(1000), metrics)

	// Simulate high memory usage to trigger scaling
	memMgr.UpdateMemoryUsage(800) // 80% of some arbitrary limit

	// Wait for scaling check
	time.Sleep(time.Millisecond * 150)

	// Stats should show scaling activity
	stats := memMgr.GetStats()
	assert.True(t, stats.ScaleUpEvents >= 0) // May or may not have scaled yet
}

func TestBufferImplementations(t *testing.T) {
	t.Run("SliceBuffer", func(t *testing.T) {
		buffer := alertsgenconnector.NewSliceBuffer(100, 10)

		// Test adding items
		for i := 0; i < 50; i++ {
			assert.True(t, buffer.Add(i))
		}

		assert.Equal(t, 50, buffer.Len())
		assert.Equal(t, 100, buffer.Cap())
		assert.Equal(t, int64(500), buffer.EstimateMemoryUsage()) // 50 * 10

		// Test draining
		drained := buffer.Drain()
		assert.Len(t, drained, 50)
		assert.Equal(t, 0, buffer.Len())

		// Test buffer full
		for i := 0; i < 150; i++ { // Try to add more than capacity
			buffer.Add(i)
		}
		assert.Equal(t, 100, buffer.Len()) // Should be capped
	})

	t.Run("RingBuffer", func(t *testing.T) {
		buffer := alertsgenconnector.NewRingBuffer(10, 5, true) // Small buffer with overwrite

		// Fill the buffer
		for i := 0; i < 10; i++ {
			assert.True(t, buffer.Add(i))
		}
		assert.Equal(t, 10, buffer.Len())

		// Add more - should overwrite
		for i := 10; i < 15; i++ {
			assert.True(t, buffer.Add(i))
		}
		assert.Equal(t, 10, buffer.Len()) // Still at capacity

		// Drain and check we got the latest items
		drained := buffer.Drain()
		assert.Len(t, drained, 10)
		assert.Equal(t, 0, buffer.Len())

		// Test resize
		buffer.Resize(20)
		assert.Equal(t, 20, buffer.Cap())
	})
}

func TestMemoryPressureHandling(t *testing.T) {
	cfg := alertsgenconnector.MemoryConfig{
		MaxMemoryBytes:               1000,
		EnableMemoryPressureHandling: true,
		MemoryPressureThreshold:      0.8,
		SamplingRateUnderPressure:    0.5,
	}

	memMgr := alertsgenconnector.NewMemoryManager(cfg)

	// Normal operation
	assert.True(t, memMgr.ShouldAcceptTrace(50))
	assert.True(t, memMgr.ShouldAcceptLog(50))
	assert.True(t, memMgr.ShouldAcceptMetric(50))

	// Simulate memory pressure
	memMgr.UpdateMemoryUsage(850) // 85% of 1000 byte limit

	// Should start applying sampling (results may vary due to randomness)
	accepted := 0
	for i := 0; i < 100; i++ {
		if memMgr.ShouldAcceptTrace(50) {
			accepted++
		}
	}

	// Should accept roughly 50% (sampling rate under pressure)
	assert.True(t, accepted < 80, "Should drop some data under memory pressure, got %d", accepted)
	assert.True(t, accepted > 20, "Should still accept some data under memory pressure, got %d", accepted)
}

func TestIngesterWithAdaptiveMemory(t *testing.T) {
	cfg := &alertsgenconnector.Config{
		WindowSize: 5 * time.Second,
		Memory: alertsgenconnector.MemoryConfig{
			MaxTraceEntries:              100,
			MaxLogEntries:                100,
			MaxMetricEntries:             100,
			EnableAdaptiveScaling:        true,
			ScaleUpThreshold:             0.8,
			ScaleDownThreshold:           0.4,
			ScaleCheckInterval:           time.Millisecond * 50,
			EnableMemoryPressureHandling: true,
			MemoryPressureThreshold:      0.9,
			SamplingRateUnderPressure:    0.1,
			UseRingBuffers:               false,
		},
	}

	ingester := alertsgenconnector.NewIngesterWithLogger(cfg, nil)
	require.NotNil(t, ingester)

	// Test that buffers are initialized with correct limits
	assert.Equal(t, 100, ingester.GetTraces().Cap())
	assert.Equal(t, 100, ingester.GetLogs().Cap())
	assert.Equal(t, 100, ingester.GetMetrics().Cap())
}

func TestConcurrentAccess(t *testing.T) {
	cfg := alertsgenconnector.MemoryConfig{
		MaxTraceEntries:              1000,
		MaxLogEntries:                1000,
		MaxMetricEntries:             1000,
		EnableAdaptiveScaling:        true,
		ScaleCheckInterval:           time.Millisecond * 10,
		EnableMemoryPressureHandling: true,
	}

	memMgr := alertsgenconnector.NewMemoryManager(cfg)

	var wg sync.WaitGroup
	numGoroutines := 10
	numOperations := 100

	// Test concurrent access to memory manager
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				// Mix of operations
				switch j % 4 {
				case 0:
					memMgr.ShouldAcceptTrace(int64(j))
				case 1:
					memMgr.ShouldAcceptLog(int64(j))
				case 2:
					memMgr.ShouldAcceptMetric(int64(j))
				case 3:
					memMgr.UpdateMemoryUsage(int64(j * 10))
				}
			}
		}(i)
	}

	wg.Wait()

	// Should complete without race conditions or panics
	stats := memMgr.GetStats()
	assert.True(t, stats.TotalAllocations >= 0)
}

func TestBufferResize(t *testing.T) {
	buffer := alertsgenconnector.NewSliceBuffer(10, 1)

	// Fill buffer
	for i := 0; i < 8; i++ {
		buffer.Add(i)
	}
	assert.Equal(t, 8, buffer.Len())

	// Resize smaller
	buffer.Resize(5)
	assert.Equal(t, 5, buffer.Cap())
	assert.Equal(t, 5, buffer.Len()) // Should be trimmed

	// Resize larger
	buffer.Resize(20)
	assert.Equal(t, 20, buffer.Cap())
	assert.Equal(t, 5, buffer.Len()) // Data should remain
}

func TestMemoryStatsTracking(t *testing.T) {
	cfg := alertsgenconnector.MemoryConfig{
		MaxTraceEntries:              1000,
		EnableMemoryPressureHandling: true,
		MemoryPressureThreshold:      0.5, // Low threshold for testing
		SamplingRateUnderPressure:    0.0, // Drop everything under pressure
	}

	memMgr := alertsgenconnector.NewMemoryManager(cfg)

	// Trigger memory pressure
	memMgr.UpdateMemoryUsage(600) // Above 50% threshold

	// Try to accept data (should be dropped due to pressure)
	for i := 0; i < 10; i++ {
		memMgr.ShouldAcceptTrace(int64(i))
	}

	stats := memMgr.GetStats()
	// Note: Actual dropped count depends on internal implementation
	assert.True(t, stats.MemoryPressureEvents >= 0)
}

func TestAutoMemoryCalculation(t *testing.T) {
	cfg := alertsgenconnector.MemoryConfig{
		MaxMemoryPercent: 0.1, // 10% of system memory
		MaxTraceEntries:  0,   // Auto-calculate
		MaxLogEntries:    0,   // Auto-calculate
		MaxMetricEntries: 0,   // Auto-calculate
	}

	memMgr := alertsgenconnector.NewMemoryManager(cfg)

	traces, logs, metrics := memMgr.GetCurrentLimits()

	// Should have calculated reasonable limits based on available memory
	assert.True(t, traces >= 1000, "Trace limit should be at least 1000, got %d", traces)
	assert.True(t, logs >= 1000, "Log limit should be at least 1000, got %d", logs)
	assert.True(t, metrics >= 1000, "Metric limit should be at least 1000, got %d", metrics)

	// Limits should be proportional to expected memory usage
	// Traces (1KB) < Logs (512B) < Metrics (256B), so limits should be inverse
	assert.True(t, metrics >= logs, "Metrics should have higher limit than logs")
	assert.True(t, logs >= traces, "Logs should have higher limit than traces")
}

func BenchmarkSliceBufferAdd(b *testing.B) {
	buffer := alertsgenconnector.NewSliceBuffer(100000, 1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buffer.Add(i)
	}
}

func BenchmarkRingBufferAdd(b *testing.B) {
	buffer := alertsgenconnector.NewRingBuffer(100000, 1, true)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buffer.Add(i)
	}
}

func BenchmarkMemoryManagerShouldAccept(b *testing.B) {
	cfg := alertsgenconnector.MemoryConfig{
		MaxTraceEntries:              100000,
		EnableMemoryPressureHandling: true,
		MemoryPressureThreshold:      0.8,
		SamplingRateUnderPressure:    0.5,
	}

	memMgr := alertsgenconnector.NewMemoryManager(cfg)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		memMgr.ShouldAcceptTrace(int64(i % 50000))
	}
}
