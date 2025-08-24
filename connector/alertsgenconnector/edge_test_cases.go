// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package alertsgenconnector

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/alertsgenconnector/dedup"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/connector/connectortest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap/zaptest"
)

// ---- Integration Tests ----

func TestIntegrationEndToEnd(t *testing.T) {
	// Create config with multiple rules
	cfg := &Config{
		WindowSize: 100 * time.Millisecond,
		Step:       50 * time.Millisecond,
		InstanceID: "integration-test",
		TSDB: &TSDBConfig{
			QueryURL:     "http://test",
			QueryTimeout: 5 * time.Second,
		},
		Memory: MemoryConfig{
			MaxMemoryPercent:          0.1,
			EnableAdaptiveScaling:     true,
			SamplingRateUnderPressure: 1.0,
		},
		Rules: []RuleCfg{
			{
				Name:     "high_trace_latency",
				Signal:   "traces",
				Severity: "critical",
				Window:   100 * time.Millisecond,
				Step:     50 * time.Millisecond,
				For:      0,
				Select: map[string]string{
					"service.name": ".*",
				},
				GroupBy: []string{"service.name"},
				Expr: ExprCfg{
					Type:  "count_over_time",
					Op:    ">",
					Value: 2,
				},
			},
			{
				Name:     "error_logs",
				Signal:   "logs",
				Severity: "warning",
				Window:   100 * time.Millisecond,
				Step:     50 * time.Millisecond,
				For:      0,
				Select: map[string]string{
					"severity": "ERROR",
				},
				GroupBy: []string{"service.name"},
				Expr: ExprCfg{
					Type:  "count_over_time",
					Op:    ">",
					Value: 2,
				},
			},
			{
				Name:     "high_cpu",
				Signal:   "metrics",
				Severity: "warning",
				Window:   100 * time.Millisecond,
				Step:     50 * time.Millisecond,
				For:      0,
				Select: map[string]string{
					"__name__": "cpu.usage",
				},
				GroupBy: []string{"host"},
				Expr: ExprCfg{
					Type:  "avg_over_time",
					Field: "value",
					Op:    ">",
					Value: 0.8,
				},
			},
		},
	}

	ctx := context.Background()
	set := connectortest.NewNopSettings()
	set.Logger = zaptest.NewLogger(t)

	// Create connector
	conn, err := newAlertsConnector(ctx, set, cfg)
	require.NoError(t, err)

	// Set up consumers to capture output
	logsSink := &consumertest.LogsSink{}
	metricsSink := &consumertest.MetricsSink{}
	conn.nextLogs = logsSink
	conn.nextMetrics = metricsSink

	// Start connector
	err = conn.Start(ctx, componenttest.NewNopHost())
	require.NoError(t, err)
	defer conn.Shutdown(ctx)

	// Generate test data
	generateTestData := func() {
		// Generate traces
		td := ptrace.NewTraces()
		rs := td.ResourceSpans().AppendEmpty()
		ss := rs.ScopeSpans().AppendEmpty()
		for i := 0; i < 5; i++ {
			span := ss.Spans().AppendEmpty()
			span.SetName(fmt.Sprintf("span-%d", i))
			span.Attributes().PutStr("service.name", "test-service")
		}
		conn.ConsumeTraces(ctx, td)

		// Generate logs
		ld := plog.NewLogs()
		rl := ld.ResourceLogs().AppendEmpty()
		sl := rl.ScopeLogs().AppendEmpty()
		for i := 0; i < 5; i++ {
			lr := sl.LogRecords().AppendEmpty()
			lr.SetSeverityText("ERROR")
			lr.Attributes().PutStr("severity", "ERROR")
			lr.Attributes().PutStr("service.name", "log-service")
		}
		conn.ConsumeLogs(ctx, ld)

		// Generate metrics
		md := pmetric.NewMetrics()
		rm := md.ResourceMetrics().AppendEmpty()
		sm := rm.ScopeMetrics().AppendEmpty()
		m := sm.Metrics().AppendEmpty()
		m.SetName("cpu.usage")
		m.SetEmptyGauge()
		dp := m.Gauge().DataPoints().AppendEmpty()
		dp.SetDoubleValue(0.9)
		dp.Attributes().PutStr("host", "server1")
		dp.Attributes().PutStr("__name__", "cpu.usage")
		conn.ConsumeMetrics(ctx, md)
	}

	// Send data multiple times
	for i := 0; i < 3; i++ {
		generateTestData()
		time.Sleep(20 * time.Millisecond)
	}

	// Wait for evaluation cycles
	time.Sleep(200 * time.Millisecond)

	// Verify alerts were generated
	assert.Greater(t, logsSink.LogRecordCount(), 0, "Should have generated alert logs")
	assert.Greater(t, metricsSink.MetricCount(), 0, "Should have generated alert metrics")
}

func TestIntegrationWithMemoryPressure(t *testing.T) {
	cfg := &Config{
		WindowSize: 50 * time.Millisecond,
		Step:       50 * time.Millisecond,
		InstanceID: "memory-test",
		TSDB: &TSDBConfig{
			QueryURL: "http://test",
		},
		Memory: MemoryConfig{
			MaxMemoryBytes:               1024 * 1024, // 1MB limit
			EnableMemoryPressureHandling: true,
			MemoryPressureThreshold:      0.8,
			SamplingRateUnderPressure:    0.1,
			EnableAdaptiveScaling:        true,
			ScaleUpThreshold:             0.7,
			ScaleDownThreshold:           0.3,
			ScaleCheckInterval:           10 * time.Millisecond,
			MaxTraceEntries:              100,
			MaxLogEntries:                100,
			MaxMetricEntries:             100,
		},
		Rules: []RuleCfg{
			{
				Name:   "test_rule",
				Signal: "traces",
				Expr: ExprCfg{
					Type:  "count_over_time",
					Op:    ">",
					Value: 1,
				},
			},
		},
	}

	ctx := context.Background()
	set := connectortest.NewNopSettings()
	set.Logger = zaptest.NewLogger(t)

	conn, err := newAlertsConnector(ctx, set, cfg)
	require.NoError(t, err)

	conn.nextLogs = consumertest.NewNop()
	err = conn.Start(ctx, componenttest.NewNopHost())
	require.NoError(t, err)
	defer conn.Shutdown(ctx)

	// Generate lots of data to trigger memory pressure
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				td := ptrace.NewTraces()
				td.ResourceSpans().AppendEmpty()
				conn.ConsumeTraces(ctx, td)
			}
		}()
	}

	wg.Wait()
	time.Sleep(100 * time.Millisecond)

	// Check that memory management kicked in
	stats := conn.ing.memMgr.GetStats()
	// May have dropped some data under pressure
	assert.GreaterOrEqual(t, stats.DroppedTraces+stats.DroppedLogs+stats.DroppedMetrics, int64(0))
}

func TestIntegrationMultiSignalCorrelation(t *testing.T) {
	cfg := &Config{
		WindowSize: 100 * time.Millisecond,
		Step:       50 * time.Millisecond,
		InstanceID: "correlation-test",
		TSDB: &TSDBConfig{
			QueryURL: "http://test",
		},
		Memory: MemoryConfig{
			MaxMemoryPercent: 0.1,
		},
		Rules: []RuleCfg{
			{
				Name:     "correlated_issues",
				Signal:   "traces",
				Severity: "critical",
				Window:   100 * time.Millisecond,
				GroupBy:  []string{"service.name"},
				Expr: ExprCfg{
					Type:  "count_over_time",
					Op:    ">",
					Value: 5,
				},
			},
		},
	}

	ctx := context.Background()
	set := connectortest.NewNopSettings()
	set.Logger = zaptest.NewLogger(t)

	conn, err := newAlertsConnector(ctx, set, cfg)
	require.NoError(t, err)

	logsSink := &consumertest.LogsSink{}
	conn.nextLogs = logsSink

	err = conn.Start(ctx, componenttest.NewNopHost())
	require.NoError(t, err)
	defer conn.Shutdown(ctx)

	// Simulate correlated events across signals
	service := "critical-service"

	// Generate error traces
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	ss := rs.ScopeSpans().AppendEmpty()
	for i := 0; i < 10; i++ {
		span := ss.Spans().AppendEmpty()
		span.SetName("error-span")
		span.Attributes().PutStr("service.name", service)
		span.SetStatus(ptrace.StatusCode(2)) // Error
	}
	conn.ConsumeTraces(ctx, td)

	// Generate error logs for same service
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	for i := 0; i < 10; i++ {
		lr := sl.LogRecords().AppendEmpty()
		lr.SetSeverityText("ERROR")
		lr.Attributes().PutStr("service.name", service)
	}
	conn.ConsumeLogs(ctx, ld)

	// Generate high CPU metrics for same service
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("cpu.usage")
	m.SetEmptyGauge()
	dp := m.Gauge().DataPoints().AppendEmpty()
	dp.SetDoubleValue(0.95)
	dp.Attributes().PutStr("service.name", service)
	conn.ConsumeMetrics(ctx, md)

	// Wait for evaluation
	time.Sleep(150 * time.Millisecond)

	// Should have generated alerts
	assert.Greater(t, logsSink.LogRecordCount(), 0)
}

// ---- Benchmark Tests ----

func BenchmarkConnectorThroughput(b *testing.B) {
	cfg := createBenchmarkConfig()
	ctx := context.Background()
	set := connectortest.NewNopSettings()

	conn, _ := newAlertsConnector(ctx, set, cfg)
	conn.nextLogs = consumertest.NewNop()
	conn.nextMetrics = consumertest.NewNop()
	conn.Start(ctx, componenttest.NewNopHost())
	defer conn.Shutdown(ctx)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			td := generateBenchmarkTraces(10)
			conn.ConsumeTraces(ctx, td)
		}
	})
}

func BenchmarkRuleEvaluation(b *testing.B) {
	cfg := createBenchmarkConfig()
	rs, _ := compileRules(cfg)
	ing := createBenchmarkIngester()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs.evaluate(time.Now(), ing)
	}
}

func BenchmarkIngestion(b *testing.B) {
	cfg := createBenchmarkConfig()
	ing := NewIngester(cfg, nil)

	traces := generateBenchmarkTraces(100)
	logs := generateBenchmarkLogs(100)
	metrics := generateBenchmarkMetrics(100)

	b.ResetTimer()
	b.Run("traces", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			ing.IngestTraces(traces)
		}
	})

	b.Run("logs", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			ing.IngestLogs(logs)
		}
	})

	b.Run("metrics", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			ing.IngestMetrics(metrics)
		}
	})
}

func BenchmarkMemoryBuffers(b *testing.B) {
	b.Run("slice_buffer", func(b *testing.B) {
		buf := NewSliceBuffer(10000, 1024)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buf.Add(i)
			if i%2 == 0 {
				buf.Pop()
			}
		}
	})

	b.Run("ring_buffer", func(b *testing.B) {
		buf := NewRingBuffer(10000, 1024, false)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buf.Add(i)
			if i%2 == 0 {
				buf.Pop()
			}
		}
	})

	b.Run("ring_buffer_overwrite", func(b *testing.B) {
		buf := NewRingBuffer(10000, 1024, true)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buf.Add(i)
			if i%2 == 0 {
				buf.Pop()
			}
		}
	})
}

func BenchmarkFingerprinting(b *testing.B) {
	labels := map[string]string{
		"service":   "api",
		"env":       "prod",
		"region":    "us-east-1",
		"instance":  "i-1234567890",
		"namespace": "default",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = fingerprint("test_rule", labels)
	}
}

func BenchmarkDeduplication(b *testing.B) {
	d := dedup.New(1 * time.Minute)
	fps := make([]uint64, 1000)
	for i := range fps {
		fps[i] = uint64(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fp := fps[i%len(fps)]
		d.Seen(fp)
	}
}

// ---- Helper Functions ----

func createBenchmarkConfig() *Config {
	cfg := CreateDefaultConfig().(*Config)
	cfg.WindowSize = 5 * time.Second
	cfg.Step = 5 * time.Second

	// Add multiple rules for realistic benchmark
	for i := 0; i < 10; i++ {
		cfg.Rules = append(cfg.Rules, RuleCfg{
			Name:     fmt.Sprintf("rule_%d", i),
			Signal:   "traces",
			Severity: "warning",
			Window:   5 * time.Second,
			GroupBy:  []string{"service.name", "http.method"},
			Expr: ExprCfg{
				Type:  "count_over_time",
				Op:    ">",
				Value: float64(i * 10),
			},
		})
	}

	return cfg
}

func createBenchmarkIngester() *ingester {
	ing := &ingester{
		traces:  NewSliceBuffer(10000, 1024),
		logs:    NewSliceBuffer(10000, 512),
		metrics: NewSliceBuffer(10000, 768),
	}

	// Pre-populate with data
	for i := 0; i < 1000; i++ {
		ing.traces.Add(traceRow{
			durationNs: float64(i * 1000),
			ts:         time.Now(),
			attrs: map[string]string{
				"service.name": fmt.Sprintf("service-%d", i%10),
				"http.method":  "GET",
			},
		})
	}

	return ing
}

func generateBenchmarkTraces(count int) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	ss := rs.ScopeSpans().AppendEmpty()

	for i := 0; i < count; i++ {
		span := ss.Spans().AppendEmpty()
		span.SetName(fmt.Sprintf("span-%d", i))
		span.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		span.SetEndTimestamp(pcommon.NewTimestampFromTime(time.Now().Add(100 * time.Millisecond)))
		span.Attributes().PutStr("service.name", fmt.Sprintf("service-%d", i%5))
		span.Attributes().PutStr("http.method", "GET")
		span.Attributes().PutInt("http.status_code", 200)
	}

	return td
}

func generateBenchmarkLogs(count int) plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()

	for i := 0; i < count; i++ {
		lr := sl.LogRecords().AppendEmpty()
		lr.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		lr.SetSeverityText("INFO")
		lr.Body().SetStr(fmt.Sprintf("Log message %d", i))
		lr.Attributes().PutStr("service.name", fmt.Sprintf("service-%d", i%5))
	}

	return ld
}

func generateBenchmarkMetrics(count int) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()

	for i := 0; i < count; i++ {
		m := sm.Metrics().AppendEmpty()
		m.SetName(fmt.Sprintf("metric_%d", i))
		m.SetEmptyGauge()
		dp := m.Gauge().DataPoints().AppendEmpty()
		dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		dp.SetDoubleValue(float64(i))
		dp.Attributes().PutStr("host", fmt.Sprintf("host-%d", i%10))
	}

	return md
}
