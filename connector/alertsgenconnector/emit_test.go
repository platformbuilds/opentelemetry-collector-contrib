// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package alertsgenconnector

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func TestBuildLogs(t *testing.T) {
	events := []alertEvent{
		{
			Rule:     "high_latency",
			State:    "firing",
			Severity: "critical",
			Labels: map[string]string{
				"service": "api",
				"env":     "prod",
			},
			Value:  500.5,
			Window: "5m",
			For:    "1m",
		},
		{
			Rule:     "error_rate",
			State:    "resolved",
			Severity: "warning",
			Labels: map[string]string{
				"service": "db",
				"env":     "staging",
			},
			Value:  0.05,
			Window: "10m",
			For:    "2m",
		},
		{
			Rule:     "service_down",
			State:    "no_data",
			Severity: "info",
			Labels:   map[string]string{},
			Value:    0,
			Window:   "1m",
			For:      "30s",
		},
	}

	ld := buildLogs(events)
	require.NotNil(t, ld)

	// Check resource logs
	assert.Equal(t, 1, ld.ResourceLogs().Len())
	rl := ld.ResourceLogs().At(0)

	// Check scope logs
	assert.Equal(t, 1, rl.ScopeLogs().Len())
	sl := rl.ScopeLogs().At(0)

	// Check log records
	assert.Equal(t, 3, sl.LogRecords().Len())

	// Verify first log record
	lr := sl.LogRecords().At(0)
	assert.Equal(t, "high_latency:firing", lr.Body().AsString())
	assert.Equal(t, "critical", lr.SeverityText())

	// Check attributes
	attrs := lr.Attributes()
	v, ok := attrs.Get("service")
	assert.True(t, ok)
	assert.Equal(t, "api", v.AsString())

	v, ok = attrs.Get("env")
	assert.True(t, ok)
	assert.Equal(t, "prod", v.AsString())

	v, ok = attrs.Get("window")
	assert.True(t, ok)
	assert.Equal(t, "5m", v.AsString())

	v, ok = attrs.Get("for")
	assert.True(t, ok)
	assert.Equal(t, "1m", v.AsString())

	v, ok = attrs.Get("value")
	assert.True(t, ok)
	assert.Equal(t, 500.5, v.Double())

	// Verify second log record
	lr = sl.LogRecords().At(1)
	assert.Equal(t, "error_rate:resolved", lr.Body().AsString())
	assert.Equal(t, "warning", lr.SeverityText())

	// Verify third log record (with empty labels)
	lr = sl.LogRecords().At(2)
	assert.Equal(t, "service_down:no_data", lr.Body().AsString())
	assert.Equal(t, "info", lr.SeverityText())

	// Should still have window, for, value attributes
	v, ok = attrs.Get("window")
	assert.True(t, ok)
}

func TestBuildLogs_Empty(t *testing.T) {
	events := []alertEvent{}

	ld := buildLogs(events)
	require.NotNil(t, ld)

	assert.Equal(t, 1, ld.ResourceLogs().Len())
	rl := ld.ResourceLogs().At(0)
	assert.Equal(t, 1, rl.ScopeLogs().Len())
	sl := rl.ScopeLogs().At(0)
	assert.Equal(t, 0, sl.LogRecords().Len())
}

func TestBuildMetrics(t *testing.T) {
	metrics := []stateMetric{
		{
			Rule: "high_latency",
			Labels: map[string]string{
				"service": "api",
				"env":     "prod",
			},
			Severity:  "critical",
			Active:    1,
			LastValue: 550.5,
		},
		{
			Rule: "error_rate",
			Labels: map[string]string{
				"service": "db",
				"env":     "staging",
			},
			Severity:  "warning",
			Active:    0,
			LastValue: 0.02,
		},
		{
			Rule:      "cpu_usage",
			Labels:    map[string]string{"host": "server1"},
			Severity:  "info",
			Active:    1,
			LastValue: 0.85,
		},
	}

	md := buildMetrics(metrics)
	require.NotNil(t, md)

	// Check resource metrics
	assert.Equal(t, 1, md.ResourceMetrics().Len())
	rm := md.ResourceMetrics().At(0)

	// Check scope metrics
	assert.Equal(t, 1, rm.ScopeMetrics().Len())
	sm := rm.ScopeMetrics().At(0)

	// Should have 2 metrics: active gauge and last_value gauge
	assert.Equal(t, 2, sm.Metrics().Len())

	// Check active gauge
	activeMetric := sm.Metrics().At(0)
	assert.Equal(t, "otel_alert_active", activeMetric.Name())
	assert.Equal(t, pmetric.MetricTypeGauge, activeMetric.Type())

	activeGauge := activeMetric.Gauge()
	assert.Equal(t, 3, activeGauge.DataPoints().Len())

	// Check first active data point
	dp := activeGauge.DataPoints().At(0)
	assert.Equal(t, int64(1), dp.IntValue())

	// Check attributes
	v, ok := dp.Attributes().Get("service")
	assert.True(t, ok)
	assert.Equal(t, "api", v.AsString())

	v, ok = dp.Attributes().Get("env")
	assert.True(t, ok)
	assert.Equal(t, "prod", v.AsString())

	v, ok = dp.Attributes().Get("rule")
	assert.True(t, ok)
	assert.Equal(t, "high_latency", v.AsString())

	v, ok = dp.Attributes().Get("severity")
	assert.True(t, ok)
	assert.Equal(t, "critical", v.AsString())

	// Check second active data point (inactive)
	dp = activeGauge.DataPoints().At(1)
	assert.Equal(t, int64(0), dp.IntValue())

	// Check last_value gauge
	valueMetric := sm.Metrics().At(1)
	assert.Equal(t, "otel_alert_last_value", valueMetric.Name())
	assert.Equal(t, pmetric.MetricTypeGauge, valueMetric.Type())

	valueGauge := valueMetric.Gauge()
	assert.Equal(t, 3, valueGauge.DataPoints().Len())

	// Check first value data point
	dp = valueGauge.DataPoints().At(0)
	assert.Equal(t, 550.5, dp.DoubleValue())

	// Check second value data point
	dp = valueGauge.DataPoints().At(1)
	assert.Equal(t, 0.02, dp.DoubleValue())

	// Check third value data point
	dp = valueGauge.DataPoints().At(2)
	assert.Equal(t, 0.85, dp.DoubleValue())
}

func TestBuildMetrics_Empty(t *testing.T) {
	metrics := []stateMetric{}

	md := buildMetrics(metrics)
	require.NotNil(t, md)

	assert.Equal(t, 1, md.ResourceMetrics().Len())
	rm := md.ResourceMetrics().At(0)
	assert.Equal(t, 1, rm.ScopeMetrics().Len())
	sm := rm.ScopeMetrics().At(0)
	assert.Equal(t, 2, sm.Metrics().Len())

	// Both metrics should have 0 data points
	assert.Equal(t, 0, sm.Metrics().At(0).Gauge().DataPoints().Len())
	assert.Equal(t, 0, sm.Metrics().At(1).Gauge().DataPoints().Len())
}

func TestBuildLogs_Timestamp(t *testing.T) {
	events := []alertEvent{
		{
			Rule:     "test",
			State:    "firing",
			Severity: "info",
			Labels:   map[string]string{},
			Value:    1.0,
			Window:   "1m",
			For:      "0s",
		},
	}

	beforeTime := time.Now()
	ld := buildLogs(events)
	afterTime := time.Now()

	lr := ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	timestamp := lr.Timestamp()

	// Timestamp should be between before and after
	assert.True(t, timestamp.AsTime().After(beforeTime) || timestamp.AsTime().Equal(beforeTime))
	assert.True(t, timestamp.AsTime().Before(afterTime) || timestamp.AsTime().Equal(afterTime))
}

func TestBuildMetrics_Timestamp(t *testing.T) {
	metrics := []stateMetric{
		{
			Rule:      "test",
			Labels:    map[string]string{},
			Severity:  "info",
			Active:    1,
			LastValue: 1.0,
		},
	}

	beforeTime := time.Now()
	md := buildMetrics(metrics)
	afterTime := time.Now()

	dp := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints().At(0)
	timestamp := dp.Timestamp()

	// Timestamp should be between before and after
	assert.True(t, timestamp.AsTime().After(beforeTime) || timestamp.AsTime().Equal(beforeTime))
	assert.True(t, timestamp.AsTime().Before(afterTime) || timestamp.AsTime().Equal(afterTime))
}

func TestBuildLogs_VariousStates(t *testing.T) {
	states := []string{"firing", "resolved", "no_data"}

	for _, state := range states {
		t.Run(state, func(t *testing.T) {
			events := []alertEvent{
				{
					Rule:     "test_rule",
					State:    state,
					Severity: "warning",
					Labels:   map[string]string{"test": "label"},
					Value:    42.0,
					Window:   "5m",
					For:      "1m",
				},
			}

			ld := buildLogs(events)
			lr := ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)

			expectedBody := "test_rule:" + state
			assert.Equal(t, expectedBody, lr.Body().AsString())
		})
	}
}

func TestBuildMetrics_VariousActiveStates(t *testing.T) {
	tests := []struct {
		name      string
		active    int64
		lastValue float64
	}{
		{"active_high", 1, 100.0},
		{"active_low", 1, 0.01},
		{"inactive_zero", 0, 0.0},
		{"inactive_with_value", 0, 50.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metrics := []stateMetric{
				{
					Rule:      "test",
					Labels:    map[string]string{},
					Severity:  "info",
					Active:    tt.active,
					LastValue: tt.lastValue,
				},
			}

			md := buildMetrics(metrics)

			// Check active gauge
			activeDP := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints().At(0)
			assert.Equal(t, tt.active, activeDP.IntValue())

			// Check value gauge
			valueDP := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(1).Gauge().DataPoints().At(0)
			assert.Equal(t, tt.lastValue, valueDP.DoubleValue())
		})
	}
}
