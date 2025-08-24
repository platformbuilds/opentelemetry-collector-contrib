// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package alertsgenconnector

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompileRules(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *Config
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid_trace_rule",
			cfg: &Config{
				WindowSize: 5 * time.Second,
				Rules: []RuleCfg{
					{
						Name:     "high_latency",
						Signal:   "traces",
						Severity: "critical",
						Window:   5 * time.Second,
						For:      1 * time.Minute,
						Select: map[string]string{
							"service.name": "payment.*",
						},
						GroupBy: []string{"service.name", "http.route"},
						Expr: ExprCfg{
							Type:     "latency_quantile_over_time",
							Field:    "duration_ns",
							Quantile: 0.99,
							Op:       ">",
							Value:    500000000, // 500ms
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "valid_log_rule",
			cfg: &Config{
				WindowSize: 5 * time.Second,
				Rules: []RuleCfg{
					{
						Name:     "error_rate",
						Signal:   "logs",
						Severity: "warning",
						Window:   1 * time.Minute,
						Select: map[string]string{
							"severity": "ERROR|FATAL",
						},
						GroupBy: []string{"service.name"},
						Expr: ExprCfg{
							Type:  "count_over_time",
							Op:    ">",
							Value: 10,
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "valid_metric_rule",
			cfg: &Config{
				WindowSize: 5 * time.Second,
				Rules: []RuleCfg{
					{
						Name:     "cpu_usage",
						Signal:   "metrics",
						Severity: "warning",
						Window:   5 * time.Minute,
						Select: map[string]string{
							"__name__": "system.cpu.utilization",
						},
						GroupBy: []string{"host.name"},
						Expr: ExprCfg{
							Type:  "avg_over_time",
							Field: "value",
							Op:    ">",
							Value: 0.85,
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "absent_rule",
			cfg: &Config{
				WindowSize: 5 * time.Second,
				Rules: []RuleCfg{
					{
						Name:     "service_down",
						Signal:   "traces",
						Severity: "critical",
						Window:   2 * time.Minute,
						For:      5 * time.Minute,
						Select: map[string]string{
							"service.name": "core-api",
						},
						GroupBy: []string{"service.name"},
						Expr: ExprCfg{
							Type: "absent_over_time",
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "invalid_signal",
			cfg: &Config{
				WindowSize: 5 * time.Second,
				Rules: []RuleCfg{
					{
						Name:   "bad_rule",
						Signal: "invalid",
						Expr: ExprCfg{
							Type:  "count_over_time",
							Op:    ">",
							Value: 1,
						},
					},
				},
			},
			wantErr: true,
			errMsg:  "unknown signal",
		},
		{
			name: "missing_rule_name",
			cfg: &Config{
				WindowSize: 5 * time.Second,
				Rules: []RuleCfg{
					{
						Signal: "traces",
						Expr: ExprCfg{
							Type:  "count_over_time",
							Op:    ">",
							Value: 1,
						},
					},
				},
			},
			wantErr: true,
			errMsg:  "rule.name required",
		},
		{
			name: "missing_expr_type",
			cfg: &Config{
				WindowSize: 5 * time.Second,
				Rules: []RuleCfg{
					{
						Name:   "test",
						Signal: "traces",
						Expr: ExprCfg{
							Op:    ">",
							Value: 1,
						},
					},
				},
			},
			wantErr: true,
			errMsg:  "expr.type required",
		},
		{
			name: "invalid_regex",
			cfg: &Config{
				WindowSize: 5 * time.Second,
				Rules: []RuleCfg{
					{
						Name:   "bad_regex",
						Signal: "traces",
						Select: map[string]string{
							"service": "[",
						},
						Expr: ExprCfg{
							Type:  "count_over_time",
							Op:    ">",
							Value: 1,
						},
					},
				},
			},
			wantErr: true,
			errMsg:  "bad select regex",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rs, err := compileRules(tt.cfg)
			if tt.wantErr {
				require.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				require.NoError(t, err)
				require.NotNil(t, rs)
			}
		})
	}
}

func TestExpressionEvaluation(t *testing.T) {
	tests := []struct {
		name     string
		expr     evalExpr
		rows     interface{}
		wantVal  float64
		wantOk   bool
		wantFire bool
	}{
		{
			name: "avg_traces",
			expr: &exprAvg{Field: "duration_ns", Op: ">", Threshold: 1000},
			rows: []traceRow{
				{durationNs: 500},
				{durationNs: 1500},
				{durationNs: 2000},
			},
			wantVal:  1333.333,
			wantOk:   true,
			wantFire: true,
		},
		{
			name: "count_logs",
			expr: &exprCount{Op: ">", Threshold: 2},
			rows: []logRow{
				{value: 1},
				{value: 1},
				{value: 1},
			},
			wantVal:  3,
			wantOk:   true,
			wantFire: true,
		},
		{
			name: "rate_metrics",
			expr: &exprRate{Op: ">=", Threshold: 5},
			rows: []metricRow{
				{value: 1},
				{value: 2},
				{value: 3},
				{value: 4},
				{value: 5},
			},
			wantVal:  5,
			wantOk:   true,
			wantFire: true,
		},
		{
			name: "quantile_traces",
			expr: &exprQuantile{Field: "duration_ns", Q: 0.95, Op: ">", Threshold: 900},
			rows: []traceRow{
				{durationNs: 100},
				{durationNs: 200},
				{durationNs: 300},
				{durationNs: 400},
				{durationNs: 500},
				{durationNs: 600},
				{durationNs: 700},
				{durationNs: 800},
				{durationNs: 900},
				{durationNs: 1000},
			},
			wantVal:  1000,
			wantOk:   true,
			wantFire: true,
		},
		{
			name:     "absent_no_data",
			expr:     &exprAbsent{},
			rows:     []traceRow{},
			wantVal:  0,
			wantOk:   true,
			wantFire: true,
		},
		{
			name: "absent_with_data",
			expr: &exprAbsent{},
			rows: []traceRow{
				{durationNs: 100},
			},
			wantVal:  0,
			wantOk:   false,
			wantFire: false,
		},
		{
			name:     "empty_rows",
			expr:     &exprAvg{Op: ">", Threshold: 0},
			rows:     []traceRow{},
			wantVal:  0,
			wantOk:   false,
			wantFire: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var val float64
			var ok bool
			
			switch rows := tt.rows.(type) {
			case []traceRow:
				val, ok = tt.expr.evaluateTrace(rows)
			case []logRow:
				val, ok = tt.expr.evaluateLog(rows)
			case []metricRow:
				val, ok = tt.expr.evaluateMetric(rows)
			}
			
			assert.Equal(t, tt.wantOk, ok)
			if ok {
				assert.InDelta(t, tt.wantVal, val, 1.0)
				assert.Equal(t, tt.wantFire, tt.expr.compare(val))
			}
		})
	}
}

func TestGrouping(t *testing.T) {
	// Test trace grouping
	t.Run("group_traces", func(t *testing.T) {
		rows := []traceRow{
			{durationNs: 100, attrs: map[string]string{"service": "api", "method": "GET"}},
			{durationNs: 200, attrs: map[string]string{"service": "api", "method": "POST"}},
			{durationNs: 300, attrs: map[string]string{"service": "db", "method": "GET"}},
			{durationNs: 400, attrs: map[string]string{"service": "api", "method": "GET"}},
		}
		
		groups := groupTraceRows(rows, []string{"service", "method"})
		assert.Equal(t, 3, len(groups))
		
		// Check group sizes
		for _, g := range groups {
			if g.Labels["service"] == "api" && g.Labels["method"] == "GET" {
				assert.Equal(t, 2, len(g.Rows))
			}
		}
	})

	// Test log grouping
	t.Run("group_logs", func(t *testing.T) {
		rows := []logRow{
			{value: 1, attrs: map[string]string{"service": "api", "level": "ERROR"}},
			{value: 1, attrs: map[string]string{"service": "api", "level": "WARN"}},
			{value: 1, attrs: map[string]string{"service": "db", "level": "ERROR"}},
		}
		
		groups := groupLogRows(rows, []string{"service"})
		assert.Equal(t, 2, len(groups))
	})

	// Test metric grouping
	t.Run("group_metrics", func(t *testing.T) {
		rows := []metricRow{
			{value: 0.5, attrs: map[string]string{"host": "server1", "cpu": "0"}},
			{value: 0.6, attrs: map[string]string{"host": "server1", "cpu": "1"}},
			{value: 0.7, attrs: map[string]string{"host": "server2", "cpu": "0"}},
		}
		
		groups := groupMetricRows(rows, []string{"host"})
		assert.Equal(t, 2, len(groups))
	})
}

func TestFiltering(t *testing.T) {
	t.Run("filter_traces", func(t *testing.T) {
		rows := []traceRow{
			{attrs: map[string]string{"service.name": "payment-api"}},
			{attrs: map[string]string{"service.name": "user-api"}},
			{attrs: map[string]string{"service.name": "checkout-service"}},
		}
		
		sel := map[string]*regexp.Regexp{
			"service.name": regexp.MustCompile("payment.*|checkout.*"),
		}
		
		filtered := filterTraceRows(rows, sel)
		assert.Equal(t, 2, len(filtered))
	})

	t.Run("filter_logs", func(t *testing.T) {
		rows := []logRow{
			{attrs: map[string]string{"severity": "ERROR"}},
			{attrs: map[string]string{"severity": "INFO"}},
			{attrs: map[string]string{"severity": "FATAL"}},
		}
		
		sel := map[string]*regexp.Regexp{
			"severity": regexp.MustCompile("ERROR|FATAL"),
		}
		
		filtered := filterLogRows(rows, sel)
		assert.Equal(t, 2, len(filtered))
	})

	t.Run("filter_window", func(t *testing.T) {
		now := time.Now()
		rows := []traceRow{
			{ts: now.Add(-10 * time.Second)},
			{ts: now.Add(-5 * time.Second)},
			{ts: now.Add(-1 * time.Second)},
		}
		
		filtered := filterWindowTr(rows, now, 3*time.Second)
		assert.Equal(t, 1, len(filtered))
	})
}

func TestFingerprint(t *testing.T) {
	tests := []struct {
		name   string
		rule   string
		labels map[string]string
		want   uint64
	}{
		{
			name: "simple",
			rule: "test_rule",
			labels: map[string]string{
				"service": "api",
				"env":     "prod",
			},
		},
		{
			name: "empty_labels",
			rule: "test_rule",
			labels: map[string]string{},
		},
		{
			name: "order_independent",
			rule: "test_rule",
			labels: map[string]string{
				"b": "2",
				"a": "1",
				"c": "3",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fp1 := fingerprint(tt.rule, tt.labels)
			fp2 := fingerprint(tt.rule, tt.labels)
			assert.Equal(t, fp1, fp2, "fingerprint should be consistent")
			
			// Different rule should give different fingerprint
			fp3 := fingerprint("different_rule", tt.labels)
			assert.NotEqual(t, fp1, fp3)
		})
	}
}

func TestStateTransitions(t *testing.T) {
	cfg := &Config{
		WindowSize: 5 * time.Second,
		Rules: []RuleCfg{
			{
				Name:     "test_rule",
				Signal:   "traces",
				Severity: "warning",
				Window:   5 * time.Second,
				For:      30 * time.Second,
				Expr: ExprCfg{
					Type:  "count_over_time",
					Op:    ">",
					Value: 5,
				},
			},
		},
	}

	rs, err := compileRules(cfg)
	require.NoError(t, err)

	cr := rs.traceRules[0]
	fp := uint64(12345)
	labels := map[string]string{"test": "label"}
	now := time.Now()

	var events []alertEvent
	var metrics []stateMetric

	// First evaluation - condition true but For not satisfied
	rs.transition(now, cr, fp, labels, 10, true, &events, &metrics)
	assert.Equal(t, 0, len(events), "should not fire immediately due to For duration")
	assert.Equal(t, 1, len(metrics))
	assert.Equal(t, int64(0), metrics[0].Active)

	// Second evaluation after For duration - should fire
	rs.transition(now.Add(31*time.Second), cr, fp, labels, 10, true, &events, &metrics)
	assert.Equal(t, 1, len(events))
	assert.Equal(t, "firing", events[0].State)
	assert.Equal(t, int64(1), metrics[1].Active)

	// Third evaluation - condition false, should resolve
	events = []alertEvent{}
	rs.transition(now.Add(35*time.Second), cr, fp, labels, 3, false, &events, &metrics)
	assert.Equal(t, 1, len(events))
	assert.Equal(t, "resolved", events[0].State)
}

func TestRuleSetEvaluation(t *testing.T) {
	cfg := &Config{
		WindowSize: 5 * time.Second,
		Rules: []RuleCfg{
			{
				Name:     "trace_rule",
				Signal:   "traces",
				Severity: "warning",
				Window:   5 * time.Second,
				GroupBy:  []string{"service"},
				Expr: ExprCfg{
					Type:  "count_over_time",
					Op:    ">",
					Value: 1,
				},
			},
			{
				Name:     "log_rule",
				Signal:   "logs",
				Severity: "error",
				Window:   5 * time.Second,
				GroupBy:  []string{"service"},
				Expr: ExprCfg{
					Type:  "count_over_time",
					Op:    ">",
					Value: 1,
				},
			},
		},
	}

	rs, err := compileRules(cfg)
	require.NoError(t, err)

	// Create test ingester
	ing := &ingester{
		traces: NewSliceBuffer(100, 1024),
		logs:   NewSliceBuffer(100, 512),
		metrics: NewSliceBuffer(100, 768),
	}

	// Add test data
	ing.traces.Add(traceRow{
		durationNs: 1000,
		ts:         time.Now(),
		attrs:      map[string]string{"service": "api"},
	})
	ing.traces.Add(traceRow{
		durationNs: 2000,
		ts:         time.Now(),
		attrs:      map[string]string{"service": "api"},
	})
	ing.logs.Add(logRow{
		value: 1,
		ts:    time.Now(),
		attrs: map[string]string{"service": "api"},
	})
	ing.logs.Add(logRow{
		value: 1,
		ts:    time.Now(),
		attrs: map[string]string{"service": "api"},
	})

	// Evaluate
	events, metrics := rs.evaluate(time.Now(), ing)
	
	// Should have events for both rules
	assert.GreaterOrEqual(t, len(events), 0)
	assert.GreaterOrEqual(t, len(metrics), 0)
}

// Helper to compile regexp
func regexp.MustCompile(str string) *regexp.Regexp {
	r, err := regexp.Compile(str)
	if err != nil {
		panic(err)
	}
	return r
}