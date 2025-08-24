// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package alertsgenconnector

import (
	"testing"
	"time"

	conn "github.com/open-telemetry/opentelemetry-collector-contrib/connector/alertsgenconnector"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateDefaultConfig(t *testing.T) {
	cfg := conn.CreateDefaultConfig().(*conn.Config)

	assert.Equal(t, 5*time.Second, cfg.WindowSize)
	assert.Equal(t, 5*time.Second, cfg.Step)
	assert.Equal(t, "default", cfg.InstanceID)

	require.NotNil(t, cfg.TSDB)
	assert.Equal(t, 30*time.Second, cfg.TSDB.QueryTimeout)
	assert.Equal(t, 10*time.Second, cfg.TSDB.WriteTimeout)
	assert.True(t, cfg.TSDB.EnableRemoteWrite)

	assert.Contains(t, cfg.Dedup.FingerprintLabels, "alertname")
	assert.Contains(t, cfg.Dedup.FingerprintLabels, "severity")
	assert.Equal(t, 30*time.Second, cfg.Dedup.Window)
	assert.True(t, cfg.Dedup.EnableTSDBDedup)

	assert.Equal(t, 100, cfg.Limits.Storm.MaxTransitionsPerMinute)
	assert.Equal(t, 20, cfg.Limits.Cardinality.MaxLabels)
	assert.Equal(t, 128, cfg.Limits.Cardinality.MaxValLen)

	assert.InDelta(t, 0.10, cfg.Memory.MaxMemoryPercent, 1e-9)
	assert.True(t, cfg.Memory.EnableAdaptiveScaling)
	assert.InDelta(t, 0.8, cfg.Memory.ScaleUpThreshold, 1e-9)
	assert.False(t, cfg.Memory.UseRingBuffers)
}

func TestConfigValidationMatrix(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(*conn.Config)
		expectError string
	}{
		{
			name: "valid config",
			setup: func(cfg *conn.Config) {
				cfg.WindowSize = 5 * time.Second
				cfg.Rules = []conn.RuleCfg{{Name: "ok", Signal: "traces", Expr: conn.ExprCfg{Type: "avg_over_time", Field: "duration", Op: ">", Value: 100}}}
				cfg.TSDB = &conn.TSDBConfig{QueryURL: "http://prometheus:9090"}
			},
		},
		{
			name: "zero window size",
			setup: func(cfg *conn.Config) {
				cfg.WindowSize = 0
				cfg.Rules = []conn.RuleCfg{{Name: "r", Signal: "traces", Expr: conn.ExprCfg{Type: "avg_over_time", Field: "duration", Op: ">", Value: 100}}}
				cfg.TSDB = &conn.TSDBConfig{QueryURL: "http://prometheus:9090"}
			},
			expectError: "window must be > 0",
		},
		{
			name: "missing tsdb url",
			setup: func(cfg *conn.Config) {
				cfg.WindowSize = 5 * time.Second
				cfg.Rules = []conn.RuleCfg{{Name: "r", Signal: "traces", Expr: conn.ExprCfg{Type: "avg_over_time", Field: "duration", Op: ">", Value: 100}}}
				cfg.TSDB = &conn.TSDBConfig{} // missing QueryURL
			},
			expectError: "tsdb.query_url required",
		},
		{
			name: "empty rule name",
			setup: func(cfg *conn.Config) {
				cfg.WindowSize = 5 * time.Second
				cfg.Rules = []conn.RuleCfg{{Name: "", Signal: "traces", Expr: conn.ExprCfg{Type: "avg_over_time", Field: "duration", Op: ">", Value: 100}}}
				cfg.TSDB = &conn.TSDBConfig{QueryURL: "http://prometheus:9090"}
			},
			expectError: "name required",
		},
		{
			name: "duplicate rule name",
			setup: func(cfg *conn.Config) {
				cfg.WindowSize = 5 * time.Second
				cfg.Rules = []conn.RuleCfg{
					{Name: "dup", Signal: "traces", Expr: conn.ExprCfg{Type: "avg_over_time", Field: "duration", Op: ">", Value: 100}},
					{Name: "dup", Signal: "logs", Expr: conn.ExprCfg{Type: "count_over_time", Field: "lines", Op: ">", Value: 5}},
				}
				cfg.TSDB = &conn.TSDBConfig{QueryURL: "http://prometheus:9090"}
			},
			expectError: `duplicate rule name "dup"`,
		},
		{
			name: "invalid signal",
			setup: func(cfg *conn.Config) {
				cfg.WindowSize = 5 * time.Second
				cfg.Rules = []conn.RuleCfg{{Name: "bad", Signal: "invalid", Expr: conn.ExprCfg{Type: "avg_over_time", Field: "duration", Op: ">", Value: 1}}}
				cfg.TSDB = &conn.TSDBConfig{QueryURL: "http://prometheus:9090"}
			},
			expectError: "invalid signal",
		},
		{
			name: "missing expr type",
			setup: func(cfg *conn.Config) {
				cfg.WindowSize = 5 * time.Second
				cfg.Rules = []conn.RuleCfg{{Name: "missing_expr_type", Signal: "traces", Expr: conn.ExprCfg{Field: "duration", Op: ">", Value: 100}}}
				cfg.TSDB = &conn.TSDBConfig{QueryURL: "http://prometheus:9090"}
			},
			expectError: "expr.type required",
		},
		{
			name: "missing operator",
			setup: func(cfg *conn.Config) {
				cfg.WindowSize = 5 * time.Second
				cfg.Rules = []conn.RuleCfg{{Name: "missing_operator", Signal: "traces", Expr: conn.ExprCfg{Type: "avg_over_time", Field: "duration", Value: 100}}}
				cfg.TSDB = &conn.TSDBConfig{QueryURL: "http://prometheus:9090"}
			},
			expectError: "expr.op required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := conn.CreateDefaultConfig().(*conn.Config)
			tt.setup(cfg)
			err := cfg.Validate()
			if tt.expectError == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectError)
			}
		})
	}
}

func TestConfigAppliesFallbacks(t *testing.T) {
	cfg := &conn.Config{
		WindowSize: 5 * time.Second,
		Rules: []conn.RuleCfg{{
			Name:   "rule",
			Signal: "traces",
			Expr:   conn.ExprCfg{Type: "avg_over_time", Field: "duration", Op: ">", Value: 100},
		}},
		TSDB: &conn.TSDBConfig{QueryURL: "http://prometheus:9090"},
	}

	err := cfg.Validate()
	require.NoError(t, err)

	assert.Equal(t, cfg.WindowSize, cfg.Step)
	assert.Equal(t, "default", cfg.InstanceID)
	assert.Equal(t, "warning", cfg.Rules[0].Severity)
	assert.True(t, cfg.Rules[0].Enabled)
}

func TestTSDBConfigValidation(t *testing.T) {
	tests := []struct {
		name        string
		tsdb        *conn.TSDBConfig
		expectError string
	}{
		{
			name: "valid",
			tsdb: &conn.TSDBConfig{
				QueryURL:             "http://prometheus:9090",
				RemoteWriteURL:       "http://prometheus:9090/api/v1/write",
				EnableRemoteWrite:    true,
				QueryTimeout:         30 * time.Second,
				WriteTimeout:         10 * time.Second,
				DedupWindow:          30 * time.Second,
				RemoteWriteBatchSize: 1000,
			},
		},
		{
			name:        "missing query url",
			tsdb:        &conn.TSDBConfig{},
			expectError: "tsdb.query_url required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := conn.CreateDefaultConfig().(*conn.Config)
			cfg.TSDB = tt.tsdb
			cfg.Rules = []conn.RuleCfg{{Name: "r", Signal: "traces", Expr: conn.ExprCfg{Type: "avg_over_time", Field: "duration", Op: ">", Value: 1}}}
			err := cfg.Validate()
			if tt.expectError == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectError)
			}
		})
	}
}

func TestMemoryLimitsValidation(t *testing.T) {
	tests := []struct {
		name        string
		mem         *conn.MemoryConfig
		expectError string
	}{
		{
			name: "valid",
			mem:  &conn.MemoryConfig{MaxMemoryPercent: 0.10, EnableAdaptiveScaling: true, ScaleUpThreshold: 0.8},
		},
		{
			name:        "invalid percent",
			mem:         &conn.MemoryConfig{MaxMemoryPercent: 1.5},
			expectError: "memory.max_percent must be between 0 and 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := conn.CreateDefaultConfig().(*conn.Config)
			cfg.Rules = []conn.RuleCfg{{Name: "r", Signal: "traces", Expr: conn.ExprCfg{Type: "avg_over_time", Field: "duration", Op: ">", Value: 1}}}
			cfg.TSDB = &conn.TSDBConfig{QueryURL: "http://prometheus:9090"}
			cfg.Memory = *tt.mem

			err := cfg.Validate()
			if tt.expectError == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectError)
			}
		})
	}
}
