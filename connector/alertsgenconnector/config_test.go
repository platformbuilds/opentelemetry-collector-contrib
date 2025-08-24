// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package alertsgenconnector

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateDefaultConfig(t *testing.T) {
	cfg := CreateDefaultConfig()
	assert.NotNil(t, cfg)

	c := cfg.(*Config)
	assert.Equal(t, 5*time.Second, c.WindowSize)
	assert.Equal(t, 5*time.Second, c.Step)
	assert.Equal(t, "default", c.InstanceID)
	assert.NotNil(t, c.TSDB)
	assert.NotNil(t, c.Memory)
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *Config
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid_config",
			cfg: &Config{
				WindowSize: 5 * time.Second,
				Step:       5 * time.Second,
				InstanceID: "test",
				TSDB: &TSDBConfig{
					QueryURL:     "http://prometheus:9090",
					QueryTimeout: 30 * time.Second,
				},
				Memory: MemoryConfig{
					MaxMemoryPercent:          0.1,
					ScaleUpThreshold:          0.8,
					ScaleDownThreshold:        0.3,
					MemoryPressureThreshold:   0.85,
					SamplingRateUnderPressure: 0.5,
				},
				Rules: []RuleCfg{
					{
						Name:     "test_rule",
						Signal:   "traces",
						Severity: "warning",
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
			name: "invalid_window",
			cfg: &Config{
				WindowSize: 0,
				TSDB:       &TSDBConfig{QueryURL: "http://test"},
				Memory:     MemoryConfig{MaxMemoryPercent: 0.1},
			},
			wantErr: true,
			errMsg:  "window must be > 0",
		},
		{
			name: "missing_tsdb",
			cfg: &Config{
				WindowSize: 5 * time.Second,
				Memory:     MemoryConfig{MaxMemoryPercent: 0.1},
			},
			wantErr: true,
			errMsg:  "tsdb required",
		},
		{
			name: "invalid_memory_percent",
			cfg: &Config{
				WindowSize: 5 * time.Second,
				TSDB:       &TSDBConfig{QueryURL: "http://test"},
				Memory:     MemoryConfig{MaxMemoryPercent: 1.5},
			},
			wantErr: true,
			errMsg:  "memory.max_percent must be between 0 and 1",
		},
		{
			name: "invalid_scale_up_threshold",
			cfg: &Config{
				WindowSize: 5 * time.Second,
				TSDB:       &TSDBConfig{QueryURL: "http://test"},
				Memory: MemoryConfig{
					MaxMemoryPercent: 0.1,
					ScaleUpThreshold: 1.5,
				},
			},
			wantErr: true,
			errMsg:  "memory.scale_up_threshold must be between 0 and 1",
		},
		{
			name: "duplicate_rule_names",
			cfg: &Config{
				WindowSize: 5 * time.Second,
				TSDB:       &TSDBConfig{QueryURL: "http://test"},
				Memory:     MemoryConfig{MaxMemoryPercent: 0.1},
				Rules: []RuleCfg{
					{
						Name:   "duplicate",
						Signal: "traces",
						Expr:   ExprCfg{Type: "count_over_time", Op: ">", Value: 1},
					},
					{
						Name:   "duplicate",
						Signal: "logs",
						Expr:   ExprCfg{Type: "count_over_time", Op: ">", Value: 1},
					},
				},
			},
			wantErr: true,
			errMsg:  "duplicate rule name",
		},
		{
			name: "auto_step",
			cfg: &Config{
				WindowSize: 10 * time.Second,
				Step:       0, // Should default to WindowSize
				TSDB:       &TSDBConfig{QueryURL: "http://test"},
				Memory:     MemoryConfig{MaxMemoryPercent: 0.1},
			},
			wantErr: false,
		},
		{
			name: "default_instance_id",
			cfg: &Config{
				WindowSize: 5 * time.Second,
				InstanceID: "", // Should default to "default"
				TSDB:       &TSDBConfig{QueryURL: "http://test"},
				Memory:     MemoryConfig{MaxMemoryPercent: 0.1},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr {
				require.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				require.NoError(t, err)

				// Check defaults were applied
				if tt.name == "auto_step" {
					assert.Equal(t, tt.cfg.WindowSize, tt.cfg.Step)
				}
				if tt.name == "default_instance_id" {
					assert.Equal(t, "default", tt.cfg.InstanceID)
				}
			}
		})
	}
}

func TestTSDBConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     TSDBConfig
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid",
			cfg: TSDBConfig{
				QueryURL:                 "http://prometheus:9090",
				RemoteWriteURL:           "http://prometheus:9090/write",
				QueryTimeout:             30 * time.Second,
				WriteTimeout:             10 * time.Second,
				RemoteWriteBatchSize:     1000,
				RemoteWriteFlushInterval: 5 * time.Second,
			},
			wantErr: false,
		},
		{
			name: "missing_query_url",
			cfg: TSDBConfig{
				QueryTimeout: 30 * time.Second,
			},
			wantErr: true,
			errMsg:  "tsdb.query_url required",
		},
		{
			name: "negative_batch_size",
			cfg: TSDBConfig{
				QueryURL:             "http://test",
				RemoteWriteBatchSize: -1,
			},
			wantErr: true,
			errMsg:  "tsdb.remote_write_batch_size must be >= 0",
		},
		{
			name: "negative_flush_interval",
			cfg: TSDBConfig{
				QueryURL:                 "http://test",
				RemoteWriteFlushInterval: -1 * time.Second,
			},
			wantErr: true,
			errMsg:  "tsdb.remote_write_flush_interval must be >= 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.validate()
			if tt.wantErr {
				require.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRuleConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		rule    RuleCfg
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid_trace_rule",
			rule: RuleCfg{
				Name:     "test",
				Signal:   "traces",
				Severity: "warning",
				Expr: ExprCfg{
					Type:  "count_over_time",
					Op:    ">",
					Value: 10,
				},
			},
			wantErr: false,
		},
		{
			name: "valid_log_rule",
			rule: RuleCfg{
				Name:   "test",
				Signal: "logs",
				Expr: ExprCfg{
					Type:  "rate_over_time",
					Op:    ">=",
					Value: 5,
				},
			},
			wantErr: false,
		},
		{
			name: "valid_metric_rule",
			rule: RuleCfg{
				Name:   "test",
				Signal: "metrics",
				Expr: ExprCfg{
					Type:  "avg_over_time",
					Op:    "<",
					Value: 100,
				},
			},
			wantErr: false,
		},
		{
			name: "missing_name",
			rule: RuleCfg{
				Signal: "traces",
				Expr: ExprCfg{
					Type:  "count_over_time",
					Op:    ">",
					Value: 10,
				},
			},
			wantErr: true,
			errMsg:  "name required",
		},
		{
			name: "invalid_signal",
			rule: RuleCfg{
				Name:   "test",
				Signal: "invalid",
				Expr: ExprCfg{
					Type:  "count_over_time",
					Op:    ">",
					Value: 10,
				},
			},
			wantErr: true,
			errMsg:  "invalid signal",
		},
		{
			name: "missing_expr",
			rule: RuleCfg{
				Name:   "test",
				Signal: "traces",
				Expr:   ExprCfg{},
			},
			wantErr: true,
			errMsg:  "expr.type required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.rule.validate()
			if tt.wantErr {
				require.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestExprConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		expr    ExprCfg
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid_avg",
			expr: ExprCfg{
				Type:  "avg_over_time",
				Field: "value",
				Op:    ">",
				Value: 100,
			},
			wantErr: false,
		},
		{
			name: "valid_quantile",
			expr: ExprCfg{
				Type:     "quantile_over_time",
				Field:    "duration",
				Quantile: 0.99,
				Op:       ">",
				Value:    500,
			},
			wantErr: false,
		},
		{
			name: "valid_rate",
			expr: ExprCfg{
				Type:         "rate",
				Op:           "!=",
				Value:        0,
				RateDuration: 1 * time.Minute,
			},
			wantErr: false,
		},
		{
			name: "missing_type",
			expr: ExprCfg{
				Op:    ">",
				Value: 10,
			},
			wantErr: true,
			errMsg:  "expr.type required",
		},
		{
			name: "missing_op",
			expr: ExprCfg{
				Type:  "count_over_time",
				Value: 10,
			},
			wantErr: true,
			errMsg:  "expr.op required",
		},
		{
			name: "invalid_op",
			expr: ExprCfg{
				Type:  "count_over_time",
				Op:    "~=",
				Value: 10,
			},
			wantErr: true,
			errMsg:  "invalid expr.op",
		},
		{
			name: "invalid_quantile_low",
			expr: ExprCfg{
				Type:     "quantile_over_time",
				Quantile: -0.1,
				Op:       ">",
				Value:    10,
			},
			wantErr: true,
			errMsg:  "expr.quantile must be within [0,1]",
		},
		{
			name: "invalid_quantile_high",
			expr: ExprCfg{
				Type:     "quantile_over_time",
				Quantile: 1.1,
				Op:       ">",
				Value:    10,
			},
			wantErr: true,
			errMsg:  "expr.quantile must be within [0,1]",
		},
		{
			name: "missing_rate_duration",
			expr: ExprCfg{
				Type:  "rate",
				Op:    ">",
				Value: 10,
			},
			wantErr: true,
			errMsg:  "expr.rate_duration must be > 0 for rate",
		},
		{
			name: "all_comparison_ops",
			expr: ExprCfg{
				Type:  "count_over_time",
				Op:    "<=",
				Value: 10,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.expr.validate()
			if tt.wantErr {
				require.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestMemoryConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *Config
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid_memory_config",
			cfg: &Config{
				WindowSize: 5 * time.Second,
				TSDB:       &TSDBConfig{QueryURL: "http://test"},
				Memory: MemoryConfig{
					MaxMemoryPercent:             0.2,
					ScaleUpThreshold:             0.8,
					ScaleDownThreshold:           0.3,
					MemoryPressureThreshold:      0.9,
					SamplingRateUnderPressure:    0.1,
					EnableAdaptiveScaling:        true,
					EnableMemoryPressureHandling: true,
				},
			},
			wantErr: false,
		},
		{
			name: "negative_memory_percent",
			cfg: &Config{
				WindowSize: 5 * time.Second,
				TSDB:       &TSDBConfig{QueryURL: "http://test"},
				Memory:     MemoryConfig{MaxMemoryPercent: -0.1},
			},
			wantErr: true,
			errMsg:  "memory.max_percent must be between 0 and 1",
		},
		{
			name: "over_one_memory_percent",
			cfg: &Config{
				WindowSize: 5 * time.Second,
				TSDB:       &TSDBConfig{QueryURL: "http://test"},
				Memory:     MemoryConfig{MaxMemoryPercent: 1.1},
			},
			wantErr: true,
			errMsg:  "memory.max_percent must be between 0 and 1",
		},
		{
			name: "invalid_scale_thresholds",
			cfg: &Config{
				WindowSize: 5 * time.Second,
				TSDB:       &TSDBConfig{QueryURL: "http://test"},
				Memory: MemoryConfig{
					MaxMemoryPercent:   0.1,
					ScaleUpThreshold:   -0.1,
					ScaleDownThreshold: 1.1,
				},
			},
			wantErr: true,
			errMsg:  "must be between 0 and 1",
		},
		{
			name: "invalid_pressure_threshold",
			cfg: &Config{
				WindowSize: 5 * time.Second,
				TSDB:       &TSDBConfig{QueryURL: "http://test"},
				Memory: MemoryConfig{
					MaxMemoryPercent:        0.1,
					MemoryPressureThreshold: 2.0,
				},
			},
			wantErr: true,
			errMsg:  "memory.memory_pressure_threshold must be between 0 and 1",
		},
		{
			name: "invalid_sampling_rate",
			cfg: &Config{
				WindowSize: 5 * time.Second,
				TSDB:       &TSDBConfig{QueryURL: "http://test"},
				Memory: MemoryConfig{
					MaxMemoryPercent:          0.1,
					SamplingRateUnderPressure: -0.5,
				},
			},
			wantErr: true,
			errMsg:  "memory.sampling_rate_under_pressure must be between 0 and 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr {
				require.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRuleDefaults(t *testing.T) {
	cfg := &Config{
		WindowSize: 5 * time.Second,
		TSDB:       &TSDBConfig{QueryURL: "http://test"},
		Memory:     MemoryConfig{MaxMemoryPercent: 0.1},
		Rules: []RuleCfg{
			{
				Name:   "test",
				Signal: "traces",
				// Severity and Enabled not set
				Expr: ExprCfg{
					Type:  "count_over_time",
					Op:    ">",
					Value: 10,
				},
			},
		},
	}

	err := cfg.Validate()
	require.NoError(t, err)

	// Check defaults were applied
	assert.Equal(t, "warning", cfg.Rules[0].Severity)
	assert.True(t, cfg.Rules[0].Enabled)
}
