package alertsgenconnector

import (
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
)

// Config is the top-level connector configuration.
type Config struct {
	component.Config `mapstructure:",squash"`

	// Sliding evaluation window (default 5s).
	WindowSize time.Duration `mapstructure:"window"`

	// Step/interval to run evaluations (default = window).
	Step time.Duration `mapstructure:"step"`

	// Unique identifier for this collector instance (for HA coordination).
	InstanceID string `mapstructure:"instance_id"`

	// Rules to evaluate.
	Rules []RuleCfg `mapstructure:"rules"`

	// HA/TSDB integration for state restore and coordination.
	TSDB *TSDBConfig `mapstructure:"tsdb"`

	// Deduplication settings.
	Dedup DedupConfig `mapstructure:"dedup"`

	// Limits & protections.
	Limits LimitsConfig `mapstructure:"limits"`

	// Notification settings.
	Notify NotifyConfig `mapstructure:"notify"`

	// Memory management settings.
	Memory MemoryConfig `mapstructure:"memory"`
}

// MemoryConfig controls adaptive memory management for high-throughput scenarios
type MemoryConfig struct {
	// Maximum total memory usage in bytes (0 = auto-detect based on available memory)
	MaxMemoryBytes int64 `mapstructure:"max_memory_bytes"`

	// Memory usage as percentage of available system memory (default: 10%)
	MaxMemoryPercent float64 `mapstructure:"max_memory_percent"`

	// Per-signal buffer limits (0 = auto-calculate)
	MaxTraceEntries  int `mapstructure:"max_trace_entries"`
	MaxLogEntries    int `mapstructure:"max_log_entries"`
	MaxMetricEntries int `mapstructure:"max_metric_entries"`

	// Adaptive scaling settings
	EnableAdaptiveScaling bool          `mapstructure:"enable_adaptive_scaling"`
	ScaleUpThreshold      float64       `mapstructure:"scale_up_threshold"`   // Scale up when usage > threshold
	ScaleDownThreshold    float64       `mapstructure:"scale_down_threshold"` // Scale down when usage < threshold
	ScaleCheckInterval    time.Duration `mapstructure:"scale_check_interval"`
	MaxScaleFactor        float64       `mapstructure:"max_scale_factor"` // Maximum buffer size multiplier

	// Memory pressure handling
	EnableMemoryPressureHandling bool    `mapstructure:"enable_memory_pressure_handling"`
	MemoryPressureThreshold      float64 `mapstructure:"memory_pressure_threshold"`    // Start dropping data when memory usage > threshold
	SamplingRateUnderPressure    float64 `mapstructure:"sampling_rate_under_pressure"` // Sample rate when under pressure

	// Ring buffer settings
	UseRingBuffers      bool `mapstructure:"use_ring_buffers"`      // Use ring buffers instead of slices
	RingBufferOverwrite bool `mapstructure:"ring_buffer_overwrite"` // Overwrite old data when buffer is full
}

// TSDBConfig defines TSDB integration for HA and state persistence.
type TSDBConfig struct {
	// Query URL for reading existing alert state (Prometheus/VM compatible)
	QueryURL string `mapstructure:"query_url"`

	// Remote write URL for publishing alert state (optional, enables HA coordination)
	RemoteWriteURL string `mapstructure:"remote_write_url"`

	// HTTP timeout for read queries.
	QueryTimeout time.Duration `mapstructure:"query_timeout"`

	// HTTP timeout for remote write requests.
	WriteTimeout time.Duration `mapstructure:"write_timeout"`

	// Window for deduplication decisions during takeover.
	DedupWindow time.Duration `mapstructure:"dedup_window"`

	// Basic auth credentials for TSDB endpoints (optional).
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`

	// Custom headers for TSDB requests (optional).
	Headers map[string]string `mapstructure:"headers"`

	// Enable/disable remote write publishing.
	EnableRemoteWrite bool `mapstructure:"enable_remote_write"`

	// Batch size for remote write requests.
	RemoteWriteBatchSize int `mapstructure:"remote_write_batch_size"`

	// Flush interval for batched remote writes.
	RemoteWriteFlushInterval time.Duration `mapstructure:"remote_write_flush_interval"`
}

// DedupConfig controls alert deduplication behavior.
type DedupConfig struct {
	// Labels to include in fingerprint calculation for deduplication.
	FingerprintLabels []string `mapstructure:"fingerprint_labels"`

	// Labels to exclude from alert output (but not from fingerprinting).
	ExcludeLabels []string `mapstructure:"exclude_labels"`

	// Time window for fingerprint-based deduplication.
	Window time.Duration `mapstructure:"window"`

	// Enable TSDB-based deduplication across instances.
	EnableTSDBDedup bool `mapstructure:"enable_tsdb_dedup"`
}

// NotifyConfig controls downstream notifications.
type NotifyConfig struct {
	// AlertManager endpoints for webhook notifications.
	AlertManagerURLs []string `mapstructure:"alertmanager_urls"`

	// HTTP timeout for notification requests.
	Timeout time.Duration `mapstructure:"timeout"`

	// Enable/disable notifications.
	Enabled bool `mapstructure:"enabled"`

	// Custom headers for notification requests.
	Headers map[string]string `mapstructure:"headers"`

	// Retry configuration for failed notifications.
	Retry RetryConfig `mapstructure:"retry"`
}

// RetryConfig defines retry behavior for failed operations.
type RetryConfig struct {
	// Enable retries.
	Enabled bool `mapstructure:"enabled"`

	// Maximum number of retry attempts.
	MaxAttempts int `mapstructure:"max_attempts"`

	// Initial backoff delay.
	InitialDelay time.Duration `mapstructure:"initial_delay"`

	// Maximum backoff delay.
	MaxDelay time.Duration `mapstructure:"max_delay"`

	// Backoff multiplier.
	Multiplier float64 `mapstructure:"multiplier"`
}

// LimitsConfig bundles protections.
type LimitsConfig struct {
	Storm       StormCfg       `mapstructure:"storm"`
	Cardinality CardinalityCfg `mapstructure:"cardinality"`
}

// StormCfg limits alert floods.
type StormCfg struct {
	// max transitions across all rules per minute; 0 = unlimited
	MaxTransitionsPerMinute int `mapstructure:"max_transitions_per_minute"`
	// max events per limiter interval
	MaxEventsPerInterval int `mapstructure:"max_events_per_interval"`
	// limiter interval window
	Interval time.Duration `mapstructure:"interval"`
	// circuit breaker threshold (0.0-1.0, fraction of failures to trigger)
	CircuitBreakerThreshold float64 `mapstructure:"circuit_breaker_threshold"`
	// circuit breaker recovery time
	CircuitBreakerRecoveryTime time.Duration `mapstructure:"circuit_breaker_recovery_time"`
}

// CardinalityCfg caps label set explosion.
type CardinalityCfg struct {
	MaxLabels            int            `mapstructure:"max_labels"`
	MaxValLen            int            `mapstructure:"max_label_value_length"`
	MaxTotalSize         int            `mapstructure:"max_total_label_size"`
	HashIfExceeds        int            `mapstructure:"hash_if_exceeds"`
	Allow                []string       `mapstructure:"allowlist"`
	Block                []string       `mapstructure:"blocklist"`
	MaxSeriesPerRule     int            `mapstructure:"max_series_per_rule"`
	LabelSamplingEnabled bool           `mapstructure:"label_sampling_enabled"`
	LabelSamplingRate    float64        `mapstructure:"label_sampling_rate"`
	Extra                map[string]any `mapstructure:",omitempty"` // reserved
}

// RuleCfg defines one rule.
type RuleCfg struct {
	Name        string            `mapstructure:"name"`
	Signal      string            `mapstructure:"signal"` // traces|logs|metrics
	Select      map[string]string `mapstructure:"select"`
	GroupBy     []string          `mapstructure:"group_by"`
	Severity    string            `mapstructure:"severity"`
	Window      time.Duration     `mapstructure:"window"`
	Step        time.Duration     `mapstructure:"step"`
	For         time.Duration     `mapstructure:"for"`
	Annotations map[string]string `mapstructure:"annotations"`
	Labels      map[string]string `mapstructure:"labels"`
	Enabled     bool              `mapstructure:"enabled"`

	Expr ExprCfg `mapstructure:"expr"`

	// Output configuration per rule.
	Outputs OutputCfg `mapstructure:"outputs"`
}

// OutputCfg controls how alerts are emitted.
type OutputCfg struct {
	// Emit as structured logs.
	Log LogOutputCfg `mapstructure:"log"`

	// Emit as metrics.
	Metric MetricOutputCfg `mapstructure:"metric"`

	// Custom notification targets.
	Notify NotifyOutputCfg `mapstructure:"notify"`
}

// LogOutputCfg controls log output format.
type LogOutputCfg struct {
	Enabled     bool              `mapstructure:"enabled"`
	Level       string            `mapstructure:"level"`
	Format      string            `mapstructure:"format"` // json|text
	ExtraFields map[string]string `mapstructure:"extra_fields"`
}

// MetricOutputCfg controls metric output.
type MetricOutputCfg struct {
	Enabled     bool              `mapstructure:"enabled"`
	Name        string            `mapstructure:"name"`
	ExtraLabels map[string]string `mapstructure:"extra_labels"`
}

// NotifyOutputCfg controls per-rule notifications.
type NotifyOutputCfg struct {
	Enabled   bool     `mapstructure:"enabled"`
	Targets   []string `mapstructure:"targets"`
	OnlyState []string `mapstructure:"only_state"` // firing|resolved|no_data
}

// ExprCfg describes the evaluation function & comparison.
type ExprCfg struct {
	// One of avg_over_time|rate_over_time|count_over_time|latency_quantile_over_time|absent_over_time
	Type string `mapstructure:"type"`

	// Field depends on type (e.g., metric value or span duration).
	Field string  `mapstructure:"field"`
	Op    string  `mapstructure:"op"` // >, >=, <, <=, ==, !=
	Value float64 `mapstructure:"value"`

	// For quantile-based exprs.
	Quantile float64 `mapstructure:"quantile"`

	// For rate-based exprs, duration to calculate rate over.
	RateDuration time.Duration `mapstructure:"rate_duration"`

	// Custom evaluation options.
	Options map[string]interface{} `mapstructure:"options"`
}

func CreateDefaultConfig() component.Config {
	return &Config{
		WindowSize: 5 * time.Second,
		Step:       5 * time.Second,
		InstanceID: "default",
		TSDB: &TSDBConfig{
			QueryTimeout:             30 * time.Second,
			WriteTimeout:             10 * time.Second,
			DedupWindow:              30 * time.Second,
			EnableRemoteWrite:        true,
			RemoteWriteBatchSize:     1000,
			RemoteWriteFlushInterval: 5 * time.Second,
		},
		Dedup: DedupConfig{
			FingerprintLabels: []string{"alertname", "severity", "cluster", "namespace", "service"},
			ExcludeLabels:     []string{"pod_ip", "container_id", "timestamp", "trace_id"},
			Window:            30 * time.Second,
			EnableTSDBDedup:   true,
		},
		Limits: LimitsConfig{
			Storm: StormCfg{
				MaxTransitionsPerMinute:    100,
				MaxEventsPerInterval:       50,
				Interval:                   1 * time.Second,
				CircuitBreakerThreshold:    0.5,
				CircuitBreakerRecoveryTime: 30 * time.Second,
			},
			Cardinality: CardinalityCfg{
				MaxLabels:            20,
				MaxValLen:            128,
				MaxTotalSize:         2048,
				HashIfExceeds:        128,
				Allow:                []string{"alertname", "severity", "rule_id"},
				Block:                []string{"pod_ip", "container_id", "trace_id"},
				MaxSeriesPerRule:     1000,
				LabelSamplingEnabled: false,
				LabelSamplingRate:    0.1,
			},
		},
		Notify: NotifyConfig{
			Enabled: true,
			Timeout: 10 * time.Second,
			Retry: RetryConfig{
				Enabled:      true,
				MaxAttempts:  3,
				InitialDelay: 1 * time.Second,
				MaxDelay:     30 * time.Second,
				Multiplier:   2.0,
			},
		},
		Memory: MemoryConfig{
			MaxMemoryPercent:             0.10, // 10% of system memory
			MaxTraceEntries:              0,    // Auto-calculate
			MaxLogEntries:                0,    // Auto-calculate
			MaxMetricEntries:             0,    // Auto-calculate
			EnableAdaptiveScaling:        true,
			ScaleUpThreshold:             0.8, // Scale up at 80% usage
			ScaleDownThreshold:           0.4, // Scale down below 40% usage
			ScaleCheckInterval:           30 * time.Second,
			MaxScaleFactor:               10.0, // Allow up to 10x scaling
			EnableMemoryPressureHandling: true,
			MemoryPressureThreshold:      0.85,  // Memory pressure at 85%
			SamplingRateUnderPressure:    0.1,   // 10% sampling under pressure
			UseRingBuffers:               false, // Use slices by default
			RingBufferOverwrite:          true,  // Overwrite when full
		},
	}
}

func (c *Config) Validate() error {
	if c.WindowSize <= 0 {
		return errors.New("window must be > 0")
	}
	if c.Step <= 0 {
		c.Step = c.WindowSize
	}
	if c.InstanceID == "" {
		c.InstanceID = "default"
	}
	if len(c.Rules) == 0 {
		return errors.New("at least one rule must be specified")
	}

	// Validate TSDB config
	if c.TSDB != nil {
		if c.TSDB.QueryURL == "" {
			return errors.New("tsdb.query_url required")
		}
		if c.TSDB.EnableRemoteWrite && c.TSDB.RemoteWriteURL == "" {
			return errors.New("tsdb.remote_write_url required when enable_remote_write is true")
		}
		if c.TSDB.QueryTimeout <= 0 {
			c.TSDB.QueryTimeout = 30 * time.Second
		}
		if c.TSDB.WriteTimeout <= 0 {
			c.TSDB.WriteTimeout = 10 * time.Second
		}
		if c.TSDB.DedupWindow <= 0 {
			c.TSDB.DedupWindow = 30 * time.Second
		}
		if c.TSDB.RemoteWriteBatchSize <= 0 {
			c.TSDB.RemoteWriteBatchSize = 1000
		}
		if c.TSDB.RemoteWriteFlushInterval <= 0 {
			c.TSDB.RemoteWriteFlushInterval = 5 * time.Second
		}
	}

	// Validate dedup config
	if c.Dedup.Window <= 0 {
		c.Dedup.Window = 30 * time.Second
	}

	// Validate memory config
	if c.Memory.MaxMemoryPercent <= 0 {
		c.Memory.MaxMemoryPercent = 0.10
	}
	if c.Memory.MaxMemoryPercent > 1.0 {
		return errors.New("memory.max_memory_percent must be <= 1.0")
	}
	if c.Memory.ScaleUpThreshold <= 0 || c.Memory.ScaleUpThreshold > 1.0 {
		return errors.New("memory.scale_up_threshold must be between 0 and 1")
	}
	if c.Memory.ScaleDownThreshold <= 0 || c.Memory.ScaleDownThreshold > 1.0 {
		return errors.New("memory.scale_down_threshold must be between 0 and 1")
	}
	if c.Memory.ScaleUpThreshold <= c.Memory.ScaleDownThreshold {
		return errors.New("memory.scale_up_threshold must be > scale_down_threshold")
	}
	if c.Memory.MaxScaleFactor <= 1.0 {
		return errors.New("memory.max_scale_factor must be > 1.0")
	}
	if c.Memory.MemoryPressureThreshold <= 0 || c.Memory.MemoryPressureThreshold > 1.0 {
		return errors.New("memory.memory_pressure_threshold must be between 0 and 1")
	}
	if c.Memory.SamplingRateUnderPressure < 0 || c.Memory.SamplingRateUnderPressure > 1.0 {
		return errors.New("memory.sampling_rate_under_pressure must be between 0 and 1")
	}

	// Validate rules
	seen := map[string]struct{}{}
	for i := range c.Rules {
		r := &c.Rules[i]
		if r.Name == "" {
			return fmt.Errorf("rule[%d]: name required", i)
		}
		if _, dup := seen[r.Name]; dup {
			return fmt.Errorf("duplicate rule name %q", r.Name)
		}
		seen[r.Name] = struct{}{}
		if r.Signal == "" {
			return fmt.Errorf("rule %q: signal required", r.Name)
		}
		if r.Signal != "traces" && r.Signal != "logs" && r.Signal != "metrics" {
			return fmt.Errorf("rule %q: signal must be traces, logs, or metrics", r.Name)
		}
		if r.Window == 0 {
			r.Window = c.WindowSize
		}
		if r.Step == 0 {
			r.Step = c.Step
		}
		if r.Expr.Type == "" {
			return fmt.Errorf("rule %q: expr.type required", r.Name)
		}
		if r.Expr.Op == "" {
			return fmt.Errorf("rule %q: expr.op required", r.Name)
		}
		if r.Severity == "" {
			r.Severity = "warning"
		}
		// Default rule to enabled if not specified
		if !r.Enabled {
			r.Enabled = true
		}
		// Validate expression type
		validExprTypes := []string{
			"avg_over_time", "rate_over_time", "count_over_time",
			"latency_quantile_over_time", "absent_over_time",
		}
		validType := false
		for _, validExpr := range validExprTypes {
			if r.Expr.Type == validExpr {
				validType = true
				break
			}
		}
		if !validType {
			return fmt.Errorf("rule %q: invalid expr.type %q", r.Name, r.Expr.Type)
		}
		// Validate quantile for quantile expressions
		if r.Expr.Type == "latency_quantile_over_time" {
			if r.Expr.Quantile <= 0 || r.Expr.Quantile > 1 {
				return fmt.Errorf("rule %q: quantile must be between 0 and 1", r.Name)
			}
		}
		// Validate operator
		validOps := []string{">", ">=", "<", "<=", "==", "!="}
		validOp := false
		for _, op := range validOps {
			if r.Expr.Op == op {
				validOp = true
				break
			}
		}
		if !validOp {
			return fmt.Errorf("rule %q: invalid expr.op %q", r.Name, r.Expr.Op)
		}
	}

	// Validate storm limits
	if c.Limits.Storm.CircuitBreakerThreshold < 0 || c.Limits.Storm.CircuitBreakerThreshold > 1 {
		return errors.New("storm.circuit_breaker_threshold must be between 0 and 1")
	}

	// Validate cardinality limits
	if c.Limits.Cardinality.LabelSamplingRate < 0 || c.Limits.Cardinality.LabelSamplingRate > 1 {
		return errors.New("cardinality.label_sampling_rate must be between 0 and 1")
	}

	// Validate notify config
	if c.Notify.Retry.Multiplier <= 0 {
		c.Notify.Retry.Multiplier = 2.0
	}
	if c.Notify.Retry.MaxAttempts <= 0 {
		c.Notify.Retry.MaxAttempts = 3
	}

	return nil
}
