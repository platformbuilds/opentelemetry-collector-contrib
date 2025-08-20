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

	// Rules to evaluate.
	Rules []RuleCfg `mapstructure:"rules"`

	// HA/TSDB integration for state restore.
	HA *HAConfig `mapstructure:"ha"`

	// Limits & protections.
	Limits LimitsConfig `mapstructure:"limits"`
}

// HAConfig defines HA via TSDB.
type HAConfig struct {
	// Base URL to a Prometheus/VictoriaMetrics-compatible read API (e.g. http://vm:8428).
	TSDBReadURL string `mapstructure:"tsdb_read_url"`
	// HTTP timeout for queries.
	QueryTimeout time.Duration `mapstructure:"query_timeout"`
	// Dedup window for takeover decisions.
	DedupWindow time.Duration `mapstructure:"dedup_window"`
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
}

// CardinalityCfg caps label set explosion.
type CardinalityCfg struct {
	MaxLabels     int            `mapstructure:"max_labels"`
	MaxValLen     int            `mapstructure:"max_label_value_length"`
	MaxTotalSize  int            `mapstructure:"max_total_label_size"`
	HashIfExceeds int            `mapstructure:"hash_if_exceeds"`
	Allow         []string       `mapstructure:"allowlist"`
	Block         []string       `mapstructure:"blocklist"`
	Extra         map[string]any `mapstructure:",omitempty"` // reserved
}

// RuleCfg defines one rule.
type RuleCfg struct {
	Name     string            `mapstructure:"name"`
	Signal   string            `mapstructure:"signal"` // traces|logs|metrics
	Select   map[string]string `mapstructure:"select"`
	GroupBy  []string          `mapstructure:"group_by"`
	Severity string            `mapstructure:"severity"`
	Window   time.Duration     `mapstructure:"window"`
	Step     time.Duration     `mapstructure:"step"`
	For      time.Duration     `mapstructure:"for"`

	Expr ExprCfg `mapstructure:"expr"`
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
}

func createDefaultConfig() component.Config {
	return &Config{
		WindowSize: 5 * time.Second,
		Step:       5 * time.Second,
		Limits: LimitsConfig{
			Storm: StormCfg{
				MaxTransitionsPerMinute: 0,
				MaxEventsPerInterval:    0,
				Interval:                1 * time.Second,
			},
			Cardinality: CardinalityCfg{
				MaxLabels:     20,
				MaxValLen:     128,
				MaxTotalSize:  2048,
				HashIfExceeds: 128,
				Allow:         []string{"alertname", "severity", "rule_id"},
				Block:         []string{"pod_ip", "container_id", "trace_id"},
			},
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
	if len(c.Rules) == 0 {
		return errors.New("at least one rule must be specified")
	}
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
		if r.Window == 0 {
			r.Window = c.WindowSize
		}
		if r.Step == 0 {
			r.Step = c.Step
		}
		if r.Expr.Type == "" {
			return fmt.Errorf("rule %q: expr.type required", r.Name)
		}
	}
	return nil
}
