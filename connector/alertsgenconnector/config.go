package alertsgenconnector

import (
	"fmt"
	"time"
)

// Config is the public configuration for the alertsgen connector.
// It satisfies component.Config.
type Config struct {
	// Sliding window used for evaluation. Default is 5s.
	WindowSize time.Duration `mapstructure:"window_size"`

	// Rules is the list of alert rules evaluated by the connector.
	Rules []RuleCfg `mapstructure:"rules"`
}

// RuleCfg defines a single alert rule.
type RuleCfg struct {
	Name     string            `mapstructure:"name"`
	Signal   string            `mapstructure:"signal"` // "traces" | "logs" | "metrics"
	Select   map[string]string `mapstructure:"select"` // label/attribute regex matchers
	Expr     ExprCfg           `mapstructure:"expr"`
	Window   time.Duration     `mapstructure:"window"`
	Step     time.Duration     `mapstructure:"step"`
	For      time.Duration     `mapstructure:"for"`
	GroupBy  []string          `mapstructure:"group_by"`
	Severity string            `mapstructure:"severity"`
}

// ExprCfg configures the expression used by a rule.
type ExprCfg struct {
	Type     string  `mapstructure:"type"` // avg_over_time|rate_over_time|count_over_time|latency_quantile_over_time|absent_over_time
	Field    string  `mapstructure:"field"`
	Op       string  `mapstructure:"op"`    // >, >=, <, <=, ==, !=
	Value    float64 `mapstructure:"value"` // threshold
	Quantile float64 `mapstructure:"quantile"`
}

// Validate performs basic config checks.
func (c *Config) Validate() error {
	if c.WindowSize <= 0 {
		return fmt.Errorf("window_size must be > 0")
	}
	for i, r := range c.Rules {
		if r.Name == "" {
			return fmt.Errorf("rules[%d]: name is required", i)
		}
		if r.Signal == "" {
			return fmt.Errorf("rules[%d]: signal is required", i)
		}
		if r.Expr.Type == "" {
			return fmt.Errorf("rules[%d]: expr.type is required", i)
		}
	}
	return nil
}
