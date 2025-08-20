package alertsgenconnector

import (
	"testing"
	"time"
)

func TestConfigValidate(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Rules = []RuleCfg{{
		Name:   "r1",
		Signal: "logs",
		Expr:   ExprCfg{Type: "count_over_time", Op: ">", Value: 0},
	}}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	cfg.WindowSize = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for window=0")
	}
}

func TestDefaultStep(t *testing.T) {
	cfg := &Config{
		WindowSize: 5 * time.Second,
		Rules: []RuleCfg{{
			Name:   "r1",
			Signal: "metrics",
			Expr:   ExprCfg{Type: "avg_over_time", Op: ">", Value: 1},
		}},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if cfg.Rules[0].Step != 5*time.Second {
		t.Fatalf("expected step=window, got %v", cfg.Rules[0].Step)
	}
}
