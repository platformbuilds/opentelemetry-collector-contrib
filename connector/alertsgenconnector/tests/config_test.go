package alertsgenconnector_test

import (
	"testing"

	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/alertsgenconnector"
)

func TestConfigValidate(t *testing.T) {
	cfg := alertsgenconnector.CreateDefaultConfig().(*alertsgenconnector.Config)
	cfg.Rules = []alertsgenconnector.RuleCfg{{
		Name:   "r1",
		Signal: "logs",
		Expr:   alertsgenconnector.ExprCfg{Type: "count_over_time", Op: ">", Value: 0},
	}}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	cfg.WindowSize = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for window=0")
	}
}
