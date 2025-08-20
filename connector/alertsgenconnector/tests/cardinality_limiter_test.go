package alertsgenconnector

import (
	"testing"

	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/alertsgenconnector/cardinality"
)

func TestCardinalityLimit(t *testing.T) {
	c := &cardinality.Control{
		MaxLabels: 3,
		Allow:     map[string]struct{}{"a": {}, "b": {}},
		Block:     map[string]struct{}{},
	}
	lbls := map[string]string{"a": "1", "b": "2", "c": "3", "d": "4"}
	out := c.Enforce(lbls)
	if len(out) > 3 {
		t.Fatalf("expected <=3 labels, got %d", len(out))
	}
}
