package alertsgenconnector_test

import (
	"testing"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/alertsgenconnector/storm"
)

func TestStormLimiter(t *testing.T) {
	lim := storm.New(5, 3, 100*time.Millisecond)
	if a, d := lim.Allow(10); a != 3 || d != 7 {
		t.Fatalf("want allow=3 drop=7, got allow=%d drop=%d", a, d)
	}
}
