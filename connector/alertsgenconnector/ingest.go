package alertsgenconnector

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// Ingestor is the central struct that handles traces, logs, and metrics ingestion
type Ingestor struct {
	cfg *Config
}

// NewIngestor creates a new ingestor instance
func NewIngestor(cfg *Config) *Ingestor {
	return &Ingestor{cfg: cfg}
}

// ---- traces ----
func ingestTraces(_ context.Context, td ptrace.Traces, ing *Ingestor) error {
	if ing == nil {
		return fmt.Errorf("ingestor is nil")
	}
	if td.ResourceSpans().Len() == 0 {
		return nil
	}

	// Example trace iteration
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			spans := rs.ScopeSpans().At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				// TODO: add cardinality, HA TSDB sync, storm control, etc.
				_ = span
			}
		}
	}
	return nil
}

// ---- logs ----
func ingestLogs(_ context.Context, ld plog.Logs, ing *Ingestor) error {
	if ing == nil {
		return fmt.Errorf("ingestor is nil")
	}
	if ld.ResourceLogs().Len() == 0 {
		return nil
	}

	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			logs := rl.ScopeLogs().At(j).LogRecords()
			for k := 0; k < logs.Len(); k++ {
				logRecord := logs.At(k)
				// TODO: Apply deduplication, cardinality, storm/backpressure
				_ = logRecord
			}
		}
	}
	return nil
}

// ---- metrics ----
func ingestMetrics(_ context.Context, md pmetric.Metrics, ing *Ingestor) error {
	if ing == nil {
		return fmt.Errorf("ingestor is nil")
	}
	if md.ResourceMetrics().Len() == 0 {
		return nil
	}

	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)
		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			metrics := rm.ScopeMetrics().At(j).Metrics()
			for k := 0; k < metrics.Len(); k++ {
				metric := metrics.At(k)
				// TODO: Apply cardinality control, HA TSDB sync, etc.
				_ = metric
			}
		}
	}
	return nil
}
