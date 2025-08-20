package alertsgenconnector

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// NOTE:
// This file defines ONLY the concrete connector structs and their methods.
// The rule engine implementation lives in rules.go (no duplication).

// ---- Common skeleton ----

type baseConnector struct {
	settings connector.Settings
	cfg      *Config
}

func (b *baseConnector) Start(_ context.Context, _ component.Host) error {
	// Validate config early.
	return b.cfg.Validate()
}

func (b *baseConnector) Shutdown(_ context.Context) error {
	return nil
}

func (b *baseConnector) Capabilities() consumer.Capabilities {
	// We don't mutate upstream data.
	return consumer.Capabilities{MutatesData: false}
}

// ---- Traces -> Traces ----

type tracesConnector struct {
	baseConnector
	next consumer.Traces
}

func newTracesConnector(_ context.Context, set connector.Settings, cfg *Config, next consumer.Traces) (connector.Traces, error) {
	return &tracesConnector{
		baseConnector: baseConnector{settings: set, cfg: cfg},
		next:          next,
	}, nil
}

func (c *tracesConnector) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	// TODO: wire rule evaluation / emission here.
	// For now, pass-through to next consumer to keep the pipeline working.
	return c.next.ConsumeTraces(ctx, td)
}

// ---- Logs -> Logs ----

type logsConnector struct {
	baseConnector
	next consumer.Logs
}

func newLogsConnector(_ context.Context, set connector.Settings, cfg *Config, next consumer.Logs) (connector.Logs, error) {
	return &logsConnector{
		baseConnector: baseConnector{settings: set, cfg: cfg},
		next:          next,
	}, nil
}

func (c *logsConnector) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	// TODO: wire rule evaluation / emission here.
	return c.next.ConsumeLogs(ctx, ld)
}

// ---- Metrics -> Metrics ----

type metricsConnector struct {
	baseConnector
	next consumer.Metrics
}

func newMetricsConnector(_ context.Context, set connector.Settings, cfg *Config, next consumer.Metrics) (connector.Metrics, error) {
	return &metricsConnector{
		baseConnector: baseConnector{settings: set, cfg: cfg},
		next:          next,
	}, nil
}

func (c *metricsConnector) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	// TODO: wire rule evaluation / emission here.
	return c.next.ConsumeMetrics(ctx, md)
}
