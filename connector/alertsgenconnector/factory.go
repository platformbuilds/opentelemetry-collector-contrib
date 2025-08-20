package alertsgenconnector

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
)

// Component type string; convert to component.Type at call sites.
const typeStr = "alertsgen"

// NewFactory returns the Alertsgen connector factory.
func NewFactory() connector.Factory {
	return connector.NewFactory(
		component.MustNewType(typeStr),
		createDefaultConfig,
		connector.WithTracesToTraces(createTracesToTraces, component.StabilityLevelAlpha),
		connector.WithLogsToLogs(createLogsToLogs, component.StabilityLevelAlpha),
		connector.WithMetricsToMetrics(createMetricsToMetrics, component.StabilityLevelAlpha),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		WindowSize: 5 * time.Second,
		Rules:      []RuleCfg{},
	}
}

func createTracesToTraces(
	ctx context.Context,
	set connector.Settings,
	cfg component.Config,
	next consumer.Traces,
) (connector.Traces, error) {
	return newTracesConnector(ctx, set, cfg.(*Config), next)
}

func createLogsToLogs(
	ctx context.Context,
	set connector.Settings,
	cfg component.Config,
	next consumer.Logs,
) (connector.Logs, error) {
	return newLogsConnector(ctx, set, cfg.(*Config), next)
}

func createMetricsToMetrics(
	ctx context.Context,
	set connector.Settings,
	cfg component.Config,
	next consumer.Metrics,
) (connector.Metrics, error) {
	return newMetricsConnector(ctx, set, cfg.(*Config), next)
}
