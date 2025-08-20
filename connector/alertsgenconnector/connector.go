package alertsgenconnector

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
)

const typeStr = component.Type("alertsgen")

// NewFactory registers the Alertsgen connector.
func NewFactory() connector.Factory {
	return connector.NewFactory(
		typeStr,
		createDefaultConfig,
		// Same-signal edges. Add more edges here if you implement them.
		connector.WithTracesToTraces(createTracesToTraces, component.StabilityLevelAlpha),
		connector.WithLogsToLogs(createLogsToLogs, component.StabilityLevelAlpha),
		connector.WithMetricsToMetrics(createMetricsToMetrics, component.StabilityLevelAlpha),
	)
}

// createDefaultConfig returns defaults for this connector.
func createDefaultConfig() component.Config {
	return &Config{
		WindowSize: 5 * time.Second, // recommended default
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
